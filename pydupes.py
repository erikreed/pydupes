import datetime
import functools
import hashlib
import itertools
import logging
import os
import pathlib
import stat
import threading
import typing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Executor, Future

import click
from tqdm import tqdm

lock = threading.Lock()
logger = logging.getLogger('pydupes')


def none_if_io_error(f):
    @functools.wraps(f)
    def wrapper(path, *args, **kwds):
        try:
            return f(path, *args, **kwds)
        except IOError as e:
            logger.error('Unable to read "%s" due to: %s', path, e)

    return wrapper


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.3f}Yi{suffix}"


@none_if_io_error
def sha256sum(path: str) -> bytes:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(path, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.digest()


class FatalCrawlException(Exception):
    pass


class PotentiallySingleThreadedExecutor(ThreadPoolExecutor):
    # TODO: benchmark https://github.com/brmmm3/fastthreadpool
    def __init__(self, max_workers: int, *args, **kwargs):
        self.disable = True
        if max_workers > 1:
            self.disable = False
            super().__init__(max_workers, *args, **kwargs)

    def submit(self, fn, *args, **kwargs):
        if self.disable:
            f = Future()
            f.set_result(fn(*args, **kwargs))
            return f
        else:
            return super().submit(fn, *args, **kwargs)

    def map(self, fn, *iterables):
        if self.disable:
            return list(map(fn, *iterables))
        else:
            return super().map(fn, *iterables)

    def __exit__(self, *args):
        if not self.disable:
            return super().__exit__(*args)


class FileCrawler:
    __slots__ = ('_root_device', '_size_to_paths', '_pool', '_roots', '_traversed_inodes', '_prog', 'num_directories')

    def __init__(self, roots: typing.List[pathlib.Path],
                 pool: Executor,
                 progress: typing.Optional[tqdm] = None):
        assert roots
        if not all(r.is_dir() for r in roots):
            raise FatalCrawlException("All roots required to be directories")
        root_stats = [r.lstat() for r in roots]
        root_device_ids = [r.st_dev for r in root_stats]
        assert len(set(root_device_ids)) == 1, 'Unable to span multiple devices'
        self._root_device = root_device_ids[0]

        self._size_to_paths = defaultdict(list)
        self._pool = pool
        self._roots = roots
        self._traversed_inodes = set()
        self._prog = progress
        self.num_directories = len(roots)

    def _traverse_path(self, path: str) -> typing.List[Future]:
        futures = []
        next_dir = None
        for p in os.scandir(path):
            ppath = p.path
            if self._prog is not None:
                self._prog.update(1)
            try:
                pstat = p.stat(follow_symlinks=False)
            except OSError as e:
                logger.error('Unable to stat %s due to: %s', ppath, e)
                continue
            if pstat.st_dev != self._root_device:
                logger.debug('Skipping file on separate device %s', ppath)
                continue
            if stat.S_ISLNK(pstat.st_mode):
                logger.debug('Skipping symlink %s', ppath)
                continue
            if pstat.st_ino in self._traversed_inodes:
                logger.debug('Skipping hardlink or already traversed %s', ppath)
                continue
            if pstat.st_size == 0:
                logger.debug('Skipping empty file $s', ppath)
                continue
            self._traversed_inodes.add(pstat.st_ino)
            if stat.S_ISREG(pstat.st_mode):
                self._size_to_paths[pstat.st_size].append(ppath)
            elif stat.S_ISDIR(pstat.st_mode):
                self.num_directories += 1
                if not next_dir:
                    next_dir = ppath
                else:
                    futures.append(self._pool.submit(self._traverse_path, ppath))
            else:
                logger.debug('Skipping device/socket/unknown: %s', ppath)
        if next_dir:
            futures.extend(self._traverse_path(next_dir))
        return futures

    def traverse(self):
        futures = [self._traverse_path(str(r)) for r in set(self._roots)]
        self._unwrap_futures(itertools.chain.from_iterable(futures))

    def size_bytes(self):
        return sum(k * len(v) for k, v in self._size_to_paths.items())

    def size_bytes_unique(self):
        return sum(k for k, v in self._size_to_paths.items() if len(v) == 1)

    def files(self):
        return itertools.chain.from_iterable(self._size_to_paths.values())

    def filter_groups(self):
        return [(k, v) for k, v in self._size_to_paths.items()
                if len(v) > 1 and k > 1]

    @classmethod
    def _unwrap_futures(cls, futures):
        for f in futures:
            cls._unwrap_futures(f.result())


class DupeFinder:
    __slots__ = ('_pool', '_output', '_file_progress', '_byte_progress',)

    def __init__(self, pool: Executor,
                 output: typing.Optional[typing.TextIO] = None,
                 file_progress: typing.Optional[tqdm] = None,
                 byte_progress: typing.Optional[tqdm] = None):
        self._pool = pool
        self._output = output
        self._file_progress = file_progress
        self._byte_progress = byte_progress

    def find(self, size_bytes: int, paths: typing.List[str]) -> typing.Tuple[str]:
        files_start = len(paths)
        if size_bytes > 8192:
            path_groups = list(self._split_by_boundaries(size_bytes, paths))
        else:
            path_groups = [paths]
        files_end = sum(len(p) for p in path_groups)
        if self._file_progress is not None:
            self._file_progress.update(files_start - files_end)
        if self._byte_progress is not None:
            self._byte_progress.update(size_bytes * (files_start - files_end))

        dupes = []
        for paths in path_groups:
            hashes = dict()
            paths.sort(key=len)
            for p, hash in zip(paths, self._map(sha256sum, paths, size_bytes)):
                if not hash:
                    # some read error occurred
                    continue
                existing = hashes.get(hash)
                if existing:
                    dupes.append(p)
                    if self._output:
                        with lock:
                            # ensure these lines are printed atomically
                            self._output.write(os.path.abspath(existing))
                            self._output.write('\0')
                            self._output.write(os.path.abspath(p))
                            self._output.write('\0')
                else:
                    hashes[hash] = p
                if self._file_progress is not None:
                    self._file_progress.update(1)
                if self._byte_progress is not None:
                    self._byte_progress.update(size_bytes)
        return tuple(dupes)

    @staticmethod
    @none_if_io_error
    def _read_boundary(path: str, size: int, use_hash=True) -> typing.Union[bytes, int]:
        with open(path, 'rb', buffering=0) as f:
            if size < 0:
                f.seek(size, os.SEEK_END)
            boundary = f.read(abs(size))
        if use_hash:
            return hash(boundary)
        return boundary

    def _split_by_boundary(self, size_bytes: int, group: typing.List[str], end: bool):
        matches = defaultdict(list)
        size = -4096 if end else 4096
        boundaries = self._map(functools.partial(self._read_boundary, size=size), group, size_bytes)
        for path, edge in zip(group, boundaries):
            matches[edge].append(path)
        return [g for boundary, g in matches.items() if boundary is not None and len(g) > 1]

    def _split_by_boundaries(self, size_bytes: int, group: typing.List[str]):
        groups = self._split_by_boundary(size_bytes, group, end=False)
        for g in groups:
            yield from self._split_by_boundary(size_bytes, g, end=True)

    def _map(self, fn, iterables, size_bytes):
        if len(iterables) < 16 or size_bytes > 2 ** 20 * 32:
            return map(fn, iterables)
        return self._pool.map(fn, iterables)


@click.command(help="A duplicate file finder that may be faster in environments with "
                    "millions of files and terabytes of data or over high latency filesystems.")
@click.argument('input_paths', type=click.Path(
    exists=True, file_okay=False, readable=True), nargs=-1)
@click.option('--output', type=click.File('w'),
              help='Save null-delimited input/duplicate filename pairs. For stdout use "-".')
@click.option('--verbose', is_flag=True, help='Enable debug logging.')
@click.option('--progress', is_flag=True, help='Enable progress bars.')
@click.option('--concurrency', type=click.IntRange(min=1), default=16,
              help='Level of IO concurrency. Setting to 0 disables all threading/concurrency.')
def main(input_paths, output, verbose, progress, concurrency):
    input_paths = [pathlib.Path(p) for p in input_paths]
    if not input_paths:
        click.echo(click.get_current_context().get_help())
        exit(1)

    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    logger.info('Traversing input paths: %s', [str(p.absolute()) for p in input_paths])

    time_start = datetime.datetime.now()
    with tqdm(smoothing=0, desc='Traversing', unit=' files',
              disable=not progress, mininterval=1) as file_progress, \
            PotentiallySingleThreadedExecutor(max_workers=concurrency) as io_pool:
        crawler = FileCrawler(input_paths, io_pool, file_progress)
        crawler.traverse()

    size = crawler.size_bytes()
    size_unique = crawler.size_bytes_unique()
    size_potential_dupes = size - size_unique
    num_files = sum(1 for _ in crawler.files())
    size_groups = crawler.filter_groups()
    num_potential_dupes = sum(len(g) for _, g in size_groups)

    time_traverse = datetime.datetime.now()
    logger.info('Traversal time: %.1fs', (time_traverse - time_start).total_seconds())
    logger.info('Cursory file count: %d (%s), excluding symlinks and dupe inodes',
                num_files,
                sizeof_fmt(size))
    logger.info('Directory count: %d', crawler.num_directories)
    logger.info('Size filter reduced file count to: %d (%s)',
                num_potential_dupes,
                sizeof_fmt(size_potential_dupes))
    del crawler

    with PotentiallySingleThreadedExecutor(max_workers=concurrency) as scheduler_pool, \
            PotentiallySingleThreadedExecutor(max_workers=concurrency) as io_pool, \
            tqdm(smoothing=0, desc='Filtering', unit=' files',
                 position=0, mininterval=1,
                 total=num_potential_dupes,
                 disable=not progress) as file_progress, \
            tqdm(smoothing=0, desc='Filtering', unit='B',
                 position=1, unit_scale=True, unit_divisor=1024,
                 total=size_potential_dupes, mininterval=1,
                 disable=not progress) as bytes_progress:

        futures = []
        sizes = []
        dupe_finder = DupeFinder(pool=io_pool, output=output,
                                 file_progress=file_progress, byte_progress=bytes_progress)
        while size_groups:
            size_bytes, group = size_groups.pop()
            futures.append(scheduler_pool.submit(dupe_finder.find, size_bytes, group))
            sizes.append(size_bytes)

        dupe_count = 0
        dupe_total_size = 0
        while futures:
            dupes = futures.pop().result()
            size_bytes = sizes.pop()
            dupe_count += len(dupes)
            dupe_total_size += len(dupes) * size_bytes

    dt_complete = datetime.datetime.now()
    logger.info('Comparison time: %.1fs', (dt_complete - time_traverse).total_seconds())
    logger.info('Total time elapsed: %.1fs', (dt_complete - time_start).total_seconds())

    logger.info('Number of duplicate files: %s', dupe_count)
    logger.info('Size of duplicate content: %s', sizeof_fmt(dupe_total_size))


if __name__ == '__main__':
    main()
