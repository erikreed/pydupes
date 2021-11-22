import concurrent.futures
import datetime
import functools
import hashlib
import itertools
import logging
import os
import pathlib
import threading
import typing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from stat import S_ISLNK, S_ISREG, S_ISDIR

import click
from tqdm import tqdm

IO_CONCURRENCY = 16
lock = threading.Lock()
logger = logging.getLogger('pydupes')


def none_if_io_error(f):
    @wraps(f)
    def wrapper(path, *args, **kwds):
        try:
            return f(path, *args, **kwds)
        except IOError as e:
            logger.error('Unable to read "%s" due to:', path, e)

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


class FileCrawler:
    def __init__(self, roots: typing.List[pathlib.Path], progress: typing.Optional[tqdm] = None):
        assert roots
        if not all(r.is_dir() for r in roots):
            raise FatalCrawlException("All roots required to be directories")
        root_stats = [r.lstat() for r in roots]
        root_device_ids = [r.st_dev for r in root_stats]
        assert len(set(root_device_ids)) == 1, 'Unable to span multiple devices'
        self._root_device = root_device_ids[0]

        self._size_to_paths = defaultdict(list)
        self._pool = ThreadPoolExecutor(max_workers=IO_CONCURRENCY)
        self._roots = roots
        self._traversed_inodes = set()
        self._prog = progress

    def _traverse_path(self, path: str) -> typing.Iterator[concurrent.futures.Future]:
        futures = []
        next_dir = None
        for p in os.scandir(path):
            ppath = p.path
            if self._prog is not None:
                self._prog.update(1)
            stat = p.stat(follow_symlinks=False)
            if stat.st_dev != self._root_device:
                logger.debug('Skipping file on separate device %s', ppath)
                continue
            if S_ISLNK(stat.st_mode):
                logger.debug('Skipping symlink %s', ppath)
                continue
            if stat.st_ino in self._traversed_inodes:
                logger.debug('Skipping hardlink or already traversed %s', ppath)
                continue
            self._traversed_inodes.add(stat.st_ino)
            if S_ISREG(stat.st_mode):
                self._size_to_paths[stat.st_size].append(ppath)
            elif S_ISDIR(stat.st_mode):
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
        futures = [self._traverse_path(str(r)) for r in self._roots]
        self._unwrap_futures(itertools.chain.from_iterable(futures))

    def size_bytes(self):
        return sum(self._size_to_paths.keys())

    def size_bytes_unique(self):
        return sum(k for k, v in self._size_to_paths.items() if len(v) == 1)

    def files(self):
        return itertools.chain.from_iterable(self._size_to_paths.values())

    def filter_groups(self):
        return {k: v for k, v in self._size_to_paths.items()
                if len(v) > 1 and k > 1}

    @classmethod
    def _unwrap_futures(cls, futures):
        for f in futures:
            cls._unwrap_futures(f.result())


class DupeFinder:
    def __init__(self, size_bytes: int,
                 pool: concurrent.futures.Executor,
                 output: typing.Optional[typing.TextIO] = None,
                 file_progress: typing.Optional[tqdm] = None,
                 byte_progress: typing.Optional[tqdm] = None):
        self._pool = pool
        self._output = output
        self._file_progress = file_progress
        self._byte_progress = byte_progress
        self.size_bytes = size_bytes

    def find(self, paths: typing.List[str]) -> typing.List[str]:
        files_start = len(paths)
        if self.size_bytes >= 8192:
            path_groups = list(self.split_by_ends(paths))
        else:
            path_groups = [paths]
        files_end = sum(len(p) for p in path_groups)
        if self._file_progress is not None:
            self._file_progress.update(files_start - files_end)
        if self._byte_progress is not None:
            self._byte_progress.update(self.size_bytes * (files_start - files_end))

        dupes = []
        for paths in path_groups:
            hashes = dict()
            paths.sort(key=len)
            for p, hash in zip(paths, self._map(sha256sum, paths)):
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
                    self._byte_progress.update(self.size_bytes)
        return dupes

    @staticmethod
    @none_if_io_error
    def _read_edge(path: str, size: int) -> bytes:
        with open(path, 'rb', buffering=0) as f:
            if size < 0:
                f.seek(size, os.SEEK_END)
            return f.read(abs(size))

    def split_by_outer_bytes(self, group: typing.List[str]):
        matches = defaultdict(list)
        for path, edge in zip(group, self._map(functools.partial(self._read_edge, size=-4096), group)):
            matches[edge].append(path)
        return [g for edge, g in matches.items() if edge and len(g) > 1]

    def split_by_ends(self, group: typing.List[str]):
        matches = defaultdict(list)
        for path, edge in zip(group, self._map(functools.partial(self._read_edge, size=4096), group)):
            matches[edge].append(path)
        groups = [g for edge, g in matches.items() if edge and len(g) > 1]
        del matches
        for g in groups:
            yield from self.split_by_outer_bytes(g)

    def _map(self, fn, iterables):
        if len(iterables) < 16:
            return map(fn, iterables)
        return self._pool.map(fn, iterables)


@click.command()
@click.argument('input_paths', type=click.Path(exists=True, file_okay=False, writable=True, readable=True), nargs=-1)
@click.option('--output', type=click.File('w'), help='Save line-delimited input/duplicate filename pairs')
@click.option('--verbose', is_flag=True)
@click.option('--progress', is_flag=True)
def main(input_paths, output, verbose, progress):
    input_paths = [pathlib.Path(p) for p in input_paths]
    if not input_paths:
        click.echo(click.get_current_context().get_help())
        exit(1)

    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    logger.info('Traversing input paths: %s', [str(p.absolute()) for p in input_paths])

    now = datetime.datetime.now()
    with tqdm(smoothing=0, desc='Traversing', unit=' files', disable=not progress) as prog:
        crawler = FileCrawler(input_paths, prog)
        crawler.traverse()

    size = crawler.size_bytes()
    size_unique = crawler.size_bytes_unique()
    size_potential_dupes = size - size_unique
    num_files = sum(1 for _ in crawler.files())
    size_groups = crawler.filter_groups()
    num_potential_dupes = sum(len(g) for g in size_groups.values())
    del crawler

    now2 = datetime.datetime.now()
    logger.info('Traversal time: %.1fs', (now2 - now).total_seconds())
    logger.info('Cursory file count: %d (%s), excluding symlinks and dupe inodes',
                num_files,
                sizeof_fmt(size))
    logger.info('Potential duplicates from size filter: %d (%s)',
                num_potential_dupes,
                sizeof_fmt(size_potential_dupes))

    with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as scheduler_pool, \
            ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as io_pool, \
            tqdm(smoothing=0, desc='Filtering', unit=' files',
                 position=0,
                 total=num_potential_dupes,
                 disable=not progress) as file_progress, \
            tqdm(smoothing=0, desc='Filtering', unit='B',
                 position=1, unit_scale=True, unit_divisor=1024,
                 total=size_potential_dupes,
                 disable=not progress) as bytes_progress:

        futures = []
        dupe_finders = []
        for s, group in size_groups.items():
            dupe_finder = DupeFinder(size_bytes=s, pool=io_pool, output=output,
                                     file_progress=file_progress, byte_progress=bytes_progress)
            futures.append(scheduler_pool.submit(dupe_finder.find, group))
            dupe_finders.append(dupe_finder)

        dupe_count = 0
        dupe_total_size = 0
        for dupe_finder, f in zip(dupe_finders, futures):
            dupes = f.result()
            dupe_count += len(dupes)
            dupe_total_size += len(dupes) * dupe_finder.size_bytes

    now3 = datetime.datetime.now()
    logger.info('Comparison time: %.1fs', (now3 - now2).total_seconds())
    logger.info('Total time elapsed: %.1fs', (now3 - now).total_seconds())

    logger.info('Number of duplicate files: %s', dupe_count)
    logger.info('Size of duplicate content: %s', sizeof_fmt(dupe_total_size))


if __name__ == '__main__':
    "xargs -0 -n2 echo ln --force --verbose {} {} < pydupes.txt"
    main()
