import datetime
import functools
import gzip
import hashlib
import itertools
import json
import logging
import os
import pathlib
import queue
import stat
import threading
import typing
from collections import defaultdict

import click
from tqdm import tqdm

CHECKPOINT_SCHEMA = 'pydupes-v1'

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


# https://stackoverflow.com/a/59832404/1337136
def cache():
    s = {}
    setdefault = s.setdefault
    n = 0

    def add(x):
        nonlocal n
        n += 1
        return setdefault(x, n) != n

    return add


class FutureFreeThreadPool:
    # TODO: benchmark https://github.com/brmmm3/fastthreadpool
    def __init__(self, threads: int, queue_size: int = 32):
        assert threads >= 0
        if threads <= 1:
            threads = 0

        self.work_queue = queue.Queue(maxsize=queue_size)
        self.threads = [threading.Thread(target=self._worker_run, daemon=True)
                        for _ in range(threads)]
        for t in self.threads:
            t.start()

    def wait_until_complete(self):
        self.work_queue.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.work_queue.join()
        for _ in self.threads:
            self.work_queue.put(None)
        for t in self.threads:
            t.join()

    @staticmethod
    def _run_task(fn_args):
        fn, cb, args = fn_args
        result = fn(*args)
        if cb:
            cb(result)

    def _worker_run(self):
        try:
            while True:
                fn_args = self.work_queue.get(block=True)
                if fn_args is None:
                    return
                self._run_task(fn_args)
                self.work_queue.task_done()
                # allow GC
                del fn_args
        except Exception:
            logger.exception('Unhandled error -- exit required')
            exit(2)

    def submit(self, fn, *args, callback=None):
        if not self.threads or len(self.work_queue.queue) > 1024:
            self._run_task((fn, callback, args))
        else:
            self.work_queue.put((fn, callback, args))

    def map_unordered(self, fn, iterables):
        size = len(iterables)
        result = []
        complete = threading.Event()

        def map_callback(out):
            result.append(out)
            if size == len(result):
                complete.set()

        for args in iterables:
            self.submit(fn, *args, callback=map_callback)
        complete.wait()
        return result

    def map(self, fn, iterables):
        if not self.threads:
            return list(map(fn, iterables))

        wrapped = functools.partial(self._wrap_with_index, fn=fn)
        iterables = list(enumerate(iterables))

        result = self.map_unordered(wrapped, iterables)
        result.sort(key=lambda ab: ab[0])
        return [r for _, r in result]

    @staticmethod
    def _wrap_with_index(i, args, fn):
        return i, fn(args)


class FileCrawler:
    def __init__(self, roots: typing.List[pathlib.Path],
                 pool: FutureFreeThreadPool,
                 progress: typing.Optional[tqdm] = None,
                 min_size: int = 1):
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
        self._traversed_inodes = cache()
        self._prog = progress
        self._min_size = min_size
        self.num_directories = len(roots)

    def _traverse_path(self, path: str):
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
            if self._traversed_inodes(pstat.st_ino):
                logger.debug('Skipping hardlink or already traversed %s', ppath)
                continue
            if stat.S_ISREG(pstat.st_mode):
                if self._min_size <= pstat.st_size:
                    self._size_to_paths[pstat.st_size].append(ppath)
            elif stat.S_ISDIR(pstat.st_mode):
                self.num_directories += 1
                if not next_dir:
                    next_dir = ppath
                else:
                    self._pool.submit(self._traverse_path, ppath)
            else:
                logger.debug('Skipping device/socket/unknown: %s', ppath)
        if next_dir:
            self._traverse_path(next_dir)

    def traverse(self):
        for r in sorted(set(self._roots)):
            self._traverse_path(str(r))
        self._pool.wait_until_complete()

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
    def __init__(self, pool: FutureFreeThreadPool,
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
            paths.sort(key=lambda s: (s.count('/'), len(s), s))
            for p, hash in zip(paths, self._map(sha256sum, paths, size_bytes)):
                if not hash:
                    # some read error occurred
                    continue
                existing = hashes.setdefault(hash, p)
                if existing != p:
                    dupes.append(p)
                    if self._output:
                        with lock:
                            # ensure these lines are printed atomically
                            self._output.write(os.path.abspath(p))
                            self._output.write('\0')
                            self._output.write(os.path.abspath(existing))
                            self._output.write('\0')
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
@click.option('--min-size', type=click.IntRange(min=0), default=1,
              help='Minimum file size (in bytes) to consider during traversal.')
@click.option('--read-concurrency', type=click.IntRange(min=1), default=8,
              help='I/O concurrency for reading files.')
@click.option('--traversal-concurrency', type=click.IntRange(min=1), default=1,
              help='I/O concurrency for traversal (stat and listing syscalls).')
@click.option('--traversal-checkpoint', type=click.Path(),
              help='Persist the traversal index in jsonl format, or load an '
                   'existing traversal if already exists. Use .gz extension to compress. Input paths are '
                   'ignored if a traversal checkpoint is loaded.')
def main(input_paths, output, verbose, progress, read_concurrency, traversal_concurrency,
         traversal_checkpoint, min_size):
    input_paths = [pathlib.Path(p) for p in input_paths]
    traversal_checkpoint = pathlib.Path(traversal_checkpoint) if traversal_checkpoint else None
    if not input_paths and not traversal_checkpoint:
        click.echo(click.get_current_context().get_help())
        exit(1)

    time_start = datetime.datetime.now()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    if traversal_checkpoint and traversal_checkpoint.exists():
        size_groups, num_potential_dupes, size_potential_dupes = load_traversal_checkpoint(
            traversal_checkpoint)
    else:
        size_groups, num_potential_dupes, size_potential_dupes = traverse_paths(
            progress, traversal_concurrency, input_paths, traversal_checkpoint, min_size)

    dt_filter_start = datetime.datetime.now()
    with FutureFreeThreadPool(threads=read_concurrency) as scheduler_pool, \
            FutureFreeThreadPool(threads=read_concurrency) as io_pool, \
            tqdm(smoothing=0, desc='Filtering', unit=' files',
                 position=0, mininterval=1,
                 total=num_potential_dupes,
                 disable=not progress) as file_progress, \
            tqdm(smoothing=0, desc='Filtering', unit='B',
                 position=1, unit_scale=True, unit_divisor=1024,
                 total=size_potential_dupes, mininterval=1,
                 disable=not progress) as bytes_progress:

        size_num_dupes = []
        dupe_finder = DupeFinder(pool=io_pool, output=output, file_progress=file_progress,
                                 byte_progress=bytes_progress)

        def callback(args):
            size, dupes = args
            size_num_dupes.append((size, len(dupes)))

        def return_with_size(size_bytes, group):
            dupes = dupe_finder.find(size_bytes, group)
            return size_bytes, dupes

        for size_bytes, group in size_groups:
            if size_bytes >= min_size:
                scheduler_pool.submit(return_with_size, size_bytes, group, callback=callback)
            else:
                file_progress.update(len(group))
                bytes_progress.update(len(group) * size_bytes)
        scheduler_pool.wait_until_complete()

    dupe_count = 0
    dupe_total_size = 0
    for size_bytes, num_dupes in size_num_dupes:
        dupe_count += num_dupes
        dupe_total_size += num_dupes * size_bytes

    dt_complete = datetime.datetime.now()
    logger.info('Comparison time: %.1fs', (dt_complete - dt_filter_start).total_seconds())
    logger.info('Total time elapsed: %.1fs', (dt_complete - time_start).total_seconds())

    logger.info('Number of duplicate files: %s', dupe_count)
    logger.info('Size of duplicate content: %s', sizeof_fmt(dupe_total_size))


def load_traversal_checkpoint(traversal_checkpoint: pathlib.Path):
    f = gzip.open(traversal_checkpoint, 'rt') if traversal_checkpoint.suffix == '.gz' else traversal_checkpoint.open()

    header = json.loads(f.readline())
    if header.get('schema') != CHECKPOINT_SCHEMA:
        logger.critical("Traversal schema mismatched -- either malformed or incompatible. Header was: %s", header)
        exit(3)

    def size_groups_iter():
        for line in f:
            size, group = json.loads(line)
            yield size, group

        f.close()

    num_potential_dupes = header['num_potential_dupes']
    size_potential_dupes = header['size_potential_dupes']
    return size_groups_iter(), num_potential_dupes, size_potential_dupes


def traverse_paths(progress: bool, traversal_concurrency: int, input_paths: typing.List[pathlib.Path],
                   traversal_checkpoint: typing.Optional[pathlib.Path], min_size: int):
    logger.info('Traversing input paths: %s', [str(p.absolute()) for p in input_paths])

    time_start = datetime.datetime.now()
    with tqdm(smoothing=0, desc='Traversing', unit=' files',
              disable=not progress, mininterval=1) as file_progress, \
            FutureFreeThreadPool(threads=traversal_concurrency) as io_pool:
        crawler = FileCrawler(input_paths, io_pool, file_progress, min_size)
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
    logger.info('Number of candidate groups: %s (largest is %s files)',
                len(size_groups), max((len(g) for _, g in size_groups), default=0))
    logger.info('Size filter reduced file count to: %d (%s)',
                num_potential_dupes,
                sizeof_fmt(size_potential_dupes))
    del crawler
    size_groups.sort(key=lambda sg: sg[0] * len(sg[1]))
    if traversal_checkpoint:
        logger.info('Saving traversal checkpoint to: %s', traversal_checkpoint)
        with (
                gzip.open(traversal_checkpoint, 'wt')
                if traversal_checkpoint.suffix == '.gz'
                else traversal_checkpoint.open('w')
        ) as f:
            json.dump(dict(
                num_potential_dupes=num_potential_dupes,
                size_potential_dupes=size_potential_dupes,
                group_count=len(size_groups),
                file_count=num_potential_dupes,
                schema=CHECKPOINT_SCHEMA
            ), f)
            f.write('\n')

            prog = tqdm(reversed(size_groups), smoothing=0, desc='Saving traversal checkpoint',
                        total=len(size_groups), mininterval=1,
                        disable=not progress)
            for i, row in enumerate(prog):
                json.dump(row, f)
                f.write('\n')

    def popping_iterator():
        while size_groups:
            yield size_groups.pop()

    return popping_iterator(), num_potential_dupes, size_potential_dupes


if __name__ == '__main__':
    main()
