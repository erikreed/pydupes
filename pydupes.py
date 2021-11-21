import os
import hashlib
import itertools
import logging
import os
import pathlib
import threading
import typing
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from stat import S_ISLNK, S_ISREG, S_ISDIR

import click

IO_CONCURRENCY = 16
lock = threading.Lock()
logger = logging.getLogger(__name__)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.3f}Yi{suffix}"


def sha256sum(path: str) -> bytes:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(path, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.digest()


class FileCrawler:
    def __init__(self, root: pathlib.Path, prog: tqdm = None):
        assert root.is_dir()
        # using newline-delimited stdout
        assert '\n' not in str(root.absolute())
        self._size_to_paths = defaultdict(list)
        self._pool = ThreadPoolExecutor(max_workers=IO_CONCURRENCY)
        self._root = root
        self._root_stat = root.lstat()
        self._traversed_inodes = set()
        self._prog = prog

    def traverse_path(self, path: typing.Optional[str] = None):
        if not path:
            path = str(self._root)
        for p in os.scandir(path):
            if self._prog is not None:
                self._prog.update(1)
            stat = p.stat(follow_symlinks=False)
            if stat.st_dev != self._root_stat.st_dev:
                logger.debug('Skipping file on separate device %s', p)
                continue
            if S_ISLNK(stat.st_mode):
                logger.debug('Skipping symlink %s', p)
                continue
            if S_ISREG(stat.st_mode):
                if stat.st_ino in self._traversed_inodes:
                    logger.debug('Skipping hardlink %s', p)
                    continue
                self._traversed_inodes.add(stat.st_ino)
                self._size_to_paths[stat.st_size].append(p.path)
            elif S_ISDIR(stat.st_mode):
                yield self._pool.submit(self.traverse_path, p.path)
            else:
                logger.debug('Skipping device/socket/unknown: %s', p)

    def size_bytes(self):
        return sum(self._size_to_paths.keys())

    def size_bytes_unique(self):
        return sum(k for k, v in self._size_to_paths.items() if len(v) == 1)

    def files(self):
        return itertools.chain.from_iterable(self._size_to_paths.values())

    def filter_groups(self):
        return {k: v for k, v in self._size_to_paths.items()
                if len(v) > 1 and k > 1}


def _unwrap_futures(futures):
    for f in futures:
        _unwrap_futures(f.result())


def find_dupes(size: int,
               paths: typing.List[str], print=False,
               file_prog: tqdm = None,
               bytes_prog: tqdm = None):
    hashes = dict()
    dupes = []
    for p in sorted(paths, key=len):
        hash = sha256sum(p)
        existing = hashes.get(hash)
        if existing:
            dupes.append(p)
            if print:
                with lock:
                    # ensure these lines are printed atomically
                    print(os.path.abspath(existing))
                    print(os.path.abspath(p))
        else:
            hashes[hash] = p
        if file_prog is not None:
            file_prog.update(1)
        if bytes_prog is not None:
            bytes_prog.update(size)
    return dupes


@click.command()
@click.argument('input_path', type=click.Path(exists=True, path_type=pathlib.Path,
                                              file_okay=False, writable=True, readable=True))
def main(input_path):
    logging.basicConfig(level=logging.INFO)

    with tqdm(smoothing=0, desc='Traversing', unit=' files') as prog:
        crawler = FileCrawler(input_path, prog)
        _unwrap_futures(crawler.traverse_path())

    size = crawler.size_bytes()
    size_unique = crawler.size_bytes_unique()
    size_potential_dupes = size - size_unique
    num_files = sum(1 for _ in crawler.files())
    size_groups = crawler.filter_groups()
    num_potential_dupes = sum(len(g) for g in size_groups.values())
    del crawler

    logger.info('Cursory file count: %d (%s), excluding symlinks and dupe inodes',
                num_files,
                sizeof_fmt(size))
    logger.info('Potential duplicates from size filter: %d (%s)',
                num_potential_dupes,
                sizeof_fmt(size_potential_dupes))

    with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as pool, \
            tqdm(smoothing=0, desc='Hashing', unit=' files',
                 position=0,
                 total=num_potential_dupes) as file_prog, \
            tqdm(smoothing=0, desc='Hashing', unit='B',
                 position=1, unit_scale=True, unit_divisor=1024,
                 total=size_potential_dupes) as bytes_prog:
        size_dupes = pool.map(
            lambda kv: (kv[0], find_dupes(*kv, file_prog=file_prog, bytes_prog=bytes_prog)), size_groups.items())
    dupe_count = sum(len(v) for _, v in size_dupes)
    dupe_total_size = sum(s * len(v) for s, v in size_dupes)

    logger.info('Number of duplicate files: %s', dupe_count)
    logger.info('Size of duplicate content: %s', sizeof_fmt(dupe_total_size))


if __name__ == '__main__':
    main()
