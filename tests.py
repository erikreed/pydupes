from click.testing import CliRunner
import collections
import io
import os
import pathlib
import tempfile

import pytest
import tqdm

from pydupes import (
    DupeFinder, FileCrawler, PotentiallySingleThreadedExecutor, FatalCrawlException, main)


def create_dir1(root: pathlib.Path):
    root.mkdir(parents=True, exist_ok=True)
    f1 = root / 'small.txt'
    f1.write_text("small file")

    f2 = root / 'empty.txt'
    f2.write_text("")

    f3 = root / 'large.bin'
    bin = [1] * 65536
    f3.write_bytes(bytes(bin))

    f4 = root / 'large-dupe.bin'
    f4.write_bytes(bytes(bin))

    f4 = root / 'large-early-boundary-diff.bin'
    bin[3] = 0
    f4.write_bytes(bytes(bin))

    f6 = root / 'large-late-boundary-diff.bin'
    bin[3] = 1
    bin[65500] = 0
    f6.write_bytes(bytes(bin))

    f7 = root / 'large-late-boundary-diff.bin'
    bin[3] = 1
    bin[65500] = 0
    f7.write_bytes(bytes(bin))

    f8 = root / 'large-middle-diff.bin'
    bin[65500] = 1
    bin[30000] = 0
    f8.write_bytes(bytes(bin))

    f9 = root / 'hardlink-to-large-middle-diff.bin'
    os.link(f8, f9)

    f10 = root / 'symlink-to-large.bin'
    os.symlink(f3, f10)


class Pools:
    single = PotentiallySingleThreadedExecutor(max_workers=0)
    threaded = PotentiallySingleThreadedExecutor(max_workers=4)


@pytest.mark.parametrize('pool', [Pools.single, Pools.threaded])
@pytest.mark.parametrize('progress', [tqdm.tqdm(disable=True), None])
class TestPydupes:
    def test_unable_read(self, pool, progress):
        with pytest.raises(FatalCrawlException):
            FileCrawler([pathlib.Path('/not-exists')], pool)

    def test_simple(self, pool, progress):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            d1 = tmp / 'dir1'
            create_dir1(d1)
            crawler = FileCrawler([d1], pool=pool, progress=progress)
            crawler.traverse()
            assert crawler.size_bytes() == 327690
            assert crawler.num_directories == 1
            assert crawler.size_bytes_unique() == 10

            # the empty file, hardlink, symlink should be skipped
            assert len(list(crawler.files())) == 6
            for f in crawler.files():
                assert 'empty' not in f
                assert 'symlink' not in f

            groups = crawler.filter_groups()
            assert len(groups) == 1
            size, g = groups[0]
            assert size == 65536
            assert len(g) == 5
            for f in g:
                assert 'large' in f

            finder = DupeFinder(pool, file_progress=progress, byte_progress=progress)
            dupes = finder.find(size, g)
            assert len(dupes) == 1
            assert dupes[0].endswith('/large-dupe.bin')

    def test_concurrent_modification(self, pool, progress):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            d1 = tmp / 'dir1'
            create_dir1(d1)
            crawler = FileCrawler([d1], pool=pool, progress=progress)
            crawler.traverse()
        groups = crawler.filter_groups()
        size, g = groups[0]
        finder = DupeFinder(pool)
        dupes = finder.find(size, g)
        assert not dupes

    def test_nested(self, pool, progress):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            d1 = tmp / 'dir1'
            d2 = tmp / 'dir2' / 'nested'
            d3 = tmp / 'dir2' / 'super' / 'nested'
            for d in [d1, d2, d3]:
                create_dir1(d)
            crawler = FileCrawler([d1, tmp / 'dir2'], pool=pool, progress=progress)
            crawler.traverse()

            assert crawler.size_bytes() == 327690 * 3
            assert crawler.num_directories == 5
            assert crawler.size_bytes_unique() == 0

            # the empty files, hardlinks, symlinks should still be skipped
            assert len(list(crawler.files())) == 6 * 3
            for f in crawler.files():
                assert 'empty' not in f
                assert 'symlink' not in f

            groups = crawler.filter_groups()
            assert len(groups) == 2
            groups.sort()
            size1, g1 = groups[0]
            assert size1 == 10
            assert len(g1) == 3
            for f in g1:
                assert 'small' in f

            finder = DupeFinder(pool, file_progress=progress, byte_progress=progress)
            dupes = finder.find(size1, g1)
            assert len(dupes) == 2

            size2, g2 = groups[1]
            assert size2 == 65536
            assert len(g2) == 15
            for f in g2:
                assert 'large' in f

            output = io.StringIO()
            finder = DupeFinder(
                pool, file_progress=progress, byte_progress=progress, output=output)
            dupes = finder.find(size2, g2)
            assert len(dupes) == 11

            pairs = collections.deque(output.getvalue().split('\0'))
            assert pairs[-1] == ''
            pairs.pop()

            assert len(pairs) == 22
            while pairs:
                a, b = pairs.popleft(), pairs.popleft()
                assert 'nested' in b or 'dupe' in b
                assert 'nested' not in a
                assert 'dupe' not in a


class TestIntegration:
    def test_cli(self):
        with tempfile.TemporaryDirectory() as tmp_dir, \
                tempfile.NamedTemporaryFile('r') as tmp_file:
            tmp_dir = pathlib.Path(tmp_dir)
            d1 = tmp_dir / 'dir1'
            create_dir1(d1)

            runner = CliRunner()
            result = runner.invoke(main, [str(d1), '--progress', '--output', tmp_file.name])
            assert result.exit_code == 0

            output = tmp_file.read()

            pairs = collections.deque(output.split('\0'))
            assert len(pairs) == 3
            assert pairs[-1] == ''
            pairs.pop()
            while pairs:
                a, b = pairs.popleft(), pairs.popleft()
                assert 'nested' in b or 'dupe' in b
                assert 'nested' not in a
                assert 'dupe' not in a
