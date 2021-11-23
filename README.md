`pydupes` is yet another duplicate file finder like rdfind/fdupes et al
that may be faster in environments with millions of files and terabytes
of data or over high latency filesystems (e.g. NFS).

The algorithm is similar to [rdfind](https://github.com/pauldreik/rdfind) with threading and consolidation of
filtering logic (instead of separate passes).
- traverse the input paths, collecting the inodes and file sizes
- for each set of files with the same size:
  - further split by matching 4KB on beginning/ends of files
  - for each non-unique (by size, boundaries) candidate set, compute the sha256 and emit pairs with matching hash

Constraints:
- traversals do not span multiple devices
- symlink following not implemented
- concurrent modification of a traversed directory could produce false duplicate pairs 
(modification after hash computation)

### Install
```bash
pip3 install --user --upgrade pydupes
```

### Usage

```bash
# Collect counts and stage the duplicate files, null-delimited source-target pairs:
pydupes /path1 /path2 --progress --output dupes.txt

# Sanity check a hardlinking of all matches:
xargs -0 -n2 echo ln --force --verbose {} {} < dupes.txt
```

### Benchmarks

