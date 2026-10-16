#!/usr/bin/env python3
"""Power-loss crash-consistency harness for VerFSNext.

Runs as root inside the dedicated powercut VM. The daemon's data filesystem sits on
dm-log-writes, which records every block write, flush and FUA in completion order,
plus marks the workload inserts when an operation is acknowledged. Replay rebuilds
the disk at any log position the way a power cut could have left it: everything
before the last completed flush is on disk, and every later write may be present,
missing or torn at 4 KiB granularity. Each rebuilt image is mounted, the daemon is
started on it and the acknowledged-operation oracle is checked; then the image is
written again and crashed again (process kill, and on some points a second
recorded power cut), which is the sequence behind B033.
"""
import argparse
import array
import bisect
import ctypes
import errno
import hashlib
import json
import mmap
import os
from pathlib import Path
import random
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import threading
import time
import traceback

STATE = Path('/var/lib/powercut')
DEVICES = Path('/etc/powercut/devices.json')
HERE = Path(__file__).resolve().parent
BIN = str(HERE / 'verfsnext')
MNT = Path('/pc/m')
REC = Path('/pc/rec')
CHK = Path('/pc/chk')
PASSWORD = 'synthetic-powercut-data-only'
XATTR = 'user.pc'
PIECE = 4096
WORKERS = ('w0', 'w1', 'w2')
VAULT_WORKER = 'w2'
WORKER_MAX_BYTES = 48 * 1024 * 1024
SIZES = [0, 1, 4095, 4096, 4097, 65536, 131071, 300001, 1048576, 3 * 1048576 + 13]
# `verfsnext check` exists since B033; --no-records-check runs older binaries.
RECORDS_CHECK = True
FAULT_WEIGHTS = dict(idle=30, kill=22, doublekill=8, term=10, enospc=10, none=20)
OUTAGE_ERRNOS = {errno.ENOTCONN, errno.ECONNABORTED, errno.ENOENT}
CONFIG_TEMPLATE = '''mount_point = "{mount}"
data_dir = "{data}"
sync_interval_ms = 1000
batch_max_size_mb = 32
batch_flush_interval_ms = 100
metadata_cache_capacity_entries = 16384
chunk_cache_capacity_mb = 32
pack_index_cache_capacity_entries = 32768
pack_max_size_mb = 8
zstd_compression_level = 3
fuse_direct_io = false
fuse_allow_other = false
fuse_attr_ttl_ms = 150
fuse_entry_ttl_ms = 150
gc_idle_min_ms = 2000
gc_pack_rewrite_min_reclaim_bytes = 1048576
gc_pack_rewrite_min_reclaim_percent = 5.0
vault_enabled = true
vault_argon2_mem_kib = 65536
vault_argon2_iters = 3
vault_argon2_parallelism = 1
'''


class Finding(RuntimeError):
    """A VerFSNext integrity or availability failure. Stops the run."""

    def __init__(self, kind, **details):
        details = plain(details)
        super().__init__(f'{kind}: {json.dumps(details, sort_keys=True, default=str)[:2000]}')
        self.kind = kind
        self.details = details


def plain(value):
    """JSON-safe copy: dicts keyed by name tuples (canonical models) become sorted pair lists."""
    if isinstance(value, dict):
        if all(isinstance(k, str) for k in value):
            return {k: plain(v) for k, v in value.items()}
        return sorted([list(k) if isinstance(k, tuple) else k, plain(v)] for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


class HarnessError(RuntimeError):
    """The harness itself could not do what it needed (device, ext4, replay). Stops the run."""


# ---------------------------------------------------------------- utilities

def now():
    return time.strftime('%Y-%m-%dT%H:%M:%S%z')


def run(*args, timeout=600, check=True, input=None):
    result = subprocess.run([str(a) for a in args], text=True, capture_output=True, timeout=timeout, input=input)
    if check and result.returncode:
        raise HarnessError(f'{list(args)}: exit {result.returncode}: {result.stderr.strip()}')
    return result


def save(path, value):
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, sort_keys=True, indent=1, default=str)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def fsync_path(path, directory=False):
    fd = os.open(path, (os.O_RDONLY | os.O_DIRECTORY) if directory else os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_all(path):
    with open(path, 'rb', buffering=0) as f:
        return f.read()


def mounted(path):
    """Whether `path` is a mount point. Unlike os.path.ismount this sees a dead FUSE
    mount, whose stat fails with ENOTCONN."""
    target = str(path)
    with open('/proc/self/mountinfo') as f:
        return any(line.split()[4] == target for line in f)


def teardown():
    """Detach whatever an interrupted run left behind, innermost first."""
    for path in (MNT, CHK, REC):
        if mounted(path):
            run('umount', '-l', path)
    for name in ('pc-ovl2', 'pc-log2', 'pc-ovl', 'pc-log'):
        dm_remove(name)


def device_sectors(dev):
    return int(run('blockdev', '--getsz', dev).stdout)


def devices():
    return json.loads(DEVICES.read_text())


class JsonLog:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()

    def write(self, **record):
        record.setdefault('t', time.time())
        line = json.dumps(record, sort_keys=True, default=str) + '\n'
        with self.lock, self.path.open('a') as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())


# ------------------------------------------------------------ device mapper

def dm_create(name, table):
    run('dmsetup', 'create', name, '--table', table)
    run('udevadm', 'settle')
    return f'/dev/mapper/{name}'


def dm_remove(name):
    if Path(f'/dev/mapper/{name}').exists():
        run('dmsetup', 'remove', name)


def dm_mark(name, text):
    run('dmsetup', 'message', name, '0', 'mark', text)


def dm_status(name):
    return run('dmsetup', 'status', name).stdout.split()


def log_writes_usage(name, log_dev):
    """Fraction of the log device dm-log-writes has used."""
    status = dm_status(name)
    return int(status[-1]) / device_sectors(log_dev)


def snapshot_table(base, cow):
    return f'0 {device_sectors(base)} snapshot {base} {cow} N 8'


# ------------------------------------------------------------- write log

class WriteLog:
    """Reader for the dm-log-writes on-disk format (drivers/md/dm-log-writes.c)."""
    MAGIC = 0x6a736677736872
    FLUSH, FUA, DISCARD, MARK = 1, 2, 4, 8

    def __init__(self, dev):
        self.fd = os.open(dev, os.O_RDONLY)
        head = os.pread(self.fd, 4096, 0)
        magic, version, nr_entries, sectorsize = struct.unpack_from('<QQQI', head)
        if magic != self.MAGIC or version != 1:
            raise HarnessError(f'{dev} is not a dm-log-writes log (magic {magic:#x}, version {version})')
        self.ss = sectorsize
        self.count = nr_entries
        self.sector = array.array('Q')
        self.nr = array.array('Q')
        self.flags = array.array('Q')
        self.pos = array.array('Q')
        self.marks = {}
        self.flushes = array.array('Q')
        pos = sectorsize
        for index in range(nr_entries):
            header = os.pread(self.fd, sectorsize, pos)
            sector, nr, flags, data_len = struct.unpack_from('<QQQQ', header)
            self.sector.append(sector)
            self.nr.append(nr)
            self.flags.append(flags)
            self.pos.append(pos)
            if flags & self.MARK:
                self.marks[header[32:32 + data_len].decode()] = index
            if flags & self.FLUSH:
                self.flushes.append(index)
            pos += sectorsize
            if not flags & (self.DISCARD | self.MARK):
                pos += nr * sectorsize
        if 'dm-log-writes-end' not in self.marks:
            raise HarnessError(f'{dev}: log has no end mark; the log device was not removed cleanly')

    def close(self):
        os.close(self.fd)

    def is_write(self, index):
        return not self.flags[index] & self.MARK and self.nr[index] > 0

    def last_flush(self, point):
        """Index of the last flush at or before `point`, or -1."""
        at = bisect.bisect_right(self.flushes, point) - 1
        return self.flushes[at] if at >= 0 else -1

    def marks_until(self, point):
        return {name for name, index in self.marks.items() if index <= point}

    def apply(self, index, fd, pieces=None):
        """Write entry `index` to `fd`. `pieces` limits it to those 4 KiB pieces of the write."""
        length = self.nr[index] * self.ss
        offset = self.sector[index] * self.ss
        if self.flags[index] & self.DISCARD:
            data = bytes(length)
        else:
            data = os.pread(self.fd, length, self.pos[index] + self.ss)
            if len(data) != length:
                raise HarnessError(f'short read of log entry {index}')
        if pieces is None:
            os.pwrite(fd, data, offset)
            return
        for piece in pieces:
            os.pwrite(fd, data[piece * PIECE:(piece + 1) * PIECE], offset + piece * PIECE)

    def plan_tail(self, point, mode, rng):
        """Entries after the last flush up to `point` that this power cut keeps, with kept pieces."""
        start = self.last_flush(point) + 1
        keep_rate = rng.random()
        plan = []
        for index in range(start, point + 1):
            if not self.is_write(index):
                continue
            pieces = (self.nr[index] * self.ss + PIECE - 1) // PIECE
            if self.flags[index] & self.FUA or mode == 'prefix':
                plan.append((index, None))
            elif mode == 'drop':
                if rng.random() < keep_rate:
                    plan.append((index, None))
            else:
                kept = [p for p in range(pieces) if rng.random() < keep_rate]
                if kept:
                    plan.append((index, kept))
        return plan


class Base:
    """The replay base device, advanced monotonically through the log (all writes up to a flush)."""

    def __init__(self, log, dev):
        self.log = log
        self.dev = dev
        self.fd = os.open(dev, os.O_RDWR)
        self.applied = -1

    def advance(self, until):
        for index in range(self.applied + 1, until + 1):
            if self.log.is_write(index):
                self.log.apply(index, self.fd)
        self.applied = max(self.applied, until)
        os.fsync(self.fd)

    def close(self):
        os.close(self.fd)


def build_overlay(name, base_dev, cow_dev, steps):
    """Snapshot `base_dev` and apply `steps` [(WriteLog, [(index, pieces)])] to the snapshot."""
    path = dm_create(name, snapshot_table(base_dev, cow_dev))
    fd = os.open(path, os.O_RDWR)
    try:
        for log, plan in steps:
            for index, pieces in plan:
                log.apply(index, fd, pieces)
        os.fsync(fd)
    finally:
        os.close(fd)
    return path


# ------------------------------------------------------------------ content

POOL = [random.Random(f'pool-{i}').randbytes(131072) for i in range(16)]


def make(label, size):
    """Mixed content: unique, shared (dedup) and compressible 128 KiB segments."""
    rng = random.Random(label)
    parts = []
    for _ in range((size + 131071) // 131072):
        kind = rng.random()
        if kind < 0.4:
            parts.append(rng.randbytes(131072))
        elif kind < 0.8:
            parts.append(POOL[rng.randrange(len(POOL))])
        else:
            parts.append((f'powercut {label} '.encode() * 8192)[:131072])
    return b''.join(parts)[:size]


class Refs:
    """Content store on the VM root disk, outside the tested stack.

    A version is addressed by the SHA-256 of its bytes and stored as a list of
    64 KiB block hashes, so the many versions of a rewritten file share blocks.
    """
    BLOCK = 65536

    def __init__(self, root):
        self.root = root
        (root / 'blocks').mkdir(parents=True, exist_ok=True)
        (root / 'versions').mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _store(path, data):
        if not path.exists():
            tmp = path.with_name(path.name + f'.{threading.get_ident()}.tmp')
            tmp.write_bytes(data)
            os.replace(tmp, path)

    def put(self, data):
        digest = sha(data)
        version = self.root / 'versions' / digest
        if not version.exists():
            blocks = []
            for offset in range(0, len(data), self.BLOCK):
                block = data[offset:offset + self.BLOCK]
                block_sha = sha(block)
                self._store(self.root / 'blocks' / block_sha, block)
                blocks.append(block_sha)
            self._store(version, json.dumps(dict(size=len(data), blocks=blocks)).encode())
        return digest

    def get(self, digest):
        version = json.loads((self.root / 'versions' / digest).read_text())
        data = b''.join((self.root / 'blocks' / block).read_bytes() for block in version['blocks'])
        if len(data) != version['size'] or sha(data) != digest:
            raise HarnessError(f'reference store damaged for {digest}')
        return data


# ------------------------------------------------------------------- models

def file_state(digest, size, mode, xattr):
    return dict(kind='f', sha=digest, size=size, mode=mode, xattr=xattr)


def apply_op(worker, model, op):
    """The model after `op` succeeds. Pure: returns a new model."""
    model = json.loads(json.dumps(model))
    if worker == 'snap':
        snaps = set(model['snaps'])
        if op['op'] == 'snap_create':
            snaps.add(op['name'])
        else:
            snaps.discard(op['name'])
        model['snaps'] = sorted(snaps)
        return model
    names, inodes = model['names'], model['inodes']
    kind = op['op']
    if kind == 'create':
        names[op['path']] = op['ino']
        inodes[op['ino']] = file_state(op['new'], op['size'], op['mode'], op['xattr'])
    elif kind == 'symlink':
        names[op['path']] = op['ino']
        inodes[op['ino']] = dict(kind='l', target=op['target'])
    elif kind in ('write', 'truncate'):
        inodes[op['ino']].update(sha=op['new'], size=op['size'])
    elif kind == 'setattr':
        inodes[op['ino']].update(mode=op['mode'], xattr=op['xattr'])
    elif kind == 'rename':
        names[op['dst']] = names.pop(op['src'])
    elif kind == 'exchange':
        names[op['a']], names[op['b']] = names[op['b']], names[op['a']]
    elif kind == 'link':
        names[op['dst']] = names[op['src']]
    elif kind == 'unlink':
        del names[op['path']]
    else:
        raise HarnessError(f'unknown op {kind}')
    live = set(names.values())
    model['inodes'] = {ino: state for ino, state in inodes.items() if ino in live}
    return model


def canonical(model):
    groups = {}
    for name, ino in model['names'].items():
        groups.setdefault(ino, []).append(name)
    return {tuple(sorted(group)): json.dumps(model['inodes'][ino], sort_keys=True)
            for ino, group in groups.items()}


def fold(initial, journal, marked):
    """Expected state at a crash point: exact models plus at most one pending op per worker.

    `initial` is {worker: {'model', 'pend'}}; `marked(name)` says whether a log mark is
    inside the replayed prefix. Acknowledgements are marks issued after the op's last
    fsync returned; a settle is a mark issued after a recovered state was made durable.
    """
    state = {w: dict(model=v['model'], pend=v['pend']) for w, v in initial.items()}
    stale = set()
    for rec in journal:
        if rec['k'] == 'settle':
            if not marked(f"s.{rec['id']}"):
                break
            for w, m in rec['models'].items():
                state[w] = dict(model=m, pend=None)
            stale.clear()
        elif rec['k'] == 'op':
            w = rec['w']
            if w in stale:
                continue
            if state[w]['pend'] is not None:
                # The pending op was acknowledged after this point, so nothing later counts.
                stale.add(w)
                continue
            if marked(f"a.{w}.{rec['seq']}"):
                state[w]['model'] = apply_op(w, state[w]['model'], rec['op'])
            else:
                state[w]['pend'] = rec['op']
    return state


def read_journal(path):
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


# ------------------------------------------------------------ verification

def mixed(actual, old, new):
    """Every byte comes from the old or the new version; bytes past either end count as zero."""
    size = max(len(actual), len(old), len(new))
    old, new = old.ljust(size, b'\0'), new.ljust(size, b'\0')
    for offset in range(0, len(actual), 65536):
        block = actual[offset:offset + 65536]
        before, after = old[offset:offset + len(block)], new[offset:offset + len(block)]
        if block != before and block != after and any(a != b and a != c for a, b, c in zip(block, before, after)):
            return False
    return True


def get_xattr(path):
    try:
        return os.getxattr(path, XATTR, follow_symlinks=False).hex()
    except OSError as error:
        if error.errno == errno.ENODATA:
            return None
        raise


def read_checked(path, label):
    try:
        return read_all(path)
    except OSError as error:
        raise Finding('read_error', label=label, path=str(path), errno=errno.errorcode.get(error.errno),
                      message=str(error))


def observe_worker(root, refs):
    """Actual worker namespace as a model; contents go to the reference store."""
    names, inodes = {}, {}
    for sub in ('a', 'b'):
        with os.scandir(root / sub) as scan:
            entries = sorted(scan, key=lambda e: e.name)
        for entry in entries:
            path = Path(entry.path)
            info = os.lstat(path)
            ino = f'x{info.st_ino}'
            names[f'{sub}/{entry.name}'] = ino
            if stat.S_ISLNK(info.st_mode):
                inodes[ino] = dict(kind='l', target=os.readlink(path))
            elif stat.S_ISREG(info.st_mode):
                if ino not in inodes:
                    data = read_checked(path, 'worker')
                    if len(data) != info.st_size:
                        raise Finding('size_mismatch', path=str(path), read=len(data), st_size=info.st_size)
                    inodes[ino] = file_state(refs.put(data), info.st_size, stat.S_IMODE(info.st_mode),
                                             get_xattr(path))
            else:
                raise Finding('unexpected_entry', path=str(path), mode=oct(info.st_mode))
    links = {}
    for ino in names.values():
        links[ino] = links.get(ino, 0) + 1
    for name, ino in names.items():
        if inodes[ino]['kind'] == 'f':
            nlink = os.lstat(root / name).st_nlink
            if nlink != links[ino]:
                raise Finding('nlink_mismatch', path=str(root / name), st_nlink=nlink, names=links[ino])
    return dict(names=names, inodes=inodes)


def check_pending_data(op, actual_state, old_state, refs):
    """Whether one inode's observed state is a valid outcome of an interrupted data op."""
    kind = op['op']
    if kind == 'create':
        new = refs.get(op['new'])
        return (actual_state['kind'] == 'f' and actual_state['size'] <= len(new)
                and actual_state['mode'] in (0o600, op['mode']) and actual_state['xattr'] in (None, op['xattr'])
                and mixed(refs.get(actual_state['sha']), b'', new))
    if actual_state['kind'] != 'f':
        return False
    old = refs.get(old_state['sha'])
    if kind == 'setattr':
        return (actual_state['sha'] == old_state['sha'] and actual_state['mode'] in (old_state['mode'], op['mode'])
                and actual_state['xattr'] in (old_state['xattr'], op['xattr']))
    if actual_state['mode'] != old_state['mode'] or actual_state['xattr'] != old_state['xattr']:
        return False
    new = refs.get(op['new'])
    data = refs.get(actual_state['sha'])
    if kind == 'write':
        return len(old) <= len(data) <= max(len(old), len(new)) and mixed(data, old, new)
    if kind == 'truncate':
        return len(data) in (len(old), len(new)) and data == old.ljust(len(new), b'\0')[:len(data)]
    raise HarnessError(f'not a data op: {kind}')


def verify_worker(worker, expected, pend, actual, refs):
    """Raise Finding unless `actual` equals `expected`, or is a valid outcome of `pend`."""
    if pend is None:
        if canonical(actual) != canonical(expected):
            raise Finding('acked_mismatch', worker=worker, expected=canonical(expected), actual=canonical(actual))
        return
    if pend['op'] in ('rename', 'exchange', 'link', 'unlink', 'symlink'):
        after = apply_op(worker, expected, pend)
        if canonical(actual) not in (canonical(expected), canonical(after)):
            raise Finding('pending_namespace_mismatch', worker=worker, pending=pend, before=canonical(expected),
                          after=canonical(after), actual=canonical(actual))
        return
    if pend['op'] == 'create':
        involved = {pend['path']}
        old_state = None
    else:
        involved = {n for n, i in expected['names'].items() if i == pend['ino']}
        old_state = expected['inodes'][pend['ino']]
    rest_expected = {k: v for k, v in canonical(expected).items() if not involved & set(k)}
    actual_groups = canonical(actual)
    rest_actual = {k: v for k, v in actual_groups.items() if not involved & set(k)}
    if rest_actual != rest_expected:
        raise Finding('acked_mismatch_beside_pending', worker=worker, pending=pend, expected=rest_expected,
                      actual=rest_actual)
    touched = [k for k in actual_groups if involved & set(k)]
    if not touched:
        if pend['op'] == 'create':
            return
        raise Finding('pending_lost_file', worker=worker, pending=pend, names=sorted(involved))
    if len(touched) != 1 or set(touched[0]) != involved:
        raise Finding('pending_link_mismatch', worker=worker, pending=pend, involved=sorted(involved),
                      actual=touched)
    state = json.loads(actual_groups[touched[0]])
    if not check_pending_data(pend, state, old_state, refs):
        raise Finding('pending_content_mismatch', worker=worker, pending=pend, old=old_state, actual=state)


def stable_spec():
    """The static corpus written once at setup: path -> (kind, payload, mode, xattr)."""
    spec = {}
    for i in range(36):
        size = [0, 73, 4095, 32769, 262145, 1048576, 4 * 1048576 + 7][i % 7]
        spec[f'd{i % 4}/file-{i:03d}'] = ('f', make(f'stable-{i}', size), 0o640 if i % 2 else 0o600,
                                          f'stable-{i}'.encode().hex())
    for i in range(6):
        spec[f'dedup/copy-{i}'] = ('f', make(f'dup-{i % 2}', 2 * 1048576), 0o644, None)
    spec['compressible'] = ('f', b'VerFSNext\x00' * 104857, 0o644, None)
    spec['link-a'] = ('f', make('hardlinked', 300001), 0o644, 'hard'.encode().hex())
    spec['symlink'] = ('l', 'd1/file-001', None, None)
    return spec


def stable_manifest():
    out = {}
    for path, (kind, payload, mode, xattr) in stable_spec().items():
        out[path] = dict(kind='l', target=payload) if kind == 'l' else file_state(sha(payload), len(payload),
                                                                                  mode, xattr)
    out['link-b'] = out['link-a']
    return out


def observe_tree(root):
    out, inos = {}, {}
    def walk(folder, prefix):
        with os.scandir(folder) as scan:
            entries = sorted(scan, key=lambda e: e.name)
        for entry in entries:
            rel = f'{prefix}{entry.name}'
            info = os.lstat(entry.path)
            if stat.S_ISDIR(info.st_mode):
                walk(entry.path, rel + '/')
            elif stat.S_ISLNK(info.st_mode):
                out[rel] = dict(kind='l', target=os.readlink(entry.path))
            else:
                data = read_checked(entry.path, 'stable')
                out[rel] = file_state(sha(data), len(data), stat.S_IMODE(info.st_mode), get_xattr(entry.path))
                inos.setdefault(info.st_ino, []).append(rel)
    walk(root, '')
    return out, sorted(sorted(v) for v in inos.values() if len(v) > 1)


def verify_stable(root, label):
    actual, links = observe_tree(root)
    expected = stable_manifest()
    if actual != expected:
        diff = {k: dict(expected=expected.get(k), actual=actual.get(k))
                for k in expected.keys() | actual.keys() if expected.get(k) != actual.get(k)}
        raise Finding('stable_mismatch', label=label, diff=diff)
    if links != [['link-a', 'link-b']]:
        raise Finding('stable_hardlink_mismatch', label=label, links=links)
    return len(actual)


# ------------------------------------------------------------------- daemon

class Daemon:
    def __init__(self, fs_root, log_path):
        self.data = fs_root / 'data'
        self.config = STATE / f'config-{fs_root.name}.toml'
        self.config.write_text(CONFIG_TEMPLATE.format(mount=MNT, data=self.data))
        self.log_path = log_path
        self.proc = None
        self.key = STATE / 'verfsnext.vault.key'

    def start(self):
        if mounted(MNT):
            raise HarnessError(f'{MNT} is still mounted')
        log = self.log_path.open('a')
        log.write(f'=== start {now()}\n')
        log.flush()
        env = dict(os.environ, RUST_LOG='info', RUST_BACKTRACE='full')
        self.proc = subprocess.Popen([BIN, '--config', str(self.config), 'mount'], stdout=log, stderr=log, env=env,
                                     start_new_session=True)
        log.close()
        deadline = time.monotonic() + 120
        while not mounted(MNT):
            if self.proc.poll() is not None:
                raise Finding('daemon_start_failed', returncode=self.proc.returncode, log=self.tail())
            if time.monotonic() > deadline:
                raise Finding('daemon_start_timeout', log=self.tail())
            time.sleep(0.02)
        # The mount is visible before the control socket answers; poll until it does.
        while self.control('stats', check=False).returncode:
            if time.monotonic() > deadline:
                raise Finding('control_socket_timeout', log=self.tail())
            time.sleep(0.05)

    def control(self, *args, check=True):
        result = run(BIN, '--config', self.config, *args, timeout=300, check=False)
        if check and result.returncode:
            raise Finding('control_failed', args=args, returncode=result.returncode, stderr=result.stderr,
                          log=self.tail())
        return result

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=60)
        self.detach()

    def detach(self):
        if mounted(MNT):
            run('umount', '-l', MNT)

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=180)
        except subprocess.TimeoutExpired:
            raise Finding('daemon_stop_timeout', log=self.tail())
        if code:
            raise Finding('daemon_stop_failed', returncode=code, log=self.tail())
        if mounted(MNT):
            raise Finding('mount_left_after_stop', log=self.tail())

    def unlock(self):
        self.control('crypt', '-u', '-p', PASSWORD, '-k', self.key)

    def check_records(self):
        if not RECORDS_CHECK:
            return
        out = self.control('check').stdout
        if 'Records check: consistent' not in out:
            raise Finding('records_check_failed', report=out, log=self.tail())

    def tail(self, lines=120):
        if not self.log_path.exists():
            return ''
        return '\n'.join(self.log_path.read_text(errors='replace').splitlines()[-lines:])

    def log_problems(self):
        """(recovery-phase errors, failures): panics and errors after mount are failures."""
        recovery, failures = [], []
        mounted = False
        for line in self.log_path.read_text(errors='replace').splitlines():
            if line.startswith('=== start'):
                mounted = False
            elif 'VerFSNext Phase 1 mounted' in line:
                mounted = True
            elif 'panicked' in line:
                failures.append(line)
            elif re.search(r'\bERROR\b', line):
                (failures if mounted else recovery).append(line)
        return recovery, failures


# ------------------------------------------------------------------ workers

class Timeline:
    """Journal of ops/settles for one recorded log, with marks into that log."""

    def __init__(self, path, dm_name):
        self.journal = JsonLog(path)
        self.dm_name = dm_name
        self.marks = set()
        self.lock = threading.Lock()

    def mark(self, name):
        with self.lock:
            if self.dm_name:
                dm_mark(self.dm_name, name)
            self.marks.add(name)


class Phase:
    """What the scheduler is doing, so workers can classify errors."""

    def __init__(self):
        self.window = None
        self.fault_at = None
        self.mmap_ok = True
        self.go = threading.Event()
        self.stop = False


class Worker(threading.Thread):
    def __init__(self, name, root, model, timeline, phase, refs, rng, seq):
        super().__init__(daemon=True)
        self.name_ = name
        self.root = root
        self.model = model
        self.timeline = timeline
        self.phase = phase
        self.refs = refs
        self.rng = rng
        self.seq = seq
        self.busy = False
        self.blocked = None
        self.error = None
        self.ops = 0

    def run(self):
        try:
            while not self.phase.stop:
                if not self.phase.go.wait(0.2) or self.blocked is not None:
                    continue
                self.busy = True
                try:
                    if self.phase.go.is_set() and self.blocked is None:
                        self.step()
                finally:
                    self.busy = False
        except BaseException as error:
            self.error = error

    def plan(self):
        raise NotImplementedError

    def step(self):
        op = self.plan()
        if op is None:
            return
        self.seq += 1
        self.timeline.journal.write(k='op', w=self.name_, seq=self.seq, op=op)
        try:
            self.execute(op)
        except OSError as error:
            self.classify(op, error)
            return
        except subprocess.SubprocessError as error:
            self.classify(op, error)
            return
        self.timeline.mark(f'a.{self.name_}.{self.seq}')
        self.timeline.journal.write(k='ack', w=self.name_, seq=self.seq)
        self.model = apply_op(self.name_, self.model, op)
        self.ops += 1

    def classify(self, op, error):
        window = self.phase.window
        code = getattr(error, 'errno', None)
        # Once the connection aborts, the kernel fails pending writeback of dirty pages and
        # fsync reports EIO. Accepted only after the signal was sent, never from a live daemon.
        after_fault = self.phase.fault_at is not None and time.monotonic() >= self.phase.fault_at
        outage = code in OUTAGE_ERRNOS or code is None or (code == errno.EIO and after_fault)
        expected = (window in ('kill', 'term') and outage) or \
                   (window == 'enospc' and code in (errno.ENOSPC, errno.EIO, None))
        self.timeline.journal.write(k='fail', w=self.name_, seq=self.seq, window=window,
                                    errno=errno.errorcode.get(code), message=str(error))
        if not expected:
            raise Finding('unexpected_op_error', worker=self.name_, op=op, window=window,
                          errno=errno.errorcode.get(code), message=str(error))
        self.blocked = op


class FileWorker(Worker):
    def plan(self):
        names, inodes = self.model['names'], self.model['inodes']
        files = sorted(n for n in names if inodes[names[n]]['kind'] == 'f')
        total = sum(s['size'] for s in inodes.values() if s['kind'] == 'f')
        rng = self.rng
        seq = self.seq + 1
        label = f'{self.name_}-{seq}-{rng.random()}'
        fresh = f"{rng.choice('ab')}/n{seq:08d}"
        if total > WORKER_MAX_BYTES and files:
            return dict(op='unlink', path=rng.choice(files))
        choices = ['create'] * (4 if len(names) < 6 else 1) + ['symlink'] * (len(names) < 40) + ['bigcreate'] * \
            (rng.random() < 0.04)
        if files:
            choices += ['write'] * 4 + ['append'] * 2 + ['truncate', 'setattr', 'link'] + ['unlink'] * 2
        if names:
            choices += ['rename'] * 2
        if len(names) >= 2:
            choices.append('exchange')
        kind = rng.choice(choices)
        sync = rng.choice(['fsync', 'fdatasync'])
        if kind in ('create', 'bigcreate'):
            size = rng.randrange(6 << 20, 20 << 20) if kind == 'bigcreate' else rng.choice(SIZES)
            data = make(label, size)
            return dict(op='create', path=fresh, ino=f'n{seq}', new=self.refs.put(data), size=size,
                        mode=rng.choice([0o600, 0o640, 0o644]), xattr=label.encode().hex(), sync=sync)
        if kind == 'symlink':
            return dict(op='symlink', path=fresh, ino=f'n{seq}', target=f'target-{label}')
        if kind == 'rename':
            src = rng.choice(sorted(names))
            dst = rng.choice(sorted(names) + [fresh] * 2)
            if names.get(dst) == names[src]:
                return None
            return dict(op='rename', src=src, dst=dst)
        if kind == 'exchange':
            a, b = rng.sample(sorted(names), 2)
            if names[a] == names[b]:
                return None
            return dict(op='exchange', a=a, b=b)
        path = rng.choice(files)
        ino = names[path]
        state = inodes[ino]
        if kind == 'link':
            return dict(op='link', src=path, dst=fresh)
        if kind == 'unlink':
            return dict(op='unlink', path=rng.choice(sorted(names)))
        if kind == 'setattr':
            return dict(op='setattr', path=path, ino=ino, mode=rng.choice([0o600, 0o640, 0o644, 0o664]),
                        xattr=rng.choice([label.encode().hex(), None]))
        old = self.refs.get(state['sha'])
        if kind == 'truncate':
            size = rng.choice([0, rng.randrange(len(old) + 1), len(old) + rng.randrange(1, 300000)])
            data = old[:size].ljust(size, b'\0')
            return dict(op='truncate', path=path, ino=ino, size=size, new=self.refs.put(data), sync=sync)
        if kind == 'append':
            offset = len(old)
            patch = make(label, rng.choice([1, 100, 4096, 9000, 65536]))
        else:
            offset = rng.randrange(len(old) + 1)
            patch = make(label, rng.choice([1, 511, 4096, 12289, 65536, 262144, 1048576]))
        data = old[:offset].ljust(offset, b'\0') + patch + old[offset + len(patch):]
        via = 'mmap' if (self.phase.mmap_ok and kind == 'write' and offset + len(patch) <= len(old)
                         and rng.random() < 0.2) else 'pwrite'
        return dict(op='write', path=path, ino=ino, offset=offset, patch=self.refs.put(patch), size=len(data),
                    new=self.refs.put(data), via=via, sync=sync)

    def sync_fd(self, fd, how):
        if how == 'fdatasync':
            os.fdatasync(fd)
        else:
            os.fsync(fd)

    def execute(self, op):
        root, kind = self.root, op['op']
        if kind == 'create':
            path = root / op['path']
            data = self.refs.get(op['new'])
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                view = memoryview(data)
                offset = 0
                while offset < len(data):
                    offset += os.pwrite(fd, view[offset:offset + 1048576], offset)
                os.fchmod(fd, op['mode'])
                os.setxattr(fd, XATTR, bytes.fromhex(op['xattr']))
                self.sync_fd(fd, 'fsync')
            finally:
                os.close(fd)
            fsync_path(path.parent, directory=True)
        elif kind == 'symlink':
            os.symlink(op['target'], root / op['path'])
            fsync_path((root / op['path']).parent, directory=True)
        elif kind == 'write':
            patch = self.refs.get(op['patch'])
            path = root / op['path']
            if op['via'] == 'mmap':
                fd = os.open(path, os.O_RDWR)
                try:
                    with mmap.mmap(fd, 0) as region:
                        region[op['offset']:op['offset'] + len(patch)] = patch
                        region.flush()
                finally:
                    os.close(fd)
            else:
                fd = os.open(path, os.O_WRONLY)
                try:
                    written = 0
                    while written < len(patch):
                        written += os.pwrite(fd, patch[written:], op['offset'] + written)
                    self.sync_fd(fd, op['sync'])
                finally:
                    os.close(fd)
        elif kind == 'truncate':
            fd = os.open(root / op['path'], os.O_WRONLY)
            try:
                os.ftruncate(fd, op['size'])
                self.sync_fd(fd, 'fsync')
            finally:
                os.close(fd)
        elif kind == 'setattr':
            path = root / op['path']
            os.chmod(path, op['mode'])
            if op['xattr'] is None:
                try:
                    os.removexattr(path, XATTR)
                except OSError as error:
                    if error.errno != errno.ENODATA:
                        raise
            else:
                os.setxattr(path, XATTR, bytes.fromhex(op['xattr']))
            fsync_path(path)
        elif kind == 'rename':
            os.rename(root / op['src'], root / op['dst'])
            self.sync_dirs(op['src'], op['dst'])
        elif kind == 'exchange':
            renameat2(root / op['a'], root / op['b'], RENAME_EXCHANGE)
            self.sync_dirs(op['a'], op['b'])
        elif kind == 'link':
            os.link(root / op['src'], root / op['dst'], follow_symlinks=False)
            self.sync_dirs(op['dst'])
        elif kind == 'unlink':
            os.unlink(root / op['path'])
            self.sync_dirs(op['path'])
        else:
            raise HarnessError(f'unknown op {kind}')

    def sync_dirs(self, *paths):
        for folder in sorted({p.split('/')[0] for p in paths}):
            fsync_path(self.root / folder, directory=True)


class SnapWorker(Worker):
    def __init__(self, daemon, *args):
        super().__init__(*args)
        self.daemon = daemon

    def plan(self):
        time.sleep(self.rng.uniform(0.5, 3.0))
        snaps = [s for s in self.model['snaps'] if s != 'base']
        if len(snaps) >= 3 or (snaps and self.rng.random() < 0.3):
            return dict(op='snap_delete', name=self.rng.choice(snaps))
        return dict(op='snap_create', name=f's{self.seq + 1:06d}')

    def execute(self, op):
        verb = 'create' if op['op'] == 'snap_create' else 'delete'
        result = run(BIN, '--config', self.daemon.config, 'snapshot', verb, op['name'], timeout=300, check=False)
        if result.returncode:
            raise subprocess.SubprocessError(f'snapshot {verb} {op["name"]}: {result.returncode}: {result.stderr}')
        fsync_path(MNT, directory=True)


libc = ctypes.CDLL(None, use_errno=True)
RENAME_EXCHANGE = 2


def renameat2(src, dst, flags):
    AT_FDCWD = -100
    if libc.renameat2(AT_FDCWD, os.fsencode(str(src)), AT_FDCWD, os.fsencode(str(dst)), flags):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(src))


def worker_root(name):
    return MNT / '.vault' / name if name == VAULT_WORKER else MNT / name


# ------------------------------------------------------------------ the run

class Crew:
    """Workers, the scheduler-facing pause/settle protocol and full-state verification."""

    def __init__(self, daemon, timeline, refs, state, rng, seq_base):
        self.daemon = daemon
        self.timeline = timeline
        self.refs = refs
        self.phase = Phase()
        self.workers = {}
        self.settles = 0
        for index, name in enumerate(WORKERS):
            self.workers[name] = FileWorker(name, worker_root(name), state[name]['model'], timeline, self.phase,
                                            refs, random.Random(f'{rng.random()}'), seq_base)
            self.workers[name].blocked = state[name]['pend']
        snap = SnapWorker(daemon, 'snap', MNT, state['snap']['model'], timeline, self.phase, refs,
                          random.Random(f'{rng.random()}'), seq_base)
        snap.blocked = state['snap']['pend']
        self.workers['snap'] = snap
        for worker in self.workers.values():
            worker.start()

    def check_errors(self):
        for worker in self.workers.values():
            if worker.error is not None:
                raise worker.error

    def pause(self):
        self.phase.go.clear()
        deadline = time.monotonic() + 600
        while any(w.busy for w in self.workers.values()):
            self.check_errors()
            if time.monotonic() > deadline:
                raise Finding('worker_hung', busy=[n for n, w in self.workers.items() if w.busy],
                              log=self.daemon.tail())
            time.sleep(0.05)
        self.check_errors()

    def resume(self, window=None, mmap_ok=True):
        self.phase.window = window
        self.phase.fault_at = None
        self.phase.mmap_ok = mmap_ok
        self.phase.go.set()

    def finish(self):
        self.pause()
        self.phase.stop = True
        for worker in self.workers.values():
            worker.join(timeout=30)

    def state(self):
        return {n: dict(model=w.model, pend=w.blocked) for n, w in self.workers.items()}

    def settle(self, label):
        """Verify the live state, adopt valid outcomes of interrupted ops, make them durable, mark."""
        self.pause()
        observed = verify_all(self.state(), self.refs)
        for name, model in observed.items():
            if name != 'snap':
                durable_worker(worker_root(name), model)
        fsync_path(MNT, directory=True)
        self.settles += 1
        settle_id = f'{label}-{self.settles}'
        self.timeline.mark(f's.{settle_id}')
        self.timeline.journal.write(k='settle', id=settle_id, models=observed)
        for name, worker in self.workers.items():
            worker.model = observed[name]
            worker.blocked = None
        return observed


def durable_worker(root, model):
    for name in model['names']:
        path = root / name
        if not os.path.islink(path):
            fsync_path(path)
    for sub in ('a', 'b'):
        fsync_path(root / sub, directory=True)
    fsync_path(root, directory=True)


def verify_all(state, refs):
    """Full oracle against the mounted daemon. Returns the observed models."""
    verify_stable(MNT / 'stable', 'live')
    verify_stable(MNT / '.vault' / 'stable', 'vault')
    observed = {}
    for name in WORKERS:
        actual = observe_worker(worker_root(name), refs)
        verify_worker(name, state[name]['model'], state[name]['pend'], actual, refs)
        observed[name] = actual
    snaps = sorted(os.listdir(MNT / '.snapshots'))
    expected = state['snap']['model']
    pend = state['snap']['pend']
    valid = [expected['snaps']] + ([apply_op('snap', expected, pend)['snaps']] if pend else [])
    if snaps not in valid:
        raise Finding('snapshot_set_mismatch', actual=snaps, valid=valid, pending=pend)
    for snap in snaps:
        verify_stable(MNT / '.snapshots' / snap / 'stable', f'snapshot:{snap}')
    observed['snap'] = dict(snaps=snaps)
    return observed


def write_stable(root):
    for path, (kind, payload, mode, xattr) in stable_spec().items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if kind == 'l':
            os.symlink(payload, target)
            continue
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            offset = 0
            while offset < len(payload):
                offset += os.pwrite(fd, payload[offset:offset + 1048576], offset)
            os.fchmod(fd, mode)
            if xattr is not None:
                os.setxattr(fd, XATTR, bytes.fromhex(xattr))
            os.fsync(fd)
        finally:
            os.close(fd)
    os.link(root / 'link-a', root / 'link-b')
    for folder in sorted({p.parent for p in root.rglob('*')} | {root}):
        fsync_path(folder, directory=True)


class Run:
    def __init__(self, run_dir, seed):
        self.dir = run_dir
        self.seed = seed
        self.rng = random.Random(seed)
        self.dev = devices()
        self.refs = Refs(run_dir / 'refs')
        self.events = JsonLog(run_dir / 'events.jsonl')
        self.status_path = STATE / 'status.json'

    def status(self, **fields):
        current = json.loads(self.status_path.read_text()) if self.status_path.exists() else {}
        current.update(fields, updated=now(), run=self.dir.name)
        save(self.status_path, current)

    # ---- record

    def record(self, seconds, log_budget, faults):
        dev = self.dev
        self.status(phase='record', run_seed=self.seed)
        teardown()
        run('blkdiscard', '-z', dev['origin'], timeout=1800)
        stale_key = STATE / 'verfsnext.vault.key'
        if stale_key.exists():
            stale_key.unlink()
        top = dm_create('pc-log', f"0 {device_sectors(dev['origin'])} log-writes {dev['origin']} {dev['log']}")
        run('mkfs.ext4', '-F', '-q', '-E', 'nodiscard,lazy_itable_init=0,lazy_journal_init=0', top)
        REC.mkdir(parents=True, exist_ok=True)
        run('mount', '-o', 'noatime,nodiscard', top, REC)
        timeline = Timeline(self.dir / 'journal.jsonl', 'pc-log')
        daemon = Daemon(REC, self.dir / 'record-daemon.log')
        daemon.start()
        key = Path(daemon.control('crypt', '-c', '-p', PASSWORD, '-path', STATE).stdout.strip().splitlines()[-1])
        if key != daemon.key:
            raise HarnessError(f'vault key written to {key}, expected {daemon.key}')
        daemon.unlock()
        for name in ('stable', '.vault/stable'):
            write_stable(MNT / name)
        for name in WORKERS:
            for sub in ('a', 'b'):
                (worker_root(name) / sub).mkdir(parents=True)
            durable_worker(worker_root(name), dict(names={}))
        fsync_path(MNT / '.vault', directory=True)
        fsync_path(MNT, directory=True)
        daemon.control('snapshot', 'create', 'base')
        fsync_path(MNT, directory=True)
        empty = dict(names={}, inodes={})
        initial = {name: dict(model=empty, pend=None) for name in WORKERS}
        initial['snap'] = dict(model=dict(snaps=['base']), pend=None)
        verify_all(initial, self.refs)
        save(self.dir / 'initial.json', initial)
        timeline.mark('setup')
        crew = Crew(daemon, timeline, self.refs, initial, self.rng, 0)
        deadline = time.monotonic() + seconds
        counts = {}
        try:
            while time.monotonic() < deadline and log_writes_usage('pc-log', dev['log']) < log_budget:
                event = self.rng.choices(faults, [FAULT_WEIGHTS[f] for f in faults])[0]
                counts[event] = counts.get(event, 0) + 1
                self.status(phase='record', event=event, events=counts,
                            log_usage=round(log_writes_usage('pc-log', dev['log']), 3))
                self.burst(crew, daemon, timeline, event, REC)
                crew.check_errors()
            crew.finish()
            observed = verify_all(crew.state(), self.refs)
            daemon.stop()
        except BaseException:
            self.events.write(kind='record_failed', error=traceback.format_exc())
            raise
        recovery, errors = daemon.log_problems()
        # ENOSPC windows legitimately log write errors, so the record phase only fails on
        # panics; every ERROR line is kept for review. Replay checks are strict.
        panics = [line for line in errors if 'panicked' in line]
        self.events.write(kind='record_done', events=counts, recovery_errors=recovery, errors=errors[:200],
                          ops={n: w.ops for n, w in crew.workers.items()})
        if panics:
            raise Finding('daemon_panic', phase='record', lines=panics[:50])
        timeline.mark('end')
        run('umount', REC)
        dm_remove('pc-log')
        save(self.dir / 'final.json', observed)
        return counts

    def burst(self, crew, daemon, timeline, event, fs_root):
        length = self.rng.uniform(4, 25)
        risky = event in ('kill', 'doublekill', 'term', 'enospc')
        crew.resume(window=None, mmap_ok=not risky)
        if event == 'kill' or event == 'doublekill' or event == 'term':
            time.sleep(self.rng.uniform(0.2, length))
            crew.phase.window = 'term' if event == 'term' else 'kill'
            timeline.mark(f'k.{event}.{time.time_ns()}')
            crew.phase.fault_at = time.monotonic()
            if event == 'term':
                daemon.stop()
            else:
                daemon.kill()
            crew.pause()
            daemon.start()
            if event == 'doublekill':
                time.sleep(self.rng.uniform(0.0, 0.8))
                daemon.kill()
                daemon.start()
            daemon.unlock()
            crew.settle(event)
            return
        if event == 'enospc':
            time.sleep(self.rng.uniform(0.2, length / 2))
            crew.phase.window = 'enospc'
            ballast = fs_root / 'ballast'
            free = os.statvfs(fs_root)
            leave = self.rng.randrange(0, 48 << 20)
            size = free.f_bavail * free.f_frsize - leave
            timeline.mark(f'e.{time.time_ns()}')
            if size > 0:
                result = run('fallocate', '-l', size, ballast, check=False)
                self.events.write(kind='enospc_ballast', bytes=size, returncode=result.returncode,
                                  stderr=result.stderr)
            time.sleep(self.rng.uniform(3, 12))
            crew.pause()
            if ballast.exists():
                ballast.unlink()
            crew.settle('enospc')
            return
        time.sleep(length)
        crew.pause()
        if event == 'idle':
            timeline.mark(f'i.{time.time_ns()}')
            time.sleep(self.rng.uniform(2.5, 9))

    # ---- replay

    def replay(self, points, nested_every):
        dev = self.dev
        log = WriteLog(dev['log'])
        journal = read_journal(self.dir / 'journal.jsonl')
        initial = json.loads((self.dir / 'initial.json').read_text())
        start = log.marks['setup']
        end = log.marks['end']
        windows = [i for m, i in log.marks.items() if m[:2] in ('k.', 'e.', 'i.', 's.')]
        chosen = set()
        while len(chosen) < points:
            r = self.rng.random()
            if r < 0.4 or not windows:
                chosen.add(self.rng.randrange(start, end + 1))
            elif r < 0.75:
                chosen.add(min(end, self.rng.choice(windows) + self.rng.randrange(0, 400)))
            else:
                flush = log.flushes[self.rng.randrange(bisect.bisect_left(log.flushes, start), len(log.flushes))]
                chosen.add(min(end, max(start, flush - 1)))
        run('blkdiscard', '-z', dev['base'], timeout=1800)
        base = Base(log, dev['base'])
        stats = dict(points=0, nested=0, nested_points=0, modes={}, recovery_errors=0)
        self.status(phase='replay', points_planned=points, log_entries=log.count, replay=stats)
        for number, point in enumerate(sorted(chosen)):
            mode = self.rng.choice(['prefix', 'drop', 'drop', 'torn', 'torn'])
            point_seed = f'{self.seed}:{point}:{mode}'
            nested = nested_every and number % nested_every == nested_every - 1
            self.check_point(log, base, journal, initial, point, mode, point_seed, nested, stats)
            stats['points'] += 1
            stats['modes'][mode] = stats['modes'].get(mode, 0) + 1
            self.status(phase='replay', replay=stats, last_point=point)
        base.advance(log.count - 1)
        base.close()
        log.close()
        origin, rebuilt = digest_device(dev['origin']), digest_device(dev['base'])
        if origin != rebuilt:
            raise HarnessError(f'full replay does not reproduce the recorded device: {origin} != {rebuilt}')
        self.events.write(kind='replay_done', stats=stats, device_sha256=origin)
        return stats

    def check_point(self, log, base, journal, initial, point, mode, point_seed, nested, stats):
        dev = self.dev
        rng = random.Random(point_seed)
        flush = log.last_flush(point)
        base.advance(flush)
        tail = log.plan_tail(point, mode, rng)
        marks = log.marks_until(point)
        expected = fold(initial, journal, marks.__contains__)
        context = dict(point=point, mode=mode, seed=point_seed, last_flush=flush, tail_entries=len(tail),
                       nested=bool(nested))
        point_dir = self.dir / 'points' / f'{point:010d}'
        point_dir.mkdir(parents=True, exist_ok=True)
        overlay = build_overlay('pc-ovl', dev['base'], dev['cow'], [(log, tail)])
        fs_dev = dm_create('pc-log2', f"0 {device_sectors(overlay)} log-writes {overlay} {dev['log2']}") \
            if nested else overlay
        try:
            self.live_check(fs_dev, point_dir, expected, rng, context, 'pc-log2' if nested else None)
        except Finding as finding:
            raise Finding(finding.kind, context=context, **finding.details)
        finally:
            dm_remove('pc-log2')
        if nested:
            stats['nested'] += 1
            log2 = WriteLog(dev['log2'])
            journal2 = read_journal(point_dir / 'journal.jsonl')
            for sub in range(4):
                p2 = rng.randrange(0, log2.count)
                mode2 = rng.choice(['prefix', 'drop', 'torn'])
                tail2 = log2.plan_tail(p2, mode2, rng)
                head2 = [(i, None) for i in range(0, log2.last_flush(p2) + 1) if log2.is_write(i)]
                expected2 = fold(expected, journal2, log2.marks_until(p2).__contains__)
                ctx2 = dict(context, level2_point=p2, level2_mode=mode2, level2_tail=len(tail2))
                overlay2 = build_overlay('pc-ovl2', dev['base'], dev['cow2'], [(log, tail), (log2, head2 + tail2)])
                try:
                    self.cold_check(overlay2, point_dir, expected2, ctx2)
                except Finding as finding:
                    raise Finding(finding.kind, context=ctx2, **finding.details)
                finally:
                    dm_remove('pc-ovl2')
                stats['nested_points'] += 1
            log2.close()
        dm_remove('pc-ovl')
        shutil.rmtree(point_dir)

    def mount_check_fs(self, fs_dev):
        CHK.mkdir(parents=True, exist_ok=True)
        result = run('mount', '-o', 'noatime,nodiscard', fs_dev, CHK, check=False)
        if result.returncode:
            raise HarnessError(f'ext4 did not mount a replayed crash state: {result.stderr}; '
                               f'dmesg: {run("dmesg").stdout[-3000:]}')

    def cold_check(self, fs_dev, point_dir, expected, context):
        """Start on a crash image, check the oracle and the records, stop."""
        self.mount_check_fs(fs_dev)
        daemon = Daemon(CHK, point_dir / 'cold-daemon.log')
        try:
            daemon.start()
            daemon.unlock()
            verify_all(expected, self.refs)
            daemon.check_records()
            daemon.stop()
            recovery, failures = daemon.log_problems()
            if failures:
                raise Finding('daemon_log_errors', lines=failures[:50])
        except Finding:
            self.preserve(point_dir, daemon, context)
            raise
        finally:
            if daemon.proc and daemon.proc.poll() is None:
                daemon.kill()
            run('umount', CHK)

    def live_check(self, fs_dev, point_dir, expected, rng, context, dm_name):
        """Cold check, then a second life: settle, more ops, SIGKILL, restart, check again."""
        self.mount_check_fs(fs_dev)
        daemon = Daemon(CHK, point_dir / 'daemon.log')
        timeline = Timeline(point_dir / 'journal.jsonl', dm_name)
        crew = None
        try:
            daemon.start()
            daemon.unlock()
            seq_base = 10 ** 9 + context['point'] * 1000
            crew = Crew(daemon, timeline, self.refs, expected, rng, seq_base)
            # The settle verifies the crash image against `expected` before adopting it.
            crew.settle('cold')
            daemon.check_records()
            crew.resume(mmap_ok=False)
            time.sleep(rng.uniform(1, 6))
            crew.phase.window = 'kill'
            crew.phase.fault_at = time.monotonic()
            daemon.kill()
            crew.pause()
            daemon.start()
            daemon.unlock()
            # Process crash: the kernel kept every write, so everything acknowledged must be there.
            after = fold(expected, read_journal(point_dir / 'journal.jsonl'), timeline.marks.__contains__)
            observed = verify_all(after, self.refs)
            if observed != verify_all(crew.state(), self.refs):
                raise HarnessError('journal fold and live worker models disagree')
            daemon.check_records()
            crew.finish()
            daemon.stop()
            recovery, failures = daemon.log_problems()
            if failures:
                raise Finding('daemon_log_errors', lines=failures[:50])
            self.events.write(kind='point_ok', recovery_errors=len(recovery), **context)
        except Finding:
            if crew is not None:
                crew.phase.stop = True
                crew.phase.go.set()
            self.preserve(point_dir, daemon, context)
            raise
        finally:
            if daemon.proc and daemon.proc.poll() is None:
                daemon.kill()
            if mounted(CHK):
                run('umount', CHK)

    def preserve(self, point_dir, daemon, context):
        """Keep the crash image's data directory before anything else touches it."""
        save(point_dir / 'context.json', context)
        if daemon.proc and daemon.proc.poll() is None:
            daemon.kill()
        run('tar', '-C', CHK, '-czf', point_dir / 'data-after.tar.gz', 'data', timeout=1800, check=False)
        keep = self.dir / 'failed-point'
        if not keep.exists():
            shutil.copytree(point_dir, keep)


def digest_device(dev):
    h = hashlib.sha256()
    with open(dev, 'rb', buffering=0) as f:
        while block := f.read(8 << 20):
            h.update(block)
    return h.hexdigest()


# -------------------------------------------------------------------- main

def cmd_run(args):
    STATE.mkdir(parents=True, exist_ok=True)
    for path in (MNT, REC, CHK):
        if not mounted(path):
            path.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.hours * 3600
    status_path = STATE / 'status.json'
    save(status_path, dict(state='running', started=now(), hours=args.hours, faults=args.faults,
                           records_check=RECORDS_CHECK,
                           binary_sha256=file_sha(BIN),
                           harness_sha256=file_sha(__file__), runs_passed=0))
    passed = 0
    while time.monotonic() < deadline:
        seed = f'{time.time_ns()}'
        run_dir = STATE / 'runs' / time.strftime('%Y%m%d-%H%M%S')
        run_dir.mkdir(parents=True)
        current = Run(run_dir, seed)
        try:
            counts = current.record(args.record_seconds, args.log_budget, args.faults)
            stats = current.replay(args.points, args.nested_every)
        except Finding as finding:
            current.status(state='failed', phase='finding', finding=finding.kind, details=finding.details)
            current.events.write(kind='finding', finding=finding.kind, details=finding.details,
                                 trace=traceback.format_exc())
            print(f'FINDING {finding}', file=sys.stderr)
            return 1
        except BaseException:
            current.status(state='failed', phase='harness_error', error=traceback.format_exc()[-4000:])
            raise
        passed += 1
        shutil.rmtree(run_dir / 'refs')
        current.status(runs_passed=passed, last_run=dict(events=counts, replay=stats))
    status = json.loads(status_path.read_text())
    status.update(state='completed', finished=now())
    save(status_path, status)
    return 0


def file_sha(path):
    return sha(Path(path).read_bytes())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('run', help='record/replay runs until the deadline; stops at the first finding')
    r.add_argument('--hours', type=float, default=6)
    r.add_argument('--record-seconds', type=int, default=900)
    r.add_argument('--log-budget', type=float, default=0.8)
    r.add_argument('--points', type=int, default=400)
    r.add_argument('--nested-every', type=int, default=8)
    r.add_argument('--faults', default=','.join(FAULT_WEIGHTS),
                   help=f'comma-separated recording events to draw from ({",".join(FAULT_WEIGHTS)})')
    r.add_argument('--no-records-check', action='store_true',
                   help='skip `verfsnext check` (binaries older than B033 do not have it)')
    args = parser.parse_args()
    global RECORDS_CHECK
    RECORDS_CHECK = not args.no_records_check
    args.faults = args.faults.split(',')
    unknown = set(args.faults) - FAULT_WEIGHTS.keys()
    if unknown:
        parser.error(f'unknown faults: {sorted(unknown)}')
    if os.geteuid() != 0:
        raise SystemExit('powercut must run as root inside the powercut VM')
    sys.exit(cmd_run(args))


if __name__ == '__main__':
    main()
