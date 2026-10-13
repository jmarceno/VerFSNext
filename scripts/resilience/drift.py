#!/usr/bin/env python3
"""Continuous same-volume stress and drift oracle. Runs inside the resilience LXC.

Writers keep rewriting the same hot files on /mnt/verfsnext for the whole run.
Some rewrites repeat a previous image, others are new, and each image mixes
shared 1 MiB pages with unique pages. Bytes already fsynced — the pre-existing
volume and drift/stable — are re-read on every pass and must not change. Hot
files may change only when their published generation changes, and every
published generation must match a later disk read.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import stat
import subprocess
import time

MOUNT = Path('/mnt/verfsnext')
ROOT = Path('/var/lib/verfsnext-soak')
ORACLE = ROOT / 'drift-oracle'
DRIFT = MOUNT / 'drift'
UNIT = 'verfsnext-soak.service'
WRITERS = 4
HOT_COUNT = 8
HOT_SIZE = 16 * 1024 * 1024
STABLE_COUNT = 32
STABLE_SIZE = 8 * 1024 * 1024
UNIQUE_SIZE = 8 * 1024 * 1024
POOL_BLOCKS = 256
BLOCK = 64 * 1024
FREE_FLOOR = 150 * 1024 * 1024
PAGE = 1024 * 1024
RETAINED = 6
DAEMON_FAILURE = re.compile(r'\bERROR\b|panicked|(sys|session|current)_total=[1-9]')
WARNED = set()


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as handle:
        json.dump(value, handle, sort_keys=True)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    sync_dir(path.parent)


def append_json(path, value):
    lock = os.open(ORACLE / 'log.lock', os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with path.open('a') as handle:
            handle.write(json.dumps(value, sort_keys=True) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        sync_dir(path.parent)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        os.close(lock)


def event(kind, **fields):
    append_json(ORACLE / 'events.jsonl', dict(time_ns=time.time_ns(), kind=kind, **fields))
    print(json.dumps(dict(kind=kind, **fields)), flush=True)


def command(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f'{args}: exit {result.returncode}: {result.stderr}')
    return result.stdout


def digest(path):
    hashed = hashlib.sha256()
    with path.open('rb', buffering=0) as handle:
        while block := handle.read(1024 * 1024):
            hashed.update(block)
    return hashed.hexdigest()


def xattrs(path):
    return {key: os.getxattr(path, key, follow_symlinks=False).hex()
            for key in sorted(os.listxattr(path, follow_symlinks=False))}


def pool():
    return [hashlib.shake_256(f'verfsnext-drift-pool-{index}'.encode()).digest(BLOCK)
            for index in range(POOL_BLOCKS)]


def hot_image(index, version, blocks):
    """Whole-file image. Repeating a version reuses the same UltraCDC chunks."""
    buffer = bytearray(HOT_SIZE)
    for offset in range(0, HOT_SIZE, BLOCK):
        buffer[offset:offset + BLOCK] = blocks[(index + version * 31 + offset // BLOCK) % len(blocks)]
    return buffer


def locked(mutator=None):
    fd = os.open(ORACLE / 'model.lock', os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        model = json.loads((ORACLE / 'model.json').read_text())
        if mutator is not None:
            mutator(model)
            save(ORACLE / 'model.json', model)
        return model
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def fail(label, **details):
    payload = dict(time_ns=time.time_ns(), label=label, **details)
    save(ORACLE / 'mismatch.json', payload)
    raise RuntimeError('drift mismatch in ' + label + ': ' + json.dumps(payload, sort_keys=True))


def describe(path, info):
    mode = stat.S_IMODE(info.st_mode)
    if stat.S_ISLNK(info.st_mode):
        return dict(type='symlink', mode=mode, target=os.readlink(path))
    if stat.S_ISDIR(info.st_mode):
        return dict(type='directory', mode=mode)
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f'unexpected filesystem object: {path}')
    return dict(type='file', mode=mode, size=info.st_size, nlink=info.st_nlink,
                sha256=digest(path), xattrs=xattrs(path))


def walk_baseline():
    """Hash every durable object except the hot and churn subtrees."""
    records = {}

    def scan(directory, prefix):
        with os.scandir(directory) as found:
            entries = sorted(found, key=lambda entry: entry.name)
        for entry in entries:
            rel = f'{prefix}/{entry.name}' if prefix else entry.name
            path = Path(entry.path)
            info = entry.stat(follow_symlinks=False)
            records[rel] = describe(path, info)
            if stat.S_ISDIR(info.st_mode) and rel not in ('drift/hot', 'drift/churn'):
                scan(path, rel)

    scan(MOUNT, '')
    return records


def write_new(path, data):
    """Create a file with 1 MiB aligned writes, matching the daemon block size."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        write_aligned(fd, data, 0)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o644)
    sync_dir(path.parent)


def write_aligned(fd, data, offset):
    view = memoryview(data)
    while view:
        step = min(1024 * 1024, len(view))
        written = os.pwrite(fd, view[:step], offset)
        if written != step:
            raise RuntimeError(f'short pwrite at {offset}: {written} of {step}')
        view = view[step:]
        offset += step


def journal_since_path():
    return ORACLE / 'journal_since'


def mark_journal():
    path = journal_since_path()
    ORACLE.mkdir(exist_ok=True)
    path.write_text(f'{time.time()}\n')
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    sync_dir(ORACLE)


def check_journal():
    since = float(journal_since_path().read_text())
    text = command('journalctl', '-u', UNIT, '--since', f'@{int(since) - 1}', '--no-pager', '-o', 'cat')
    failures, warnings = [], []
    for line in text.splitlines():
        if DAEMON_FAILURE.search(line):
            failures.append(line)
        elif ' WARN ' in line and line not in WARNED:
            WARNED.add(line)
            warnings.append(line)
    if warnings:
        append_json(ORACLE / 'warnings.jsonl', dict(time_ns=time.time_ns(), lines=warnings))
    mark_journal()
    if failures:
        event('daemon_log_failure', lines=failures)
        raise RuntimeError('daemon logged errors: ' + json.dumps(failures))
    return len(warnings)


def free_bytes():
    info = os.statvfs('/')
    return info.f_bavail * info.f_frsize


def pack_bytes():
    return int(command('du', '-sb', str(ROOT / 'data/packs')).split()[0])


def space_tight():
    return free_bytes() < FREE_FLOOR


def file_state(path):
    info = path.stat()
    return dict(type='file', mode=stat.S_IMODE(info.st_mode), size=info.st_size, nlink=info.st_nlink,
                sha256=digest(path), xattrs=xattrs(path))


def audit(final):
    started = time.monotonic()
    model = locked()
    baseline = walk_baseline()
    if set(baseline) != set(model['baseline']):
        fail('baseline-namespace', missing=sorted(set(model['baseline']) - set(baseline)),
             unexpected=sorted(set(baseline) - set(model['baseline'])))
    checked = bytes_checked = skipped = retried = 0
    for rel, expected in model['baseline'].items():
        actual = baseline[rel]
        checked += 1
        bytes_checked += actual.get('size', 0)
        if actual != expected:
            fail('baseline', path=rel, expected=expected, actual=actual)
    for rel, expected in model['hot'].items():
        path = MOUNT / rel
        before = locked()['hot'][rel]
        if before['writing']:
            if final:
                fail('hot-writing-at-final', path=rel, record=before)
            skipped += 1
            continue
        actual = file_state(path)
        after = locked()['hot'][rel]
        if after['generation'] != before['generation'] or after['writing']:
            if final:
                fail('hot-changed-during-final-read', path=rel, before=before, after=after)
            retried += 1
            continue
        checked += 1
        bytes_checked += actual['size']
        published = dict(type='file', mode=before['mode'], size=before['size'], nlink=before['nlink'],
                         sha256=before['sha256'], xattrs=before['xattrs'])
        if actual != published:
            fail('hot', path=rel, expected=published, actual=actual, generation=before['generation'])
    warnings = check_journal()
    free = free_bytes()
    packs = pack_bytes()
    status = dict(phase='final' if final else 'running', passes=model['passes'] + 1,
                  files_checked=checked, bytes_checked=bytes_checked, bytes_written=model['bytes_written'],
                  skipped_hot=skipped, retried_hot=retried, free_bytes=free, pack_bytes=packs,
                  warnings=warnings, duration_s=time.monotonic() - started, updated_at=time.time())
    def count(current):
        current['passes'] += 1
    locked(count)
    save(ORACLE / 'status.json', status)
    append_json(ORACLE / 'passes.jsonl', status)
    event('pass', **status)
    return status


def mark():
    mark_journal()
    event('journal_marked')


def init():
    if (ORACLE / 'model.json').exists():
        raise RuntimeError('drift oracle already exists; refuse to mix two runs')
    if not journal_since_path().exists():
        raise RuntimeError('journal cursor was not marked before init')
    ORACLE.mkdir(exist_ok=True)
    save(ORACLE / 'status.json', dict(phase='init', updated_at=time.time()))
    (ORACLE / 'model.lock').touch()
    for leaf in ('stable', 'hot', 'churn'):
        (DRIFT / leaf).mkdir(parents=True)
    blocks = pool()
    for index in range(STABLE_COUNT):
        write_new(DRIFT / 'stable' / f'stable-{index:02d}',
                    hashlib.shake_256(f'verfsnext-drift-stable-{index}'.encode()).digest(STABLE_SIZE))
    hot = {}
    for index in range(HOT_COUNT):
        blob = hot_image(index, 0, blocks)
        path = DRIFT / 'hot' / f'hot-{index}'
        write_new(path, blob)
        info = path.stat()
        hot[f'drift/hot/hot-{index}'] = dict(
            type='file', mode=stat.S_IMODE(info.st_mode), size=info.st_size, nlink=info.st_nlink,
            sha256=hashlib.sha256(blob).hexdigest(), xattrs=xattrs(path), generation=0, writing=False)
    unique = hashlib.shake_256(b'verfsnext-drift-unique-0').digest(UNIQUE_SIZE)
    unique_path = DRIFT / 'hot' / 'unique'
    write_new(unique_path, unique)
    info = unique_path.stat()
    hot['drift/hot/unique'] = dict(
        type='file', mode=stat.S_IMODE(info.st_mode), size=info.st_size, nlink=info.st_nlink,
        sha256=hashlib.sha256(unique).hexdigest(), xattrs=xattrs(unique_path), generation=0, writing=False)
    save(ORACLE / 'model.json', dict(baseline=walk_baseline(), hot=hot, bytes_written=0, passes=0))
    check_journal()
    free = free_bytes()
    event('initialized', stable_files=STABLE_COUNT, hot_files=len(hot), free_bytes=free,
          baseline_files=len(json.loads((ORACLE / 'model.json').read_text())['baseline']))


def stopped():
    return (ORACLE / 'stop.json').exists()


def idle_epoch():
    path = ORACLE / 'idle.json'
    if not path.exists():
        return None
    return json.loads(path.read_text())['epoch']


def begin(rel):
    def mutate(model):
        record = model['hot'][rel]
        if record['writing']:
            raise RuntimeError(f'{rel} is already being written')
        record['writing'] = True
    locked(mutate)


def commit(rel, sha, size, added):
    def mutate(model):
        record = model['hot'][rel]
        record.update(sha256=sha, size=size, writing=False, generation=record['generation'] + 1)
        model['bytes_written'] += added
    locked(mutate)


def shared_pages():
    return [hashlib.shake_256(f'verfsnext-drift-page-{index}'.encode()).digest(PAGE) for index in range(64)]


def compose(pages, tag, rng):
    """Mix repeated pages with unique pages. tag selects the unique bytes."""
    parts = []
    for page in range(HOT_SIZE // PAGE):
        if rng.random() < 0.5:
            parts.append(pages[(page + rng.randrange(len(pages))) % len(pages)])
        else:
            parts.append(hashlib.shake_256(f'{tag}:{page}'.encode()).digest(PAGE))
    return b''.join(parts)


def rewrite_all(rel, blob):
    begin(rel)
    fd = os.open(MOUNT / rel, os.O_WRONLY)
    try:
        write_aligned(fd, blob, 0)
        os.fsync(fd)
    finally:
        os.close(fd)
    commit(rel, hashlib.sha256(blob).hexdigest(), len(blob), len(blob))


def writer(slot):
    pages = shared_pages()
    rng = random.Random()
    indexes = (slot, slot + WRITERS)
    retained = {index: [] for index in indexes}
    current = {}
    seq = 0
    for index in indexes:
        blob = compose(pages, f'init-writer-{slot}-{index}', rng)
        current[index] = bytearray(blob)
        retained[index].append(bytes(blob))
    while not stopped():
        if idle_epoch() is not None:
            time.sleep(0.05)
            continue
        index = indexes[rng.randrange(len(indexes))]
        history = retained[index]
        tight = space_tight()
        roll = rng.random()
        if tight or roll < 0.35:
            blob = history[rng.randrange(len(history))]
            current[index] = bytearray(blob)
        elif roll < 0.7:
            seq += 1
            blob = compose(pages, f'{slot}:{index}:{seq}:{time.time_ns()}', rng)
            current[index] = bytearray(blob)
            history.append(blob)
            if len(history) > RETAINED:
                del history[0]
        else:
            seq += 1
            offset = rng.randrange(0, HOT_SIZE, PAGE)
            if rng.random() < 0.5:
                page = pages[rng.randrange(len(pages))]
            else:
                page = hashlib.shake_256(f'{slot}:{index}:{seq}:{offset}'.encode()).digest(PAGE)
            current[index][offset:offset + PAGE] = page
            blob = bytes(current[index])
            history.append(blob)
            if len(history) > RETAINED:
                del history[0]
        rewrite_all(f'drift/hot/hot-{index}', blob)
    event('writer_stopped', slot=slot)


def recycle(pages):
    path = DRIFT / 'churn' / 'cycle.bin'
    blob = b''.join(pages[index % len(pages)] for index in range(HOT_SIZE // PAGE))
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        write_aligned(fd, blob, 0)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.unlink(path)
    sync_dir(path.parent)


def churn():
    pages = shared_pages()
    rng = random.Random()
    retained = []
    seq = 0
    while not stopped():
        if idle_epoch() is not None:
            time.sleep(0.05)
            continue
        folder = DRIFT / 'churn'
        names = sorted(entry.name for entry in os.scandir(folder) if entry.is_file())
        if space_tight():
            if names:
                os.unlink(folder / names[0])
                sync_dir(folder)
            time.sleep(0.3)
            continue
        if names and rng.random() < 0.2:
            os.unlink(folder / rng.choice(names))
            sync_dir(folder)
            continue
        seq += 1
        if retained and rng.random() < 0.4:
            blob = retained[rng.randrange(len(retained))]
        else:
            blob = compose(pages, f'churn:{seq}:{time.time_ns()}', rng)
            retained.append(blob)
            if len(retained) > RETAINED:
                del retained[0]
        path = folder / f'c{seq % 24:02d}'
        if path.exists():
            os.unlink(path)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            write_aligned(fd, blob, 0)
            os.fsync(fd)
        finally:
            os.close(fd)
        sync_dir(folder)
        if rng.random() < 0.15:
            recycle(pages)
    event('churn_stopped')


def check(seconds):
    deadline = time.monotonic() + seconds
    next_idle = time.monotonic() + 60
    while time.monotonic() < deadline:
        if time.monotonic() >= next_idle:
            epoch = time.time_ns()
            save(ORACLE / 'idle.json', dict(epoch=epoch))
            time.sleep(3)
            audit(final=False)
            os.unlink(ORACLE / 'idle.json')
            sync_dir(ORACLE)
            next_idle = time.monotonic() + 60
            continue
        audit(final=False)
        pause = min(15, deadline - time.monotonic(), next_idle - time.monotonic())
        if pause > 0:
            time.sleep(pause)
    save(ORACLE / 'stop.json', dict(time_ns=time.time_ns()))
    quiet = time.monotonic() + 120
    while time.monotonic() < quiet:
        if not any(row['writing'] for row in locked()['hot'].values()):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError('hot files still marked writing 120s after stop')
    audit(final=True)
    event('checker_stopped')


def run(seconds):
    script = str(Path(__file__).resolve())
    procs = [subprocess.Popen(['python3', script, 'writer', '--slot', str(slot)]) for slot in range(WRITERS)]
    procs.append(subprocess.Popen(['python3', script, 'churn']))
    procs.append(subprocess.Popen(['python3', script, 'check', '--seconds', str(seconds)]))
    failed = None
    deadline = time.monotonic() + seconds + 600
    try:
        while time.monotonic() < deadline:
            finished = True
            for proc in procs:
                code = proc.poll()
                if code is None:
                    finished = False
                elif code:
                    raise RuntimeError(f'{proc.args} exited {code}')
            if finished:
                return
            time.sleep(1)
        raise RuntimeError('drift run exceeded its deadline')
    except Exception as error:
        failed = error
        raise
    finally:
        if failed is not None:
            save(ORACLE / 'stop.json', dict(time_ns=time.time_ns(), error=str(failed)))
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()


def verify():
    if any(row['writing'] for row in locked()['hot'].values()):
        raise RuntimeError('verify started while a hot file is still being written')
    status = audit(final=True)
    save(ORACLE / 'result.json', dict(status='passed', **status))
    event('verified', **status)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['mark', 'init', 'run', 'verify', 'writer', 'churn', 'check'])
    parser.add_argument('--seconds', type=float, default=0)
    parser.add_argument('--slot', type=int, default=0)
    args = parser.parse_args()
    if args.action == 'mark':
        mark()
    elif args.action == 'init':
        init()
    elif args.action == 'run':
        run(args.seconds)
    elif args.action == 'verify':
        verify()
    elif args.action == 'writer':
        writer(args.slot)
    elif args.action == 'churn':
        churn()
    else:
        check(args.seconds)
