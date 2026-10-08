#!/usr/bin/env python3
"""OS-level workload and integrity oracle for the dedicated resilience LXC."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time
import traceback

ROOT = Path('/var/lib/verfsnext-soak')
MOUNT = Path('/mnt/verfsnext')
EVIDENCE = ROOT / 'evidence'
REFERENCE = ROOT / 'reference'
CONFIG = '/etc/verfsnext-soak.toml'
BIN = str(Path(__file__).resolve().parent / 'verfsnext')
PASSWORD = 'synthetic-resilience-data-only'
CURRENT_CYCLE = 0


class CopyFailure(RuntimeError):
    def __init__(self, returncode, stderr):
        super().__init__(f'copy failed: {returncode}: {stderr}')
        self.returncode = returncode
        self.stderr = stderr


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    sync_dir(path.parent)


def event(kind, **fields):
    value = dict(time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                 boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 kind=kind, cycle=CURRENT_CYCLE)
    value.update(fields)
    with (EVIDENCE / 'guest-events.jsonl').open('a') as f:
        f.write(json.dumps(value, sort_keys=True) + '\n')
        f.flush()
        os.fsync(f.fileno())


def command(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(f'{args}: exit {result.returncode}: {result.stderr}')
    return result.stdout


def control(*args):
    return command(BIN, '--config', CONFIG, *args)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb', buffering=0) as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def contents(label, size):
    return hashlib.shake_256(('verfsnext-soak-v1:' + label).encode()).digest(size)


def write_file(path, data):
    with path.open('wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    sync_dir(path.parent)


def walk(root):
    with os.scandir(root) as scan:
        entries = sorted(scan, key=lambda entry: entry.name)
    for entry in entries:
        path = Path(entry.path)
        info = entry.stat(follow_symlinks=False)
        yield path, info
        if stat.S_ISDIR(info.st_mode):
            yield from walk(path)


def durable_tree(root):
    directories = [root]
    for path, info in walk(root):
        if stat.S_ISREG(info.st_mode):
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        elif stat.S_ISDIR(info.st_mode):
            directories.append(path)
    for directory in reversed(directories):
        sync_dir(directory)
    sync_dir(root.parent)


def manifest(root):
    result, links = {}, {}
    for path, info in walk(root):
        rel = str(path.relative_to(root))
        row = dict(mode=stat.S_IMODE(info.st_mode), uid=info.st_uid, gid=info.st_gid)
        if stat.S_ISLNK(info.st_mode):
            row.update(type='symlink', target=os.readlink(path))
        elif stat.S_ISDIR(info.st_mode):
            row.update(type='directory')
        elif stat.S_ISREG(info.st_mode):
            row.update(type='file', size=info.st_size, sha256=digest(path),
                       xattrs={key: os.getxattr(path, key).hex()
                               for key in sorted(os.listxattr(path))})
            if info.st_nlink > 1:
                key = (info.st_dev, info.st_ino)
                links.setdefault(key, []).append(rel)
        else:
            raise RuntimeError(f'unexpected filesystem object: {path}')
        result[rel] = row
    return dict(entries=result, hardlinks=sorted(sorted(v) for v in links.values()))


def compare(label, root, expected, *, check_hardlinks=True):
    actual = manifest(root)
    equal = actual == expected if check_hardlinks else actual['entries'] == expected['entries']
    if not equal:
        differences = []
        for rel in sorted(expected['entries'].keys() | actual['entries'].keys()):
            old, new = expected['entries'].get(rel), actual['entries'].get(rel)
            if old != new:
                differences.append(dict(path=rel, expected=old, actual=new))
        save(EVIDENCE / 'mismatch.json', dict(time_ns=time.time_ns(), cycle=CURRENT_CYCLE, label=label,
             differences=differences, expected_hardlinks=expected['hardlinks'],
             actual_hardlinks=actual['hardlinks']))
        raise RuntimeError(f'integrity mismatch in {label}; see mismatch.json')
    return len(actual['entries'])


def init():
    REFERENCE.mkdir()
    for name in ('stable', 'tree', 'vault'):
        (REFERENCE / name).mkdir()
    for i in range(48):
        folder = REFERENCE / 'tree' / f'dir-{i % 6}'
        folder.mkdir(exist_ok=True)
        size = [0, 73, 4095, 32769, 262145, 1048576][i % 6]
        path = folder / f'file-{i:03d}'
        write_file(path, contents(f'initial-{i}', size))
        path.chmod(0o640 if i % 2 else 0o750)
        os.setxattr(path, 'user.resilience', f'initial-{i}'.encode())
    for name in ('stable', 'vault'):
        for i in range(6):
            write_file(REFERENCE / name / f'dedup-{i}', contents(f'duplicate-{i % 2}', 1048576))
        write_file(REFERENCE / name / 'compressible', b'VerFSNext\x00' * 65536)
    os.link(REFERENCE / 'tree/dir-5/file-005', REFERENCE / 'tree/hardlink')
    os.symlink('dir-1/file-001', REFERENCE / 'tree/symlink')
    write_file(ROOT / 'copy-source.bin', contents('copy-source', 16 * 1024 * 1024))
    control('crypt', '-c', '-p', PASSWORD, '-path', str(ROOT))
    unlock()
    for name in ('stable', 'tree'):
        command('rsync', '-aHAX', '--checksum', str(REFERENCE / name) + '/', str(MOUNT / name) + '/')
    command('rsync', '-aHAX', str(REFERENCE / 'vault') + '/', str(MOUNT / '.vault') + '/')
    (MOUNT / 'inflight').mkdir()
    write_file(MOUNT / 'probe.bin', contents('probe-0', 2 * 1024 * 1024))
    for name in ('stable', 'tree', '.vault'):
        durable_tree(MOUNT / name)
    sync_dir(MOUNT)
    expected = dict(stable=manifest(REFERENCE / 'stable'), tree=manifest(REFERENCE / 'tree'),
                    vault=manifest(REFERENCE / 'vault'), probe=digest(MOUNT / 'probe.bin'), snapshots=[])
    save(EVIDENCE / 'expected.json', expected)
    event('initialized', corpus_files=sum(len(expected[x]['entries']) for x in ('stable', 'tree', 'vault')))


def unlock():
    control('crypt', '-u', '-p', PASSWORD, '-k', str(ROOT / 'verfsnext.vault.key'))
    event('vault_unlocked')


def prepare(cycle):
    expected = json.loads((EVIDENCE / 'expected.json').read_text())
    event('prepare_begin', cycle=cycle)
    # Old snapshot manifests stay immutable while the live trees are mutated.
    if len(expected['snapshots']) == 2:
        old = expected['snapshots'].pop(0)
        control('snapshot', 'delete', old['name'])
    folder = REFERENCE / 'tree/dir-3'
    write_file(folder / 'changing.bin', contents(f'changing-{cycle}', 2 * 1024 * 1024 + cycle % 127))
    os.setxattr(folder / 'changing.bin', 'user.resilience', str(cycle).encode())
    transient = REFERENCE / 'tree/renamed.bin'
    write_file(REFERENCE / 'tree/rename.tmp', contents(f'renamed-{cycle}', 131073))
    os.replace(REFERENCE / 'tree/rename.tmp', transient)
    transient.chmod(0o600 if cycle % 2 else 0o644)
    removed = REFERENCE / 'tree/deleted.bin'
    if cycle % 2:
        write_file(removed, contents('delete-restore', 65536))
    elif removed.exists():
        removed.unlink()
    # In-place truncate/overwrite against a bounded scratch file exercises refcounts.
    scratch = MOUNT / 'inflight/churn.bin'
    write_file(scratch, contents(f'churn-{cycle}', 4 * 1024 * 1024))
    with scratch.open('r+b') as f:
        f.truncate(65537)
        f.seek(8192)
        f.write(contents('overwrite', 16384))
        f.flush()
        os.fsync(f.fileno())
    scratch.unlink()
    sync_dir(scratch.parent)
    # Keep a live handle after unlink, and close a second handle first (B006).
    scratch = MOUNT / 'inflight/open-unlinked.bin'
    payload = contents('open-unlinked', 65536)
    write_file(scratch, payload)
    with scratch.open('rb') as first:
        second = scratch.open('rb')
        scratch.unlink()
        second.close()
        if first.read() != payload:
            raise RuntimeError('open-unlinked file lost data after another handle closed')
    command('rsync', '-aHAX', '--delete', '--checksum', str(REFERENCE / 'tree') + '/', str(MOUNT / 'tree') + '/')
    write_file(REFERENCE / 'vault/current.bin', contents(f'vault-{cycle % 4}', 1048576))
    command('rsync', '-aHAX', '--delete', '--checksum', str(REFERENCE / 'vault') + '/', str(MOUNT / '.vault') + '/')
    durable_tree(MOUNT / 'tree')
    durable_tree(MOUNT / '.vault')
    expected['tree'] = manifest(REFERENCE / 'tree')
    expected['vault'] = manifest(REFERENCE / 'vault')
    name = f'cycle-{cycle:06d}'
    control('snapshot', 'create', name)
    sync_dir(MOUNT)
    expected['snapshots'].append(dict(name=name, tree=expected['tree'], stable=expected['stable']))
    save(EVIDENCE / 'expected.json', expected)
    compare('live-tree-before-fault', MOUNT / 'tree', expected['tree'])
    compare('vault-before-fault', MOUNT / '.vault', expected['vault'])
    save(EVIDENCE / 'pending.json', dict(cycle=cycle, old=expected['probe'],
         new=hashlib.sha256(contents(f'probe-{cycle}', 2 * 1024 * 1024)).hexdigest()))
    event('durable_checkpoint', cycle=cycle, snapshots=[s['name'] for s in expected['snapshots']],
          manifest_sha256=digest(EVIDENCE / 'expected.json'))


def background(cycle):
    event('background_begin', cycle=cycle)
    planned_fault = json.loads((EVIDENCE / 'pending.json').read_text())['fault']
    copy = subprocess.Popen(['rsync', '--inplace', '--bwlimit=1024', str(ROOT / 'copy-source.bin'),
                             str(MOUNT / 'inflight/copy.bin')],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    save(EVIDENCE / 'copy-process.json', dict(cycle=cycle, pid=copy.pid))
    save(EVIDENCE / 'progress.json', dict(cycle=cycle, phase='writing', bytes=0))
    # Readers run simultaneously with rsync and atomic overwrite traffic.
    reader = subprocess.Popen(['python3', __file__, 'reader', '--cycle', str(cycle)])
    tmp = MOUNT / 'probe.tmp'
    data = contents(f'probe-{cycle}', 2 * 1024 * 1024)
    with tmp.open('wb', buffering=0) as f:
        for offset in range(0, len(data), 131072):
            f.write(data[offset:offset + 131072])
            save(EVIDENCE / 'progress.json', dict(cycle=cycle, phase='writing', bytes=offset + 131072))
            time.sleep(0.10)
        os.fsync(f.fileno())
    save(EVIDENCE / 'progress.json', dict(cycle=cycle, phase='file_synced', bytes=len(data)))
    time.sleep(0.25)
    os.replace(tmp, MOUNT / 'probe.bin')
    save(EVIDENCE / 'progress.json', dict(cycle=cycle, phase='renamed', bytes=len(data)))
    time.sleep(0.25)
    sync_dir(MOUNT)
    save(EVIDENCE / 'probe-ack.json', dict(cycle=cycle, sha256=hashlib.sha256(data).hexdigest()))
    event('probe_fsync_ack', cycle=cycle)
    stdout, stderr = copy.communicate(timeout=40)
    # Only copy_sigkill intentionally ends this child; every other unexpected exit is logged.
    event('copy_exit', cycle=cycle, returncode=copy.returncode,
          stdout=stdout.decode(), stderr=stderr.decode())
    reader.wait(timeout=10)
    if reader.returncode:
        raise RuntimeError(f'concurrent reader failed: {reader.returncode}')
    allowed = (0, -9) if planned_fault == 'copy_sigkill' else (0,)
    if copy.returncode not in allowed:
        raise CopyFailure(copy.returncode, stderr.decode())
    event('background_end', cycle=cycle)


def worker():
    background(CURRENT_CYCLE)


def observe_error(action, error):
    path = EVIDENCE / 'fault-window.json'
    window = json.loads(path.read_text()) if path.exists() else {}
    attached = any(line.split()[4] == str(MOUNT)
                   for line in Path('/proc/self/mountinfo').read_text().splitlines())
    unavailable_io = isinstance(error, OSError) and (
        error.errno == errno.ENOTCONN or
        (error.errno == errno.ENOENT and not attached and error.filename is not None
         and Path(error.filename).is_relative_to(MOUNT)))
    unavailable_copy = isinstance(error, CopyFailure) and error.returncode == 23 and (
        'Transport endpoint is not connected' in error.stderr or
        ('No such file or directory' in error.stderr and not attached))
    expected_outage = (
        action in ('reader', 'worker', 'background')
        and window.get('cycle') == CURRENT_CYCLE
        and window.get('fault') in ('daemon_sigkill', 'daemon_sigterm', 'container_reboot',
                                    'container_hard_stop', 'gc_idle_restart')
        and window.get('phase') in ('injecting', 'recovering')
        and (unavailable_io or unavailable_copy)
    )
    frame = traceback.extract_tb(error.__traceback__)[-1]
    observation = dict(time_ns=time.time_ns(), cycle=CURRENT_CYCLE, action=action,
                       errno=getattr(error, 'errno', None), path=getattr(error, 'filename', None),
                       operation=frame.line, source=frame.filename, source_line=frame.lineno,
                       mount_attached=attached, fault_window=window,
                       classification='injected_outage' if expected_outage else 'unexpected_error',
                       traceback=traceback.format_exc())
    save(EVIDENCE / f'observation-{CURRENT_CYCLE:06d}-{action}.json', observation)
    event('guest_error', **observation)
    return expected_outage


def reader(cycle):
    expected = json.loads((EVIDENCE / 'expected.json').read_text())
    for _ in range(10):
        compare('concurrent-stable-reader', MOUNT / 'stable', expected['stable'])
        time.sleep(0.15)
    event('concurrent_reader_ok', cycle=cycle)


def verify(cycle):
    expected = json.loads((EVIDENCE / 'expected.json').read_text())
    event('verification_begin', cycle=cycle)
    count = 0
    for label, path in [('stable', 'stable'), ('tree', 'tree'), ('vault', '.vault')]:
        count += compare(label, MOUNT / path, expected[label])
    for snap in expected['snapshots']:
        for label in ('tree', 'stable'):
            count += compare(f"snapshot:{snap['name']}:{label}",
                             MOUNT / '.snapshots' / snap['name'] / label, snap[label], check_hardlinks=False)
    if cycle:
        pending = json.loads((EVIDENCE / 'pending.json').read_text())
        ack = json.loads((EVIDENCE / 'probe-ack.json').read_text()) if (EVIDENCE / 'probe-ack.json').exists() else None
        accepted = [pending['new']] if ack and ack['cycle'] == cycle else [pending['old'], pending['new']]
    else:
        accepted = [expected['probe']]
    actual = digest(MOUNT / 'probe.bin')
    if actual not in accepted:
        save(EVIDENCE / 'mismatch.json', dict(cycle=cycle, path='probe.bin', actual=actual,
                                             accepted=accepted, time_ns=time.time_ns()))
        raise RuntimeError('atomic overwrite produced invalid or non-durable data')
    expected['probe'] = actual
    save(EVIDENCE / 'expected.json', expected)
    save(EVIDENCE / f'verified-{cycle:06d}.json', dict(cycle=cycle, time_ns=time.time_ns(),
         objects_verified=count + 1, manifest_sha256=digest(EVIDENCE / 'expected.json'), probe_sha256=actual))
    event('verification_ok', cycle=cycle, objects_verified=count + 1)


def cleanup():
    for path in (MOUNT / 'inflight').iterdir():
        path.unlink()
    if (MOUNT / 'probe.tmp').exists():
        (MOUNT / 'probe.tmp').unlink()
    sync_dir(MOUNT / 'inflight')
    sync_dir(MOUNT)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'unlock', 'prepare', 'background', 'worker', 'reader', 'verify', 'cleanup'])
    parser.add_argument('--cycle', type=int, default=0)
    args = parser.parse_args()
    CURRENT_CYCLE = args.cycle
    try:
        if args.action == 'worker':
            CURRENT_CYCLE = json.loads((EVIDENCE / 'pending.json').read_text())['cycle']
        if args.action in ('prepare', 'background', 'reader', 'verify'):
            globals()[args.action](args.cycle)
        else:
            globals()[args.action]()
    except Exception as error:
        expected_outage = observe_error(args.action, error)
        if expected_outage and args.action == 'reader':
            event('concurrent_reader_interrupted_by_fault')
        else:
            raise
