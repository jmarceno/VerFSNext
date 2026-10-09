#!/usr/bin/env python3
"""OS-level workload and integrity oracle for the dedicated resilience LXC."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import stat
import subprocess
import time
import traceback

ROOT = Path('/var/lib/verfsnext-soak')
MOUNT = Path('/mnt/verfsnext')
EVIDENCE = ROOT / 'evidence'
REFERENCE = ROOT / 'reference'
# Acknowledged-write oracle lives on the LXC's ext4, never on the tested mount.
ACKS = ROOT / 'acks'
ACK_WORKERS = 2
ACK_MAX_FILES = 12
ACK_MAX_SIZE = 3 * 1024 * 1024
PACKS = ROOT / 'data/packs'
UNIT = 'verfsnext-soak.service'
COPY_SIZE = 16 * 1024 * 1024
PROBE_SIZE = 2 * 1024 * 1024
CONFIG = '/etc/verfsnext-soak.toml'
BIN = str(Path(__file__).resolve().parent / 'verfsnext')
PASSWORD = 'synthetic-resilience-data-only'
CURRENT_CYCLE = 0


class CommandFailure(RuntimeError):
    def __init__(self, args, returncode, stderr):
        super().__init__(f'{args}: exit {returncode}: {stderr}')
        self.returncode = returncode
        self.stderr = stderr


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
        raise CommandFailure(args, result.returncode, result.stderr)
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
    (MOUNT / 'acked').mkdir()
    ACKS.mkdir()
    for worker in range(ACK_WORKERS):
        (MOUNT / 'acked' / f'w{worker}').mkdir()
        (ACKS / f'w{worker}' / 'ref').mkdir(parents=True)
        save(ACKS / f'w{worker}' / 'model.json', dict(files={}, pending=None, ops=0))
    sync_dir(MOUNT / 'acked')
    write_file(MOUNT / 'probe.bin', contents('probe-0', PROBE_SIZE))
    for name in ('stable', 'tree', '.vault', 'acked'):
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
         new=hashlib.sha256(contents(f'probe-{cycle}', PROBE_SIZE)).hexdigest()))
    event('durable_checkpoint', cycle=cycle, snapshots=[s['name'] for s in expected['snapshots']],
          manifest_sha256=digest(EVIDENCE / 'expected.json'))


def background(cycle):
    event('background_begin', cycle=cycle)
    planned_fault = json.loads((EVIDENCE / 'pending.json').read_text())['fault']
    stop = ACKS / 'stop.json'
    if stop.exists():
        stop.unlink()
    copy = subprocess.Popen(['rsync', '--inplace', '--fsync', '--bwlimit=1024', str(ROOT / 'copy-source.bin'),
                             str(MOUNT / 'inflight/copy.bin')],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    save(EVIDENCE / 'copy-process.json', dict(cycle=cycle, pid=copy.pid))
    save(EVIDENCE / 'progress.json', dict(cycle=cycle, phase='writing', bytes=0))
    # Readers, acknowledged-write workers and snapshot churn run alongside rsync and atomic overwrite.
    reader = subprocess.Popen(['python3', __file__, 'reader', '--cycle', str(cycle)])
    helpers = [subprocess.Popen(['python3', __file__, 'ackwriter', '--cycle', str(cycle), '--worker', str(worker)])
               for worker in range(ACK_WORKERS)]
    if planned_fault == 'snapshot_op_kill':
        helpers.append(subprocess.Popen(['python3', __file__, 'snapshotter', '--cycle', str(cycle)]))
    tmp = MOUNT / 'probe.tmp'
    data = contents(f'probe-{cycle}', PROBE_SIZE)
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
    if copy.returncode == 0:
        # rsync --fsync synced the file; the directory fsync completes the acknowledgement.
        sync_dir(MOUNT / 'inflight')
        save(EVIDENCE / 'copy-ack.json', dict(cycle=cycle, sha256=digest(ROOT / 'copy-source.bin')))
        event('copy_fsync_ack', cycle=cycle)
    save(stop, dict(cycle=cycle))
    for helper in helpers:
        helper.wait(timeout=60)
        if helper.returncode:
            raise RuntimeError(f'helper {helper.args[2]} failed: {helper.returncode}')
    reader.wait(timeout=10)
    if reader.returncode:
        raise RuntimeError(f'concurrent reader failed: {reader.returncode}')
    allowed = (0, -9) if planned_fault == 'copy_sigkill' else (0,)
    if copy.returncode not in allowed:
        raise CopyFailure(copy.returncode, stderr.decode())
    event('background_end', cycle=cycle)


def stopped(cycle):
    path = ACKS / 'stop.json'
    return path.exists() and json.loads(path.read_text())['cycle'] == cycle


POOL = {}


def ack_contents(label, size):
    # Half the 64 KiB segments come from a small shared pool, so deletes and overwrites move dedup refcounts.
    rng = random.Random(label)
    parts = []
    for index in range((size + 65535) // 65536):
        if rng.random() < 0.5:
            key = rng.randrange(16)
            if key not in POOL:
                POOL[key] = contents(f'ack-pool-{key}', 65536)
            parts.append(POOL[key])
        else:
            parts.append(contents(f'{label}:{index}', 65536))
    return b''.join(parts)[:size]


def store_ref(worker, data):
    sha = hashlib.sha256(data).hexdigest()
    path = ACKS / f'w{worker}' / 'ref' / sha
    if not path.exists():
        write_file(path, data)
    return sha


def ref_bytes(worker, sha):
    return (ACKS / f'w{worker}' / 'ref' / sha).read_bytes()


def ack_state(path):
    info = os.lstat(path)
    return dict(sha256=digest(path), size=info.st_size, mode=stat.S_IMODE(info.st_mode),
                xattr=os.getxattr(path, 'user.ack').hex() if 'user.ack' in os.listxattr(path) else None)


def pwrite_all(fd, data, offset):
    view = memoryview(data)
    while view:
        written = os.pwrite(fd, view, offset)
        view, offset = view[written:], offset + written


def ackwriter(cycle, worker):
    """Serial POSIX mutations; each is recorded as pending before and acknowledged after fsync."""
    model_path = ACKS / f'w{worker}' / 'model.json'
    model = json.loads(model_path.read_text())
    if model['pending'] is not None:
        raise RuntimeError(f'ack worker {worker} started with an unsettled pending operation')
    folder = MOUNT / 'acked' / f'w{worker}'
    rng = random.Random(f'{cycle}:{worker}')
    sizes = [0, 1, 4095, 4096, 65537, 131072, 300001, 1048576, 2 * 1024 * 1024 + 13]
    while not stopped(cycle):
        files = model['files']
        names = sorted(files)
        choices = ['create'] * 3 if len(names) < 4 else []
        if len(names) < ACK_MAX_FILES:
            choices.append('create')
        if names:
            choices += ['overwrite', 'overwrite', 'truncate', 'rename', 'unlink', 'setattr']
        op = rng.choice(choices)
        seq = model['ops'] + 1
        label = f'ack-{cycle}-{worker}-{seq}'
        if op == 'create':
            name = f'f{seq:08d}'
            data = ack_contents(label, rng.choice(sizes))
            mode = rng.choice([0o600, 0o640, 0o644])
            pending = dict(op=op, name=name, new=store_ref(worker, data), mode=mode, xattr=label.encode().hex())
        elif op in ('overwrite', 'truncate'):
            name = rng.choice(names)
            old = ref_bytes(worker, files[name]['sha256'])
            if op == 'overwrite':
                offset = rng.randrange(len(old) + 1)
                patch = ack_contents(label, rng.randrange(1, 262145))
                data = old[:offset] + patch + old[offset + len(patch):]
                if len(data) > ACK_MAX_SIZE:
                    data = data[:ACK_MAX_SIZE]
                    patch = data[offset:]
            else:
                offset = rng.randrange(min(len(old), ACK_MAX_SIZE - 65536) + 65537)
                data = old[:offset] + bytes(max(0, offset - len(old)))
            pending = dict(op=op, name=name, old=files[name]['sha256'], new=store_ref(worker, data), offset=offset)
        elif op == 'rename':
            src = rng.choice(names)
            dst = rng.choice(names + [f'f{seq:08d}'] * 2)
            if dst == src:
                continue
            pending = dict(op=op, src=src, dst=dst)
        elif op == 'unlink':
            pending = dict(op=op, name=rng.choice(names))
        else:
            name = rng.choice(names)
            pending = dict(op=op, name=name, mode=rng.choice([0o600, 0o640, 0o644, 0o664]), xattr=label.encode().hex())
        model.update(pending=pending, ops=seq)
        save(model_path, model)
        if op == 'create':
            fd = os.open(folder / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                pwrite_all(fd, data, 0)
                os.fchmod(fd, mode)
                os.setxattr(fd, 'user.ack', bytes.fromhex(pending['xattr']))
                os.fsync(fd)
            finally:
                os.close(fd)
            sync_dir(folder)
            files[name] = dict(sha256=pending['new'], size=len(data), mode=mode, xattr=pending['xattr'])
        elif op == 'overwrite':
            fd = os.open(folder / name, os.O_WRONLY)
            try:
                pwrite_all(fd, patch, offset)
                os.fsync(fd)
            finally:
                os.close(fd)
            files[name].update(sha256=pending['new'], size=len(data))
        elif op == 'truncate':
            fd = os.open(folder / name, os.O_WRONLY)
            try:
                os.ftruncate(fd, len(data))
                os.fsync(fd)
            finally:
                os.close(fd)
            files[name].update(sha256=pending['new'], size=len(data))
        elif op == 'rename':
            os.rename(folder / pending['src'], folder / pending['dst'])
            sync_dir(folder)
            files[pending['dst']] = files.pop(pending['src'])
        elif op == 'unlink':
            os.unlink(folder / pending['name'])
            sync_dir(folder)
            del files[pending['name']]
        else:
            os.chmod(folder / name, pending['mode'])
            os.setxattr(folder / name, 'user.ack', bytes.fromhex(pending['xattr']))
            fd = os.open(folder / name, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            files[name].update(mode=pending['mode'], xattr=pending['xattr'])
        model['pending'] = None
        save(model_path, model)
    event('ackwriter_stopped', worker=worker, ops=model['ops'])


def snapshotter(cycle):
    count = 0
    while not stopped(cycle):
        name = f'stress-{cycle:06d}-{count:04d}'
        control('snapshot', 'create', name)
        control('snapshot', 'delete', name)
        count += 1
    event('snapshotter_stopped', iterations=count)


def daemon_pid():
    pid = int(command('systemctl', 'show', '--property=MainPID', '--value', UNIT).strip())
    if not pid:
        raise RuntimeError(f'{UNIT} has no main process')
    return pid


def rewrite_files():
    return [entry.name for folder in os.scandir(PACKS) if folder.is_dir()
            for entry in os.scandir(folder.path) if entry.name.endswith('.rewrite')]


def gc_kill(cycle):
    """Kill the daemon as soon as a GC pack rewrite temp file appears, or after 30 s without one."""
    pid = daemon_pid()
    started = time.monotonic()
    found = []
    while time.monotonic() - started < 30 and not found:
        found = rewrite_files()
        if not found:
            time.sleep(0.003)
    os.kill(pid, signal.SIGKILL)
    result = dict(hit=bool(found), files=found, waited=time.monotonic() - started)
    event('gc_kill', **result)
    print(json.dumps(result))


def restart_kill(cycle, delay):
    """Start the daemon and SIGKILL it again while it is recovering or freshly mounted."""
    command('systemctl', 'start', '--no-block', UNIT)
    time.sleep(delay)
    pid = int(command('systemctl', 'show', '--property=MainPID', '--value', UNIT).strip())
    attached = any(line.split()[4] == str(MOUNT)
                   for line in Path('/proc/self/mountinfo').read_text().splitlines())
    if pid:
        os.kill(pid, signal.SIGKILL)
    result = dict(delay=delay, pid=pid, mount_attached=attached)
    event('restart_kill', **result)
    print(json.dumps(result))


def worker():
    background(CURRENT_CYCLE)


def observe_error(action, error):
    path = EVIDENCE / 'fault-window.json'
    window = json.loads(path.read_text()) if path.exists() else {}
    attached = any(line.split()[4] == str(MOUNT)
                   for line in Path('/proc/self/mountinfo').read_text().splitlines())
    unavailable_io = isinstance(error, OSError) and (
        (error.errno in (errno.ENOTCONN, errno.ECONNABORTED)
         and (error.filename is None or Path(error.filename).is_relative_to(MOUNT))) or
        (error.errno == errno.ENOENT and not attached and error.filename is not None
         and Path(error.filename).is_relative_to(MOUNT)))
    unavailable_copy = isinstance(error, CopyFailure) and error.returncode in (11, 23) and (
        'Transport endpoint is not connected' in error.stderr or
        ('Software caused connection abort' in error.stderr and str(MOUNT) in error.stderr) or
        ('No such file or directory' in error.stderr and str(MOUNT) in error.stderr and not attached))
    # The snapshot CLI only reports availability; snapshot integrity is judged by verify.
    unavailable_control = action == 'snapshotter' and isinstance(error, CommandFailure)
    expected_outage = (
        (action in ('reader', 'worker', 'background', 'snapshotter') or action.startswith('ackwriter'))
        and window.get('cycle') == CURRENT_CYCLE
        and window.get('fault') in ('daemon_sigkill', 'daemon_sigterm', 'container_reboot',
                                    'container_hard_stop', 'gc_idle_restart', 'gc_rewrite_kill',
                                    'snapshot_op_kill', 'recovery_double_kill')
        and window.get('phase') in ('injecting', 'recovering')
        and (unavailable_io or unavailable_copy or unavailable_control)
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


def fail_mismatch(label, **details):
    save(EVIDENCE / 'mismatch.json', dict(time_ns=time.time_ns(), cycle=CURRENT_CYCLE, label=label, **details))
    raise RuntimeError(f'integrity mismatch in {label}; see mismatch.json')


def mixed(actual, old, new):
    """Every byte must come from the old or the new version; bytes past either end count as zero."""
    size = max(len(actual), len(old), len(new))
    old, new = old.ljust(size, b'\0'), new.ljust(size, b'\0')
    for offset in range(0, len(actual), 65536):
        block = actual[offset:offset + 65536]
        before, after = old[offset:offset + len(block)], new[offset:offset + len(block)]
        if block != before and block != after and any(a != b and a != c for a, b, c in zip(block, before, after)):
            return False
    return True


def verify_acks():
    count = 0
    for worker in range(ACK_WORKERS):
        model_path = ACKS / f'w{worker}' / 'model.json'
        model = json.loads(model_path.read_text())
        folder = MOUNT / 'acked' / f'w{worker}'
        files, pending = model['files'], model['pending']
        with os.scandir(folder) as scan:
            actual = {entry.name: ack_state(Path(entry.path)) for entry in scan}
        involved = set() if pending is None else {pending[k] for k in ('name', 'src', 'dst') if k in pending}
        label = f'acked:w{worker}'
        for name in sorted((files.keys() | actual.keys()) - involved):
            if files.get(name) != actual.get(name):
                fail_mismatch(label, path=name, expected=files.get(name), actual=actual.get(name), pending=pending)
        if pending is not None:
            op = pending['op']
            name = pending.get('name')
            state = actual.get(name)
            ok = True
            if op == 'create':
                ok = state is None or (state['size'] <= len(ref_bytes(worker, pending['new'])) and
                                       mixed((folder / name).read_bytes(), b'', ref_bytes(worker, pending['new'])))
            elif op == 'overwrite':
                old, new = ref_bytes(worker, pending['old']), ref_bytes(worker, pending['new'])
                ok = (state is not None and len(old) <= state['size'] <= len(new)
                      and state['mode'] == files[name]['mode'] and state['xattr'] == files[name]['xattr']
                      and mixed((folder / name).read_bytes(), old, new))
            elif op == 'truncate':
                old, new = ref_bytes(worker, pending['old']), ref_bytes(worker, pending['new'])
                ok = (state is not None and state['size'] in (len(old), len(new))
                      and state['mode'] == files[name]['mode'] and state['xattr'] == files[name]['xattr']
                      and (folder / name).read_bytes() == old.ljust(len(new), b'\0')[:state['size']])
            elif op == 'rename':
                src, dst = pending['src'], pending['dst']
                ok = ((actual.get(src) == files[src] and actual.get(dst) == files.get(dst)) or
                      (src not in actual and actual.get(dst) == files[src]))
            elif op == 'unlink':
                ok = state is None or state == files[name]
            else:
                ok = (state is not None and state['sha256'] == files[name]['sha256']
                      and state['mode'] in (files[name]['mode'], pending['mode'])
                      and state['xattr'] in (files[name]['xattr'], pending['xattr']))
            if not ok:
                fail_mismatch(label, pending=pending, expected={k: files.get(k) for k in involved},
                              actual={k: actual.get(k) for k in involved})
            # Unacknowledged outcomes were valid; adopt them so the next cycle has an exact model.
            for name in actual:
                store_ref(worker, (folder / name).read_bytes())
            event('ack_pending_settled', worker=worker, pending=pending,
                  outcome={k: actual.get(k) for k in involved})
            model.update(files=actual, pending=None)
            save(model_path, model)
        referenced = {row['sha256'] for row in model['files'].values()}
        for ref in (ACKS / f'w{worker}' / 'ref').iterdir():
            if ref.name not in referenced:
                ref.unlink()
        count += len(actual)
    return count


def verify_partial(cycle):
    """Unacknowledged files may be short or zero-filled, but never contain foreign bytes."""
    copy = MOUNT / 'inflight/copy.bin'
    ack_path = EVIDENCE / 'copy-ack.json'
    acked = ack_path.exists() and json.loads(ack_path.read_text())['cycle'] == cycle
    if acked and (not copy.exists() or digest(copy) != json.loads(ack_path.read_text())['sha256']):
        fail_mismatch('inflight/copy.bin', acknowledged=True, exists=copy.exists())
    if copy.exists() and not acked:
        data = copy.read_bytes()
        if len(data) > COPY_SIZE or not mixed(data, b'', (ROOT / 'copy-source.bin').read_bytes()):
            fail_mismatch('inflight/copy.bin', acknowledged=False, size=len(data))
    tmp = MOUNT / 'probe.tmp'
    if cycle and tmp.exists():
        data = tmp.read_bytes()
        if len(data) > PROBE_SIZE or not mixed(data, b'', contents(f'probe-{cycle}', PROBE_SIZE)):
            fail_mismatch('probe.tmp', size=len(data))


def verify_snapshots(expected):
    retained = {snap['name'] for snap in expected['snapshots']}
    with os.scandir(MOUNT / '.snapshots') as scan:
        names = sorted(entry.name for entry in scan)
    count = 0
    for name in names:
        if name in retained:
            continue
        if not name.startswith('stress-'):
            fail_mismatch('.snapshots', unexpected=name, retained=sorted(retained))
        # A snapshot that survived a crash during creation or deletion must be complete.
        for label in ('tree', 'stable'):
            count += compare(f'snapshot:{name}:{label}', MOUNT / '.snapshots' / name / label,
                             expected[label], check_hardlinks=False)
        control('snapshot', 'delete', name)
        event('stress_snapshot_verified_and_deleted', name=name)
    if not retained <= set(names):
        fail_mismatch('.snapshots', missing=sorted(retained - set(names)))
    return count


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
    count += verify_snapshots(expected)
    count += verify_acks()
    verify_partial(cycle)
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
    # The acknowledgement is consumed first: a crash afterwards leaves an unacknowledged copy check.
    ack = EVIDENCE / 'copy-ack.json'
    if ack.exists():
        ack.unlink()
        sync_dir(EVIDENCE)
    for path in (MOUNT / 'inflight').iterdir():
        path.unlink()
    if (MOUNT / 'probe.tmp').exists():
        (MOUNT / 'probe.tmp').unlink()
    sync_dir(MOUNT / 'inflight')
    sync_dir(MOUNT)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'unlock', 'prepare', 'background', 'worker', 'reader', 'verify',
                                           'cleanup', 'ackwriter', 'snapshotter', 'gc-kill', 'restart-kill'])
    parser.add_argument('--cycle', type=int, default=0)
    parser.add_argument('--worker', type=int, default=0)
    parser.add_argument('--delay', type=float, default=0)
    args = parser.parse_args()
    CURRENT_CYCLE = args.cycle
    action = f'ackwriter{args.worker}' if args.action == 'ackwriter' else args.action
    try:
        if args.action == 'worker':
            CURRENT_CYCLE = json.loads((EVIDENCE / 'pending.json').read_text())['cycle']
        if args.action == 'ackwriter':
            ackwriter(args.cycle, args.worker)
        elif args.action == 'gc-kill':
            gc_kill(args.cycle)
        elif args.action == 'restart-kill':
            restart_kill(args.cycle, args.delay)
        elif args.action in ('prepare', 'background', 'reader', 'verify', 'snapshotter'):
            globals()[args.action](args.cycle)
        else:
            globals()[args.action]()
    except Exception as error:
        expected_outage = observe_error(action, error)
        # Helpers end cleanly after a classified outage; their observation is judged by the supervisor.
        if expected_outage and args.action in ('reader', 'ackwriter', 'snapshotter'):
            event('helper_interrupted_by_fault', action=action)
        else:
            raise
