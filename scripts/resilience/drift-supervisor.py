#!/usr/bin/env python3
"""Host-side supervisor for one continuous drift soak in LXC 101."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import traceback

UNIT = 'verfsnext-soak.service'
ORACLE_STATUS = '/var/lib/verfsnext-soak/drift-oracle/status.json'


def save(path, value):
    with path.with_suffix('.tmp').open('w') as handle:
        json.dump(value, handle, sort_keys=True, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(path.with_suffix('.tmp'), path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class DriftSupervisor:
    def __init__(self, args):
        self.ct = str(args.ct)
        self.root = Path(args.root)
        self.drift = args.drift_script
        self.seconds = args.seconds
        self.root.mkdir(parents=True)
        self.path = self.root / 'status.json'
        if self.path.exists():
            raise RuntimeError(f'{self.path} already exists; refuse to reuse a drift run directory')
        self.unlock = self.unlock_script()
        self.state = dict(ct=args.ct, status='running', phase='start', seconds=args.seconds,
                          started_at=time.time(), drift_script=self.drift, unlock_script=self.unlock)
        self.checkpoint()

    def unlock_script(self):
        values = {}
        for line in Path('/var/lib/verfsnext-resilience/101/release.env').read_text().splitlines():
            key, value = line.split('=', 1)
            values[key] = value
        if 'RESILIENCE_GUEST' not in values:
            raise RuntimeError('release.env has no RESILIENCE_GUEST')
        return values['RESILIENCE_GUEST']

    def event(self, kind, **fields):
        value = dict(time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                     host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                     kind=kind, **fields)
        with (self.root / 'host-events.jsonl').open('a') as handle:
            handle.write(json.dumps(value, sort_keys=True) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        print(json.dumps(value), flush=True)

    def checkpoint(self, **values):
        self.state.update(values, updated_at=time.time())
        save(self.path, self.state)

    def run(self, *args, timeout):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            self.event('command_failed', argv=args, returncode=result.returncode,
                       stdout=result.stdout[-4000:], stderr=result.stderr[-4000:])
            raise RuntimeError(f'{args}: exit {result.returncode}: {result.stderr[-4000:]}')
        return result.stdout

    def guest(self, *args, timeout):
        return self.run('pct', 'exec', self.ct, '--', 'env', 'PYTHONUNBUFFERED=1', *args, timeout=timeout)

    def require_running(self):
        state = self.run('lxc-info', '-n', self.ct, '-sH', timeout=30).strip()
        if state != 'RUNNING':
            raise RuntimeError(f'LXC {self.ct} is {state}')

    def mount_fresh(self):
        self.require_running()
        self.guest('systemctl', 'stop', 'verfsnext-workload.service', timeout=30)
        self.guest('systemctl', 'stop', UNIT, timeout=60)
        mounts = self.guest('cat', '/proc/self/mountinfo', timeout=30)
        if any(line.split()[4] == '/mnt/verfsnext' for line in mounts.splitlines()):
            self.guest('fusermount3', '-uz', '/mnt/verfsnext', timeout=30)
        self.guest('systemctl', 'start', UNIT, timeout=30)
        for attempt in range(60):
            result = subprocess.run(['pct', 'exec', self.ct, '--', 'mountpoint', '-q', '/mnt/verfsnext'],
                                    capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.event('mount_ready', attempts=attempt + 1)
                self.guest('python3', self.unlock, 'unlock', timeout=90)
                return
            time.sleep(0.5)
        raise RuntimeError('FUSE mount did not become ready within 30 seconds')

    def main(self):
        self.event('drift_supervisor_started', seconds=self.seconds)
        self.guest('python3', self.drift, 'mark', timeout=60)
        self.checkpoint(phase='mounting')
        self.mount_fresh()
        self.checkpoint(phase='init')
        self.guest('python3', self.drift, 'init', timeout=900)
        self.checkpoint(phase='stress')
        self.guest('python3', self.drift, 'run', '--seconds', str(self.seconds),
                   timeout=int(self.seconds) + 700)
        self.checkpoint(phase='remount_verify')
        self.mount_fresh()
        self.guest('python3', self.drift, 'verify', timeout=900)
        guest = json.loads(self.guest('cat', ORACLE_STATUS, timeout=30))
        self.checkpoint(status='completed', phase='stopped', finished_at=time.time(), guest=guest)
        self.event('drift_completed', guest=guest)

    def fail(self):
        error = traceback.format_exc()
        self.event('drift_failed', traceback=error, phase=self.state['phase'])
        self.checkpoint(status='failed', failed_at=time.time(), error=error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ct', type=int, required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--drift-script', required=True)
    parser.add_argument('--seconds', type=float, required=True)
    args = parser.parse_args()
    supervisor = DriftSupervisor(args)
    try:
        supervisor.main()
    except Exception:
        supervisor.fail()
        raise
