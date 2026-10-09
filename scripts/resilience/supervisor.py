#!/usr/bin/env python3
"""Persistent Proxmox supervisor. All filesystem traffic runs inside one LXC."""
import argparse
import json
import os
from pathlib import Path
import random
import re
import subprocess
import time
import traceback

FAULTS = ['copy_sigkill', 'daemon_sigkill', 'daemon_sigterm', 'container_reboot',
          'container_hard_stop', 'gc_idle_restart', 'gc_rewrite_kill', 'snapshot_op_kill',
          'recovery_double_kill']
# Faults after which the daemon recovers from an abrupt stop; a torn WAL tail is legitimate there.
CRASH_FAULTS = {'daemon_sigkill', 'container_hard_stop', 'gc_idle_restart', 'gc_rewrite_kill',
                'snapshot_op_kill', 'recovery_double_kill'}
UNIT = 'verfsnext-soak.service'
DAEMON_LINE = re.compile(r' verfsnext\[\d+\]: ')
DAEMON_FAILURE = re.compile(r'\bERROR\b|panicked|(sys|session|current)_total=[1-9]')
WAL_TAIL = re.compile(r'Corrupted WAL record detected|Corruption in WAL')


def save(path, value):
    with path.with_suffix('.tmp').open('w') as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(path.with_suffix('.tmp'), path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Supervisor:
    def __init__(self, args):
        self.ct = str(args.ct)
        self.guest_script = args.guest_script
        self.root = Path(args.root)
        self.evidence = self.root / 'evidence'
        self.path = self.root / 'status.json'
        self.interval = args.interval
        self.limit_cycles = args.cycles
        if self.path.exists():
            self.state = json.loads(self.path.read_text())
            if args.extend_hours:
                if self.state['status'] != 'completed':
                    raise RuntimeError('extension requires a completed, passing run')
                self.state.update(status='running', phase='idle',
                                  target_seconds=self.state['active_seconds'] + args.extend_hours * 3600,
                                  armed_at=time.time(), extension_hours=args.extend_hours,
                                  mode='endurance')
                del self.state['finished_at']
        else:
            self.state = dict(ct=args.ct, status='running', phase='idle', cycle=0,
                              active_seconds=0, target_seconds=args.hours * 3600,
                              started_at=time.time(), faults={}, verifications=0,
                              mode='preflight' if args.cycles else 'endurance',
                              preflight_verifications=0, endurance_verifications=0,
                              seed=time.time_ns(), gc_rewrite_hits=0, gc_rewrite_misses=0,
                              wal_tail_repairs=0, daemon_warnings=0,
                              known_findings=['Snapshots clone hardlink names to separate inodes; snapshot hashes and metadata are checked, live hardlinks are checked strictly.'])
        self.started_cycles = self.state['cycle']

    def event(self, kind, **fields):
        value = dict(time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                     host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                     cycle=self.state['cycle'], kind=kind, **fields)
        with (self.root / 'host-events.jsonl').open('a') as f:
            f.write(json.dumps(value, sort_keys=True) + '\n')
            f.flush()
            os.fsync(f.fileno())
        print(json.dumps(value), flush=True)

    def checkpoint(self, **values):
        self.state.update(values, updated_at=time.time())
        save(self.path, self.state)

    def run(self, *args, timeout=90, allowed=(0,)):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if result.returncode not in allowed:
            self.event('command_failed', argv=args, returncode=result.returncode,
                       stdout=result.stdout, stderr=result.stderr)
            raise RuntimeError(f'{args}: exit {result.returncode}: {result.stderr}')
        if result.returncode:
            self.event('expected_command_exit', argv=args, returncode=result.returncode,
                       stdout=result.stdout, stderr=result.stderr)
        return result.stdout

    def guest(self, *args, **kwargs):
        return self.run('pct', 'exec', self.ct, '--', *args, **kwargs)

    def action(self, action):
        return self.guest('python3', self.guest_script, action, '--cycle', str(self.state['cycle']))

    def container_running(self):
        return self.run('pct', 'status', self.ct).strip() == 'status: running'

    def wait_daemon(self):
        for attempt in range(60):
            result = subprocess.run(['pct', 'exec', self.ct, '--', 'mountpoint', '-q', '/mnt/verfsnext'],
                                    capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.event('mount_ready', attempts=attempt + 1)
                return
            time.sleep(0.5)
        raise RuntimeError('FUSE mount did not become ready within 30 seconds')

    def mount_fresh(self):
        if not self.container_running():
            self.run('pct', 'start', self.ct)
        self.guest('systemctl', 'stop', 'verfsnext-workload.service')
        self.guest('systemctl', 'stop', UNIT)
        mounts = self.guest('cat', '/proc/self/mountinfo')
        if any(line.split()[4] == '/mnt/verfsnext' for line in mounts.splitlines()):
            self.guest('fusermount3', '-uz', '/mnt/verfsnext')
        self.guest('systemctl', 'start', UNIT)
        self.wait_daemon()
        self.action('unlock')

    def capture(self, suffix):
        cycle = self.state['cycle']
        # Consecutive captures overlap by a second, so every daemon line is inspected at least once.
        since = self.state.get('journal_since', self.state.get('cycle_started_at', time.time() - 60))
        captured_at = time.time()
        journal = self.guest('journalctl', '-u', UNIT, '-u', 'verfsnext-workload.service',
                             '--since', f'@{int(since) - 1}', '--no-pager', '-o', 'short-iso-precise')
        path = self.root / f'journal-{cycle:06d}-{suffix}.log'
        path.write_text(journal)
        with path.open('rb') as f:
            os.fsync(f.fileno())
        self.checkpoint(journal_since=captured_at)
        if suffix != 'failure':
            self.check_daemon_log(journal, suffix)
        report = dict(time_ns=time.time_ns(), df=self.guest('df', '-B1', '/'),
                      memory=self.guest('cat', '/sys/fs/cgroup/memory.events'),
                      memory_current_peak_max=self.guest('cat', '/sys/fs/cgroup/memory.current',
                                                        '/sys/fs/cgroup/memory.peak', '/sys/fs/cgroup/memory.max'),
                      enforced_memory_max=Path(f'/sys/fs/cgroup/lxc/{self.ct}/memory.max').read_text().strip(),
                      enforced_cpu_max=Path(f'/sys/fs/cgroup/lxc/{self.ct}/cpu.max').read_text().strip(),
                      packs=self.guest('find', '/var/lib/verfsnext-soak/data/packs',
                                       '-maxdepth', '2', '-type', 'f', '-printf', '%P %s %T@\n'))
        save(self.root / f'resources-{cycle:06d}-{suffix}.json', report)

    def check_daemon_log(self, journal, suffix):
        lines = [line for line in journal.splitlines() if DAEMON_LINE.search(line)]
        warnings = [line for line in lines if ' WARN ' in line]
        if warnings:
            self.checkpoint(daemon_warnings=self.state['daemon_warnings'] + len(warnings))
            self.event('daemon_warnings', capture=suffix, lines=warnings)
        failures = [line for line in lines if DAEMON_FAILURE.search(line) or WAL_TAIL.search(line)]
        tail = [line for line in failures if WAL_TAIL.search(line)]
        if tail and suffix == 'after' and self.state.get('last_fault') in CRASH_FAULTS:
            # Data loss from a torn tail is judged by the durability oracle, not by this message.
            self.checkpoint(wal_tail_repairs=self.state['wal_tail_repairs'] + 1)
            self.event('wal_tail_repair', lines=tail)
            failures = [line for line in failures if line not in tail]
        if failures:
            self.event('daemon_log_failure', capture=suffix, lines=failures)
            raise RuntimeError(f'daemon logged errors in {suffix} capture; see host-events.jsonl')

    def wait_workload(self, label):
        for _ in range(600):
            active = self.guest('systemctl', 'is-active', 'verfsnext-workload.service', allowed=(0, 3))
            if active.strip() != 'active':
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f'workload did not finish before {label}')
        result = self.guest('systemctl', 'show', '--property=Result', '--value', 'verfsnext-workload.service')
        if result.strip() != 'success':
            raise RuntimeError(f'{label} workload failed: {result}')

    def kill_daemon(self):
        self.guest('systemctl', 'kill', '--kill-whom=main', '--signal=SIGKILL', UNIT)

    def capacity(self):
        free = int(self.guest('df', '--output=avail', '-B1', '/').splitlines()[-1])
        def strict_walk_error(error):
            raise error
        evidence_bytes = sum((Path(directory) / name).stat().st_size
                             for directory, _, files in os.walk(self.root, onerror=strict_walk_error)
                             for name in files)
        self.checkpoint(rootfs_free_bytes=free, evidence_bytes=evidence_bytes)
        if free < 768 * 1024 * 1024 or evidence_bytes > 64 * 1024 * 1024:
            raise RuntimeError('resource budget reached; evidence preserved, no further writes allowed')

    def wait_progress(self, cycle):
        for _ in range(300):
            path = self.evidence / 'progress.json'
            if path.exists():
                progress = json.loads(path.read_text())
                if progress['cycle'] == cycle and progress['bytes'] >= 131072:
                    return progress
            time.sleep(0.05)
        raise RuntimeError('background workload failed to publish progress')

    def verify_recovery(self):
        cycle = self.state['cycle']
        self.action('verify')
        if (self.evidence / 'mismatch.json').exists():
            raise RuntimeError('completed concurrent read detected a real mismatch; see mismatch.json')
        for path in sorted(p for p in self.evidence.iterdir()
                           if p.name.startswith(f'observation-{cycle:06d}-') and p.suffix == '.json'):
            observation = json.loads(path.read_text())
            self.event('workload_observation', observation=observation)
            if observation['classification'] != 'injected_outage':
                raise RuntimeError(f'unexpected workload error; see {path.name}')

    def fault(self, kind):
        cycle = self.state['cycle']
        shutdown_result = None
        self.capture('before')
        pending = json.loads((self.evidence / 'pending.json').read_text())
        pending['fault'] = kind
        save(self.evidence / 'pending.json', pending)
        window = dict(cycle=cycle, fault=kind, phase='workload')
        save(self.evidence / 'fault-window.json', window)
        self.guest('systemctl', 'start', 'verfsnext-workload.service')
        progress = self.wait_progress(cycle)
        # Seeded random timing spans probe writes, file fsync, rename, acknowledgement and copy-only traffic.
        rng = random.Random(f"{self.state['seed']}:{cycle}")
        delay = rng.uniform(0.0, 3.0)
        time.sleep(delay)
        progress = json.loads((self.evidence / 'progress.json').read_text())
        window.update(phase='injecting', injection_intent_ns=time.time_ns())
        save(self.evidence / 'fault-window.json', window)
        self.checkpoint(phase='fault_pending', last_fault=kind, fault_progress=progress)
        self.event('fault_intent', fault=kind, workload=progress, delay=delay,
                   expected_sha256=self.run('sha256sum', str(self.evidence / 'expected.json')).split()[0])
        if kind == 'copy_sigkill':
            copy = json.loads((self.evidence / 'copy-process.json').read_text())
            if copy['cycle'] != cycle:
                raise RuntimeError('copy PID belongs to another cycle')
            self.guest('kill', '-KILL', '--', '-' + str(copy['pid']))
            self.wait_workload('copy interruption')
        elif kind in ('daemon_sigkill', 'snapshot_op_kill'):
            self.kill_daemon()
        elif kind == 'recovery_double_kill':
            self.kill_daemon()
            self.guest('systemctl', 'stop', 'verfsnext-workload.service')
            self.guest('systemctl', 'stop', UNIT)
            mounts = self.guest('cat', '/proc/self/mountinfo')
            if any(line.split()[4] == '/mnt/verfsnext' for line in mounts.splitlines()):
                self.guest('fusermount3', '-uz', '/mnt/verfsnext')
            # The second kill lands during metadata/WAL recovery or right after the mount appears.
            result = json.loads(self.guest('python3', self.guest_script, 'restart-kill', '--cycle', str(cycle),
                                           '--delay', f'{rng.uniform(0.02, 1.0):.3f}').splitlines()[-1])
            self.event('recovery_killed', **result)
        elif kind == 'daemon_sigterm':
            self.guest('systemctl', 'stop', UNIT)
            shutdown_result = self.guest('systemctl', 'show', '--property=Result', '--value', UNIT).strip()
            self.event('graceful_shutdown_result', result=shutdown_result)
        elif kind == 'container_reboot':
            self.run('pct', 'shutdown', self.ct, '--timeout', '45')
            self.run('pct', 'start', self.ct)
        elif kind == 'container_hard_stop':
            self.run('pct', 'stop', self.ct)
            self.run('pct', 'start', self.ct)
        elif kind == 'gc_idle_restart':
            # Finish writes, then kill at a random point of the idle GC window.
            self.wait_workload('GC idle window')
            idle = rng.uniform(1.0, 8.0)
            time.sleep(idle)
            self.event('gc_idle_window_elapsed', seconds=idle)
            self.kill_daemon()
        elif kind == 'gc_rewrite_kill':
            # Finish writes, then kill while a pack rewrite temp file exists (the B006 window).
            self.wait_workload('GC rewrite window')
            result = json.loads(self.guest('python3', self.guest_script, 'gc-kill', '--cycle', str(cycle),
                                           timeout=90).splitlines()[-1])
            counter = 'gc_rewrite_hits' if result['hit'] else 'gc_rewrite_misses'
            self.checkpoint(**{counter: self.state[counter] + 1})
            self.event('gc_rewrite_killed', **result)
        self.event('fault_applied', fault=kind)
        window.update(phase='recovering', fault_applied_ns=time.time_ns())
        save(self.evidence / 'fault-window.json', window)
        self.checkpoint(phase='recovering')
        self.mount_fresh()
        window.update(phase='recovered', recovered_ns=time.time_ns())
        save(self.evidence / 'fault-window.json', window)
        self.capture('after')
        self.verify_recovery()
        if shutdown_result is not None and shutdown_result != 'success':
            raise RuntimeError(f'graceful daemon shutdown failed: {shutdown_result}')
        verified = json.loads((self.evidence / f'verified-{cycle:06d}.json').read_text())
        self.state['faults'][kind] = self.state['faults'].get(kind, 0) + 1
        count_key = 'preflight_verifications' if self.state['mode'] == 'preflight' else 'endurance_verifications'
        self.checkpoint(phase='idle', verifications=self.state['verifications'] + 1,
                        last_verified=verified, **{count_key: self.state[count_key] + 1})
        self.event('cycle_passed', fault=kind, verification=verified)
        self.action('cleanup')

    def main(self):
        if self.state['status'] != 'running':
            self.event('run_already_terminal', status=self.state['status'])
            return
        self.event('supervisor_started', configuration=self.state)
        if self.state['phase'] == 'preparing':
            raise RuntimeError('host interrupted checkpoint preparation; run is inconclusive, preserve evidence')
        if self.state['phase'] in ('fault_pending', 'recovering'):
            self.mount_fresh()
            window_path = self.evidence / 'fault-window.json'
            if window_path.exists():
                window = json.loads(window_path.read_text())
                if window['cycle'] == self.state['cycle']:
                    window.update(phase='recovered', recovered_ns=time.time_ns())
                    save(window_path, window)
            self.verify_recovery()
            self.checkpoint(phase='idle')
            self.event('interrupted_cycle_verified')
        else:
            self.mount_fresh()
            self.verify_recovery()
        while self.state['active_seconds'] < self.state['target_seconds']:
            started = time.monotonic()
            if self.limit_cycles and self.state['cycle'] - self.started_cycles >= self.limit_cycles:
                break
            self.capacity()
            cycle = self.state['cycle'] + 1
            kind = FAULTS[(cycle - 1) % len(FAULTS)]
            self.checkpoint(cycle=cycle, phase='preparing', cycle_started_at=time.time())
            self.action('prepare')
            checkpoint = json.loads((self.evidence / 'expected.json').read_text())
            save(self.root / f'checkpoint-{cycle:06d}.json', checkpoint)
            self.checkpoint(phase='fault_pending')
            self.fault(kind)
            # A full read after idle can detect GC damage even when no restart occurred.
            idle = max(0, self.interval - (time.monotonic() - started))
            self.checkpoint(next_cycle_at=time.time() + idle)
            time.sleep(idle)
            self.action('verify')
            elapsed = time.monotonic() - started
            self.checkpoint(active_seconds=self.state['active_seconds'] + elapsed)
        self.action('verify')
        self.capture('final')
        self.guest('systemctl', 'stop', UNIT)
        self.checkpoint(status='completed', phase='stopped', finished_at=time.time())
        self.event('run_completed', result=self.state)

    def fail(self):
        error = traceback.format_exc()
        self.event('run_failed', traceback=error, phase=self.state['phase'])
        self.checkpoint(status='failed', failed_at=time.time(), error=error)
        if self.container_running():
            try:
                self.capture('failure')
            finally:
                self.run('lxc-freeze', '-n', self.ct)
                self.event('container_frozen_for_debugging')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ct', type=int, required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--guest-script', required=True)
    parser.add_argument('--hours', type=float, default=3)
    parser.add_argument('--extend-hours', type=float, default=0)
    parser.add_argument('--interval', type=float, default=120)
    parser.add_argument('--cycles', type=int, default=0)
    parser.add_argument('--arm-only', action='store_true')
    args = parser.parse_args()
    supervisor = Supervisor(args)
    try:
        if args.arm_only:
            supervisor.checkpoint()
            supervisor.event('run_armed')
        else:
            supervisor.main()
    except Exception:
        supervisor.fail()
        raise
