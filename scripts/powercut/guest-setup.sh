#!/usr/bin/env bash
# One-time setup inside the power-cut VM (run as root by deploy.sh --setup).
set -euo pipefail

# First boot runs apt through cloud-init; wait for it to release the lock. Exit code 2
# is "done, degraded": Proxmox's generated user-data uses a deprecated key.
status=0
cloud-init status --wait >/dev/null || status=$?
if [ "$status" -ne 0 ] && [ "$status" -ne 2 ]; then
    cloud-init status --long >&2
    exit "$status"
fi
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q python3 e2fsprogs fuse3 attr dmsetup util-linux qemu-guest-agent
systemctl enable --now qemu-guest-agent

cat >/etc/modules-load.d/powercut.conf <<'EOF'
dm_log_writes
dm_snapshot
fuse
EOF
modprobe dm_log_writes
modprobe dm_snapshot
modprobe fuse

mkdir -p /etc/powercut /var/lib/powercut /opt/powercut/releases /pc
python3 - <<'EOF'
import json, os
names = ['origin', 'log', 'base', 'cow', 'log2', 'cow2']
devices = {n: f'/dev/disk/by-id/virtio-pc-{n}' for n in names}
for name, path in devices.items():
    if not os.path.exists(path):
        raise SystemExit(f'missing device {path}')
sizes = {n: int(open(f'/sys/class/block/{os.path.basename(os.path.realpath(p))}/size').read())
         for n, p in devices.items()}
if sizes['origin'] != sizes['base']:
    raise SystemExit(f'origin and base differ in size: {sizes}')
json.dump(devices, open('/etc/powercut/devices.json', 'w'), indent=1)
EOF

cat >/etc/systemd/system/powercut.service <<'EOF'
[Unit]
Description=VerFSNext power-cut crash-consistency run
After=local-fs.target

[Service]
Type=simple
EnvironmentFile=/etc/powercut/run.env
ExecStart=/usr/bin/python3 /opt/powercut/current/powercut.py run $POWERCUT_ARGS
Restart=no
LimitNOFILE=65536
EOF
[ -f /etc/powercut/run.env ] || echo 'POWERCUT_ARGS=--hours 6' >/etc/powercut/run.env
systemctl daemon-reload
echo "guest setup done"
