#!/usr/bin/env bash
# Deploys the harness and a headless daemon build to the power-cut VM, and starts runs.
#
#   deploy.sh --setup                        one-time package/device/unit setup in the VM
#   deploy.sh [--binary PATH] [--label L]    install a release (default: build this checkout)
#   deploy.sh --start "--hours 6"            start powercut.service with these arguments
#   deploy.sh --status                       print the run status
set -euo pipefail

VMID=${VMID:-103}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)

vm_ip() {
    # The VM's DHCP address, found by its MAC in the node's neighbour table after a sweep
    # (works before the guest agent is installed).
    ssh proxmox "MAC=\$(qm config $VMID | sed -n 's/^net0: virtio=\([^,]*\),.*/\1/p'); \
        for i in \$(seq 1 254); do ping -c1 -W1 192.168.0.\$i >/dev/null 2>&1 & done; wait; \
        ip -4 neigh | grep -i \"lladdr \$MAC\" | cut -d' ' -f1 | head -1" | grep . \
        || { echo "VM $VMID has no address in the neighbour table" >&2; exit 1; }
}

HOST=${POWERCUT_HOST:-}
if [ -z "$HOST" ]; then
    HOST=powercut@$(vm_ip)
fi
SSH=(ssh -o StrictHostKeyChecking=accept-new "$HOST")

case "${1:-}" in
--setup)
    scp -q -o StrictHostKeyChecking=accept-new "$HERE/guest-setup.sh" "$HOST:/tmp/guest-setup.sh"
    "${SSH[@]}" sudo bash /tmp/guest-setup.sh
    exit 0
    ;;
--start)
    "${SSH[@]}" "echo 'POWERCUT_ARGS=$2' | sudo tee /etc/powercut/run.env >/dev/null && sudo systemctl start powercut.service"
    "${SSH[@]}" systemctl status powercut.service --no-pager | head -5
    exit 0
    ;;
--status)
    "${SSH[@]}" sudo cat /var/lib/powercut/status.json
    exit 0
    ;;
esac

BINARY=""
LABEL=""
while [ $# -gt 0 ]; do
    case "$1" in
    --binary) BINARY=$2; shift 2 ;;
    --label) LABEL=$2; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
    esac
done

REV=$(git -C "$REPO" rev-parse --short HEAD)
if [ -z "$BINARY" ]; then
    if [ -n "$(git -C "$REPO" status --porcelain -- src crates Cargo.toml Cargo.lock)" ]; then
        echo "uncommitted daemon changes; commit them or pass --binary" >&2
        exit 1
    fi
    (cd "$REPO" && cargo remote -h -- build --profile headless --no-default-features)
    BINARY=$REPO/target/headless/verfsnext
    LABEL=${LABEL:-$REV}
fi
if [ -z "$LABEL" ]; then
    echo "--label is required with --binary" >&2
    exit 2
fi
RELEASE=$LABEL-harness-$(sha256sum "$HERE/powercut.py" | cut -c1-8)
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
cp "$BINARY" "$STAGE/verfsnext"
cp "$HERE/powercut.py" "$STAGE/powercut.py"
python3 - "$STAGE" "$LABEL" "$REV" <<'EOF'
import hashlib, json, sys
from pathlib import Path
stage, label, rev = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.iterdir()}
json.dump(dict(label=label, harness_revision=rev, sha256=files), open(stage / 'provenance.json', 'w'), indent=1)
EOF
"${SSH[@]}" "systemctl is-active --quiet powercut.service && { echo 'powercut.service is running' >&2; exit 1; } || true"
"${SSH[@]}" "mkdir -p /tmp/powercut-release && rm -rf /tmp/powercut-release/*"
scp -q "$STAGE"/* "$HOST:/tmp/powercut-release/"
"${SSH[@]}" "sudo rm -rf /opt/powercut/releases/$RELEASE && sudo mkdir -p /opt/powercut/releases/$RELEASE && \
    sudo cp /tmp/powercut-release/* /opt/powercut/releases/$RELEASE/ && \
    sudo chmod 755 /opt/powercut/releases/$RELEASE/verfsnext && \
    sudo ln -sfn /opt/powercut/releases/$RELEASE /opt/powercut/current"
echo "installed release $RELEASE on $HOST"
