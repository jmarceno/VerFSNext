#!/usr/bin/env bash
# Creates the dedicated power-cut VM on the Proxmox node (run once, from the workstation).
# The VM needs its own kernel: dm-log-writes and dm-snapshot cannot be used from an LXC.
set -euo pipefail

VMID=${VMID:-103}
STORAGE=${STORAGE:-local-lvm}
NAME=verfsnext-powercut
KEY=${KEY:-$HOME/.ssh/id_ed25519.pub}
BASE_URL=https://cloud.debian.org/images/cloud/trixie/latest
IMAGE=debian-13-generic-amd64.qcow2

scp -q "$KEY" proxmox:/tmp/powercut-key.pub
ssh proxmox bash -s -- "$VMID" "$STORAGE" "$NAME" "$BASE_URL" "$IMAGE" <<'REMOTE'
set -euo pipefail
VMID=$1 STORAGE=$2 NAME=$3 BASE_URL=$4 IMAGE=$5
if qm status "$VMID" >/dev/null 2>&1; then
    echo "VM $VMID already exists; refusing to touch it" >&2
    exit 1
fi
CACHE=/var/lib/verfsnext-powercut
mkdir -p "$CACHE"
cd "$CACHE"
wget -q -O SHA512SUMS "$BASE_URL/SHA512SUMS"
if [ ! -f "$IMAGE" ] || ! grep " $IMAGE\$" SHA512SUMS | sha512sum -c --quiet -; then
    wget -q -O "$IMAGE.part" "$BASE_URL/$IMAGE"
    mv "$IMAGE.part" "$IMAGE"
    grep " $IMAGE\$" SHA512SUMS | sha512sum -c --quiet -
fi

qm create "$VMID" --name "$NAME" --memory 6144 --cores 4 --cpu host --ostype l26 \
    --net0 virtio,bridge=vmbr0 --scsihw virtio-scsi-single --agent enabled=1 \
    --serial0 socket --vga serial0 --onboot 0
qm set "$VMID" --scsi0 "$STORAGE:0,import-from=$CACHE/$IMAGE,discard=on,iothread=1"
qm resize "$VMID" scsi0 32G
qm set "$VMID" --ide2 "$STORAGE:cloudinit" --boot order=scsi0 --ciuser powercut \
    --sshkeys /tmp/powercut-key.pub --ipconfig0 ip=dhcp
# Origin and base must have the same size. Thin-provisioned: about 92 GiB at most.
for spec in "virtio1 10 pc-origin" "virtio2 24 pc-log" "virtio3 10 pc-base" \
            "virtio4 4 pc-cow" "virtio5 8 pc-log2" "virtio6 4 pc-cow2"; do
    read -r bus size serial <<<"$spec"
    qm set "$VMID" "--$bus" "$STORAGE:$size,discard=on,detect_zeroes=on,serial=$serial"
done
rm -f /tmp/powercut-key.pub
qm start "$VMID"
echo "VM $VMID started; wait for cloud-init, then run scripts/powercut/deploy.sh --setup"
REMOTE
