#!/bin/bash
# Detached on Proxmox: preflight -> arm 6h -> start the persistent endurance service.
set -euo pipefail
REL=$1; CYCLES=$2; INTERVAL=$3
R=/var/lib/verfsnext-resilience
SUP=$R/releases/$REL/supervisor.py
GUEST=/opt/verfsnext-resilience/releases/$REL/guest.py
python3 $SUP --ct 101 --root $R/101 --guest-script $GUEST --hours 100 --cycles $CYCLES --interval $INTERVAL
status=$(python3 -c "import json;print(json.load(open('$R/101/status.json'))['status'])")
cp $R/101/status.json $R/101/preflight-completed.json
if [ "$status" != completed ]; then echo "preflight status $status; endurance not armed" >&2; exit 1; fi
python3 $SUP --ct 101 --root $R/101 --guest-script $GUEST --extend-hours 6 --arm-only
systemctl enable verfsnext-resilience-101.service
systemctl start --no-block verfsnext-resilience-101.service
