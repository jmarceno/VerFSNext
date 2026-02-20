#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="verfsnext"
SERVICE_USER="verfs"
SERVICE_GROUP="verfs"
STATE_DIR="/var/lib/verfs"
CONFIG_DIR="/etc/verfsnext"
CONFIG_FILE="$CONFIG_DIR/config.toml"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
BIN_FILE="/usr/local/bin/verfsnext"

DRY_RUN=0

if [[ $EUID -eq 0 ]]; then
  SUDO=()
else
  SUDO=(sudo)
fi

print_cmd() {
  local out=""
  local part
  for part in "$@"; do
    out+=" $(printf '%q' "$part")"
  done
  printf '+%s\n' "$out"
}

run_cmd() {
  print_cmd "$@"
  if [[ $DRY_RUN -eq 0 ]]; then
    "$@"
  fi
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

toml_path_value() {
  local key=$1
  local file=$2
  if [[ -r "$file" ]]; then
    sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*\"\\([^\"]\\+\\)\"[[:space:]]*\\(#.*\\)\?$/\\1/p" "$file" | head -n1
  elif [[ ${#SUDO[@]} -gt 0 ]]; then
    "${SUDO[@]}" sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*\"\\([^\"]\\+\\)\"[[:space:]]*\\(#.*\\)\?$/\\1/p" "$file" | head -n1
  else
    return 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|-n)
      DRY_RUN=1
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: $0 [--dry-run|-n]" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Dry run enabled. No changes will be made."
fi

need_cmd systemctl
need_cmd install
need_cmd rm
need_cmd sed
need_cmd id
need_cmd getent
if [[ $EUID -ne 0 ]]; then
  need_cmd sudo
fi

data_dir=""
if [[ -f "$CONFIG_FILE" ]]; then
  data_dir="$(toml_path_value data_dir "$CONFIG_FILE" || true)"
fi

if [[ -n "$data_dir" ]]; then
  echo "Preserving data_dir: $data_dir"
else
  echo "No readable data_dir found in $CONFIG_FILE. Data directories are not removed by this script."
fi

if "${SUDO[@]}" systemctl list-unit-files | sed -n "s/^${SERVICE_NAME}\\.service[[:space:]].*/found/p" | head -n1 | grep -q "found"; then
  run_cmd "${SUDO[@]}" systemctl disable --now "${SERVICE_NAME}.service" || true
else
  echo "Service unit ${SERVICE_NAME}.service not registered, skipping disable/stop."
fi

run_cmd "${SUDO[@]}" systemctl reset-failed "${SERVICE_NAME}.service" || true

if [[ -f "$UNIT_FILE" ]]; then
  run_cmd "${SUDO[@]}" rm -f "$UNIT_FILE"
fi
run_cmd "${SUDO[@]}" systemctl daemon-reload

if [[ -f "$BIN_FILE" ]]; then
  run_cmd "${SUDO[@]}" rm -f "$BIN_FILE"
fi

if [[ -d "$CONFIG_DIR" ]]; then
  run_cmd "${SUDO[@]}" rm -rf "$CONFIG_DIR"
fi

if [[ -d "$STATE_DIR" ]]; then
  run_cmd "${SUDO[@]}" rm -rf "$STATE_DIR"
fi

if id -u "$SERVICE_USER" >/dev/null 2>&1; then
  run_cmd "${SUDO[@]}" userdel "$SERVICE_USER" || true
fi

if getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
  run_cmd "${SUDO[@]}" groupdel "$SERVICE_GROUP" || true
fi

echo "Uninstall completed."
echo "Preserved data_dir: ${data_dir:-<unknown>}"
