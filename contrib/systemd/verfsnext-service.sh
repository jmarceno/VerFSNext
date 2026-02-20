#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="verfsnext"
SERVICE_USER="verfs"
SERVICE_GROUP="verfs"
STATE_DIR="/var/lib/verfs"
CONFIG_DIR="/etc/verfsnext"
CONFIG_FILE="$CONFIG_DIR/config.toml"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}.service"
BIN_DST="/usr/local/bin/verfsnext"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
UNIT_SRC="$SCRIPT_DIR/verfsnext.service"
BIN_SRC="$REPO_ROOT/target/release/verfsnext"

if [[ -f "$REPO_ROOT/config.toml" ]]; then
  CONFIG_SRC="$REPO_ROOT/config.toml"
else
  CONFIG_SRC="$REPO_ROOT/config.toml.example"
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--dry-run|-n] <install|update-bin>"
  exit 1
fi

if [[ $EUID -eq 0 ]]; then
  SUDO=()
else
  SUDO=(sudo)
fi

DRY_RUN=0
ACTION=""
VALIDATION_CONFIG_FILE="$CONFIG_FILE"

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

print_group_access_notice() {
  local caller_user="${SUDO_USER:-${USER:-}}"
  local yellow='\033[1;33m'
  local reset='\033[0m'
  echo -e "${yellow}NOTICE:${reset} verfsnext control commands require access to group '${SERVICE_GROUP}'."
  if [[ -n "$caller_user" && "$caller_user" != "root" ]]; then
    echo -e "${yellow}NOTICE:${reset} user '${caller_user}' must be in '${SERVICE_GROUP}' (re-login or run: newgrp ${SERVICE_GROUP})."
  fi
}

grant_caller_group_access() {
  local caller_user="${SUDO_USER:-${USER:-}}"

  if [[ -z "$caller_user" || "$caller_user" == "root" || "$caller_user" == "$SERVICE_USER" ]]; then
    return 0
  fi

  if ! id -u "$caller_user" >/dev/null 2>&1; then
    return 0
  fi

  if id -nG "$caller_user" | grep -qw "$SERVICE_GROUP"; then
    return 0
  fi

  echo "Granting user $caller_user membership in group $SERVICE_GROUP..."
  run_cmd "${SUDO[@]}" usermod -a -G "$SERVICE_GROUP" "$caller_user"
  echo "Group update applied for $caller_user. Re-login or run: newgrp $SERVICE_GROUP"
}

ensure_control_socket_permissions() {
  local data_dir socket_path attempt
  data_dir="$(toml_path_value data_dir "$CONFIG_FILE" || true)"
  if [[ -z "$data_dir" ]]; then
    echo "warning: unable to parse data_dir from $CONFIG_FILE; skipping socket permission adjustment." >&2
    return 0
  fi

  socket_path="$data_dir/verfsnext.sock"
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "Dry run: would ensure $socket_path is owned by group $SERVICE_GROUP with mode 660."
    return 0
  fi

  for attempt in {1..20}; do
    if "${SUDO[@]}" test -S "$socket_path"; then
      break
    fi
    sleep 0.5
  done

  if ! "${SUDO[@]}" test -S "$socket_path"; then
    echo "warning: control socket not found at $socket_path after service start." >&2
    return 0
  fi

  echo "Ensuring control socket group access on $socket_path..."
  run_cmd "${SUDO[@]}" chgrp "$SERVICE_GROUP" "$socket_path"
  run_cmd "${SUDO[@]}" chmod 660 "$socket_path"
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

build_binary() {
  echo "Building release binary..."
  run_cmd cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
  if [[ $DRY_RUN -eq 0 && ! -x "$BIN_SRC" ]]; then
    echo "build did not produce $BIN_SRC" >&2
    exit 1
  fi
}

install_binary() {
  echo "Installing binary to $BIN_DST..."
  run_cmd "${SUDO[@]}" install -Dm755 "$BIN_SRC" "$BIN_DST"
}

ensure_service_user() {
  if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
    echo "Creating group $SERVICE_GROUP..."
    run_cmd "${SUDO[@]}" groupadd --system "$SERVICE_GROUP"
  fi

  if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    echo "Creating user $SERVICE_USER..."
    run_cmd "${SUDO[@]}" useradd \
      --system \
      --home-dir "$STATE_DIR" \
      --shell /usr/sbin/nologin \
      --gid "$SERVICE_GROUP" \
      "$SERVICE_USER"
  fi

  run_cmd "${SUDO[@]}" install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR"
}

install_config() {
  echo "Preparing config at $CONFIG_FILE..."
  run_cmd "${SUDO[@]}" install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CONFIG_DIR"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    run_cmd "${SUDO[@]}" install -m640 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CONFIG_SRC" "$CONFIG_FILE"
    echo "Installed config template from $CONFIG_SRC"
  else
    run_cmd "${SUDO[@]}" chown "$SERVICE_USER:$SERVICE_GROUP" "$CONFIG_FILE"
  fi
}

validate_config_paths() {
  local mount_point data_dir
  local check_write_perms=1
  local caller_user="${SUDO_USER:-${USER:-root}}"
  mount_point="$(toml_path_value mount_point "$VALIDATION_CONFIG_FILE" || true)"
  data_dir="$(toml_path_value data_dir "$VALIDATION_CONFIG_FILE" || true)"

  if [[ -z "$mount_point" || -z "$data_dir" ]]; then
    echo "config validation failed: mount_point and data_dir must be set in $VALIDATION_CONFIG_FILE" >&2
    exit 1
  fi

  if [[ ! -d "$mount_point" ]]; then
    echo "Creating mount_point: $mount_point"
    run_cmd "${SUDO[@]}" install -d -m 0777 -o "$caller_user" "$mount_point"
  fi

  if [[ ! -d "$data_dir" ]]; then
    echo "Creating data_dir: $data_dir"
    run_cmd "${SUDO[@]}" install -d -m 0777 -o "$caller_user" "$data_dir"
  fi

  ensure_subdir_owned "$data_dir/metadata"
  ensure_subdir_owned "$data_dir/packs"

  if [[ $DRY_RUN -eq 1 ]] && ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    check_write_perms=0
    echo "Dry run: skipping write permission checks for user $SERVICE_USER (user would be created during install)."
  fi

  if [[ $check_write_perms -eq 1 ]]; then
    if [[ $DRY_RUN -eq 0 ]] && ! can_user_write_dir "$mount_point"; then
      echo "config validation failed: $SERVICE_USER cannot write mount_point: $mount_point" >&2
      echo "Please fix the permissions for $mount_point so that user $SERVICE_USER can write to it." >&2
      exit 1
    fi

    if [[ $DRY_RUN -eq 0 ]] && ! can_user_write_dir "$data_dir"; then
      echo "config validation failed: $SERVICE_USER cannot write data_dir: $data_dir" >&2
      echo "Please fix the permissions for $data_dir so that user $SERVICE_USER can write to it." >&2
      exit 1
    fi
  fi
}

ensure_subdir_owned() {
  local dir_path=$1
  if [[ ! -d "$dir_path" ]]; then
    echo "Creating directory: $dir_path"
    run_cmd "${SUDO[@]}" install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$dir_path"
    return 0
  fi

  local check_write_perms=1
  if [[ $DRY_RUN -eq 1 ]] && ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    check_write_perms=0
  fi

  if [[ $check_write_perms -eq 1 ]] && [[ $DRY_RUN -eq 0 ]] && ! can_user_write_dir "$dir_path"; then
    echo "config validation failed: $SERVICE_USER cannot write to $dir_path" >&2
    echo "Please fix the permissions for $dir_path so that it is writable." >&2
    exit 1
  fi
}

can_user_write_dir() {
  local target_dir=$1
  if [[ $EUID -eq 0 ]]; then
    if command -v runuser >/dev/null 2>&1; then
      runuser -u "$SERVICE_USER" -- test -w "$target_dir"
    elif command -v sudo >/dev/null 2>&1; then
      sudo -u "$SERVICE_USER" test -w "$target_dir"
    else
      echo "missing required command: runuser (or sudo) for permission validation" >&2
      exit 1
    fi
  else
    "${SUDO[@]}" -u "$SERVICE_USER" test -w "$target_dir"
  fi
}

install_unit() {
  echo "Installing systemd unit..."
  run_cmd "${SUDO[@]}" install -Dm644 "$UNIT_SRC" "$UNIT_DST"
  run_cmd "${SUDO[@]}" systemctl daemon-reload
}

enable_and_start() {
  echo "Enabling and starting ${SERVICE_NAME}.service..."
  run_cmd "${SUDO[@]}" systemctl enable --now "${SERVICE_NAME}.service"
}

restart_if_active() {
  if "${SUDO[@]}" systemctl is-active --quiet "${SERVICE_NAME}.service"; then
    echo "Restarting active service ${SERVICE_NAME}.service..."
    run_cmd "${SUDO[@]}" systemctl restart "${SERVICE_NAME}.service"
  else
    echo "${SERVICE_NAME}.service is not active; binary updated."
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|-n)
      DRY_RUN=1
      ;;
    install|update-bin)
      if [[ -n "$ACTION" ]]; then
        echo "only one action is allowed: install or update-bin" >&2
        exit 1
      fi
      ACTION="$1"
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: $0 [--dry-run|-n] <install|update-bin>" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ -z "$ACTION" ]]; then
  echo "missing action: install or update-bin" >&2
  echo "usage: $0 [--dry-run|-n] <install|update-bin>" >&2
  exit 1
fi

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Dry run enabled. No changes will be made."
  if [[ -r "$CONFIG_FILE" ]]; then
    VALIDATION_CONFIG_FILE="$CONFIG_FILE"
    echo "Dry run: validating using installed config $VALIDATION_CONFIG_FILE"
  else
    VALIDATION_CONFIG_FILE="$CONFIG_SRC"
    echo "Dry run: validating using source config $VALIDATION_CONFIG_FILE"
  fi
fi

need_cmd cargo
need_cmd install
need_cmd systemctl
need_cmd sed
need_cmd grep
need_cmd getent
need_cmd id
need_cmd chgrp
need_cmd chmod
if [[ $EUID -ne 0 ]]; then
  need_cmd sudo
fi

if ! getent group fuse >/dev/null 2>&1; then
  echo "warning: group 'fuse' not found; service may fail if /dev/fuse is not world-accessible." >&2
fi

case "$ACTION" in
  install)
    need_cmd useradd
    need_cmd groupadd
    need_cmd usermod
    build_binary
    install_binary
    ensure_service_user
    print_group_access_notice
    grant_caller_group_access
    if getent group fuse >/dev/null 2>&1; then
      run_cmd "${SUDO[@]}" usermod -a -G fuse "$SERVICE_USER"
    fi
    install_config
    validate_config_paths
    install_unit
    enable_and_start
    ensure_control_socket_permissions
    echo "Done. Check status with: ${SUDO[*]} systemctl status ${SERVICE_NAME}"
    ;;
  update-bin)
    if [[ ! -f "$UNIT_DST" || ! -x "$BIN_DST" ]]; then
      echo "VerFSNext service is not installed. Run: $0 install" >&2
      exit 1
    fi
    build_binary
    install_binary
    restart_if_active
    ensure_control_socket_permissions
    ;;
  *)
    echo "unknown command: $ACTION" >&2
    echo "usage: $0 [--dry-run|-n] <install|update-bin>" >&2
    exit 1
    ;;
esac
