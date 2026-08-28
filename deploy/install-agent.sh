#!/bin/sh
# Install relay-agent on a Linux host.
#
# POSIX sh on purpose: this runs the same under bash, zsh, dash and busybox ash,
# and your interactive shell (fish included) is irrelevant because of the
# shebang. Supports systemd and OpenRC, and prints manual instructions for
# anything else.
#
# Re-running is safe: an existing config and key are never overwritten, so this
# doubles as the upgrade path for a new binary.
#
#   sudo sh install-agent.sh --agent-id legacy-01 \
#        --endpoint https://cn-sy1.rains3.com --bucket legacy-control-relay \
#        --access-key AKxxx --secret-key SKxxx --shared-key BASE64KEY
#
# Omitted values are prompted for when running on a terminal. --shared-key is
# generated if absent, and printed so you can configure the controller with it.

set -eu

BIN_SRC=""
BIN_DIR="/usr/local/bin"
CONF_DIR="/etc/relay"
AGENT_ID=""
ENDPOINT=""
REGION=""
BUCKET=""
S3_PREFIX="relay-prod/"
ACCESS_KEY=""
SECRET_KEY=""
SHARED_KEY=""
KEY_ID="primary"
MODE="full"
RUN_AS=""
ALLOWED_ROOTS=""
NO_START=0

usage() {
    cat <<'USAGE'
Usage: sudo sh install-agent.sh [options]

Required (prompted for if omitted and stdin is a terminal):
  --agent-id ID          Unique id for this machine; must also appear in the
                         controller's allowed_agents
  --endpoint URL         S3 endpoint, e.g. https://cn-sy1.rains3.com
  --bucket NAME          Bucket name
  --access-key ID        S3 access key id for THIS agent (not the controller's)
  --secret-key SECRET    S3 secret access key

Optional:
  --shared-key BASE64    32-byte relay key, base64. Generated if omitted.
  --key-id NAME          Label for the shared key (default: primary)
  --region NAME          S3 region (default: derived from the endpoint host)
  --s3-prefix PREFIX     Key namespace in the bucket (default: relay-prod/)
  --binary PATH          relay-agent to install (default: ./relay-agent, then
                         ./target/release/relay-agent)
  --bin-dir DIR          Install directory (default: /usr/local/bin)
  --conf-dir DIR         Config directory (default: /etc/relay)
  --restricted           Confine the agent: dedicated unprivileged user, no
                         shell access, file operations limited to --allowed-roots
  --allowed-roots LIST   Colon-separated roots for --restricted mode
  --run-as USER          Override the service user
  --no-start             Install and enable, but do not start
  -h, --help             This text

Default mode is full capability: runs as root, exec can run any program, file
operations are not confined. That matches a host where the controller side is
the trust boundary. Use --restricted for a shared or higher-risk machine.
USAGE
}

die() {
    echo "install-agent: $*" >&2
    exit 1
}

need_value() {
    [ -n "${2:-}" ] || die "$1 requires a value"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --agent-id)      need_value "$1" "${2:-}"; AGENT_ID=$2; shift 2 ;;
        --endpoint)      need_value "$1" "${2:-}"; ENDPOINT=$2; shift 2 ;;
        --region)        need_value "$1" "${2:-}"; REGION=$2; shift 2 ;;
        --bucket)        need_value "$1" "${2:-}"; BUCKET=$2; shift 2 ;;
        --s3-prefix)     need_value "$1" "${2:-}"; S3_PREFIX=$2; shift 2 ;;
        --access-key)    need_value "$1" "${2:-}"; ACCESS_KEY=$2; shift 2 ;;
        --secret-key)    need_value "$1" "${2:-}"; SECRET_KEY=$2; shift 2 ;;
        --shared-key)    need_value "$1" "${2:-}"; SHARED_KEY=$2; shift 2 ;;
        --key-id)        need_value "$1" "${2:-}"; KEY_ID=$2; shift 2 ;;
        --binary)        need_value "$1" "${2:-}"; BIN_SRC=$2; shift 2 ;;
        --bin-dir)       need_value "$1" "${2:-}"; BIN_DIR=$2; shift 2 ;;
        --conf-dir)      need_value "$1" "${2:-}"; CONF_DIR=$2; shift 2 ;;
        --allowed-roots) need_value "$1" "${2:-}"; ALLOWED_ROOTS=$2; shift 2 ;;
        --run-as)        need_value "$1" "${2:-}"; RUN_AS=$2; shift 2 ;;
        --restricted)    MODE="restricted"; shift ;;
        --no-start)      NO_START=1; shift ;;
        -h|--help)       usage; exit 0 ;;
        *)               usage >&2; die "unknown option $1" ;;
    esac
done

[ "$(id -u)" = "0" ] || die "must run as root (try: sudo sh $0 ...)"

if [ -z "$RUN_AS" ]; then
    if [ "$MODE" = "restricted" ]; then RUN_AS="s3-relay"; else RUN_AS="root"; fi
fi

# --- locate and sanity check the binary ------------------------------------

if [ -z "$BIN_SRC" ]; then
    for candidate in ./relay-agent ./target/release/relay-agent; do
        if [ -f "$candidate" ]; then BIN_SRC=$candidate; break; fi
    done
fi
[ -n "$BIN_SRC" ] || die "no relay-agent found; pass --binary PATH"
[ -f "$BIN_SRC" ] || die "$BIN_SRC does not exist"

# Catch the classic mistake of copying over a binary for the wrong CPU, whose
# native error ("Exec format error") explains nothing. Byte 18 of an ELF header
# is e_machine on every little-endian target we care about.
if command -v od >/dev/null 2>&1; then
    elf_machine=$(od -An -t u1 -j 18 -N 1 "$BIN_SRC" 2>/dev/null | tr -d ' \n' || true)
    case "$(uname -m)" in
        x86_64|amd64)  want_machine=62 ;;
        aarch64|arm64) want_machine=183 ;;
        *)             want_machine="" ;;
    esac
    if [ -n "$want_machine" ] && [ -n "$elf_machine" ] && [ "$elf_machine" != "$want_machine" ]; then
        die "$BIN_SRC is not built for $(uname -m); rebuild it for this architecture"
    fi
fi

# --- collect settings -------------------------------------------------------

prompt() {
    # prompt VARNAME "question" [secret]
    prompt_current=$(eval "printf '%s' \"\${$1}\"")
    [ -z "$prompt_current" ] || return 0
    [ -t 0 ] || die "$1 is required; pass the matching option or run on a terminal"
    printf '%s: ' "$2" >&2
    if [ "${3:-}" = "secret" ] && command -v stty >/dev/null 2>&1; then
        stty -echo 2>/dev/null || true
        read -r prompt_value
        stty echo 2>/dev/null || true
        printf '\n' >&2
    else
        read -r prompt_value
    fi
    [ -n "$prompt_value" ] || die "$1 must not be empty"
    eval "$1=\$prompt_value"
}

CONF_FILE="$CONF_DIR/relay.toml"
CONFIG_EXISTS=0
[ -f "$CONF_FILE" ] && CONFIG_EXISTS=1

if [ "$CONFIG_EXISTS" = "0" ]; then
    prompt AGENT_ID   "Agent id for this machine (e.g. legacy-01)"
    prompt ENDPOINT   "S3 endpoint URL (e.g. https://cn-sy1.rains3.com)"
    prompt BUCKET     "Bucket name"
    prompt ACCESS_KEY "S3 access key id for this agent"
    prompt SECRET_KEY "S3 secret access key" secret

    case "$ENDPOINT" in
        https://*) : ;;
        http://*)  die "endpoint must be https:// (the agent refuses plaintext by default)" ;;
        *)         die "endpoint must start with https://" ;;
    esac

    # Most S3-compatible endpoints put the region first in the host name
    # (cn-sy1.rains3.com), which is a better guess than us-east-1.
    if [ -z "$REGION" ]; then
        REGION=$(printf '%s' "$ENDPOINT" | sed -e 's|^https://||' -e 's|/.*$||' -e 's|\..*$||')
        [ -n "$REGION" ] || REGION="us-east-1"
    fi

    case "$S3_PREFIX" in
        */) : ;;
        *)  S3_PREFIX="$S3_PREFIX/" ;;
    esac

    if [ "$MODE" = "restricted" ] && [ -z "$ALLOWED_ROOTS" ]; then
        prompt ALLOWED_ROOTS "Colon-separated allowed roots (e.g. /srv/app:/var/log/app)"
    fi
fi

GENERATED_KEY=0
if [ "$CONFIG_EXISTS" = "0" ] && [ -z "$SHARED_KEY" ]; then
    if [ -r /dev/urandom ] && command -v base64 >/dev/null 2>&1; then
        SHARED_KEY=$(head -c 32 /dev/urandom | base64 | tr -d '\n')
        GENERATED_KEY=1
    else
        die "cannot generate a shared key here; pass --shared-key BASE64"
    fi
fi

# --- service account --------------------------------------------------------

if [ "$RUN_AS" != "root" ]; then
    if ! id "$RUN_AS" >/dev/null 2>&1; then
        if command -v useradd >/dev/null 2>&1; then
            useradd --system --no-create-home --shell /usr/sbin/nologin "$RUN_AS"
        elif command -v adduser >/dev/null 2>&1; then
            # busybox / Alpine
            adduser -S -D -H -s /sbin/nologin "$RUN_AS"
        else
            die "cannot create user $RUN_AS; create it manually or use --run-as root"
        fi
        echo "created service user $RUN_AS"
    fi
fi

# --- install ----------------------------------------------------------------

mkdir -p "$BIN_DIR" "$CONF_DIR"
install -m 0755 "$BIN_SRC" "$BIN_DIR/relay-agent"
echo "installed $BIN_DIR/relay-agent"

if [ "$CONFIG_EXISTS" = "1" ]; then
    echo "kept existing $CONF_FILE (delete it to regenerate)"
else
    if [ "$MODE" = "restricted" ]; then
        capability_block="allow_any_path    = false
allow_any_program = false

# exec may only run these exact absolute paths, and no shell is involved.
allowed_programs = []

# File operations are confined to these roots.
allowed_roots = ["$(printf '%s' "$ALLOWED_ROOTS" | sed 's|:|", "|g')"]"
    else
        capability_block="# Full capability: this agent can do what a local shell could. The trust
# boundary is the controller side, where Claude Code gates every call.
allow_any_path    = true
allow_any_program = true"
    fi

    umask 077
    cat >"$CONF_FILE" <<EOF
# Generated by install-agent.sh.
#
# This file holds credentials, so its mode (0600) is what protects them. The
# agent warns on startup if it is ever loosened. Do not commit it.

[s3]
endpoint          = "$ENDPOINT"
region            = "$REGION"
bucket            = "$BUCKET"
prefix            = "$S3_PREFIX"
force_path_style  = true
access_key_id     = "$ACCESS_KEY"
secret_access_key = "$SECRET_KEY"

[relay]
key_id     = "$KEY_ID"
# Must be byte-identical to the controller's value.
shared_key = "$SHARED_KEY"

[agent]
id = "$AGENT_ID"

$capability_block

# Raise max_timeout_secs together with the controller's max_transfer_secs if
# you push large files: a transfer is one command and is bounded by both.
max_timeout_secs = 1800
max_blob_bytes   = 1073741824

poll_ms            = 200
poll_max_ms        = 5000
full_scan_secs     = 60
doorbell           = true
heartbeat_secs     = 15
heartbeat_ttl_secs = 45
EOF
    chmod 0600 "$CONF_FILE"
    echo "wrote $CONF_FILE (mode 0600)"
fi

[ "$RUN_AS" = "root" ] || chown -R "$RUN_AS" "$CONF_DIR"

# --- service manager --------------------------------------------------------

if [ -d /run/systemd/system ]; then
    INIT="systemd"
elif command -v rc-update >/dev/null 2>&1; then
    INIT="openrc"
else
    INIT="none"
fi

case "$INIT" in
systemd)
    if [ "$MODE" = "restricted" ]; then
        hardening="NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$(printf '%s' "$ALLOWED_ROOTS" | tr ':' ' ')"
    else
        # Deliberately unsandboxed: confining the unit here would silently undo
        # allow_any_path and produce confusing permission errors instead.
        hardening="# No sandboxing: this unit is intentionally full-capability."
    fi
    cat >/etc/systemd/system/relay-agent.service <<EOF
[Unit]
Description=S3 MCP Relay Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_AS
Environment=RELAY_CONFIG=$CONF_FILE
ExecStart=$BIN_DIR/relay-agent
Restart=on-failure
RestartSec=5
$hardening

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable relay-agent >/dev/null 2>&1 || systemctl enable relay-agent
    if [ "$NO_START" = "0" ]; then
        systemctl restart relay-agent
        sleep 1
        if systemctl is-active --quiet relay-agent; then
            echo "relay-agent is running"
        else
            echo "relay-agent failed to start; recent log:" >&2
            journalctl -u relay-agent -n 20 --no-pager >&2 || true
            exit 1
        fi
    fi
    ;;
openrc)
    cat >/etc/init.d/relay-agent <<EOF
#!/sbin/openrc-run
name="relay-agent"
description="S3 MCP Relay Agent"
command="$BIN_DIR/relay-agent"
command_user="$RUN_AS"
command_background=true
pidfile="/run/relay-agent.pid"
output_log="/var/log/relay-agent.log"
error_log="/var/log/relay-agent.log"

depend() {
    need net
}

start_pre() {
    export RELAY_CONFIG="$CONF_FILE"
}
EOF
    chmod 0755 /etc/init.d/relay-agent
    rc-update add relay-agent default >/dev/null 2>&1 || rc-update add relay-agent default
    if [ "$NO_START" = "0" ]; then
        rc-service relay-agent restart
        echo "relay-agent started (log: /var/log/relay-agent.log)"
    fi
    ;;
none)
    echo "no systemd or OpenRC detected; run it yourself with:" >&2
    echo "  RELAY_CONFIG=$CONF_FILE $BIN_DIR/relay-agent" >&2
    ;;
esac

# --- what to do next --------------------------------------------------------

echo
echo "Agent id: ${AGENT_ID:-<from existing config>}"
if [ "$GENERATED_KEY" = "1" ]; then
    echo
    echo "A shared key was generated. The controller needs the SAME value, and"
    echo "this is the only time it is printed:"
    echo
    echo "  RELAY_SHARED_KEY=$SHARED_KEY"
    echo
fi
echo "On the controller side, add this agent to allowed_agents and restart the"
echo "MCP server, then check it with list_agents."
