#!/bin/sh
# Install s3-relay-mcp on the machine that runs Claude Code.
#
# POSIX sh, so bash / zsh / dash / busybox ash all work and your interactive
# shell does not matter.
#
# Unlike the agent, the controller is not a service: Claude Code spawns it as a
# stdio child process on demand and it exits with the session. There is nothing
# to enable or start — registering it with Claude Code IS the autostart. It must
# NOT be run under systemd: stdio is a one-to-one pipe, and a resident copy
# would have nobody on the other end.
#
# User-level by default, no root needed.
#
#   sh install-controller.sh --agents legacy-01,legacy-02 \
#      --endpoint https://cn-sy1.rains3.com --bucket legacy-control-relay \
#      --access-key AKxxx --secret-key SKxxx --shared-key BASE64KEY
#
# Re-running never overwrites an existing config or key, so this is also the
# upgrade path for a new binary.

set -eu

BIN_SRC=""
SCOPE="user"
BIN_DIR=""
CONF_DIR=""
AGENTS=""
ENDPOINT=""
REGION=""
BUCKET=""
S3_PREFIX="relay-prod/"
ACCESS_KEY=""
SECRET_KEY=""
SHARED_KEY=""
KEY_ID="primary"
SERVER_NAME="s3-relay"
MCP_SCOPE="user"
REGISTER=1
LOG_FILE=""

usage() {
    cat <<'USAGE'
Usage: sh install-controller.sh [options]

Required (prompted for if omitted and stdin is a terminal):
  --agents LIST          Comma-separated agent ids this controller may reach.
                         Must match each agent's own id.
  --endpoint URL         S3 endpoint, e.g. https://cn-sy1.rains3.com
  --bucket NAME          Bucket name
  --access-key ID        S3 access key id for the CONTROLLER (not an agent's)
  --secret-key SECRET    S3 secret access key

Optional:
  --shared-key BASE64    32-byte relay key, base64. Must be identical to the
                         value on every agent. Generated if omitted.
  --key-id NAME          Label for the shared key (default: primary)
  --region NAME          S3 region (default: derived from the endpoint host)
  --s3-prefix PREFIX     Key namespace in the bucket (default: relay-prod/)
  --binary PATH          s3-relay-mcp to install (default: ./s3-relay-mcp, then
                         ./target/release/s3-relay-mcp)
  --scope user|system    user: ~/.local/bin + ~/.config/relay (default, no root)
                         system: /usr/local/bin + /etc/relay (needs root)
  --name NAME            MCP server name (default: s3-relay)
  --mcp-scope SCOPE      Scope passed to `claude mcp add` (default: user)
  --log-file PATH        Controller log (default: ~/.local/state/relay/
                         controller.log, or /var/log/relay/ under --scope system)
  --no-register          Install only; print the MCP config instead of adding it
  -h, --help             This text

The controller has no capability switches of its own. What an agent will accept
is decided on the agent side; what gets sent is decided by Claude Code.
USAGE
}

die() {
    echo "install-controller: $*" >&2
    exit 1
}

need_value() {
    [ -n "${2:-}" ] || die "$1 requires a value"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --agents)     need_value "$1" "${2:-}"; AGENTS=$2; shift 2 ;;
        --endpoint)   need_value "$1" "${2:-}"; ENDPOINT=$2; shift 2 ;;
        --region)     need_value "$1" "${2:-}"; REGION=$2; shift 2 ;;
        --bucket)     need_value "$1" "${2:-}"; BUCKET=$2; shift 2 ;;
        --s3-prefix)  need_value "$1" "${2:-}"; S3_PREFIX=$2; shift 2 ;;
        --access-key) need_value "$1" "${2:-}"; ACCESS_KEY=$2; shift 2 ;;
        --secret-key) need_value "$1" "${2:-}"; SECRET_KEY=$2; shift 2 ;;
        --shared-key) need_value "$1" "${2:-}"; SHARED_KEY=$2; shift 2 ;;
        --key-id)     need_value "$1" "${2:-}"; KEY_ID=$2; shift 2 ;;
        --binary)     need_value "$1" "${2:-}"; BIN_SRC=$2; shift 2 ;;
        --scope)      need_value "$1" "${2:-}"; SCOPE=$2; shift 2 ;;
        --name)       need_value "$1" "${2:-}"; SERVER_NAME=$2; shift 2 ;;
        --mcp-scope)  need_value "$1" "${2:-}"; MCP_SCOPE=$2; shift 2 ;;
        --log-file)   need_value "$1" "${2:-}"; LOG_FILE=$2; shift 2 ;;
        --no-register) REGISTER=0; shift ;;
        -h|--help)    usage; exit 0 ;;
        *)            usage >&2; die "unknown option $1" ;;
    esac
done

case "$SCOPE" in
    user)
        BIN_DIR="$HOME/.local/bin"
        CONF_DIR="$HOME/.config/relay"
        [ -n "$LOG_FILE" ] || LOG_FILE="${XDG_STATE_HOME:-$HOME/.local/state}/relay/controller.log"
        ;;
    system)
        [ "$(id -u)" = "0" ] || die "--scope system needs root (try sudo)"
        BIN_DIR="/usr/local/bin"
        CONF_DIR="/etc/relay"
        [ -n "$LOG_FILE" ] || LOG_FILE="/var/log/relay/controller.log"
        ;;
    *)
        die "--scope must be user or system"
        ;;
esac

# --- locate and sanity check the binary ------------------------------------

if [ -z "$BIN_SRC" ]; then
    for candidate in ./s3-relay-mcp ./target/release/s3-relay-mcp; do
        if [ -f "$candidate" ]; then BIN_SRC=$candidate; break; fi
    done
fi
[ -n "$BIN_SRC" ] || die "no s3-relay-mcp found; pass --binary PATH"
[ -f "$BIN_SRC" ] || die "$BIN_SRC does not exist"

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

CONF_FILE="$CONF_DIR/controller.toml"
CONFIG_EXISTS=0
[ -f "$CONF_FILE" ] && CONFIG_EXISTS=1

if [ "$CONFIG_EXISTS" = "0" ]; then
    prompt AGENTS     "Agent ids this controller may reach (comma separated)"
    prompt ENDPOINT   "S3 endpoint URL (e.g. https://cn-sy1.rains3.com)"
    prompt BUCKET     "Bucket name"
    prompt ACCESS_KEY "S3 access key id for the controller"
    prompt SECRET_KEY "S3 secret access key" secret

    case "$ENDPOINT" in
        https://*) : ;;
        http://*)  die "endpoint must be https:// (plaintext is refused by default)" ;;
        *)         die "endpoint must start with https://" ;;
    esac

    if [ -z "$REGION" ]; then
        REGION=$(printf '%s' "$ENDPOINT" | sed -e 's|^https://||' -e 's|/.*$||' -e 's|\..*$||')
        [ -n "$REGION" ] || REGION="us-east-1"
    fi

    case "$S3_PREFIX" in
        */) : ;;
        *)  S3_PREFIX="$S3_PREFIX/" ;;
    esac
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

# --- install ----------------------------------------------------------------

mkdir -p "$BIN_DIR" "$CONF_DIR"
install -m 0755 "$BIN_SRC" "$BIN_DIR/s3-relay-mcp"
echo "installed $BIN_DIR/s3-relay-mcp"

if [ "$CONFIG_EXISTS" = "1" ]; then
    echo "kept existing $CONF_FILE (delete it to regenerate)"
else
    toml_agents=$(printf '%s' "$AGENTS" | sed -e 's/[[:space:]]//g' -e 's/,/", "/g')
    umask 077
    cat >"$CONF_FILE" <<EOF
# Generated by install-controller.sh.
#
# This file holds credentials, so its mode (0600) is what protects them. The
# agent warns on startup if it is ever loosened. Do not commit it.

[s3]
endpoint         = "$ENDPOINT"
region           = "$REGION"
bucket           = "$BUCKET"
prefix           = "$S3_PREFIX"
force_path_style = true
access_key_id     = "$ACCESS_KEY"
secret_access_key = "$SECRET_KEY"

[relay]
key_id     = "$KEY_ID"
# Every agent must carry this exact value.
shared_key = "$SHARED_KEY"

[controller]
# An agent missing from this list stays invisible to list_agents even when its
# heartbeat is healthy. Adding one means editing this file and restarting the
# MCP server, since it is read once at startup.
allowed_agents = ["$toml_agents"]

queue_ttl_secs = 120
max_exec_secs  = 300
max_wait_secs  = 430

# Transfers get their own ceiling. Every agent's max_timeout_secs must be at
# least this large, or it will reject push_file / pull_file as out of policy.
max_transfer_secs = 1800

poll_ms = 200
EOF
    chmod 0600 "$CONF_FILE"
    echo "wrote $CONF_FILE (mode 0600)"
fi

# --- register with Claude Code ----------------------------------------------
#
# The config path travels as an environment variable in the MCP entry, so no
# launcher script is needed: Claude spawns the binary directly.

MCP_BIN="$BIN_DIR/s3-relay-mcp"

REGISTERED=0
if [ "$REGISTER" = "1" ] && command -v claude >/dev/null 2>&1; then
    # Remove first so a re-run updates rather than failing on a duplicate.
    claude mcp remove "$SERVER_NAME" -s "$MCP_SCOPE" >/dev/null 2>&1 || true
    if claude mcp add "$SERVER_NAME" -s "$MCP_SCOPE" \
         -e "RELAY_CONFIG=$CONF_FILE" -e "RELAY_LOG_FILE=$LOG_FILE" \
         -- "$MCP_BIN" >/dev/null 2>&1; then
        REGISTERED=1
        echo "registered MCP server '$SERVER_NAME' (scope: $MCP_SCOPE)"
    else
        echo "could not register automatically; add it by hand (see below)" >&2
    fi
fi

echo
if [ "$REGISTERED" = "0" ]; then
    echo "Add this to your Claude MCP config:"
    echo
    printf '  "%s": {\n' "$SERVER_NAME"
    printf '    "command": "%s",\n' "$MCP_BIN"
    printf '    "env": {\n'
    printf '      "RELAY_CONFIG": "%s",\n' "$CONF_FILE"
    printf '      "RELAY_LOG_FILE": "%s"\n' "$LOG_FILE"
    printf '    }\n'
    printf '  }\n'
    echo
    echo "or run:"
    echo
    echo "  claude mcp add $SERVER_NAME -s $MCP_SCOPE \\"
    echo "    -e RELAY_CONFIG=$CONF_FILE -e RELAY_LOG_FILE=$LOG_FILE -- $MCP_BIN"
    echo
fi
echo "Log file: $LOG_FILE"

if [ "$GENERATED_KEY" = "1" ]; then
    echo "A shared key was generated. EVERY agent needs this exact value, and"
    echo "this is the only time it is printed:"
    echo
    echo "  RELAY_SHARED_KEY=$SHARED_KEY"
    echo
    echo "Pass it to install-agent.sh with --shared-key."
    echo
fi

case ":$PATH:" in
    *":$BIN_DIR:"*) : ;;
    *) echo "note: $BIN_DIR is not in PATH. That is fine — the MCP config uses"
       echo "      an absolute path — but you will not be able to run the"
       echo "      binaries by name from a shell." ;;
esac

echo "Then restart Claude Code and call list_agents to confirm the link."
