#!/bin/sh
# One-line relay summary for a status line (claude-hud, starship, tmux, PS1).
#
# Reads the status file the controller refreshes in the background, so this
# costs one file read and never touches the network or the model. If no Claude
# session is running there is no controller and therefore no fresh file — this
# prints nothing rather than an error, which is what a status line wants.
#
# Output examples:
#   ⬢ 2 agents · ⚙ 1 job
#   ⬢ 2 agents · ✓ resnet50
#   ⬢ 1/2 agents · ✗ resnet50 (exit 1)
#
#   --format=json   pass the raw file through (for a HUD that parses it itself)
#   --stale-secs=N  treat the file as dead after N seconds (default 120)

set -eu

STATUS_FILE="${RELAY_STATUS_FILE:-${XDG_RUNTIME_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}}/relay/status.json}"
FORMAT="text"
STALE_SECS=120

for arg in "$@"; do
    case "$arg" in
        --format=*)     FORMAT=${arg#*=} ;;
        --stale-secs=*) STALE_SECS=${arg#*=} ;;
        --file=*)       STATUS_FILE=${arg#*=} ;;
        -h|--help)      sed -n '2,16p' "$0"; exit 0 ;;
    esac
done

[ -r "$STATUS_FILE" ] || exit 0

if [ "$FORMAT" = "json" ]; then
    cat "$STATUS_FILE"
    exit 0
fi

# jq is the clean path; without it, fall back to a count that is still useful.
if ! command -v jq >/dev/null 2>&1; then
    agents=$(grep -c '"id":' "$STATUS_FILE" 2>/dev/null || echo 0)
    [ "$agents" -gt 0 ] && printf '⬢ %s agents' "$agents"
    exit 0
fi

now=$(date +%s)
jq -r --argjson now "$now" --argjson stale "$STALE_SECS" '
  # A file older than the stale window means no live session; say nothing.
  if ($now - .updated_at) > $stale then ""
  else
    ( .agents | length ) as $total
    | ( [ .agents[] | select(.last_seen_secs < 45) ] | length ) as $online
    | ( [ .agents[].jobs_running ] | add // 0 ) as $running
    # Only the newest finished job is worth a status line slot.
    | ( [ .agents[].jobs_finished[]? ]
        | sort_by(.finished_at // 0) | reverse | first ) as $last
    | ( if $online == $total then "⬢ \($total) agents"
        else "⬢ \($online)/\($total) agents" end ) as $head
    | ( if $running > 0 then " · ⚙ \($running) job\(if $running > 1 then "s" else "" end)"
        elif $last != null then
          ( $last.label // ($last.job | .[0:8]) ) as $name
          | if $last.state == "succeeded" then " · ✓ \($name)"
            else " · ✗ \($name)\(if $last.exit_code != null then " (exit \($last.exit_code))" else "" end)"
            end
        else "" end ) as $tail
    # Busiest GPU across all agents: one number that answers "is the hardware
    # actually working right now".
    | ( [ .agents[].metrics?.gpus[]?.utilization_pct | select(. != null) ] | max ) as $gpu
    | ( if $gpu != null then " · 🖥 \($gpu)%" else "" end ) as $gpu_part
    | $head + $tail + $gpu_part
  end
' "$STATUS_FILE" 2>/dev/null || true
