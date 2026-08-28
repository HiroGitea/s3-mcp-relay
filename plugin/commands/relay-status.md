---
description: Show every relay agent, its jobs, and any recent errors
allowed-tools: Bash(cat:*), Bash(test:*), mcp__s3-relay__list_agents
---

Report the state of every relay agent.

Prefer the status file, which the controller refreshes every 30 seconds without
needing a tool call:

!`cat "${RELAY_STATUS_FILE:-${XDG_RUNTIME_DIR:-$HOME/.cache}/relay/status.json}" 2>/dev/null || echo '{"unavailable":true}'`

If that came back `unavailable`, or `updated_at` is more than a couple of
minutes behind now, fall back to calling `list_agents` directly.

Present, per agent:

- **Online** — with how long ago it was last seen. Over ~45 seconds means the
  heartbeat has gone stale, not necessarily that the machine is down.
- **Jobs running** — with labels if they have them.
- **Jobs finished** — state and exit code. Call out anything that is not
  `succeeded`; a failed or timed-out training run is the whole reason this
  command exists.
- **Recent errors** — flag `[response]` entries specially: those mean a command
  ran to completion and its result was lost, so the controller saw a timeout for
  work that actually happened.

Keep it to a few lines per agent. If everything is healthy and idle, one line
total is the right answer.
