---
name: relay-status
description: Report the health of S3 relay agents, their running and completed jobs, and recent relay errors. Use when the user asks for relay, agent, or remote-job status.
---

# Report relay status

Prefer the controller's cached status file when it exists and its `updated_at`
value is no more than two minutes old. The default path is
`${RELAY_STATUS_FILE:-${XDG_RUNTIME_DIR:-$HOME/.cache}/relay/status.json}`.
Reading it avoids an S3 round trip. If it is missing or stale, call
`list_agents`.

Report each agent's online state and last-seen age, running jobs, recently
finished jobs, and recent errors. Treat a heartbeat older than roughly 45
seconds as stale, not definitive proof that the machine is down.

Call out any job whose state is not `succeeded`. Highlight `[response]` errors:
they mean work ran but its response was lost, so a timeout may have been
reported even though the side effect occurred.

Keep healthy idle output to one line. Use a few lines per agent only when jobs
or errors need attention.
