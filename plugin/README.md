<div align="center">

# S3 MCP Relay for Claude Code

**MCP registration, operational guidance, and local job visibility in one plugin.**

[Project documentation](../README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

</div>

This package integrates `s3-relay-mcp` with Claude Code and provides the
operational guidance required to use remote commands, detached jobs, and file
transfers safely.

## Package contents

| Component | Purpose |
|---|---|
| `.mcp.json` | Registers `s3-relay-mcp` so installing the plugin is the whole setup |
| `skills/relay-ops` | Teaches the failure modes: exec vs start_job, when a timeout does not mean "did not run", why `read_file` on a large file is a mistake |
| `commands/relay-status.md` | `/relay-status` — agents, jobs, recent errors |
| `scripts/hud-segment.sh` | One line for a status line, from the cached status file |
| `hooks/hooks.json` | Detects a previous unclean Claude Code exit at the next session start |
| `scripts/session-recovery.js` | Maintains the durable per-session markers used by the recovery hook |

The `relay-ops` skill supplements the MCP tool schemas with operational
semantics, including ambiguous timeouts, detached-job selection, and
context-safe file transfer.

## Prerequisites

The plugin does not include platform-specific binaries or credentials. Build
and install the controller with `deploy/install-controller.sh`, then define the
following environment variables before enabling the plugin:

```sh
export RELAY_MCP_BIN="$HOME/.local/bin/s3-relay-mcp"
export RELAY_CONFIG="$HOME/.config/relay/controller.toml"
export RELAY_LOG_FILE="$HOME/.local/state/relay/controller.log"
```

> [!WARNING]
> If `install-controller.sh` already registered the MCP server directly, remove
> or disable that registration before enabling the plugin. Loading the same
> server twice creates conflicting tool registrations.

## Interrupted session recovery

At session start, the plugin records the session id, project path, transcript
path, start time, and Claude process id under `CLAUDE_PLUGIN_DATA`. A normal
`SessionEnd` removes that marker. If Claude Code disappears without reaching
`SessionEnd`, the next session in the same project displays the interrupted
session id and a ready-to-run `/resume <session-id>` command.

The hook checks whether the recorded process is still alive, so opening a
second Claude Code session in the same project does not mislabel the first one
as crashed. Resuming the interrupted session clears its stale marker and does
not repeat the warning. The hook never resumes a session or retries relay work
automatically.

## Status line

The controller refreshes a local JSON status file every 30 seconds. A status
line can therefore display agent and job state without an MCP request or model
turn.

```sh
plugin/scripts/hud-segment.sh
# ⬢ 2 agents · ⚙ 1 job
```

For `claude-hud`, configure the script as a custom command segment. The same
command can be used from tmux or a shell prompt. It emits no output when the
status file is unavailable or stale.

`jq` is used when present and the script degrades to an agent count without it.
`--format=json` passes the raw file through for a HUD that would rather parse it
itself.

## Status file shape

```json
{
  "updated_at": 1735000000,
  "interval_secs": 30,
  "agents": [
    {
      "id": "legacy-01",
      "hostname": "gpu-box",
      "os": "linux x86_64",
      "version": "0.1.0",
      "last_seen_secs": 4,
      "jobs_running": 1,
      "jobs_finished": [
        { "job": "…", "label": "resnet50", "state": "succeeded", "exit_code": 0, "finished_at": 1734999000 }
      ],
      "errors": []
    }
  ]
}
```

Treat `updated_at` as the authority on freshness. `last_seen_secs` above ~45
means the agent's heartbeat has gone stale; the machine may be down, or the
agent may simply be unable to reach S3.
