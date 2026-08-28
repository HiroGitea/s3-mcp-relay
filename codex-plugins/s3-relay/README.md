<div align="center">

# S3 MCP Relay for Codex

**Local MCP integration and operational guidance for isolated Linux hosts.**

[Project documentation](../../README.md) · [Claude Code plugin](../../plugin/)

</div>

This package registers the local `s3-relay-mcp` server with Codex and provides
two task-specific skills:

- `relay-ops` explains safe command, job, and file-transfer semantics.
- `relay-status` reports agent health, running jobs, completed jobs, and recent errors.

## Package contents

| Component | Purpose |
|---|---|
| `.mcp.json` | Registers the local `s3-relay-mcp` process |
| `skills/relay-ops` | Defines safe command, job, timeout, and file-transfer semantics |
| `skills/relay-status` | Reports agent health, job state, and recent errors |

## Prerequisites

Build and install the controller first, then expose its paths to Codex:

```sh
export RELAY_MCP_BIN="$HOME/.local/bin/s3-relay-mcp"
export RELAY_CONFIG="$HOME/.config/relay/controller.toml"
export RELAY_LOG_FILE="$HOME/.local/state/relay/controller.log"
```

The plugin does not include platform-specific binaries or credentials.

Initialize the controller with `s3-relay-mcp init`, enroll nodes with
`s3-relay-mcp add <pairing-code>`, and use `status` to inspect the local
registry. Agent and job logs are encrypted in transit, stored under the
controller log directory, and indexed by the controller SQLite database. No
credential or private key is bundled in this plugin.

> [!WARNING]
> If the MCP server is already registered separately, remove or disable that
> registration before enabling this plugin. Loading the same server twice
> creates conflicting tool registrations.
