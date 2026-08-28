<div align="center">

# S3 MCP Relay

**Securely operate isolated Linux hosts through an S3-compatible object store.**

No inbound ports, VPN, bastion host, or direct network route.
End-to-end encryption keeps the object store limited to transient ciphertext transport.

[![Rust](https://img.shields.io/badge/Rust-1.94%2B-000?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/Protocol-MCP-2563eb?style=flat-square)](https://modelcontextprotocol.io/)
[![License](https://img.shields.io/badge/License-MIT-16a34a?style=flat-square)](LICENSE)

English · [简体中文](docs/README.zh-CN.md) · [日本語](docs/README.ja.md)

[Overview](#overview) · [Security](#security-model) · [Quick start](#quick-start) · [Tools](#tools) · [Plugins](#plugins)

</div>

```text
  MCP client ──stdio── s3-relay-mcp ──HTTPS──▶ ┌──────────────┐
                                               │ S3-compatible│
                                               │ object store │
  relay-agent ◀───────── HTTPS polling ─────── └──────────────┘
  (isolated server)
```

Both endpoints make **outbound-only** HTTPS requests to the same bucket. No
direct connection between the controller and the isolated host is required.

---

## Overview

Some environments prohibit all inbound connectivity while still permitting
access to an approved object store. Conventional approaches such as VPNs,
bastion hosts, and exposed SSH ports may therefore be unavailable or explicitly
disallowed.

S3 MCP Relay uses that approved outbound channel as a narrow control plane. The
controller and remote agent exchange authenticated ciphertext through a
dedicated bucket or prefix without establishing a direct network path.

## Capabilities

| Capability | Description |
|---|---|
| **Run programs** | `exec` for short commands, `start_job` for work measured in hours |
| **Move files** | Small ones inline; large ones streamed through the bucket, never through the conversation |
| **Manage paths** | Read, write, list, create, delete, move |
| **Report health** | Heartbeats, recent errors, job outcomes — visible without asking |

## Security model

- **End-to-end encryption.** XChaCha20-Poly1305 on every object, with the S3 key
  authenticated as AAD so a valid ciphertext cannot be replayed into another
  agent's mailbox. The storage provider sees only ciphertext; server-side
  encryption protects disks, not the service itself.
- **HTTPS enforced.** Plaintext endpoints are refused unless explicitly enabled
  for local testing.
- **Ephemeral transport.** Consumers delete objects after successful reads. The
  bucket is designed as a transit layer rather than a history store.
- **Per-agent isolation.** Each agent gets its own credentials and its own IAM
  policy scoped to its own prefix.
- **Bounded by construction.** Command expiry, protocol versioning, execution
  timeouts, output caps, path confinement, and an environment allowlist that
  refuses to pass credentials to child processes.

### Two capability modes

**Restricted** (default): `exec` runs only exact allowlisted absolute paths, no
shell; file operations stay inside declared roots.

> [!WARNING]
> **Full mode** (`allow_any_path` + `allow_any_program`) intentionally provides
> remote-code-execution capability comparable to a local shell. Enable it only
> when the bucket, controller, agent, and MCP session share the same trusted
> administrative boundary. Do not enable it on a shared host.

## Quick start

### Requirements

| Requirement | Minimum |
|---|---|
| Rust toolchain | Rust 1.94 or later |
| Object storage | S3-compatible endpoint with a dedicated bucket or prefix |
| Remote host | Linux with systemd or OpenRC |
| MCP client host | A supported local MCP client, such as Claude Code or Codex |

### 1. Build

```bash
cargo build --release --workspace
```

### 2. Install the agent

For new installations, initialize the controller first. The interactive setup
prompts for RainYun connection information and writes an owner-only TOML:

```bash
s3-relay-mcp init
```

Enter the printed controller public key while initializing the agent:

```bash
relay-agent init legacy-01
```

The agent prints a compact public pairing code and the exact command to run on
the controller:

```bash
s3-relay-mcp add <pairing-code>
```

The copied code contains only the agent name and X25519 public key. The Emoji
sequence is a human verification fingerprint, not key material. Controller and
agent private keys remain in their respective `0600` TOML files; the per-agent
transport key is derived with X25519+HKDF in memory and is never persisted.
The compact code uses a binary Base64URL representation rather than JSON, and
the Agent prints the same enrollment command again whenever it starts.

Both binaries support the same lifecycle commands:

```text
init | reset | status | update
```

The controller additionally provides `add <pairing-code>`.

The installer commands below remain available for legacy shared-key deployments.

Run the installer on the isolated Linux host:

```bash
sudo sh deploy/install-agent.sh \
  --agent-id legacy-01 \
  --endpoint https://cn-sy1.rains3.com \
  --bucket my-relay-bucket \
  --access-key AK... --secret-key SK...
```

It detects systemd or OpenRC, writes a `0600` config, generates a shared key and
prints it once.

### 3. Install the controller

Run the controller installer on the MCP client host:

```bash
sh deploy/install-controller.sh \
  --agents legacy-01 \
  --endpoint https://cn-sy1.rains3.com \
  --bucket my-relay-bucket \
  --access-key AK... --secret-key SK... \
  --shared-key <the key from step 2>
```

The controller is installed at user scope and does not require root. If the
`claude` CLI is available, the installer can register the MCP server
automatically.

### 4. Confirm connectivity

Restart the MCP client and invoke `list_agents`.

## Tools

| Tool | Notes |
|---|---|
| `list_agents` | Live agents, their jobs, and recent errors |
| `ping` | Round-trip check |
| `exec` | One program, no shell, arguments never expanded |
| `start_job` · `list_jobs` · `job_output` · `cancel_job` | Detached long-running work |
| `read_file` · `write_file` | ≤1 MiB, content passes through the conversation |
| `push_file` · `pull_file` | Any size, streamed through the bucket |
| `list_dir` · `make_dir` · `remove` · `move_path` | Path management |
| `publish_update` · `update_status` · `retract_update` | Roll a new agent binary out to the fleet |
| `stand_down` | Stop one process when two agents share an identity |

### Tool selection

> [!IMPORTANT]
> Use `exec` only for short-lived commands. If it reaches the controller timeout,
> the remote process may continue without supervision. Use `start_job` for
> builds, training, imports, or any operation that may take several minutes.

`read_file` returns content inside the tool result, so it lands in the model's
context — 100 KB is roughly 40k tokens. Use `pull_file` for anything larger.

## Long-running work

`start_job` returns a job id immediately and supervises the process for up to six
hours by default. Output streams straight to files on the agent, so a
multi-gigabyte training log costs no memory anywhere.

All job stdout/stderr and rotated Agent runtime logs are uploaded incrementally
as encrypted, idempotent chunks. The controller writes raw logs under
`controller.log_dir`; SQLite indexes enrolled public keys, Agent events, and
the next byte offset for every log source. SQLite never stores private keys or
derived transport keys. A chunk is deleted from S3 only after the controller
has synced it locally.

Agent-local logs are cleaned by age and total-size limits. Running job logs and
the active Agent runtime log are never deleted. Files without a complete local
upload-offset marker are also retained, even when that temporarily exceeds the
size limit, so an S3 outage cannot turn cleanup into data loss. Defaults are seven days, 1 GiB
total, and cleanup every six hours:

```toml
[controller]
database = "/home/operator/.config/relay/controller.db"
log_dir = "/home/operator/.local/state/relay/logs"

[agent]
job_retention_days = 7
job_max_total_bytes = 1073741824
job_cleanup_interval_secs = 21600
job_ship_chunk_bytes = 131072
```

Tagging a version such as `v0.2.0` starts the GitHub Actions release workflow,
builds Linux, macOS, and Windows binaries, and publishes them to the release.
The `update` command downloads the matching latest-release asset over HTTPS.

**Job completion is not pushed to the model.** This is a property of the MCP
interaction model rather than an implementation defect. The agent includes job
outcomes in its heartbeat, and the controller refreshes a local status file
every 30 seconds. Results remain available through `list_jobs`, `/relay-status`,
or a configured status line.

## File transfer

```
push_file(agent_id="legacy-01",
          local_path="~/torch-2.4.0-cp311-linux_x86_64.whl",
          remote_path="/srv/app/wheels/torch-2.4.0.whl")
→ {"bytes": 198234112, "chunks": 24, "sha256": "…"}
```

Files are split into 8 MiB chunks, sealed individually, and staged under a
per-transfer prefix. Peak memory is ~16 MB regardless of file size. The receiver
assembles into a temporary sibling file and renames only after the SHA-256
matches, so an interrupted transfer never leaves something that looks complete.
Chunks are deleted on success and on failure.

Unlike commands, transfers are **not** at-most-once — copying bytes has no side
effects, so a failed transfer is safe to retry.

## Operational semantics

The following behaviors differ intentionally from a conventional remote shell.

**A timeout does not mean the command did not run.** Delivery is at-most-once:
the agent may have executed it and then failed to upload the response. Before
retrying anything with side effects, check whether it already happened. Lost
responses appear in `recent_errors` for exactly this reason.

**Latency is real but small.** The agent polls every 200 ms while active, backing
off to 5 s when idle. A command to an idle agent may wait a few seconds.

**Adding an agent requires a restart.** `allowed_agents` is read once at startup.
An agent missing from it stays invisible however healthy it is.

## Configuration

TOML file plus environment variables; the environment always wins.

```toml
[s3]
endpoint = "https://cn-sy1.rains3.com"
bucket   = "my-relay-bucket"
prefix   = "relay-prod/"

[agent]
id = "legacy-01"
allow_any_path    = true
allow_any_program = true
```

Configuration files may contain secrets and must therefore be restricted to
their owner. The process warns when a secret-bearing file is more permissive
than `0600`. Unknown keys are rejected to prevent misspelled security settings
from silently changing policy.

Every setting is available through either source. See
[`relay.toml.example`](relay.toml.example) for the file form, and
[`deploy/relay.env.example`](deploy/relay.env.example) for the same settings as
environment variables — the shape a systemd `EnvironmentFile` wants.

## Plugins

| Package | Contents |
|---|---|
| [Claude Code plugin](plugin/) | MCP registration, `relay-ops` skill, `/relay-status` command, and status-line helper |
| [Codex plugin](codex-plugins/s3-relay/) | MCP registration plus `relay-ops` and `relay-status` skills |

The skills provide operational context that tool schemas cannot express fully,
including ambiguous timeouts, detached-job selection, and context-safe file
transfer.

## Architecture

**Doorbell.** Listing a prefix is the most expensive request an idle agent makes.
The controller overwrites a single doorbell object after enqueueing; the agent
only HEADs that key and lists when the ETag changes. A periodic full scan
catches anything a lost doorbell would have missed.

**Adaptive polling.** 200 ms while active, backing off to 5 s when idle, snapping
back the moment work arrives. Claude's calls are bursty, and this fits that shape.

**Heartbeats.** Written on their own task, deliberately: commands run serially
and can take hours, and a shared loop would let the heartbeat go stale and make
a busy agent look dead.

**Layout.** `cmd/<agent>/`, `resp/<agent>/`, `agents/<agent>.json`,
`doorbell/<agent>.json`, `blob/<agent>/<transfer>/`,
`updates/manifest/<agent>.json`, `updates/blob/<release>/`.

## Fleet updates

The controller publishes a new `relay-agent` binary into the bucket and every
agent installs it on its own. There is no inbound connection to make and no
machine to log into, which is the same constraint that motivated the relay.

```text
publish_update(local_path="target/aarch64-unknown-linux-gnu/release/relay-agent")
```

An update is deliberately **not** a command. Commands expire in minutes and are
consumed once, so any machine that happened to be rebooting during a rollout
would miss it forever. The manifest instead waits in the bucket, and an agent
that has been off for a week installs it the moment it comes back.

**One upload, any number of agents.** Sealing a 15 MB binary separately for each
machine would multiply the controller's upload by the fleet size. Instead the
binary is encrypted once under a random per-release key, and only that key
travels per-agent, inside a few hundred bytes of manifest sealed with the
agent's own key. The bucket still never holds anything readable.

**Mixed architectures.** The platform is read out of the binary's own header and
recorded in the manifest; an agent whose platform differs refuses the release
rather than exec-ing a foreign binary. Publish once per architecture — the
matching agents take it and the rest report why they did not.

Before an agent replaces anything it checks that the release is for its
platform, that its own binary does not already hash to the published one, that
the manifest is newer than the last one it applied, that no job is running, and
finally that the downloaded binary actually starts and answers `--version`. Only
then does it move the old binary aside and restart into the new one; if the new
binary will not run, the old one is put back. `update_status` shows what each
agent is running against what has been published, and `retract_update` stops a
rollout that has not reached everyone yet.

Two deployment details matter, and both are already in the shipped unit:

* `Restart=always`, because the agent exits on purpose to restart into the new
  binary. Under `Restart=on-failure` a clean exit would stop the service, which
  is why the agent exits non-zero (70) instead.
* `ReadWritePaths` covering the install directory, or `ProtectSystem=strict`
  makes every update fail on a read-only filesystem.

Set `agent.auto_update = false` to opt a host out. On a `--restricted` install
this is worth thinking about: an agent that can rewrite its own executable can
run anything as its service user after a restart, which is a real widening of
what the sandbox is otherwise holding back.

### Retiring a finished rollout

Once every agent that still matters is running the published build, the
controller deletes the release and its manifests from the bucket on its own.
Publishing again starts the next cycle.

"Every agent that still matters" is the hard part, because a heartbeat only
exists while an agent is alive — the bucket cannot tell a machine that is
rebooting from one that was decommissioned last spring. The controller
remembers when each agent was last heard from, and anything silent for longer
than `controller.update_seen_days` (default 3) is treated as gone: it does not
hold the rollout open, and its manifest is removed with the rest. A machine off
for longer than that comes back to no manifest and stays on its old binary
until something is published again, so raise the window if your fleet has
machines that sleep for weeks.

Completion is decided by hash, not version string. Agents report the SHA-256 of
the binary they are running, so a rebuilt `0.1.0` is not mistaken for the one it
replaced.

### Unattended releases

Point the controller at a repository and the whole cycle runs without anyone
asking for it — a release is tagged, agents install it, the sweep removes it,
the next tag begins again:

```toml
[controller]
auto_publish_repo = "HiroGitea/s3-mcp-relay"
```

This is off unless configured, and deliberately so: it downloads binaries from
the internet and hands them to every machine you own. What it does check is that
only platforms agents actually run are fetched, that an asset whose header does
not match the platform its name claims is refused rather than published, and
that a version already rolled out is not uploaded again — the ledger outlives
the release in the bucket, so a completed-and-swept rollout is not restarted
from scratch. Each agent still applies all five of its own checks on top.

## Two agents on one identity

Cloning a machine clones its agent identity, and the result is worse than one
of them losing: both poll the same mailbox, fetch-then-delete is not atomic, so
a command meant for one host can run on the other — or on both. Responses
overwrite each other, heartbeats flap between two machines, and shipped logs
interleave at the same offsets.

Each agent process therefore mints a random **instance** id at startup and puts
it in its heartbeat. It is never written to disk, which is the point: a cloned
image carries every file, so only a value created at run time can tell two
copies apart. Before writing its heartbeat an agent reads the current one, and
a fresh heartbeat carrying somebody else's instance means a duplicate exists.

Both sides then suspend command execution, and one yields automatically: the
higher instance id stands down. Plain string comparison, evaluated
independently, because S3 offers no atomic compare-and-swap to build a lease on
and not every S3-compatible store implements conditional writes. The collision
is reported in `list_agents` with both instance ids, and the suspended side
declines commands with an explanation rather than letting them time out.

To choose which machine keeps working instead of accepting the automatic
verdict:

```text
stand_down(agent_id="gpu-01", instance="<the one to stop>")
```

A command carrying an `instance` is left in the bucket by every other process,
so it reaches the intended one even though both are reading the same prefix.
Standing down is permanent for that process — a heartbeat timing out will not
quietly return a machine you retired to service.

The way to avoid all of this is to not clone an identity: delete
`~/.config/relay/agent.toml` before saving a disk image, and let each instance
run `relay-agent init` for itself.

## Storage requirements

Ephemeral transport depends on the following bucket configuration:

1. Dedicated bucket or prefix; versioning and Object Lock **off**, or deletes
   only create delete markers.
2. A short lifecycle rule as a backstop for crashes, applied **per prefix** —
   see `deploy/s3-lifecycle.example.json`. A single rule over the whole prefix
   would also expire published updates, and with them the ability of an offline
   machine to ever catch up.
3. No replication, archival, or access logs containing payloads.
4. Server-side encryption is fine but does not replace the end-to-end layer.
5. TLS enforced by bucket policy; separate identities for controller and agent.

If the provider must not receive even transient ciphertext, object storage is
not an appropriate transport. Use a reviewed in-memory messaging system; this
property cannot be provided by client-side code alone.

## Workspace

```text
crates/common       protocol, crypto, config, S3 transport, transfer
crates/controller   MCP stdio server (s3-relay-mcp)
crates/agent        agent for the isolated server (relay-agent)
deploy/             install scripts, IAM examples, systemd unit, env template
plugin/             Claude Code plugin
codex-plugins/      Codex plugins
```

## License

Distributed under the [MIT License](LICENSE).
