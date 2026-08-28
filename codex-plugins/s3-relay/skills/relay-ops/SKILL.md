---
name: relay-ops
description: Operate remote machines through the S3 relay, including choosing between exec and start_job, moving files without consuming model context, installing packages onto a host with no internet access, and interpreting relay timeouts. Use with s3-relay MCP tools, when a relay command times out, or when a download, pip install, apt-get or git clone fails on the agent.
---

# Operating the S3 relay

Every tool here reaches a machine that cannot be connected to directly.
Commands travel through an S3-compatible bucket, the remote agent polls for
them, and results return through the same channel. Three consequences drive the
workflow: there is latency, there is no push, and a timeout does not prove that
nothing happened.

## Pick the right tool

| Work | Fast or small | Slow or large |
|---|---|---|
| Commands | `exec` | `start_job` |
| File content | `read_file` / `write_file` | `pull_file` / `push_file` |

Use `exec` for commands expected to finish in seconds or a few minutes. Use
`start_job` for builds, training, imports, heavy installs, or anything that may
outlast the controller wait ceiling. If uncertain, prefer `start_job`: an
`exec` timeout can leave the remote process running without supervision.

Use `read_file` and `write_file` only for small text files. Their contents pass
through model context. Use `pull_file` and `push_file` for binaries or anything
larger than a few tens of kilobytes; they stream through the bucket and return
only paths and checksums.

## Install onto a host with no internet

The agent's only route off the machine is the bucket, so any command that
reaches for the network fails there: `pip install` times out, `apt-get` reports
a DNS failure, `curl` cannot connect, `git clone` never returns. These are not
network faults to debug on the agent, and retrying them does not help.

Move the bytes through the relay instead:

1. Download or build the artifact on the controller machine, which has a
   network.
2. `push_file` it to the agent; it streams through the bucket, so size is not a
   concern and nothing enters model context.
3. Install from the local copy — `pip install /tmp/pkg.whl`, `apt-get install
   ./pkg.deb`, `tar -xf`.

For Python dependencies, run `pip download -r requirements.txt -d wheels/` on
the controller, adding `--platform` and `--only-binary=:all:` when the agent's
architecture differs, push the wheels, and install with `pip install --no-index
--find-links /tmp/wheels -r requirements.txt`. `--no-index` prevents pip from
contacting PyPI again.

When the outdated component is the agent binary itself, use `publish_update`
rather than pushing it by hand; it delivers to the whole fleet at once and each
agent verifies and restarts on its own.

## Handle long jobs

After `start_job` returns:

1. Report the job id and label to the user.
2. Explain that completion is not pushed into the current turn.
3. When asked to check, call `list_jobs`.
4. For a failed job, use `job_output` for the stderr tail or `pull_file` on the
   returned `stderr_path` for the complete log.

Do not create a polling loop unless the user explicitly requests monitoring.
The controller refreshes its local status file every 30 seconds, so status can
usually be checked without another remote round trip.

## Interpret failures conservatively

A timeout does not mean the command did not run. Delivery is at-most-once: the
agent may finish the command and then fail to upload its response. Before
retrying any side-effecting operation, use a read-only check to determine
whether it already happened. Check `recent_errors` in `list_agents`; a lost
response is recorded there.

An agent missing from `list_agents` may be busy, unable to reach S3, or absent
from the controller's `allowed_agents`. Adding an allowed agent requires a
controller restart because that configuration is read only at startup.

File transfers are different from commands. They are checksum-verified byte
copies, so a failed transfer is safe to retry.

The remote agent polls every 200 ms while active and backs off to 5 seconds
while idle. A few seconds of latency for an idle agent is normal; do not resend
the operation just because it is not immediate.
