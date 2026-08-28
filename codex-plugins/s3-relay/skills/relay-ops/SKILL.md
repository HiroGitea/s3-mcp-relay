---
name: relay-ops
description: Operate remote machines through the S3 relay, including choosing between exec and start_job, moving files without consuming model context, and interpreting relay timeouts. Use with s3-relay MCP tools or when a relay command times out.
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
