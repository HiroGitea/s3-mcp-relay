---
name: relay-ops
description: Operating remote machines through the S3 relay - choosing between exec and start_job, moving files without blowing up the context, and reading the failure modes correctly. Load when working with s3-relay MCP tools (list_agents, exec, push_file, start_job) or when a relay command times out.
---

# Operating the S3 relay

Every tool here reaches a machine you cannot connect to directly. Commands go
into an S3 bucket, the agent polls for them, and results come back the same way.
Three consequences drive everything below: **there is latency**, **there is no
push**, and **a timeout does not mean nothing happened**.

## Pick the right tool for the size and duration

Two axes decide, and getting either wrong is the most common mistake:

| | fast (seconds) | slow (minutes to hours) |
|---|---|---|
| **small output** | `exec` | `start_job` |
| **bulk bytes** | `read_file` / `write_file` | `pull_file` / `push_file` |

### exec vs start_job

`exec` blocks until the program exits and is capped by the controller wait
ceiling — a few minutes. Use it for `systemctl status`, `git pull`, a quick
script.

`start_job` returns a job id immediately and supervises the process for up to
six hours by default. Use it for **anything that could outlast a coffee break**:
model training, a large build, a batch import, `pip install` of a heavy wheel
set.

Choosing `exec` for a long task does not fail cleanly — it times out, and the
program **keeps running** on the agent with nobody watching it. If you are not
sure, use `start_job`.

### read_file vs pull_file

`read_file` returns content inside the tool result, which means it lands in the
conversation. A 1 MiB cap applies, but the real limit is much lower: 100 KB of
base64 is roughly 40k tokens.

`pull_file` streams through the bucket and returns only a path and a checksum.
Use it for anything above a few tens of kilobytes, and always for binaries.

Same split in the other direction: `write_file` for a config file, `push_file`
for a wheel, a dataset, an installer.

## Long jobs: nobody will tell you when it finishes

An MCP server cannot wake Claude up. After `start_job` returns, **you will not
be notified** — not when it succeeds, not when it crashes.

What actually happens: the agent records the outcome and puts it in its
heartbeat. That means the result is waiting for whoever asks next.

So after starting a long job:

1. Tell the user the job id and label, and say explicitly that you will not know
   the outcome unless asked to check.
2. Suggest `/relay-status`, which reads a status file the controller refreshes
   every 30 seconds — no tool call, no model turn needed. If they run
   `claude-hud`, it can show the same thing in their status line permanently.
3. When asked to check, use `list_jobs`. For a job that ended badly, `job_output`
   gives the tail of stderr; `pull_file` on the returned `stderr_path` gets the
   whole log.

Do not poll `list_jobs` in a loop on your own initiative — each call is a
round trip through S3, and the user did not ask for a watchdog. If they want
one, `/loop 5m` is the right tool.

## Failure modes that are not what they look like

**A timeout does not mean the command did not run.** Delivery is at-most-once,
not exactly-once: the agent may have executed it and then failed to upload the
response. Before retrying anything with side effects — a write, a move, a
`systemctl restart` — run a read-only check to see whether it already happened.
This is the single most important habit with this system.

**An agent missing from `list_agents` may be busy, not dead.** It might also
just not be in `allowed_agents` on the controller side, in which case it stays
invisible no matter how healthy it is. Adding one requires editing the config
and restarting the MCP server.

**Check `recent_errors` in `list_agents` output.** Failures that happen outside
a command — a lost response, a poll that could not reach S3 — appear only there.
A lost response is the dangerous one: the work happened, and the controller saw
a timeout.

**File transfers are not at-most-once**, unlike commands. They are pure byte
copies verified by SHA-256, so a failed transfer is safe to simply retry.

## Latency is real but small

The agent polls every 200 ms while active, backing off to 5 s when idle. A
command sent to an idle agent may sit for a few seconds before it is noticed.
That is normal — do not interpret it as a hang, and do not resend.
