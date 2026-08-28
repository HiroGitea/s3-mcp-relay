#!/usr/bin/env node
"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");

function readInput() {
  try {
    return JSON.parse(fs.readFileSync(0, "utf8") || "{}");
  } catch (_) {
    return {};
  }
}

function stateDirectory() {
  const pluginData = process.env.CLAUDE_PLUGIN_DATA;
  const base = pluginData || path.join(os.homedir(), ".claude", "s3-relay-plugin-data");
  return path.join(base, "session-recovery");
}

function normalizedProject(projectPath) {
  const resolved = path.resolve(projectPath || ".").replace(/\\/g, "/");
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function processInfo(pid) {
  try {
    if (process.platform === "win32") {
      const powershell = process.env.SystemRoot
        ? path.join(process.env.SystemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
        : "powershell.exe";
      const script = [
        `$p = Get-CimInstance Win32_Process -Filter \"ProcessId = ${pid}\"`,
        "if ($null -ne $p) {",
        "  [pscustomobject]@{ pid = $p.ProcessId; parentPid = $p.ParentProcessId; command = $p.CommandLine } | ConvertTo-Json -Compress",
        "}"
      ].join("; ");
      const output = execFileSync(powershell, ["-NoProfile", "-Command", script], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 2000
      }).trim();
      return output ? JSON.parse(output) : null;
    }

    const output = execFileSync("ps", ["-o", "ppid=", "-o", "command=", "-p", String(pid)], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 2000
    }).trim();
    const match = output.match(/^(\d+)\s+([\s\S]*)$/);
    return match ? { pid, parentPid: Number(match[1]), command: match[2] } : null;
  } catch (_) {
    return null;
  }
}

function findClaudePid() {
  let pid = process.ppid;
  let fallback = pid;

  for (let depth = 0; depth < 10 && pid > 1; depth += 1) {
    const info = processInfo(pid);
    if (!info) break;
    fallback = Number(info.pid) || fallback;
    const command = String(info.command || "");
    if (/claude/i.test(command) && !/session-recovery\.js/i.test(command)) {
      return Number(info.pid);
    }
    pid = Number(info.parentPid);
  }

  return fallback;
}

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error && error.code === "EPERM";
  }
}

function markerPath(directory, sessionId) {
  const safeId = String(sessionId).replace(/[^a-zA-Z0-9._-]/g, "_");
  return path.join(directory, `${safeId}.json`);
}

function removeMarker(directory, sessionId) {
  if (!sessionId) return;
  try {
    fs.rmSync(markerPath(directory, sessionId), { force: true });
  } catch (_) {
    // Recovery hints must never prevent Claude Code from starting or exiting.
  }
}

function writeMarker(directory, input) {
  const record = {
    sessionId: input.session_id,
    sessionTitle: input.session_title || null,
    transcriptPath: input.transcript_path || null,
    cwd: path.resolve(input.cwd || process.cwd()),
    pid: findClaudePid(),
    startedAt: new Date().toISOString()
  };
  const destination = markerPath(directory, record.sessionId);
  const temporary = `${destination}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(record, null, 2)}\n`, "utf8");
  fs.renameSync(temporary, destination);
}

function staleSessions(directory, input) {
  const currentProject = normalizedProject(input.cwd || process.cwd());
  const found = [];

  for (const filename of fs.readdirSync(directory)) {
    if (!filename.endsWith(".json")) continue;
    const fullPath = path.join(directory, filename);
    let record;
    try {
      record = JSON.parse(fs.readFileSync(fullPath, "utf8"));
    } catch (_) {
      fs.rmSync(fullPath, { force: true });
      continue;
    }

    if (record.sessionId === input.session_id) {
      fs.rmSync(fullPath, { force: true });
      continue;
    }
    if (processIsAlive(Number(record.pid))) continue;

    fs.rmSync(fullPath, { force: true });
    if (normalizedProject(record.cwd) === currentProject) found.push(record);
  }

  return found;
}

function showRecoveryHint(records) {
  if (records.length === 0) return;
  const recent = records
    .sort((left, right) => String(right.startedAt).localeCompare(String(left.startedAt)))
    .slice(0, 3);
  const entries = recent.map((record) => {
    const label = record.sessionTitle ? `${record.sessionTitle} (${record.sessionId})` : record.sessionId;
    return `- ${label}\n  /resume ${record.sessionId}`;
  });
  const message = [
    "Detected a Claude Code session that may have exited unexpectedly.",
    ...entries,
    "Use the listed /resume command to open the interrupted session."
  ].join("\n");
  const context = [
    "One or more earlier Claude Code sessions in this project appear to have exited unexpectedly.",
    "The user has been shown their session IDs and /resume commands.",
    "Do not resume or repeat interrupted operations automatically."
  ].join(" ");

  process.stdout.write(JSON.stringify({
    systemMessage: message,
    hookSpecificOutput: {
      hookEventName: "SessionStart",
      additionalContext: context
    }
  }));
}

function main() {
  const mode = process.argv[2];
  const input = readInput();
  if (!input.session_id) return;

  const directory = stateDirectory();
  fs.mkdirSync(directory, { recursive: true });

  if (mode === "end") {
    removeMarker(directory, input.session_id);
    return;
  }
  if (mode !== "start") return;

  const stale = staleSessions(directory, input);
  writeMarker(directory, input);
  showRecoveryHint(stale);
}

try {
  main();
} catch (_) {
  // This advisory hook must never interrupt Claude Code.
}
