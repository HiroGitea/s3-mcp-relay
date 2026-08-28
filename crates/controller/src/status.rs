//! A status file for things that are not Claude.
//!
//! Everything else in the controller happens because the model called a tool.
//! A status line cannot do that — it renders many times a minute and has no way
//! to ask a question. So the controller polls heartbeats on its own and writes
//! a small JSON file that anything can read: `claude-hud`, a shell prompt, a
//! `watch` loop.
//!
//! This is also what makes a finished training run visible without anyone
//! asking. An MCP server cannot push to Claude, but it can leave the outcome
//! somewhere a person will see it, and a person can then ask.
//!
//! The file only exists while a Claude session is running, since that is the
//! only time this process exists. Readers should treat `updated_at` as the
//! authority on freshness rather than assuming the file is live.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use common::protocol::now_unix;
use common::Transport;
use serde::Serialize;

#[derive(Serialize)]
struct StatusFile {
    updated_at: i64,
    /// Seconds between refreshes, so a reader can decide when to call it stale.
    interval_secs: u64,
    agents: Vec<AgentStatus>,
}

#[derive(Serialize)]
struct AgentStatus {
    id: String,
    hostname: String,
    os: String,
    version: String,
    /// Seconds since the agent last published a heartbeat.
    last_seen_secs: i64,
    jobs_running: u32,
    /// Finished jobs the agent is still reporting, newest first.
    jobs_finished: Vec<FinishedJob>,
    /// Recent non-command errors, newest last.
    errors: Vec<String>,
    /// Snapshot health, absent if the agent could not sample any of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<common::protocol::Metrics>,
}

#[derive(Serialize)]
struct FinishedJob {
    job: String,
    label: Option<String>,
    state: String,
    exit_code: Option<i32>,
    finished_at: Option<i64>,
}

/// Poll heartbeats and rewrite the status file until the process exits.
pub async fn cache_loop(
    transport: Transport,
    allowed: std::collections::HashSet<String>,
    path: PathBuf,
    interval: Duration,
) {
    loop {
        if let Err(error) = write_once(&transport, &allowed, &path, interval).await {
            // Never fatal: a missing status file degrades a status line, it
            // does not affect the control channel at all.
            tracing::debug!(%error, "status cache refresh failed");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn write_once(
    transport: &Transport,
    allowed: &std::collections::HashSet<String>,
    path: &Path,
    interval: Duration,
) -> Result<()> {
    let heartbeats = transport.list_heartbeats().await?;
    let now = now_unix();
    let agents = heartbeats
        .into_iter()
        .filter(|heartbeat| allowed.contains(&heartbeat.agent_id))
        .map(|heartbeat| AgentStatus {
            metrics: heartbeat.metrics,
            id: heartbeat.agent_id,
            hostname: heartbeat.hostname,
            os: heartbeat.os,
            version: heartbeat.agent_version,
            last_seen_secs: now.saturating_sub(heartbeat.at).max(0),
            jobs_running: heartbeat.jobs_running,
            jobs_finished: heartbeat
                .jobs_finished
                .into_iter()
                .map(|job| FinishedJob {
                    job: job.job,
                    label: job.label,
                    state: format!("{:?}", job.state).to_lowercase(),
                    exit_code: job.exit_code,
                    finished_at: job.finished_at,
                })
                .collect(),
            errors: heartbeat
                .recent_errors
                .into_iter()
                .map(|event| format!("[{:?}] {}", event.kind, event.message).to_lowercase())
                .collect(),
        })
        .collect();

    let status = StatusFile {
        updated_at: now,
        interval_secs: interval.as_secs(),
        agents,
    };
    let json = serde_json::to_vec_pretty(&status).context("serialize status")?;

    if let Some(parent) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    // Write and rename: a status line polling on its own schedule must never
    // catch a half-written file and fail to parse it.
    let temp = path.with_extension("json.tmp");
    tokio::fs::write(&temp, &json)
        .await
        .with_context(|| format!("write {}", temp.display()))?;
    tokio::fs::rename(&temp, path)
        .await
        .with_context(|| format!("rename into {}", path.display()))?;
    Ok(())
}

/// Default location, following XDG where it applies.
pub fn default_path() -> PathBuf {
    if let Ok(runtime) = std::env::var("XDG_RUNTIME_DIR") {
        if !runtime.is_empty() {
            return PathBuf::from(runtime).join("relay/status.json");
        }
    }
    if let Ok(cache) = std::env::var("XDG_CACHE_HOME") {
        if !cache.is_empty() {
            return PathBuf::from(cache).join("relay/status.json");
        }
    }
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => PathBuf::from(home).join(".cache/relay/status.json"),
        _ => PathBuf::from("relay-status.json"),
    }
}
