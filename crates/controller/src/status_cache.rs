//! Status cache plus controller-side event and raw-log ingestion.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::protocol::{AgentEvent, JobStatus, Metrics};
use common::Transport;
use serde::Serialize;

use crate::log_store::LogStore;
use crate::registry::Registry;

#[derive(Serialize)]
struct StatusFile { updated_at: i64, interval_secs: u64, agents: Vec<AgentStatus> }

#[derive(Serialize)]
struct AgentStatus {
    id: String,
    hostname: String,
    os: String,
    version: String,
    last_seen_secs: i64,
    jobs_running: u32,
    jobs_finished: Vec<JobStatus>,
    metrics: Option<Metrics>,
    errors: Vec<AgentEvent>,
}

pub async fn cache_loop(
    transports: Arc<HashMap<String, Transport>>, allowed: HashSet<String>, registry: Registry,
    logs: LogStore, path: PathBuf, interval: Duration,
) {
    loop {
        if let Err(error) = refresh(&transports, &allowed, &registry, &logs, &path, interval).await {
            tracing::debug!(%error, "status/log refresh failed");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn refresh(
    transports: &HashMap<String, Transport>, allowed: &HashSet<String>, registry: &Registry,
    logs: &LogStore, path: &PathBuf, interval: Duration,
) -> Result<()> {
    let now = common::protocol::now_unix();
    let mut agents = Vec::new();
    for id in allowed {
        let Some(transport) = transports.get(id) else { continue };
        if let Some(heartbeat) = transport.read_heartbeat(id).await? {
            for event in &heartbeat.recent_errors {
                registry.event(id, event.at, &format!("{:?}", event.kind), &event.message)?;
            }
            agents.push(AgentStatus {
                id: heartbeat.agent_id,
                hostname: heartbeat.hostname,
                os: heartbeat.os,
                version: heartbeat.agent_version,
                last_seen_secs: now.saturating_sub(heartbeat.at),
                jobs_running: heartbeat.jobs_running,
                jobs_finished: heartbeat.jobs_finished,
                metrics: heartbeat.metrics,
                errors: heartbeat.recent_errors,
            });
        }
        for (key, chunk) in transport.pending_log_chunks(id).await? {
            logs.ingest(&chunk)?;
            transport.acknowledge_log_chunk(&key).await?;
        }
    }
    agents.sort_by(|a, b| a.id.cmp(&b.id));
    let status = StatusFile { updated_at: now, interval_secs: interval.as_secs(), agents };
    if let Some(parent) = path.parent() { tokio::fs::create_dir_all(parent).await?; }
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&status)?).await.context("write status cache")?;
    tokio::fs::rename(&tmp, path).await.context("install status cache")?;
    Ok(())
}

pub fn default_path() -> PathBuf {
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") { return PathBuf::from(runtime).join("relay/status.json"); }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        return PathBuf::from(home).join(".cache/relay/status.json");
    }
    PathBuf::from("relay-status.json")
}
