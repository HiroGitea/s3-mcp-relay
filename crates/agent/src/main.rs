mod admin;
mod events;
mod executor;
mod jobs;
mod log_shipper;
mod metrics;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use common::protocol::{now_unix, EventKind};
use common::{optional_env, required_env, validate_agent_id, Crypto, Heartbeat, Response, S3Config, Transport};
use events::EventLog;
use executor::Policy;
use jobs::JobManager;
use tokio::time::Instant;
use tracing::{error, info, warn};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    if admin::dispatch()? { return Ok(()); }
    let config_path = common::config::init_config()?;
    let job_dir = PathBuf::from(
        optional_env("AGENT_JOB_DIR").unwrap_or_else(|| "/var/lib/relay-agent/jobs".to_owned()),
    );
    std::fs::create_dir_all(&job_dir)?;
    let file_appender = tracing_appender::rolling::daily(&job_dir, "agent.log");
    let (file_writer, _log_guard) = tracing_appender::non_blocking(file_appender);
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(tracing_subscriber::fmt::layer().json().with_writer(file_writer))
        .init();

    if let Some(path) = config_path {
        info!(config = %path.display(), "loaded config file");
    }

    let agent_id = required_env("RELAY_AGENT_ID")?;
    validate_agent_id(&agent_id)?;
    if let Some(public_key) = optional_env("AGENT_PUBLIC_KEY") {
        if let Ok(code) = common::pairing::encode_enrollment(&agent_id, &public_key) {
            info!("If this node is not enrolled, run on the controller: s3-relay-mcp add {code}");
        }
    }
    let mut policy = Policy::from_env()?;
    let poll = PollPolicy::from_env()?;

    let job_manager = JobManager::new(job_dir.clone())?;
    jobs::report_orphans(&job_dir);
    // So pull_file can retrieve a training log without the operator having to
    // remember to add the job directory to allowed_roots.
    policy.permit_root(&job_dir)?;
    let policy = policy;
    let heartbeat_seconds = bounded_env("AGENT_HEARTBEAT_SECS", 15, 5, 1_800)?;
    let heartbeat_ttl = bounded_env("AGENT_HEARTBEAT_TTL_SECS", 45, 10, 3_600)? as i64;
    if heartbeat_ttl < (heartbeat_seconds * 2) as i64 {
        anyhow::bail!("AGENT_HEARTBEAT_TTL_SECS must be at least twice AGENT_HEARTBEAT_SECS");
    }
    let transport = Transport::connect(&S3Config::from_env()?, Crypto::from_env()?).await?;

    info!(%agent_id, "relay agent started");

    // Heartbeats run on their own task. Commands are executed serially on the
    // main loop and may legitimately run for minutes; sharing one task would
    // let the heartbeat go stale mid-command and make the controller declare a
    // busy agent dead.
    let log = EventLog::new();
    let shipper = log_shipper::LogShipper::new(
        job_dir.clone(), bounded_env("AGENT_JOB_SHIP_CHUNK_BYTES", 131_072, 4_096, 524_288)? as usize,
    );
    let ship_transport = transport.clone();
    let ship_agent = agent_id.clone();
    let ship_log = log.clone();
    let shipping = tokio::spawn(async move {
        loop {
            if let Err(error) = shipper.ship_once(&ship_transport, &ship_agent).await {
                warn!(%error, "central log upload failed");
                ship_log.record(EventKind::Transfer, format!("central log upload: {error:#}"));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let cleanup_policy = jobs::CleanupPolicy {
        retention: Duration::from_secs(bounded_env("AGENT_JOB_RETENTION_DAYS", 7, 1, 365)?.saturating_mul(86_400)),
        max_total_bytes: bounded_env("AGENT_JOB_MAX_TOTAL_BYTES", 1_073_741_824, 1_048_576, u64::MAX)?,
    };
    let cleanup_interval = Duration::from_secs(bounded_env("AGENT_JOB_CLEANUP_INTERVAL_SECS", 21_600, 300, 604_800)?);
    let cleanup_jobs = job_manager.clone();
    let cleanup = tokio::spawn(async move {
        loop {
            tokio::time::sleep(cleanup_interval).await;
            match cleanup_jobs.cleanup(cleanup_policy) {
                Ok((files, bytes)) if files > 0 => info!(files, bytes, "cleaned old local logs"),
                Ok(_) => {}
                Err(error) => warn!(%error, "local log cleanup failed"),
            }
        }
    });
    // Disk usage is sampled for the filesystem holding job output: that is the
    // one a training run fills up.
    let collector = metrics::Collector::new(
        job_dir.clone(),
        optional_env("AGENT_GPU_METRICS")
            .map_or(true, |value| !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "no")),
    );
    let heartbeat = tokio::spawn(heartbeat_loop(
        transport.clone(),
        agent_id.clone(),
        Duration::from_secs(heartbeat_seconds),
        heartbeat_ttl,
        log.clone(),
        job_manager.clone(),
        collector,
    ));

    let result = run_loop(&transport, &agent_id, &policy, &poll, &log, &job_manager).await;

    heartbeat.abort();
    shipping.abort();
    cleanup.abort();
    if let Err(error) = transport.delete_heartbeat(&agent_id).await {
        warn!(%error, "could not remove heartbeat during shutdown");
    }
    result
}

async fn heartbeat_loop(
    transport: Transport,
    agent_id: String,
    every: Duration,
    ttl_secs: i64,
    log: EventLog,
    jobs: JobManager,
    collector: metrics::Collector,
) {
    loop {
        let sampled = collector.sample().await;
        let heartbeat = Heartbeat {
            agent_id: agent_id.clone(),
            hostname: executor::hostname(),
            os: executor::os_string(),
            agent_version: env!("CARGO_PKG_VERSION").to_owned(),
            at: now_unix(),
            ttl_secs,
            recent_errors: log.snapshot(),
            jobs_running: jobs.running_count(),
            jobs_finished: jobs.recently_finished(),
            metrics: Some(sampled),
        };
        if let Err(error) = transport.write_heartbeat(&heartbeat).await {
            warn!(%error, "heartbeat upload failed");
            // Recorded so the next successful heartbeat reports the gap. If
            // uploads keep failing the controller sees a stale agent instead,
            // which is the same conclusion by another route.
            log.record(EventKind::Heartbeat, format!("{error:#}"));
        }
        tokio::time::sleep(every).await;
    }
}

/// How often the agent looks for work.
///
/// Listing a prefix is the most expensive S3 request this agent makes, so the
/// default path is a HEAD against a single doorbell object and a LIST only when
/// its ETag changed. On top of that the interval backs off while the mailbox
/// stays empty, which keeps an idle agent cheap without delaying the first
/// command of a burst.
struct PollPolicy {
    /// Interval used right after work arrived, and the floor for backoff.
    active: Duration,
    /// Ceiling the interval backs off to while nothing arrives.
    idle: Duration,
    /// List unconditionally at least this often, so a command whose doorbell
    /// update failed is still picked up eventually.
    full_scan: Duration,
    /// Set AGENT_DOORBELL=false to list on every tick instead. Useful when the
    /// agent credentials cannot read the doorbell prefix.
    use_doorbell: bool,
}

impl PollPolicy {
    fn from_env() -> Result<Self> {
        let active = bounded_env("AGENT_POLL_MS", 200, 100, 60_000)?;
        let idle = bounded_env("AGENT_POLL_MAX_MS", 5_000, 100, 300_000)?;
        if idle < active {
            anyhow::bail!("AGENT_POLL_MAX_MS must be greater than or equal to AGENT_POLL_MS");
        }
        Ok(Self {
            active: Duration::from_millis(active),
            idle: Duration::from_millis(idle),
            full_scan: Duration::from_secs(bounded_env("AGENT_FULL_SCAN_SECS", 60, 5, 3_600)?),
            use_doorbell: optional_env("AGENT_DOORBELL").map_or(true, |value| {
                !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "no")
            }),
        })
    }
}

async fn run_loop(
    transport: &Transport,
    agent_id: &str,
    policy: &Policy,
    poll: &PollPolicy,
    log: &EventLog,
    jobs: &JobManager,
) -> Result<()> {
    let mut interval = poll.active;
    let mut seen_doorbell: Option<String> = None;
    // Set to now so the very first tick lists unconditionally and picks up
    // anything a previous run left in the mailbox.
    let mut next_full_scan = Instant::now();

    loop {
        let forced = !poll.use_doorbell || Instant::now() >= next_full_scan;
        let mut observed = seen_doorbell.clone();

        // Read the doorbell BEFORE listing. Anything the controller writes
        // after this point changes the tag again, so it cannot be missed.
        let scan = if forced {
            true
        } else {
            match transport.doorbell_tag(agent_id).await {
                Ok(tag) => {
                    let changed = tag != seen_doorbell;
                    observed = tag;
                    changed
                }
                Err(error) => {
                    warn!(%error, "doorbell check failed; listing instead");
                    log.record(EventKind::Doorbell, format!("{error:#}"));
                    true
                }
            }
        };

        let mut worked = false;
        if scan {
            next_full_scan = Instant::now() + poll.full_scan;
            seen_doorbell = observed;
            match transport.drain_commands(agent_id).await {
                Ok(commands) => {
                    worked = !commands.is_empty();
                    for command in commands {
                        if let Err(error) = command.validate_routing_for(agent_id) {
                            warn!(command_id = %command.id, %error, "discarded command with unsafe routing fields");
                            // No Response can be sent: the routing fields are
                            // exactly what a response key is built from, so
                            // the heartbeat is the only way to report this.
                            log.record(EventKind::Command, format!("discarded command: {error:#}"));
                            continue;
                        }
                        let response = match command.validate_for_agent(agent_id, policy.max_timeout_secs) {
                            Ok(()) => executor::execute(&command, policy, transport, jobs).await,
                            Err(error) => Response::err(&command, error.to_string()),
                        };
                        if let Err(error) = send_response_with_retry(transport, &response).await {
                            error!(command_id = %command.id, %error, "response permanently lost");
                            // The worst case in the whole system: the command
                            // ran, its effect happened, and the controller will
                            // only ever see a timeout. Surfacing it is what
                            // lets an operator tell this apart from a command
                            // that never started.
                            log.record(
                                EventKind::Response,
                                format!("lost response for command {}: {error:#}", command.id),
                            );
                        }
                    }
                }
                Err(error) => {
                    warn!(%error, "command poll failed");
                    log.record(EventKind::Poll, format!("{error:#}"));
                }
            }
        }

        interval = if worked {
            poll.active
        } else {
            (interval * 2).min(poll.idle)
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("shutdown signal received"); return Ok(()); }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

async fn send_response_with_retry(transport: &Transport, response: &Response) -> Result<()> {
    let mut last = None;
    for delay in [0u64, 1, 2, 4] {
        if delay > 0 { tokio::time::sleep(Duration::from_secs(delay)).await; }
        match transport.send_response(response).await {
            Ok(()) => return Ok(()),
            Err(error) => last = Some(error),
        }
    }
    Err(last.expect("retry loop executes")).context("upload response after retries")
}

fn bounded_env(name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let value = optional_env(name)
        .map_or(Ok(default), |value| value.parse().with_context(|| format!("parse {name}")))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be in {min}..={max}");
    }
    Ok(value)
}
