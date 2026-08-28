//! Authenticated wire types exchanged through the S3 mailbox.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type MsgId = String;
pub type AgentId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub id: MsgId,
    pub agent_id: AgentId,
    pub protocol: u32,
    pub created_at: i64,
    /// Latest time at which the agent may begin this command.
    pub expires_at: i64,
    pub timeout_secs: u64,
    /// Restrict this command to one running agent process.
    ///
    /// Only meaningful when two agents share an identity — from a cloned disk
    /// image, normally. They poll the same mailbox, so an ordinary command
    /// cannot be aimed at one of them; this can. An agent leaves a command
    /// addressed to a different instance where it found it, rather than
    /// consuming it, so the intended one still receives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    pub kind: CommandKind,
}

impl Command {
    pub fn new(
        agent_id: impl Into<AgentId>,
        kind: CommandKind,
        timeout_secs: u64,
        queue_ttl_secs: u64,
    ) -> Self {
        let created_at = now_unix();
        Self {
            id: Uuid::new_v4().to_string(),
            agent_id: agent_id.into(),
            protocol: crate::PROTOCOL_VERSION,
            created_at,
            expires_at: created_at.saturating_add(queue_ttl_secs.min(i64::MAX as u64) as i64),
            timeout_secs,
            instance: None,
            kind,
        }
    }

    /// Address this command to one specific agent process.
    pub fn for_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    pub fn validate_for_agent(&self, expected_agent: &str, max_timeout_secs: u64) -> Result<()> {
        self.validate_routing_for(expected_agent)?;
        if self.protocol != crate::PROTOCOL_VERSION {
            bail!(
                "protocol mismatch: command={}, agent={}",
                self.protocol,
                crate::PROTOCOL_VERSION
            );
        }
        let now = now_unix();
        if self.created_at > now.saturating_add(60) {
            bail!("command timestamp is too far in the future");
        }
        if self.expires_at < now {
            bail!("command expired before execution");
        }
        if self.expires_at < self.created_at {
            bail!("command expiry precedes creation");
        }
        if self.timeout_secs == 0 || self.timeout_secs > max_timeout_secs {
            bail!("command timeout is outside the agent policy");
        }
        Ok(())
    }

    /// Validate fields that are later reused to construct an S3 response key.
    pub fn validate_routing_for(&self, expected_agent: &str) -> Result<()> {
        validate_agent_id(&self.agent_id)?;
        if self.agent_id != expected_agent {
            bail!("command targets a different agent");
        }
        if Uuid::parse_str(&self.id).is_err() {
            bail!("command id is not a UUID");
        }
        // Transfer ids also end up in an S3 key, so they get the same
        // treatment as the agent id and the command id.
        if let CommandKind::PushBlob { transfer, .. } | CommandKind::PullBlob { transfer, .. } =
            &self.kind
        {
            validate_transfer_id(transfer)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommandKind {
    Ping,
    /// Execute one program directly, without a shell. `program` must be in the
    /// agent-side allowlist. Arguments are passed without shell expansion.
    Exec {
        program: String,
        #[serde(default)]
        args: Vec<String>,
        cwd: Option<String>,
        stdin_b64: Option<String>,
    },
    ReadFile {
        path: String,
        max_bytes: Option<usize>,
    },
    WriteFile {
        path: String,
        contents_b64: String,
        append: bool,
    },
    ListDir { path: String },
    /// Write a file the controller has already staged in the bucket.
    ///
    /// Bulk payloads never travel inside a command: a machine-learning wheel
    /// would be tens of millions of tokens if it passed through the MCP result
    /// and the model context. Instead the controller splits the file into
    /// sealed chunks under `blob/<agent>/<transfer>/`, and this command carries
    /// only the manifest needed to reassemble them.
    PushBlob {
        /// Transfer id. Must be a UUID: it becomes part of an S3 key.
        transfer: String,
        dest_path: String,
        chunks: u32,
        total_bytes: u64,
        /// Lowercase hex SHA-256 of the assembled file.
        sha256: String,
    },
    /// Start a long-running program and return immediately.
    ///
    /// `Exec` blocks until the program exits, which caps it at the controller
    /// wait ceiling — fine for a systemctl call, useless for training a model
    /// for six hours. A job detaches: the agent supervises it, streams its
    /// output to a file on the agent, and publishes the outcome when it ends.
    StartJob {
        /// Job id. Must be a UUID: it becomes part of an S3 key.
        job: String,
        program: String,
        #[serde(default)]
        args: Vec<String>,
        cwd: Option<String>,
        /// Kill the job after this long. Unlike a command timeout this can be
        /// hours, and it is the only thing standing between a hung training
        /// run and a process that lives until the machine reboots.
        max_runtime_secs: u64,
        /// Human label carried through to the job list.
        #[serde(default)]
        label: Option<String>,
    },
    /// Report on jobs the agent knows about.
    ListJobs,
    /// Fetch the tail of a running or finished job's output.
    JobOutput {
        job: String,
        /// Bytes from the end of each stream.
        tail_bytes: Option<usize>,
    },
    /// Signal a running job. The agent kills the process group.
    CancelJob { job: String },
    MakeDir {
        path: String,
        /// Create missing intermediate directories too.
        parents: bool,
    },
    Remove {
        path: String,
        /// Required to delete a non-empty directory, so a mistyped path cannot
        /// take a tree with it by accident.
        recursive: bool,
    },
    Move {
        from: String,
        to: String,
    },
    /// Stop consuming commands, permanently, until this process is restarted.
    ///
    /// The manual half of collision handling: when two agents share an
    /// identity they both suspend themselves and one yields automatically, but
    /// the automatic rule cannot know which machine you would rather keep. Send
    /// this to the instance you want to stand down and the other resumes.
    StandDown,
    /// Stage a remote file in the bucket for the controller to collect.
    PullBlob {
        transfer: String,
        path: String,
        /// Refuse rather than truncate above this size, so a partial transfer
        /// is never mistaken for a complete file.
        max_bytes: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: MsgId,
    pub agent_id: AgentId,
    pub protocol: u32,
    pub finished_at: i64,
    pub ok: bool,
    pub error: Option<String>,
    pub payload: ResponsePayload,
}

impl Response {
    pub fn ok(cmd: &Command, payload: ResponsePayload) -> Self {
        Self {
            id: cmd.id.clone(),
            agent_id: cmd.agent_id.clone(),
            protocol: crate::PROTOCOL_VERSION,
            finished_at: now_unix(),
            ok: true,
            error: None,
            payload,
        }
    }

    pub fn err(cmd: &Command, error: impl Into<String>) -> Self {
        Self {
            id: cmd.id.clone(),
            agent_id: cmd.agent_id.clone(),
            protocol: crate::PROTOCOL_VERSION,
            finished_at: now_unix(),
            ok: false,
            error: Some(error.into()),
            payload: ResponsePayload::Empty,
        }
    }

    pub fn validate_for(&self, cmd: &Command) -> Result<()> {
        if self.id != cmd.id || self.agent_id != cmd.agent_id {
            bail!("response correlation mismatch");
        }
        if self.protocol != crate::PROTOCOL_VERSION {
            bail!("response protocol mismatch");
        }
        if self.finished_at > now_unix().saturating_add(60) {
            bail!("response timestamp is too far in the future");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResponsePayload {
    Pong {
        hostname: String,
        os: String,
        agent_version: String,
    },
    Exec {
        stdout_b64: String,
        stderr_b64: String,
        stdout_truncated: bool,
        stderr_truncated: bool,
        exit_code: Option<i32>,
        timed_out: bool,
    },
    FileContents {
        contents_b64: String,
        truncated: bool,
    },
    Written { bytes: usize },
    DirListing { entries: Vec<DirEntry> },
    JobStarted { job: String, pid: Option<u32> },
    JobList { jobs: Vec<JobStatus> },
    JobOutput {
        stdout_tail: String,
        stderr_tail: String,
        stdout_truncated: bool,
        stderr_truncated: bool,
        /// Full output stays on the agent; use pull_file to retrieve it whole.
        stdout_path: String,
        stderr_path: String,
    },
    /// A `PushBlob` landed on disk and its hash matched the manifest.
    BlobWritten { bytes: u64, sha256: String },
    /// A `PullBlob` is staged in the bucket and ready for the controller.
    BlobStaged { chunks: u32, total_bytes: u64, sha256: String },
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatus {
    pub job: String,
    pub label: Option<String>,
    pub program: String,
    pub state: JobState,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub exit_code: Option<i32>,
    /// Present when the job ended badly, so a caller does not have to fetch
    /// the output just to learn what went wrong.
    pub error: Option<String>,
    pub stdout_path: String,
    pub stderr_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Succeeded,
    /// Exited non-zero.
    Failed,
    /// Hit `max_runtime_secs` and was killed.
    TimedOut,
    Cancelled,
    /// The agent restarted while this job was running, so its fate is unknown:
    /// the child died with the old process, but whatever it had already written
    /// to disk is still there.
    Lost,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, JobState::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub agent_id: AgentId,
    pub hostname: String,
    pub os: String,
    pub agent_version: String,
    /// SHA-256 of the binary this agent is running, sampled once at startup.
    ///
    /// The version string cannot answer "did this machine install the release
    /// I published": a rebuilt `0.1.0` carries the same string as the one it
    /// replaced. The hash is the same identity a published release is named by,
    /// so comparing the two is exact. `None` from an agent old enough to
    /// predate the field, which falls the comparison back to the version.
    #[serde(default)]
    pub binary_sha256: Option<String>,
    /// Identifies this *process*, not this machine: a fresh random value on
    /// every start, held only in memory.
    ///
    /// Never persisted, and that is the entire point. Two machines cloned from
    /// one disk image share an agent id, a keypair and every file on disk —
    /// anything written down would be copied along with them. A value minted at
    /// startup is the one thing that cannot be, so it is what makes a duplicate
    /// visible.
    #[serde(default)]
    pub instance: String,
    /// Set when another process was seen holding this same identity, naming its
    /// instance. Both sides report it, so one `list_agents` names both.
    #[serde(default)]
    pub conflict_with: Option<String>,
    /// Whether this process has stopped taking commands — either because it
    /// yielded to the other side of a collision, or because it was told to
    /// stand down.
    #[serde(default)]
    pub suspended: bool,
    pub at: i64,
    pub ttl_secs: i64,
    /// Problems the agent hit outside of any single command, newest last.
    ///
    /// Command failures already travel back in a [`Response`]. These are the
    /// ones that otherwise reach nobody: a heartbeat that failed to upload, a
    /// poll that could not reach S3, a response that was lost after the work
    /// was already done. Riding along on the heartbeat costs no extra request,
    /// and the object is overwritten every interval so nothing accumulates.
    #[serde(default)]
    pub recent_errors: Vec<AgentEvent>,
    /// Jobs currently running.
    #[serde(default)]
    pub jobs_running: u32,
    /// Jobs that finished recently, newest first.
    ///
    /// This is how a long job reports home. An MCP server cannot push anything
    /// to Claude on its own, so the outcome has to be somewhere a later
    /// `list_agents` will find it — and somewhere a status line can poll
    /// without asking the model to do anything.
    #[serde(default)]
    pub jobs_finished: Vec<JobStatus>,
    /// Machine health, sampled when the heartbeat is written.
    ///
    /// This rides along because the heartbeat is already being written every
    /// interval: reporting load and GPU usage costs no extra request. It is
    /// deliberately a snapshot and not a time series — for graphs, deploy a
    /// real exporter (the relay is a good way to install one) rather than
    /// turning this control channel into a metrics pipeline.
    #[serde(default)]
    pub metrics: Option<Metrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metrics {
    /// 1-minute load average.
    pub load_1m: Option<f32>,
    pub cpu_count: Option<u32>,
    pub mem_total_mb: Option<u64>,
    /// Available, not free: free excludes reclaimable cache and reads far
    /// lower than what a program can actually allocate.
    pub mem_available_mb: Option<u64>,
    /// Filesystem holding the job directory — where training output lands, and
    /// therefore the one that fills up.
    pub disk_total_mb: Option<u64>,
    pub disk_free_mb: Option<u64>,
    #[serde(default)]
    pub gpus: Vec<GpuMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuMetrics {
    pub index: u32,
    pub name: String,
    pub utilization_pct: Option<u32>,
    pub memory_used_mb: Option<u64>,
    pub memory_total_mb: Option<u64>,
    pub temperature_c: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub at: i64,
    pub kind: EventKind,
    pub message: String,
}

/// Idempotent agent log segment. The controller writes it at `offset`, so an
/// agent restart and replay cannot duplicate data in the centralized log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogChunk {
    pub agent_id: AgentId,
    pub source: String,
    pub offset: u64,
    pub data_b64: String,
    pub at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Could not publish a heartbeat.
    Heartbeat,
    /// Could not list or fetch commands.
    Poll,
    /// Could not check the doorbell.
    Doorbell,
    /// A response was lost after the command had already run — the dangerous
    /// one, because the side effect happened and the controller saw a timeout.
    Response,
    /// A command was rejected before execution.
    Command,
    /// A bulk transfer failed.
    Transfer,
    /// A published update could not be checked or installed — or was held back
    /// because the agent was busy. The only channel that reports this: an
    /// update is not a command, so there is no `Response` to carry it home.
    Update,
    /// Another process is using this agent's identity. Reported rather than
    /// worked around, because the damage is silent: commands meant for one
    /// machine run on another, or on both.
    Collision,
}

impl Heartbeat {
    pub fn is_stale(&self) -> bool {
        now_unix().saturating_sub(self.at) > self.ttl_secs
    }
}

/// What the controller published for one agent to install, and everything that
/// agent needs to fetch and trust it.
///
/// A fleet update is deliberately *not* a [`Command`]: a command expires after
/// `queue_ttl_secs` and is consumed at most once, so any machine that happened
/// to be offline during the rollout would never see it. This manifest instead
/// sits in the bucket until it is replaced, and every agent picks it up on its
/// own schedule — including one that comes back a week later.
///
/// The binary itself is *not* per-agent. Sealing a 15 MB artifact separately
/// for every machine would multiply the controller's upload by the fleet size,
/// so the payload is encrypted once under a random per-release key at a shared
/// prefix, and only that key travels per-agent, inside this manifest, which is
/// sealed with the agent's own key like every other object addressed to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateManifest {
    pub agent_id: AgentId,
    /// Human-readable label for the build, e.g. `0.2.0`. Reporting only: what
    /// actually decides whether an agent installs this is `sha256`.
    pub version: String,
    /// `"{os} {arch}"`, exactly as the agent reports it. An agent that does not
    /// match refuses the update rather than exec-ing a foreign binary.
    pub target: String,
    /// Names the shared chunk prefix. A UUID: it becomes part of an S3 key.
    pub release: String,
    pub chunks: u32,
    pub total_bytes: u64,
    /// Lowercase hex SHA-256 of the assembled binary. This is the identity of
    /// the release: an agent whose own binary already hashes to this is up to
    /// date, which stays correct even when a version string is reused.
    pub sha256: String,
    /// Base64 32-byte key the release chunks are sealed with.
    pub release_key_b64: String,
    pub published_at: i64,
}

/// Tiny object the controller overwrites whenever it enqueues a command, so the
/// agent can detect new work with a HEAD instead of a LIST. The agent compares
/// ETags and never reads the body; the fields exist only so the object is a
/// well-formed sealed message like everything else in the bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Doorbell {
    pub agent_id: AgentId,
    pub at: i64,
}

pub fn validate_agent_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 64 {
        bail!("agent id must contain 1..=64 characters");
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        bail!("agent id may contain only ASCII letters, digits, '-' and '_'");
    }
    Ok(())
}

/// Transfer ids become a component of an S3 key, so they are restricted to
/// UUIDs for the same reason agent ids are restricted to a safe alphabet.
pub fn validate_transfer_id(value: &str) -> Result<()> {
    if Uuid::parse_str(value).is_err() {
        bail!("transfer id must be a UUID");
    }
    Ok(())
}

pub fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_agent_ids_that_can_escape_prefix() {
        assert!(validate_agent_id("server-01_prod").is_ok());
        assert!(validate_agent_id("../server").is_err());
        assert!(validate_agent_id("a/b").is_err());
        assert!(validate_agent_id("").is_err());
    }
}
