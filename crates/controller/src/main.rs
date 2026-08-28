mod admin;
mod log_store;
mod registry;
mod status_cache;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use common::{
    blob, optional_env, validate_agent_id, Command, CommandKind, Crypto, Response, ResponsePayload,
    S3Config, Transport,
};

/// Default ceiling for `pull_file` when the caller does not give one. Large
/// enough for the archives this exists to move, small enough that a mistaken
/// path does not try to drag a disk image through the bucket.
const DEFAULT_PULL_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock},
    schemars, tool, tool_router,
    transport::stdio,
    ErrorData as McpError, ServiceExt,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Clone)]
struct Controller {
    transports: Arc<HashMap<String, Transport>>,
    allowed_agents: HashSet<String>,
    queue_ttl_secs: u64,
    max_exec_secs: u64,
    max_wait_secs: u64,
    /// Transfers get their own, much larger ceiling: moving a few hundred
    /// megabytes legitimately takes longer than any command should.
    max_transfer_secs: u64,
    poll_interval: Duration,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct AgentArg {
    /// Agent identifier configured in RELAY_AGENT_ID on the remote server.
    agent_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ExecArgs {
    agent_id: String,
    /// Exact program name or path present in the agent's allowlist.
    program: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    /// Optional UTF-8 text sent to the program's standard input.
    stdin: Option<String>,
    /// Execution timeout; capped by both controller and agent policy.
    timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadFileArgs {
    agent_id: String,
    path: String,
    max_bytes: Option<usize>,
    /// Return base64 instead of requiring UTF-8 text.
    #[serde(default)]
    binary: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WriteFileArgs {
    agent_id: String,
    path: String,
    /// UTF-8 text by default, or base64 when `base64` is true.
    content: String,
    #[serde(default)]
    base64: bool,
    #[serde(default)]
    append: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListDirArgs {
    agent_id: String,
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct StartJobArgs {
    agent_id: String,
    /// Exact program path, subject to the same rules as exec.
    program: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    /// Kill the job after this long. Defaults to 6 hours.
    max_runtime_secs: Option<u64>,
    /// Short human label, e.g. "resnet50 finetune".
    label: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct JobArg {
    agent_id: String,
    job: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct JobOutputArgs {
    agent_id: String,
    job: String,
    /// Bytes from the end of each stream. Defaults to 8 KiB.
    tail_bytes: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MakeDirArgs {
    agent_id: String,
    path: String,
    /// Create missing intermediate directories.
    #[serde(default = "default_true")]
    parents: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RemoveArgs {
    agent_id: String,
    path: String,
    /// Required to delete a directory that is not empty.
    #[serde(default)]
    recursive: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MoveArgs {
    agent_id: String,
    from: String,
    to: String,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PushFileArgs {
    agent_id: String,
    /// Path on the machine running this MCP server.
    local_path: String,
    /// Destination on the agent, inside its allowed roots. Overwritten if it
    /// already exists.
    remote_path: String,
    /// Whole-transfer timeout; capped by CONTROL_MAX_TRANSFER_SECS.
    timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PullFileArgs {
    agent_id: String,
    /// Source on the agent, inside its allowed roots.
    remote_path: String,
    /// Destination on the machine running this MCP server.
    local_path: String,
    /// Refuse rather than truncate above this size. Defaults to 1 GiB.
    max_bytes: Option<u64>,
    timeout_secs: Option<u64>,
}

#[tool_router(server_handler)]
impl Controller {
    #[tool(description = "List currently live relay agents. Stale heartbeat objects are deleted.")]
    async fn list_agents(&self) -> Result<CallToolResult, McpError> {
        let mut agents = Vec::new();
        for (id, transport) in self.transports.iter() {
            match transport.read_heartbeat(id).await {
                Ok(Some(agent)) => agents.push(agent),
                Ok(None) => {}
                Err(error) => return Ok(tool_error(format!("failed to read {id}: {error:#}"))),
            }
        }
        agents.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        self.json_result(&agents)
    }

    #[tool(description = "Check that a specific remote agent is online and responding through S3.")]
    async fn ping(&self, Parameters(args): Parameters<AgentArg>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::Ping, 10).await
    }

    #[tool(description = "Run one allowlisted program directly on the remote agent. No shell is used and arguments are not expanded.")]
    async fn exec(&self, Parameters(args): Parameters<ExecArgs>) -> Result<CallToolResult, McpError> {
        let timeout = args.timeout_secs.unwrap_or(60).clamp(1, self.max_exec_secs);
        let stdin_b64 = args.stdin.map(|value| B64.encode(value.as_bytes()));
        self.run(args.agent_id, CommandKind::Exec {
            program: args.program, args: args.args, cwd: args.cwd, stdin_b64,
        }, timeout).await
    }

    #[tool(description = "Read a file under an agent-side allowed root. Text mode validates UTF-8; binary mode returns base64.")]
    async fn read_file(&self, Parameters(args): Parameters<ReadFileArgs>) -> Result<CallToolResult, McpError> {
        let binary = args.binary;
        let result = self.relay(args.agent_id, CommandKind::ReadFile {
            path: args.path, max_bytes: args.max_bytes,
        }, 30).await;
        match result {
            Ok(response) if response.ok => match response.payload {
                ResponsePayload::FileContents { contents_b64, truncated } if binary => {
                    self.json_result(&json!({ "content_base64": contents_b64, "truncated": truncated }))
                }
                ResponsePayload::FileContents { contents_b64, truncated } => match B64.decode(contents_b64) {
                    Ok(bytes) => match String::from_utf8(bytes) {
                        Ok(content) => self.json_result(&json!({ "content": content, "truncated": truncated })),
                        Err(_) => Ok(tool_error("file is not valid UTF-8; retry with binary=true")),
                    },
                    Err(error) => Ok(tool_error(format!("agent returned invalid base64: {error}"))),
                },
                _ => Ok(tool_error("agent returned an unexpected payload")),
            },
            Ok(response) => Ok(response_error(response)),
            Err(error) => Ok(tool_error(format!("relay failed: {error:#}"))),
        }
    }

    #[tool(description = "Create, overwrite, or append a file under an agent-side allowed root.")]
    async fn write_file(&self, Parameters(args): Parameters<WriteFileArgs>) -> Result<CallToolResult, McpError> {
        let contents_b64 = if args.base64 {
            if let Err(error) = B64.decode(&args.content) {
                return Ok(tool_error(format!("content is not valid base64: {error}")));
            }
            args.content
        } else {
            B64.encode(args.content.as_bytes())
        };
        self.run(args.agent_id, CommandKind::WriteFile {
            path: args.path, contents_b64, append: args.append,
        }, 30).await
    }

    #[tool(description = "List a directory under an agent-side allowed root.")]
    async fn list_dir(&self, Parameters(args): Parameters<ListDirArgs>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::ListDir { path: args.path }, 30).await
    }

    #[tool(description = "Start a long-running program on the remote agent and return immediately with a job id. Use this instead of exec for anything that outlasts a few minutes — model training, large builds, batch jobs. Output streams to a file on the agent; poll with list_jobs / job_output. The outcome also rides home on the heartbeat, so list_agents shows finished jobs even if nothing polled.")]
    async fn start_job(&self, Parameters(args): Parameters<StartJobArgs>) -> Result<CallToolResult, McpError> {
        let job = blob::new_transfer_id();
        let max_runtime_secs = args.max_runtime_secs.unwrap_or(21_600); // 6h
        self.run(args.agent_id, CommandKind::StartJob {
            job,
            program: args.program,
            args: args.args,
            cwd: args.cwd,
            max_runtime_secs,
            label: args.label,
        }, 60).await
    }

    #[tool(description = "List jobs on the remote agent: running ones first, then recently finished with their exit codes.")]
    async fn list_jobs(&self, Parameters(args): Parameters<AgentArg>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::ListJobs, 30).await
    }

    #[tool(description = "Fetch the tail of a job's stdout and stderr. The full logs stay on the agent; use pull_file with the returned paths to retrieve them whole.")]
    async fn job_output(&self, Parameters(args): Parameters<JobOutputArgs>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::JobOutput {
            job: args.job, tail_bytes: args.tail_bytes,
        }, 30).await
    }

    #[tool(description = "Kill a running job on the remote agent.")]
    async fn cancel_job(&self, Parameters(args): Parameters<JobArg>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::CancelJob { job: args.job }, 30).await
    }

    #[tool(description = "Create a directory on the remote agent. Succeeds if it already exists.")]
    async fn make_dir(&self, Parameters(args): Parameters<MakeDirArgs>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::MakeDir {
            path: args.path, parents: args.parents,
        }, 30).await
    }

    #[tool(description = "Delete a file or directory on the remote agent. Deleting a non-empty directory requires recursive=true.")]
    async fn remove(&self, Parameters(args): Parameters<RemoveArgs>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::Remove {
            path: args.path, recursive: args.recursive,
        }, 60).await
    }

    #[tool(description = "Move or rename a path on the remote agent. Both ends must be inside the allowed roots.")]
    async fn move_path(&self, Parameters(args): Parameters<MoveArgs>) -> Result<CallToolResult, McpError> {
        self.run(args.agent_id, CommandKind::Move {
            from: args.from, to: args.to,
        }, 60).await
    }

    #[tool(description = "Copy a file from this machine to the remote agent, streaming it through the bucket. Bulk data never enters the conversation, so this is how to move large files such as wheels, archives or installers. Missing destination directories are created.")]
    async fn push_file(&self, Parameters(args): Parameters<PushFileArgs>) -> Result<CallToolResult, McpError> {
        match self.push(args).await {
            Ok(value) => self.json_result(&value),
            Err(error) => Ok(tool_error(format!("push failed: {error:#}"))),
        }
    }

    #[tool(description = "Copy a file from the remote agent to this machine, streaming it through the bucket.")]
    async fn pull_file(&self, Parameters(args): Parameters<PullFileArgs>) -> Result<CallToolResult, McpError> {
        match self.pull(args).await {
            Ok(value) => self.json_result(&value),
            Err(error) => Ok(tool_error(format!("pull failed: {error:#}"))),
        }
    }

    async fn push(&self, args: PushFileArgs) -> Result<serde_json::Value> {
        self.validate_agent(&args.agent_id)?;
        let transfer = blob::new_transfer_id();
        let source = PathBuf::from(&args.local_path);

        let transport = self.transport_for(&args.agent_id)?;
        let manifest = match blob::stage_file(transport, &args.agent_id, &transfer, &source).await {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = transport.delete_blob(&args.agent_id, &transfer).await;
                return Err(error);
            }
        };

        let timeout = args.timeout_secs.unwrap_or(self.max_transfer_secs).clamp(1, self.max_transfer_secs);
        let outcome = self.relay_with(
            args.agent_id.clone(),
            CommandKind::PushBlob {
                transfer: transfer.clone(),
                dest_path: args.remote_path.clone(),
                chunks: manifest.chunks,
                total_bytes: manifest.total_bytes,
                sha256: manifest.sha256.clone(),
            },
            timeout,
            self.transfer_wait_cap(timeout),
        ).await;

        match outcome {
            Ok(response) if response.ok => Ok(json!({
                "remote_path": args.remote_path,
                "bytes": manifest.total_bytes,
                "chunks": manifest.chunks,
                "sha256": manifest.sha256,
            })),
            Ok(response) => {
                // The agent rejected it, so nothing is holding the chunks.
                let _ = transport.delete_blob(&args.agent_id, &transfer).await;
                anyhow::bail!(
                    "{}",
                    response.error.unwrap_or_else(|| "agent rejected the transfer".to_owned())
                )
            }
            Err(error) => {
                // Ambiguous: the agent may still be assembling right now, and
                // deleting chunks would corrupt a transfer that is about to
                // succeed. Leave them to the bucket lifecycle rule.
                Err(error)
            }
        }
    }

    async fn pull(&self, args: PullFileArgs) -> Result<serde_json::Value> {
        self.validate_agent(&args.agent_id)?;
        let transfer = blob::new_transfer_id();
        let timeout = args.timeout_secs.unwrap_or(self.max_transfer_secs).clamp(1, self.max_transfer_secs);
        let response = self.relay_with(
            args.agent_id.clone(),
            CommandKind::PullBlob {
                transfer: transfer.clone(),
                path: args.remote_path.clone(),
                max_bytes: args.max_bytes.unwrap_or(DEFAULT_PULL_LIMIT_BYTES),
            },
            timeout,
            self.transfer_wait_cap(timeout),
        ).await?;

        if !response.ok {
            anyhow::bail!(
                "{}",
                response.error.unwrap_or_else(|| "agent could not stage the file".to_owned())
            );
        }
        let ResponsePayload::BlobStaged { chunks, total_bytes, sha256 } = response.payload else {
            anyhow::bail!("agent returned an unexpected payload");
        };

        let manifest = blob::Manifest { chunks, total_bytes, sha256: sha256.clone() };
        let dest = PathBuf::from(&args.local_path);
        if let Some(parent) = dest.parent().filter(|path| !path.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create local directory {}", parent.display()))?;
        }
        let transport = self.transport_for(&args.agent_id)?;
        let assembled = blob::assemble_file(transport, &args.agent_id, &transfer, &manifest, &dest).await;

        // The agent cannot know when this side is finished, so cleanup is ours
        // either way.
        let _ = transport.delete_blob(&args.agent_id, &transfer).await;
        let bytes = assembled?;
        Ok(json!({
            "local_path": args.local_path,
            "bytes": bytes,
            "chunks": chunks,
            "sha256": sha256,
        }))
    }

    /// A transfer may legitimately outlast `max_wait_secs`, which is sized for
    /// commands, so give it room for queueing plus the transfer itself.
    fn transfer_wait_cap(&self, timeout_secs: u64) -> u64 {
        self.queue_ttl_secs.saturating_add(timeout_secs).saturating_add(30)
    }

    async fn run(&self, agent_id: String, kind: CommandKind, timeout_secs: u64) -> Result<CallToolResult, McpError> {
        match self.relay(agent_id, kind, timeout_secs).await {
            Ok(response) if response.ok => self.render_success(response),
            Ok(response) => Ok(response_error(response)),
            Err(error) => Ok(tool_error(format!("relay failed: {error:#}"))),
        }
    }

    async fn relay(&self, agent_id: String, kind: CommandKind, timeout_secs: u64) -> Result<Response> {
        let capped = timeout_secs.clamp(1, self.max_exec_secs);
        self.relay_with(agent_id, kind, capped, self.max_wait_secs).await
    }

    /// `wait_cap_secs` bounds how long the controller blocks on the response.
    /// Commands and transfers use very different values, so it is explicit.
    async fn relay_with(
        &self,
        agent_id: String,
        kind: CommandKind,
        timeout_secs: u64,
        wait_cap_secs: u64,
    ) -> Result<Response> {
        self.validate_agent(&agent_id)?;
        let transport = self.transport_for(&agent_id)?;
        let timeout_secs = timeout_secs.max(1);
        // Audit line: subject, action, key and result only. Command payloads —
        // file contents, stdin, program arguments — are deliberately absent.
        let action = action_of(&kind);
        let started = std::time::Instant::now();
        let command = Command::new(agent_id, kind, timeout_secs, self.queue_ttl_secs);
        tracing::info!(
            agent = %command.agent_id, action, command_id = %command.id, timeout_secs,
            "dispatch"
        );
        transport.send_command(&command).await?;
        let wait_secs = self.queue_ttl_secs.saturating_add(timeout_secs).saturating_add(10)
            .min(wait_cap_secs);
        let response = transport.await_response(
            &command, Duration::from_secs(wait_secs), self.poll_interval,
        ).await?;
        let elapsed_ms = started.elapsed().as_millis();
        match response {
            Some(response) => {
                tracing::info!(
                    agent = %command.agent_id, action, command_id = %command.id,
                    ok = response.ok, elapsed_ms,
                    error = response.error.as_deref().unwrap_or(""),
                    "complete"
                );
                Ok(response)
            }
            None => {
                // If the agent never claimed the object, remove it now. If it
                // already claimed it, S3 delete is harmless and the command
                // remains at-most-once.
                let _ = transport.delete_command(&command).await;
                // Worth a warning rather than an info: a timeout does NOT mean
                // the command did not run, and the log is where someone
                // reconstructs that later.
                tracing::warn!(
                    agent = %command.agent_id, action, command_id = %command.id, elapsed_ms,
                    "timeout; the command may still have executed"
                );
                anyhow::bail!("timed out waiting for agent response; command id {}", command.id)
            }
        }
    }

    fn validate_agent(&self, agent_id: &str) -> Result<()> {
        validate_agent_id(agent_id)?;
        if !self.allowed_agents.contains(agent_id) {
            anyhow::bail!("agent is not present in CONTROL_ALLOWED_AGENTS");
        }
        Ok(())
    }

    fn transport_for(&self, agent_id: &str) -> Result<&Transport> {
        self.validate_agent(agent_id)?;
        self.transports.get(agent_id).ok_or_else(|| anyhow::anyhow!("agent has no encryption key"))
    }

    fn render_success(&self, response: Response) -> Result<CallToolResult, McpError> {
        match response.payload {
            ResponsePayload::Exec {
                stdout_b64, stderr_b64, stdout_truncated, stderr_truncated,
                exit_code, timed_out,
            } => {
                let stdout = decode_lossy(&stdout_b64);
                let stderr = decode_lossy(&stderr_b64);
                self.json_result(&json!({
                    "stdout": stdout, "stderr": stderr,
                    "stdout_truncated": stdout_truncated, "stderr_truncated": stderr_truncated,
                    "exit_code": exit_code, "timed_out": timed_out,
                }))
            }
            payload => self.json_result(&payload),
        }
    }

    fn json_result<T: serde::Serialize>(&self, value: &T) -> Result<CallToolResult, McpError> {
        match serde_json::to_string_pretty(value) {
            Ok(text) => Ok(CallToolResult::success(vec![ContentBlock::text(text)])),
            Err(error) => Ok(tool_error(format!("could not serialize result: {error}"))),
        }
    }
}

/// Action name for the audit log. Never includes payload data.
fn action_of(kind: &CommandKind) -> &'static str {
    match kind {
        CommandKind::Ping => "ping",
        CommandKind::Exec { .. } => "exec",
        CommandKind::ReadFile { .. } => "read_file",
        CommandKind::WriteFile { .. } => "write_file",
        CommandKind::ListDir { .. } => "list_dir",
        CommandKind::MakeDir { .. } => "make_dir",
        CommandKind::Remove { .. } => "remove",
        CommandKind::Move { .. } => "move",
        CommandKind::PushBlob { .. } => "push_file",
        CommandKind::PullBlob { .. } => "pull_file",
        CommandKind::StartJob { .. } => "start_job",
        CommandKind::ListJobs => "list_jobs",
        CommandKind::JobOutput { .. } => "job_output",
        CommandKind::CancelJob { .. } => "cancel_job",
    }
}

fn tool_error(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message.into())])
}

fn response_error(response: Response) -> CallToolResult {
    tool_error(response.error.unwrap_or_else(|| "agent operation failed".to_owned()))
}

fn decode_lossy(value: &str) -> String {
    B64.decode(value).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|error| format!("<invalid base64 from agent: {error}>"))
}

/// Set up logging before anything else can fail.
///
/// stderr alone is not enough here: Claude Code owns this process and its
/// stderr is not somewhere a person looks. A file is where you actually go
/// after "the agent did something strange an hour ago".
///
/// stdout is never a destination — it carries MCP protocol frames.
fn init_logging() -> Result<Option<PathBuf>> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false);

    // Read straight from the environment: the config file is not parsed yet,
    // and logging has to be up before that can report anything.
    let path = std::env::var("RELAY_LOG_FILE").ok().filter(|value| !value.is_empty());
    let Some(path) = path else {
        tracing_subscriber::registry().with(filter).with(stderr_layer).init();
        return Ok(None);
    };

    let path = PathBuf::from(path);
    if let Some(parent) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create log directory {}", parent.display()))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open log file {}", path.display()))?;
    // No built-in rotation: append forever and let logrotate handle it, rather
    // than reinventing rotation badly inside a process that has no scheduler.
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file)
        .with_ansi(false);
    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer)
        .init();
    Ok(Some(path))
}

#[tokio::main]
async fn main() -> Result<()> {
    if admin::dispatch()? { return Ok(()); }
    let log_file = init_logging()?;
    if let Some(path) = &log_file {
        tracing::info!(path = %path.display(), "logging to file");
    }

    // Config file first, so every setting below can come from either source.
    // Logging goes to stderr; stdout carries MCP frames only.
    if let Some(path) = common::config::init_config()? {
        tracing::info!(config = %path.display(), "loaded config file");
    }

    let s3 = S3Config::from_env()?;
    let database = optional_env("CONTROL_DATABASE").map(PathBuf::from).unwrap_or_else(registry::default_path);
    let registry = registry::Registry::open(database.clone())?;
    let paired = registry.agents()?;
    let mut transports = HashMap::new();
    if paired.is_empty() {
        let legacy: HashSet<String> = optional_env("CONTROL_ALLOWED_AGENTS").unwrap_or_default()
            .split(',').map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned).collect();
        let crypto = Crypto::from_env().context("no paired agents; legacy RELAY_SHARED_KEY is required")?;
        for agent in legacy {
            validate_agent_id(&agent)?;
            transports.insert(agent, Transport::connect(&s3, crypto.clone()).await?);
        }
    } else {
        let controller_private = common::required_env("CONTROL_PRIVATE_KEY")?;
        for agent in paired {
            validate_agent_id(&agent.id)?;
            let shared = common::pairing::derive_key(&controller_private, &agent.public_key, &agent.id)?;
            transports.insert(agent.id, Transport::connect(&s3, Crypto::from_base64(&shared, "x25519-v1")?).await?);
        }
    }
    if transports.is_empty() { anyhow::bail!("no agents enrolled; run s3-relay-mcp add <pairing-code>"); }
    let allowed_agents: HashSet<String> = transports.keys().cloned().collect();
    let transports = Arc::new(transports);

    let controller = Controller {
        transports: transports.clone(),
        allowed_agents,
        queue_ttl_secs: bounded_env("CONTROL_QUEUE_TTL_SECS", 120, 1, 3_600)?,
        max_exec_secs: bounded_env("CONTROL_MAX_EXEC_SECS", 300, 1, 3_600)?,
        max_wait_secs: bounded_env("CONTROL_MAX_WAIT_SECS", 430, 10, 7_200)?,
        max_transfer_secs: bounded_env("CONTROL_MAX_TRANSFER_SECS", 1_800, 10, 86_400)?,
        // Starting interval only; await_response doubles it up to a ceiling.
        poll_interval: Duration::from_millis(bounded_env("CONTROL_POLL_MS", 200, 100, 60_000)?),
    };
    // Refresh a status file in the background so a status line can show agent
    // and job state without the model having to call anything. Set
    // RELAY_STATUS_INTERVAL_SECS=0 to turn the polling off.
    let status_interval = bounded_env("RELAY_STATUS_INTERVAL_SECS", 30, 0, 3_600)?;
    if status_interval > 0 {
        let path = optional_env("RELAY_STATUS_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(status_cache::default_path);
        tracing::info!(path = %path.display(), interval_secs = status_interval, "status cache enabled");
        let log_dir = optional_env("CONTROL_LOG_DIR").map(PathBuf::from)
            .unwrap_or_else(|| database.parent().unwrap_or_else(|| std::path::Path::new(".")).join("logs"));
        let log_store = log_store::LogStore::new(log_dir, database.clone())?;
        tokio::spawn(status_cache::cache_loop(
            transports,
            controller.allowed_agents.clone(),
            registry,
            log_store,
            path,
            Duration::from_secs(status_interval),
        ));
    }

    let service = controller.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

fn bounded_env(name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let value = optional_env(name)
        .map_or(Ok(default), |value| value.parse().with_context(|| format!("parse {name}")))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be in {min}..={max}");
    }
    Ok(value)
}
