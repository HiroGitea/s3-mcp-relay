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
/// Ceiling for a published agent binary. Comfortably above any real build, and
/// low enough that pointing this at a disk image fails immediately instead of
/// after uploading it to the whole fleet's bucket.
const MAX_UPDATE_BYTES: u64 = 512 * 1024 * 1024;
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PublishUpdateArgs {
    /// relay-agent binary on this machine, built for the target below.
    local_path: String,
    /// Agents to offer it to. Omit for every enrolled agent. Offline agents are
    /// included on purpose: the manifest waits in the bucket until they return.
    agents: Option<Vec<String>>,
    /// Platform the binary is built for, as "{os} {arch}", e.g. "linux x86_64".
    /// Read from the executable header when omitted. Agents whose platform
    /// differs refuse the release, so a mixed fleet takes one call per
    /// architecture.
    target: Option<String>,
    /// Version label, for reporting only. Defaults to this controller's own
    /// version. What actually decides whether an agent installs is the hash.
    version: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RetractUpdateArgs {
    /// Agents to withdraw the offer from. Omit for all of them.
    agents: Option<Vec<String>>,
    /// Also delete the uploaded binary. Only do this once no agent still needs
    /// it: an agent that has not checked in yet would find its chunks gone.
    #[serde(default)]
    delete_release: bool,
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

    #[tool(description = "Publish a new relay-agent binary to the fleet through the bucket. The binary is uploaded once no matter how many agents there are, and each agent installs it on its own schedule — including machines that are offline right now, which pick it up when they return. An agent whose platform does not match refuses it, so a mixed fleet needs one call per architecture. Agents defer the restart until their running jobs finish, and roll back if the new binary fails to start.")]
    async fn publish_update(&self, Parameters(args): Parameters<PublishUpdateArgs>) -> Result<CallToolResult, McpError> {
        match self.publish(args).await {
            Ok(value) => self.json_result(&value),
            Err(error) => Ok(tool_error(format!("publish failed: {error:#}"))),
        }
    }

    #[tool(description = "Show what each agent is running against what has been published to it: version, platform, and whether the published release has been installed yet.")]
    async fn update_status(&self) -> Result<CallToolResult, McpError> {
        let mut rows = Vec::new();
        for (id, transport) in self.transports.iter() {
            let heartbeat = transport.read_heartbeat(id).await.ok().flatten();
            let manifest = match transport.read_update_manifest(id).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    rows.push(json!({ "agent_id": id, "error": format!("{error:#}") }));
                    continue;
                }
            };
            rows.push(json!({
                "agent_id": id,
                "online": heartbeat.is_some(),
                "running_version": heartbeat.as_ref().map(|hb| hb.agent_version.clone()),
                "platform": heartbeat.as_ref().map(|hb| hb.os.clone()),
                "jobs_running": heartbeat.as_ref().map(|hb| hb.jobs_running),
                "published": manifest.as_ref().map(|m| json!({
                    "version": m.version,
                    "target": m.target,
                    "sha256": m.sha256,
                    "published_at": m.published_at,
                })),
                // Only meaningful once the agent has checked in since the
                // publish; an offline agent reports the version it last ran.
                "installed": match (&manifest, &heartbeat) {
                    (Some(m), Some(hb)) => Some(hb.agent_version == m.version),
                    _ => None,
                },
            }));
        }
        rows.sort_by(|a, b| a["agent_id"].as_str().cmp(&b["agent_id"].as_str()));
        self.json_result(&rows)
    }

    #[tool(description = "Withdraw a published update so agents that have not installed it yet will not. Agents that already restarted into it are unaffected — publish the previous binary to move those back.")]
    async fn retract_update(&self, Parameters(args): Parameters<RetractUpdateArgs>) -> Result<CallToolResult, McpError> {
        let agents = match self.targets(args.agents) {
            Ok(agents) => agents,
            Err(error) => return Ok(tool_error(format!("{error:#}"))),
        };
        let mut releases: HashSet<String> = HashSet::new();
        let mut retracted = Vec::new();
        for agent in &agents {
            let Ok(transport) = self.transport_for(agent) else { continue };
            if args.delete_release {
                if let Ok(Some(manifest)) = transport.read_update_manifest(agent).await {
                    releases.insert(manifest.release);
                }
            }
            match transport.delete_update_manifest(agent).await {
                Ok(()) => retracted.push(agent.clone()),
                Err(error) => return Ok(tool_error(format!("could not retract {agent}: {error:#}"))),
            }
        }
        let mut deleted = Vec::new();
        for release in releases {
            if let Some(transport) = self.transports.values().next() {
                match transport.delete_release(&release).await {
                    Ok(()) => deleted.push(release),
                    Err(error) => return Ok(tool_error(format!("could not delete release: {error:#}"))),
                }
            }
        }
        self.json_result(&json!({ "retracted": retracted, "releases_deleted": deleted }))
    }

    async fn publish(&self, args: PublishUpdateArgs) -> Result<serde_json::Value> {
        let source = PathBuf::from(&args.local_path);
        let metadata = tokio::fs::metadata(&source)
            .await
            .with_context(|| format!("read {}", source.display()))?;
        if !metadata.is_file() {
            anyhow::bail!("{} is not a file", source.display());
        }
        if metadata.len() == 0 {
            anyhow::bail!("{} is empty", source.display());
        }
        if metadata.len() > MAX_UPDATE_BYTES {
            anyhow::bail!(
                "{} is {} bytes, over the {MAX_UPDATE_BYTES} byte ceiling for an update",
                source.display(),
                metadata.len()
            );
        }

        let target = match args.target {
            Some(target) => target,
            None => detect_target(&source).await?.ok_or_else(|| {
                anyhow::anyhow!(
                    "could not read a platform from {}; pass target explicitly, e.g. \"linux x86_64\"",
                    source.display()
                )
            })?,
        };
        let agents = self.targets(args.agents)?;
        let version = args.version.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

        // One upload for the whole fleet: the binary is sealed with a one-off
        // release key at a shared prefix, and only that key travels per agent.
        let release = blob::new_transfer_id();
        let release_key = common::update::new_release_key();
        let crypto = Crypto::from_base64(&release_key, "release")?;
        let staging = self.transports.values().next()
            .context("no agents enrolled")?;
        let (chunks, total_bytes, sha256) =
            match common::update::stage_release(staging, &release, &crypto, &source).await {
                Ok(staged) => staged,
                Err(error) => {
                    let _ = staging.delete_release(&release).await;
                    return Err(error);
                }
            };

        let published_at = common::protocol::now_unix();
        let mut results = Vec::new();
        let mut delivered = 0usize;
        for agent in &agents {
            let transport = self.transport_for(agent)?;
            let heartbeat = transport.read_heartbeat(agent).await.ok().flatten();
            let manifest = common::UpdateManifest {
                agent_id: agent.clone(),
                version: version.clone(),
                target: target.clone(),
                release: release.clone(),
                chunks,
                total_bytes,
                sha256: sha256.clone(),
                release_key_b64: release_key.clone(),
                published_at,
            };
            let outcome = transport.put_update_manifest(&manifest).await;
            let published = outcome.is_ok();
            if published {
                delivered += 1;
            }
            results.push(json!({
                "agent_id": agent,
                "published": published,
                "error": outcome.err().map(|error| format!("{error:#}")),
                "online": heartbeat.is_some(),
                "running_version": heartbeat.as_ref().map(|hb| hb.agent_version.clone()),
                // False here means the agent will refuse this release. Reported
                // rather than blocked, because an agent that has never checked
                // in has no known platform and is still worth publishing to.
                "platform_matches": heartbeat.as_ref().map(|hb| hb.os == target),
            }));
        }

        if delivered == 0 {
            // Nothing can reach the chunks, so leave nothing behind.
            let _ = staging.delete_release(&release).await;
            anyhow::bail!("no agent could be given the update; the release was removed");
        }

        tracing::info!(
            release = %release, %version, %target, agents = delivered, bytes = total_bytes,
            "published update"
        );
        Ok(json!({
            "release": release,
            "version": version,
            "target": target,
            "sha256": sha256,
            "bytes": total_bytes,
            "chunks": chunks,
            "published_at": published_at,
            "agents": results,
        }))
    }

    /// Resolve an optional agent list to a validated set, defaulting to all.
    fn targets(&self, requested: Option<Vec<String>>) -> Result<Vec<String>> {
        let mut agents = match requested {
            Some(agents) => {
                for agent in &agents {
                    self.validate_agent(agent)?;
                }
                agents
            }
            None => self.allowed_agents.iter().cloned().collect(),
        };
        agents.sort();
        agents.dedup();
        if agents.is_empty() {
            anyhow::bail!("no agents selected");
        }
        Ok(agents)
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

/// Read `"{os} {arch}"` out of an executable header.
///
/// Publishing the wrong architecture is the easy mistake to make here — the
/// binaries for every target come out of one CI run with similar names — and
/// the agent's own check would catch it only after a round trip. The file
/// already says what it is, so read it rather than trusting the caller.
///
/// Returns `None` for anything unrecognised, which asks the caller for an
/// explicit target rather than guessing.
async fn detect_target(path: &std::path::Path) -> Result<Option<String>> {
    use tokio::io::AsyncReadExt;

    let mut head = vec![0u8; 1024];
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let read = file.read(&mut head).await.context("read executable header")?;
    head.truncate(read);
    Ok(target_from_header(&head))
}

fn target_from_header(head: &[u8]) -> Option<String> {
    // ELF: e_machine is a little-endian u16 at offset 18 on every target here.
    // The OS is not reliably encoded (EI_OSABI reads as SysV on Linux), so ELF
    // is treated as Linux, which is what an agent host actually is.
    if head.len() >= 20 && head[..4] == [0x7f, b'E', b'L', b'F'] {
        let arch = match u16::from_le_bytes([head[18], head[19]]) {
            62 => "x86_64",
            183 => "aarch64",
            243 => "riscv64",
            _ => return None,
        };
        return Some(format!("linux {arch}"));
    }
    // Mach-O, 64-bit little endian. cputype follows the magic.
    if head.len() >= 8 && u32::from_le_bytes([head[0], head[1], head[2], head[3]]) == 0xfeed_facf {
        let arch = match u32::from_le_bytes([head[4], head[5], head[6], head[7]]) {
            0x0100_000c => "aarch64",
            0x0100_0007 => "x86_64",
            _ => return None,
        };
        return Some(format!("macos {arch}"));
    }
    // PE: the COFF machine field sits just past the signature, which the DOS
    // stub points at from offset 0x3c.
    if head.len() >= 0x40 && &head[..2] == b"MZ" {
        let offset = u32::from_le_bytes([head[0x3c], head[0x3d], head[0x3e], head[0x3f]]) as usize;
        if head.len() >= offset + 6 && &head[offset..offset + 4] == b"PE\0\0" {
            let arch = match u16::from_le_bytes([head[offset + 4], head[offset + 5]]) {
                0x8664 => "x86_64",
                0xaa64 => "aarch64",
                _ => return None,
            };
            return Some(format!("windows {arch}"));
        }
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    fn elf(machine: u16) -> Vec<u8> {
        let mut head = vec![0u8; 64];
        head[..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
        head[18..20].copy_from_slice(&machine.to_le_bytes());
        head
    }

    #[test]
    fn reads_the_architecture_out_of_an_elf_header() {
        assert_eq!(target_from_header(&elf(62)).as_deref(), Some("linux x86_64"));
        assert_eq!(target_from_header(&elf(183)).as_deref(), Some("linux aarch64"));
        // The string has to match what the agent reports, or every agent would
        // refuse the release it was just given.
        assert_eq!(target_from_header(&elf(62)).as_deref(), Some("linux x86_64"));
    }

    #[test]
    fn reads_mach_o_and_pe_headers() {
        let mut macho = vec![0u8; 64];
        macho[..4].copy_from_slice(&0xfeed_facfu32.to_le_bytes());
        macho[4..8].copy_from_slice(&0x0100_000cu32.to_le_bytes());
        assert_eq!(target_from_header(&macho).as_deref(), Some("macos aarch64"));

        let mut pe = vec![0u8; 128];
        pe[..2].copy_from_slice(b"MZ");
        pe[0x3c..0x40].copy_from_slice(&64u32.to_le_bytes());
        pe[64..68].copy_from_slice(b"PE\0\0");
        pe[68..70].copy_from_slice(&0x8664u16.to_le_bytes());
        assert_eq!(target_from_header(&pe).as_deref(), Some("windows x86_64"));
    }

    #[test]
    fn refuses_to_guess_at_anything_else() {
        // A shell script, a truncated file, and an unknown machine all have to
        // ask for an explicit target rather than publish under a wrong one.
        assert_eq!(target_from_header(b"#!/bin/sh\necho hi\n"), None);
        assert_eq!(target_from_header(&[0x7f, b'E', b'L', b'F']), None);
        assert_eq!(target_from_header(&elf(0x9999)), None);
        assert_eq!(target_from_header(&[]), None);
    }
}
