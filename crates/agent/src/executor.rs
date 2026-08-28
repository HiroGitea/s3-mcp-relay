//! Agent-side policy enforcement and bounded command execution.

use std::ffi::OsString;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use crate::jobs::{self, JobManager};
use common::blob::{self, Manifest};
use common::{optional_env, Command, CommandKind, DirEntry, Response, ResponsePayload, Transport};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct Policy {
    allowed_roots: Vec<PathBuf>,
    allowed_programs: HashSet<PathBuf>,
    pub child_env: Vec<(OsString, OsString)>,
    /// Ceiling for a detached job. Hours, not minutes: this bounds a training
    /// run, not a command.
    pub max_job_secs: u64,
    pub max_timeout_secs: u64,
    max_file_bytes: usize,
    max_output_bytes: usize,
    /// Ceiling for a bulk transfer. Separate from `max_file_bytes`, which
    /// bounds what fits in a single command payload; a transfer streams
    /// through the bucket and is never held in memory.
    max_blob_bytes: u64,
    /// Skip the allowed-roots check entirely. Set this when the agent is meant
    /// to have the same reach a local shell would, and the trust boundary is
    /// somewhere else — typically the controller side, where Claude Code
    /// already gates every call.
    allow_any_path: bool,
    /// Skip the program allowlist. With this on, `exec("/bin/bash", ["-c", …])`
    /// works, which is equivalent to remote code execution by design.
    allow_any_program: bool,
}

impl Policy {
    pub fn from_env() -> Result<Self> {
        let roots = optional_env("AGENT_ALLOWED_ROOTS")
            .map(|value| {
                let value = OsString::from(value);
                std::env::split_paths(&value).collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut allowed_roots = Vec::with_capacity(roots.len());
        for root in roots {
            allowed_roots.push(
                std::fs::canonicalize(&root)
                    .with_context(|| format!("canonicalize allowed root {}", root.display()))?,
            );
        }
        let mut allowed_programs = HashSet::new();
        for value in optional_env("AGENT_ALLOWED_PROGRAMS").unwrap_or_default()
            .split(',').map(str::trim).filter(|value| !value.is_empty())
        {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                bail!("every AGENT_ALLOWED_PROGRAMS entry must be an absolute path: {value}");
            }
            allowed_programs.insert(std::fs::canonicalize(&path)
                .with_context(|| format!("canonicalize allowed program {}", path.display()))?);
        }
        let env_names = optional_env("AGENT_CHILD_ENV_ALLOWLIST")
            .unwrap_or_else(|| "LANG,LC_ALL,TZ,SYSTEMROOT,WINDIR,TEMP,TMP".to_owned());
        let mut child_env = Vec::new();
        for name in env_names.split(',').map(str::trim).filter(|value| !value.is_empty()) {
            if is_secret_env_name(name) {
                bail!("AGENT_CHILD_ENV_ALLOWLIST may not include secret variable {name}");
            }
            if let Some(value) = std::env::var_os(name) {
                child_env.push((OsString::from(name), value));
            }
        }
        let max_timeout_secs = number_env("AGENT_MAX_TIMEOUT_SECS", 300u64)?;
        let max_file_bytes = number_env("AGENT_MAX_FILE_BYTES", 1_048_576usize)?;
        let max_output_bytes = number_env("AGENT_MAX_OUTPUT_BYTES", 1_048_576usize)?;
        if !(1..=3_600).contains(&max_timeout_secs) {
            bail!("AGENT_MAX_TIMEOUT_SECS must be in 1..=3600");
        }
        if !(1..=1_048_576).contains(&max_file_bytes) {
            bail!("AGENT_MAX_FILE_BYTES must be in 1..=1048576");
        }
        if !(1..=1_048_576).contains(&max_output_bytes) {
            bail!("AGENT_MAX_OUTPUT_BYTES must be in 1..=1048576");
        }
        let max_blob_bytes = number_env("AGENT_MAX_BLOB_BYTES", 1_073_741_824u64)?;
        if !(1..=1_099_511_627_776).contains(&max_blob_bytes) {
            bail!("AGENT_MAX_BLOB_BYTES must be in 1..=1099511627776");
        }
        let max_job_secs = number_env("AGENT_MAX_JOB_SECS", 86_400u64)?;
        if !(1..=2_592_000).contains(&max_job_secs) {
            bail!("AGENT_MAX_JOB_SECS must be in 1..=2592000");
        }
        let allow_any_path = flag_env("AGENT_ALLOW_ANY_PATH");
        let allow_any_program = flag_env("AGENT_ALLOW_ANY_PROGRAM");
        if allow_any_program {
            tracing::warn!(
                "AGENT_ALLOW_ANY_PROGRAM is on: exec can run any executable on this host, \
                 including a shell. The controller side is the only thing gating it."
            );
        }
        if allow_any_path {
            tracing::warn!("AGENT_ALLOW_ANY_PATH is on: file operations are not confined to any root");
        }
        Ok(Self {
            allowed_roots,
            allowed_programs,
            child_env,
            max_job_secs,
            max_timeout_secs,
            max_file_bytes,
            max_output_bytes,
            max_blob_bytes,
            allow_any_path,
            allow_any_program,
        })
    }

    fn check_existing_path(&self, value: &str) -> Result<PathBuf> {
        let path = std::fs::canonicalize(value)
            .with_context(|| format!("canonicalize path {value}"))?;
        self.check_canonical(&path)?;
        Ok(path)
    }

    fn check_write_path(&self, value: &str) -> Result<PathBuf> {
        let requested = PathBuf::from(value);
        if requested.exists() {
            return self.check_existing_path(value);
        }
        let parent = requested.parent().filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| anyhow::anyhow!("write path must have an existing parent directory"))?;
        let canonical_parent = std::fs::canonicalize(parent)
            .with_context(|| format!("canonicalize parent {}", parent.display()))?;
        self.check_canonical(&canonical_parent)?;
        let name = requested.file_name().ok_or_else(|| anyhow::anyhow!("invalid write path"))?;
        Ok(canonical_parent.join(name))
    }

    /// Like [`check_write_path`](Self::check_write_path), but creates missing
    /// parent directories instead of refusing.
    ///
    /// Only transfers use this. A push typically targets a directory that does
    /// not exist yet (`.../wheels/`), and the agent has no mkdir: requiring the
    /// operator to pre-create every destination, or to put a shell utility in
    /// the exec allowlist, would be worse for security than this.
    ///
    /// The containment check happens against the nearest *existing* ancestor,
    /// before anything is created, so a path that escapes the allowed roots
    /// cannot cause even an empty directory to appear outside them.
    fn prepare_write_path(&self, value: &str) -> Result<PathBuf> {
        let requested = PathBuf::from(value);
        if requested.exists() {
            return self.check_existing_path(value);
        }
        let parent = requested
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| anyhow::anyhow!("write path must name a file inside a directory"))?;

        let mut existing = parent;
        loop {
            if existing.exists() {
                break;
            }
            existing = existing
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .ok_or_else(|| anyhow::anyhow!("no existing ancestor of {}", parent.display()))?;
        }
        let anchor = std::fs::canonicalize(existing)
            .with_context(|| format!("canonicalize {}", existing.display()))?;
        self.check_canonical(&anchor)?;

        std::fs::create_dir_all(parent)
            .with_context(|| format!("create directory {}", parent.display()))?;
        // Re-resolve now that the directory exists: this catches a symlink
        // planted between the check above and the creation.
        let canonical_parent = std::fs::canonicalize(parent)
            .with_context(|| format!("canonicalize {}", parent.display()))?;
        self.check_canonical(&canonical_parent)?;

        let name = requested
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("write path must end in a file name"))?;
        Ok(canonical_parent.join(name))
    }

    fn check_canonical(&self, path: &Path) -> Result<()> {
        if self.allow_any_path || self.allowed_roots.iter().any(|root| path.starts_with(root)) {
            Ok(())
        } else {
            bail!("path is outside AGENT_ALLOWED_ROOTS")
        }
    }

    /// Let job output live under an allowed root without the operator having
    /// to remember to list it. Without this, `pull_file` on a training log
    /// fails for a reason that has nothing to do with what was asked.
    pub fn permit_root(&mut self, root: &Path) -> Result<()> {
        let canonical = std::fs::canonicalize(root)
            .with_context(|| format!("canonicalize {}", root.display()))?;
        if !self.allowed_roots.contains(&canonical) {
            self.allowed_roots.push(canonical);
        }
        Ok(())
    }

    fn check_program(&self, program: &str) -> Result<PathBuf> {
        if program.is_empty() { bail!("program must not be empty"); }
        let requested = PathBuf::from(program);
        if !requested.is_absolute() {
            bail!("program must be an absolute path");
        }
        let canonical = std::fs::canonicalize(&requested)
            .with_context(|| format!("canonicalize program {}", requested.display()))?;
        // Absolute-path and existence checks still apply: they turn a typo into
        // a clear error instead of a confusing spawn failure.
        if !self.allow_any_program && !self.allowed_programs.contains(&canonical) {
            bail!("program is not present in AGENT_ALLOWED_PROGRAMS");
        }
        Ok(canonical)
    }
}

pub async fn execute(
    cmd: &Command,
    policy: &Policy,
    transport: &Transport,
    jobs: &JobManager,
) -> Response {
    match run(cmd, policy, transport, jobs).await {
        Ok(response) => response,
        Err(error) => Response::err(cmd, format!("{error:#}")),
    }
}

async fn run(
    cmd: &Command,
    policy: &Policy,
    transport: &Transport,
    jobs: &JobManager,
) -> Result<Response> {
    match &cmd.kind {
        CommandKind::Ping => Ok(Response::ok(cmd, ResponsePayload::Pong {
            hostname: hostname(), os: os_string(),
            agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        })),
        CommandKind::Exec { program, args, cwd, stdin_b64 } => {
            exec(cmd, policy, program, args, cwd.as_deref(), stdin_b64.as_deref()).await
        }
        CommandKind::ReadFile { path, max_bytes } => read_file(cmd, policy, path, *max_bytes).await,
        CommandKind::WriteFile { path, contents_b64, append } => {
            write_file(cmd, policy, path, contents_b64, *append).await
        }
        CommandKind::ListDir { path } => list_dir(cmd, policy, path).await,
        CommandKind::PushBlob { transfer, dest_path, chunks, total_bytes, sha256 } => {
            push_blob(cmd, policy, transport, transfer, dest_path, *chunks, *total_bytes, sha256).await
        }
        CommandKind::PullBlob { transfer, path, max_bytes } => {
            pull_blob(cmd, policy, transport, transfer, path, *max_bytes).await
        }
        CommandKind::StartJob { job, program, args, cwd, max_runtime_secs, label } => {
            start_job(cmd, policy, jobs, job, program, args, cwd.as_deref(), *max_runtime_secs, label.clone())
        }
        CommandKind::ListJobs => {
            Ok(Response::ok(cmd, ResponsePayload::JobList { jobs: jobs.list() }))
        }
        CommandKind::JobOutput { job, tail_bytes } => job_output(cmd, jobs, job, *tail_bytes).await,
        CommandKind::CancelJob { job } => {
            jobs.cancel(job)?;
            Ok(Response::ok(cmd, ResponsePayload::Empty))
        }
        CommandKind::MakeDir { path, parents } => make_dir(cmd, policy, path, *parents).await,
        CommandKind::Remove { path, recursive } => remove(cmd, policy, path, *recursive).await,
        CommandKind::Move { from, to } => move_path(cmd, policy, from, to).await,
    }
}

#[allow(clippy::too_many_arguments)]
fn start_job(
    cmd: &Command,
    policy: &Policy,
    jobs: &JobManager,
    job: &str,
    program: &str,
    args: &[String],
    cwd: Option<&str>,
    max_runtime_secs: u64,
    label: Option<String>,
) -> Result<Response> {
    common::validate_transfer_id(job).context("job id")?;
    let program = policy.check_program(program)?;
    let cwd = cwd.map(|value| policy.check_existing_path(value)).transpose()?;
    // A job outlives the command that started it, so its ceiling is its own
    // setting rather than the command timeout.
    if max_runtime_secs == 0 || max_runtime_secs > policy.max_job_secs {
        bail!("max_runtime_secs must be in 1..={}", policy.max_job_secs);
    }
    let pid = jobs.start(
        job.to_owned(),
        &program,
        args,
        cwd.as_deref(),
        &policy.child_env,
        Duration::from_secs(max_runtime_secs),
        label,
    )?;
    Ok(Response::ok(cmd, ResponsePayload::JobStarted { job: job.to_owned(), pid }))
}

async fn job_output(
    cmd: &Command,
    jobs: &JobManager,
    job: &str,
    tail_bytes: Option<usize>,
) -> Result<Response> {
    common::validate_transfer_id(job).context("job id")?;
    let status = jobs.get(job).ok_or_else(|| anyhow::anyhow!("no such job {job}"))?;
    let (stdout_tail, stdout_truncated) = jobs::tail(&jobs.stdout_path(job), tail_bytes).await?;
    let (stderr_tail, stderr_truncated) = jobs::tail(&jobs.stderr_path(job), tail_bytes).await?;
    Ok(Response::ok(cmd, ResponsePayload::JobOutput {
        stdout_tail,
        stderr_tail,
        stdout_truncated,
        stderr_truncated,
        stdout_path: status.stdout_path,
        stderr_path: status.stderr_path,
    }))
}

async fn make_dir(cmd: &Command, policy: &Policy, path: &str, parents: bool) -> Result<Response> {
    let requested = PathBuf::from(path);
    if requested.exists() {
        // Idempotent: an existing directory is success, an existing file is not.
        let resolved = policy.check_existing_path(path)?;
        if resolved.is_dir() {
            return Ok(Response::ok(cmd, ResponsePayload::Empty));
        }
        bail!("{path} already exists and is not a directory");
    }
    // prepare_write_path validates containment against the nearest existing
    // ancestor and then creates the parents, which is exactly the work here;
    // it leaves only the final component to create.
    let target = policy.prepare_write_path(path)?;
    if parents {
        tokio::fs::create_dir_all(&target).await
    } else {
        tokio::fs::create_dir(&target).await
    }
    .with_context(|| format!("create directory {}", target.display()))?;
    Ok(Response::ok(cmd, ResponsePayload::Empty))
}

async fn remove(cmd: &Command, policy: &Policy, path: &str, recursive: bool) -> Result<Response> {
    let target = policy.check_existing_path(path)?;
    // Async because removing a large tree is not instant, and this runs on the
    // same task that polls for the next command.
    if target.is_dir() {
        if recursive {
            tokio::fs::remove_dir_all(&target).await
        } else {
            // Refuses when non-empty, which is the point: recursive deletion
            // has to be asked for explicitly.
            tokio::fs::remove_dir(&target).await
        }
        .with_context(|| format!("remove directory {}", target.display()))?;
    } else {
        tokio::fs::remove_file(&target)
            .await
            .with_context(|| format!("remove file {}", target.display()))?;
    }
    Ok(Response::ok(cmd, ResponsePayload::Empty))
}

async fn move_path(cmd: &Command, policy: &Policy, from: &str, to: &str) -> Result<Response> {
    // Both ends are checked: moving something out of the allowed roots is as
    // much of an escape as writing outside them.
    let source = policy.check_existing_path(from)?;
    let dest = policy.prepare_write_path(to)?;
    tokio::fs::rename(&source, &dest)
        .await
        .with_context(|| format!("move {} to {}", source.display(), dest.display()))?;
    Ok(Response::ok(cmd, ResponsePayload::Empty))
}

/// Assemble a file the controller staged in the bucket.
///
/// The chunks land in a sibling temporary file and are renamed into place only
/// after the hash matches, so an interrupted transfer never leaves something
/// that looks like a complete file. Rename within a directory is atomic on both
/// Unix and Windows.
async fn push_blob(
    cmd: &Command,
    policy: &Policy,
    transport: &Transport,
    transfer: &str,
    dest_path: &str,
    chunks: u32,
    total_bytes: u64,
    expected_sha256: &str,
) -> Result<Response> {
    if total_bytes > policy.max_blob_bytes {
        bail!("transfer of {total_bytes} bytes exceeds AGENT_MAX_BLOB_BYTES");
    }
    let dest = policy.prepare_write_path(dest_path)?;
    let manifest = Manifest {
        chunks,
        total_bytes,
        sha256: expected_sha256.to_owned(),
    };
    let bytes = blob::assemble_file(transport, &cmd.agent_id, transfer, &manifest, &dest).await?;

    // The bytes are safe on disk now, so the staging area can go. Failing to
    // clean up is not worth failing the transfer over; the bucket lifecycle
    // rule is the backstop.
    if let Err(error) = transport.delete_blob(&cmd.agent_id, transfer).await {
        tracing::warn!(%transfer, %error, "could not delete transfer chunks");
    }
    Ok(Response::ok(
        cmd,
        ResponsePayload::BlobWritten { bytes, sha256: manifest.sha256 },
    ))
}

/// Stage a local file in the bucket for the controller to collect.
async fn pull_blob(
    cmd: &Command,
    policy: &Policy,
    transport: &Transport,
    transfer: &str,
    path: &str,
    max_bytes: u64,
) -> Result<Response> {
    let source = policy.check_existing_path(path)?;
    let limit = max_bytes.min(policy.max_blob_bytes);
    let size = tokio::fs::metadata(&source)
        .await
        .with_context(|| format!("stat {}", source.display()))?
        .len();
    // Refuse rather than truncate: a partial transfer that reports success
    // would be indistinguishable from a complete one.
    if size > limit {
        bail!("file is {size} bytes, above the {limit} byte transfer limit");
    }

    match blob::stage_file(transport, &cmd.agent_id, transfer, &source).await {
        Ok(manifest) => Ok(Response::ok(
            cmd,
            ResponsePayload::BlobStaged {
                chunks: manifest.chunks,
                total_bytes: manifest.total_bytes,
                sha256: manifest.sha256,
            },
        )),
        Err(error) => {
            let _ = transport.delete_blob(&cmd.agent_id, transfer).await;
            Err(error)
        }
    }
}

async fn exec(
    cmd: &Command,
    policy: &Policy,
    program: &str,
    args: &[String],
    cwd: Option<&str>,
    stdin_b64: Option<&str>,
) -> Result<Response> {
    let program = policy.check_program(program)?;
    let cwd = cwd.map(|value| policy.check_existing_path(value)).transpose()?;
    let stdin = stdin_b64.map(|value| B64.decode(value).context("decode stdin base64")).transpose()?;
    if stdin.as_ref().is_some_and(|value| value.len() > policy.max_file_bytes) {
        bail!("stdin exceeds AGENT_MAX_FILE_BYTES");
    }

    let mut builder = tokio::process::Command::new(&program);
    builder.args(args).stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true)
        .env_clear().envs(policy.child_env.iter().cloned());
    if let Some(directory) = cwd { builder.current_dir(directory); }
    let mut child = builder.spawn().with_context(|| format!("spawn {}", program.display()))?;

    if let Some(input) = stdin {
        let mut pipe = child.stdin.take().context("child stdin unavailable")?;
        tokio::spawn(async move { let _ = pipe.write_all(&input).await; });
    }
    let stdout = child.stdout.take().context("child stdout unavailable")?;
    let stderr = child.stderr.take().context("child stderr unavailable")?;
    let limit = policy.max_output_bytes;
    let stdout_task = tokio::spawn(read_bounded(stdout, limit));
    let stderr_task = tokio::spawn(read_bounded(stderr, limit));

    let duration = Duration::from_secs(cmd.timeout_secs.min(policy.max_timeout_secs).max(1));
    let (status, timed_out) = match tokio::time::timeout(duration, child.wait()).await {
        Ok(result) => (Some(result.context("wait for child")?), false),
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            (None, true)
        }
    };
    let (stdout, stdout_truncated) = stdout_task.await.context("join stdout reader")??;
    let (stderr, stderr_truncated) = stderr_task.await.context("join stderr reader")??;
    Ok(Response::ok(cmd, ResponsePayload::Exec {
        stdout_b64: B64.encode(stdout), stderr_b64: B64.encode(stderr),
        stdout_truncated, stderr_truncated,
        exit_code: status.and_then(|value| value.code()), timed_out,
    }))
}

async fn read_bounded(mut reader: impl AsyncRead + Unpin, limit: usize) -> Result<(Vec<u8>, bool)> {
    let mut kept = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 { break; }
        let remaining = limit.saturating_sub(kept.len());
        let take = remaining.min(count);
        kept.extend_from_slice(&buffer[..take]);
        truncated |= take < count;
    }
    Ok((kept, truncated))
}

async fn read_file(cmd: &Command, policy: &Policy, value: &str, requested: Option<usize>) -> Result<Response> {
    let path = policy.check_existing_path(value)?;
    let limit = requested.unwrap_or(policy.max_file_bytes).min(policy.max_file_bytes);
    let file = tokio::fs::File::open(&path).await.with_context(|| format!("open {}", path.display()))?;
    let (bytes, truncated) = read_bounded(file, limit).await?;
    Ok(Response::ok(cmd, ResponsePayload::FileContents { contents_b64: B64.encode(bytes), truncated }))
}

async fn write_file(cmd: &Command, policy: &Policy, value: &str, contents_b64: &str, append: bool) -> Result<Response> {
    let path = policy.check_write_path(value)?;
    let data = B64.decode(contents_b64).context("decode file content base64")?;
    if data.len() > policy.max_file_bytes { bail!("write exceeds AGENT_MAX_FILE_BYTES"); }
    let mut file = tokio::fs::OpenOptions::new().create(true).write(true).append(append)
        .truncate(!append).open(&path).await.with_context(|| format!("open {}", path.display()))?;
    file.write_all(&data).await.with_context(|| format!("write {}", path.display()))?;
    file.flush().await?;
    Ok(Response::ok(cmd, ResponsePayload::Written { bytes: data.len() }))
}

async fn list_dir(cmd: &Command, policy: &Policy, value: &str) -> Result<Response> {
    let path = policy.check_existing_path(value)?;
    let mut directory = tokio::fs::read_dir(&path).await.with_context(|| format!("read directory {}", path.display()))?;
    let mut entries = Vec::new();
    while let Some(entry) = directory.next_entry().await? {
        if entries.len() >= 10_000 { bail!("directory has more than 10,000 entries"); }
        let metadata = entry.metadata().await.ok();
        entries.push(DirEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            is_dir: metadata.as_ref().is_some_and(|value| value.is_dir()),
            size: metadata.as_ref().map_or(0, |value| value.len()),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Response::ok(cmd, ResponsePayload::DirListing { entries }))
}

fn flag_env(name: &str) -> bool {
    optional_env(name)
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn number_env<T>(name: &str, default: T) -> Result<T>
where T: std::str::FromStr, T::Err: std::error::Error + Send + Sync + 'static {
    match optional_env(name) {
        Some(value) => value.parse().with_context(|| format!("parse {name}")),
        None => Ok(default),
    }
}

fn is_secret_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    upper.starts_with("S3_")
        || upper.starts_with("RELAY_")
        || upper.contains("SECRET")
        || upper.contains("TOKEN")
        || upper.contains("PASSWORD")
        || upper.contains("CREDENTIAL")
        || upper.ends_with("_KEY")
}

pub fn hostname() -> String {
    std::env::var("COMPUTERNAME").or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_owned())
}

/// What this machine reports as its platform. Shared with the update path, so
/// the string the controller matches a release against and the string the agent
/// checks it with cannot drift apart.
pub fn os_string() -> String { common::update::current_target() }
