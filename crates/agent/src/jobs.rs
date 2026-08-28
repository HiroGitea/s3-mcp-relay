//! Detached long-running processes.
//!
//! `Exec` waits for the program to exit, so it is bounded by how long the
//! controller is willing to block — minutes. Training a model takes hours, and
//! nothing in the command path can wait that long.
//!
//! A job instead returns as soon as the process is spawned. The agent
//! supervises it, streams both output streams straight to files (so a
//! multi-gigabyte training log costs no memory), and records the outcome. The
//! result travels home on the heartbeat, which is the only channel that reaches
//! the controller without anyone asking: an MCP server cannot push to Claude,
//! so the outcome has to be waiting somewhere by the time someone looks.
//!
//! Jobs do not survive an agent restart. The child dies with its parent, and
//! anything still marked running at startup is reported as `Lost` rather than
//! quietly forgotten — whatever it wrote to disk before dying is still there.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use common::protocol::{now_unix, JobState, JobStatus};
use tokio::sync::oneshot;
use tracing::{info, warn};

/// Finished jobs kept in the heartbeat. Enough to notice "the training run
/// ended while I was away", small enough not to bloat every heartbeat.
const FINISHED_IN_HEARTBEAT: usize = 5;
/// Finished jobs kept in memory for `list_jobs`.
const FINISHED_RETAINED: usize = 50;
const DEFAULT_TAIL_BYTES: usize = 8 * 1024;
const MAX_TAIL_BYTES: usize = 256 * 1024;

struct Entry {
    status: JobStatus,
    /// Dropping this cancels the supervisor, which kills the child.
    cancel: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct JobManager {
    inner: Arc<Mutex<HashMap<String, Entry>>>,
    dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupPolicy {
    pub retention: Duration,
    pub max_total_bytes: u64,
}

impl JobManager {
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create job directory {}", dir.display()))?;
        Ok(Self { inner: Arc::new(Mutex::new(HashMap::new())), dir })
    }

    pub fn directory(&self) -> &Path {
        &self.dir
    }

    pub fn stdout_path(&self, job: &str) -> PathBuf {
        self.dir.join(format!("{job}.out"))
    }

    pub fn stderr_path(&self, job: &str) -> PathBuf {
        self.dir.join(format!("{job}.err"))
    }

    /// Spawn the job and return once the process exists. Everything after that
    /// happens on a supervisor task.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &self,
        job: String,
        program: &Path,
        args: &[String],
        cwd: Option<&Path>,
        env: &[(std::ffi::OsString, std::ffi::OsString)],
        max_runtime: Duration,
        label: Option<String>,
    ) -> Result<Option<u32>> {
        {
            let guard = self.inner.lock().map_err(|_| anyhow::anyhow!("job table poisoned"))?;
            if guard.contains_key(&job) {
                bail!("job {job} already exists");
            }
        }

        let stdout_path = self.stdout_path(&job);
        let stderr_path = self.stderr_path(&job);
        // Redirect at the file descriptor level: output never passes through
        // the agent, so a training log of any size costs nothing here.
        let stdout_file = std::fs::File::create(&stdout_path)
            .with_context(|| format!("create {}", stdout_path.display()))?;
        let stderr_file = std::fs::File::create(&stderr_path)
            .with_context(|| format!("create {}", stderr_path.display()))?;

        let mut builder = tokio::process::Command::new(program);
        builder
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .kill_on_drop(true)
            .env_clear()
            .envs(env.iter().cloned());
        if let Some(dir) = cwd {
            builder.current_dir(dir);
        }
        let mut child = builder
            .spawn()
            .with_context(|| format!("spawn {}", program.display()))?;
        let pid = child.id();

        let status = JobStatus {
            job: job.clone(),
            label,
            program: program.display().to_string(),
            state: JobState::Running,
            started_at: now_unix(),
            finished_at: None,
            exit_code: None,
            error: None,
            stdout_path: stdout_path.display().to_string(),
            stderr_path: stderr_path.display().to_string(),
        };

        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        {
            let mut guard = self.inner.lock().map_err(|_| anyhow::anyhow!("job table poisoned"))?;
            guard.insert(job.clone(), Entry { status, cancel: Some(cancel_tx) });
        }

        let table = self.inner.clone();
        let job_id = job.clone();
        tokio::spawn(async move {
            // The wait lives on its own task so the supervisor can abort it.
            // Aborting drops the Child, and kill_on_drop turns that into a
            // signal to the process — which is how both the timeout and the
            // cancel paths actually stop the work.
            let mut waiter = tokio::spawn(async move { child.wait().await });
            let (state, exit_code, error) = tokio::select! {
                joined = &mut waiter => match joined {
                    Ok(Ok(status)) => {
                        let code = status.code();
                        let state = if status.success() { JobState::Succeeded } else { JobState::Failed };
                        (state, code, None)
                    }
                    Ok(Err(error)) => (JobState::Failed, None, Some(format!("wait failed: {error}"))),
                    Err(error) => (JobState::Failed, None, Some(format!("supervisor failed: {error}"))),
                },
                _ = tokio::time::sleep(max_runtime) => {
                    waiter.abort();
                    (JobState::TimedOut, None, Some(format!(
                        "killed after max_runtime_secs ({}s)", max_runtime.as_secs()
                    )))
                }
                _ = &mut cancel_rx => {
                    waiter.abort();
                    (JobState::Cancelled, None, Some("cancelled".to_owned()))
                }
            };

            if let Ok(mut guard) = table.lock() {
                if let Some(entry) = guard.get_mut(&job_id) {
                    entry.status.state = state;
                    entry.status.exit_code = exit_code;
                    entry.status.error = error;
                    entry.status.finished_at = Some(now_unix());
                    entry.cancel = None;
                }
                prune(&mut guard);
            }
            info!(job = %job_id, ?state, "job finished");
        });

        Ok(pid)
    }

    pub fn cancel(&self, job: &str) -> Result<()> {
        let mut guard = self.inner.lock().map_err(|_| anyhow::anyhow!("job table poisoned"))?;
        let entry = guard.get_mut(job).ok_or_else(|| anyhow::anyhow!("no such job {job}"))?;
        if entry.status.state.is_terminal() {
            bail!("job {job} already finished");
        }
        match entry.cancel.take() {
            // Send failing means the supervisor is already on its way out.
            Some(sender) => { let _ = sender.send(()); Ok(()) }
            None => bail!("job {job} is already being cancelled"),
        }
    }

    pub fn list(&self) -> Vec<JobStatus> {
        let Ok(guard) = self.inner.lock() else { return Vec::new() };
        let mut jobs: Vec<JobStatus> = guard.values().map(|entry| entry.status.clone()).collect();
        // Running first, then most recently started.
        jobs.sort_by(|a, b| {
            a.state
                .is_terminal()
                .cmp(&b.state.is_terminal())
                .then(b.started_at.cmp(&a.started_at))
        });
        jobs
    }

    pub fn get(&self, job: &str) -> Option<JobStatus> {
        let guard = self.inner.lock().ok()?;
        guard.get(job).map(|entry| entry.status.clone())
    }

    pub fn running_count(&self) -> u32 {
        let Ok(guard) = self.inner.lock() else { return 0 };
        guard.values().filter(|entry| !entry.status.state.is_terminal()).count() as u32
    }

    /// Recently finished jobs for the heartbeat, newest first.
    pub fn recently_finished(&self) -> Vec<JobStatus> {
        let Ok(guard) = self.inner.lock() else { return Vec::new() };
        let mut done: Vec<JobStatus> = guard
            .values()
            .filter(|entry| entry.status.state.is_terminal())
            .map(|entry| entry.status.clone())
            .collect();
        done.sort_by_key(|status| std::cmp::Reverse(status.finished_at.unwrap_or(0)));
        done.truncate(FINISHED_IN_HEARTBEAT);
        done
    }

    /// Delete old local copies after they had time to reach the controller.
    /// Running job output and the currently active agent log are never removed.
    pub fn cleanup(&self, policy: CleanupPolicy) -> Result<(u64, u64)> {
        use std::collections::HashSet;
        let running: HashSet<String> = self.inner.lock()
            .map_err(|_| anyhow::anyhow!("job table poisoned"))?.values()
            .filter(|entry| !entry.status.state.is_terminal())
            .map(|entry| entry.status.job.clone()).collect();
        let now = std::time::SystemTime::now();
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default().to_owned();
            let job_log = matches!(path.extension().and_then(|v| v.to_str()), Some("out" | "err"));
            let agent_log = name.starts_with("agent.log.");
            if !job_log && !agent_log { continue; }
            let stem = path.file_stem().and_then(|v| v.to_str()).unwrap_or_default().to_owned();
            let metadata = entry.metadata()?;
            let mut marker_name = path.as_os_str().to_os_string();
            marker_name.push(".shipped");
            let marker = PathBuf::from(marker_name);
            let uploaded = std::fs::read_to_string(&marker).ok()
                .and_then(|value| value.trim().parse::<u64>().ok()).unwrap_or(0) >= metadata.len();
            files.push((path, stem, metadata.len(), metadata.modified().unwrap_or(std::time::UNIX_EPOCH), agent_log, uploaded, marker));
        }
        let mut removed_files = 0;
        let mut removed_bytes = 0;
        for (path, stem, size, modified, agent_log, uploaded, marker) in &files {
            let active_agent_log = *agent_log && now.duration_since(*modified).unwrap_or_default() < Duration::from_secs(3600);
            if *uploaded && !running.contains(stem) && !active_agent_log
                && now.duration_since(*modified).unwrap_or_default() >= policy.retention
                && std::fs::remove_file(path).is_ok() {
                let _ = std::fs::remove_file(marker);
                removed_files += 1;
                removed_bytes += *size;
            }
        }
        let mut remaining: Vec<_> = files.into_iter().filter(|(path, _, _, _, _, _, _)| path.exists()).collect();
        let mut total: u64 = remaining.iter().map(|(_, _, size, _, _, _, _)| *size).sum();
        remaining.sort_by_key(|(_, _, _, modified, _, _, _)| *modified);
        for (path, stem, size, modified, agent_log, uploaded, marker) in remaining {
            if total <= policy.max_total_bytes { break; }
            let active_agent_log = agent_log && now.duration_since(modified).unwrap_or_default() < Duration::from_secs(3600);
            if !uploaded || running.contains(&stem) || active_agent_log { continue; }
            if std::fs::remove_file(path).is_ok() {
                let _ = std::fs::remove_file(marker);
                total = total.saturating_sub(size);
                removed_files += 1;
                removed_bytes += size;
            }
        }
        Ok((removed_files, removed_bytes))
    }
}

fn prune(table: &mut HashMap<String, Entry>) {
    let mut finished: Vec<(String, i64)> = table
        .iter()
        .filter(|(_, entry)| entry.status.state.is_terminal())
        .map(|(id, entry)| (id.clone(), entry.status.finished_at.unwrap_or(0)))
        .collect();
    if finished.len() <= FINISHED_RETAINED {
        return;
    }
    finished.sort_by_key(|(_, at)| *at);
    let excess = finished.len() - FINISHED_RETAINED;
    // Output files are left in place on purpose: the job record is gone but
    // whatever it produced may still be wanted.
    for (id, _) in finished.into_iter().take(excess) {
        table.remove(&id);
    }
}

/// Last `limit` bytes of a file, plus whether anything was skipped.
pub async fn tail(path: &Path, limit: Option<usize>) -> Result<(String, bool)> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let limit = limit.unwrap_or(DEFAULT_TAIL_BYTES).clamp(1, MAX_TAIL_BYTES);
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        // A job that has not written anything yet has no file to read.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((String::new(), false))
        }
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let size = file.metadata().await.context("stat job output")?.len();
    let truncated = size > limit as u64;
    if truncated {
        file.seek(std::io::SeekFrom::Start(size - limit as u64))
            .await
            .context("seek job output")?;
    }
    let mut buffer = Vec::with_capacity(limit.min(size as usize));
    file.take(limit as u64)
        .read_to_end(&mut buffer)
        .await
        .context("read job output")?;
    // Output is arbitrary bytes and the tail may start mid-character.
    Ok((String::from_utf8_lossy(&buffer).into_owned(), truncated))
}

/// Mark anything left running by a previous process as `Lost`.
///
/// Called at startup. There is nothing to recover — the children died with the
/// old agent — but saying so beats leaving a job that looks like it is still
/// training when nothing is.
pub fn report_orphans(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    let orphans = entries
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "out"))
        .count();
    if orphans > 0 {
        warn!(
            count = orphans, dir = %dir.display(),
            "job output from a previous run is still on disk; those jobs are not running"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tail_returns_the_end_of_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        tokio::fs::write(&path, b"0123456789").await.unwrap();

        let (text, truncated) = tail(&path, Some(4)).await.unwrap();
        assert_eq!(text, "6789");
        assert!(truncated);

        let (text, truncated) = tail(&path, Some(100)).await.unwrap();
        assert_eq!(text, "0123456789");
        assert!(!truncated);
    }

    #[tokio::test]
    async fn tail_of_a_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (text, truncated) = tail(&dir.path().join("nope"), None).await.unwrap();
        assert!(text.is_empty());
        assert!(!truncated);
    }

    #[test]
    fn prune_keeps_the_newest_finished_jobs() {
        let mut table = HashMap::new();
        for index in 0..FINISHED_RETAINED + 10 {
            let id = format!("job-{index}");
            table.insert(
                id.clone(),
                Entry {
                    status: JobStatus {
                        job: id,
                        label: None,
                        program: "/bin/true".into(),
                        state: JobState::Succeeded,
                        started_at: index as i64,
                        finished_at: Some(index as i64),
                        exit_code: Some(0),
                        error: None,
                        stdout_path: String::new(),
                        stderr_path: String::new(),
                    },
                    cancel: None,
                },
            );
        }
        prune(&mut table);
        assert_eq!(table.len(), FINISHED_RETAINED);
        // The oldest went first.
        assert!(!table.contains_key("job-0"));
        assert!(table.contains_key(&format!("job-{}", FINISHED_RETAINED + 9)));
    }
}
