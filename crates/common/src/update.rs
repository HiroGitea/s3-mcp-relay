//! Fleet self-update: installing a build the controller published to the bucket.
//!
//! The controller writes one [`UpdateManifest`] per agent and one encrypted copy
//! of the binary for the whole fleet (see [`transport`](crate::transport)). Each
//! agent polls its own manifest and installs on its own schedule, so a machine
//! that was powered off during the rollout catches up when it returns — which a
//! command could never do, since commands expire in minutes.
//!
//! Replacing the binary of a process that exists to run arbitrary programs is
//! about as consequential as this system gets, so the path is paranoid by
//! design and refuses in five separate places:
//!
//! * **Wrong machine.** `target` must equal this host's os/arch. Nothing else
//!   protects an aarch64 box from an x86-64 build; the manifest is per-agent,
//!   but the operator picking agents is not infallible.
//! * **Already installed.** The release is identified by the SHA-256 of the
//!   binary, not by its version string, so republishing a rebuilt `0.2.0` is
//!   still seen as new and reinstalling an unchanged one is still a no-op.
//! * **Rollback of a rollback.** `published_at` must be strictly newer than the
//!   last one applied. The manifest is sealed and key-bound, so only the
//!   controller can write it — but anyone who can write to the bucket could
//!   restore a *previous* ciphertext at the same key, and without this an old
//!   build could be forced back onto the fleet. Deliberate downgrades still
//!   work: the controller republishes the older binary with a current
//!   timestamp.
//! * **Work in progress.** Installing means restarting, and a restart kills
//!   running jobs. A six-hour training run is not worth losing to a routine
//!   update, so the install waits until the agent is idle.
//! * **A binary that does not run.** The staged file must execute and answer
//!   `--version` before it is allowed to replace anything. This is the one that
//!   matters most: without it a single bad build reaches every machine at once
//!   and none of them come back.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::blob::Hasher;
use crate::protocol::{now_unix, UpdateManifest};
use crate::transport::{Transport, BLOB_CHUNK_BYTES};
use crate::Crypto;

/// Exit code the agent uses after installing an update, to ask its supervisor
/// for a restart into the new binary.
///
/// Deliberately non-zero. The shipped unit uses `Restart=always`, where zero
/// would be enough, but an agent installed before this feature existed is very
/// likely still on `Restart=on-failure` — and under that policy a clean exit
/// means *stop*. An update that silently took the fleet offline would be the
/// worst possible outcome, so this exits in the one way both policies restart.
pub const EXIT_UPDATED: i32 = 70;

/// How long the staged binary gets to answer `--version` before it is judged
/// broken. Generous: a cold page-in of a large binary on a loaded machine with
/// slow storage is not evidence of a bad build.
const SMOKE_TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// `"{os} {arch}"`. The single definition of what a build targets, shared by
/// the heartbeat the controller reads and the check the agent makes.
pub fn current_target() -> String {
    format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)
}

/// What the last install decided, persisted so the monotonicity check survives
/// a restart. Small, and losing it is not fatal: a fresh state file only means
/// the next published manifest is accepted on its own terms.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateState {
    /// `published_at` of the newest manifest acted on, including ones skipped
    /// as already-installed. Never moves backwards.
    pub last_published_at: i64,
    pub version: String,
    pub sha256: String,
    pub applied_at: i64,
}

impl UpdateState {
    pub fn load(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_vec_pretty(self).context("serialize update state")?;
        let temp = path.with_extension("tmp");
        std::fs::write(&temp, text).with_context(|| format!("write {}", temp.display()))?;
        std::fs::rename(&temp, path).with_context(|| format!("install {}", path.display()))
    }
}

#[derive(Debug, Clone)]
pub enum Outcome {
    /// No manifest published for this agent.
    None,
    /// This binary already hashes to what the manifest describes.
    UpToDate,
    /// Installed. The caller is expected to exit with [`EXIT_UPDATED`].
    Applied { version: String, sha256: String },
    /// Held back until the agent is idle.
    Deferred { jobs_running: u32 },
    Skipped { reason: String },
}

impl Outcome {
    /// Whether the decision is final for this manifest. A deferred update must
    /// be reconsidered on the next tick even though nothing in the bucket
    /// changed, so the ETag cache must not treat it as settled.
    pub fn is_settled(&self) -> bool {
        !matches!(self, Outcome::Deferred { .. })
    }
}

/// Fetch, verify and install whatever the controller published for `agent_id`.
///
/// `jobs_running` is passed in rather than looked up so this stays independent
/// of the agent's job manager, which keeps it testable and keeps `common` free
/// of agent-only types.
pub async fn check_and_apply(
    transport: &Transport,
    agent_id: &str,
    state_path: &Path,
    jobs_running: u32,
) -> Result<Outcome> {
    let Some(manifest) = transport.read_update_manifest(agent_id).await? else {
        return Ok(Outcome::None);
    };

    let target = current_target();
    if manifest.target != target {
        return Ok(Outcome::Skipped {
            reason: format!(
                "release targets {} but this host is {target}",
                manifest.target
            ),
        });
    }

    let current = std::env::current_exe().context("locate the running binary")?;
    let installed = file_sha256(&current)
        .await
        .with_context(|| format!("hash the running binary {}", current.display()))?;

    let mut state = UpdateState::load(state_path);
    if installed == manifest.sha256 {
        // Record the timestamp anyway: this manifest has been fully accounted
        // for, and remembering that closes the replay window on it.
        if manifest.published_at > state.last_published_at {
            state.last_published_at = manifest.published_at;
            state.version = manifest.version.clone();
            state.sha256 = manifest.sha256.clone();
            let _ = state.save(state_path);
        }
        return Ok(Outcome::UpToDate);
    }

    if manifest.published_at <= state.last_published_at {
        return Ok(Outcome::Skipped {
            reason: format!(
                "manifest published_at {} is not newer than the last applied {}",
                manifest.published_at, state.last_published_at
            ),
        });
    }

    if jobs_running > 0 {
        return Ok(Outcome::Deferred { jobs_running });
    }

    let crypto = Crypto::from_base64(&manifest.release_key_b64, "release")
        .context("release key in the update manifest is unusable")?;

    // Staged beside the binary it will replace, so the final move is a rename
    // within one directory — atomic, and never a cross-device copy that could
    // leave a half-written executable in place.
    let staged = sibling(&current, ".relay-update");
    let result = install(transport, &manifest, &crypto, &current, &staged).await;
    if result.is_err() {
        let _ = std::fs::remove_file(&staged);
    }
    result?;

    state.last_published_at = manifest.published_at;
    state.version = manifest.version.clone();
    state.sha256 = manifest.sha256.clone();
    state.applied_at = now_unix();
    // A state file that could not be written is worth reporting but not worth
    // undoing a good install for: the worst case is that the same manifest is
    // considered again after a restart, where the hash check now says
    // up-to-date anyway.
    if let Err(error) = state.save(state_path) {
        tracing::warn!(%error, "could not persist update state");
    }

    Ok(Outcome::Applied {
        version: manifest.version,
        sha256: manifest.sha256,
    })
}

async fn install(
    transport: &Transport,
    manifest: &UpdateManifest,
    crypto: &Crypto,
    current: &Path,
    staged: &Path,
) -> Result<()> {
    download(transport, manifest, crypto, staged).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(staged, std::fs::Permissions::from_mode(0o755))
            .context("make the staged binary executable")?;
    }

    smoke_test(staged).await?;

    // Rename the old binary aside rather than deleting it: on Unix this is
    // allowed while it is running, on Windows it is the only way to replace a
    // running image, and either way it leaves something to fall back to by
    // hand if the new build turns out to be bad in a way `--version` could not
    // detect.
    let backup = sibling(current, ".bak");
    let _ = std::fs::remove_file(&backup);
    std::fs::rename(current, &backup)
        .with_context(|| format!("move {} aside", current.display()))?;
    if let Err(error) = std::fs::rename(staged, current) {
        // Put the old binary back, or the service has no executable at all.
        if let Err(restore) = std::fs::rename(&backup, current) {
            tracing::error!(
                %restore, backup = %backup.display(), path = %current.display(),
                "could not restore the previous binary; the service will not start until this is fixed by hand"
            );
        }
        return Err(error).with_context(|| format!("install {}", current.display()));
    }
    Ok(())
}

async fn download(
    transport: &Transport,
    manifest: &UpdateManifest,
    crypto: &Crypto,
    dest: &Path,
) -> Result<()> {
    let mut file = tokio::fs::File::create(dest)
        .await
        .with_context(|| format!("create {}", dest.display()))?;
    let mut hasher = Hasher::new();
    let mut written: u64 = 0;
    for index in 0..manifest.chunks {
        let data = transport
            .get_release_chunk(&manifest.release, index, crypto)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("release chunk {index} is missing; the release may have been cleaned up")
            })?;
        written = written.saturating_add(data.len() as u64);
        if written > manifest.total_bytes {
            bail!("release carries more bytes than its manifest declares");
        }
        hasher.update(&data);
        file.write_all(&data).await.context("write release chunk")?;
    }
    file.flush().await.context("flush the staged binary")?;
    // Closed before anything execs or renames it: Windows refuses both while a
    // handle is open, and on Unix an unflushed tail would fail the hash below.
    drop(file);

    if written != manifest.total_bytes {
        bail!(
            "release assembled {written} bytes but the manifest declares {}",
            manifest.total_bytes
        );
    }
    let sha256 = hasher.finish_hex();
    if sha256 != manifest.sha256 {
        bail!(
            "release hash mismatch: manifest says {}, assembled {sha256}",
            manifest.sha256
        );
    }
    Ok(())
}

/// Run the staged binary once before trusting it with the fleet.
///
/// `--version` is the cheapest thing that still proves the file is executable,
/// built for this architecture, and dynamically linkable on this host — the
/// three ways a released binary usually fails to start. It runs with a clean
/// environment so nothing in the agent's own configuration can influence the
/// result.
async fn smoke_test(path: &Path) -> Result<()> {
    let mut command = tokio::process::Command::new(path);
    command.arg("--version").env_clear().stdin(std::process::Stdio::null());
    // PATH is not needed to exec an absolute path, but a dynamic loader on some
    // systems still wants a minimal environment to resolve libraries.
    if let Some(path_var) = std::env::var_os("PATH") {
        command.env("PATH", path_var);
    }

    let output = tokio::time::timeout(SMOKE_TEST_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            anyhow::anyhow!("staged binary did not answer --version within {SMOKE_TEST_TIMEOUT:?}")
        })?
        .with_context(|| format!("execute the staged binary {}", path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "staged binary failed its --version check ({}): {}",
            output.status,
            stderr.trim().chars().take(200).collect::<String>()
        );
    }
    Ok(())
}

/// Split a local file into sealed chunks under a release prefix.
///
/// The controller side of a publish. Mirrors [`blob::stage_file`](crate::blob::stage_file)
/// but seals with the one-off release key instead of a per-agent key, which is
/// what lets one upload serve the whole fleet.
pub async fn stage_release(
    transport: &Transport,
    release: &str,
    crypto: &Crypto,
    source: &Path,
) -> Result<(u32, u64, String)> {
    let mut file = tokio::fs::File::open(source)
        .await
        .with_context(|| format!("open {}", source.display()))?;
    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; BLOB_CHUNK_BYTES];
    let mut chunks: u32 = 0;
    let mut total_bytes: u64 = 0;
    loop {
        let read = fill(&mut file, &mut buffer).await?;
        if read == 0 && chunks > 0 {
            break;
        }
        let data = &buffer[..read];
        hasher.update(data);
        transport.put_release_chunk(release, chunks, data, crypto).await?;
        total_bytes = total_bytes.saturating_add(read as u64);
        chunks = chunks.checked_add(1).context("release has too many chunks")?;
        if read < buffer.len() {
            break;
        }
    }
    Ok((chunks, total_bytes, hasher.finish_hex()))
}

/// Fresh 32-byte release key, base64 encoded. One per publish: it is only ever
/// seen by the agents that were given the matching manifest.
pub fn new_release_key() -> String {
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    B64.encode(key)
}

pub async fn file_sha256(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await.context("read file")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finish_hex())
}

/// Read until `buffer` is full or EOF. A short read in the middle of a file
/// would otherwise produce an undersized chunk and break length accounting on
/// the receiving side.
async fn fill(file: &mut tokio::fs::File, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = file.read(&mut buffer[filled..]).await.context("read source file")?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

/// Append a suffix to a file name, keeping it in the same directory.
///
/// Not `with_extension`: that would turn `relay-agent.exe` into
/// `relay-agent.bak` and lose the extension Windows needs to exec it.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffixes_keep_the_original_extension() {
        assert_eq!(
            sibling(Path::new("/usr/local/bin/relay-agent"), ".bak"),
            PathBuf::from("/usr/local/bin/relay-agent.bak")
        );
        // The case with_extension would get wrong.
        assert_eq!(
            sibling(Path::new("C:/bin/relay-agent.exe"), ".relay-update"),
            PathBuf::from("C:/bin/relay-agent.exe.relay-update")
        );
    }

    #[test]
    fn release_keys_are_32_bytes_and_distinct() {
        let first = new_release_key();
        let second = new_release_key();
        assert_eq!(B64.decode(&first).unwrap().len(), 32);
        assert_ne!(first, second, "a release key must never be reused");
        Crypto::from_base64(&first, "release").expect("usable as a key");
    }

    #[test]
    fn deferred_updates_are_reconsidered_but_settled_ones_are_not() {
        assert!(!Outcome::Deferred { jobs_running: 1 }.is_settled());
        assert!(Outcome::UpToDate.is_settled());
        assert!(Outcome::Skipped { reason: String::new() }.is_settled());
    }

    #[test]
    fn state_round_trips_through_a_file() {
        let dir = std::env::temp_dir().join(format!("relay-update-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("update-state.json");
        let state = UpdateState {
            last_published_at: 1_700_000_000,
            version: "0.2.0".into(),
            sha256: "abc".into(),
            applied_at: 1_700_000_001,
        };
        state.save(&path).unwrap();
        let loaded = UpdateState::load(&path);
        assert_eq!(loaded.last_published_at, 1_700_000_000);
        assert_eq!(loaded.version, "0.2.0");
        // A missing or corrupt file must read as "nothing applied yet" rather
        // than failing the agent's startup.
        assert_eq!(UpdateState::load(&dir.join("absent.json")).last_published_at, 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
