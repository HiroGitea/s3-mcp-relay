use std::collections::HashMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use common::protocol::{now_unix, LogChunk};
use common::Transport;

#[derive(Clone)]
pub struct LogShipper {
    dir: PathBuf,
    offsets: Arc<Mutex<HashMap<PathBuf, u64>>>,
    chunk_bytes: usize,
}

impl LogShipper {
    pub fn new(dir: PathBuf, chunk_bytes: usize) -> Self {
        Self { dir, offsets: Arc::new(Mutex::new(HashMap::new())), chunk_bytes }
    }

    pub async fn ship_once(&self, transport: &Transport, agent_id: &str) -> Result<()> {
        let entries = std::fs::read_dir(&self.dir).with_context(|| format!("scan {}", self.dir.display()))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(source) = safe_source(&path) else { continue };
            let offset = self.offsets.lock().map_err(|_| anyhow::anyhow!("log offset table poisoned"))?
                .get(&path).copied().unwrap_or_else(|| read_marker(&path));
            let size = entry.metadata()?.len();
            if size <= offset { continue; }
            let read_len = (size - offset).min(self.chunk_bytes as u64) as usize;
            let mut file = std::fs::File::open(&path)?;
            file.seek(std::io::SeekFrom::Start(offset))?;
            let mut data = vec![0; read_len];
            file.read_exact(&mut data)?;
            let chunk = LogChunk {
                agent_id: agent_id.to_owned(), source, offset,
                data_b64: B64.encode(data), at: now_unix(),
            };
            transport.publish_log_chunk(&chunk).await?;
            let next = offset + read_len as u64;
            std::fs::write(marker_path(&path), next.to_string())?;
            self.offsets.lock().map_err(|_| anyhow::anyhow!("log offset table poisoned"))?
                .insert(path, next);
        }
        Ok(())
    }
}

/// Suffix of the sidecar file that records how much of a log has been shipped.
/// Named once because [`marker_path`] creates these files and [`safe_source`]
/// has to refuse them; the two disagreeing is what caused the loop below.
const MARKER_SUFFIX: &str = ".shipped";

/// Longest source name the transport accepts. Checked here as well so an
/// over-long name is skipped quietly instead of being retried forever against
/// a limit this side never sees.
const MAX_SOURCE_BYTES: usize = 96;

fn marker_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(MARKER_SUFFIX);
    PathBuf::from(value)
}

fn read_marker(path: &Path) -> u64 {
    std::fs::read_to_string(marker_path(path)).ok()
        .and_then(|value| value.trim().parse().ok()).unwrap_or(0)
}

fn safe_source(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?.to_owned();
    // Markers sit beside the logs they track and are named after them, so
    // `agent.log.<date>.shipped` still matches the agent-log prefix below.
    // Shipping one writes a marker for the marker, and every pass appends
    // another suffix — until the name passes 96 bytes, the transport rejects
    // it, and the agent retries the same doomed upload once a second forever,
    // writing a warning each time into the very log it is trying to ship.
    if name.ends_with(MARKER_SUFFIX) {
        return None;
    }
    let job_log = matches!(path.extension().and_then(|v| v.to_str()), Some("out" | "err"));
    let agent_log = name.starts_with("agent.log.");
    ((job_log || agent_log)
        && name.len() <= MAX_SOURCE_BYTES
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')))
        .then_some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ships_job_output_and_the_agent_log() {
        assert_eq!(safe_source(Path::new("/jobs/abc.out")).as_deref(), Some("abc.out"));
        assert_eq!(safe_source(Path::new("/jobs/abc.err")).as_deref(), Some("abc.err"));
        assert_eq!(
            safe_source(Path::new("/jobs/agent.log.2026-08-28")).as_deref(),
            Some("agent.log.2026-08-28")
        );
    }

    #[test]
    fn never_ships_its_own_offset_markers() {
        // The regression: each of these was shipped, producing the next one.
        assert_eq!(safe_source(Path::new("/jobs/agent.log.2026-08-28.shipped")), None);
        assert_eq!(safe_source(Path::new("/jobs/agent.log.2026-08-28.shipped.shipped")), None);
        assert_eq!(safe_source(Path::new("/jobs/abc.out.shipped")), None);
    }

    #[test]
    fn a_marker_is_named_so_that_it_is_refused() {
        // Ties the two halves together: whatever marker_path produces for a
        // shippable log must be something safe_source declines.
        let log = Path::new("/jobs/agent.log.2026-08-28");
        assert!(safe_source(log).is_some());
        assert_eq!(safe_source(&marker_path(log)), None);
    }

    #[test]
    fn skips_names_the_transport_would_reject() {
        let long = format!("agent.log.{}", "x".repeat(MAX_SOURCE_BYTES));
        assert_eq!(safe_source(Path::new(&long)), None);
        assert_eq!(safe_source(Path::new("/jobs/weird name.out")), None);
    }
}
