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

fn marker_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".shipped");
    PathBuf::from(value)
}

fn read_marker(path: &Path) -> u64 {
    std::fs::read_to_string(marker_path(path)).ok()
        .and_then(|value| value.trim().parse().ok()).unwrap_or(0)
}

fn safe_source(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?.to_owned();
    let job_log = matches!(path.extension().and_then(|v| v.to_str()), Some("out" | "err"));
    let agent_log = name.starts_with("agent.log.");
    ((job_log || agent_log) && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')))
        .then_some(name)
}
