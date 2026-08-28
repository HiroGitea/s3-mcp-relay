use std::fs::OpenOptions;
use std::io::{Seek, Write};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use common::LogChunk;
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Clone)]
pub struct LogStore { root: PathBuf, database: PathBuf }

impl LogStore {
    pub fn new(root: PathBuf, database: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Connection::open(&database)?.execute_batch(
            "CREATE TABLE IF NOT EXISTS log_offsets(
               agent_id TEXT NOT NULL, source TEXT NOT NULL, next_offset INTEGER NOT NULL,
               path TEXT NOT NULL, updated_at INTEGER NOT NULL,
               PRIMARY KEY(agent_id, source)
             );"
        )?;
        Ok(Self { root, database })
    }

    pub fn ingest(&self, chunk: &LogChunk) -> Result<()> {
        common::validate_agent_id(&chunk.agent_id)?;
        if chunk.source.is_empty()
            || !chunk.source.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')) {
            bail!("unsafe log source");
        }
        let data = B64.decode(&chunk.data_b64).context("decode log chunk")?;
        let dir = self.root.join(&chunk.agent_id);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(&chunk.source);
        let conn = Connection::open(&self.database)?;
        let next = conn.query_row(
            "SELECT next_offset FROM log_offsets WHERE agent_id=?1 AND source=?2",
            params![chunk.agent_id, chunk.source], |row| row.get::<_, i64>(0),
        ).optional()?.unwrap_or(0).max(0) as u64;
        if chunk.offset > next {
            bail!("log chunk gap for {}/{}: expected {}, got {}", chunk.agent_id, chunk.source, next, chunk.offset);
        }
        if chunk.offset == next {
            let mut file = OpenOptions::new().create(true).write(true).read(true).open(&path)?;
            file.seek(std::io::SeekFrom::Start(chunk.offset))?;
            file.write_all(&data)?;
            file.sync_data()?;
            let new_next = next.saturating_add(data.len() as u64);
            conn.execute(
                "INSERT INTO log_offsets(agent_id,source,next_offset,path,updated_at) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(agent_id,source) DO UPDATE SET next_offset=excluded.next_offset,path=excluded.path,updated_at=excluded.updated_at",
                params![chunk.agent_id, chunk.source, new_next as i64, path.display().to_string(), chunk.at],
            )?;
        }
        Ok(())
    }
}
