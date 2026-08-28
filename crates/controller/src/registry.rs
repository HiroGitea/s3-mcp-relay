use std::path::{Path, PathBuf};

use anyhow::Result;
use rusqlite::{params, Connection};

#[derive(Debug, Clone)]
pub struct AgentKey {
    pub id: String,
    pub public_key: String,
}

#[derive(Clone)]
pub struct Registry { path: PathBuf }

impl Registry {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let this = Self { path: path.into() };
        if let Some(parent) = this.path.parent() { std::fs::create_dir_all(parent)?; }
        let conn = Connection::open(&this.path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS agents(
               id TEXT PRIMARY KEY, public_key TEXT NOT NULL,
               added_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS events(
               id INTEGER PRIMARY KEY, agent_id TEXT NOT NULL, at INTEGER NOT NULL,
               kind TEXT NOT NULL, message TEXT NOT NULL,
               UNIQUE(agent_id, at, kind, message)
             );
             CREATE INDEX IF NOT EXISTS events_agent_at ON events(agent_id, at);"
        )?;
        Ok(this)
    }

    pub fn path(&self) -> &Path { &self.path }

    pub fn add(&self, key: &AgentKey) -> Result<()> {
        let now = common::protocol::now_unix();
        Connection::open(&self.path)?.execute(
            "INSERT INTO agents(id,public_key,added_at,updated_at) VALUES(?1,?2,?3,?3)
             ON CONFLICT(id) DO UPDATE SET public_key=excluded.public_key,updated_at=excluded.updated_at",
            params![key.id, key.public_key, now],
        )?;
        Ok(())
    }

    pub fn agents(&self) -> Result<Vec<AgentKey>> {
        let conn = Connection::open(&self.path)?;
        let mut stmt = conn.prepare("SELECT id,public_key FROM agents ORDER BY id")?;
        // Collected into a local first: returning the expression directly would
        // keep the row iterator borrowing `stmt` past the end of the function.
        let agents = stmt
            .query_map([], |row| Ok(AgentKey { id: row.get(0)?, public_key: row.get(1)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(agents)
    }

    pub fn event(&self, agent: &str, at: i64, kind: &str, message: &str) -> Result<()> {
        Connection::open(&self.path)?.execute(
            "INSERT OR IGNORE INTO events(agent_id,at,kind,message) VALUES(?1,?2,?3,?4)",
            params![agent, at, kind, message],
        )?;
        Ok(())
    }
}

pub fn default_path() -> PathBuf {
    std::env::var_os("RELAY_CONFIG").map(PathBuf::from)
        .and_then(|path| path.parent().map(|parent| parent.join("controller.db")))
        .unwrap_or_else(|| common::pairing::config_path("controller").with_extension("db"))
}
