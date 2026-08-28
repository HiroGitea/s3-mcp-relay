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
             CREATE INDEX IF NOT EXISTS events_agent_at ON events(agent_id, at);
             -- When each agent was last heard from, and what it was running.
             -- Heartbeats are deleted as soon as they go stale, so the bucket
             -- cannot answer 'when did we last see this machine' — which is
             -- exactly what decides whether a silent agent still counts.
             CREATE TABLE IF NOT EXISTS agent_seen(
               id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL,
               version TEXT, binary_sha256 TEXT, platform TEXT
             );
             -- One row per published release, kept after the release itself is
             -- cleaned out of the bucket. Without this, automatic publishing
             -- would see no manifest, conclude nothing had been published, and
             -- upload the same build again on every check.
             CREATE TABLE IF NOT EXISTS rollouts(
               release TEXT PRIMARY KEY, version TEXT NOT NULL, target TEXT NOT NULL,
               sha256 TEXT NOT NULL, published_at INTEGER NOT NULL, completed_at INTEGER
             );
             CREATE INDEX IF NOT EXISTS rollouts_version ON rollouts(version, target);"
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

    /// Record that an agent is alive, and what it is running.
    pub fn mark_seen(&self, seen: &SeenAgent) -> Result<()> {
        Connection::open(&self.path)?.execute(
            "INSERT INTO agent_seen(id,last_seen,version,binary_sha256,platform) VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen,version=excluded.version,
               binary_sha256=excluded.binary_sha256,platform=excluded.platform",
            params![seen.id, seen.last_seen, seen.version, seen.binary_sha256, seen.platform],
        )?;
        Ok(())
    }

    /// Agents heard from at or after `cutoff`.
    ///
    /// Everything older is treated as decommissioned: it does not hold a
    /// rollout open, and it is not counted when deciding which platforms need
    /// a build.
    pub fn seen_since(&self, cutoff: i64) -> Result<Vec<SeenAgent>> {
        let conn = Connection::open(&self.path)?;
        let mut stmt = conn.prepare(
            "SELECT id,last_seen,version,binary_sha256,platform FROM agent_seen
             WHERE last_seen >= ?1 ORDER BY id",
        )?;
        let rows = stmt
            .query_map(params![cutoff], |row| {
                Ok(SeenAgent {
                    id: row.get(0)?,
                    last_seen: row.get(1)?,
                    version: row.get(2)?,
                    binary_sha256: row.get(3)?,
                    platform: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn record_rollout(&self, rollout: &Rollout) -> Result<()> {
        Connection::open(&self.path)?.execute(
            "INSERT INTO rollouts(release,version,target,sha256,published_at) VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(release) DO UPDATE SET version=excluded.version,target=excluded.target,
               sha256=excluded.sha256,published_at=excluded.published_at",
            params![rollout.release, rollout.version, rollout.target, rollout.sha256, rollout.published_at],
        )?;
        Ok(())
    }

    pub fn complete_rollout(&self, release: &str, at: i64) -> Result<()> {
        Connection::open(&self.path)?.execute(
            "UPDATE rollouts SET completed_at=?2 WHERE release=?1",
            params![release, at],
        )?;
        Ok(())
    }

    /// Whether this version has already been rolled out to this platform.
    ///
    /// Asked before publishing automatically, and answered without downloading
    /// anything — which is the point. The row outlives the release in the
    /// bucket, so a rollout that finished and was swept up is still remembered
    /// and is not started again from the beginning on the next check.
    pub fn already_published(&self, version: &str, target: &str) -> Result<bool> {
        let conn = Connection::open(&self.path)?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM rollouts WHERE version=?1 AND target=?2",
            params![version, target],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }
}

#[derive(Debug, Clone)]
pub struct SeenAgent {
    pub id: String,
    pub last_seen: i64,
    pub version: Option<String>,
    pub binary_sha256: Option<String>,
    pub platform: Option<String>,
}

impl SeenAgent {
    /// Whether this agent is running the build a manifest describes.
    ///
    /// The hash is exact and is what a release is named by. An agent too old to
    /// report one falls back to the version string, which is weaker — a rebuilt
    /// release carries the same string — but is better than never being able to
    /// conclude a rollout finished.
    pub fn runs(&self, sha256: &str, version: &str) -> bool {
        match &self.binary_sha256 {
            Some(installed) => installed == sha256,
            None => self.version.as_deref() == Some(version),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rollout {
    pub release: String,
    pub version: String,
    pub target: String,
    pub sha256: String,
    pub published_at: i64,
}

pub fn default_path() -> PathBuf {
    std::env::var_os("RELAY_CONFIG").map(PathBuf::from)
        .and_then(|path| path.parent().map(|parent| parent.join("controller.db")))
        .unwrap_or_else(|| common::pairing::config_path("controller").with_extension("db"))
}
