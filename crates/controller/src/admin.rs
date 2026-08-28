use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use common::pairing;

use crate::registry::{self, AgentKey, Registry};

pub fn dispatch() -> Result<bool> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first().map(String::as_str) else { return Ok(false) };
    match command {
        "init" => init()?,
        "add" => add(args.get(1).context("usage: s3-relay-mcp add <pairing-code>")?)?,
        "reset" => reset()?,
        "status" => status()?,
        "update" => pairing::update("s3-relay-mcp")?,
        "-h" | "--help" | "help" => help(),
        _ => bail!("unknown command {command}; use init, add, reset, status, update, or run without arguments"),
    }
    Ok(true)
}

fn init() -> Result<()> {
    let path = pairing::config_path("controller");
    if path.exists() { bail!("{} already exists; use reset first", path.display()); }
    let endpoint = pairing::prompt("RainYun S3 endpoint", false)?;
    let region = pairing::prompt("RainYun region", false)?;
    let bucket = pairing::prompt("RainYun bucket", false)?;
    let access = pairing::prompt("RainYun access key id", false)?;
    let secret = pairing::prompt("RainYun secret access key", true)?;
    let (private, public) = pairing::generate_keypair();
    let database = path.with_extension("db");
    let log_dir = path.parent().unwrap_or_else(|| std::path::Path::new(".")).join("logs");
    let q = |v: &str| toml::Value::String(v.to_owned()).to_string();
    let text = format!(
        "[s3]\nendpoint = {}\nregion = {}\nbucket = {}\nprefix = \"relay-prod/\"\nforce_path_style = true\naccess_key_id = {}\nsecret_access_key = {}\n\n[controller]\nprivate_key = {}\npublic_key = {}\ndatabase = {}\nlog_dir = {}\nqueue_ttl_secs = 120\nmax_exec_secs = 300\nmax_wait_secs = 430\nmax_transfer_secs = 1800\npoll_ms = 200\n",
        q(&endpoint), q(&region), q(&bucket), q(&access), q(&secret),
        q(&private), q(&public), q(&database.display().to_string()), q(&log_dir.display().to_string())
    );
    pairing::write_private(&path, &text)?;
    Registry::open(database)?;
    println!("wrote {} (owner-only)", path.display());
    println!("Controller public key (enter this during agent init):\n{public}");
    println!("verification: {}", pairing::fingerprint(&public)?);
    Ok(())
}

fn add(code: &str) -> Result<()> {
    let enrollment = pairing::decode_enrollment(code)?;
    let (private, database, _) = settings()?;
    let _derived = pairing::derive_key(&private, &enrollment.public_key, &enrollment.agent_id)?;
    Registry::open(database)?.add(&AgentKey {
        id: enrollment.agent_id.clone(), public_key: enrollment.public_key.clone(),
    })?;
    println!("added agent {}", enrollment.agent_id);
    println!("verification: {}", pairing::fingerprint(&enrollment.public_key)?);
    println!("Restart the MCP session; the agent will then appear in list_agents.");
    Ok(())
}

fn reset() -> Result<()> {
    let path = pairing::config_path("controller");
    let database = settings().map(|(_, db, _)| db).unwrap_or_else(|_| registry::default_path());
    let confirm = pairing::prompt("Type RESET to delete controller configuration and enrolled nodes", false)?;
    if confirm != "RESET" { bail!("reset cancelled"); }
    if path.exists() { std::fs::remove_file(&path)?; }
    for db in [database.clone(), PathBuf::from(format!("{}-wal", database.display())), PathBuf::from(format!("{}-shm", database.display()))] {
        if db.exists() { std::fs::remove_file(db)?; }
    }
    println!("controller reset");
    Ok(())
}

fn status() -> Result<()> {
    let path = pairing::config_path("controller");
    if !path.exists() { println!("not initialized ({})", path.display()); return Ok(()); }
    let (_, database, log_dir) = settings()?;
    let registry = Registry::open(database)?;
    println!("initialized=yes config={} database={} log_dir={} agents={}",
        path.display(), registry.path().display(), log_dir.display(), registry.agents()?.len());
    Ok(())
}

fn settings() -> Result<(String, PathBuf, PathBuf)> {
    let path = pairing::config_path("controller");
    let table: toml::Table = std::fs::read_to_string(&path)?.parse()?;
    let section = table.get("controller").and_then(toml::Value::as_table).context("missing [controller]")?;
    let private = section.get("private_key").and_then(toml::Value::as_str).context("missing controller.private_key")?.to_owned();
    let database = section.get("database").and_then(toml::Value::as_str).map(PathBuf::from).unwrap_or_else(registry::default_path);
    let log_dir = section.get("log_dir").and_then(toml::Value::as_str).map(PathBuf::from)
        .unwrap_or_else(|| database.parent().unwrap_or_else(|| std::path::Path::new(".")).join("logs"));
    Ok((private, database, log_dir))
}

fn help() {
    println!("s3-relay-mcp [init | add <pairing-code> | reset | status | update]\nRun without arguments to start the MCP server.");
}
