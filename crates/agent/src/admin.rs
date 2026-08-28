use anyhow::{bail, Context, Result};
use common::pairing;

pub fn dispatch() -> Result<bool> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first().map(String::as_str) else { return Ok(false) };
    match command {
        "init" => init(args.get(1).map(String::as_str))?,
        "reset" => reset()?,
        "status" => status()?,
        "update" => pairing::update("relay-agent")?,
        "-h" | "--help" | "help" => help(),
        _ => bail!("unknown command {command}; use init, reset, status, update, or run without arguments"),
    }
    Ok(true)
}

fn init(suggested_name: Option<&str>) -> Result<()> {
    let path = pairing::config_path("agent");
    if path.exists() { bail!("{} already exists; use reset first", path.display()); }
    let agent_id = suggested_name.map(ToOwned::to_owned)
        .unwrap_or(pairing::prompt("Agent name", false)?);
    common::validate_agent_id(&agent_id)?;
    let endpoint = pairing::prompt("RainYun S3 endpoint", false)?;
    let region = pairing::prompt("RainYun region", false)?;
    let bucket = pairing::prompt("RainYun bucket", false)?;
    let access = pairing::prompt("RainYun access key id", false)?;
    let secret = pairing::prompt("RainYun secret access key", true)?;
    let controller_public = pairing::prompt("Controller public key", false)?;
    let (private, public) = pairing::generate_keypair();
    let _ = pairing::derive_key(&private, &controller_public, &agent_id)?;
    let q = |v: &str| toml::Value::String(v.to_owned()).to_string();
    let text = format!(
        "[s3]\nendpoint = {}\nregion = {}\nbucket = {}\nprefix = \"relay-prod/\"\nforce_path_style = true\naccess_key_id = {}\nsecret_access_key = {}\n\n[agent]\nid = {}\nprivate_key = {}\npublic_key = {}\ncontroller_public_key = {}\nallow_any_path = false\nallow_any_program = false\nallowed_roots = []\nallowed_programs = []\njob_retention_days = 7\njob_max_total_bytes = 1073741824\njob_cleanup_interval_secs = 21600\njob_ship_chunk_bytes = 131072\n",
        q(&endpoint), q(&region), q(&bucket), q(&access), q(&secret), q(&agent_id),
        q(&private), q(&public), q(&controller_public)
    );
    pairing::write_private(&path, &text)?;
    let code = pairing::encode_enrollment(&agent_id, &public)?;
    println!("wrote {} (owner-only)", path.display());
    println!("verification: {}", pairing::fingerprint(&public)?);
    println!("On the controller run:\n  s3-relay-mcp add {code}");
    Ok(())
}

fn reset() -> Result<()> {
    let path = pairing::config_path("agent");
    let confirm = pairing::prompt("Type RESET to delete the agent identity", false)?;
    if confirm != "RESET" { bail!("reset cancelled"); }
    if path.exists() { std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?; }
    println!("agent identity reset");
    Ok(())
}

fn status() -> Result<()> {
    let path = pairing::config_path("agent");
    if !path.exists() { println!("not initialized ({})", path.display()); return Ok(()); }
    let table: toml::Table = std::fs::read_to_string(&path)?.parse()?;
    let id = table.get("agent").and_then(|v| v.get("id")).and_then(toml::Value::as_str).unwrap_or("unknown");
    println!("agent={id} config={} initialized=yes", path.display());
    Ok(())
}

fn help() {
    println!("relay-agent [init [name] | reset | status | update]\nRun without arguments to start the agent.");
}
