//! X25519 enrollment and compact, copy-safe public pairing codes.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD};
use base64::Engine;
use hkdf::Hkdf;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

const DOMAIN: &[u8] = b"s3-relay/x25519/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Enrollment {
    #[serde(rename = "n")]
    pub agent_id: String,
    #[serde(rename = "p")]
    pub public_key: String,
    #[serde(rename = "v")]
    version: u8,
}

pub fn generate_keypair() -> (String, String) {
    let private = StaticSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&private);
    (B64.encode(private.to_bytes()), B64.encode(public.as_bytes()))
}

pub fn derive_key(private_b64: &str, peer_public_b64: &str, agent_id: &str) -> Result<String> {
    crate::validate_agent_id(agent_id)?;
    let private: [u8; 32] = B64.decode(private_b64.trim())
        .context("private key is not base64")?.try_into()
        .map_err(|_| anyhow::anyhow!("private key must contain 32 bytes"))?;
    let public: [u8; 32] = B64.decode(peer_public_b64.trim())
        .context("public key is not base64")?.try_into()
        .map_err(|_| anyhow::anyhow!("public key must contain 32 bytes"))?;
    let shared = StaticSecret::from(private).diffie_hellman(&PublicKey::from(public));
    let hk = Hkdf::<Sha256>::new(Some(DOMAIN), shared.as_bytes());
    let mut key = [0u8; 32];
    hk.expand(agent_id.as_bytes(), &mut key).map_err(|_| anyhow::anyhow!("HKDF expansion failed"))?;
    Ok(B64.encode(key))
}

pub fn encode_enrollment(agent_id: &str, public_key: &str) -> Result<String> {
    crate::validate_agent_id(agent_id)?;
    let public: [u8; 32] = B64.decode(public_key.trim())?.try_into()
        .map_err(|_| anyhow::anyhow!("pairing public key must contain 32 bytes"))?;
    let mut bytes = Vec::with_capacity(34 + agent_id.len());
    bytes.push(1);
    bytes.push(agent_id.len() as u8);
    bytes.extend_from_slice(agent_id.as_bytes());
    bytes.extend_from_slice(&public);
    Ok(format!("R1-{}", URL_SAFE_NO_PAD.encode(bytes)))
}

pub fn decode_enrollment(value: &str) -> Result<Enrollment> {
    let encoded = value.trim().strip_prefix("R1-").context("pairing code must start with R1-")?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).context("pairing code is not valid base64url")?;
    if bytes.len() < 34 || bytes[0] != 1 { bail!("unsupported or malformed pairing code"); }
    let name_len = bytes[1] as usize;
    if bytes.len() != 34 + name_len { bail!("malformed pairing code length"); }
    let agent_id = std::str::from_utf8(&bytes[2..2 + name_len]).context("agent name is not UTF-8")?.to_owned();
    crate::validate_agent_id(&agent_id)?;
    let public_key = B64.encode(&bytes[2 + name_len..]);
    Ok(Enrollment { agent_id, public_key, version: 1 })
}

pub fn fingerprint(public_key: &str) -> Result<String> {
    const EMOJI: [&str; 16] = ["🐟", "🌲", "🚀", "🔑", "🐼", "🍋", "🧊", "🌙", "🦊", "⚙️", "🌊", "🍀", "🧭", "🔥", "🐙", "⭐"];
    let digest = Sha256::digest(B64.decode(public_key.trim())?);
    Ok((0..6).map(|i| EMOJI[(digest[i] & 0x0f) as usize]).collect::<Vec<_>>().join(""))
}

pub fn prompt(label: &str, secret: bool) -> Result<String> {
    eprint!("{label}: ");
    io::stderr().flush()?;
    if secret && cfg!(unix) { let _ = Command::new("stty").arg("-echo").status(); }
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    if secret && cfg!(unix) { let _ = Command::new("stty").arg("echo").status(); eprintln!(); }
    let value = value.trim().to_owned();
    if value.is_empty() { bail!("{label} must not be empty"); }
    Ok(value)
}

pub fn write_private(path: &Path, text: &str) -> Result<()> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).with_context(|| format!("write {}", tmp.display()))?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("install {}", path.display()))
}

pub fn config_path(role: &str) -> PathBuf {
    if let Ok(path) = std::env::var("RELAY_CONFIG") { return PathBuf::from(path); }
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    home.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."))
        .join(".config").join("relay").join(format!("{role}.toml"))
}

pub fn update(binary: &str) -> Result<()> {
    let base = std::env::var("RELAY_UPDATE_BASE_URL").ok()
        .or_else(|| option_env!("RELAY_UPDATE_BASE_URL").map(ToOwned::to_owned))
        .context("this build has no update source; set RELAY_UPDATE_BASE_URL")?;
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let asset = format!("{binary}-{os}-{arch}{suffix}");
    let url = format!("{}/{asset}", base.trim_end_matches('/'));
    let current = std::env::current_exe()?;
    let downloaded = current.with_extension("update");
    let status = Command::new("curl").args(["-fL", "--proto", "=https", "--tlsv1.2", "-o"])
        .arg(&downloaded).arg(&url).status().context("run curl")?;
    if !status.success() { bail!("download failed: {url}"); }
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&downloaded, std::fs::Permissions::from_mode(0o755))?;
        std::fs::rename(&downloaded, &current).context("replace current binary")?;
        println!("updated {} from {}", current.display(), url);
    }
    #[cfg(windows)] println!("downloaded update to {}; replace {} after this process exits", downloaded.display(), current.display());
    Ok(())
}
