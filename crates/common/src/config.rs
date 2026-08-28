//! Settings for both ends of the relay, resolved from two sources.
//!
//! An optional TOML file supplies the boring parts; the process environment
//! always overrides it. That ordering lets a deployment keep a readable,
//! committable `relay.toml` while systemd credentials, a keychain, or the
//! launching shell inject the parts that must not sit on disk.
//!
//! Secrets may live in the file. When they do, the file permissions are the
//! only thing protecting them, so a config that carries a secret and is group-
//! or world-readable draws a warning on load. Keeping secrets in the
//! environment instead is still supported and still slightly safer, since a
//! stray `cat` or backup cannot leak what was never written down.
//!
//! Unknown keys are hard errors — a typo in a security setting like
//! `agent.allowed_programs` would otherwise fail closed and cost an afternoon
//! of debugging.
//!
//! The same binaries work against MinIO, Ceph RGW, Cloudflare R2, Backblaze B2,
//! RainYun ROS, or AWS S3 by pointing `s3.endpoint` somewhere else.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};

static FILE: OnceLock<toml::Table> = OnceLock::new();

/// Every setting the file may contain. Anything else is rejected on load.
const KNOWN_KEYS: &[&str] = &[
    "s3.endpoint",
    "s3.region",
    "s3.bucket",
    "s3.prefix",
    "s3.force_path_style",
    "s3.allow_insecure_http",
    "s3.access_key_id",
    "s3.secret_access_key",
    "relay.key_id",
    "relay.shared_key",
    "controller.allowed_agents",
    "controller.queue_ttl_secs",
    "controller.max_exec_secs",
    "controller.max_wait_secs",
    "controller.max_transfer_secs",
    "controller.poll_ms",
    "controller.private_key",
    "controller.public_key",
    "controller.database",
    "controller.log_dir",
    "controller.auto_cleanup",
    "controller.cleanup_check_secs",
    "controller.update_seen_days",
    "controller.auto_publish_repo",
    "controller.auto_publish_check_secs",
    "agent.id",
    "agent.allowed_roots",
    "agent.allowed_programs",
    "agent.allow_any_path",
    "agent.allow_any_program",
    "agent.max_blob_bytes",
    "agent.child_env_allowlist",
    "agent.max_timeout_secs",
    "agent.max_file_bytes",
    "agent.max_output_bytes",
    "agent.poll_ms",
    "agent.poll_max_ms",
    "agent.full_scan_secs",
    "agent.doorbell",
    "agent.heartbeat_secs",
    "agent.heartbeat_ttl_secs",
    "agent.private_key",
    "agent.public_key",
    "agent.controller_public_key",
    "agent.job_dir",
    "agent.job_retention_days",
    "agent.job_max_total_bytes",
    "agent.job_cleanup_interval_secs",
    "agent.job_ship_chunk_bytes",
    "agent.auto_update",
    "agent.update_check_secs",
];

/// Settings whose presence in the file makes its permissions security-relevant.
const SECRET_KEYS: &[&str] = &[
    "s3.access_key_id",
    "s3.secret_access_key",
    "relay.shared_key",
    "controller.private_key",
    "agent.private_key",
];

/// Load the optional TOML file. Call once at startup, before reading settings.
///
/// Looks at `RELAY_CONFIG` first, then `relay.toml` in the working directory.
/// Returns the path that was loaded, or `None` when running purely on
/// environment variables. A missing `RELAY_CONFIG` target is an error, since
/// naming a file that is not there is always a mistake; a missing default
/// `relay.toml` is not.
pub fn init_config() -> Result<Option<PathBuf>> {
    let explicit = std::env::var("RELAY_CONFIG").ok().filter(|value| !value.is_empty());
    let path = explicit.as_deref().map_or_else(|| PathBuf::from("relay.toml"), PathBuf::from);
    if !path.exists() {
        if explicit.is_some() {
            bail!("RELAY_CONFIG points at {}, which does not exist", path.display());
        }
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("read config file {}", path.display()))?;
    let table: toml::Table = text
        .parse()
        .with_context(|| format!("parse config file {}", path.display()))?;
    validate(&table, &path)?;
    warn_on_loose_permissions(&table, &path);
    FILE.set(table)
        .map_err(|_| anyhow::anyhow!("init_config was called more than once"))?;
    Ok(Some(path))
}

/// A config file may hold secrets, in which case its mode is the only thing
/// protecting them. Warn rather than refuse: failing to start would surface
/// inside Claude as an opaque MCP error, while a warning on stderr says exactly
/// what to fix.
#[cfg(unix)]
fn warn_on_loose_permissions(table: &toml::Table, path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    if !SECRET_KEYS.iter().any(|key| lookup_path(table, key).is_some()) {
        return;
    }
    let Ok(metadata) = std::fs::metadata(path) else { return };
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        tracing::warn!(
            path = %path.display(),
            mode = format!("{mode:03o}"),
            "config file holds a secret but is readable beyond its owner; chmod 600 it"
        );
    }
}

#[cfg(not(unix))]
fn warn_on_loose_permissions(_table: &toml::Table, _path: &Path) {}

fn validate(table: &toml::Table, path: &Path) -> Result<()> {
    let known: BTreeSet<&str> = KNOWN_KEYS.iter().copied().collect();
    for (section, value) in table {
        let Some(fields) = value.as_table() else {
            bail!(
                "{} may only contain [sections], but `{section}` is a bare value",
                path.display()
            );
        };
        for field in fields.keys() {
            let dotted = format!("{section}.{field}");
            if !known.contains(dotted.as_str()) {
                bail!("unknown setting `{dotted}` in {}", path.display());
            }
        }
    }
    Ok(())
}

fn lookup_path<'a>(table: &'a toml::Table, dotted: &str) -> Option<&'a toml::Value> {
    let (section, field) = dotted.split_once('.')?;
    table.get(section)?.as_table()?.get(field)
}

/// Map an environment variable name onto its TOML location.
fn toml_path(key: &str) -> Option<(&'static str, String)> {
    // The agent id names the machine, so it reads better under [agent] than as
    // `relay.agent_id`, which is what the generic rule below would produce.
    if key == "RELAY_AGENT_ID" {
        return Some(("agent", "id".to_owned()));
    }
    let (prefix, rest) = key.split_once('_')?;
    let section = match prefix {
        "S3" => "s3",
        "AGENT" => "agent",
        "CONTROL" => "controller",
        "RELAY" => "relay",
        _ => return None,
    };
    Some((section, rest.to_ascii_lowercase()))
}

fn from_file(key: &str) -> Option<String> {
    let (section, field) = toml_path(key)?;
    let value = FILE.get()?.get(section)?.as_table()?.get(&field)?;
    render(key, value)
}

/// Flatten a TOML value into the string shape the existing parsers expect.
fn render(key: &str, value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::String(text) => Some(text.clone()),
        toml::Value::Integer(number) => Some(number.to_string()),
        toml::Value::Boolean(flag) => Some(flag.to_string()),
        toml::Value::Array(items) => {
            // Allowed roots are parsed with std::env::split_paths, which wants
            // ';' on Windows and ':' elsewhere. Every other list is comma
            // separated, and must stay that way: a Windows program path like
            // C:\Windows\System32\cmd.exe contains a colon.
            let separator = if key == "AGENT_ALLOWED_ROOTS" {
                if cfg!(windows) { ";" } else { ":" }
            } else {
                ","
            };
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(item.as_str()?.to_owned());
            }
            Some(parts.join(separator))
        }
        _ => None,
    }
}

/// Everything needed to reach the shared bucket.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Full endpoint URL, e.g. `https://minio.example.com:9000`,
    /// `https://<account>.r2.cloudflarestorage.com`, or for RainYun ROS
    /// `https://cn-sy1.rains3.com` (the console shows it with the bucket name
    /// appended; drop that part).
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Key namespace inside the bucket; keeps the relay tidy and lets one
    /// bucket host several independent channels. Always ends with `/`.
    pub prefix: String,
    /// Path-style addressing (`endpoint/bucket/key`). Required by most
    /// self-hosted S3 servers; AWS/R2 also accept it.
    pub force_path_style: bool,
    /// Reject plaintext HTTP endpoints unless this is explicitly enabled for
    /// a trusted local development network.
    pub allow_insecure_http: bool,
}

impl S3Config {
    /// Build from the environment and the optional config file.
    ///
    /// | Variable                | `relay.toml`               | Required | Default     |
    /// |-------------------------|----------------------------|----------|-------------|
    /// | `S3_ENDPOINT`           | `s3.endpoint`              | yes      | —           |
    /// | `S3_BUCKET`             | `s3.bucket`                | yes      | —           |
    /// | `S3_ACCESS_KEY_ID`      | `s3.access_key_id`         | yes      | —           |
    /// | `S3_SECRET_ACCESS_KEY`  | `s3.secret_access_key`     | yes      | —           |
    /// | `S3_REGION`             | `s3.region`                | no       | `us-east-1` |
    /// | `S3_PREFIX`             | `s3.prefix`                | no       | `relay/`    |
    /// | `S3_FORCE_PATH_STYLE`   | `s3.force_path_style`      | no       | `true`      |
    /// | `S3_ALLOW_INSECURE_HTTP`| `s3.allow_insecure_http`   | no       | `false`     |
    pub fn from_env() -> Result<Self> {
        let mut prefix = optional("S3_PREFIX").unwrap_or_else(|| "relay/".to_string());
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        Ok(Self {
            endpoint: required("S3_ENDPOINT")?,
            region: optional("S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            bucket: required("S3_BUCKET")?,
            access_key_id: required("S3_ACCESS_KEY_ID")?,
            secret_access_key: required("S3_SECRET_ACCESS_KEY")?,
            prefix,
            force_path_style: optional("S3_FORCE_PATH_STYLE")
                .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "0" | "false" | "no"))
                .unwrap_or(true),
            allow_insecure_http: optional("S3_ALLOW_INSECURE_HTTP")
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
        })
    }

    pub fn validate(&self) -> Result<()> {
        let lower = self.endpoint.to_ascii_lowercase();
        if !lower.starts_with("https://")
            && !(self.allow_insecure_http && lower.starts_with("http://"))
        {
            anyhow::bail!(
                "S3_ENDPOINT must use https:// (set S3_ALLOW_INSECURE_HTTP=true only for trusted local development)"
            );
        }
        if self.bucket.trim().is_empty() {
            anyhow::bail!("S3_BUCKET must not be empty");
        }
        if self.prefix.starts_with('/') || self.prefix.contains("..") {
            anyhow::bail!("S3_PREFIX must be a relative key prefix without '..'");
        }
        Ok(())
    }
}

/// Read a required setting. The environment wins; the config file is the
/// fallback.
pub fn required_env(key: &str) -> Result<String> {
    optional_env(key).ok_or_else(|| match toml_path(key) {
        Some((section, field)) => anyhow::anyhow!(
            "missing required setting: set {key} in the environment, or `{field}` under [{section}] in the config file"
        ),
        None => anyhow::anyhow!("missing required setting {key}"),
    })
}

/// Read an optional setting. The environment wins; the config file is the
/// fallback. Empty environment values count as unset so a blank line in a
/// systemd `EnvironmentFile` does not mask a config-file value.
pub fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| from_file(key))
}

fn required(key: &str) -> Result<String> {
    required_env(key)
}

fn optional(key: &str) -> Option<String> {
    optional_env(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("test fixture parses")
    }

    #[test]
    fn accepts_secrets_in_the_config_file() {
        validate(
            &table("[relay]\nshared_key = \"AAAA\"\n[s3]\nsecret_access_key = \"SK\"\n"),
            Path::new("relay.toml"),
        )
        .expect("secrets are allowed in the file; permissions are what protect them");
    }

    #[test]
    fn every_secret_key_is_a_known_key() {
        // A secret that is not also a known key would be rejected as unknown
        // before its permission check ever ran.
        for key in SECRET_KEYS {
            assert!(KNOWN_KEYS.contains(key), "{key} is missing from KNOWN_KEYS");
        }
    }

    #[test]
    fn refuses_unknown_keys() {
        let path = Path::new("relay.toml");
        let error = validate(&table("[agent]\nallow_programs = []\n"), path)
            .expect_err("a typo must not be silently ignored");
        assert!(error.to_string().contains("agent.allow_programs"));
    }

    #[test]
    fn accepts_a_full_agent_section() {
        let text = "[s3]\nendpoint = \"https://cn-sy1.rains3.com\"\nbucket = \"relay\"\n\
                    [agent]\nid = \"legacy-01\"\npoll_ms = 200\ndoorbell = true\n\
                    allowed_programs = [\"/usr/bin/systemctl\"]\n";
        validate(&table(text), Path::new("relay.toml")).expect("valid config");
    }

    #[test]
    fn renders_lists_with_the_separator_each_parser_expects() {
        let value = toml::Value::Array(vec![
            toml::Value::String("/srv/app".into()),
            toml::Value::String("/var/log/app".into()),
        ]);
        let roots = render("AGENT_ALLOWED_ROOTS", &value).expect("roots render");
        assert_eq!(roots, if cfg!(windows) { "/srv/app;/var/log/app" } else { "/srv/app:/var/log/app" });
        let programs = render("AGENT_ALLOWED_PROGRAMS", &value).expect("programs render");
        assert_eq!(programs, "/srv/app,/var/log/app");
    }

    #[test]
    fn maps_env_names_onto_toml_paths() {
        assert_eq!(toml_path("S3_ENDPOINT"), Some(("s3", "endpoint".to_owned())));
        assert_eq!(toml_path("CONTROL_ALLOWED_AGENTS"), Some(("controller", "allowed_agents".to_owned())));
        assert_eq!(toml_path("AGENT_MAX_FILE_BYTES"), Some(("agent", "max_file_bytes".to_owned())));
        // Special-cased so the file can say [agent] id = "..."
        assert_eq!(toml_path("RELAY_AGENT_ID"), Some(("agent", "id".to_owned())));
        assert_eq!(toml_path("PATH"), None);
    }

    #[test]
    fn every_known_key_is_reachable_from_some_env_name() {
        // Guards against adding a KNOWN_KEYS entry that no lookup can ever hit,
        // which would accept a setting and then ignore it.
        for dotted in KNOWN_KEYS {
            let (section, field) = dotted.split_once('.').expect("known keys are dotted");
            let env_name = match section {
                "s3" => format!("S3_{}", field.to_ascii_uppercase()),
                "agent" if field == "id" => "RELAY_AGENT_ID".to_owned(),
                "agent" => format!("AGENT_{}", field.to_ascii_uppercase()),
                "controller" => format!("CONTROL_{}", field.to_ascii_uppercase()),
                "relay" => format!("RELAY_{}", field.to_ascii_uppercase()),
                other => panic!("unexpected section {other}"),
            };
            let mapped = toml_path(&env_name)
                .unwrap_or_else(|| panic!("{dotted} is unreachable via {env_name}"));
            assert_eq!(mapped.0, section, "{dotted} maps to the wrong section");
            assert_eq!(mapped.1, field, "{dotted} maps to the wrong field");
        }
    }
}
