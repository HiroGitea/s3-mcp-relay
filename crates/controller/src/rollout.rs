//! Retiring a finished rollout, and starting the next one on its own.
//!
//! Once every agent that still matters is running the published build, the
//! release has done its job and the bucket should not keep carrying it. This
//! sweeps it out, and — when a source repository is configured — notices the
//! next version and publishes it without being asked.
//!
//! "Every agent that still matters" is the whole difficulty. A heartbeat only
//! exists while an agent is alive, so the bucket cannot distinguish a machine
//! that is rebooting from one that was decommissioned last spring. The registry
//! remembers when each agent was last heard from, and anything silent for
//! longer than the cutoff is treated as gone: it does not hold a rollout open,
//! and its manifest is removed with the rest.
//!
//! That is a deliberate trade. A machine off for longer than the cutoff comes
//! back to no manifest and stays on its old binary until something is published
//! again. The alternative — waiting forever — means one retired server pins
//! every release in the bucket permanently.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use common::{Transport, UpdateManifest};

use crate::registry::Registry;

/// What a sweep found, for the log line and for tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    pub releases_removed: Vec<String>,
    pub manifests_removed: usize,
}

pub struct Policy {
    /// An agent silent for longer than this is treated as decommissioned.
    pub seen_window: Duration,
    pub cleanup: bool,
}

/// Remove releases every relevant agent has already installed.
pub async fn sweep(
    transports: &HashMap<String, Transport>,
    registry: &Registry,
    policy: &Policy,
) -> Result<Swept> {
    let mut swept = Swept::default();
    if !policy.cleanup {
        return Ok(swept);
    }
    let now = common::protocol::now_unix();
    let cutoff = now.saturating_sub(policy.seen_window.as_secs() as i64);
    let live: HashMap<String, crate::registry::SeenAgent> = registry
        .seen_since(cutoff)?
        .into_iter()
        .map(|agent| (agent.id.clone(), agent))
        .collect();

    // Group the published manifests by release: one release normally covers
    // several agents, and it can only be deleted once none of them still need
    // it.
    let mut by_release: HashMap<String, Vec<(String, UpdateManifest)>> = HashMap::new();
    for (id, transport) in transports {
        if let Some(manifest) = transport.read_update_manifest(id).await? {
            by_release.entry(manifest.release.clone()).or_default().push((id.clone(), manifest));
        }
    }

    for (release, holders) in by_release {
        let mut complete = true;
        for (id, manifest) in &holders {
            let Some(agent) = live.get(id) else {
                // Silent past the cutoff: not waited for, and its manifest goes
                // with the release.
                continue;
            };
            if !agent.runs(&manifest.sha256, &manifest.version) {
                complete = false;
                break;
            }
        }
        if !complete {
            continue;
        }

        let version = holders.first().map(|(_, m)| m.version.clone()).unwrap_or_default();
        for (id, _) in &holders {
            let Some(transport) = transports.get(id) else { continue };
            transport
                .delete_update_manifest(id)
                .await
                .with_context(|| format!("retract the finished manifest for {id}"))?;
            swept.manifests_removed += 1;
        }
        // Manifests first, then the payload: an agent that reads a manifest
        // whose chunks are already gone would report a failed update, while one
        // that finds no manifest simply has nothing to do.
        if let Some(transport) = transports.values().next() {
            transport
                .delete_release(&release)
                .await
                .with_context(|| format!("delete completed release {release}"))?;
        }
        registry.complete_rollout(&release, now)?;
        tracing::info!(%release, %version, agents = holders.len(), "rollout complete; release removed");
        swept.releases_removed.push(release);
    }
    Ok(swept)
}

/// One asset of a published GitHub release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    pub name: String,
    pub url: String,
}

/// The newest release of a repository, as far as the update path cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestRelease {
    pub tag: String,
    pub assets: Vec<Asset>,
}

/// Asset name the release workflow produces for a platform.
///
/// The workflow labels its matrix `linux-x86_64`, `macos-aarch64` and so on,
/// which is the agent's own `"{os} {arch}"` with the space swapped for a dash.
pub fn asset_name(target: &str) -> String {
    let suffix = if target.starts_with("windows") { ".exe" } else { "" };
    format!("relay-agent-{}{suffix}", target.replace(' ', "-"))
}

pub fn parse_release(body: &[u8]) -> Result<LatestRelease> {
    let value: serde_json::Value =
        serde_json::from_slice(body).context("decode the GitHub release response")?;
    let tag = value
        .get("tag_name")
        .and_then(serde_json::Value::as_str)
        .context("release has no tag_name")?
        .to_owned();
    let assets = value
        .get("assets")
        .and_then(serde_json::Value::as_array)
        .map(|assets| {
            assets
                .iter()
                .filter_map(|asset| {
                    Some(Asset {
                        name: asset.get("name")?.as_str()?.to_owned(),
                        url: asset.get("browser_download_url")?.as_str()?.to_owned(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(LatestRelease { tag, assets })
}

/// `owner/repo`, rejected early rather than interpolated into a URL unchecked.
pub fn validate_repo(repo: &str) -> Result<()> {
    let Some((owner, name)) = repo.split_once('/') else {
        anyhow::bail!("repository must be in owner/name form, got {repo}");
    };
    let ok = |part: &str| {
        !part.is_empty()
            && part.len() <= 100
            && part.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    if !ok(owner) || !ok(name) {
        anyhow::bail!("repository {repo} contains characters that are not valid in owner/name");
    }
    Ok(())
}

/// Fetch a URL with curl.
///
/// curl rather than an HTTP client crate: the agent-side manual `update` path
/// already depends on it, it is present on every platform this runs on, and
/// pulling a second TLS stack into the controller for one request an hour is a
/// poor trade.
async fn curl(url: &str, dest: Option<&std::path::Path>) -> Result<Vec<u8>> {
    let mut command = tokio::process::Command::new("curl");
    command
        .args(["-fsSL", "--proto", "=https", "--tlsv1.2", "--max-time", "600"])
        .arg("-H")
        .arg("Accept: application/vnd.github+json")
        .arg("-H")
        .arg("User-Agent: s3-mcp-relay");
    if let Some(path) = dest {
        command.arg("-o").arg(path);
    }
    let output = command.arg(url).output().await.context("run curl")?;
    if !output.status.success() {
        anyhow::bail!(
            "curl failed for {url}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

pub async fn latest_release(repo: &str) -> Result<LatestRelease> {
    validate_repo(repo)?;
    let url = format!("https://api.github.com/repos/{repo}/releases/latest");
    parse_release(&curl(&url, None).await?)
}

pub async fn download_asset(asset: &Asset, dest: &std::path::Path) -> Result<()> {
    curl(&asset.url, Some(dest)).await?;
    Ok(())
}

/// Where downloaded release assets are staged before being published.
pub fn staging_dir() -> PathBuf {
    std::env::temp_dir().join("s3-relay-releases")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_names_match_the_release_workflow() {
        assert_eq!(asset_name("linux x86_64"), "relay-agent-linux-x86_64");
        assert_eq!(asset_name("linux aarch64"), "relay-agent-linux-aarch64");
        assert_eq!(asset_name("macos aarch64"), "relay-agent-macos-aarch64");
        assert_eq!(asset_name("windows x86_64"), "relay-agent-windows-x86_64.exe");
    }

    #[test]
    fn reads_tag_and_assets_out_of_a_release() {
        let body = br#"{
            "tag_name": "v0.2.0",
            "assets": [
              {"name": "relay-agent-linux-x86_64", "browser_download_url": "https://example.com/a"},
              {"name": "s3-relay-mcp-linux-x86_64", "browser_download_url": "https://example.com/b"}
            ]
        }"#;
        let release = parse_release(body).expect("parses");
        assert_eq!(release.tag, "v0.2.0");
        assert_eq!(release.assets.len(), 2);
        assert_eq!(release.assets[0].name, "relay-agent-linux-x86_64");
        assert_eq!(release.assets[0].url, "https://example.com/a");
    }

    #[test]
    fn a_release_without_assets_is_not_an_error() {
        // A tag pushed before the build finished uploading. Nothing to publish
        // yet, and the next check picks it up — failing here would just log
        // noise every hour.
        let release = parse_release(br#"{"tag_name": "v0.3.0"}"#).expect("parses");
        assert_eq!(release.tag, "v0.3.0");
        assert!(release.assets.is_empty());
    }

    #[test]
    fn rejects_repositories_that_could_escape_the_url() {
        assert!(validate_repo("HiroGitea/s3-mcp-relay").is_ok());
        assert!(validate_repo("owner/name.with.dots").is_ok());
        assert!(validate_repo("owner").is_err());
        assert!(validate_repo("owner/name/extra").is_err());
        assert!(validate_repo("owner/../../etc").is_err());
        assert!(validate_repo("owner/name?query=1").is_err());
        assert!(validate_repo("/name").is_err());
    }

    #[test]
    fn an_agent_without_a_hash_falls_back_to_its_version() {
        let with_hash = crate::registry::SeenAgent {
            id: "a".into(),
            last_seen: 0,
            version: Some("0.1.0".into()),
            binary_sha256: Some("abc".into()),
            platform: None,
        };
        assert!(with_hash.runs("abc", "anything"));
        // The hash is authoritative when present: a matching version must not
        // rescue a mismatched binary.
        assert!(!with_hash.runs("def", "0.1.0"));

        let legacy = crate::registry::SeenAgent {
            binary_sha256: None,
            ..with_hash.clone()
        };
        assert!(legacy.runs("anything", "0.1.0"));
        assert!(!legacy.runs("anything", "0.2.0"));
    }
}
