//! S3-compatible encrypted mailbox transport.
//!
//! Key layout under `S3_PREFIX`:
//! `cmd/<agent>/<id>.json`, `resp/<agent>/<id>.json`, `agents/<agent>.json`,
//! and `doorbell/<agent>.json`. Consumers delete command/response objects
//! immediately after copying them into memory. Heartbeats are overwritten and
//! stale ones are deleted by the controller.
//!
//! Two prefixes are not transit and are deliberately *not* consumed on read:
//! `blob/<agent>/<transfer>/` for bulk file transfers, and
//! `updates/` for fleet self-updates. Both are pure byte copies with no side
//! effects, so a failed fetch can simply be retried.
//!
//! The doorbell exists so an idle agent does not have to LIST its command
//! prefix on every tick: LIST is by far the most expensive request here, while
//! a HEAD against one fixed key is priced like a GET and transfers no body.
//! It is deliberately outside `cmd/` so [`drain_commands`](Transport::drain_commands)
//! never tries to consume it.

use anyhow::{Context, Result};
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::AsyncReadExt;

use crate::protocol::{now_unix, UpdateManifest};
use crate::{
    validate_agent_id, validate_transfer_id, Command, Crypto, Doorbell, Heartbeat, LogChunk, Response,
    S3Config,
};

const MAX_RELAY_OBJECT_BYTES: usize = 4 * 1024 * 1024;
const MAX_LISTED_OBJECTS: usize = 10_000;
/// Upper bound for the controller response-poll backoff. Two seconds keeps the
/// worst-case added latency small while cutting the request count for a long
/// command by roughly three quarters.
const RESPONSE_POLL_CEILING: std::time::Duration = std::time::Duration::from_secs(2);
/// Plaintext bytes per bulk-transfer chunk.
///
/// Chunks are deliberately much larger than `MAX_RELAY_OBJECT_BYTES`, which
/// bounds command objects: a transfer is streamed, so the only cost of a big
/// chunk is peak memory (the plaintext plus its ciphertext, so twice this),
/// while a small one multiplies the request and cleanup count. At 8 MiB a
/// 200 MB wheel is 25 objects and 16 MB of peak memory.
///
/// Chunk size is a sender-side choice, not part of the protocol: the receiver
/// reads whatever each object happens to contain, bounded by
/// `MAX_BLOB_CHUNK_BYTES`. Changing this does not break either end.
pub const BLOB_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Hard ceiling on a chunk the receiver will accept, so a hostile or corrupt
/// object cannot drive an unbounded allocation.
pub const MAX_BLOB_CHUNK_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct Transport {
    client: Client,
    bucket: String,
    prefix: String,
    crypto: Crypto,
}

impl Transport {
    pub async fn connect(cfg: &S3Config, crypto: Crypto) -> Result<Self> {
        cfg.validate()?;
        let creds = Credentials::new(
            &cfg.access_key_id,
            &cfg.secret_access_key,
            None,
            None,
            "s3-mcp-relay-static",
        );
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .endpoint_url(&cfg.endpoint)
            .credentials_provider(creds)
            .force_path_style(cfg.force_path_style)
            .build();
        Ok(Self {
            client: Client::from_conf(conf),
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
            crypto,
        })
    }

    fn cmd_prefix(&self, agent: &str) -> String { format!("{}cmd/{agent}/", self.prefix) }
    fn cmd_key(&self, agent: &str, id: &str) -> String { format!("{}cmd/{agent}/{id}.json", self.prefix) }
    fn resp_key(&self, agent: &str, id: &str) -> String { format!("{}resp/{agent}/{id}.json", self.prefix) }
    fn agents_prefix(&self) -> String { format!("{}agents/", self.prefix) }
    fn agent_key(&self, agent: &str) -> String { format!("{}agents/{agent}.json", self.prefix) }
    fn doorbell_key(&self, agent: &str) -> String { format!("{}doorbell/{agent}.json", self.prefix) }
    fn log_prefix(&self, agent: &str) -> String { format!("{}logs/{agent}/", self.prefix) }
    fn log_key(&self, chunk: &LogChunk) -> String {
        format!("{}{}-{:020}.json", self.log_prefix(&chunk.agent_id), chunk.source, chunk.offset)
    }

    async fn put_encrypted<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let body = self.crypto.seal(key, value)?;
        if body.len() > MAX_RELAY_OBJECT_BYTES {
            anyhow::bail!("encrypted relay object exceeds {} bytes", MAX_RELAY_OBJECT_BYTES);
        }
        self.client.put_object().bucket(&self.bucket).key(key)
            .content_type("application/vnd.s3-mcp-relay.encrypted+json")
            .body(ByteStream::from(body)).send().await
            .with_context(|| format!("put_object {key}"))?;
        Ok(())
    }

    /// Fetch an object with an explicit size ceiling. Command objects and blob
    /// chunks have very different limits, and the cap is enforced twice: once
    /// from the advertised content length, and again while reading, because a
    /// server may understate or omit it.
    async fn get_raw(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        let out = match self.client.get_object().bucket(&self.bucket).key(key).send().await {
            Ok(out) => out,
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() { return Ok(None); }
                return Err(anyhow::Error::new(svc).context(format!("get_object {key}")));
            }
        };
        if out.content_length().is_some_and(|size| size < 0 || size as usize > max_bytes) {
            anyhow::bail!("S3 relay object {key} exceeds {max_bytes} bytes");
        }
        let mut bytes = Vec::new();
        let mut body = out.body.into_async_read().take((max_bytes + 1) as u64);
        body.read_to_end(&mut bytes).await.context("read S3 object body")?;
        if bytes.len() > max_bytes {
            anyhow::bail!("S3 relay object {key} exceeds {max_bytes} bytes");
        }
        Ok(Some(bytes))
    }

    async fn get_encrypted<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.get_raw(key, MAX_RELAY_OBJECT_BYTES).await? {
            Some(bytes) => Ok(Some(self.crypto.open(key, &bytes)?)),
            None => Ok(None),
        }
    }

    /// At-most-once handoff. This favors never replaying side effects over
    /// automatic retry after an agent crash.
    async fn take_encrypted<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let Some(bytes) = self.get_raw(key, MAX_RELAY_OBJECT_BYTES).await? else { return Ok(None); };
        self.delete_key(key).await?;
        Ok(Some(self.crypto.open(key, &bytes)?))
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut request = self.client.list_objects_v2().bucket(&self.bucket).prefix(prefix);
            if let Some(value) = &token { request = request.continuation_token(value); }
            let out = request.send().await.with_context(|| format!("list_objects_v2 {prefix}"))?;
            for object in out.contents() {
                if let Some(key) = object.key() {
                    if keys.len() >= MAX_LISTED_OBJECTS {
                        anyhow::bail!("relay prefix contains more than {MAX_LISTED_OBJECTS} objects");
                    }
                    keys.push(key.to_owned());
                }
            }
            if out.is_truncated().unwrap_or(false) {
                token = out.next_continuation_token().map(ToOwned::to_owned);
            } else { break; }
        }
        Ok(keys)
    }

    pub async fn delete_key(&self, key: &str) -> Result<()> {
        self.client.delete_object().bucket(&self.bucket).key(key).send().await
            .with_context(|| format!("delete_object {key}"))?;
        Ok(())
    }

    pub async fn send_command(&self, cmd: &Command) -> Result<()> {
        validate_agent_id(&cmd.agent_id)?;
        // Order matters: the command object must exist before the doorbell that
        // announces it, so a doorbell the agent observes always corresponds to
        // work it can already see. A failed ring only delays pickup until the
        // next full scan, so it must not fail the send.
        self.put_encrypted(&self.cmd_key(&cmd.agent_id, &cmd.id), cmd).await?;
        if let Err(error) = self.ring_doorbell(&cmd.agent_id).await {
            tracing::warn!(
                agent = %cmd.agent_id, %error,
                "doorbell update failed; the agent will pick this command up on its next full scan"
            );
        }
        Ok(())
    }

    /// Overwrite the doorbell so the next agent-side HEAD sees a new ETag.
    /// Every [`Crypto::seal`] uses a fresh random nonce, so the body — and
    /// therefore the ETag — differs on every ring even when `at` has not moved.
    pub async fn ring_doorbell(&self, agent: &str) -> Result<()> {
        validate_agent_id(agent)?;
        let doorbell = Doorbell { agent_id: agent.to_owned(), at: now_unix() };
        self.put_encrypted(&self.doorbell_key(agent), &doorbell).await
    }

    /// Cheap change check. Returns the current ETag, or `None` when no doorbell
    /// exists yet. The body is never fetched and never decrypted: sealing the
    /// doorbell keeps plaintext out of the bucket, but it cannot authenticate
    /// the *ring* itself, since any write at all changes the ETag. Restricting
    /// who may write `doorbell/` is an IAM job. A spurious ring only costs one
    /// wasted LIST, and a missed one is caught by the periodic full scan.
    pub async fn doorbell_tag(&self, agent: &str) -> Result<Option<String>> {
        validate_agent_id(agent)?;
        let key = self.doorbell_key(agent);
        match self.client.head_object().bucket(&self.bucket).key(&key).send().await {
            Ok(out) => Ok(out.e_tag().map(ToOwned::to_owned)),
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_not_found() { return Ok(None); }
                Err(anyhow::Error::new(svc).context(format!("head_object {key}")))
            }
        }
    }

    pub async fn delete_command(&self, cmd: &Command) -> Result<()> {
        validate_agent_id(&cmd.agent_id)?;
        self.delete_key(&self.cmd_key(&cmd.agent_id, &cmd.id)).await
    }

    /// Poll for the matching response until `timeout`.
    ///
    /// The interval starts at `initial_interval` and doubles up to a ceiling,
    /// so a ping still returns in well under a second while a five-minute exec
    /// does not cost one GET per second for its entire run. The ceiling never
    /// drops below `initial_interval`, so configuring a deliberately slow poll
    /// is still honoured.
    pub async fn await_response(
        &self,
        cmd: &Command,
        timeout: std::time::Duration,
        initial_interval: std::time::Duration,
    ) -> Result<Option<Response>> {
        let key = self.resp_key(&cmd.agent_id, &cmd.id);
        let deadline = tokio::time::Instant::now() + timeout;
        let ceiling = initial_interval.max(RESPONSE_POLL_CEILING);
        let mut interval = initial_interval;
        loop {
            if let Some(response) = self.take_encrypted::<Response>(&key).await? {
                response.validate_for(cmd)?;
                return Ok(Some(response));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline { return Ok(None); }
            tokio::time::sleep(interval.min(deadline - now)).await;
            interval = (interval * 2).min(ceiling);
        }
    }

    pub async fn drain_commands(&self, agent: &str) -> Result<Vec<Command>> {
        validate_agent_id(agent)?;
        let keys = self.list_keys(&self.cmd_prefix(agent)).await?;
        let mut commands = Vec::with_capacity(keys.len());
        for key in keys {
            match self.take_encrypted::<Command>(&key).await {
                Ok(Some(command)) => commands.push(command),
                Ok(None) => {}
                Err(error) => tracing::warn!(%key, %error, "discarded invalid relay command"),
            }
        }
        commands.sort_by_key(|command| command.created_at);
        Ok(commands)
    }

    pub async fn send_response(&self, response: &Response) -> Result<()> {
        validate_agent_id(&response.agent_id)?;
        self.put_encrypted(&self.resp_key(&response.agent_id, &response.id), response).await
    }

    // --- Bulk transfer ------------------------------------------------------
    //
    // Chunks are sealed with Crypto::seal_bytes rather than the JSON envelope,
    // so a chunk object is the payload plus 41 bytes instead of 1.78x its size.
    // Unlike commands, chunks are NOT consumed on read: a transfer is a pure
    // byte copy with no side effects, so a failed chunk can simply be fetched
    // again. Whoever finishes the transfer deletes the whole sub-prefix.

    fn blob_prefix(&self, agent: &str, transfer: &str) -> String {
        format!("{}blob/{agent}/{transfer}/", self.prefix)
    }

    fn blob_chunk_key(&self, agent: &str, transfer: &str, index: u32) -> String {
        // Zero padded so lexicographic listing order matches chunk order.
        format!("{}{index:08}", self.blob_prefix(agent, transfer))
    }

    pub async fn put_blob_chunk(
        &self,
        agent: &str,
        transfer: &str,
        index: u32,
        data: &[u8],
    ) -> Result<()> {
        validate_agent_id(agent)?;
        validate_transfer_id(transfer)?;
        if data.len() > MAX_BLOB_CHUNK_BYTES {
            anyhow::bail!("blob chunk exceeds {MAX_BLOB_CHUNK_BYTES} bytes");
        }
        let key = self.blob_chunk_key(agent, transfer, index);
        let body = self.crypto.seal_bytes(&key, data)?;
        self.client.put_object().bucket(&self.bucket).key(&key)
            .content_type("application/vnd.s3-mcp-relay.chunk")
            .body(ByteStream::from(body)).send().await
            .with_context(|| format!("put_object {key}"))?;
        Ok(())
    }

    pub async fn get_blob_chunk(
        &self,
        agent: &str,
        transfer: &str,
        index: u32,
    ) -> Result<Option<Vec<u8>>> {
        validate_agent_id(agent)?;
        validate_transfer_id(transfer)?;
        let key = self.blob_chunk_key(agent, transfer, index);
        match self.get_raw(&key, MAX_BLOB_CHUNK_BYTES).await? {
            Some(bytes) => Ok(Some(self.crypto.open_bytes(&key, &bytes)?)),
            None => Ok(None),
        }
    }

    /// Remove every chunk of a transfer. Safe to call twice, and safe to call
    /// on a transfer that was never completed.
    pub async fn delete_blob(&self, agent: &str, transfer: &str) -> Result<()> {
        validate_agent_id(agent)?;
        validate_transfer_id(transfer)?;
        for key in self.list_keys(&self.blob_prefix(agent, transfer)).await? {
            if let Err(error) = self.delete_key(&key).await {
                tracing::warn!(%key, %error, "could not delete blob chunk");
            }
        }
        Ok(())
    }

    // --- Fleet updates ------------------------------------------------------
    //
    // Two object classes with two different keys, which is the whole point:
    //
    //   updates/manifest/<agent>.json   sealed with that agent's key
    //   updates/blob/<release>/<index>  sealed with the release key
    //
    // Only the manifest is per-agent, and it is a few hundred bytes. The
    // binary is written once for the entire fleet, so publishing to twenty
    // machines costs one upload rather than twenty. The release key that
    // unlocks it is carried inside each sealed manifest, so the bucket still
    // never holds anything an S3 operator could read.

    fn update_manifest_key(&self, agent: &str) -> String {
        format!("{}updates/manifest/{agent}.json", self.prefix)
    }

    fn release_prefix(&self, release: &str) -> String {
        format!("{}updates/blob/{release}/", self.prefix)
    }

    fn release_chunk_key(&self, release: &str, index: u32) -> String {
        // Zero padded, like blob chunks, so listing order matches chunk order.
        format!("{}{index:08}", self.release_prefix(release))
    }

    pub async fn put_update_manifest(&self, manifest: &UpdateManifest) -> Result<()> {
        validate_agent_id(&manifest.agent_id)?;
        validate_transfer_id(&manifest.release)?;
        self.put_encrypted(&self.update_manifest_key(&manifest.agent_id), manifest).await
    }

    pub async fn read_update_manifest(&self, agent: &str) -> Result<Option<UpdateManifest>> {
        validate_agent_id(agent)?;
        let key = self.update_manifest_key(agent);
        let Some(manifest) = self.get_encrypted::<UpdateManifest>(&key).await? else {
            return Ok(None);
        };
        // The manifest is AEAD-bound to this key, so it cannot have been copied
        // from another agent's slot; this catches a controller that published a
        // mismatched body into its own agent's slot.
        if manifest.agent_id != agent {
            anyhow::bail!("update manifest at {key} names a different agent");
        }
        validate_transfer_id(&manifest.release)?;
        Ok(Some(manifest))
    }

    /// Cheap change check, the same trick as [`doorbell_tag`](Self::doorbell_tag):
    /// an agent that already installed the current release should not pay for a
    /// GET every few minutes just to learn nothing changed.
    pub async fn update_manifest_tag(&self, agent: &str) -> Result<Option<String>> {
        validate_agent_id(agent)?;
        let key = self.update_manifest_key(agent);
        match self.client.head_object().bucket(&self.bucket).key(&key).send().await {
            Ok(out) => Ok(out.e_tag().map(ToOwned::to_owned)),
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_not_found() { return Ok(None); }
                Err(anyhow::Error::new(svc).context(format!("head_object {key}")))
            }
        }
    }

    pub async fn delete_update_manifest(&self, agent: &str) -> Result<()> {
        validate_agent_id(agent)?;
        self.delete_key(&self.update_manifest_key(agent)).await
    }

    /// Release chunks take an explicit [`Crypto`] because they are sealed with
    /// the one-off release key rather than this transport's per-agent key.
    pub async fn put_release_chunk(
        &self,
        release: &str,
        index: u32,
        data: &[u8],
        crypto: &Crypto,
    ) -> Result<()> {
        validate_transfer_id(release)?;
        if data.len() > MAX_BLOB_CHUNK_BYTES {
            anyhow::bail!("release chunk exceeds {MAX_BLOB_CHUNK_BYTES} bytes");
        }
        let key = self.release_chunk_key(release, index);
        let body = crypto.seal_bytes(&key, data)?;
        self.client.put_object().bucket(&self.bucket).key(&key)
            .content_type("application/vnd.s3-mcp-relay.chunk")
            .body(ByteStream::from(body)).send().await
            .with_context(|| format!("put_object {key}"))?;
        Ok(())
    }

    pub async fn get_release_chunk(
        &self,
        release: &str,
        index: u32,
        crypto: &Crypto,
    ) -> Result<Option<Vec<u8>>> {
        validate_transfer_id(release)?;
        let key = self.release_chunk_key(release, index);
        match self.get_raw(&key, MAX_BLOB_CHUNK_BYTES).await? {
            Some(bytes) => Ok(Some(crypto.open_bytes(&key, &bytes)?)),
            None => Ok(None),
        }
    }

    /// Remove every chunk of a release. Controller-side only: agents are not
    /// granted list or delete on this prefix.
    pub async fn delete_release(&self, release: &str) -> Result<()> {
        validate_transfer_id(release)?;
        for key in self.list_keys(&self.release_prefix(release)).await? {
            if let Err(error) = self.delete_key(&key).await {
                tracing::warn!(%key, %error, "could not delete release chunk");
            }
        }
        Ok(())
    }

    pub async fn write_heartbeat(&self, heartbeat: &Heartbeat) -> Result<()> {
        validate_agent_id(&heartbeat.agent_id)?;
        self.put_encrypted(&self.agent_key(&heartbeat.agent_id), heartbeat).await
    }

    pub async fn delete_heartbeat(&self, agent: &str) -> Result<()> {
        validate_agent_id(agent)?;
        self.delete_key(&self.agent_key(agent)).await
    }

    pub async fn read_heartbeat(&self, agent: &str) -> Result<Option<Heartbeat>> {
        validate_agent_id(agent)?;
        let heartbeat = self.get_encrypted::<Heartbeat>(&self.agent_key(agent)).await?;
        Ok(heartbeat.filter(|value| !value.is_stale()))
    }

    pub async fn publish_log_chunk(&self, chunk: &LogChunk) -> Result<()> {
        validate_agent_id(&chunk.agent_id)?;
        if chunk.source.is_empty() || chunk.source.len() > 96
            || !chunk.source.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')) {
            anyhow::bail!("unsafe log source name");
        }
        self.put_encrypted(&self.log_key(chunk), chunk).await
    }

    pub async fn pending_log_chunks(&self, agent: &str) -> Result<Vec<(String, LogChunk)>> {
        validate_agent_id(agent)?;
        let mut keys = self.list_keys(&self.log_prefix(agent)).await?;
        keys.sort();
        let mut chunks = Vec::new();
        for key in keys {
            if let Some(chunk) = self.get_encrypted::<LogChunk>(&key).await? { chunks.push((key, chunk)); }
        }
        Ok(chunks)
    }

    pub async fn acknowledge_log_chunk(&self, key: &str) -> Result<()> {
        if !key.starts_with(&format!("{}logs/", self.prefix)) { anyhow::bail!("invalid log object key"); }
        self.delete_key(key).await
    }

    pub async fn list_heartbeats(&self) -> Result<Vec<Heartbeat>> {
        let prefix = self.agents_prefix();
        let keys = self.list_keys(&prefix).await?;
        let mut live = Vec::new();
        for key in keys {
            match self.get_encrypted::<Heartbeat>(&key).await {
                Ok(Some(heartbeat)) if validate_agent_id(&heartbeat.agent_id).is_err()
                    || self.agent_key(&heartbeat.agent_id) != key
                    || heartbeat.ttl_secs <= 0
                    || heartbeat.ttl_secs > 3_600 => {
                    tracing::warn!(%key, "discarding heartbeat with invalid routing or TTL");
                    let _ = self.delete_key(&key).await;
                }
                Ok(Some(heartbeat)) if heartbeat.is_stale() => {
                    if let Err(error) = self.delete_key(&key).await {
                        tracing::warn!(%key, %error, "failed to delete stale heartbeat");
                    }
                }
                Ok(Some(heartbeat)) => live.push(heartbeat),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%key, %error, "discarding invalid heartbeat");
                    let _ = self.delete_key(&key).await;
                }
            }
        }
        live.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        Ok(live)
    }
}
