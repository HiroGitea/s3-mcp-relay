//! Helpers shared by both ends of a bulk file transfer.
//!
//! Transfers exist because command payloads cannot carry bulk data: a command
//! travels through the MCP tool result and therefore through the model context,
//! where a single wheel or tarball would be millions of tokens. A transfer
//! instead moves the bytes through the bucket and lets the command carry only
//! a manifest — size, chunk count, and the hash below.
//!
//! Neither side ever holds the whole file in memory: both hash and copy it one
//! chunk at a time.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::transport::{Transport, BLOB_CHUNK_BYTES};

/// What a staged transfer contains, and what the receiving side needs to
/// rebuild and verify it. Travels inside a sealed command, never in the bucket
/// on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub chunks: u32,
    pub total_bytes: u64,
    /// Lowercase hex SHA-256 of the assembled file.
    pub sha256: String,
}

/// Chunk uploads kept in flight at once.
///
/// Sequential uploads waste most of a high-latency link: one 8 MiB PUT to an
/// endpoint 140 ms away spends far longer waiting than sending, and the window
/// never opens up. Overlapping them is what turns that into usable throughput.
///
/// The ceiling is memory, not the network. Every upload in flight holds its
/// plaintext *and* its ciphertext, so peak use is roughly
/// `concurrency × chunk_size × 2` — at 64 × 8 MiB that is a gigabyte, which is
/// fine on a 251 GB training box and fatal on a 4 GB Jetson. Hence a modest
/// default and an explicit budget below.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 8;
pub const MAX_UPLOAD_CONCURRENCY: usize = 64;
/// Memory the in-flight chunks may occupy. Concurrency is reduced to fit.
const UPLOAD_MEMORY_BUDGET: usize = 256 * 1024 * 1024;

/// How many uploads may overlap for a given chunk size, honouring the budget.
pub fn upload_concurrency(requested: usize, chunk_size: usize) -> usize {
    let requested = requested.clamp(1, MAX_UPLOAD_CONCURRENCY);
    let per_chunk = chunk_size.saturating_mul(2).max(1);
    let affordable = (UPLOAD_MEMORY_BUDGET / per_chunk).max(1);
    requested.min(affordable)
}

/// Read `source` in chunks and upload them with `put`, up to `concurrency` at
/// a time, returning the chunk count, byte count and SHA-256.
///
/// Reading and hashing stay sequential — the hash is order-dependent and a file
/// read is fast next to a network round trip. Only the uploads overlap.
pub async fn upload_chunks<F, Fut>(
    source: &Path,
    chunk_size: usize,
    concurrency: usize,
    put: F,
) -> Result<(u32, u64, String)>
where
    F: Fn(u32, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let concurrency = upload_concurrency(concurrency, chunk_size);
    let mut file = tokio::fs::File::open(source)
        .await
        .with_context(|| format!("open {}", source.display()))?;
    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; chunk_size];
    let mut chunks: u32 = 0;
    let mut total_bytes: u64 = 0;
    let mut inflight = tokio::task::JoinSet::new();

    loop {
        let read = fill(&mut file, &mut buffer).await?;
        if read == 0 && chunks > 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total_bytes = total_bytes.saturating_add(read as u64);
        let index = chunks;
        let data = buffer[..read].to_vec();
        let future = put(index, data);
        inflight.spawn(future);
        chunks = chunks.checked_add(1).context("file has too many chunks")?;

        // Bound the number in flight, and surface a failure as soon as it
        // happens rather than after reading the rest of the file.
        while inflight.len() >= concurrency {
            match inflight.join_next().await {
                Some(joined) => joined.context("upload task panicked")??,
                None => break,
            }
        }
        if read < buffer.len() {
            break;
        }
    }

    while let Some(joined) = inflight.join_next().await {
        joined.context("upload task panicked")??;
    }
    Ok((chunks, total_bytes, hasher.finish_hex()))
}

/// Split a local file into sealed chunks in the bucket.
///
/// Both ends use this: the controller when pushing a file out, the agent when
/// serving a pull. On error the caller is responsible for
/// [`Transport::delete_blob`] — this function does not clean up, because the
/// caller knows whether a retry is coming.
pub async fn stage_file(
    transport: &Transport,
    agent: &str,
    transfer: &str,
    source: &Path,
) -> Result<Manifest> {
    let (owned_transport, owned_agent, owned_transfer) =
        (transport.clone(), agent.to_owned(), transfer.to_owned());
    // An empty file still produces one empty chunk, so a transfer always has at
    // least one object and the receiver needs no special case.
    let (chunks, total_bytes, sha256) = upload_chunks(
        source,
        BLOB_CHUNK_BYTES,
        configured_upload_concurrency(),
        move |index, data| {
            let (transport, agent, transfer) = (
                owned_transport.clone(),
                owned_agent.clone(),
                owned_transfer.clone(),
            );
            async move { transport.put_blob_chunk(&agent, &transfer, index, &data).await }
        },
    )
    .await?;
    Ok(Manifest { chunks, total_bytes, sha256 })
}

/// Upload concurrency from configuration, for both ends of the relay.
pub fn configured_upload_concurrency() -> usize {
    crate::optional_env("RELAY_UPLOAD_CONCURRENCY")
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_UPLOAD_CONCURRENCY)
}

/// Rebuild a staged transfer at `dest` and verify it against `manifest`.
///
/// Chunks land in a sibling temporary file that is renamed into place only
/// after the hash matches, so an interrupted or corrupted transfer never leaves
/// behind something that looks like a complete file. Rename within a directory
/// is atomic on both Unix and Windows.
///
/// Like [`stage_file`], cleanup of the chunks is the caller's decision.
pub async fn assemble_file(
    transport: &Transport,
    agent: &str,
    transfer: &str,
    manifest: &Manifest,
    dest: &Path,
) -> Result<u64> {
    let temp = temp_sibling(dest);
    match assemble_into(transport, agent, transfer, manifest, &temp).await {
        Ok(bytes) => {
            tokio::fs::rename(&temp, dest)
                .await
                .with_context(|| format!("move assembled file into {}", dest.display()))?;
            Ok(bytes)
        }
        Err(error) => {
            let _ = tokio::fs::remove_file(&temp).await;
            Err(error)
        }
    }
}

async fn assemble_into(
    transport: &Transport,
    agent: &str,
    transfer: &str,
    manifest: &Manifest,
    temp: &Path,
) -> Result<u64> {
    let mut file = tokio::fs::File::create(temp)
        .await
        .with_context(|| format!("create {}", temp.display()))?;
    let mut hasher = Hasher::new();
    let mut written: u64 = 0;
    for index in 0..manifest.chunks {
        let data = transport
            .get_blob_chunk(agent, transfer, index)
            .await?
            .ok_or_else(|| anyhow::anyhow!("transfer chunk {index} is missing from the bucket"))?;
        written = written.saturating_add(data.len() as u64);
        if written > manifest.total_bytes {
            bail!("transfer carries more bytes than its manifest declares");
        }
        hasher.update(&data);
        file.write_all(&data).await.context("write transfer chunk")?;
    }
    file.flush().await.context("flush assembled file")?;
    if written != manifest.total_bytes {
        bail!(
            "transfer assembled {written} bytes but the manifest declares {}",
            manifest.total_bytes
        );
    }
    let sha256 = hasher.finish_hex();
    if sha256 != manifest.sha256 {
        bail!("transfer hash mismatch: manifest says {}, assembled {sha256}", manifest.sha256);
    }
    Ok(written)
}

/// Read until `buffer` is full or EOF arrives. A bare `read` may return short
/// at any time, which would otherwise produce undersized chunks in the middle
/// of a file and break the receiver-side length accounting.
async fn fill(file: &mut tokio::fs::File, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = file.read(&mut buffer[filled..]).await.context("read source file")?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn temp_sibling(dest: &Path) -> PathBuf {
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(".relay-partial");
    dest.with_file_name(name)
}

/// Incremental SHA-256 over a file streamed in chunks.
pub struct Hasher(Sha256);

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Lowercase hex, which is what the manifest carries and what every
    /// `sha256sum` style tool prints.
    pub fn finish_hex(self) -> String {
        hex(&self.0.finalize())
    }
}

/// Fresh transfer id. A UUID, because it becomes part of an S3 key and
/// [`validate_transfer_id`](crate::validate_transfer_id) rejects anything else.
pub fn new_transfer_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finish_hex()
}

/// How many chunks a file of `bytes` length splits into. An empty file is one
/// empty chunk, so a transfer always has at least one object to fetch and the
/// receiving side never has to special-case zero.
pub fn chunk_count(bytes: u64, chunk_size: usize) -> u32 {
    let chunk_size = chunk_size.max(1) as u64;
    let count = bytes.div_ceil(chunk_size).max(1);
    count.min(u32::MAX as u64) as u32
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_known_sha256_of_abc() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn streaming_matches_one_shot() {
        let mut hasher = Hasher::new();
        hasher.update(b"hello ");
        hasher.update(b"world");
        assert_eq!(hasher.finish_hex(), sha256_hex(b"hello world"));
    }

    #[test]
    fn concurrency_is_capped_by_memory_not_by_the_request() {
        // Each upload in flight holds plaintext and ciphertext, so the product
        // of concurrency and chunk size is what has to stay bounded. Asking for
        // 64 at 8 MiB would be a gigabyte — fine on a training box, fatal on a
        // 4 GB Jetson, and the caller cannot be expected to know which it is on.
        assert_eq!(upload_concurrency(64, 8 * 1024 * 1024), 16);
        assert_eq!(upload_concurrency(64, 1024 * 1024), 64);
        // A request below the ceiling is honoured as-is.
        assert_eq!(upload_concurrency(4, 8 * 1024 * 1024), 4);
        // Never zero: that would upload nothing at all.
        assert_eq!(upload_concurrency(0, 8 * 1024 * 1024), 1);
        assert_eq!(upload_concurrency(1000, usize::MAX), 1);
    }

    #[tokio::test]
    async fn uploads_every_chunk_exactly_once_and_hashes_in_order() {
        use std::sync::{Arc, Mutex};
        let dir = std::env::temp_dir().join(format!("relay-upload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.bin");
        let payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &payload).unwrap();

        let seen: Arc<Mutex<Vec<(u32, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let (chunks, bytes, sha) = upload_chunks(&path, 1024, 8, move |index, data| {
            let recorder = recorder.clone();
            async move {
                recorder.lock().unwrap().push((index, data.len()));
                Ok(())
            }
        })
        .await
        .unwrap();

        assert_eq!(bytes, payload.len() as u64);
        assert_eq!(chunks, 10, "10000 bytes in 1024-byte chunks");
        // Order of completion is not guaranteed, but coverage is: every index
        // exactly once, and the hash must match a plain sequential digest.
        let mut indices: Vec<u32> = seen.lock().unwrap().iter().map(|(i, _)| *i).collect();
        indices.sort();
        assert_eq!(indices, (0..10).collect::<Vec<_>>());
        assert_eq!(sha, sha256_hex(&payload));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failing_chunk_fails_the_whole_upload() {
        let dir = std::env::temp_dir().join(format!("relay-upfail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.bin");
        std::fs::write(&path, vec![0u8; 5000]).unwrap();
        let result = upload_chunks(&path, 1024, 4, |index, _data| async move {
            if index == 2 { anyhow::bail!("simulated failure") } else { Ok(()) }
        })
        .await;
        assert!(result.is_err(), "one bad chunk must not produce a manifest");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn counts_chunks_including_the_empty_file() {
        assert_eq!(chunk_count(0, 1024), 1);
        assert_eq!(chunk_count(1, 1024), 1);
        assert_eq!(chunk_count(1024, 1024), 1);
        assert_eq!(chunk_count(1025, 1024), 2);
        assert_eq!(chunk_count(4096, 1024), 4);
    }
}
