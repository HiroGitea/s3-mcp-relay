//! End-to-end authenticated encryption for every relay object.
//!
//! S3 server-side encryption protects disks but still exposes plaintext to the
//! S3 service. XChaCha20-Poly1305 protects the command/response content all the
//! way between controller and agent. The object key is authenticated as AAD,
//! so a valid object cannot be copied to a different mailbox path.

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const ENVELOPE_VERSION: u32 = 1;
const AAD_DOMAIN: &[u8] = b"s3-mcp-relay/v1\0";

/// Key id stamped on every object whose key came from X25519 enrolment.
///
/// Both ends must choose the same string or nothing decrypts, so neither is
/// allowed to spell it out: the controller derives a key per agent and seals
/// with this, and an enrolled agent expects exactly this. It was previously
/// written as a literal on the controller side while the agent fell back to
/// `"primary"` — the shared-key default — so every command a paired agent
/// received was discarded as a key id mismatch, and `relay-agent init` had no
/// way to know it needed to say otherwise.
pub const PAIRED_KEY_ID: &str = "x25519-v1";

/// Key id for the older deployment style, where one key is configured on both
/// sides by hand as `RELAY_SHARED_KEY`.
pub const SHARED_KEY_ID: &str = "primary";

#[derive(Clone)]
pub struct Crypto {
    cipher: XChaCha20Poly1305,
    key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    version: u32,
    key_id: String,
    nonce_b64: String,
    ciphertext_b64: String,
}

impl Crypto {
    /// Build from configuration: a hand-configured `RELAY_SHARED_KEY`, or the
    /// key derived from X25519 enrolment.
    ///
    /// The default key id follows the same branch as the key itself. It has to:
    /// a derived key is always sealed by the controller as [`PAIRED_KEY_ID`],
    /// so defaulting an enrolled agent to the shared-key id instead left the
    /// two ends unable to read each other's objects at all.
    pub fn from_env() -> Result<Self> {
        let (encoded, default_key_id) = match crate::optional_env("RELAY_SHARED_KEY") {
            Some(value) => (value, SHARED_KEY_ID),
            None => {
                let private = crate::required_env("AGENT_PRIVATE_KEY")?;
                let controller = crate::required_env("AGENT_CONTROLLER_PUBLIC_KEY")?;
                let agent = crate::required_env("RELAY_AGENT_ID")?;
                (
                    crate::pairing::derive_key(&private, &controller, &agent)?,
                    PAIRED_KEY_ID,
                )
            }
        };
        let key_id =
            crate::optional_env("RELAY_KEY_ID").unwrap_or_else(|| default_key_id.to_owned());
        Self::from_base64(&encoded, key_id)
    }

    pub fn from_base64(encoded: &str, key_id: impl Into<String>) -> Result<Self> {
        let key = B64
            .decode(encoded.trim())
            .context("RELAY_SHARED_KEY is not valid base64")?;
        if key.len() != 32 {
            bail!("RELAY_SHARED_KEY must decode to exactly 32 bytes");
        }
        let key_id = key_id.into();
        if key_id.is_empty() || key_id.len() > 64 {
            bail!("RELAY_KEY_ID must contain 1..=64 characters");
        }
        Ok(Self {
            cipher: XChaCha20Poly1305::new_from_slice(&key).expect("validated key length"),
            key_id,
        })
    }

    pub fn seal<T: Serialize>(&self, object_key: &str, value: &T) -> Result<Vec<u8>> {
        let plaintext = serde_json::to_vec(value).context("serialize relay message")?;
        let mut nonce = [0u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let aad = aad(object_key);
        let ciphertext = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypt relay message"))?;
        let envelope = Envelope {
            version: ENVELOPE_VERSION,
            key_id: self.key_id.clone(),
            nonce_b64: B64.encode(nonce),
            ciphertext_b64: B64.encode(ciphertext),
        };
        serde_json::to_vec(&envelope).context("serialize encrypted envelope")
    }

    /// Seal opaque bytes without the JSON envelope.
    ///
    /// Layout: `version(1) | nonce(24) | ciphertext`.
    ///
    /// Bulk transfer chunks use this instead of [`seal`](Self::seal) because
    /// the envelope costs 78% expansion — base64 inside JSON inside base64 —
    /// which would nearly double the bytes moved for a large file. A chunk has
    /// no fields worth inspecting, so nothing is lost.
    ///
    /// There is no `key_id` here, unlike the JSON envelope: a chunk is only
    /// ever fetched because a sealed command referenced it, so a key mismatch
    /// has already been reported by then with a clearer error.
    pub fn seal_bytes(&self, object_key: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let aad = aad(object_key);
        let ciphertext = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload { msg: plaintext, aad: &aad },
            )
            .map_err(|_| anyhow::anyhow!("encrypt relay chunk"))?;
        let mut out = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
        out.push(ENVELOPE_VERSION as u8);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Inverse of [`seal_bytes`](Self::seal_bytes).
    pub fn open_bytes(&self, object_key: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        // A valid chunk is version + nonce + at least the Poly1305 tag.
        if bytes.len() < 1 + 24 + 16 {
            bail!("relay chunk is too short to be valid");
        }
        if bytes[0] != ENVELOPE_VERSION as u8 {
            bail!("unsupported relay chunk version {}", bytes[0]);
        }
        let aad = aad(object_key);
        self.cipher
            .decrypt(
                XNonce::from_slice(&bytes[1..25]),
                Payload { msg: &bytes[25..], aad: &aad },
            )
            .map_err(|_| anyhow::anyhow!("chunk authentication failed"))
    }

    pub fn open<T: DeserializeOwned>(&self, object_key: &str, bytes: &[u8]) -> Result<T> {
        let envelope: Envelope =
            serde_json::from_slice(bytes).context("decode encrypted envelope")?;
        if envelope.version != ENVELOPE_VERSION {
            bail!("unsupported encrypted envelope version {}", envelope.version);
        }
        if envelope.key_id != self.key_id {
            bail!("relay key id mismatch: received {}", envelope.key_id);
        }
        let nonce = B64.decode(&envelope.nonce_b64).context("decode nonce")?;
        if nonce.len() != 24 {
            bail!("invalid XChaCha20 nonce length");
        }
        let ciphertext = B64
            .decode(&envelope.ciphertext_b64)
            .context("decode ciphertext")?;
        let aad = aad(object_key);
        let plaintext = self
            .cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("message authentication failed"))?;
        serde_json::from_slice(&plaintext).context("decode authenticated relay message")
    }
}

fn aad(object_key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(AAD_DOMAIN.len() + object_key.len());
    out.extend_from_slice(AAD_DOMAIN);
    out.extend_from_slice(object_key.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        value: String,
    }

    #[test]
    fn round_trip_and_path_binding() {
        let key = B64.encode([7u8; 32]);
        let crypto = Crypto::from_base64(&key, "test").unwrap();
        let sealed = crypto
            .seal("relay/cmd/a/1.json", &Sample { value: "ok".into() })
            .unwrap();
        let opened: Sample = crypto.open("relay/cmd/a/1.json", &sealed).unwrap();
        assert_eq!(opened, Sample { value: "ok".into() });
        assert!(crypto
            .open::<Sample>("relay/cmd/b/1.json", &sealed)
            .is_err());
    }

    #[test]
    fn chunk_round_trip_is_path_bound_and_compact() {
        let key = B64.encode([9u8; 32]);
        let crypto = Crypto::from_base64(&key, "test").unwrap();
        let payload = vec![0xABu8; 4096];
        let sealed = crypto.seal_bytes("relay/blob/a/t/00000001", &payload).unwrap();

        // version + nonce + tag, and nothing else: no base64, no JSON.
        assert_eq!(sealed.len(), payload.len() + 1 + 24 + 16);

        let opened = crypto.open_bytes("relay/blob/a/t/00000001", &sealed).unwrap();
        assert_eq!(opened, payload);
        // A chunk moved to another key must not authenticate.
        assert!(crypto.open_bytes("relay/blob/a/t/00000002", &sealed).is_err());
    }

    #[test]
    fn the_two_key_ids_are_distinct_and_stable() {
        // Both sides of an enrolled pair default to PAIRED_KEY_ID, and the two
        // deployment styles must stay distinguishable. Changing either string
        // breaks every deployment already in the field.
        assert_eq!(PAIRED_KEY_ID, "x25519-v1");
        assert_eq!(SHARED_KEY_ID, "primary");
        assert_ne!(PAIRED_KEY_ID, SHARED_KEY_ID);
    }

    #[test]
    fn a_key_id_mismatch_is_refused_rather_than_misread() {
        let key = B64.encode([5u8; 32]);
        let controller = Crypto::from_base64(&key, PAIRED_KEY_ID).unwrap();
        let agent_wrong = Crypto::from_base64(&key, SHARED_KEY_ID).unwrap();
        let sealed = controller
            .seal("relay/cmd/a/1.json", &Sample { value: "ok".into() })
            .unwrap();

        // The regression: same key, different id, and every command discarded.
        let error = agent_wrong
            .open::<Sample>("relay/cmd/a/1.json", &sealed)
            .expect_err("a mismatched key id must not decrypt");
        assert!(error.to_string().contains("key id mismatch"));

        let agent_right = Crypto::from_base64(&key, PAIRED_KEY_ID).unwrap();
        assert_eq!(
            agent_right.open::<Sample>("relay/cmd/a/1.json", &sealed).unwrap(),
            Sample { value: "ok".into() }
        );
    }

    #[test]
    fn rejects_truncated_chunks() {
        let key = B64.encode([3u8; 32]);
        let crypto = Crypto::from_base64(&key, "test").unwrap();
        assert!(crypto.open_bytes("relay/blob/a/t/00000001", &[1u8; 8]).is_err());
        assert!(crypto.open_bytes("relay/blob/a/t/00000001", &[]).is_err());
    }
}
