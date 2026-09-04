//! Kalshi request signing: RSA-PSS over SHA-256.
//!
//! Verified against <https://docs.kalshi.com/getting_started/api_keys> and
//! `quick_start_websockets`, read 2026-08-28.
//!
//! # The signed message
//!
//! ```text
//! {timestamp_ms}{METHOD}{path}
//! ```
//!
//! concatenated with no separators. `timestamp_ms` is milliseconds since the
//! Unix epoch as a decimal string, `METHOD` is the uppercase HTTP verb, and
//! `path` is the request path **with any query string removed**. The signature
//! is base64-encoded into `KALSHI-ACCESS-SIGNATURE`.
//!
//! # Two things here are easy to get wrong and silent when wrong
//!
//! 1. **Salt length.** Kalshi specifies `PSS.DIGEST_LENGTH` — a salt equal to
//!    the digest size, 32 bytes for SHA-256. The RustCrypto `rsa` crate offers
//!    both; [`Pss::new::<Sha256>()`] is the digest-length variant and
//!    `new_with_salt` with a larger length produces signatures the exchange
//!    rejects with no useful error.
//! 2. **The query string.** `/trade-api/v2/markets?limit=100` is signed as
//!    `/trade-api/v2/markets`. Signing the query is the classic 401 nobody can
//!    find.
//!
//! The private key never leaves this process: it is read from a file path, held
//! in memory, and never logged, serialized, or included in any error message.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
use rsa::pss::{BlindedSigningKey, VerifyingKey};
use rsa::signature::{Keypair, RandomizedSigner, SignatureEncoding, Verifier};
use rsa::{RsaPrivateKey, RsaPublicKey};
use sha2::Sha256;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// The exact WebSocket path Kalshi expects to be signed for the upgrade
/// handshake.
///
/// Not the REST base path, and never with a query string. See
/// [`sign_websocket_upgrade`].
pub const WS_SIGNING_PATH: &str = "/trade-api/ws/v2";

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("reading private key from {path}")]
    ReadKey {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Deliberately carries no key material, not even a snippet.
    #[error("private key at {path} is not a valid PKCS#8 RSA key")]
    DecodeKey {
        path: String,
        #[source]
        source: rsa::pkcs8::Error,
    },

    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,

    #[error("signing failed")]
    Sign(#[source] rsa::signature::Error),

    #[error("{origin} is not a valid PEM public key")]
    DecodePublicKey {
        origin: String,
        #[source]
        source: rsa::pkcs8::spki::Error,
    },
}

/// Milliseconds since the Unix epoch, as the decimal string Kalshi expects in
/// `KALSHI-ACCESS-TIMESTAMP`.
pub fn timestamp_ms_now() -> Result<String, AuthError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthError::ClockBeforeEpoch)?
        .as_millis();
    Ok(millis.to_string())
}

/// Strip any query string from a path, since Kalshi signs the path alone.
///
/// `"/trade-api/v2/markets?limit=100&cursor=abc"` → `"/trade-api/v2/markets"`.
#[must_use]
pub fn path_without_query(path: &str) -> &str {
    match path.split_once('?') {
        Some((before, _)) => before,
        None => path,
    }
}

/// Build the exact byte string Kalshi signs: `{timestamp}{METHOD}{path}`.
///
/// Exposed separately from signing so it can be asserted directly in tests —
/// the concatenation is the part that regresses, not the RSA.
#[must_use]
pub fn signing_message(timestamp_ms: &str, method: &str, path: &str) -> String {
    format!("{timestamp_ms}{method}{}", path_without_query(path))
}

/// A loaded API key: the key ID plus the private key used to sign with it.
///
/// `Debug` is implemented by hand to print only the key ID. Deriving it would
/// put key material into any log line that formats a struct containing this.
#[derive(Clone)]
pub struct Credentials {
    key_id: String,
    signing_key: BlindedSigningKey<Sha256>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("key_id", &self.key_id)
            .field("private_key", &"<redacted>")
            .finish()
    }
}

/// The three headers Kalshi requires on every authenticated request, REST or
/// WebSocket upgrade.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedHeaders {
    pub access_key: String,
    pub signature: String,
    pub timestamp: String,
}

impl SignedHeaders {
    pub const KEY_HEADER: &'static str = "KALSHI-ACCESS-KEY";
    pub const SIGNATURE_HEADER: &'static str = "KALSHI-ACCESS-SIGNATURE";
    pub const TIMESTAMP_HEADER: &'static str = "KALSHI-ACCESS-TIMESTAMP";

    /// `(name, value)` pairs ready to attach to a request.
    #[must_use]
    pub fn as_pairs(&self) -> [(&'static str, &str); 3] {
        [
            (Self::KEY_HEADER, self.access_key.as_str()),
            (Self::SIGNATURE_HEADER, self.signature.as_str()),
            (Self::TIMESTAMP_HEADER, self.timestamp.as_str()),
        ]
    }
}

impl Credentials {
    /// Load a PKCS#8 PEM private key from disk.
    ///
    /// The key is read once at startup and stays in this process.
    pub fn from_pem_file(key_id: impl Into<String>, path: &Path) -> Result<Credentials, AuthError> {
        let pem = std::fs::read_to_string(path).map_err(|source| AuthError::ReadKey {
            path: path.display().to_string(),
            source,
        })?;
        Credentials::from_pem(key_id, &pem, &path.display().to_string())
    }

    /// Load from PEM text already in memory. `origin` is used only to describe
    /// the key in error messages; it never contains key material.
    pub fn from_pem(
        key_id: impl Into<String>,
        pem: &str,
        origin: &str,
    ) -> Result<Credentials, AuthError> {
        let private_key =
            RsaPrivateKey::from_pkcs8_pem(pem).map_err(|source| AuthError::DecodeKey {
                path: origin.to_owned(),
                source,
            })?;
        Ok(Credentials {
            key_id: key_id.into(),
            // BlindedSigningKey applies RSA blinding, which defends the private
            // key against timing side channels. Its salt length is the digest
            // length (32 bytes for SHA-256), which is what Kalshi specifies as
            // PSS.DIGEST_LENGTH. Do not substitute a max-salt-length variant.
            signing_key: BlindedSigningKey::<Sha256>::new(private_key),
        })
    }

    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Sign `{timestamp}{METHOD}{path}` and return the three required headers.
    ///
    /// Any query string in `path` is stripped before signing.
    pub fn sign(
        &self,
        method: &str,
        path: &str,
        timestamp_ms: &str,
    ) -> Result<SignedHeaders, AuthError> {
        let message = signing_message(timestamp_ms, method, path);
        let mut rng = rand_core::OsRng;
        let signature = self
            .signing_key
            .try_sign_with_rng(&mut rng, message.as_bytes())
            .map_err(AuthError::Sign)?;
        Ok(SignedHeaders {
            access_key: self.key_id.clone(),
            signature: BASE64.encode(signature.to_bytes()),
            timestamp: timestamp_ms.to_owned(),
        })
    }

    /// Sign a REST request, stamping the current time.
    pub fn sign_rest(&self, method: &str, path: &str) -> Result<SignedHeaders, AuthError> {
        self.sign(method, path, &timestamp_ms_now()?)
    }

    /// Sign the WebSocket upgrade handshake.
    ///
    /// # This is the single most common Kalshi auth failure
    ///
    /// The upgrade is signed as **`GET`** over the literal path
    /// [`WS_SIGNING_PATH`] — `/trade-api/ws/v2` — and nothing else. Not the
    /// full `wss://` URL, not the REST base path, not the path plus a query
    /// string. Every public market-data channel still requires this: the
    /// connection is authenticated even though the data is public.
    ///
    /// It exists as its own named function precisely so no caller has to
    /// remember the rule.
    pub fn sign_websocket_upgrade(&self) -> Result<SignedHeaders, AuthError> {
        self.sign("GET", WS_SIGNING_PATH, &timestamp_ms_now()?)
    }

    /// Verify a signature this key produced.
    ///
    /// Present for tests: RSA-PSS is randomized, so two signatures over the
    /// same message differ and cannot be compared against a fixed vector. The
    /// meaningful assertion is that a signature verifies against the public key
    /// and that a signature over a *different* message does not.
    #[must_use]
    pub fn verify(&self, message: &str, signature_b64: &str) -> bool {
        let Ok(bytes) = BASE64.decode(signature_b64) else {
            return false;
        };
        let Ok(signature) = rsa::pss::Signature::try_from(bytes.as_slice()) else {
            return false;
        };
        self.signing_key
            .verifying_key()
            .verify(message.as_bytes(), &signature)
            .is_ok()
    }
}

/// Verify an RSA-PSS/SHA-256 signature against a PEM public key.
///
/// Used by the cross-implementation known-answer test to prove this crate's PSS
/// parameters — in particular the digest-length salt — match what Kalshi's
/// documented signing procedure produces. Not used by the daemon at runtime.
pub fn verify_with_public_key_pem(
    public_key_pem: &str,
    message: &str,
    signature_b64: &str,
    origin: &str,
) -> Result<bool, AuthError> {
    let public_key = RsaPublicKey::from_public_key_pem(public_key_pem).map_err(|source| {
        AuthError::DecodePublicKey {
            origin: origin.to_owned(),
            source,
        }
    })?;
    let Ok(bytes) = BASE64.decode(signature_b64) else {
        return Ok(false);
    };
    let Ok(signature) = rsa::pss::Signature::try_from(bytes.as_slice()) else {
        return Ok(false);
    };
    let verifying = VerifyingKey::<Sha256>::new(public_key);
    Ok(verifying.verify(message.as_bytes(), &signature).is_ok())
}
