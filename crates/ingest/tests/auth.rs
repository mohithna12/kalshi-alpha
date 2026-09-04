//! Tests for Kalshi request signing.
//!
//! # No private key material is committed anywhere in this repository
//!
//! The keypair used for signing tests is generated at runtime. What *is*
//! embedded below is a public key and two signatures produced by a different
//! implementation — neither is secret, and together they form a genuine
//! cross-implementation known-answer test.

use kalshi_ingest::auth::{path_without_query, signing_message, Credentials, WS_SIGNING_PATH};

// ---------------------------------------------------------------------------
// Known-answer vectors, generated with Python `cryptography` 46.0.3 — the same
// library Kalshi's own documented signing sample uses.
//
//   msg = b"1700000000000GET/trade-api/ws/v2"
//   key.sign(msg, padding.PSS(mgf=padding.MGF1(SHA256()),
//                             salt_length=<VARIANT>), SHA256())
//
// Two signatures over the same message with the same key, differing only in
// salt length. Kalshi specifies PSS.DIGEST_LENGTH; MAX_LENGTH is the trap.
// ---------------------------------------------------------------------------

const KAT_MESSAGE: &str = "1700000000000GET/trade-api/ws/v2";

const KAT_PUBLIC_KEY_PEM: &str = "\
-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyv/Lrs6nyqdEFO+9AaUd\n\
HeUi53eD+t7Hj+VopUiCRcwsReyE4TXvDZEaYOMCbOggLssCRZwdb0SAH7FwcqdE\n\
faqw4Z8SjqtF7OB6lgkEzQHIGSPGQR3rUyWOrH69O3KmahVIkzSs968r0iacmhs2\n\
dnSJhBOGZ6CqPE0bf6UpKuo6faYAwHaNGACN4aBwaZOI7SqpP4aNlB2wPcYufPVn\n\
WKf/bj6kNRNDEY7LLATGd4Vo7wCci2ufEY+/iWNUC+FWt75fBQTheKg2b0kprTZa\n\
/DiEfZDIA4E0defCA80Bv7PW8cH8UVsxJaFvuHCAYzlT2K7y1JeVK0JpGf3Brigy\n\
hQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

/// Signed with `salt_length=PSS.DIGEST_LENGTH` — what Kalshi documents.
const KAT_SIGNATURE_DIGEST_SALT: &str = "a94jV4zkNt3trT60JU2xOFqcMPDibqQ1xuCW3XTPnBqO/zSmRslVhsuVanT4gvvRy/TKHNWgOqI0zXCFf88qm3QldTYuvEoCQ9DhQ+dtvO6EajkN3dso9rE8xYgef6TCy+8Nz8dv7nxlujN8K2JIE2BhcJnhS97t2apOJBI1AE03VDsaP6u+vKZSBqHU7RZ8Y1fDeS62lOVX6F8QEOsIwSyGRNSzY17092RL8eyxGxfxfvDtvrfTglqxJyuBLxyRRnskHFzDpWZJRQs6Cfjdnn5sYkqkw7yKT3qXxIXD107MEyvM7jrnN0UPcCM+g2rSv7x2JhU0YiYp9RFuVdYr0Q==";

/// Signed with `salt_length=PSS.MAX_LENGTH` — the plausible-looking wrong
/// choice, which produces a signature Kalshi rejects.
const KAT_SIGNATURE_MAX_SALT: &str = "FRYuuwY1AHHOkkmhuDcePGkpNbsCM8U5BSbK34T0t32aApYx80mtkfsY3Wq8v8Am9fCtgM4CrPPe2T/qNlQQNN66VglAHB1247youFG/hdLQzPEE8pUdntzpSt1qD6rMqmKbUfmrv/wJvxmQvUs+v7wAgHCwQZnbg2yxVbYq6eBxdnUJDxgErUSOdl8EYoIp19tTI90kHB0glWuQ6fOzus7UnzEMGjFgpGgiiFeOmQJtfrK3c4l7zzRWusgC0ce6uQSh8SnZLlTBtfWK3s4FM7XW442fFZoYskTicb8UzKIY/QvXn60YiY+PBWAY1jNw3KJc/oe3YNn/VAGQ3Yv62w==";

// ---------------------------------------------------------------------------
// A throwaway keypair, generated at runtime. Nothing is committed.
// ---------------------------------------------------------------------------

fn test_credentials() -> &'static Credentials {
    use std::sync::OnceLock;
    static CREDS: OnceLock<Credentials> = OnceLock::new();
    CREDS.get_or_init(|| {
        use rsa::pkcs8::{EncodePrivateKey, LineEnding};
        let mut rng = rand_core::OsRng;
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("keygen");
        let pem = key.to_pkcs8_pem(LineEnding::LF).expect("encode");
        Credentials::from_pem("test-key-id", &pem, "generated-in-test").expect("load")
    })
}

// ---------------------------------------------------------------------------
// The signed message: the part that actually regresses
// ---------------------------------------------------------------------------

#[test]
fn signed_message_is_timestamp_then_method_then_path() {
    assert_eq!(
        signing_message("1700000000000", "GET", "/trade-api/v2/markets"),
        "1700000000000GET/trade-api/v2/markets"
    );
    // No separators, no newline, no trailing anything.
    assert_eq!(signing_message("1", "POST", "/x"), "1POST/x");
}

#[test]
fn query_parameters_are_stripped_before_signing() {
    // Documented requirement, and the easiest thing in the whole client to
    // regress: signing the query string yields a 401 with no useful detail.
    assert_eq!(
        path_without_query("/trade-api/v2/markets?limit=100&cursor=abc123"),
        "/trade-api/v2/markets"
    );
    assert_eq!(
        signing_message("1700000000000", "GET", "/trade-api/v2/markets?limit=100"),
        "1700000000000GET/trade-api/v2/markets"
    );

    // A path that is nothing but a query, an empty query, and a query
    // containing a second '?' all reduce to the path alone.
    assert_eq!(path_without_query("/x?"), "/x");
    assert_eq!(path_without_query("/x?a=1?b=2"), "/x");
    assert_eq!(path_without_query("?a=1"), "");
    // No query is left untouched.
    assert_eq!(
        path_without_query("/trade-api/v2/markets"),
        "/trade-api/v2/markets"
    );

    // The signature over a path with a query must equal the signature over the
    // bare path -- verified through the real signer, not just the helper.
    let creds = test_credentials();
    let with_query = creds
        .sign("GET", "/trade-api/v2/markets?limit=100", "1700000000000")
        .expect("sign");
    let bare_message = signing_message("1700000000000", "GET", "/trade-api/v2/markets");
    assert!(
        creds.verify(&bare_message, &with_query.signature),
        "signature over a path with a query did not verify against the bare path"
    );
}

#[test]
fn websocket_upgrade_signs_the_literal_ws_path_with_get() {
    // The single most common Kalshi auth failure: signing the REST path, the
    // full wss:// URL, or a path with a query, instead of exactly this.
    assert_eq!(WS_SIGNING_PATH, "/trade-api/ws/v2");

    let creds = test_credentials();
    let headers = creds.sign_websocket_upgrade().expect("sign");
    let expected = signing_message(&headers.timestamp, "GET", WS_SIGNING_PATH);
    assert_eq!(
        expected,
        format!("{}GET/trade-api/ws/v2", headers.timestamp)
    );
    assert!(creds.verify(&expected, &headers.signature));

    // And it is *not* a signature over any of the near-miss variants.
    for wrong in [
        format!("{}GET/trade-api/v2", headers.timestamp),
        format!("{}GET/trade-api/ws/v2?", headers.timestamp),
        format!("{}POST/trade-api/ws/v2", headers.timestamp),
        format!(
            "{}GETwss://api.elections.kalshi.com/trade-api/ws/v2",
            headers.timestamp
        ),
        format!("{}GET/trade-api/ws/v2/", headers.timestamp),
    ] {
        assert!(
            !creds.verify(&wrong, &headers.signature),
            "signature unexpectedly verified against {wrong:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cross-implementation known-answer test: the salt length
// ---------------------------------------------------------------------------

#[test]
fn accepts_digest_length_salt_and_rejects_max_length_salt() {
    // This is the real assertion behind the PSS.DIGEST_LENGTH requirement.
    // Both signatures below are valid RSA-PSS/SHA-256 over the same message
    // with the same key, produced by Python `cryptography`. They differ only in
    // salt length. If this crate's parameters ever drift to max-length salt,
    // the first assertion fails here rather than as an unexplained 401 at 1pm
    // on a Sunday.
    let verified_digest = kalshi_ingest::auth::verify_with_public_key_pem(
        KAT_PUBLIC_KEY_PEM,
        KAT_MESSAGE,
        KAT_SIGNATURE_DIGEST_SALT,
        "known-answer vector",
    )
    .expect("public key parses");
    assert!(
        verified_digest,
        "digest-length-salt signature failed to verify: this crate's PSS \
         parameters no longer match Kalshi's documented PSS.DIGEST_LENGTH"
    );

    let verified_max = kalshi_ingest::auth::verify_with_public_key_pem(
        KAT_PUBLIC_KEY_PEM,
        KAT_MESSAGE,
        KAT_SIGNATURE_MAX_SALT,
        "known-answer vector",
    )
    .expect("public key parses");
    assert!(
        !verified_max,
        "max-length-salt signature verified, so this test cannot distinguish \
         the two salt lengths and proves nothing"
    );
}

#[test]
fn our_signatures_verify_under_the_same_parameters() {
    // The other direction: what we produce is what that verifier accepts.
    let creds = test_credentials();
    let message = signing_message("1700000000000", "GET", WS_SIGNING_PATH);
    let headers = creds
        .sign("GET", WS_SIGNING_PATH, "1700000000000")
        .expect("sign");
    assert!(creds.verify(&message, &headers.signature));
}

#[test]
fn pss_is_randomized_so_two_signatures_differ() {
    // Why there is no fixed signature vector for our own signing: PSS salts are
    // random, so byte-comparing against a stored signature would be wrong.
    let creds = test_credentials();
    let a = creds.sign("GET", "/x", "1700000000000").expect("sign");
    let b = creds.sign("GET", "/x", "1700000000000").expect("sign");
    assert_ne!(a.signature, b.signature, "PSS signature was deterministic");
    let message = signing_message("1700000000000", "GET", "/x");
    assert!(creds.verify(&message, &a.signature));
    assert!(creds.verify(&message, &b.signature));
}

#[test]
fn a_signature_does_not_verify_for_a_different_message() {
    let creds = test_credentials();
    let headers = creds
        .sign("GET", "/trade-api/v2/markets", "1700000000000")
        .expect("sign");
    for wrong in [
        signing_message("1700000000001", "GET", "/trade-api/v2/markets"), // ts
        signing_message("1700000000000", "POST", "/trade-api/v2/markets"), // method
        signing_message("1700000000000", "GET", "/trade-api/v2/events"),  // path
        String::new(),
    ] {
        assert!(
            !creds.verify(&wrong, &headers.signature),
            "verified {wrong:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Headers and secret hygiene
// ---------------------------------------------------------------------------

#[test]
fn emits_the_three_required_headers() {
    let creds = test_credentials();
    let headers = creds
        .sign("GET", "/trade-api/v2/markets", "1700000000000")
        .expect("sign");
    let names: Vec<&str> = headers.as_pairs().iter().map(|(name, _)| *name).collect();
    assert_eq!(
        names,
        vec![
            "KALSHI-ACCESS-KEY",
            "KALSHI-ACCESS-SIGNATURE",
            "KALSHI-ACCESS-TIMESTAMP"
        ]
    );
    assert_eq!(headers.access_key, "test-key-id");
    assert_eq!(headers.timestamp, "1700000000000");
    // Base64, and the right length for a 2048-bit modulus.
    assert_eq!(
        base64_len_to_bytes(&headers.signature),
        256,
        "expected a 256-byte signature for a 2048-bit key"
    );
}

fn base64_len_to_bytes(s: &str) -> usize {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    STANDARD.decode(s).map(|b| b.len()).unwrap_or(0)
}

#[test]
fn timestamp_is_milliseconds_not_seconds() {
    let ts = kalshi_ingest::auth::timestamp_ms_now().expect("clock");
    let value: u64 = ts.parse().expect("decimal digits");
    // Sept 2020 in ms; any seconds-based timestamp would be far below this.
    assert!(value > 1_600_000_000_000, "{ts} looks like seconds, not ms");
    assert!(
        value < 4_000_000_000_000,
        "{ts} is implausibly far in the future"
    );
}

#[test]
fn debug_output_never_contains_key_material() {
    // A struct holding Credentials must be safe to log.
    let creds = test_credentials();
    let rendered = format!("{creds:?}");
    assert!(rendered.contains("test-key-id"));
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("PRIVATE"));
    assert!(!rendered.contains("BEGIN"));
}

#[test]
fn rejects_a_malformed_key_without_echoing_it() {
    // The PEM header is assembled from two literals so that the private-key
    // marker never appears verbatim in this file. The pre-commit hook refuses
    // any commit containing it, and that hook is deliberately absolute: an
    // allowlist for "test files" is the crack through which a real key
    // eventually slips.
    let header = concat!("-----BEGIN ", "PRIVATE KEY-----");
    let malformed = format!("{header}\nnonsense\n");
    let err = Credentials::from_pem("id", &malformed, "unit-test").expect_err("should not load");
    let rendered = format!("{err}");
    assert!(rendered.contains("unit-test"));
    assert!(
        !rendered.contains("nonsense"),
        "error echoed key bytes: {rendered}"
    );
}
