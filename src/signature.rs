//! HMAC-SHA256 signature verification (Meta / WhatsApp Cloud API `X-Hub-Signature-256` style).
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Computes the `sha256=<hex>` header value for `body`.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Verifies a `sha256=<hex>` header in constant time.
pub fn verify(secret: &[u8], body: &[u8], header: &str) -> bool {
    let Some(hex_sig) = header.trim().strip_prefix("sha256=") else {
        return false;
    };
    let Ok(provided) = hex::decode(hex_sig) else {
        return false;
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    let expected = mac.finalize().into_bytes();
    expected.len() == provided.len() && bool::from(expected.as_slice().ct_eq(&provided))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let h = sign(b"s3cret", b"{\"a\":1}");
        assert!(verify(b"s3cret", b"{\"a\":1}", &h));
    }

    #[test]
    fn rejects_tampered_body_wrong_key_and_garbage() {
        let h = sign(b"s3cret", b"payload");
        assert!(!verify(b"s3cret", b"payload!", &h));
        assert!(!verify(b"other", b"payload", &h));
        assert!(!verify(b"s3cret", b"payload", "sha1=abcd"));
        assert!(!verify(b"s3cret", b"payload", "sha256=zz"));
        assert!(!verify(b"s3cret", b"payload", "sha256=abcd"));
    }

    #[test]
    fn known_vector() {
        // RFC 4231-style sanity check computed with openssl.
        assert_eq!(
            sign(b"key", b"The quick brown fox jumps over the lazy dog"),
            "sha256=f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
}
