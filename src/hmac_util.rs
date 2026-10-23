//! Infallible HMAC-SHA256 construction (issue #1246).
//!
//! `Hmac::<Sha256>::new_from_slice` returns a `Result` even though HMAC accepts
//! keys of any length, which led to `expect("HMAC accepts any key length")`
//! calls throughout the crate. [`hmac_sha256`] performs the RFC 2104 key
//! normalisation itself (hash keys longer than the block size, then zero-pad)
//! and hands a block-sized key to the infallible `KeyInit::new`, producing
//! exactly the same MAC as `new_from_slice`.

use hmac::digest::KeyInit;
use hmac::Hmac;
use sha2::{Digest, Sha256};

/// SHA-256 block size in bytes — the HMAC key length after normalisation.
const SHA256_BLOCK_SIZE: usize = 64;

/// Build an HMAC-SHA256 instance keyed with `key` (any length).
pub fn hmac_sha256(key: &[u8]) -> Hmac<Sha256> {
    let mut block = [0u8; SHA256_BLOCK_SIZE];
    if key.len() > SHA256_BLOCK_SIZE {
        let digest = Sha256::digest(key);
        block[..digest.len()].copy_from_slice(&digest);
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    <Hmac<Sha256> as KeyInit>::new(&block.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::Mac;

    fn reference(key: &[u8], msg: &[u8]) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }

    fn ours(key: &[u8], msg: &[u8]) -> Vec<u8> {
        let mut mac = hmac_sha256(key);
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }

    #[test]
    fn matches_new_from_slice_for_all_key_lengths() {
        for len in [0usize, 1, 32, 63, 64, 65, 128, 1000] {
            let key = vec![0xA5u8; len];
            assert_eq!(ours(&key, b"payload"), reference(&key, b"payload"), "key len {len}");
        }
    }

    #[test]
    fn rfc4231_test_case_2() {
        let mut mac = hmac_sha256(b"Jefe");
        mac.update(b"what do ya want for nothing?");
        assert_eq!(
            hex::encode(mac.finalize().into_bytes()),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
