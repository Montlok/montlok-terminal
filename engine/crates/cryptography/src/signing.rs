// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use aws_lc_rs::{hmac, rand as lc_rand, rsa::KeyPair, signature as lc_signature};
use base64::prelude::*;
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, VerifyingKey};
use nautilus_core::hex;
use thiserror::Error;

/// Failures returned while creating an Ed25519 signature.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Ed25519SigningError {
    /// The private key seed is not a 32-byte Ed25519 seed.
    #[error("invalid Ed25519 private key length: expected 32 bytes, got {actual}")]
    InvalidPrivateKeyLength {
        /// Actual private key length.
        actual: usize,
    },
}

/// Failures returned while verifying an Ed25519 signature.
#[derive(Debug, Error)]
pub enum Ed25519VerificationError {
    /// The public key is not a 32-byte Ed25519 public key.
    #[error("invalid Ed25519 public key length: expected 32 bytes, got {actual}")]
    InvalidPublicKeyLength {
        /// Actual public key length.
        actual: usize,
    },

    /// The public key bytes do not encode a valid Ed25519 point.
    #[error("invalid Ed25519 public key")]
    InvalidPublicKey,

    /// The detached signature is not valid Base64.
    #[error("invalid Base64 Ed25519 signature: {0}")]
    InvalidSignatureEncoding(#[from] base64::DecodeError),

    /// The detached signature is not 64 bytes long.
    #[error("invalid Ed25519 signature length: expected 64 bytes, got {actual}")]
    InvalidSignatureLength {
        /// Actual signature length.
        actual: usize,
    },

    /// The signature does not authenticate the supplied message and public key.
    #[error("Ed25519 signature verification failed")]
    VerificationFailed,
}

/// Generates an HMAC-SHA256 signature for the given data using the provided secret.
///
/// This function creates a cryptographic hash-based message authentication code (HMAC)
/// using SHA-256 as the underlying hash function. The resulting signature is returned
/// as a lowercase hexadecimal string.
///
/// # Errors
///
/// Returns an error if signature generation fails due to key or cryptographic errors.
pub fn hmac_signature(secret: &str, data: &str) -> anyhow::Result<String> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let tag = hmac::sign(&key, data.as_bytes());
    Ok(hex::encode(tag.as_ref()))
}

/// Signs `data` using RSA PKCS#1 v1.5 SHA-256 with the provided private key in PEM format.
///
/// # Errors
///
/// Returns an error if:
/// - `data` is empty.
/// - `private_key_pem` is not a valid PEM-encoded PKCS#8 RSA private key or cannot be parsed.
/// - Signature generation fails due to key or cryptographic errors.
pub fn rsa_signature(private_key_pem: &str, data: &str) -> anyhow::Result<String> {
    if data.is_empty() {
        anyhow::bail!("Query string cannot be empty");
    }

    // Remove PEM headings and decode to DER bytes using the `pem` crate
    let pem = pem::parse(private_key_pem.trim())
        .map_err(|e| anyhow::anyhow!("Failed to parse PEM: {e}"))?;

    // Ensure this is a private key
    if !pem.tag().ends_with("PRIVATE KEY") {
        anyhow::bail!("PEM does not contain a private key");
    }

    // Construct RSA key pair from PKCS#8 DER bytes
    let key_pair = KeyPair::from_pkcs8(pem.contents())
        .map_err(|_| anyhow::anyhow!("Failed to decode RSA private key"))?;

    // Prepare RNG and output buffer (signature length = modulus length)
    let rng = lc_rand::SystemRandom::new();
    let mut signature = vec![0u8; key_pair.public_modulus_len()];

    key_pair
        .sign(
            &lc_signature::RSA_PKCS1_SHA256,
            &rng,
            data.as_bytes(),
            &mut signature,
        )
        .map_err(|_| anyhow::anyhow!("Failed to generate RSA signature"))?;

    Ok(BASE64_STANDARD.encode(signature))
}

/// Signs `data` using Ed25519 with the provided private key seed.
///
/// # Errors
///
/// Returns an error if the provided private key seed is invalid or signature creation fails.
pub fn ed25519_signature(private_key: &[u8], data: &str) -> anyhow::Result<String> {
    Ok(BASE64_STANDARD.encode(ed25519_sign_bytes(private_key, data.as_bytes())?))
}

/// Signs arbitrary bytes with a 32-byte Ed25519 private key seed.
///
/// The returned value is the raw 64-byte detached signature. This primitive is intended for
/// canonical binary protocols; callers that need a textual signature can use
/// [`ed25519_signature`].
///
/// # Errors
///
/// Returns an error when `private_key` is not exactly 32 bytes long.
pub fn ed25519_sign_bytes(
    private_key: &[u8],
    data: &[u8],
) -> Result<[u8; 64], Ed25519SigningError> {
    let private_key: &[u8; 32] =
        private_key
            .try_into()
            .map_err(|_| Ed25519SigningError::InvalidPrivateKeyLength {
                actual: private_key.len(),
            })?;
    let signing_key = SigningKey::from_bytes(private_key);
    let signature: Ed25519Signature = signing_key.sign(data);
    Ok(signature.to_bytes())
}

/// Verifies a Base64-encoded Ed25519 signature over UTF-8 `data`.
///
/// This is the verification counterpart to [`ed25519_signature`].
///
/// # Errors
///
/// Returns an error when the public key or signature encoding is invalid, or when the
/// signature does not authenticate `data`.
pub fn ed25519_verify(
    public_key: &[u8],
    data: &str,
    signature: &str,
) -> Result<(), Ed25519VerificationError> {
    let signature = BASE64_STANDARD.decode(signature)?;
    ed25519_verify_bytes(public_key, data.as_bytes(), &signature)
}

/// Verifies a raw detached Ed25519 signature over arbitrary bytes.
///
/// # Errors
///
/// Returns an error when the public key or signature has an invalid length, the public key
/// does not encode a valid Ed25519 point, or the signature does not authenticate `data`.
pub fn ed25519_verify_bytes(
    public_key: &[u8],
    data: &[u8],
    signature: &[u8],
) -> Result<(), Ed25519VerificationError> {
    let public_key: &[u8; 32] =
        public_key
            .try_into()
            .map_err(|_| Ed25519VerificationError::InvalidPublicKeyLength {
                actual: public_key.len(),
            })?;
    let verifying_key = VerifyingKey::from_bytes(public_key)
        .map_err(|_| Ed25519VerificationError::InvalidPublicKey)?;
    let signature: &[u8; 64] =
        signature
            .try_into()
            .map_err(|_| Ed25519VerificationError::InvalidSignatureLength {
                actual: signature.len(),
            })?;
    let signature = Ed25519Signature::from_bytes(signature);

    verifying_key
        .verify_strict(data, &signature)
        .map_err(|_| Ed25519VerificationError::VerificationFailed)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        "mysecretkey",
        "data-to-sign",
        "19ed21a8b2a6b847d7d7aea059ab3134cd58f13c860cfbe89338c718685fe077"
    )]
    #[case(
        "anothersecretkey",
        "somedata",
        "fb44dab41435775b44a96aa008af58cbf1fa1cea32f4605562c586b98f7326c5"
    )]
    #[case(
        "",
        "data-without-secret",
        "740c92f9c332fbb22d80aa6a3c9c10197a3e9dc61ca7e3c298c21597e4672133"
    )]
    #[case(
        "mysecretkey",
        "",
        "bb4e89236de3b03c17e36d48ca059fa277b88165cb14813a49f082ed8974b9f4"
    )]
    #[case(
        "",
        "",
        "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
    )]
    fn test_hmac_signature(
        #[case] secret: &str,
        #[case] data: &str,
        #[case] expected_signature: &str,
    ) {
        let result = hmac_signature(secret, data).unwrap();
        assert_eq!(
            result, expected_signature,
            "Expected signature did not match"
        );
    }

    #[rstest]
    #[case(
        "-----BEGIN TEST KEY-----
MIIBVwIBADANBgkqhkiG9w0BAQEFAASCATswggE3AgEAAkEAu/...
-----END PRIVATE KEY-----",
        ""
    )]
    fn test_rsa_signature_empty_query(#[case] private_key_pem: &str, #[case] query_string: &str) {
        let result = rsa_signature(private_key_pem, query_string);
        assert!(
            result.is_err(),
            "Expected an error with empty query string, but got Ok"
        );
    }

    #[rstest]
    #[case(
        "-----BEGIN INVALID KEY-----
INVALID_KEY_DATA
-----END INVALID KEY-----",
        "This is a test query"
    )]
    fn test_rsa_signature_invalid_key(#[case] private_key_pem: &str, #[case] query_string: &str) {
        let result = rsa_signature(private_key_pem, query_string);
        assert!(
            result.is_err(),
            "Expected an error due to invalid key, but got Ok"
        );
    }

    const fn valid_ed25519_private_key() -> [u8; 32] {
        [
            0x0c, 0x74, 0x18, 0x92, 0x6b, 0x5d, 0xe9, 0x8f, 0xe2, 0xb6, 0x47, 0x8a, 0x51, 0xf9,
            0x97, 0x31, 0x9a, 0xcd, 0x2d, 0xbc, 0xf9, 0x94, 0xea, 0x8f, 0xc3, 0x1b, 0x65, 0x24,
            0x1f, 0x91, 0xd8, 0x6f,
        ]
    }

    #[rstest]
    #[case(valid_ed25519_private_key(), "This is a test query")]
    #[case(valid_ed25519_private_key(), "")]
    fn test_ed25519_signature(#[case] private_key_bytes: [u8; 32], #[case] query_string: &str) {
        let result = ed25519_signature(&private_key_bytes, query_string);
        assert!(
            result.is_ok(),
            "Expected valid signature but got an error: {result:?}"
        );
        assert!(!result.unwrap().is_empty(), "Signature should not be empty");
    }

    #[rstest]
    #[case("This is a test query")]
    #[case("")]
    fn test_ed25519_verify_accepts_signature(#[case] data: &str) {
        let private_key = valid_ed25519_private_key();
        let public_key = SigningKey::from_bytes(&private_key)
            .verifying_key()
            .to_bytes();
        let signature = ed25519_signature(&private_key, data).unwrap();

        ed25519_verify(&public_key, data, &signature).unwrap();
    }

    #[rstest]
    fn test_ed25519_raw_bytes_round_trip() {
        let private_key = valid_ed25519_private_key();
        let public_key = SigningKey::from_bytes(&private_key)
            .verifying_key()
            .to_bytes();
        let data = b"canonical\0binary\xffpayload";
        let signature = ed25519_sign_bytes(&private_key, data).unwrap();

        ed25519_verify_bytes(&public_key, data, &signature).unwrap();
    }

    #[rstest]
    fn test_ed25519_sign_bytes_rejects_invalid_seed_length() {
        assert_eq!(
            ed25519_sign_bytes(&[0; 31], b"data"),
            Err(Ed25519SigningError::InvalidPrivateKeyLength { actual: 31 })
        );
    }

    #[rstest]
    fn test_ed25519_verify_rejects_tampered_data() {
        let private_key = valid_ed25519_private_key();
        let public_key = SigningKey::from_bytes(&private_key)
            .verifying_key()
            .to_bytes();
        let signature = ed25519_signature(&private_key, "original").unwrap();

        assert!(matches!(
            ed25519_verify(&public_key, "tampered", &signature),
            Err(Ed25519VerificationError::VerificationFailed)
        ));
    }

    #[rstest]
    fn test_ed25519_verify_reports_malformed_inputs() {
        assert!(matches!(
            ed25519_verify(&[0; 31], "data", "AA=="),
            Err(Ed25519VerificationError::InvalidPublicKeyLength { actual: 31 })
        ));
        assert!(matches!(
            ed25519_verify(&[0; 32], "data", "not base64"),
            Err(Ed25519VerificationError::InvalidSignatureEncoding(_))
        ));

        let public_key = SigningKey::from_bytes(&valid_ed25519_private_key())
            .verifying_key()
            .to_bytes();
        assert!(matches!(
            ed25519_verify_bytes(&public_key, b"data", &[0; 63]),
            Err(Ed25519VerificationError::InvalidSignatureLength { actual: 63 })
        ));
    }
}
