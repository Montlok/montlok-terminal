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

//! Pure verification of signed native strategy artifacts.
//!
//! This module deliberately accepts artifact bytes and policy values supplied by the caller. It
//! does not resolve paths, read files, inspect the process environment, or otherwise consult
//! ambient state.

use std::collections::{BTreeMap, BTreeSet};

use nautilus_analysis::{PromotionAttestation, SignedPromotionAttestation};
use nautilus_cryptography::signing::{Ed25519VerificationError, ed25519_verify_bytes};
use nautilus_provenance::ContentDigest;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use thiserror::Error;

use crate::STRATEGY_ARTIFACT_MANIFEST_VERSION;

const SIGNING_DOMAIN: &[u8] = b"nautilus-plugin/strategy-artifact-manifest/v5";
const TRUST_POLICY_DOMAIN: &[u8] = b"nautilus-plugin/strategy-artifact-trust-policy/v5";
const ED25519_PUBLIC_KEY_LENGTH: usize = 32;
const ED25519_SIGNATURE_LENGTH: usize = 64;
const MAX_KEY_ID_LENGTH: usize = 255;
const MAX_COMPATIBILITY_FIELD_LENGTH: usize = 255;
const MAX_IDENTITY_FIELD_LENGTH: usize = 1_024;
const MAX_TRUSTED_KEYS_PER_ROLE: usize = 128;
const MAX_REVOKED_KEY_IDS_PER_ROLE: usize = 1_024;

struct BoundedTextVisitor<const MAXIMUM: usize>;

impl<const MAXIMUM: usize> Visitor<'_> for BoundedTextVisitor<MAXIMUM> {
    type Value = String;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "a UTF-8 string no longer than {MAXIMUM} bytes")
    }

    fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > MAXIMUM {
            return Err(E::invalid_length(value.len(), &self));
        }
        Ok(value.to_string())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > MAXIMUM {
            return Err(E::invalid_length(value.len(), &self));
        }
        Ok(value)
    }
}

fn deserialize_key_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(BoundedTextVisitor::<MAX_KEY_ID_LENGTH>)
}

fn deserialize_compatibility_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(BoundedTextVisitor::<MAX_COMPATIBILITY_FIELD_LENGTH>)
}

fn deserialize_identity_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(BoundedTextVisitor::<MAX_IDENTITY_FIELD_LENGTH>)
}

struct BoundedKeyId(String);

impl<'de> Deserialize<'de> for BoundedKeyId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_key_id(deserializer).map(Self)
    }
}

struct TrustedKeyMapVisitor;

impl<'de> Visitor<'de> for TrustedKeyMapVisitor {
    type Value = BTreeMap<String, Vec<u8>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "at most {MAX_TRUSTED_KEYS_PER_ROLE} Ed25519 public keys"
        )
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        if let Some(length) = map.size_hint()
            && length > MAX_TRUSTED_KEYS_PER_ROLE
        {
            return Err(de::Error::invalid_length(length, &self));
        }
        let mut keys = BTreeMap::new();
        while let Some(BoundedKeyId(key_id)) = map.next_key()? {
            if keys.len() == MAX_TRUSTED_KEYS_PER_ROLE {
                return Err(de::Error::invalid_length(keys.len() + 1, &self));
            }
            let public_key: [u8; ED25519_PUBLIC_KEY_LENGTH] = map.next_value()?;
            if keys.insert(key_id, public_key.to_vec()).is_some() {
                return Err(de::Error::custom("duplicate trusted key ID"));
            }
        }
        Ok(keys)
    }
}

fn deserialize_trusted_key_map<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_map(TrustedKeyMapVisitor)
}

struct RevokedKeySetVisitor;

impl<'de> Visitor<'de> for RevokedKeySetVisitor {
    type Value = BTreeSet<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "at most {MAX_REVOKED_KEY_IDS_PER_ROLE} revoked key IDs"
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let Some(length) = sequence.size_hint()
            && length > MAX_REVOKED_KEY_IDS_PER_ROLE
        {
            return Err(de::Error::invalid_length(length, &self));
        }
        let mut key_ids = BTreeSet::new();
        while let Some(BoundedKeyId(key_id)) = sequence.next_element()? {
            if key_ids.len() == MAX_REVOKED_KEY_IDS_PER_ROLE {
                return Err(de::Error::invalid_length(key_ids.len() + 1, &self));
            }
            if !key_ids.insert(key_id) {
                return Err(de::Error::custom("duplicate revoked key ID"));
            }
        }
        Ok(key_ids)
    }
}

fn deserialize_revoked_key_set<'de, D>(deserializer: D) -> Result<BTreeSet<String>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(RevokedKeySetVisitor)
}

struct DetachedSignatureVisitor;

impl<'de> Visitor<'de> for DetachedSignatureVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "exactly {ED25519_SIGNATURE_LENGTH} signature bytes"
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut signature = Vec::with_capacity(ED25519_SIGNATURE_LENGTH);
        while let Some(byte) = sequence.next_element()? {
            if signature.len() == ED25519_SIGNATURE_LENGTH {
                return Err(de::Error::invalid_length(signature.len() + 1, &self));
            }
            signature.push(byte);
        }
        if signature.len() != ED25519_SIGNATURE_LENGTH {
            return Err(de::Error::invalid_length(signature.len(), &self));
        }
        Ok(signature)
    }
}

/// A 32-byte BLAKE3 digest used by the strategy artifact trust contract.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactDigest(pub [u8; 32]);

impl ArtifactDigest {
    /// Returns the digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the digest as lowercase hexadecimal.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            output.push(nibble_to_hex(byte >> 4));
            output.push(nibble_to_hex(byte & 0x0f));
        }
        output
    }
}

impl std::fmt::Display for ArtifactDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

const fn nibble_to_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + nibble - 10) as char,
        _ => unreachable!(),
    }
}

/// Computes the BLAKE3 digest of the exact native artifact bytes.
#[must_use]
pub fn artifact_content_digest(content: &[u8]) -> ArtifactDigest {
    ArtifactDigest(*ContentDigest::digest_bytes(content).as_bytes())
}

/// Runtime container whose exact bytes are authenticated by a strategy manifest.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyRuntimeArtifact {
    /// The currently executing Nautilus host binary contains the strategy implementation.
    HostExecutable,
    /// A Python source module is loaded from the authenticated bytes.
    PythonSource,
}

/// Identity and provenance claims signed for a native strategy artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "StrategyArtifactManifestWire")]
pub struct StrategyArtifactManifest {
    /// Manifest schema version.
    pub schema_version: u32,
    /// Digest of the exact artifact bytes to be loaded.
    pub artifact_content_digest: ArtifactDigest,
    /// Stable release artifact identity referenced by promotion evidence.
    pub artifact_id: String,
    /// Runtime container represented by `artifact_content_digest`.
    pub runtime_artifact: StrategyRuntimeArtifact,
    /// Fully qualified native type or Python `module:Class` entrypoint.
    pub entrypoint: String,
    /// Stable strategy identity.
    pub strategy_id: String,
    /// Strategy release version.
    pub strategy_version: String,
    /// Compilation target triple.
    pub target_triple: String,
    /// Nautilus native plug-in ABI version.
    pub nautilus_abi_version: u32,
    /// Nautilus release version used by the strategy.
    pub nautilus_version: String,
    /// Fixed-point precision mode used by the strategy.
    pub precision_mode: String,
    /// Digest of the policy under which the strategy was approved.
    pub policy_digest: ArtifactDigest,
    /// Trust-store identity of the evaluator which authorized this release.
    pub promotion_evaluator_key_id: String,
    /// Canonical digest of the exact ordered evaluation suite used for promotion.
    pub promotion_suite_definition_digest: ArtifactDigest,
    /// Digest of the complete evaluator-signed promotion-attestation envelope.
    pub promotion_attestation_digest: ArtifactDigest,
    /// Canonical digest of the complete run provenance evaluated for this release.
    pub evaluated_run_provenance_digest: ArtifactDigest,
    /// Canonical commitment to the exact paired profile/listing/route market scope.
    pub evaluated_market_scope_commitment: ArtifactDigest,
    /// Stable identity of the exact strategy configuration evaluated for this release.
    pub strategy_config_artifact_id: String,
    /// Digest of the exact strategy configuration bytes evaluated for this release.
    pub strategy_config_digest: ArtifactDigest,
    /// Deployment or release-channel scope which prevents cross-environment reuse.
    pub deployment_scope: String,
    /// Identifier of the Ed25519 key which signed this manifest.
    pub signer_key_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyArtifactManifestWire {
    schema_version: u32,
    artifact_content_digest: ArtifactDigest,
    #[serde(deserialize_with = "deserialize_identity_text")]
    artifact_id: String,
    runtime_artifact: StrategyRuntimeArtifact,
    #[serde(deserialize_with = "deserialize_identity_text")]
    entrypoint: String,
    #[serde(deserialize_with = "deserialize_identity_text")]
    strategy_id: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    strategy_version: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    target_triple: String,
    nautilus_abi_version: u32,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    nautilus_version: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    precision_mode: String,
    policy_digest: ArtifactDigest,
    #[serde(deserialize_with = "deserialize_key_id")]
    promotion_evaluator_key_id: String,
    promotion_suite_definition_digest: ArtifactDigest,
    promotion_attestation_digest: ArtifactDigest,
    evaluated_run_provenance_digest: ArtifactDigest,
    evaluated_market_scope_commitment: ArtifactDigest,
    #[serde(deserialize_with = "deserialize_identity_text")]
    strategy_config_artifact_id: String,
    strategy_config_digest: ArtifactDigest,
    #[serde(deserialize_with = "deserialize_identity_text")]
    deployment_scope: String,
    #[serde(deserialize_with = "deserialize_key_id")]
    signer_key_id: String,
}

impl TryFrom<StrategyArtifactManifestWire> for StrategyArtifactManifest {
    type Error = StrategyArtifactTrustError;

    fn try_from(wire: StrategyArtifactManifestWire) -> Result<Self, Self::Error> {
        let manifest = Self {
            schema_version: wire.schema_version,
            artifact_content_digest: wire.artifact_content_digest,
            artifact_id: wire.artifact_id,
            runtime_artifact: wire.runtime_artifact,
            entrypoint: wire.entrypoint,
            strategy_id: wire.strategy_id,
            strategy_version: wire.strategy_version,
            target_triple: wire.target_triple,
            nautilus_abi_version: wire.nautilus_abi_version,
            nautilus_version: wire.nautilus_version,
            precision_mode: wire.precision_mode,
            policy_digest: wire.policy_digest,
            promotion_evaluator_key_id: wire.promotion_evaluator_key_id,
            promotion_suite_definition_digest: wire.promotion_suite_definition_digest,
            promotion_attestation_digest: wire.promotion_attestation_digest,
            evaluated_run_provenance_digest: wire.evaluated_run_provenance_digest,
            evaluated_market_scope_commitment: wire.evaluated_market_scope_commitment,
            strategy_config_artifact_id: wire.strategy_config_artifact_id,
            strategy_config_digest: wire.strategy_config_digest,
            deployment_scope: wire.deployment_scope,
            signer_key_id: wire.signer_key_id,
        };
        validate_manifest_shape(&manifest)?;
        Ok(manifest)
    }
}

impl StrategyArtifactManifest {
    /// Computes the deterministic, domain-separated BLAKE3 payload signed by Ed25519.
    ///
    /// The encoding is independent of Serde formats. Every variable-length field is framed by a
    /// big-endian `u64` length and every manifest field is included in declaration order.
    #[must_use]
    pub fn signing_payload(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SIGNING_DOMAIN);
        hasher.update(&self.schema_version.to_be_bytes());
        write_bytes(&mut hasher, self.artifact_content_digest.as_bytes());
        write_str(&mut hasher, &self.artifact_id);
        hasher.update(&[match self.runtime_artifact {
            StrategyRuntimeArtifact::HostExecutable => 0,
            StrategyRuntimeArtifact::PythonSource => 1,
        }]);
        write_str(&mut hasher, &self.entrypoint);
        write_str(&mut hasher, &self.strategy_id);
        write_str(&mut hasher, &self.strategy_version);
        write_str(&mut hasher, &self.target_triple);
        hasher.update(&self.nautilus_abi_version.to_be_bytes());
        write_str(&mut hasher, &self.nautilus_version);
        write_str(&mut hasher, &self.precision_mode);
        write_bytes(&mut hasher, self.policy_digest.as_bytes());
        write_str(&mut hasher, &self.promotion_evaluator_key_id);
        write_bytes(
            &mut hasher,
            self.promotion_suite_definition_digest.as_bytes(),
        );
        write_bytes(&mut hasher, self.promotion_attestation_digest.as_bytes());
        write_bytes(&mut hasher, self.evaluated_run_provenance_digest.as_bytes());
        write_bytes(
            &mut hasher,
            self.evaluated_market_scope_commitment.as_bytes(),
        );
        write_str(&mut hasher, &self.strategy_config_artifact_id);
        write_bytes(&mut hasher, self.strategy_config_digest.as_bytes());
        write_str(&mut hasher, &self.deployment_scope);
        write_str(&mut hasher, &self.signer_key_id);
        *hasher.finalize().as_bytes()
    }
}

fn write_str(hasher: &mut blake3::Hasher, value: &str) {
    write_bytes(hasher, value.as_bytes());
}

fn write_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

/// A raw, detached Ed25519 signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DetachedSignature(pub Vec<u8>);

impl DetachedSignature {
    /// Creates a detached signature from raw bytes.
    #[must_use]
    pub fn new(signature: impl Into<Vec<u8>>) -> Self {
        Self(signature.into())
    }

    /// Returns the raw signature bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DetachedSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_seq(DetachedSignatureVisitor)
            .map(Self)
    }
}

/// A signed reference to a native strategy artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyArtifactReference {
    /// Signed artifact identity and provenance claims.
    pub manifest: StrategyArtifactManifest,
    /// Detached Ed25519 signature over [`StrategyArtifactManifest::signing_payload`].
    pub signature: DetachedSignature,
}

impl StrategyArtifactReference {
    /// Verifies this reference and the supplied exact artifact bytes against `policy`.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed trust error for invalid provenance, content, or compatibility.
    pub fn verify(
        &self,
        artifact_content: &[u8],
        promotion_attestation: &SignedPromotionAttestation,
        policy: &TrustPolicy,
    ) -> Result<VerifiedStrategyArtifact, StrategyArtifactTrustError> {
        verify_strategy_artifact(self, artifact_content, promotion_attestation, policy)
    }
}

/// Explicit inputs that determine whether a strategy artifact is trusted by a host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "TrustPolicyWire")]
pub struct TrustPolicy {
    /// Monotonically increasing deployment policy epoch.
    ///
    /// Epoch zero is invalid. Hosts must additionally enforce an externally persisted minimum
    /// epoch; embedding the epoch in this policy only makes it part of the signed capability
    /// commitment and does not itself provide rollback-resistant storage.
    pub policy_epoch: u64,
    /// Trusted Ed25519 public keys, indexed by stable key identifier.
    pub trusted_signing_keys: BTreeMap<String, Vec<u8>>,
    /// Key identifiers denied even when a matching public key remains trusted.
    pub revoked_signing_key_ids: BTreeSet<String>,
    /// Trusted evaluator Ed25519 public keys, indexed by stable key identifier.
    pub trusted_evaluator_keys: BTreeMap<String, Vec<u8>>,
    /// Evaluator key identifiers denied even when their public key remains trusted.
    pub revoked_evaluator_key_ids: BTreeSet<String>,
    /// Target triple required by this host.
    pub expected_target_triple: String,
    /// Nautilus plug-in ABI required by this host.
    pub expected_nautilus_abi_version: u32,
    /// Nautilus release required by this host.
    pub expected_nautilus_version: String,
    /// Precision mode required by this host.
    pub expected_precision_mode: String,
    /// Policy digest required for promotion into this host.
    pub expected_policy_digest: ArtifactDigest,
    /// Promotion attestation digest required for this deployment.
    pub expected_promotion_attestation_digest: ArtifactDigest,
    /// Exact paired profile/listing/route market scope required by this host.
    pub expected_market_scope_commitment: ArtifactDigest,
    /// Deployment or release-channel scope required by this host.
    pub expected_deployment_scope: String,
    /// Evaluation suite required to authorize promotion.
    pub expected_promotion_suite_id: String,
    /// Evaluation suite version required to authorize promotion.
    pub expected_promotion_suite_version: String,
    /// Canonical digest of the exact ordered evaluation suite required for promotion.
    pub expected_promotion_suite_definition_digest: ArtifactDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustPolicyWire {
    policy_epoch: u64,
    #[serde(deserialize_with = "deserialize_trusted_key_map")]
    trusted_signing_keys: BTreeMap<String, Vec<u8>>,
    #[serde(deserialize_with = "deserialize_revoked_key_set")]
    revoked_signing_key_ids: BTreeSet<String>,
    #[serde(deserialize_with = "deserialize_trusted_key_map")]
    trusted_evaluator_keys: BTreeMap<String, Vec<u8>>,
    #[serde(deserialize_with = "deserialize_revoked_key_set")]
    revoked_evaluator_key_ids: BTreeSet<String>,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    expected_target_triple: String,
    expected_nautilus_abi_version: u32,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    expected_nautilus_version: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    expected_precision_mode: String,
    expected_policy_digest: ArtifactDigest,
    expected_promotion_attestation_digest: ArtifactDigest,
    expected_market_scope_commitment: ArtifactDigest,
    #[serde(deserialize_with = "deserialize_identity_text")]
    expected_deployment_scope: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    expected_promotion_suite_id: String,
    #[serde(deserialize_with = "deserialize_compatibility_text")]
    expected_promotion_suite_version: String,
    expected_promotion_suite_definition_digest: ArtifactDigest,
}

impl TryFrom<TrustPolicyWire> for TrustPolicy {
    type Error = StrategyArtifactTrustError;

    fn try_from(wire: TrustPolicyWire) -> Result<Self, Self::Error> {
        let policy = Self {
            policy_epoch: wire.policy_epoch,
            trusted_signing_keys: wire.trusted_signing_keys,
            revoked_signing_key_ids: wire.revoked_signing_key_ids,
            trusted_evaluator_keys: wire.trusted_evaluator_keys,
            revoked_evaluator_key_ids: wire.revoked_evaluator_key_ids,
            expected_target_triple: wire.expected_target_triple,
            expected_nautilus_abi_version: wire.expected_nautilus_abi_version,
            expected_nautilus_version: wire.expected_nautilus_version,
            expected_precision_mode: wire.expected_precision_mode,
            expected_policy_digest: wire.expected_policy_digest,
            expected_promotion_attestation_digest: wire.expected_promotion_attestation_digest,
            expected_market_scope_commitment: wire.expected_market_scope_commitment,
            expected_deployment_scope: wire.expected_deployment_scope,
            expected_promotion_suite_id: wire.expected_promotion_suite_id,
            expected_promotion_suite_version: wire.expected_promotion_suite_version,
            expected_promotion_suite_definition_digest: wire
                .expected_promotion_suite_definition_digest,
        };
        policy.validate()?;
        Ok(policy)
    }
}

impl TrustPolicy {
    /// Validates the complete policy shape before it is used for a trust decision.
    ///
    /// This checks bounded canonical text, bounded key collections, exact Ed25519 key lengths,
    /// revocation conflicts, and strict signer/evaluator role separation.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed error for any malformed or ambiguous policy value.
    pub fn validate(&self) -> Result<(), StrategyArtifactTrustError> {
        validate_trust_policy_shape(self)
    }

    /// Validates the policy and computes its canonical commitment.
    ///
    /// Hosts should use this method, rather than [`Self::commitment`], when pinning a policy
    /// received from an external configuration boundary.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed error without producing a commitment when the policy is malformed.
    pub fn validated_commitment(&self) -> Result<ArtifactDigest, StrategyArtifactTrustError> {
        self.validate()?;
        Ok(self.commitment())
    }

    /// Computes the canonical commitment a host pins before accepting verified artifacts.
    ///
    /// This compatibility helper hashes every policy field but does not validate externally
    /// constructed values. Trust boundaries should use [`Self::validated_commitment`].
    #[must_use]
    pub fn commitment(&self) -> ArtifactDigest {
        let mut hasher = blake3::Hasher::new();
        write_bytes(&mut hasher, TRUST_POLICY_DOMAIN);
        hasher.update(&self.policy_epoch.to_be_bytes());
        hasher.update(&(self.trusted_signing_keys.len() as u64).to_be_bytes());
        for (key_id, public_key) in &self.trusted_signing_keys {
            write_str(&mut hasher, key_id);
            write_bytes(&mut hasher, public_key);
        }
        hasher.update(&(self.revoked_signing_key_ids.len() as u64).to_be_bytes());
        for key_id in &self.revoked_signing_key_ids {
            write_str(&mut hasher, key_id);
        }
        hasher.update(&(self.trusted_evaluator_keys.len() as u64).to_be_bytes());
        for (key_id, public_key) in &self.trusted_evaluator_keys {
            write_str(&mut hasher, key_id);
            write_bytes(&mut hasher, public_key);
        }
        hasher.update(&(self.revoked_evaluator_key_ids.len() as u64).to_be_bytes());
        for key_id in &self.revoked_evaluator_key_ids {
            write_str(&mut hasher, key_id);
        }
        write_str(&mut hasher, &self.expected_target_triple);
        hasher.update(&self.expected_nautilus_abi_version.to_be_bytes());
        write_str(&mut hasher, &self.expected_nautilus_version);
        write_str(&mut hasher, &self.expected_precision_mode);
        write_bytes(&mut hasher, self.expected_policy_digest.as_bytes());
        write_bytes(
            &mut hasher,
            self.expected_promotion_attestation_digest.as_bytes(),
        );
        write_bytes(
            &mut hasher,
            self.expected_market_scope_commitment.as_bytes(),
        );
        write_str(&mut hasher, &self.expected_deployment_scope);
        write_str(&mut hasher, &self.expected_promotion_suite_id);
        write_str(&mut hasher, &self.expected_promotion_suite_version);
        write_bytes(
            &mut hasher,
            self.expected_promotion_suite_definition_digest.as_bytes(),
        );
        ArtifactDigest(*hasher.finalize().as_bytes())
    }

    /// Verifies a strategy artifact reference without consulting ambient state.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed trust error for invalid provenance, content, or compatibility.
    pub fn verify(
        &self,
        reference: &StrategyArtifactReference,
        artifact_content: &[u8],
        promotion_attestation: &SignedPromotionAttestation,
    ) -> Result<VerifiedStrategyArtifact, StrategyArtifactTrustError> {
        verify_strategy_artifact(reference, artifact_content, promotion_attestation, self)
    }
}

/// Evidence that an artifact reference passed every trust and compatibility check.
///
/// Instances can only be constructed by this module's verifier.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedStrategyArtifact {
    reference: StrategyArtifactReference,
    trust_policy_commitment: ArtifactDigest,
    trust_policy_epoch: u64,
}

impl VerifiedStrategyArtifact {
    /// Returns the verified signed reference.
    #[must_use]
    pub const fn reference(&self) -> &StrategyArtifactReference {
        &self.reference
    }

    /// Returns the verified strategy artifact manifest.
    #[must_use]
    pub const fn manifest(&self) -> &StrategyArtifactManifest {
        &self.reference.manifest
    }

    /// Returns the verified artifact content digest.
    #[must_use]
    pub const fn content_digest(&self) -> ArtifactDigest {
        self.reference.manifest.artifact_content_digest
    }

    /// Returns the exact trust-policy commitment used to mint this capability.
    #[must_use]
    pub const fn trust_policy_commitment(&self) -> ArtifactDigest {
        self.trust_policy_commitment
    }

    /// Returns the policy epoch under which this capability was verified.
    #[must_use]
    pub const fn trust_policy_epoch(&self) -> u64 {
        self.trust_policy_epoch
    }
}

/// Fail-closed strategy artifact verification failures.
#[derive(Debug, Error)]
pub enum StrategyArtifactTrustError {
    /// Epoch zero cannot participate in rollback-protected trust decisions.
    #[error("strategy artifact trust policy epoch must be greater than zero")]
    InvalidPolicyEpoch,

    /// A required textual trust-policy field is empty.
    #[error("strategy artifact trust policy field '{field}' must not be empty")]
    EmptyTrustPolicyField {
        /// Empty field name.
        field: &'static str,
    },

    /// A textual trust-boundary field exceeds its protocol limit.
    #[error(
        "strategy artifact trust field '{field}' is too long: maximum {maximum} bytes, got {actual}"
    )]
    FieldTooLong {
        /// Field name.
        field: &'static str,
        /// Protocol maximum in bytes.
        maximum: usize,
        /// Actual UTF-8 byte length.
        actual: usize,
    },

    /// A trust-boundary identity contains leading/trailing whitespace or control characters.
    #[error("strategy artifact trust field '{field}' is not canonical text")]
    NonCanonicalTextField {
        /// Field name.
        field: &'static str,
    },

    /// A trust-policy key collection exceeds its protocol limit.
    #[error(
        "strategy artifact trust policy collection '{field}' is too large: maximum {maximum} entries, got {actual}"
    )]
    PolicyCollectionTooLarge {
        /// Collection field name.
        field: &'static str,
        /// Protocol maximum entry count.
        maximum: usize,
        /// Actual entry count.
        actual: usize,
    },

    /// A trusted public key does not have the exact Ed25519 public-key length.
    #[error(
        "invalid {role} Ed25519 public key length for '{key_id}': expected {expected} bytes, got {actual}"
    )]
    InvalidPolicyPublicKeyLength {
        /// Key role.
        role: &'static str,
        /// Stable key identifier.
        key_id: String,
        /// Required key length.
        expected: usize,
        /// Actual key length.
        actual: usize,
    },

    /// A key ID is simultaneously trusted and revoked for one role.
    #[error("{role} key '{key_id}' is both trusted and revoked")]
    TrustedKeyIsRevoked {
        /// Key role.
        role: &'static str,
        /// Conflicting key identifier.
        key_id: String,
    },

    /// One key identifier is assigned to both release-signing and evaluator roles.
    #[error("key ID '{key_id}' is assigned to both signer and evaluator roles")]
    TrustRoleKeyIdConflict {
        /// Ambiguous key identifier.
        key_id: String,
    },

    /// One Ed25519 public key is assigned to both release-signing and evaluator roles.
    #[error(
        "Ed25519 public key material is shared by signer '{signer_key_id}' and evaluator '{evaluator_key_id}'"
    )]
    TrustRolePublicKeyConflict {
        /// Signing-key identifier.
        signer_key_id: String,
        /// Evaluator-key identifier.
        evaluator_key_id: String,
    },

    /// The same public key material is aliased by multiple IDs inside one role.
    #[error(
        "{role} Ed25519 public key material is aliased by both '{first_key_id}' and '{second_key_id}'"
    )]
    DuplicatePolicyPublicKey {
        /// Key role.
        role: &'static str,
        /// First key identifier.
        first_key_id: String,
        /// Second key identifier.
        second_key_id: String,
    },

    /// The signed manifest uses an unknown schema.
    #[error("unsupported strategy artifact manifest schema {actual}; expected {expected}")]
    UnsupportedManifestSchema {
        /// Expected schema version.
        expected: u32,
        /// Actual schema version.
        actual: u32,
    },

    /// A required textual identity or compatibility field is empty.
    #[error("strategy artifact manifest field '{field}' must not be empty")]
    EmptyManifestField {
        /// Empty field name.
        field: &'static str,
    },

    /// A release manifest assigns the same key identity to signer and evaluator roles.
    #[error("manifest key ID '{key_id}' is assigned to both signer and evaluator roles")]
    ManifestTrustRoleConflict {
        /// Ambiguous manifest key identifier.
        key_id: String,
    },

    /// The artifact reference carries a malformed detached signature.
    #[error("invalid strategy artifact signature length: expected {expected} bytes, got {actual}")]
    InvalidDetachedSignatureLength {
        /// Required Ed25519 signature length.
        expected: usize,
        /// Actual signature length.
        actual: usize,
    },

    /// The manifest names an explicitly revoked signing key.
    #[error("strategy artifact signer key '{key_id}' is revoked")]
    RevokedSigner {
        /// Revoked key identifier.
        key_id: String,
    },

    /// The manifest names a key absent from the trusted policy set.
    #[error("strategy artifact signer key '{key_id}' is not trusted")]
    UntrustedSigner {
        /// Untrusted key identifier.
        key_id: String,
    },

    /// Ed25519 signature verification failed.
    #[error("invalid strategy artifact signature: {0}")]
    InvalidSignature(#[source] Ed25519VerificationError),

    /// The signed promotion envelope names an explicitly revoked evaluator key.
    #[error("promotion evaluator key '{key_id}' is revoked")]
    RevokedEvaluator {
        /// Revoked evaluator key identifier.
        key_id: String,
    },

    /// The signed promotion envelope names an evaluator absent from the trusted policy set.
    #[error("promotion evaluator key '{key_id}' is not trusted")]
    UntrustedEvaluator {
        /// Untrusted evaluator key identifier.
        key_id: String,
    },

    /// Evaluator Ed25519 signature verification failed.
    #[error("invalid promotion evaluator signature: {0}")]
    InvalidEvaluatorSignature(#[source] Ed25519VerificationError),

    /// The supplied artifact bytes do not match the signed content digest.
    #[error("strategy artifact content digest mismatch: expected {expected}, got {actual}")]
    ContentDigestMismatch {
        /// Signed digest.
        expected: ArtifactDigest,
        /// Digest computed from supplied artifact bytes.
        actual: ArtifactDigest,
    },

    /// The signed compilation target differs from the policy target.
    #[error("strategy artifact target triple mismatch: expected '{expected}', got '{actual}'")]
    TargetTripleMismatch {
        /// Policy value.
        expected: String,
        /// Manifest value.
        actual: String,
    },

    /// The signed Nautilus ABI differs from the policy ABI.
    #[error("strategy artifact Nautilus ABI mismatch: expected {expected}, got {actual}")]
    NautilusAbiMismatch {
        /// Policy value.
        expected: u32,
        /// Manifest value.
        actual: u32,
    },

    /// The signed Nautilus release differs from the policy release.
    #[error("strategy artifact Nautilus version mismatch: expected '{expected}', got '{actual}'")]
    NautilusVersionMismatch {
        /// Policy value.
        expected: String,
        /// Manifest value.
        actual: String,
    },

    /// The signed fixed-point precision mode differs from the host policy.
    #[error("strategy artifact precision mode mismatch: expected '{expected}', got '{actual}'")]
    PrecisionModeMismatch {
        /// Policy value.
        expected: String,
        /// Manifest value.
        actual: String,
    },

    /// The signed approval policy digest is not the required policy digest.
    #[error("strategy artifact policy digest mismatch: expected {expected}, got {actual}")]
    PolicyDigestMismatch {
        /// Policy value.
        expected: ArtifactDigest,
        /// Manifest value.
        actual: ArtifactDigest,
    },

    /// The signed promotion attestation is not authorized for this deployment.
    #[error(
        "strategy artifact promotion attestation digest mismatch: expected {expected}, got {actual}"
    )]
    PromotionAttestationDigestMismatch {
        /// Policy value.
        expected: ArtifactDigest,
        /// Manifest value.
        actual: ArtifactDigest,
    },

    /// The evaluator named by the release manifest differs from the signed envelope.
    #[error("promotion evaluator mismatch: expected '{expected}', got '{actual}'")]
    PromotionEvaluatorMismatch {
        /// Evaluator key ID signed into the release manifest.
        expected: String,
        /// Evaluator key ID carried by the signed envelope.
        actual: String,
    },

    /// The signed artifact is scoped to a different deployment.
    #[error("strategy artifact deployment scope mismatch: expected '{expected}', got '{actual}'")]
    DeploymentScopeMismatch {
        /// Policy value.
        expected: String,
        /// Manifest value.
        actual: String,
    },

    /// The supplied evaluator-signed envelope failed its canonical integrity check.
    #[error("strategy artifact signed promotion envelope failed its integrity check")]
    InvalidSignedPromotionAttestation,

    /// The enclosed promotion attestation failed its canonical integrity check.
    #[error("strategy artifact promotion claims failed their integrity check")]
    InvalidPromotionAttestation,

    /// Promotion evidence did not name the exact signed release artifact.
    #[error("promotion candidate mismatch: expected '{expected}', got '{actual}'")]
    PromotionCandidateMismatch {
        /// Signed release artifact identity and digest.
        expected: String,
        /// Attested candidate identity and digest.
        actual: String,
    },

    /// Strong promotion admission requires an exact run-evidence artifact.
    #[error("strategy artifact promotion attestation has no run-evidence artifact")]
    MissingPromotionEvidence,

    /// Strong promotion admission requires the evaluated run provenance digest.
    #[error("strategy artifact promotion attestation has no run provenance digest")]
    MissingRunProvenanceDigest,

    /// The signed release names a different evaluated run provenance digest.
    #[error("evaluated run provenance digest mismatch: expected {expected}, got '{actual}'")]
    RunProvenanceDigestMismatch {
        /// Signed manifest digest.
        expected: ArtifactDigest,
        /// Attested digest string.
        actual: String,
    },

    /// The signed release manifest targets a market scope not authorized by host policy.
    #[error("market scope policy mismatch: expected {expected}, got {actual}")]
    MarketScopePolicyMismatch {
        /// Policy-pinned paired market-scope commitment.
        expected: ArtifactDigest,
        /// Manifest-signed paired market-scope commitment.
        actual: ArtifactDigest,
    },

    /// Strong promotion admission requires the exact paired market-scope commitment.
    #[error("strategy artifact promotion attestation has no market scope commitment")]
    MissingMarketScopeCommitment,

    /// The evaluator attested a different market scope from the signed release manifest.
    #[error("market scope attestation mismatch: expected {expected}, got '{actual}'")]
    MarketScopeAttestationMismatch {
        /// Manifest-signed paired market-scope commitment.
        expected: ArtifactDigest,
        /// Evaluator-attested commitment string.
        actual: String,
    },

    /// Strong promotion admission requires the exact evaluated strategy configuration.
    #[error("strategy artifact promotion attestation has no strategy configuration artifact")]
    MissingStrategyConfigArtifact,

    /// The attested configuration differs from the signed release contract.
    #[error("strategy configuration mismatch: expected '{expected}', got '{actual}'")]
    StrategyConfigMismatch {
        /// Signed configuration identity and digest.
        expected: String,
        /// Attested configuration identity and digest.
        actual: String,
    },

    /// Strong promotion admission requires the evaluated strategy entrypoint.
    #[error("strategy artifact promotion attestation has no strategy entrypoint")]
    MissingStrategyEntrypoint,

    /// The attested runtime entrypoint differs from the signed release contract.
    #[error("strategy entrypoint mismatch: expected '{expected}', got '{actual}'")]
    StrategyEntrypointMismatch {
        /// Signed manifest entrypoint.
        expected: String,
        /// Attested runtime entrypoint.
        actual: String,
    },

    /// Strong promotion admission requires the concrete evaluated strategy identity.
    #[error("strategy artifact promotion attestation has no strategy runtime ID")]
    MissingStrategyRuntimeId,

    /// The attested strategy identity differs from the signed release contract.
    #[error("strategy runtime ID mismatch: expected '{expected}', got '{actual}'")]
    StrategyRuntimeIdMismatch {
        /// Signed manifest strategy identity.
        expected: String,
        /// Attested runtime strategy identity.
        actual: String,
    },

    /// The attestation was created for a different evaluation suite.
    #[error("promotion suite mismatch: expected '{expected}', got '{actual}'")]
    PromotionSuiteMismatch {
        /// Policy suite identity and version.
        expected: String,
        /// Attested suite identity and version.
        actual: String,
    },

    /// The attestation used different gates under the expected suite identity and version.
    #[error("promotion suite definition mismatch: expected {expected}, got {actual}")]
    PromotionSuiteDefinitionMismatch {
        /// Policy-pinned suite definition digest.
        expected: ArtifactDigest,
        /// Attested suite definition digest.
        actual: ArtifactDigest,
    },
}

/// Verifies exact artifact bytes and a signed reference against an explicit trust policy.
///
/// Verification is deterministic and performs no path resolution or ambient I/O.
///
/// # Errors
///
/// Returns a fail-closed trust error for invalid provenance, content, or compatibility.
pub fn verify_strategy_artifact(
    reference: &StrategyArtifactReference,
    artifact_content: &[u8],
    promotion_attestation: &SignedPromotionAttestation,
    policy: &TrustPolicy,
) -> Result<VerifiedStrategyArtifact, StrategyArtifactTrustError> {
    let trust_policy_commitment = policy.validated_commitment()?;

    let manifest = &reference.manifest;
    validate_manifest_shape(manifest)?;
    if reference.signature.as_bytes().len() != ED25519_SIGNATURE_LENGTH {
        return Err(StrategyArtifactTrustError::InvalidDetachedSignatureLength {
            expected: ED25519_SIGNATURE_LENGTH,
            actual: reference.signature.as_bytes().len(),
        });
    }
    validate_signed_promotion_attestation_shape(promotion_attestation)?;

    if policy
        .revoked_signing_key_ids
        .contains(&manifest.signer_key_id)
    {
        return Err(StrategyArtifactTrustError::RevokedSigner {
            key_id: manifest.signer_key_id.clone(),
        });
    }

    let public_key = policy
        .trusted_signing_keys
        .get(&manifest.signer_key_id)
        .ok_or_else(|| StrategyArtifactTrustError::UntrustedSigner {
            key_id: manifest.signer_key_id.clone(),
        })?;
    ed25519_verify_bytes(
        public_key,
        &manifest.signing_payload(),
        reference.signature.as_bytes(),
    )
    .map_err(StrategyArtifactTrustError::InvalidSignature)?;

    let actual_content_digest = artifact_content_digest(artifact_content);
    if actual_content_digest != manifest.artifact_content_digest {
        return Err(StrategyArtifactTrustError::ContentDigestMismatch {
            expected: manifest.artifact_content_digest,
            actual: actual_content_digest,
        });
    }
    if manifest.target_triple != policy.expected_target_triple {
        return Err(StrategyArtifactTrustError::TargetTripleMismatch {
            expected: policy.expected_target_triple.clone(),
            actual: manifest.target_triple.clone(),
        });
    }
    if manifest.nautilus_abi_version != policy.expected_nautilus_abi_version {
        return Err(StrategyArtifactTrustError::NautilusAbiMismatch {
            expected: policy.expected_nautilus_abi_version,
            actual: manifest.nautilus_abi_version,
        });
    }
    if manifest.nautilus_version != policy.expected_nautilus_version {
        return Err(StrategyArtifactTrustError::NautilusVersionMismatch {
            expected: policy.expected_nautilus_version.clone(),
            actual: manifest.nautilus_version.clone(),
        });
    }
    if manifest.precision_mode != policy.expected_precision_mode {
        return Err(StrategyArtifactTrustError::PrecisionModeMismatch {
            expected: policy.expected_precision_mode.clone(),
            actual: manifest.precision_mode.clone(),
        });
    }
    if manifest.policy_digest != policy.expected_policy_digest {
        return Err(StrategyArtifactTrustError::PolicyDigestMismatch {
            expected: policy.expected_policy_digest,
            actual: manifest.policy_digest,
        });
    }
    if manifest.promotion_attestation_digest != policy.expected_promotion_attestation_digest {
        return Err(
            StrategyArtifactTrustError::PromotionAttestationDigestMismatch {
                expected: policy.expected_promotion_attestation_digest,
                actual: manifest.promotion_attestation_digest,
            },
        );
    }
    if manifest.promotion_suite_definition_digest
        != policy.expected_promotion_suite_definition_digest
    {
        return Err(
            StrategyArtifactTrustError::PromotionSuiteDefinitionMismatch {
                expected: policy.expected_promotion_suite_definition_digest,
                actual: manifest.promotion_suite_definition_digest,
            },
        );
    }
    validate_market_scope_policy(manifest, policy)?;
    if manifest.deployment_scope != policy.expected_deployment_scope {
        return Err(StrategyArtifactTrustError::DeploymentScopeMismatch {
            expected: policy.expected_deployment_scope.clone(),
            actual: manifest.deployment_scope.clone(),
        });
    }
    validate_signed_promotion_attestation(manifest, promotion_attestation, policy)?;

    Ok(VerifiedStrategyArtifact {
        reference: reference.clone(),
        trust_policy_commitment,
        trust_policy_epoch: policy.policy_epoch,
    })
}

fn validate_market_scope_policy(
    manifest: &StrategyArtifactManifest,
    policy: &TrustPolicy,
) -> Result<(), StrategyArtifactTrustError> {
    if manifest.evaluated_market_scope_commitment != policy.expected_market_scope_commitment {
        return Err(StrategyArtifactTrustError::MarketScopePolicyMismatch {
            expected: policy.expected_market_scope_commitment,
            actual: manifest.evaluated_market_scope_commitment,
        });
    }
    Ok(())
}

fn validate_signed_promotion_attestation(
    manifest: &StrategyArtifactManifest,
    signed_attestation: &SignedPromotionAttestation,
    policy: &TrustPolicy,
) -> Result<(), StrategyArtifactTrustError> {
    validate_signed_promotion_attestation_shape(signed_attestation)?;
    if !signed_attestation.is_internally_consistent() {
        return Err(StrategyArtifactTrustError::InvalidSignedPromotionAttestation);
    }
    if signed_attestation.evaluator_key_id() != manifest.promotion_evaluator_key_id {
        return Err(StrategyArtifactTrustError::PromotionEvaluatorMismatch {
            expected: manifest.promotion_evaluator_key_id.clone(),
            actual: signed_attestation.evaluator_key_id().to_string(),
        });
    }
    let evaluator_key_id = signed_attestation.evaluator_key_id();
    if policy.revoked_evaluator_key_ids.contains(evaluator_key_id) {
        return Err(StrategyArtifactTrustError::RevokedEvaluator {
            key_id: evaluator_key_id.to_string(),
        });
    }
    let evaluator_public_key = policy
        .trusted_evaluator_keys
        .get(evaluator_key_id)
        .ok_or_else(|| StrategyArtifactTrustError::UntrustedEvaluator {
            key_id: evaluator_key_id.to_string(),
        })?;
    ed25519_verify_bytes(
        evaluator_public_key,
        &signed_attestation.signing_payload(),
        signed_attestation.signature().as_bytes(),
    )
    .map_err(StrategyArtifactTrustError::InvalidEvaluatorSignature)?;

    let envelope_digest = ArtifactDigest(*signed_attestation.envelope_digest().as_bytes());
    if envelope_digest != manifest.promotion_attestation_digest {
        return Err(
            StrategyArtifactTrustError::PromotionAttestationDigestMismatch {
                expected: manifest.promotion_attestation_digest,
                actual: envelope_digest,
            },
        );
    }

    validate_promotion_attestation(manifest, signed_attestation.attestation(), policy)
}

fn validate_promotion_attestation(
    manifest: &StrategyArtifactManifest,
    promotion_attestation: &PromotionAttestation,
    policy: &TrustPolicy,
) -> Result<(), StrategyArtifactTrustError> {
    if !promotion_attestation.is_internally_consistent() {
        return Err(StrategyArtifactTrustError::InvalidPromotionAttestation);
    }
    let candidate = promotion_attestation.candidate();
    let expected_candidate_digest = manifest.artifact_content_digest.to_string();
    if candidate.artifact_id() != manifest.artifact_id.as_str()
        || candidate.digest() != expected_candidate_digest
    {
        return Err(StrategyArtifactTrustError::PromotionCandidateMismatch {
            expected: format!(
                "{}@{}",
                manifest.artifact_id, manifest.artifact_content_digest
            ),
            actual: format!("{}@{}", candidate.artifact_id(), candidate.digest()),
        });
    }
    if promotion_attestation.evidence_artifact().is_none() {
        return Err(StrategyArtifactTrustError::MissingPromotionEvidence);
    }
    let run_provenance_digest = promotion_attestation
        .run_provenance_digest()
        .ok_or(StrategyArtifactTrustError::MissingRunProvenanceDigest)?;
    let expected_run_provenance_digest = manifest.evaluated_run_provenance_digest.to_string();
    if run_provenance_digest != expected_run_provenance_digest.as_str() {
        return Err(StrategyArtifactTrustError::RunProvenanceDigestMismatch {
            expected: manifest.evaluated_run_provenance_digest,
            actual: run_provenance_digest.to_string(),
        });
    }
    let market_scope_commitment = promotion_attestation
        .market_scope_commitment()
        .ok_or(StrategyArtifactTrustError::MissingMarketScopeCommitment)?;
    let expected_market_scope_commitment = manifest.evaluated_market_scope_commitment.to_string();
    if market_scope_commitment != expected_market_scope_commitment.as_str() {
        return Err(StrategyArtifactTrustError::MarketScopeAttestationMismatch {
            expected: manifest.evaluated_market_scope_commitment,
            actual: market_scope_commitment.to_string(),
        });
    }
    let strategy_config = promotion_attestation
        .strategy_config_artifact()
        .ok_or(StrategyArtifactTrustError::MissingStrategyConfigArtifact)?;
    let expected_config_digest = manifest.strategy_config_digest.to_string();
    if strategy_config.artifact_id() != manifest.strategy_config_artifact_id.as_str()
        || strategy_config.digest() != expected_config_digest
    {
        return Err(StrategyArtifactTrustError::StrategyConfigMismatch {
            expected: format!(
                "{}@{}",
                manifest.strategy_config_artifact_id, manifest.strategy_config_digest
            ),
            actual: format!(
                "{}@{}",
                strategy_config.artifact_id(),
                strategy_config.digest()
            ),
        });
    }
    let strategy_entrypoint = promotion_attestation
        .strategy_entrypoint()
        .ok_or(StrategyArtifactTrustError::MissingStrategyEntrypoint)?;
    if strategy_entrypoint != manifest.entrypoint.as_str() {
        return Err(StrategyArtifactTrustError::StrategyEntrypointMismatch {
            expected: manifest.entrypoint.clone(),
            actual: strategy_entrypoint.to_string(),
        });
    }
    let strategy_runtime_id = promotion_attestation
        .strategy_runtime_id()
        .ok_or(StrategyArtifactTrustError::MissingStrategyRuntimeId)?;
    if strategy_runtime_id != manifest.strategy_id.as_str() {
        return Err(StrategyArtifactTrustError::StrategyRuntimeIdMismatch {
            expected: manifest.strategy_id.clone(),
            actual: strategy_runtime_id.to_string(),
        });
    }
    let attestation_scope = promotion_attestation.promotion_scope().unwrap_or_default();
    if attestation_scope != policy.expected_deployment_scope {
        return Err(StrategyArtifactTrustError::DeploymentScopeMismatch {
            expected: policy.expected_deployment_scope.clone(),
            actual: attestation_scope.to_string(),
        });
    }
    validate_promotion_suite(manifest, promotion_attestation, policy)
}

fn validate_promotion_suite(
    manifest: &StrategyArtifactManifest,
    promotion_attestation: &PromotionAttestation,
    policy: &TrustPolicy,
) -> Result<(), StrategyArtifactTrustError> {
    if promotion_attestation.suite_id() != policy.expected_promotion_suite_id.as_str()
        || promotion_attestation.suite_version() != policy.expected_promotion_suite_version.as_str()
    {
        return Err(StrategyArtifactTrustError::PromotionSuiteMismatch {
            expected: format!(
                "{}@{}",
                policy.expected_promotion_suite_id, policy.expected_promotion_suite_version
            ),
            actual: format!(
                "{}@{}",
                promotion_attestation.suite_id(),
                promotion_attestation.suite_version()
            ),
        });
    }
    let suite_definition_digest =
        ArtifactDigest(*promotion_attestation.suite_definition_digest().as_bytes());
    if suite_definition_digest != manifest.promotion_suite_definition_digest
        || suite_definition_digest != policy.expected_promotion_suite_definition_digest
    {
        return Err(
            StrategyArtifactTrustError::PromotionSuiteDefinitionMismatch {
                expected: policy.expected_promotion_suite_definition_digest,
                actual: suite_definition_digest,
            },
        );
    }
    Ok(())
}

fn validate_trust_policy_shape(policy: &TrustPolicy) -> Result<(), StrategyArtifactTrustError> {
    if policy.policy_epoch == 0 {
        return Err(StrategyArtifactTrustError::InvalidPolicyEpoch);
    }

    validate_policy_collection_size(
        "trusted_signing_keys",
        policy.trusted_signing_keys.len(),
        MAX_TRUSTED_KEYS_PER_ROLE,
    )?;
    validate_policy_collection_size(
        "revoked_signing_key_ids",
        policy.revoked_signing_key_ids.len(),
        MAX_REVOKED_KEY_IDS_PER_ROLE,
    )?;
    validate_policy_collection_size(
        "trusted_evaluator_keys",
        policy.trusted_evaluator_keys.len(),
        MAX_TRUSTED_KEYS_PER_ROLE,
    )?;
    validate_policy_collection_size(
        "revoked_evaluator_key_ids",
        policy.revoked_evaluator_key_ids.len(),
        MAX_REVOKED_KEY_IDS_PER_ROLE,
    )?;

    for (field, value, maximum) in [
        (
            "expected_target_triple",
            policy.expected_target_triple.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "expected_nautilus_version",
            policy.expected_nautilus_version.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "expected_precision_mode",
            policy.expected_precision_mode.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "expected_deployment_scope",
            policy.expected_deployment_scope.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "expected_promotion_suite_id",
            policy.expected_promotion_suite_id.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "expected_promotion_suite_version",
            policy.expected_promotion_suite_version.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
    ] {
        validate_policy_text(field, value, maximum)?;
    }

    validate_policy_key_set(
        "signer",
        "trusted_signing_keys.key_id",
        "revoked_signing_key_ids",
        &policy.trusted_signing_keys,
        &policy.revoked_signing_key_ids,
    )?;
    validate_policy_key_set(
        "evaluator",
        "trusted_evaluator_keys.key_id",
        "revoked_evaluator_key_ids",
        &policy.trusted_evaluator_keys,
        &policy.revoked_evaluator_key_ids,
    )?;

    for key_id in policy
        .trusted_signing_keys
        .keys()
        .chain(&policy.revoked_signing_key_ids)
    {
        if policy.trusted_evaluator_keys.contains_key(key_id)
            || policy.revoked_evaluator_key_ids.contains(key_id)
        {
            return Err(StrategyArtifactTrustError::TrustRoleKeyIdConflict {
                key_id: key_id.clone(),
            });
        }
    }

    for (signer_key_id, signer_key) in &policy.trusted_signing_keys {
        for (evaluator_key_id, evaluator_key) in &policy.trusted_evaluator_keys {
            if signer_key == evaluator_key {
                return Err(StrategyArtifactTrustError::TrustRolePublicKeyConflict {
                    signer_key_id: signer_key_id.clone(),
                    evaluator_key_id: evaluator_key_id.clone(),
                });
            }
        }
    }

    Ok(())
}

fn validate_policy_collection_size(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), StrategyArtifactTrustError> {
    if actual > maximum {
        return Err(StrategyArtifactTrustError::PolicyCollectionTooLarge {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn validate_policy_key_set(
    role: &'static str,
    trusted_field: &'static str,
    revoked_field: &'static str,
    trusted: &BTreeMap<String, Vec<u8>>,
    revoked: &BTreeSet<String>,
) -> Result<(), StrategyArtifactTrustError> {
    for (key_id, public_key) in trusted {
        validate_policy_text(trusted_field, key_id, MAX_KEY_ID_LENGTH)?;
        if public_key.len() != ED25519_PUBLIC_KEY_LENGTH {
            return Err(StrategyArtifactTrustError::InvalidPolicyPublicKeyLength {
                role,
                key_id: key_id.clone(),
                expected: ED25519_PUBLIC_KEY_LENGTH,
                actual: public_key.len(),
            });
        }
        if revoked.contains(key_id) {
            return Err(StrategyArtifactTrustError::TrustedKeyIsRevoked {
                role,
                key_id: key_id.clone(),
            });
        }
    }
    for key_id in revoked {
        validate_policy_text(revoked_field, key_id, MAX_KEY_ID_LENGTH)?;
    }

    for (first_index, (first_key_id, first_key)) in trusted.iter().enumerate() {
        for (second_key_id, second_key) in trusted.iter().skip(first_index + 1) {
            if first_key == second_key {
                return Err(StrategyArtifactTrustError::DuplicatePolicyPublicKey {
                    role,
                    first_key_id: first_key_id.clone(),
                    second_key_id: second_key_id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_policy_text(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), StrategyArtifactTrustError> {
    if value.is_empty() {
        return Err(StrategyArtifactTrustError::EmptyTrustPolicyField { field });
    }
    validate_bounded_canonical_text(field, value, maximum)
}

fn validate_bounded_canonical_text(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), StrategyArtifactTrustError> {
    if value.len() > maximum {
        return Err(StrategyArtifactTrustError::FieldTooLong {
            field,
            maximum,
            actual: value.len(),
        });
    }
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(StrategyArtifactTrustError::NonCanonicalTextField { field });
    }
    Ok(())
}

fn validate_signed_promotion_attestation_shape(
    signed_attestation: &SignedPromotionAttestation,
) -> Result<(), StrategyArtifactTrustError> {
    validate_bounded_canonical_text(
        "promotion_attestation.evaluator_key_id",
        signed_attestation.evaluator_key_id(),
        MAX_KEY_ID_LENGTH,
    )?;
    let attestation = signed_attestation.attestation();
    for (field, value, maximum) in [
        (
            "promotion_attestation.suite_id",
            attestation.suite_id(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "promotion_attestation.suite_version",
            attestation.suite_version(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "promotion_attestation.candidate.artifact_id",
            attestation.candidate().artifact_id(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "promotion_attestation.candidate.digest",
            attestation.candidate().digest(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
    ] {
        validate_bounded_canonical_text(field, value, maximum)?;
    }
    validate_optional_artifact_reference(
        "promotion_attestation.evidence_artifact",
        attestation.evidence_artifact(),
    )?;
    validate_optional_text(
        "promotion_attestation.run_provenance_digest",
        attestation.run_provenance_digest(),
        MAX_COMPATIBILITY_FIELD_LENGTH,
    )?;
    validate_optional_text(
        "promotion_attestation.market_scope_commitment",
        attestation.market_scope_commitment(),
        MAX_COMPATIBILITY_FIELD_LENGTH,
    )?;
    validate_optional_artifact_reference(
        "promotion_attestation.strategy_config_artifact",
        attestation.strategy_config_artifact(),
    )?;
    validate_optional_text(
        "promotion_attestation.strategy_entrypoint",
        attestation.strategy_entrypoint(),
        MAX_IDENTITY_FIELD_LENGTH,
    )?;
    validate_optional_text(
        "promotion_attestation.strategy_runtime_id",
        attestation.strategy_runtime_id(),
        MAX_IDENTITY_FIELD_LENGTH,
    )?;
    validate_optional_artifact_reference("promotion_attestation.baseline", attestation.baseline())?;
    validate_optional_text(
        "promotion_attestation.promotion_scope",
        attestation.promotion_scope(),
        MAX_IDENTITY_FIELD_LENGTH,
    )
}

fn validate_optional_artifact_reference(
    field: &'static str,
    reference: Option<&nautilus_analysis::ArtifactReference>,
) -> Result<(), StrategyArtifactTrustError> {
    if let Some(reference) = reference {
        validate_bounded_canonical_text(field, reference.artifact_id(), MAX_IDENTITY_FIELD_LENGTH)?;
        validate_bounded_canonical_text(field, reference.digest(), MAX_COMPATIBILITY_FIELD_LENGTH)?;
    }
    Ok(())
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    maximum: usize,
) -> Result<(), StrategyArtifactTrustError> {
    if let Some(value) = value {
        validate_bounded_canonical_text(field, value, maximum)?;
    }
    Ok(())
}

fn validate_manifest_shape(
    manifest: &StrategyArtifactManifest,
) -> Result<(), StrategyArtifactTrustError> {
    if manifest.schema_version != STRATEGY_ARTIFACT_MANIFEST_VERSION {
        return Err(StrategyArtifactTrustError::UnsupportedManifestSchema {
            expected: STRATEGY_ARTIFACT_MANIFEST_VERSION,
            actual: manifest.schema_version,
        });
    }

    for (field, value, maximum) in [
        (
            "artifact_id",
            manifest.artifact_id.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "strategy_config_artifact_id",
            manifest.strategy_config_artifact_id.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "entrypoint",
            manifest.entrypoint.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "strategy_id",
            manifest.strategy_id.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "strategy_version",
            manifest.strategy_version.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "target_triple",
            manifest.target_triple.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "nautilus_version",
            manifest.nautilus_version.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "precision_mode",
            manifest.precision_mode.as_str(),
            MAX_COMPATIBILITY_FIELD_LENGTH,
        ),
        (
            "promotion_evaluator_key_id",
            manifest.promotion_evaluator_key_id.as_str(),
            MAX_KEY_ID_LENGTH,
        ),
        (
            "deployment_scope",
            manifest.deployment_scope.as_str(),
            MAX_IDENTITY_FIELD_LENGTH,
        ),
        (
            "signer_key_id",
            manifest.signer_key_id.as_str(),
            MAX_KEY_ID_LENGTH,
        ),
    ] {
        if value.is_empty() {
            return Err(StrategyArtifactTrustError::EmptyManifestField { field });
        }
        validate_bounded_canonical_text(field, value, maximum)?;
    }
    if manifest.signer_key_id == manifest.promotion_evaluator_key_id {
        return Err(StrategyArtifactTrustError::ManifestTrustRoleConflict {
            key_id: manifest.signer_key_id.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signer, SigningKey};
    use nautilus_analysis::{
        ArtifactReference, AttestationError, EvaluationGate, EvaluationSuite, Observations,
        SignedAttestationError,
    };
    use nautilus_core::UnixNanos;
    use rstest::rstest;

    use super::*;

    const ARTIFACT: &[u8] = b"native strategy artifact bytes";
    const SIGNING_KEY_BYTES: [u8; 32] = [7; 32];
    const EVALUATOR_KEY_BYTES: [u8; 32] = [11; 32];
    const EVALUATOR_KEY_ID: &str = "promotion-evaluator-2026-01";

    fn digest(label: &[u8]) -> ArtifactDigest {
        artifact_content_digest(label)
    }

    fn distinct_public_keys(count: usize, prefix: &str) -> BTreeMap<String, Vec<u8>> {
        (0..count)
            .map(|index| {
                let mut public_key = vec![0; ED25519_PUBLIC_KEY_LENGTH];
                public_key[..8].copy_from_slice(&(index as u64).to_be_bytes());
                (format!("{prefix}-{index}"), public_key)
            })
            .collect()
    }

    fn distinct_key_ids(count: usize, prefix: &str) -> BTreeSet<String> {
        (0..count)
            .map(|index| format!("{prefix}-{index}"))
            .collect()
    }

    fn manifest() -> StrategyArtifactManifest {
        StrategyArtifactManifest {
            schema_version: STRATEGY_ARTIFACT_MANIFEST_VERSION,
            artifact_content_digest: artifact_content_digest(ARTIFACT),
            artifact_id: "strategy:momentum-es:2.1.0".to_string(),
            runtime_artifact: StrategyRuntimeArtifact::HostExecutable,
            entrypoint: "weilan::strategies::MomentumEs".to_string(),
            strategy_id: "momentum-es".to_string(),
            strategy_version: "2.1.0".to_string(),
            target_triple: "x86_64-unknown-linux-gnu".to_string(),
            nautilus_abi_version: 1,
            nautilus_version: "0.60.0".to_string(),
            precision_mode: "high-precision".to_string(),
            policy_digest: digest(b"policy-v3"),
            promotion_evaluator_key_id: EVALUATOR_KEY_ID.to_string(),
            promotion_suite_definition_digest: digest(b"promotion-suite"),
            promotion_attestation_digest: digest(b"promotion-42"),
            evaluated_run_provenance_digest: digest(b"run-provenance-42"),
            evaluated_market_scope_commitment: digest(b"market-scope-okx"),
            strategy_config_artifact_id: "config:momentum-es:2.1.0".to_string(),
            strategy_config_digest: digest(b"momentum-es-config"),
            deployment_scope: "okx-production-a".to_string(),
            signer_key_id: "release-2026-01".to_string(),
        }
    }

    fn policy(attestation: &SignedPromotionAttestation) -> TrustPolicy {
        let signing_key = SigningKey::from_bytes(&SIGNING_KEY_BYTES);
        let evaluator_key = SigningKey::from_bytes(&EVALUATOR_KEY_BYTES);
        let claims = attestation.attestation();
        TrustPolicy {
            policy_epoch: 7,
            trusted_signing_keys: BTreeMap::from([(
                "release-2026-01".to_string(),
                signing_key.verifying_key().to_bytes().to_vec(),
            )]),
            revoked_signing_key_ids: BTreeSet::new(),
            trusted_evaluator_keys: BTreeMap::from([(
                EVALUATOR_KEY_ID.to_string(),
                evaluator_key.verifying_key().to_bytes().to_vec(),
            )]),
            revoked_evaluator_key_ids: BTreeSet::new(),
            expected_target_triple: "x86_64-unknown-linux-gnu".to_string(),
            expected_nautilus_abi_version: 1,
            expected_nautilus_version: "0.60.0".to_string(),
            expected_precision_mode: "high-precision".to_string(),
            expected_policy_digest: digest(b"policy-v3"),
            expected_promotion_attestation_digest: ArtifactDigest(
                *attestation.envelope_digest().as_bytes(),
            ),
            expected_market_scope_commitment: ArtifactDigest(
                *claims
                    .market_scope_commitment()
                    .unwrap()
                    .parse::<ContentDigest>()
                    .unwrap()
                    .as_bytes(),
            ),
            expected_deployment_scope: "okx-production-a".to_string(),
            expected_promotion_suite_id: "release-promotion".to_string(),
            expected_promotion_suite_version: "1".to_string(),
            expected_promotion_suite_definition_digest: ArtifactDigest(
                *claims.suite_definition_digest().as_bytes(),
            ),
        }
    }

    fn promotion_suite(minimum: f64) -> EvaluationSuite {
        EvaluationSuite::new(
            "release-promotion",
            "1",
            vec![EvaluationGate::Minimum {
                metric: "score".to_string(),
                minimum,
            }],
        )
        .unwrap()
    }

    fn promotion_attestation_for_suite(
        manifest: &StrategyArtifactManifest,
        suite: &EvaluationSuite,
    ) -> SignedPromotionAttestation {
        let outcome = suite.evaluate(&Observations::from([("score".to_string(), 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new(
                manifest.artifact_id.clone(),
                manifest.artifact_content_digest.to_string(),
            )
            .unwrap(),
            ArtifactReference::new("backtest-result:run-42", digest(b"run-42").to_string())
                .unwrap(),
            manifest.evaluated_run_provenance_digest.to_string(),
            manifest.evaluated_market_scope_commitment.to_string(),
            ArtifactReference::new(
                manifest.strategy_config_artifact_id.clone(),
                manifest.strategy_config_digest.to_string(),
            )
            .unwrap(),
            manifest.entrypoint.clone(),
            manifest.strategy_id.clone(),
            None,
            manifest.deployment_scope.clone(),
            UnixNanos::new(42),
        )
        .unwrap();
        sign_promotion_attestation(attestation)
    }

    fn promotion_attestation(manifest: &StrategyArtifactManifest) -> SignedPromotionAttestation {
        promotion_attestation_for_suite(manifest, &promotion_suite(1.0))
    }

    fn sign_promotion_attestation(attestation: PromotionAttestation) -> SignedPromotionAttestation {
        sign_promotion_attestation_with(attestation, EVALUATOR_KEY_ID, &EVALUATOR_KEY_BYTES)
    }

    fn sign_promotion_attestation_with(
        attestation: PromotionAttestation,
        evaluator_key_id: &str,
        key_bytes: &[u8; 32],
    ) -> SignedPromotionAttestation {
        let signing_key = SigningKey::from_bytes(key_bytes);
        let payload = attestation
            .evaluator_signing_payload(evaluator_key_id)
            .unwrap();
        let signature = signing_key.sign(&payload).to_bytes();
        attestation
            .into_signed(evaluator_key_id, signature)
            .unwrap()
    }

    fn bind_promotion(
        manifest: &mut StrategyArtifactManifest,
        attestation: &SignedPromotionAttestation,
    ) {
        manifest.promotion_evaluator_key_id = attestation.evaluator_key_id().to_string();
        manifest.promotion_suite_definition_digest = ArtifactDigest(
            *attestation
                .attestation()
                .suite_definition_digest()
                .as_bytes(),
        );
        manifest.promotion_attestation_digest =
            ArtifactDigest(*attestation.envelope_digest().as_bytes());
    }

    fn approved_manifest() -> (StrategyArtifactManifest, SignedPromotionAttestation) {
        let mut manifest = manifest();
        let attestation = promotion_attestation(&manifest);
        bind_promotion(&mut manifest, &attestation);
        (manifest, attestation)
    }

    fn approved_reference() -> (
        StrategyArtifactReference,
        SignedPromotionAttestation,
        TrustPolicy,
    ) {
        let (manifest, attestation) = approved_manifest();
        let policy = policy(&attestation);
        (signed_reference(manifest), attestation, policy)
    }

    fn signed_reference(manifest: StrategyArtifactManifest) -> StrategyArtifactReference {
        let signing_key = SigningKey::from_bytes(&SIGNING_KEY_BYTES);
        let signature = signing_key.sign(&manifest.signing_payload()).to_bytes();
        StrategyArtifactReference {
            manifest,
            signature: DetachedSignature::new(signature),
        }
    }

    #[rstest]
    fn verifies_fully_matching_artifact() {
        let (reference, attestation, policy) = approved_reference();

        let verified = reference.verify(ARTIFACT, &attestation, &policy).unwrap();

        assert_eq!(verified.reference(), &reference);
        assert_eq!(verified.content_digest(), artifact_content_digest(ARTIFACT));
        assert_eq!(verified.trust_policy_commitment(), policy.commitment());
        assert_eq!(verified.trust_policy_epoch(), policy.policy_epoch);
        assert_eq!(
            reference.manifest.evaluated_market_scope_commitment,
            policy.expected_market_scope_commitment
        );
        let expected_market_scope_commitment = reference
            .manifest
            .evaluated_market_scope_commitment
            .to_string();
        assert_eq!(
            attestation.attestation().market_scope_commitment(),
            Some(expected_market_scope_commitment.as_str())
        );
    }

    #[rstest]
    fn signing_payload_is_deterministic_and_domain_separated() {
        let manifest = manifest();

        assert_eq!(manifest.signing_payload(), manifest.signing_payload());
        assert_ne!(
            manifest.signing_payload(),
            *blake3::hash(ARTIFACT).as_bytes()
        );
    }

    #[rstest]
    fn artifact_digest_matches_native_provenance_contract() {
        assert_eq!(
            artifact_content_digest(ARTIFACT).to_string(),
            ContentDigest::digest_bytes(ARTIFACT).to_string()
        );
    }

    #[rstest]
    fn trust_policy_commitment_is_deterministic_and_binds_authority() {
        let (_, attestation, policy) = approved_reference();
        let mut broadened = policy.clone();
        broadened
            .trusted_signing_keys
            .insert("attacker-controlled-key".to_string(), vec![9; 32]);
        let mut other_scope = policy.clone();
        other_scope.expected_deployment_scope.push_str("-other");
        let mut other_suite_definition = policy.clone();
        other_suite_definition
            .expected_promotion_suite_definition_digest
            .0[0] ^= 1;
        let mut other_evaluator = policy.clone();
        other_evaluator
            .trusted_evaluator_keys
            .insert("promotion-evaluator-rotated".to_string(), vec![13; 32]);
        let mut revoked_evaluator = policy.clone();
        revoked_evaluator
            .revoked_evaluator_key_ids
            .insert(EVALUATOR_KEY_ID.to_string());
        let mut newer_epoch = policy.clone();
        newer_epoch.policy_epoch += 1;
        let mut other_market_scope = policy.clone();
        other_market_scope.expected_market_scope_commitment.0[0] ^= 1;

        assert_eq!(policy.commitment(), policy.commitment());
        assert_ne!(policy.commitment(), broadened.commitment());
        assert_ne!(policy.commitment(), other_scope.commitment());
        assert_ne!(policy.commitment(), other_suite_definition.commitment());
        assert_ne!(policy.commitment(), other_evaluator.commitment());
        assert_ne!(policy.commitment(), revoked_evaluator.commitment());
        assert_ne!(policy.commitment(), newer_epoch.commitment());
        assert_ne!(policy.commitment(), other_market_scope.commitment());
        assert_eq!(
            policy.expected_promotion_attestation_digest,
            ArtifactDigest(*attestation.envelope_digest().as_bytes())
        );
    }

    #[rstest]
    fn rejects_zero_policy_epoch_before_minting_capability() {
        let (reference, attestation, mut policy) = approved_reference();
        policy.policy_epoch = 0;

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::InvalidPolicyEpoch)
        ));
    }

    #[rstest]
    fn validated_commitment_matches_compatible_hash_for_valid_policy() {
        let (_, _, policy) = approved_reference();

        assert_eq!(policy.validated_commitment().unwrap(), policy.commitment());
    }

    #[rstest]
    fn rejects_every_empty_expected_policy_string() {
        let (_, _, policy) = approved_reference();
        let mut invalid_policies = Vec::new();

        let mut invalid = policy.clone();
        invalid.expected_target_triple.clear();
        invalid_policies.push(("expected_target_triple", invalid));
        let mut invalid = policy.clone();
        invalid.expected_nautilus_version.clear();
        invalid_policies.push(("expected_nautilus_version", invalid));
        let mut invalid = policy.clone();
        invalid.expected_precision_mode.clear();
        invalid_policies.push(("expected_precision_mode", invalid));
        let mut invalid = policy.clone();
        invalid.expected_deployment_scope.clear();
        invalid_policies.push(("expected_deployment_scope", invalid));
        let mut invalid = policy.clone();
        invalid.expected_promotion_suite_id.clear();
        invalid_policies.push(("expected_promotion_suite_id", invalid));
        let mut invalid = policy;
        invalid.expected_promotion_suite_version.clear();
        invalid_policies.push(("expected_promotion_suite_version", invalid));

        for (expected_field, invalid) in invalid_policies {
            assert!(matches!(
                invalid.validate(),
                Err(StrategyArtifactTrustError::EmptyTrustPolicyField { field })
                    if field == expected_field
            ));
            assert!(invalid.validated_commitment().is_err());
        }
    }

    #[rstest]
    fn rejects_oversized_and_noncanonical_policy_text() {
        let (_, _, mut oversized) = approved_reference();
        oversized.expected_deployment_scope = "x".repeat(MAX_IDENTITY_FIELD_LENGTH + 1);
        assert!(matches!(
            oversized.validate(),
            Err(StrategyArtifactTrustError::FieldTooLong {
                field: "expected_deployment_scope",
                ..
            })
        ));

        let (_, _, mut noncanonical) = approved_reference();
        noncanonical.expected_promotion_suite_id = " release-promotion".to_string();
        assert!(matches!(
            noncanonical.validate(),
            Err(StrategyArtifactTrustError::NonCanonicalTextField {
                field: "expected_promotion_suite_id"
            })
        ));
    }

    #[rstest]
    #[case(31)]
    #[case(33)]
    fn rejects_non_ed25519_signing_key_lengths(#[case] key_length: usize) {
        let (_, _, mut policy) = approved_reference();
        policy
            .trusted_signing_keys
            .insert("release-2026-01".to_string(), vec![7; key_length]);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::InvalidPolicyPublicKeyLength {
                role: "signer",
                actual,
                ..
            }) if actual == key_length
        ));
    }

    #[rstest]
    #[case(31)]
    #[case(33)]
    fn rejects_non_ed25519_evaluator_key_lengths(#[case] key_length: usize) {
        let (_, _, mut policy) = approved_reference();
        policy
            .trusted_evaluator_keys
            .insert(EVALUATOR_KEY_ID.to_string(), vec![11; key_length]);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::InvalidPolicyPublicKeyLength {
                role: "evaluator",
                actual,
                ..
            }) if actual == key_length
        ));
    }

    #[rstest]
    fn rejects_trusted_and_revoked_signer_conflict() {
        let (reference, _, mut policy) = approved_reference();
        policy
            .revoked_signing_key_ids
            .insert(reference.manifest.signer_key_id);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::TrustedKeyIsRevoked { role: "signer", .. })
        ));
    }

    #[rstest]
    fn rejects_trusted_and_revoked_evaluator_conflict() {
        let (_, _, mut policy) = approved_reference();
        policy
            .revoked_evaluator_key_ids
            .insert(EVALUATOR_KEY_ID.to_string());

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::TrustedKeyIsRevoked {
                role: "evaluator",
                ..
            })
        ));
    }

    #[rstest]
    fn rejects_signer_evaluator_key_id_overlap() {
        let (_, _, mut policy) = approved_reference();
        policy
            .trusted_evaluator_keys
            .insert("release-2026-01".to_string(), vec![19; 32]);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::TrustRoleKeyIdConflict { key_id })
                if key_id == "release-2026-01"
        ));
    }

    #[rstest]
    fn rejects_signer_evaluator_public_key_overlap() {
        let (_, _, mut policy) = approved_reference();
        let signer_public_key = policy.trusted_signing_keys["release-2026-01"].clone();
        policy
            .trusted_evaluator_keys
            .insert(EVALUATOR_KEY_ID.to_string(), signer_public_key);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::TrustRolePublicKeyConflict { .. })
        ));
    }

    #[rstest]
    fn rejects_public_key_aliases_within_one_role() {
        let (_, _, mut policy) = approved_reference();
        let signer_public_key = policy.trusted_signing_keys["release-2026-01"].clone();
        policy
            .trusted_signing_keys
            .insert("release-alias".to_string(), signer_public_key);

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::DuplicatePolicyPublicKey { role: "signer", .. })
        ));
    }

    #[rstest]
    fn rejects_oversized_policy_key_collections_before_key_iteration() {
        let (_, _, mut policy) = approved_reference();
        policy.trusted_signing_keys =
            distinct_public_keys(MAX_TRUSTED_KEYS_PER_ROLE + 1, "release");

        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::PolicyCollectionTooLarge {
                field: "trusted_signing_keys",
                actual,
                ..
            }) if actual == MAX_TRUSTED_KEYS_PER_ROLE + 1
        ));
    }

    #[rstest]
    fn rejects_oversized_evaluator_and_revocation_collections() {
        let (_, _, mut policy) = approved_reference();
        policy.trusted_evaluator_keys =
            distinct_public_keys(MAX_TRUSTED_KEYS_PER_ROLE + 1, "evaluator");
        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::PolicyCollectionTooLarge {
                field: "trusted_evaluator_keys",
                ..
            })
        ));

        let (_, _, mut policy) = approved_reference();
        policy.revoked_signing_key_ids =
            distinct_key_ids(MAX_REVOKED_KEY_IDS_PER_ROLE + 1, "revoked-signer");
        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::PolicyCollectionTooLarge {
                field: "revoked_signing_key_ids",
                ..
            })
        ));

        let (_, _, mut policy) = approved_reference();
        policy.revoked_evaluator_key_ids =
            distinct_key_ids(MAX_REVOKED_KEY_IDS_PER_ROLE + 1, "revoked-evaluator");
        assert!(matches!(
            policy.validate(),
            Err(StrategyArtifactTrustError::PolicyCollectionTooLarge {
                field: "revoked_evaluator_key_ids",
                ..
            })
        ));
    }

    #[rstest]
    fn verification_rejects_malformed_policy_before_artifact_crypto() {
        let (mut reference, attestation, mut policy) = approved_reference();
        reference.signature = DetachedSignature::new(Vec::new());
        policy.expected_target_triple.clear();

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::EmptyTrustPolicyField {
                field: "expected_target_triple"
            })
        ));
    }

    #[rstest]
    fn rejects_manifest_role_overlap_and_oversized_fields() {
        let (mut reference, attestation, policy) = approved_reference();
        reference.manifest.signer_key_id = EVALUATOR_KEY_ID.to_string();
        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::ManifestTrustRoleConflict { .. })
        ));

        let (mut reference, attestation, policy) = approved_reference();
        reference.manifest.entrypoint = "x".repeat(MAX_IDENTITY_FIELD_LENGTH + 1);
        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::FieldTooLong {
                field: "entrypoint",
                ..
            })
        ));
    }

    #[rstest]
    fn rejects_malformed_reference_signature_before_verification() {
        let (mut reference, attestation, policy) = approved_reference();
        reference.signature = DetachedSignature::new(vec![0; ED25519_SIGNATURE_LENGTH - 1]);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::InvalidDetachedSignatureLength {
                actual,
                ..
            }) if actual == ED25519_SIGNATURE_LENGTH - 1
        ));
    }

    #[rstest]
    fn deserialization_rejects_invalid_policy_manifest_and_signature_shapes() {
        let (_, _, valid_policy) = approved_reference();
        let valid_policy_json = serde_json::to_vec(&valid_policy).unwrap();
        let decoded: TrustPolicy = serde_json::from_slice(&valid_policy_json).unwrap();
        assert_eq!(decoded, valid_policy);

        let mut invalid_policy = valid_policy;
        invalid_policy.expected_precision_mode.clear();
        let invalid_policy_json = serde_json::to_vec(&invalid_policy).unwrap();
        assert!(serde_json::from_slice::<TrustPolicy>(&invalid_policy_json).is_err());

        let mut invalid_manifest = manifest();
        invalid_manifest.signer_key_id = invalid_manifest.promotion_evaluator_key_id.clone();
        let invalid_manifest_json = serde_json::to_vec(&invalid_manifest).unwrap();
        assert!(
            serde_json::from_slice::<StrategyArtifactManifest>(&invalid_manifest_json).is_err()
        );

        let mut oversized_manifest = manifest();
        oversized_manifest.entrypoint = "x".repeat(MAX_IDENTITY_FIELD_LENGTH + 1);
        let oversized_manifest_json = serde_json::to_vec(&oversized_manifest).unwrap();
        assert!(
            serde_json::from_slice::<StrategyArtifactManifest>(&oversized_manifest_json).is_err()
        );

        let mut missing_manifest_scope = serde_json::to_value(manifest()).unwrap();
        missing_manifest_scope
            .as_object_mut()
            .unwrap()
            .remove("evaluated_market_scope_commitment");
        assert!(
            serde_json::from_value::<StrategyArtifactManifest>(missing_manifest_scope).is_err()
        );

        let (_, _, policy) = approved_reference();
        let mut missing_policy_scope = serde_json::to_value(policy).unwrap();
        missing_policy_scope
            .as_object_mut()
            .unwrap()
            .remove("expected_market_scope_commitment");
        assert!(serde_json::from_value::<TrustPolicy>(missing_policy_scope).is_err());

        let (_, _, policy) = approved_reference();
        let mut malformed_policy_scope = serde_json::to_value(policy).unwrap();
        malformed_policy_scope["expected_market_scope_commitment"] =
            serde_json::json!(vec![0_u8; 33]);
        assert!(serde_json::from_value::<TrustPolicy>(malformed_policy_scope).is_err());

        let mut malformed_scope_digest = serde_json::to_value(manifest()).unwrap();
        malformed_scope_digest["evaluated_market_scope_commitment"] =
            serde_json::json!(vec![0_u8; 31]);
        assert!(
            serde_json::from_value::<StrategyArtifactManifest>(malformed_scope_digest).is_err()
        );

        let mut legacy_manifest = serde_json::to_value(manifest()).unwrap();
        legacy_manifest["schema_version"] =
            serde_json::json!(STRATEGY_ARTIFACT_MANIFEST_VERSION - 1);
        assert!(serde_json::from_value::<StrategyArtifactManifest>(legacy_manifest).is_err());

        let mut unknown_manifest_field = serde_json::to_value(manifest()).unwrap();
        unknown_manifest_field["market_support"] = serde_json::json!("all-markets");
        assert!(
            serde_json::from_value::<StrategyArtifactManifest>(unknown_manifest_field).is_err()
        );

        assert!(serde_json::from_str::<DetachedSignature>("[0]").is_err());
    }

    #[rstest]
    fn deserialization_enforces_policy_limits_while_reading_json() {
        let (_, _, mut oversized_collection) = approved_reference();
        oversized_collection.trusted_signing_keys =
            distinct_public_keys(MAX_TRUSTED_KEYS_PER_ROLE + 1, "release");
        let json = serde_json::to_vec(&oversized_collection).unwrap();
        assert!(serde_json::from_slice::<TrustPolicy>(&json).is_err());

        let (_, _, mut oversized_key_id) = approved_reference();
        let public_key = oversized_key_id
            .trusted_signing_keys
            .remove("release-2026-01")
            .unwrap();
        oversized_key_id
            .trusted_signing_keys
            .insert("k".repeat(MAX_KEY_ID_LENGTH + 1), public_key);
        let json = serde_json::to_vec(&oversized_key_id).unwrap();
        assert!(serde_json::from_slice::<TrustPolicy>(&json).is_err());

        let (_, _, mut oversized_public_key) = approved_reference();
        oversized_public_key
            .trusted_signing_keys
            .insert("release-2026-01".to_string(), vec![7; 1_024]);
        let json = serde_json::to_vec(&oversized_public_key).unwrap();
        assert!(serde_json::from_slice::<TrustPolicy>(&json).is_err());

        let (_, _, mut oversized_text) = approved_reference();
        oversized_text.expected_deployment_scope = "x".repeat(MAX_IDENTITY_FIELD_LENGTH + 1);
        let json = serde_json::to_vec(&oversized_text).unwrap();
        assert!(serde_json::from_slice::<TrustPolicy>(&json).is_err());

        let signature_json = format!(
            "[{}]",
            std::iter::repeat_n("0", ED25519_SIGNATURE_LENGTH + 1)
                .collect::<Vec<_>>()
                .join(",")
        );
        assert!(serde_json::from_str::<DetachedSignature>(&signature_json).is_err());

        let (_, _, policy) = approved_reference();
        let valid_json = serde_json::to_string(&policy).unwrap();
        let trusted_map_json = serde_json::to_string(&policy.trusted_signing_keys).unwrap();
        let public_key_json =
            serde_json::to_string(&policy.trusted_signing_keys["release-2026-01"]).unwrap();
        let duplicate_map_json = format!(
            "{{\"release-2026-01\":{public_key_json},\"release-2026-01\":{public_key_json}}}"
        );
        let duplicate_key_json = valid_json.replacen(&trusted_map_json, &duplicate_map_json, 1);
        assert!(serde_json::from_str::<TrustPolicy>(&duplicate_key_json).is_err());
    }

    #[rstest]
    fn signature_binds_every_manifest_field() {
        let reference = signed_reference(manifest());
        let original_payload = reference.manifest.signing_payload();
        let mut mutations = Vec::new();

        let mut value = reference.manifest.clone();
        value.schema_version += 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.artifact_content_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.artifact_id.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.runtime_artifact = StrategyRuntimeArtifact::PythonSource;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.entrypoint.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.strategy_id.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.strategy_version.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.target_triple.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.nautilus_abi_version += 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.nautilus_version.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.precision_mode.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.policy_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.promotion_evaluator_key_id.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.promotion_suite_definition_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.promotion_attestation_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.evaluated_run_provenance_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.evaluated_market_scope_commitment.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.strategy_config_artifact_id.push('x');
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.strategy_config_digest.0[0] ^= 1;
        mutations.push(value);
        let mut value = reference.manifest.clone();
        value.deployment_scope.push('x');
        mutations.push(value);
        let mut value = reference.manifest;
        value.signer_key_id.push('x');
        mutations.push(value);

        assert!(
            mutations
                .iter()
                .all(|manifest| manifest.signing_payload() != original_payload)
        );
    }

    #[rstest]
    fn rejects_bad_signature() {
        let (mut reference, attestation, policy) = approved_reference();
        reference.signature.0[0] ^= 1;

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::InvalidSignature(_))
        ));
    }

    #[rstest]
    fn rejects_untrusted_signer() {
        let (reference, attestation, mut policy) = approved_reference();
        policy.trusted_signing_keys.clear();

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::UntrustedSigner { .. })
        ));
    }

    #[rstest]
    fn rejects_revoked_signer_even_if_trusted() {
        let (reference, attestation, mut policy) = approved_reference();
        policy
            .revoked_signing_key_ids
            .insert(reference.manifest.signer_key_id.clone());

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::TrustedKeyIsRevoked { role: "signer", .. })
        ));
    }

    #[rstest]
    fn rejects_invalid_evaluator_signature() {
        let (reference, attestation, policy) = approved_reference();
        let mut signature = attestation.signature().as_bytes().to_vec();
        signature[0] ^= 1;
        let tampered = SignedPromotionAttestation::new(
            attestation.evaluator_key_id(),
            attestation.attestation().clone(),
            signature,
        )
        .unwrap();

        assert!(matches!(
            reference.verify(ARTIFACT, &tampered, &policy),
            Err(StrategyArtifactTrustError::InvalidEvaluatorSignature(_))
        ));
    }

    #[rstest]
    fn rejects_untrusted_evaluator() {
        let (reference, attestation, mut policy) = approved_reference();
        policy.trusted_evaluator_keys.clear();

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::UntrustedEvaluator { .. })
        ));
    }

    #[rstest]
    fn rejects_revoked_evaluator_even_if_trusted() {
        let (reference, attestation, mut policy) = approved_reference();
        policy
            .revoked_evaluator_key_ids
            .insert(EVALUATOR_KEY_ID.to_string());

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::TrustedKeyIsRevoked {
                role: "evaluator",
                ..
            })
        ));
    }

    #[rstest]
    fn rejects_evaluator_identity_not_signed_into_release_manifest() {
        const OTHER_KEY_BYTES: [u8; 32] = [17; 32];
        let (reference, attestation, policy) = approved_reference();
        let other = sign_promotion_attestation_with(
            attestation.attestation().clone(),
            "promotion-evaluator-attacker",
            &OTHER_KEY_BYTES,
        );

        assert!(matches!(
            reference.verify(ARTIFACT, &other, &policy),
            Err(StrategyArtifactTrustError::PromotionEvaluatorMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_valid_but_unpinned_evaluator_envelope() {
        let (reference, _, policy) = approved_reference();
        let other = promotion_attestation_for_suite(&reference.manifest, &promotion_suite(0.5));

        assert!(matches!(
            reference.verify(ARTIFACT, &other, &policy),
            Err(StrategyArtifactTrustError::PromotionAttestationDigestMismatch { .. })
        ));
    }

    #[rstest]
    fn release_signer_cannot_substitute_an_untrusted_evaluator() {
        const ATTACKER_KEY_BYTES: [u8; 32] = [23; 32];
        let mut manifest = manifest();
        let trusted = promotion_attestation(&manifest);
        let attacker = sign_promotion_attestation_with(
            trusted.attestation().clone(),
            "attacker-controlled-evaluator",
            &ATTACKER_KEY_BYTES,
        );
        bind_promotion(&mut manifest, &attacker);
        let reference = signed_reference(manifest);
        let policy = policy(&attacker);

        assert!(matches!(
            reference.verify(ARTIFACT, &attacker, &policy),
            Err(StrategyArtifactTrustError::UntrustedEvaluator { .. })
        ));
    }

    #[rstest]
    fn rejects_content_mismatch() {
        let (reference, attestation, policy) = approved_reference();

        assert!(matches!(
            reference.verify(b"substituted artifact", &attestation, &policy),
            Err(StrategyArtifactTrustError::ContentDigestMismatch { .. })
        ));
    }

    #[rstest]
    fn refuses_to_sign_legacy_attestation_without_exact_run_evidence() {
        let manifest = manifest();
        let suite = EvaluationSuite::new(
            "release-promotion",
            "1",
            vec![EvaluationGate::Minimum {
                metric: "score".to_string(),
                minimum: 1.0,
            }],
        )
        .unwrap();
        let outcome = suite.evaluate(&Observations::from([("score".to_string(), 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome(
            &outcome,
            ArtifactReference::new(
                manifest.artifact_id.clone(),
                manifest.artifact_content_digest.to_string(),
            )
            .unwrap(),
            None,
            UnixNanos::new(42),
        )
        .unwrap();
        let error = attestation
            .into_signed(EVALUATOR_KEY_ID, [0_u8; 64])
            .unwrap_err();

        assert_eq!(error, SignedAttestationError::IncompleteReleaseAttestation);
    }

    #[rstest]
    fn rejects_attestation_for_different_release_bytes() {
        let mut manifest = manifest();
        let mut attested_manifest = manifest.clone();
        attested_manifest.artifact_content_digest = digest(b"different release");
        let attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let policy = policy(&attestation);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::PromotionCandidateMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_attestation_for_different_run_provenance() {
        let mut manifest = manifest();
        let mut attested_manifest = manifest.clone();
        attested_manifest.evaluated_run_provenance_digest = digest(b"other-run-provenance");
        let attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let policy = policy(&attestation);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::RunProvenanceDigestMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_resigned_attestation_for_different_market_scope_even_when_envelope_is_pinned() {
        let mut release_manifest = manifest();
        let expected_market_scope = release_manifest.evaluated_market_scope_commitment;
        let mut attested_manifest = release_manifest.clone();
        attested_manifest.evaluated_market_scope_commitment = digest(b"market-scope-us-smart");
        let attacker_attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut release_manifest, &attacker_attestation);
        let reference = signed_reference(release_manifest);
        let mut policy = policy(&attacker_attestation);
        policy.expected_market_scope_commitment = expected_market_scope;

        assert_eq!(
            reference.manifest.deployment_scope,
            policy.expected_deployment_scope
        );
        assert_eq!(
            policy.expected_promotion_attestation_digest,
            ArtifactDigest(*attacker_attestation.envelope_digest().as_bytes())
        );
        assert!(matches!(
            reference.verify(ARTIFACT, &attacker_attestation, &policy),
            Err(StrategyArtifactTrustError::MarketScopeAttestationMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_resigned_release_and_attestation_outside_policy_market_scope() {
        let approved_market_scope = manifest().evaluated_market_scope_commitment;
        let mut attacker_manifest = manifest();
        attacker_manifest.evaluated_market_scope_commitment = digest(b"market-scope-us-smart");
        let attacker_attestation = promotion_attestation(&attacker_manifest);
        bind_promotion(&mut attacker_manifest, &attacker_attestation);
        let reference = signed_reference(attacker_manifest);
        let mut policy = policy(&attacker_attestation);
        policy.expected_market_scope_commitment = approved_market_scope;

        assert_eq!(
            reference.manifest.deployment_scope,
            policy.expected_deployment_scope
        );
        assert!(matches!(
            reference.verify(ARTIFACT, &attacker_attestation, &policy),
            Err(StrategyArtifactTrustError::MarketScopePolicyMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_attestation_for_different_strategy_config() {
        let mut manifest = manifest();
        let mut attested_manifest = manifest.clone();
        attested_manifest.strategy_config_digest = digest(b"other-strategy-config");
        let attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let policy = policy(&attestation);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::StrategyConfigMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_attestation_for_different_strategy_entrypoint() {
        let mut manifest = manifest();
        let mut attested_manifest = manifest.clone();
        attested_manifest.entrypoint = "attacker::SubstitutedStrategy".to_string();
        let attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let policy = policy(&attestation);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::StrategyEntrypointMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_attestation_for_different_strategy_runtime_id() {
        let mut manifest = manifest();
        let mut attested_manifest = manifest.clone();
        attested_manifest.strategy_id = "substituted-runtime-id".to_string();
        let attestation = promotion_attestation(&attested_manifest);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let policy = policy(&attestation);

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::StrategyRuntimeIdMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_different_gate_definition_under_same_suite_identity() {
        let mut manifest = manifest();
        let attestation = promotion_attestation_for_suite(&manifest, &promotion_suite(0.5));
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let mut policy = policy(&attestation);
        policy.expected_promotion_suite_definition_digest =
            ArtifactDigest(*promotion_suite(1.0).canonical_digest().as_bytes());

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::PromotionSuiteDefinitionMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_delimiter_collision_in_promotion_candidate() {
        let mut manifest = manifest();
        manifest.artifact_id = "strategy@collision".to_string();
        let suite = EvaluationSuite::new(
            "release-promotion",
            "1",
            vec![EvaluationGate::Minimum {
                metric: "score".to_string(),
                minimum: 1.0,
            }],
        )
        .unwrap();
        let outcome = suite.evaluate(&Observations::from([("score".to_string(), 2.0)]), None);
        let error = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new(
                "strategy",
                format!("collision@{}", manifest.artifact_content_digest),
            )
            .unwrap(),
            ArtifactReference::new("backtest-result:run-42", digest(b"run-42").to_string())
                .unwrap(),
            manifest.evaluated_run_provenance_digest.to_string(),
            manifest.evaluated_market_scope_commitment.to_string(),
            ArtifactReference::new(
                manifest.strategy_config_artifact_id.clone(),
                manifest.strategy_config_digest.to_string(),
            )
            .unwrap(),
            manifest.entrypoint.clone(),
            manifest.strategy_id.clone(),
            None,
            manifest.deployment_scope,
            UnixNanos::new(42),
        )
        .unwrap_err();

        assert_eq!(
            error,
            AttestationError::InvalidReleaseDigest { field: "candidate" }
        );
    }

    #[rstest]
    fn rejects_delimiter_collision_in_promotion_suite() {
        let mut manifest = manifest();
        let suite = EvaluationSuite::new(
            "release-promotion",
            "1@2",
            vec![EvaluationGate::Minimum {
                metric: "score".to_string(),
                minimum: 1.0,
            }],
        )
        .unwrap();
        let outcome = suite.evaluate(&Observations::from([("score".to_string(), 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new(
                manifest.artifact_id.clone(),
                manifest.artifact_content_digest.to_string(),
            )
            .unwrap(),
            ArtifactReference::new("backtest-result:run-42", digest(b"run-42").to_string())
                .unwrap(),
            manifest.evaluated_run_provenance_digest.to_string(),
            manifest.evaluated_market_scope_commitment.to_string(),
            ArtifactReference::new(
                manifest.strategy_config_artifact_id.clone(),
                manifest.strategy_config_digest.to_string(),
            )
            .unwrap(),
            manifest.entrypoint.clone(),
            manifest.strategy_id.clone(),
            None,
            manifest.deployment_scope.clone(),
            UnixNanos::new(42),
        )
        .unwrap();
        let attestation = sign_promotion_attestation(attestation);
        bind_promotion(&mut manifest, &attestation);
        let reference = signed_reference(manifest);
        let mut policy = policy(&attestation);
        policy.expected_promotion_suite_id = "release-promotion@1".to_string();
        policy.expected_promotion_suite_version = "2".to_string();

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::PromotionSuiteMismatch { .. })
        ));
    }

    #[rstest]
    #[case("target")]
    #[case("nautilus_version")]
    #[case("precision")]
    #[case("policy")]
    #[case("promotion")]
    #[case("market_scope")]
    fn rejects_policy_mismatch(#[case] mismatch: &str) {
        let (reference, attestation, mut policy) = approved_reference();
        match mismatch {
            "target" => policy.expected_target_triple.push('x'),
            "nautilus_version" => policy.expected_nautilus_version.push('x'),
            "precision" => policy.expected_precision_mode.push('x'),
            "policy" => policy.expected_policy_digest.0[0] ^= 1,
            "promotion" => policy.expected_promotion_attestation_digest.0[0] ^= 1,
            "market_scope" => policy.expected_market_scope_commitment.0[0] ^= 1,
            _ => unreachable!(),
        }

        let error = reference
            .verify(ARTIFACT, &attestation, &policy)
            .unwrap_err();
        assert!(matches!(
            (mismatch, error),
            (
                "target",
                StrategyArtifactTrustError::TargetTripleMismatch { .. }
            ) | (
                "nautilus_version",
                StrategyArtifactTrustError::NautilusVersionMismatch { .. }
            ) | (
                "precision",
                StrategyArtifactTrustError::PrecisionModeMismatch { .. }
            ) | (
                "policy",
                StrategyArtifactTrustError::PolicyDigestMismatch { .. }
            ) | (
                "promotion",
                StrategyArtifactTrustError::PromotionAttestationDigestMismatch { .. }
            ) | (
                "market_scope",
                StrategyArtifactTrustError::MarketScopePolicyMismatch { .. }
            )
        ));
    }

    #[rstest]
    fn rejects_abi_mismatch() {
        let (reference, attestation, mut policy) = approved_reference();
        policy.expected_nautilus_abi_version += 1;

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::NautilusAbiMismatch { .. })
        ));
    }

    #[rstest]
    fn rejects_manifest_field_tampering_before_compatibility_checks() {
        let (mut reference, attestation, policy) = approved_reference();
        reference.manifest.strategy_version.push_str("-tampered");

        assert!(matches!(
            reference.verify(ARTIFACT, &attestation, &policy),
            Err(StrategyArtifactTrustError::InvalidSignature(_))
        ));
    }
}
