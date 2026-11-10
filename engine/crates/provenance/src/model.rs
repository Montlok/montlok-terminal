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

//! Strongly validated provenance values and immutable records.

use std::{collections::BTreeSet, fmt::Display, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{ProvenanceError, hash::CanonicalHasher};

const CONTENT_DOMAIN: &[u8] = b"nautilus-provenance/content/v1";
const DESCRIPTOR_DOMAIN: &[u8] = b"nautilus-provenance/artifact-descriptor/v1";
const DERIVATION_DOMAIN: &[u8] = b"nautilus-provenance/derivation/v2";
const MARKET_SCOPE_DOMAIN: &[u8] = b"nautilus-provenance/execution-market-scope/v2";
const RUN_DOMAIN: &[u8] = b"nautilus-provenance/run/v9";

/// A canonical lowercase hexadecimal BLAKE3 digest.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentDigest([u8; 32]);

/// Incremental equivalent of [`ContentDigest::digest_bytes`] with an exact byte budget.
///
/// The declared length is committed before any content is accepted, so callers can stream large
/// canonical transcripts without buffering them while preserving byte-for-byte digest parity.
#[derive(Clone, Debug)]
pub struct ContentDigestBuilder {
    hasher: blake3::Hasher,
    expected: u64,
    observed: u64,
}

impl ContentDigestBuilder {
    /// Starts a content digest which must receive exactly `total_len` bytes.
    #[must_use]
    pub fn new(total_len: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(CONTENT_DOMAIN.len() as u64).to_be_bytes());
        hasher.update(CONTENT_DOMAIN);
        hasher.update(&total_len.to_be_bytes());
        Self {
            hasher,
            expected: total_len,
            observed: 0,
        }
    }

    /// Adds the next transcript chunk without exceeding the declared length.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::ContentDigestLengthExceeded`] when the chunk would cross the
    /// declared total or cannot be represented as `u64`.
    pub fn update(&mut self, chunk: &[u8]) -> Result<(), ProvenanceError> {
        let chunk_len = u64::try_from(chunk.len()).map_err(|_| {
            ProvenanceError::ContentDigestLengthExceeded {
                expected: self.expected,
                observed: u64::MAX,
            }
        })?;
        let observed = self.observed.checked_add(chunk_len).ok_or(
            ProvenanceError::ContentDigestLengthExceeded {
                expected: self.expected,
                observed: u64::MAX,
            },
        )?;
        if observed > self.expected {
            return Err(ProvenanceError::ContentDigestLengthExceeded {
                expected: self.expected,
                observed,
            });
        }
        self.hasher.update(chunk);
        self.observed = observed;
        Ok(())
    }

    /// Finishes only after exactly the declared number of bytes were supplied.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::ContentDigestLengthMismatch`] on an incomplete transcript.
    pub fn finish(self) -> Result<ContentDigest, ProvenanceError> {
        if self.observed != self.expected {
            return Err(ProvenanceError::ContentDigestLengthMismatch {
                expected: self.expected,
                observed: self.observed,
            });
        }
        Ok(ContentDigest::from_array(
            *self.hasher.finalize().as_bytes(),
        ))
    }
}

impl ContentDigest {
    /// Hashes opaque content using the provenance content domain.
    #[must_use]
    pub fn digest_bytes(content: &[u8]) -> Self {
        let mut hasher = CanonicalHasher::new(CONTENT_DOMAIN);
        hasher.write_bytes(content);
        hasher.finish()
    }

    /// Returns the raw 32-byte digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Wraps the raw output of a domain-separated BLAKE3 transcript.
    #[must_use]
    pub const fn from_array(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl Display for ContentDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ContentDigest {
    type Err = ProvenanceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(ProvenanceError::InvalidDigest {
                value: value.to_string(),
                reason: "expected exactly 64 hexadecimal characters",
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ProvenanceError::InvalidDigest {
                value: value.to_string(),
                reason: "expected lowercase hexadecimal characters",
            });
        }

        let mut bytes = [0u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
        }
        Ok(Self(bytes))
    }
}

impl Serialize for ContentDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ContentDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

/// A stable, validated artifact identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactId(String);

impl ArtifactId {
    /// Creates a validated artifact identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::InvalidIdentifier`] for empty, overlong, or unsupported values.
    pub fn new(value: impl Into<String>) -> Result<Self, ProvenanceError> {
        validate_identifier("artifact id", value.into()).map(Self)
    }

    /// Returns the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ArtifactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ArtifactId {
    type Err = ProvenanceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for ArtifactId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ArtifactId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// A stable identity for the process which produced an artifact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessIdentity(String);

impl ProcessIdentity {
    /// Creates a validated process identity.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::InvalidIdentifier`] for empty, overlong, or unsupported values.
    pub fn new(value: impl Into<String>) -> Result<Self, ProvenanceError> {
        validate_identifier("process identity", value.into()).map(Self)
    }

    /// Returns the process identity as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ProcessIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ProcessIdentity {
    type Err = ProvenanceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for ProcessIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProcessIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// The semantic role of an artifact.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// Market or reference data.
    Dataset,
    /// Executable or source strategy.
    Strategy,
    /// Trained or otherwise parameterized model.
    Model,
    /// Runtime or research configuration.
    Config,
    /// Evaluation results.
    Evaluation,
    /// Promotion decision or approval.
    Promotion,
    /// Human- or machine-readable report.
    Report,
    /// Any artifact outside the defined categories.
    Other,
}

/// A compact artifact identity used by lineage records.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    id: ArtifactId,
    digest: ContentDigest,
}

/// A source dataset together with the canonical records materialized for replay.
///
/// Binding both digests prevents a decoder, query, or catalog change from silently producing
/// different replay data from an artifact carrying the same source identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct DatasetBinding {
    source: ArtifactRef,
    materialized_digest: ContentDigest,
    decoder: ProcessIdentity,
    replay_ordinal: u64,
    record_count: u64,
}

impl DatasetBinding {
    /// Creates a validated dataset binding.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::EmptyDataset`] when `record_count` is zero.
    pub fn new(
        source: ArtifactRef,
        materialized_digest: ContentDigest,
        decoder: ProcessIdentity,
        replay_ordinal: u64,
        record_count: u64,
    ) -> Result<Self, ProvenanceError> {
        if record_count == 0 {
            return Err(ProvenanceError::EmptyDataset(source.id().clone()));
        }
        Ok(Self {
            source,
            materialized_digest,
            decoder,
            replay_ordinal,
            record_count,
        })
    }

    /// Returns the exact source artifact.
    #[must_use]
    pub const fn source(&self) -> &ArtifactRef {
        &self.source
    }

    /// Returns the digest of canonical records delivered to the replay iterator.
    #[must_use]
    pub const fn materialized_digest(&self) -> &ContentDigest {
        &self.materialized_digest
    }

    /// Returns the versioned decoder or query process which materialized the records.
    #[must_use]
    pub const fn decoder(&self) -> &ProcessIdentity {
        &self.decoder
    }

    /// Returns the stream insertion ordinal used to break equal-timestamp replay ties.
    #[must_use]
    pub const fn replay_ordinal(&self) -> u64 {
        self.replay_ordinal
    }

    /// Returns the number of canonical records expected to be processed.
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
}

impl<'de> Deserialize<'de> for DatasetBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UntrustedDatasetBinding {
            source: ArtifactRef,
            materialized_digest: ContentDigest,
            decoder: ProcessIdentity,
            replay_ordinal: u64,
            record_count: u64,
        }

        let value = UntrustedDatasetBinding::deserialize(deserializer)?;
        Self::new(
            value.source,
            value.materialized_digest,
            value.decoder,
            value.replay_ordinal,
            value.record_count,
        )
        .map_err(de::Error::custom)
    }
}

/// Exact, versioned execution-environment bundle used to configure a run.
///
/// The bundle is expected to contain the engine, venue, instrument, fee, latency, settlement,
/// calendar, and market-rule inputs required by its deployment scope. The measured venue and
/// instrument identities bind the loader's materialized universe without imposing any
/// cryptocurrency-specific market taxonomy on the provenance layer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExecutionEnvironmentBinding {
    artifact: ArtifactRef,
    loader: ProcessIdentity,
    market_contexts: Vec<ArtifactRef>,
    venues: Vec<String>,
    instruments: Vec<String>,
    scope: Option<ExecutionEnvironmentScope>,
}

/// Exact replay and routing scope applied by an execution-environment loader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExecutionEnvironmentScope {
    replay_start_ns: u64,
    replay_end_ns: u64,
    knowledge_cutoff_ns: u64,
    market_scopes: Vec<ExecutionMarketScope>,
}

/// Materialized venue keyed to its paired market, listing, and execution-route identities.
///
/// Keeping these identities in one value prevents independently sorted identity sets from
/// erasing which runtime venue received which profile, listing, and route.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "the serialized contract intentionally labels all tuple members as stable IDs"
)]
pub struct ExecutionMarketScope {
    materialized_venue_id: String,
    market_profile_id: String,
    listing_venue_id: String,
    execution_route_id: String,
}

impl ExecutionMarketScope {
    /// Creates a validated market-scope tuple.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::InvalidIdentifier`] when any identity is malformed.
    pub fn new(
        materialized_venue_id: impl Into<String>,
        market_profile_id: impl Into<String>,
        listing_venue_id: impl Into<String>,
        execution_route_id: impl Into<String>,
    ) -> Result<Self, ProvenanceError> {
        Ok(Self {
            materialized_venue_id: validate_identifier(
                "materialized venue id",
                materialized_venue_id.into(),
            )?,
            market_profile_id: validate_identifier("market profile id", market_profile_id.into())?,
            listing_venue_id: validate_identifier("listing venue id", listing_venue_id.into())?,
            execution_route_id: validate_identifier(
                "execution route id",
                execution_route_id.into(),
            )?,
        })
    }

    /// Returns the exact materialized venue identity keyed by the environment binding.
    #[must_use]
    pub fn materialized_venue_id(&self) -> &str {
        &self.materialized_venue_id
    }

    /// Returns the exact market-profile identity.
    #[must_use]
    pub fn market_profile_id(&self) -> &str {
        &self.market_profile_id
    }

    /// Returns the exact listing-venue identity.
    #[must_use]
    pub fn listing_venue_id(&self) -> &str {
        &self.listing_venue_id
    }

    /// Returns the exact execution-route identity paired with this listing.
    #[must_use]
    pub fn execution_route_id(&self) -> &str {
        &self.execution_route_id
    }
}

impl<'de> Deserialize<'de> for ExecutionMarketScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[expect(
            clippy::struct_field_names,
            reason = "wire field names must exactly mirror the validated stable-ID contract"
        )]
        struct UntrustedExecutionMarketScope {
            materialized_venue_id: String,
            market_profile_id: String,
            listing_venue_id: String,
            execution_route_id: String,
        }

        let value = UntrustedExecutionMarketScope::deserialize(deserializer)?;
        Self::new(
            value.materialized_venue_id,
            value.market_profile_id,
            value.listing_venue_id,
            value.execution_route_id,
        )
        .map_err(de::Error::custom)
    }
}

impl ExecutionEnvironmentScope {
    /// Creates a validated, canonical market replay scope.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::InvalidReplayWindow`] when the half-open replay window is empty
    /// or reversed, [`ProvenanceError::EmptyExecutionEnvironment`] when no market scope is named,
    /// [`ProvenanceError::DuplicateMarketScopeListing`] when one listing is ambiguously paired,
    /// [`ProvenanceError::DuplicateMarketScopeVenue`] when one materialized venue is ambiguously
    /// paired, or [`ProvenanceError::InvalidIdentifier`] for malformed identities.
    pub fn new(
        replay_start_ns: u64,
        replay_end_ns: u64,
        knowledge_cutoff_ns: u64,
        mut market_scopes: Vec<ExecutionMarketScope>,
    ) -> Result<Self, ProvenanceError> {
        if replay_start_ns >= replay_end_ns {
            return Err(ProvenanceError::InvalidReplayWindow {
                replay_start_ns,
                replay_end_ns,
            });
        }
        if knowledge_cutoff_ns > replay_start_ns {
            return Err(ProvenanceError::KnowledgeCutoffAfterReplayStart {
                knowledge_cutoff_ns,
                replay_start_ns,
            });
        }
        if market_scopes.is_empty() {
            return Err(ProvenanceError::EmptyExecutionEnvironment {
                field: "market scope",
            });
        }
        market_scopes.sort_unstable();
        market_scopes.dedup();
        let mut listings = BTreeSet::new();
        let mut materialized_venues = BTreeSet::new();
        for market_scope in &market_scopes {
            if !materialized_venues.insert(market_scope.materialized_venue_id()) {
                return Err(ProvenanceError::DuplicateMarketScopeVenue {
                    materialized_venue_id: market_scope.materialized_venue_id().to_string(),
                });
            }
            if !listings.insert(market_scope.listing_venue_id()) {
                return Err(ProvenanceError::DuplicateMarketScopeListing {
                    listing_venue_id: market_scope.listing_venue_id().to_string(),
                });
            }
        }
        Ok(Self {
            replay_start_ns,
            replay_end_ns,
            knowledge_cutoff_ns,
            market_scopes,
        })
    }

    /// Returns the inclusive replay start in Unix nanoseconds.
    #[must_use]
    pub const fn replay_start_ns(&self) -> u64 {
        self.replay_start_ns
    }

    /// Returns the exclusive replay end in Unix nanoseconds.
    #[must_use]
    pub const fn replay_end_ns(&self) -> u64 {
        self.replay_end_ns
    }

    /// Returns the point-in-time knowledge cutoff in Unix nanoseconds.
    #[must_use]
    pub const fn knowledge_cutoff_ns(&self) -> u64 {
        self.knowledge_cutoff_ns
    }

    /// Returns canonical paired market scopes, sorted lexicographically by tuple identity.
    #[must_use]
    pub fn market_scopes(&self) -> &[ExecutionMarketScope] {
        &self.market_scopes
    }

    /// Computes the canonical commitment to the exact paired market scope.
    ///
    /// Replay bounds and the knowledge cutoff remain bound by [`RunProvenance::canonical_digest`]
    /// but are intentionally excluded here so a deployment policy can pin market semantics
    /// independently of one historical evaluation window.
    #[must_use]
    pub fn market_scope_commitment(&self) -> ContentDigest {
        let mut hasher = CanonicalHasher::new(MARKET_SCOPE_DOMAIN);
        hasher.write_u64(self.market_scopes.len() as u64);
        for market_scope in &self.market_scopes {
            hasher.write_str(market_scope.materialized_venue_id());
            hasher.write_str(market_scope.market_profile_id());
            hasher.write_str(market_scope.listing_venue_id());
            hasher.write_str(market_scope.execution_route_id());
        }
        hasher.finish()
    }
}

impl<'de> Deserialize<'de> for ExecutionEnvironmentScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UntrustedExecutionEnvironmentScope {
            replay_start_ns: u64,
            replay_end_ns: u64,
            knowledge_cutoff_ns: u64,
            market_scopes: Vec<ExecutionMarketScope>,
        }

        let value = UntrustedExecutionEnvironmentScope::deserialize(deserializer)?;
        Self::new(
            value.replay_start_ns,
            value.replay_end_ns,
            value.knowledge_cutoff_ns,
            value.market_scopes,
        )
        .map_err(de::Error::custom)
    }
}

impl ExecutionEnvironmentBinding {
    /// Creates a canonical execution-environment binding.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::EmptyExecutionEnvironment`] when no venue or instrument is
    /// present, or [`ProvenanceError::InvalidIdentifier`] for malformed identities.
    pub fn new(
        artifact: ArtifactRef,
        loader: ProcessIdentity,
        mut market_contexts: Vec<ArtifactRef>,
        venues: Vec<String>,
        instruments: Vec<String>,
    ) -> Result<Self, ProvenanceError> {
        market_contexts.sort_unstable();
        market_contexts.dedup();
        let venues = canonical_environment_identities("environment venue", venues)?;
        if venues.is_empty() {
            return Err(ProvenanceError::EmptyExecutionEnvironment { field: "venue" });
        }
        let instruments = canonical_environment_identities("environment instrument", instruments)?;
        if instruments.is_empty() {
            return Err(ProvenanceError::EmptyExecutionEnvironment {
                field: "instrument",
            });
        }
        Ok(Self {
            artifact,
            loader,
            market_contexts,
            venues,
            instruments,
            scope: None,
        })
    }

    /// Creates a canonical execution-environment binding with exact replay and routing scope.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as [`Self::new`], or
    /// [`ProvenanceError::MarketScopeVenueMismatch`] when scope venue keys do not exactly match
    /// the materialized venue universe.
    pub fn new_scoped(
        artifact: ArtifactRef,
        loader: ProcessIdentity,
        market_contexts: Vec<ArtifactRef>,
        venues: Vec<String>,
        instruments: Vec<String>,
        scope: ExecutionEnvironmentScope,
    ) -> Result<Self, ProvenanceError> {
        let mut binding = Self::new(artifact, loader, market_contexts, venues, instruments)?;
        let scope_venues = scope
            .market_scopes()
            .iter()
            .map(|market_scope| market_scope.materialized_venue_id().to_string())
            .collect::<Vec<_>>();
        if binding.venues != scope_venues {
            return Err(ProvenanceError::MarketScopeVenueMismatch {
                binding_venues: binding.venues,
                scope_venues,
            });
        }
        binding.scope = Some(scope);
        Ok(binding)
    }

    /// Returns the exact environment bundle artifact.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    /// Returns the versioned loader which applied the bundle.
    #[must_use]
    pub const fn loader(&self) -> &ProcessIdentity {
        &self.loader
    }

    /// Returns external market-context artifacts actually materialized by the loader.
    ///
    /// An empty set means the selected closed runtime profile has no external context inputs.
    #[must_use]
    pub fn market_contexts(&self) -> &[ArtifactRef] {
        &self.market_contexts
    }

    /// Returns the canonical venue universe materialized by the loader.
    #[must_use]
    pub fn venues(&self) -> &[String] {
        &self.venues
    }

    /// Returns the canonical instrument universe materialized by the loader.
    #[must_use]
    pub fn instruments(&self) -> &[String] {
        &self.instruments
    }

    /// Returns the exact replay and routing scope when the loader supplied one.
    #[must_use]
    pub const fn scope(&self) -> Option<&ExecutionEnvironmentScope> {
        self.scope.as_ref()
    }
}

impl<'de> Deserialize<'de> for ExecutionEnvironmentBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UntrustedExecutionEnvironmentBinding {
            artifact: ArtifactRef,
            loader: ProcessIdentity,
            market_contexts: Vec<ArtifactRef>,
            venues: Vec<String>,
            instruments: Vec<String>,
            #[serde(default)]
            scope: Option<ExecutionEnvironmentScope>,
        }

        let value = UntrustedExecutionEnvironmentBinding::deserialize(deserializer)?;
        match value.scope {
            Some(scope) => Self::new_scoped(
                value.artifact,
                value.loader,
                value.market_contexts,
                value.venues,
                value.instruments,
                scope,
            ),
            None => Self::new(
                value.artifact,
                value.loader,
                value.market_contexts,
                value.venues,
                value.instruments,
            ),
        }
        .map_err(de::Error::custom)
    }
}

/// Exact executable or source artifact and runtime identity used by a strategy run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyBinding {
    artifact: ArtifactRef,
    entrypoint: String,
    runtime_id: String,
}

impl StrategyBinding {
    /// Creates a validated strategy binding.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::InvalidIdentifier`] for invalid runtime identities.
    pub fn new(
        artifact: ArtifactRef,
        entrypoint: impl Into<String>,
        runtime_id: impl Into<String>,
    ) -> Result<Self, ProvenanceError> {
        Ok(Self {
            artifact,
            entrypoint: validate_runtime_identity("strategy entrypoint", entrypoint.into())?,
            runtime_id: validate_identifier("strategy runtime id", runtime_id.into())?,
        })
    }

    /// Returns the exact executable or source artifact.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    /// Returns the fully qualified runtime type or Python entrypoint.
    #[must_use]
    pub fn entrypoint(&self) -> &str {
        &self.entrypoint
    }

    /// Returns the concrete strategy identity used by the run.
    #[must_use]
    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }
}

impl<'de> Deserialize<'de> for StrategyBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct UntrustedStrategyBinding {
            artifact: ArtifactRef,
            entrypoint: String,
            runtime_id: String,
        }

        let value = UntrustedStrategyBinding::deserialize(deserializer)?;
        Self::new(value.artifact, value.entrypoint, value.runtime_id).map_err(de::Error::custom)
    }
}

impl ArtifactRef {
    /// Creates an artifact reference.
    #[must_use]
    pub const fn new(id: ArtifactId, digest: ContentDigest) -> Self {
        Self { id, digest }
    }

    /// Returns the artifact id.
    #[must_use]
    pub const fn id(&self) -> &ArtifactId {
        &self.id
    }

    /// Returns the expected content digest.
    #[must_use]
    pub const fn digest(&self) -> &ContentDigest {
        &self.digest
    }
}

/// An immutable artifact descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDescriptor {
    id: ArtifactId,
    kind: ArtifactKind,
    content_digest: ContentDigest,
}

impl ArtifactDescriptor {
    /// Creates an immutable artifact descriptor.
    #[must_use]
    pub const fn new(id: ArtifactId, kind: ArtifactKind, content_digest: ContentDigest) -> Self {
        Self {
            id,
            kind,
            content_digest,
        }
    }

    /// Returns the artifact id.
    #[must_use]
    pub const fn id(&self) -> &ArtifactId {
        &self.id
    }

    /// Returns the artifact kind.
    #[must_use]
    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }

    /// Returns the artifact content digest.
    #[must_use]
    pub const fn content_digest(&self) -> &ContentDigest {
        &self.content_digest
    }

    /// Returns the compact reference for this descriptor.
    #[must_use]
    pub fn artifact_ref(&self) -> ArtifactRef {
        ArtifactRef::new(self.id.clone(), self.content_digest)
    }

    /// Computes the canonical descriptor digest.
    #[must_use]
    pub fn canonical_digest(&self) -> ContentDigest {
        let mut hasher = CanonicalHasher::new(DESCRIPTOR_DOMAIN);
        hasher.write_str(self.id.as_str());
        hasher.write_u8(self.kind as u8);
        hasher.write_digest(&self.content_digest);
        hasher.finish()
    }
}

/// An immutable record binding inputs to one output and its producing process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DerivationRecord {
    inputs: Vec<ArtifactRef>,
    output: ArtifactRef,
    process: ProcessIdentity,
    timestamp_ns: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationRecordRepr {
    inputs: Vec<ArtifactRef>,
    output: ArtifactRef,
    process: ProcessIdentity,
    timestamp_ns: u64,
}

impl<'de> Deserialize<'de> for DerivationRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = DerivationRecordRepr::deserialize(deserializer)?;
        Ok(Self::new(
            repr.inputs,
            repr.output,
            repr.process,
            repr.timestamp_ns,
        ))
    }
}

impl DerivationRecord {
    /// Creates a derivation record whose input order and multiplicity are semantically bound.
    #[must_use]
    pub fn new(
        inputs: Vec<ArtifactRef>,
        output: ArtifactRef,
        process: ProcessIdentity,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            inputs,
            output,
            process,
            timestamp_ns,
        }
    }

    /// Returns the ordered input sequence, including repeated inputs.
    #[must_use]
    pub fn inputs(&self) -> &[ArtifactRef] {
        &self.inputs
    }

    /// Returns the output reference.
    #[must_use]
    pub const fn output(&self) -> &ArtifactRef {
        &self.output
    }

    /// Returns the producing process identity.
    #[must_use]
    pub const fn process(&self) -> &ProcessIdentity {
        &self.process
    }

    /// Returns the caller-supplied Unix timestamp in nanoseconds.
    #[must_use]
    pub const fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }

    /// Computes the canonical derivation digest.
    #[must_use]
    pub fn canonical_digest(&self) -> ContentDigest {
        let mut hasher = CanonicalHasher::new(DERIVATION_DOMAIN);
        hasher.write_u64(self.inputs.len() as u64);
        for input in &self.inputs {
            hasher.write_ref(input);
        }
        hasher.write_ref(&self.output);
        hasher.write_str(self.process.as_str());
        hasher.write_u64(self.timestamp_ns);
        hasher.finish()
    }
}

/// Artifact bindings which make a simulation, evaluation, or live run reproducible.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunProvenance {
    /// Source and materialized input datasets, treated canonically as a set.
    pub data: Vec<DatasetBinding>,
    /// Engine, venue, instrument, and market-rule environment used by the run.
    pub environment: Option<ExecutionEnvironmentBinding>,
    /// Strategy artifact used by the run.
    pub strategy: Option<StrategyBinding>,
    /// Optional model artifact used by the strategy.
    pub model: Option<ArtifactRef>,
    /// Runtime configuration artifact.
    pub config: Option<ArtifactRef>,
    /// Risk, execution, or governance policy artifact.
    pub policy: Option<ArtifactRef>,
    /// Evaluation artifact produced for the run.
    pub evaluation: Option<ArtifactRef>,
    /// Promotion decision associated with the run.
    pub promotion: Option<ArtifactRef>,
}

impl RunProvenance {
    /// Returns all bound references in deterministic, de-duplicated order.
    #[must_use]
    pub fn references(&self) -> Vec<ArtifactRef> {
        let mut references: Vec<_> = self
            .data
            .iter()
            .map(|binding| binding.source().clone())
            .collect();
        if let Some(strategy) = &self.strategy {
            references.push(strategy.artifact().clone());
        }
        if let Some(environment) = &self.environment {
            references.push(environment.artifact().clone());
            references.extend(environment.market_contexts().iter().cloned());
        }
        references.extend(
            [
                self.model.as_ref(),
                self.config.as_ref(),
                self.policy.as_ref(),
                self.evaluation.as_ref(),
                self.promotion.as_ref(),
            ]
            .into_iter()
            .flatten()
            .cloned(),
        );
        references.sort_unstable();
        references.dedup();
        references
    }

    /// Computes the canonical digest of all run bindings.
    #[must_use]
    pub fn canonical_digest(&self) -> ContentDigest {
        let mut data = self.data.clone();
        data.sort_unstable();
        data.dedup();

        let mut hasher = CanonicalHasher::new(RUN_DOMAIN);
        hasher.write_u64(data.len() as u64);
        for binding in &data {
            hasher.write_ref(binding.source());
            hasher.write_digest(binding.materialized_digest());
            hasher.write_str(binding.decoder().as_str());
            hasher.write_u64(binding.replay_ordinal());
            hasher.write_u64(binding.record_count());
        }
        match self.environment.as_ref() {
            Some(binding) => {
                hasher.write_u8(1);
                hasher.write_ref(binding.artifact());
                hasher.write_str(binding.loader().as_str());
                hasher.write_u64(binding.market_contexts().len() as u64);
                for context in binding.market_contexts() {
                    hasher.write_ref(context);
                }
                hasher.write_u64(binding.venues().len() as u64);
                for venue in binding.venues() {
                    hasher.write_str(venue);
                }
                hasher.write_u64(binding.instruments().len() as u64);
                for instrument in binding.instruments() {
                    hasher.write_str(instrument);
                }
                match binding.scope() {
                    Some(scope) => {
                        hasher.write_u8(1);
                        hasher.write_u64(scope.replay_start_ns());
                        hasher.write_u64(scope.replay_end_ns());
                        hasher.write_u64(scope.knowledge_cutoff_ns());
                        hasher.write_u64(scope.market_scopes().len() as u64);
                        for market_scope in scope.market_scopes() {
                            hasher.write_str(market_scope.materialized_venue_id());
                            hasher.write_str(market_scope.market_profile_id());
                            hasher.write_str(market_scope.listing_venue_id());
                            hasher.write_str(market_scope.execution_route_id());
                        }
                    }
                    None => hasher.write_u8(0),
                }
            }
            None => hasher.write_u8(0),
        }
        match self.strategy.as_ref() {
            Some(binding) => {
                hasher.write_u8(1);
                hasher.write_ref(binding.artifact());
                hasher.write_str(binding.entrypoint());
                hasher.write_str(binding.runtime_id());
            }
            None => hasher.write_u8(0),
        }
        hasher.write_optional_ref(self.model.as_ref());
        hasher.write_optional_ref(self.config.as_ref());
        hasher.write_optional_ref(self.policy.as_ref());
        hasher.write_optional_ref(self.evaluation.as_ref());
        hasher.write_optional_ref(self.promotion.as_ref());
        hasher.finish()
    }
}

fn canonical_environment_identities(
    kind: &'static str,
    values: Vec<String>,
) -> Result<Vec<String>, ProvenanceError> {
    let mut values = values
        .into_iter()
        .map(|value| validate_identifier(kind, value))
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn validate_identifier(kind: &'static str, value: String) -> Result<String, ProvenanceError> {
    if value.is_empty() {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "value must not be empty",
        });
    }
    if value.len() > 255 {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "value exceeds 255 bytes",
        });
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
    }) {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "only ASCII alphanumerics and - _ . : / are permitted",
        });
    }
    Ok(value)
}

fn validate_runtime_identity(kind: &'static str, value: String) -> Result<String, ProvenanceError> {
    if value.is_empty() {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "value must not be empty",
        });
    }
    if value.len() > 1024 {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "value exceeds 1024 bytes",
        });
    }
    if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ProvenanceError::InvalidIdentifier {
            kind,
            value,
            reason: "only printable non-whitespace ASCII is permitted",
        });
    }
    Ok(value)
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Serialize;

    use super::*;

    #[rstest]
    #[case("")]
    #[case("abc")]
    #[case("ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789")]
    #[case("g000000000000000000000000000000000000000000000000000000000000000")]
    fn digest_rejects_non_canonical_values(#[case] value: &str) {
        assert!(matches!(
            value.parse::<ContentDigest>(),
            Err(ProvenanceError::InvalidDigest { .. })
        ));
    }

    #[rstest]
    fn digest_round_trip() {
        let value = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(value.parse::<ContentDigest>().unwrap().to_string(), value);
    }

    #[rstest]
    fn streaming_content_digest_matches_one_shot_and_enforces_exact_length() {
        let content = b"streamed canonical transcript";
        let mut builder = ContentDigestBuilder::new(content.len() as u64);
        builder.update(&content[..7]).unwrap();
        builder.update(&content[7..]).unwrap();
        assert_eq!(
            builder.finish().unwrap(),
            ContentDigest::digest_bytes(content)
        );

        let short = ContentDigestBuilder::new(1).finish().unwrap_err();
        assert!(matches!(
            short,
            ProvenanceError::ContentDigestLengthMismatch {
                expected: 1,
                observed: 0,
            }
        ));
        let mut long = ContentDigestBuilder::new(0);
        assert!(matches!(
            long.update(b"x"),
            Err(ProvenanceError::ContentDigestLengthExceeded {
                expected: 0,
                observed: 1,
            })
        ));
    }

    #[rstest]
    fn run_digest_treats_data_as_a_set() {
        let first = DatasetBinding::new(
            descriptor("data/a", b"a").artifact_ref(),
            ContentDigest::digest_bytes(b"decoded-a"),
            ProcessIdentity::new("decoder/sbe-v1").unwrap(),
            0,
            1,
        )
        .unwrap();
        let second = DatasetBinding::new(
            descriptor("data/b", b"b").artifact_ref(),
            ContentDigest::digest_bytes(b"decoded-b"),
            ProcessIdentity::new("decoder/sbe-v1").unwrap(),
            1,
            1,
        )
        .unwrap();
        let one = RunProvenance {
            data: vec![first.clone(), second.clone()],
            ..RunProvenance::default()
        };
        let two = RunProvenance {
            data: vec![second, first.clone(), first],
            ..RunProvenance::default()
        };
        assert_eq!(one.canonical_digest(), two.canonical_digest());
    }

    #[rstest]
    fn dataset_binding_rejects_empty_materialization_at_all_boundaries() {
        let source = descriptor("data/empty", b"source").artifact_ref();
        let materialized_digest = ContentDigest::digest_bytes(b"decoded");
        let decoder = ProcessIdentity::new("decoder/sbe-v1").unwrap();

        assert!(matches!(
            DatasetBinding::new(source.clone(), materialized_digest, decoder.clone(), 0, 0),
            Err(ProvenanceError::EmptyDataset(id)) if id == *source.id()
        ));

        let wire = UntrustedDatasetBinding {
            source,
            materialized_digest,
            decoder,
            replay_ordinal: 0,
            record_count: 0,
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();
        assert!(rmp_serde::from_slice::<DatasetBinding>(&bytes).is_err());
    }

    #[rstest]
    fn dataset_binding_deserialization_rejects_unknown_fields() {
        let binding = DatasetBinding::new(
            descriptor("data/quotes", b"source").artifact_ref(),
            ContentDigest::digest_bytes(b"decoded"),
            ProcessIdentity::new("decoder/sbe-v1").unwrap(),
            0,
            1,
        )
        .unwrap();
        let wire = UntrustedDatasetBindingWithExtra {
            source: binding.source().clone(),
            materialized_digest: *binding.materialized_digest(),
            decoder: binding.decoder().clone(),
            replay_ordinal: binding.replay_ordinal(),
            record_count: binding.record_count(),
            untrusted: true,
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();
        assert!(rmp_serde::from_slice::<DatasetBinding>(&bytes).is_err());
    }

    #[rstest]
    fn run_digest_binds_materialized_dataset_and_replay_contract() {
        let source = descriptor("data/quotes", b"source").artifact_ref();
        let binding = DatasetBinding::new(
            source.clone(),
            ContentDigest::digest_bytes(b"decoded-a"),
            ProcessIdentity::new("decoder/sbe-v1").unwrap(),
            0,
            1,
        )
        .unwrap();
        let original = RunProvenance {
            data: vec![binding],
            ..RunProvenance::default()
        };

        for changed in [
            DatasetBinding::new(
                source.clone(),
                ContentDigest::digest_bytes(b"decoded-b"),
                ProcessIdentity::new("decoder/sbe-v1").unwrap(),
                0,
                1,
            )
            .unwrap(),
            DatasetBinding::new(
                source.clone(),
                ContentDigest::digest_bytes(b"decoded-a"),
                ProcessIdentity::new("decoder/sbe-v2").unwrap(),
                0,
                1,
            )
            .unwrap(),
            DatasetBinding::new(
                source.clone(),
                ContentDigest::digest_bytes(b"decoded-a"),
                ProcessIdentity::new("decoder/sbe-v1").unwrap(),
                1,
                1,
            )
            .unwrap(),
            DatasetBinding::new(
                source,
                ContentDigest::digest_bytes(b"decoded-a"),
                ProcessIdentity::new("decoder/sbe-v1").unwrap(),
                0,
                2,
            )
            .unwrap(),
        ] {
            let changed = RunProvenance {
                data: vec![changed],
                ..RunProvenance::default()
            };
            assert_ne!(original.canonical_digest(), changed.canonical_digest());
        }
    }

    #[rstest]
    fn execution_environment_binding_is_canonical_and_bound_to_run_digest() {
        let artifact = descriptor("environment/cross-market", b"environment-v1").artifact_ref();
        let context = descriptor("market-context/calendar", b"calendar-v1").artifact_ref();
        let binding = ExecutionEnvironmentBinding::new(
            artifact.clone(),
            ProcessIdentity::new("environment-loader/v1").unwrap(),
            vec![context.clone(), context.clone()],
            vec!["XNAS".to_string(), "OKX".to_string(), "XNAS".to_string()],
            vec![
                "AAPL.XNAS".to_string(),
                "BTC-USDT.OKX".to_string(),
                "AAPL.XNAS".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(binding.venues(), ["OKX", "XNAS"]);
        assert_eq!(binding.instruments(), ["AAPL.XNAS", "BTC-USDT.OKX"]);
        assert_eq!(binding.market_contexts(), std::slice::from_ref(&context));

        let original = RunProvenance {
            environment: Some(binding),
            ..RunProvenance::default()
        };
        for changed in [
            ExecutionEnvironmentBinding::new(
                artifact.clone(),
                ProcessIdentity::new("environment-loader/v2").unwrap(),
                vec![context.clone()],
                vec!["OKX".to_string(), "XNAS".to_string()],
                vec!["AAPL.XNAS".to_string(), "BTC-USDT.OKX".to_string()],
            )
            .unwrap(),
            ExecutionEnvironmentBinding::new(
                artifact.clone(),
                ProcessIdentity::new("environment-loader/v1").unwrap(),
                vec![context],
                vec!["OKX".to_string()],
                vec!["AAPL.XNAS".to_string(), "BTC-USDT.OKX".to_string()],
            )
            .unwrap(),
            ExecutionEnvironmentBinding::new(
                artifact,
                ProcessIdentity::new("environment-loader/v1").unwrap(),
                vec![descriptor("market-context/calendar-v2", b"calendar-v2").artifact_ref()],
                vec!["OKX".to_string(), "XNAS".to_string()],
                vec!["BTC-USDT.OKX".to_string()],
            )
            .unwrap(),
        ] {
            assert_ne!(
                original.canonical_digest(),
                RunProvenance {
                    environment: Some(changed),
                    ..RunProvenance::default()
                }
                .canonical_digest()
            );
        }
    }

    #[rstest]
    fn execution_environment_binding_rejects_incomplete_or_untrusted_wire_values() {
        let artifact = descriptor("environment/test", b"environment-v1").artifact_ref();
        let loader = ProcessIdentity::new("environment-loader/v1").unwrap();
        let context = descriptor("market-context/calendar", b"calendar-v1").artifact_ref();
        assert!(
            ExecutionEnvironmentBinding::new(
                artifact.clone(),
                loader.clone(),
                Vec::new(),
                vec!["XNAS".to_string()],
                vec!["AAPL.XNAS".to_string()],
            )
            .is_ok()
        );
        assert!(matches!(
            ExecutionEnvironmentBinding::new(
                artifact.clone(),
                loader.clone(),
                vec![context.clone()],
                Vec::new(),
                vec!["AAPL.XNAS".to_string()],
            ),
            Err(ProvenanceError::EmptyExecutionEnvironment { field: "venue" })
        ));
        assert!(matches!(
            ExecutionEnvironmentBinding::new(
                artifact.clone(),
                loader.clone(),
                vec![context.clone()],
                vec!["XNAS".to_string()],
                Vec::new(),
            ),
            Err(ProvenanceError::EmptyExecutionEnvironment {
                field: "instrument"
            })
        ));

        let wire = UntrustedExecutionEnvironmentBindingWithExtra {
            artifact,
            loader,
            market_contexts: vec![context],
            venues: vec!["XNAS".to_string()],
            instruments: vec!["AAPL.XNAS".to_string()],
            continuous_trading: true,
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();
        assert!(rmp_serde::from_slice::<ExecutionEnvironmentBinding>(&bytes).is_err());
    }

    #[rstest]
    fn scoped_environment_binds_replay_cutoff_profiles_and_routes() {
        let artifact = descriptor("environment/four-market", b"environment-v2").artifact_ref();
        let loader = ProcessIdentity::new("environment-loader/v2").unwrap();
        let scope = ExecutionEnvironmentScope::new(
            100,
            200,
            90,
            vec![
                market_scope("US.CONSOLIDATED.v1", "listing/XNAS", "SMART.US.v1"),
                market_scope("CRYPTO.OKX.v1", "listing/OKX", "OKX.DIRECT.v1"),
                market_scope("US.CONSOLIDATED.v1", "listing/XNAS", "SMART.US.v1"),
            ],
        )
        .unwrap();
        assert_eq!(
            scope.market_scopes(),
            [
                market_scope("CRYPTO.OKX.v1", "listing/OKX", "OKX.DIRECT.v1"),
                market_scope("US.CONSOLIDATED.v1", "listing/XNAS", "SMART.US.v1"),
            ]
        );

        let scoped = ExecutionEnvironmentBinding::new_scoped(
            artifact.clone(),
            loader.clone(),
            Vec::new(),
            vec!["OKX".to_string(), "XNAS".to_string()],
            vec!["BTC-USDT.OKX".to_string(), "AAPL.XNAS".to_string()],
            scope,
        )
        .unwrap();
        let unscoped = ExecutionEnvironmentBinding::new(
            artifact,
            loader,
            Vec::new(),
            vec!["OKX".to_string(), "XNAS".to_string()],
            vec!["BTC-USDT.OKX".to_string(), "AAPL.XNAS".to_string()],
        )
        .unwrap();
        assert_ne!(
            RunProvenance {
                environment: Some(scoped.clone()),
                ..RunProvenance::default()
            }
            .canonical_digest(),
            RunProvenance {
                environment: Some(unscoped),
                ..RunProvenance::default()
            }
            .canonical_digest()
        );
        assert_eq!(
            rmp_serde::from_slice::<ExecutionEnvironmentBinding>(
                &rmp_serde::to_vec_named(&scoped).unwrap()
            )
            .unwrap(),
            scoped
        );
        assert!(matches!(
            ExecutionEnvironmentScope::new(
                200,
                200,
                90,
                vec![market_scope("CN.A.v1", "listing/XSHG", "SSE.DIRECT.v1")],
            ),
            Err(ProvenanceError::InvalidReplayWindow { .. })
        ));
        assert!(matches!(
            ExecutionEnvironmentScope::new(
                100,
                200,
                101,
                vec![market_scope("CN.A.v1", "listing/XSHG", "SSE.DIRECT.v1")],
            ),
            Err(ProvenanceError::KnowledgeCutoffAfterReplayStart {
                knowledge_cutoff_ns: 101,
                replay_start_ns: 100,
            })
        ));

        let wire = rmp_serde::to_vec_named(&UntrustedExecutionEnvironmentScope {
            replay_start_ns: 100,
            replay_end_ns: 200,
            knowledge_cutoff_ns: 101,
            market_scopes: vec![market_scope("CN.A.v1", "listing/XSHG", "SSE.DIRECT.v1")],
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<ExecutionEnvironmentScope>(&wire).is_err());
    }

    #[rstest]
    fn market_scope_commitment_is_order_independent_but_preserves_pairing() {
        let okx = market_scope("CRYPTO.OKX.v1", "listing/OKX", "OKX.DIRECT.v1");
        let nasdaq = market_scope("US.CONSOLIDATED.v1", "listing/XNAS", "SMART.US.v1");
        let first = ExecutionEnvironmentScope::new(100, 200, 90, vec![nasdaq.clone(), okx.clone()])
            .unwrap();
        let reordered = ExecutionEnvironmentScope::new(100, 200, 90, vec![okx, nasdaq]).unwrap();
        let route_swapped = ExecutionEnvironmentScope::new(
            100,
            200,
            90,
            vec![
                market_scope("CRYPTO.OKX.v1", "listing/OKX", "SMART.US.v1"),
                market_scope("US.CONSOLIDATED.v1", "listing/XNAS", "OKX.DIRECT.v1"),
            ],
        )
        .unwrap();

        assert_eq!(
            first.market_scope_commitment(),
            reordered.market_scope_commitment()
        );
        assert_ne!(
            first.market_scope_commitment(),
            route_swapped.market_scope_commitment()
        );
    }

    #[rstest]
    fn market_scope_rejects_ambiguous_or_mismatched_materialized_venues() {
        assert!(matches!(
            ExecutionEnvironmentScope::new(
                100,
                200,
                90,
                vec![
                    ExecutionMarketScope::new(
                        "SIM-A",
                        "US.CONSOLIDATED.v1",
                        "listing/XNAS",
                        "SMART.US.v1",
                    )
                    .unwrap(),
                    ExecutionMarketScope::new(
                        "SIM-B",
                        "US.CONSOLIDATED.v1",
                        "listing/XNAS",
                        "XNAS.DIRECT.v1",
                    )
                    .unwrap(),
                ],
            ),
            Err(ProvenanceError::DuplicateMarketScopeListing { .. })
        ));

        assert!(matches!(
            ExecutionEnvironmentScope::new(
                100,
                200,
                90,
                vec![
                    ExecutionMarketScope::new(
                        "XNAS",
                        "US.CONSOLIDATED.v1",
                        "listing/XNAS",
                        "SMART.US.v1",
                    )
                    .unwrap(),
                    ExecutionMarketScope::new(
                        "XNAS",
                        "US.CONSOLIDATED.v1",
                        "listing/XNYS",
                        "XNYS.DIRECT.v1",
                    )
                    .unwrap(),
                ],
            ),
            Err(ProvenanceError::DuplicateMarketScopeVenue { .. })
        ));

        let scope = ExecutionEnvironmentScope::new(
            100,
            200,
            90,
            vec![
                ExecutionMarketScope::new(
                    "ATTACKER-VENUE",
                    "CRYPTO.OKX.v1",
                    "listing/OKX",
                    "OKX.DIRECT.v1",
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert!(matches!(
            ExecutionEnvironmentBinding::new_scoped(
                descriptor("environment/mismatch", b"environment").artifact_ref(),
                ProcessIdentity::new("environment-loader/v2").unwrap(),
                Vec::new(),
                vec!["OKX".to_string()],
                vec!["BTC-USDT.OKX".to_string()],
                scope,
            ),
            Err(ProvenanceError::MarketScopeVenueMismatch { .. })
        ));
    }

    #[rstest]
    fn market_scope_deserialization_rejects_legacy_independent_identity_sets() {
        let wire = rmp_serde::to_vec_named(&LegacyExecutionEnvironmentScope {
            replay_start_ns: 100,
            replay_end_ns: 200,
            knowledge_cutoff_ns: 90,
            market_profile_ids: vec!["CRYPTO.OKX.v1".to_string(), "US.v1".to_string()],
            execution_route_ids: vec!["OKX.DIRECT.v1".to_string(), "SMART.US.v1".to_string()],
        })
        .unwrap();

        assert!(rmp_serde::from_slice::<ExecutionEnvironmentScope>(&wire).is_err());
    }

    #[rstest]
    fn strategy_binding_validates_wire_identity_and_binds_run_digest() {
        let artifact = descriptor("strategy/release", b"binary").artifact_ref();
        let binding =
            StrategyBinding::new(artifact.clone(), "weilan::MomentumStrategy", "MOMENTUM-001")
                .unwrap();
        let original = RunProvenance {
            strategy: Some(binding.clone()),
            ..RunProvenance::default()
        };
        let changed_entrypoint = RunProvenance {
            strategy: Some(
                StrategyBinding::new(
                    artifact.clone(),
                    "weilan::MeanReversionStrategy",
                    binding.runtime_id(),
                )
                .unwrap(),
            ),
            ..RunProvenance::default()
        };
        let changed_runtime_id = RunProvenance {
            strategy: Some(
                StrategyBinding::new(artifact, binding.entrypoint(), "MOMENTUM-002").unwrap(),
            ),
            ..RunProvenance::default()
        };

        assert_ne!(
            original.canonical_digest(),
            changed_entrypoint.canonical_digest()
        );
        assert_ne!(
            original.canonical_digest(),
            changed_runtime_id.canonical_digest()
        );

        let wire = UntrustedStrategyBinding {
            artifact: binding.artifact().clone(),
            entrypoint: "weilan strategy",
            runtime_id: binding.runtime_id(),
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();
        assert!(rmp_serde::from_slice::<StrategyBinding>(&bytes).is_err());
    }

    #[rstest]
    fn run_provenance_deserialization_rejects_unknown_fields() {
        let wire = UntrustedRunProvenanceWithExtra {
            data: Vec::new(),
            environment: None,
            strategy: None,
            model: None,
            config: None,
            policy: None,
            evaluation: None,
            promotion: None,
            untrusted_market_assumption: "continuous-trading",
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();

        assert!(rmp_serde::from_slice::<RunProvenance>(&bytes).is_err());
    }

    #[rstest]
    fn derivation_deserialization_preserves_input_order_and_multiplicity() {
        let first = descriptor("data/a", b"a").artifact_ref();
        let second = descriptor("data/b", b"b").artifact_ref();
        let output = descriptor("derived", b"derived").artifact_ref();
        let process = ProcessIdentity::new("pipeline/v1").unwrap();
        let untrusted = UntrustedDerivationRecord {
            inputs: vec![second.clone(), first.clone(), second],
            output: output.clone(),
            process: process.clone(),
            timestamp_ns: 42,
        };
        let bytes = rmp_serde::to_vec_named(&untrusted).unwrap();

        let decoded: DerivationRecord = rmp_serde::from_slice(&bytes).unwrap();
        let canonical = DerivationRecord::new(
            vec![
                untrusted.inputs[0].clone(),
                first,
                untrusted.inputs[0].clone(),
            ],
            output,
            process,
            42,
        );

        assert_eq!(decoded, canonical);
        assert_eq!(decoded.canonical_digest(), canonical.canonical_digest());
        assert_eq!(decoded.inputs().len(), 3);
    }

    #[rstest]
    fn derivation_digest_binds_input_order() {
        let first = descriptor("data/a", b"a").artifact_ref();
        let second = descriptor("data/b", b"b").artifact_ref();
        let output = descriptor("derived", b"derived").artifact_ref();
        let process = ProcessIdentity::new("pipeline/v2").unwrap();

        let forward = DerivationRecord::new(
            vec![first.clone(), second.clone()],
            output.clone(),
            process.clone(),
            42,
        );
        let reversed = DerivationRecord::new(vec![second, first], output, process, 42);

        assert_ne!(forward.canonical_digest(), reversed.canonical_digest());
    }

    #[rstest]
    #[case("bad input!", "derived", "pipeline/v1", true)]
    #[case("data/a", "bad output!", "pipeline/v1", true)]
    #[case("data/a", "derived", "bad process!", true)]
    #[case("data/a", "derived", "pipeline/v1", false)]
    fn derivation_deserialization_validates_nested_fields(
        #[case] input_id: &str,
        #[case] output_id: &str,
        #[case] process: &str,
        #[case] canonical_digest: bool,
    ) {
        let mut digest = ContentDigest::digest_bytes(b"content").to_string();
        if !canonical_digest {
            digest.make_ascii_uppercase();
        }
        let untrusted = UntrustedRawDerivationRecord {
            inputs: vec![UntrustedArtifactRef {
                id: input_id,
                digest: &digest,
            }],
            output: UntrustedArtifactRef {
                id: output_id,
                digest: &digest,
            },
            process,
            timestamp_ns: 42,
        };
        let bytes = rmp_serde::to_vec_named(&untrusted).unwrap();

        assert!(rmp_serde::from_slice::<DerivationRecord>(&bytes).is_err());
    }

    #[derive(Serialize)]
    struct UntrustedDerivationRecord {
        inputs: Vec<ArtifactRef>,
        output: ArtifactRef,
        process: ProcessIdentity,
        timestamp_ns: u64,
    }

    #[derive(Serialize)]
    struct UntrustedArtifactRef<'a> {
        id: &'a str,
        digest: &'a str,
    }

    #[derive(Serialize)]
    struct UntrustedRawDerivationRecord<'a> {
        inputs: Vec<UntrustedArtifactRef<'a>>,
        output: UntrustedArtifactRef<'a>,
        process: &'a str,
        timestamp_ns: u64,
    }

    #[derive(Serialize)]
    struct UntrustedDatasetBinding {
        source: ArtifactRef,
        materialized_digest: ContentDigest,
        decoder: ProcessIdentity,
        replay_ordinal: u64,
        record_count: u64,
    }

    #[derive(Serialize)]
    struct UntrustedDatasetBindingWithExtra {
        source: ArtifactRef,
        materialized_digest: ContentDigest,
        decoder: ProcessIdentity,
        replay_ordinal: u64,
        record_count: u64,
        untrusted: bool,
    }

    #[derive(Serialize)]
    struct UntrustedStrategyBinding<'a> {
        artifact: ArtifactRef,
        entrypoint: &'a str,
        runtime_id: &'a str,
    }

    #[derive(Serialize)]
    struct UntrustedRunProvenanceWithExtra<'a> {
        data: Vec<DatasetBinding>,
        environment: Option<ExecutionEnvironmentBinding>,
        strategy: Option<StrategyBinding>,
        model: Option<ArtifactRef>,
        config: Option<ArtifactRef>,
        policy: Option<ArtifactRef>,
        evaluation: Option<ArtifactRef>,
        promotion: Option<ArtifactRef>,
        untrusted_market_assumption: &'a str,
    }

    #[derive(Serialize)]
    struct UntrustedExecutionEnvironmentBindingWithExtra {
        artifact: ArtifactRef,
        loader: ProcessIdentity,
        market_contexts: Vec<ArtifactRef>,
        venues: Vec<String>,
        instruments: Vec<String>,
        continuous_trading: bool,
    }

    #[derive(Serialize)]
    struct UntrustedExecutionEnvironmentScope {
        replay_start_ns: u64,
        replay_end_ns: u64,
        knowledge_cutoff_ns: u64,
        market_scopes: Vec<ExecutionMarketScope>,
    }

    #[derive(Serialize)]
    struct LegacyExecutionEnvironmentScope {
        replay_start_ns: u64,
        replay_end_ns: u64,
        knowledge_cutoff_ns: u64,
        market_profile_ids: Vec<String>,
        execution_route_ids: Vec<String>,
    }

    fn market_scope(profile: &str, listing: &str, route: &str) -> ExecutionMarketScope {
        let materialized_venue = listing.rsplit('/').next().unwrap();
        ExecutionMarketScope::new(materialized_venue, profile, listing, route).unwrap()
    }

    fn descriptor(id: &str, content: &[u8]) -> ArtifactDescriptor {
        ArtifactDescriptor::new(
            ArtifactId::new(id).unwrap(),
            ArtifactKind::Dataset,
            ContentDigest::digest_bytes(content),
        )
    }
}
