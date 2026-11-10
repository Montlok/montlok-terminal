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

//! Error types for provenance models and stores.

use thiserror::Error;

use crate::{ArtifactId, ContentDigest};

/// Errors returned while constructing or persisting provenance.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProvenanceError {
    /// A streaming digest received more bytes than its declared content length.
    #[error("streaming content digest exceeded declared length {expected}: observed {observed}")]
    ContentDigestLengthExceeded {
        /// Declared content length.
        expected: u64,
        /// Bytes supplied after the rejected update.
        observed: u64,
    },
    /// A streaming digest was finalized before its declared content length was supplied.
    #[error("streaming content digest length mismatch: expected {expected}, observed {observed}")]
    ContentDigestLengthMismatch {
        /// Declared content length.
        expected: u64,
        /// Bytes supplied before finalization.
        observed: u64,
    },
    /// A content digest was not canonical lowercase BLAKE3 hexadecimal.
    #[error("invalid BLAKE3 digest `{value}`: {reason}")]
    InvalidDigest {
        /// The rejected value.
        value: String,
        /// Why validation failed.
        reason: &'static str,
    },
    /// An identifier was empty, too long, or contained an unsupported character.
    #[error("invalid {kind} `{value}`: {reason}")]
    InvalidIdentifier {
        /// The identifier category.
        kind: &'static str,
        /// The rejected value.
        value: String,
        /// Why validation failed.
        reason: &'static str,
    },
    /// A dataset binding did not contain any materialized records.
    #[error("dataset artifact `{0}` must contain at least one materialized record")]
    EmptyDataset(ArtifactId),
    /// An execution-environment binding omitted required venue or instrument identities.
    #[error("execution environment must contain at least one {field}")]
    EmptyExecutionEnvironment {
        /// The missing environment dimension.
        field: &'static str,
    },
    /// More than one market-scope tuple claimed the same materialized listing venue.
    #[error(
        "execution environment contains multiple market scopes for listing `{listing_venue_id}`"
    )]
    DuplicateMarketScopeListing {
        /// Ambiguously paired listing identity.
        listing_venue_id: String,
    },
    /// More than one market-scope tuple claimed the same materialized venue.
    #[error(
        "execution environment contains multiple market scopes for materialized venue `{materialized_venue_id}`"
    )]
    DuplicateMarketScopeVenue {
        /// Ambiguously paired materialized venue identity.
        materialized_venue_id: String,
    },
    /// Market-scope venue keys did not exactly match the materialized venue universe.
    #[error(
        "execution environment market-scope venues {scope_venues:?} do not match materialized venues {binding_venues:?}"
    )]
    MarketScopeVenueMismatch {
        /// Canonical materialized venues from the environment binding.
        binding_venues: Vec<String>,
        /// Canonical materialized venue keys from paired market scopes.
        scope_venues: Vec<String>,
    },
    /// A replay scope ended at or before it started.
    #[error("invalid replay window [{replay_start_ns}, {replay_end_ns})")]
    InvalidReplayWindow {
        /// Inclusive replay start in Unix nanoseconds.
        replay_start_ns: u64,
        /// Exclusive replay end in Unix nanoseconds.
        replay_end_ns: u64,
    },
    /// Point-in-time knowledge included information newer than the replay start.
    #[error("knowledge cutoff {knowledge_cutoff_ns} exceeds replay start {replay_start_ns}")]
    KnowledgeCutoffAfterReplayStart {
        /// Point-in-time knowledge cutoff in Unix nanoseconds.
        knowledge_cutoff_ns: u64,
        /// Inclusive replay start in Unix nanoseconds.
        replay_start_ns: u64,
    },
    /// An artifact id was already bound to a different immutable descriptor.
    #[error("artifact id conflict for `{0}`")]
    ArtifactIdConflict(ArtifactId),
    /// A digest was already bound to a different artifact id.
    #[error("digest `{digest}` is already bound to `{existing}`, not `{requested}`")]
    DigestConflict {
        /// The digest with an existing binding.
        digest: ContentDigest,
        /// The existing artifact id.
        existing: ArtifactId,
        /// The attempted artifact id.
        requested: ArtifactId,
    },
    /// An artifact reference named an artifact absent from the store.
    #[error("artifact `{0}` is not registered")]
    MissingArtifact(ArtifactId),
    /// An artifact reference carried a digest different from the registered descriptor.
    #[error("artifact `{id}` digest mismatch: expected `{expected}`, received `{received}`")]
    ReferenceDigestMismatch {
        /// The referenced artifact id.
        id: ArtifactId,
        /// The registered digest.
        expected: ContentDigest,
        /// The digest in the reference.
        received: ContentDigest,
    },
    /// An output artifact was listed as its own direct input.
    #[error("artifact `{0}` cannot derive from itself")]
    SelfLineage(ArtifactId),
    /// A derivation would introduce a cycle in the lineage graph.
    #[error("derivation of `{output}` from `{input}` would create a lineage cycle")]
    Cycle {
        /// The proposed output artifact.
        output: ArtifactId,
        /// The input through which the cycle would be introduced.
        input: ArtifactId,
    },
    /// An output artifact already had a different derivation.
    #[error("derivation conflict for output artifact `{0}`")]
    DerivationConflict(ArtifactId),
    /// A persistent backend operation failed.
    #[error("provenance backend error: {0}")]
    Backend(String),
    /// Persisted provenance could not be decoded or violated storage invariants.
    #[error("corrupt provenance store: {0}")]
    Corrupt(String),
}
