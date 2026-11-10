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

//! Provenance store contract and backend-independent graph validation.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ArtifactDescriptor, ArtifactId, ArtifactRef, DerivationRecord, ProvenanceError};

/// Storage contract for immutable artifacts and their derivation graph.
pub trait ProvenanceStore {
    /// Registers an immutable artifact descriptor.
    ///
    /// Identical registration is idempotent. Rebinding either its id or digest is rejected.
    ///
    /// # Errors
    ///
    /// Returns a typed conflict, validation, corruption, or backend error.
    fn register_artifact(&mut self, descriptor: ArtifactDescriptor) -> Result<(), ProvenanceError>;

    /// Gets an artifact descriptor by id.
    ///
    /// # Errors
    ///
    /// Returns a backend, corruption, or typed graph validation error when storage cannot be read
    /// safely.
    fn get_artifact(&self, id: &ArtifactId) -> Result<Option<ArtifactDescriptor>, ProvenanceError>;

    /// Records the unique derivation for an output artifact.
    ///
    /// Identical insertion is idempotent. All references must already be registered and
    /// self-lineage, cycles, and replacement of an existing derivation are rejected.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, conflict, backend, or corruption error.
    fn record_derivation(&mut self, record: DerivationRecord) -> Result<(), ProvenanceError>;

    /// Gets the derivation which produced `output`.
    ///
    /// # Errors
    ///
    /// Returns a backend, corruption, or typed graph validation error when storage cannot be read
    /// safely.
    fn get_derivation(
        &self,
        output: &ArtifactId,
    ) -> Result<Option<DerivationRecord>, ProvenanceError>;

    /// Returns every transitive ancestor of `artifact` in deterministic sorted order.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::MissingArtifact`] when the starting artifact or a persisted
    /// lineage reference is missing, and a typed backend/corruption error for unsafe storage.
    fn ancestors(&self, artifact: &ArtifactId) -> Result<Vec<ArtifactRef>, ProvenanceError>;
}

pub(crate) fn validate_reference(
    artifacts: &BTreeMap<ArtifactId, ArtifactDescriptor>,
    reference: &ArtifactRef,
) -> Result<(), ProvenanceError> {
    let descriptor = artifacts
        .get(reference.id())
        .ok_or_else(|| ProvenanceError::MissingArtifact(reference.id().clone()))?;
    if descriptor.content_digest() != reference.digest() {
        return Err(ProvenanceError::ReferenceDigestMismatch {
            id: reference.id().clone(),
            expected: *descriptor.content_digest(),
            received: *reference.digest(),
        });
    }
    Ok(())
}

pub(crate) fn validate_derivation(
    artifacts: &BTreeMap<ArtifactId, ArtifactDescriptor>,
    derivations: &BTreeMap<ArtifactId, DerivationRecord>,
    record: &DerivationRecord,
) -> Result<(), ProvenanceError> {
    validate_reference(artifacts, record.output())?;
    for input in record.inputs() {
        validate_reference(artifacts, input)?;
        if input.id() == record.output().id() {
            return Err(ProvenanceError::SelfLineage(input.id().clone()));
        }
        if reaches(derivations, input.id(), record.output().id()) {
            return Err(ProvenanceError::Cycle {
                output: record.output().id().clone(),
                input: input.id().clone(),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "redb")]
pub(crate) fn validate_snapshot(
    artifacts: &BTreeMap<ArtifactId, ArtifactDescriptor>,
    derivations: &BTreeMap<ArtifactId, DerivationRecord>,
) -> Result<(), ProvenanceError> {
    let mut digest_owners = BTreeMap::new();
    for (id, descriptor) in artifacts {
        if descriptor.id() != id {
            return Err(ProvenanceError::Corrupt(format!(
                "artifact map key `{id}` disagrees with stored descriptor"
            )));
        }
        if let Some(existing) = digest_owners.insert(*descriptor.content_digest(), id) {
            return Err(ProvenanceError::Corrupt(format!(
                "digest `{}` is bound to both `{existing}` and `{id}`",
                descriptor.content_digest()
            )));
        }
    }

    for (output, record) in derivations {
        if record.output().id() != output {
            return Err(ProvenanceError::Corrupt(format!(
                "derivation map key `{output}` disagrees with stored output"
            )));
        }
        validate_derivation(artifacts, derivations, record)?;
    }
    Ok(())
}

pub(crate) fn collect_ancestors(
    artifacts: &BTreeMap<ArtifactId, ArtifactDescriptor>,
    derivations: &BTreeMap<ArtifactId, DerivationRecord>,
    artifact: &ArtifactId,
) -> Result<Vec<ArtifactRef>, ProvenanceError> {
    if !artifacts.contains_key(artifact) {
        return Err(ProvenanceError::MissingArtifact(artifact.clone()));
    }

    let mut pending = BTreeSet::from([artifact.clone()]);
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop_first() {
        if let Some(record) = derivations.get(&current) {
            for input in record.inputs() {
                validate_reference(artifacts, input)?;
                if input.id() != artifact && visited.insert(input.id().clone()) {
                    pending.insert(input.id().clone());
                }
            }
        }
    }

    visited
        .into_iter()
        .map(|id| {
            artifacts
                .get(&id)
                .map(ArtifactDescriptor::artifact_ref)
                .ok_or(ProvenanceError::MissingArtifact(id))
        })
        .collect()
}

fn reaches(
    derivations: &BTreeMap<ArtifactId, DerivationRecord>,
    start: &ArtifactId,
    target: &ArtifactId,
) -> bool {
    let mut pending = vec![start];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if current == target {
            return true;
        }
        if visited.insert(current.clone())
            && let Some(record) = derivations.get(current)
        {
            pending.extend(record.inputs().iter().map(ArtifactRef::id));
        }
    }
    false
}
