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

//! In-memory provenance store.

use std::collections::BTreeMap;

use crate::{
    ArtifactDescriptor, ArtifactId, ArtifactRef, ContentDigest, DerivationRecord, ProvenanceError,
    ProvenanceStore,
    store::{collect_ancestors, validate_derivation},
};

/// Deterministic in-memory implementation of [`ProvenanceStore`].
#[derive(Clone, Debug, Default)]
pub struct MemoryProvenanceStore {
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    digest_index: BTreeMap<ContentDigest, ArtifactId>,
    derivations: BTreeMap<ArtifactId, DerivationRecord>,
}

impl MemoryProvenanceStore {
    /// Creates an empty provenance store.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            artifacts: BTreeMap::new(),
            digest_index: BTreeMap::new(),
            derivations: BTreeMap::new(),
        }
    }
}

impl ProvenanceStore for MemoryProvenanceStore {
    fn register_artifact(&mut self, descriptor: ArtifactDescriptor) -> Result<(), ProvenanceError> {
        if let Some(existing) = self.artifacts.get(descriptor.id()) {
            return if existing == &descriptor {
                Ok(())
            } else {
                Err(ProvenanceError::ArtifactIdConflict(descriptor.id().clone()))
            };
        }
        if let Some(existing) = self.digest_index.get(descriptor.content_digest()) {
            return Err(ProvenanceError::DigestConflict {
                digest: *descriptor.content_digest(),
                existing: existing.clone(),
                requested: descriptor.id().clone(),
            });
        }

        self.digest_index
            .insert(*descriptor.content_digest(), descriptor.id().clone());
        self.artifacts.insert(descriptor.id().clone(), descriptor);
        Ok(())
    }

    fn get_artifact(&self, id: &ArtifactId) -> Result<Option<ArtifactDescriptor>, ProvenanceError> {
        Ok(self.artifacts.get(id).cloned())
    }

    fn record_derivation(&mut self, record: DerivationRecord) -> Result<(), ProvenanceError> {
        if let Some(existing) = self.derivations.get(record.output().id()) {
            return if existing == &record {
                Ok(())
            } else {
                Err(ProvenanceError::DerivationConflict(
                    record.output().id().clone(),
                ))
            };
        }
        validate_derivation(&self.artifacts, &self.derivations, &record)?;
        self.derivations
            .insert(record.output().id().clone(), record);
        Ok(())
    }

    fn get_derivation(
        &self,
        output: &ArtifactId,
    ) -> Result<Option<DerivationRecord>, ProvenanceError> {
        Ok(self.derivations.get(output).cloned())
    }

    fn ancestors(&self, artifact: &ArtifactId) -> Result<Vec<ArtifactRef>, ProvenanceError> {
        collect_ancestors(&self.artifacts, &self.derivations, artifact)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{ArtifactKind, ProcessIdentity};

    #[rstest]
    fn identical_insertions_are_idempotent() {
        let mut store = MemoryProvenanceStore::new();
        let source = artifact("source", b"source");
        let output = artifact("output", b"output");
        store.register_artifact(source.clone()).unwrap();
        store.register_artifact(source.clone()).unwrap();
        store.register_artifact(output.clone()).unwrap();

        let record = derivation(&source, &output, 1);
        store.record_derivation(record.clone()).unwrap();
        store.record_derivation(record.clone()).unwrap();
        assert_eq!(store.get_derivation(output.id()).unwrap(), Some(record));
    }

    #[rstest]
    fn rejects_id_and_digest_conflicts() {
        let mut store = MemoryProvenanceStore::new();
        let first = artifact("first", b"one");
        store.register_artifact(first.clone()).unwrap();

        let same_id = ArtifactDescriptor::new(
            first.id().clone(),
            ArtifactKind::Model,
            ContentDigest::digest_bytes(b"two"),
        );
        assert!(matches!(
            store.register_artifact(same_id),
            Err(ProvenanceError::ArtifactIdConflict(_))
        ));

        let same_digest = ArtifactDescriptor::new(
            ArtifactId::new("second").unwrap(),
            ArtifactKind::Dataset,
            *first.content_digest(),
        );
        assert!(matches!(
            store.register_artifact(same_digest),
            Err(ProvenanceError::DigestConflict { .. })
        ));
    }

    #[rstest]
    fn rejects_missing_reference_and_self_lineage() {
        let mut store = MemoryProvenanceStore::new();
        let output = artifact("output", b"output");
        store.register_artifact(output.clone()).unwrap();
        let missing = artifact("missing", b"missing");

        assert!(matches!(
            store.record_derivation(derivation(&missing, &output, 1)),
            Err(ProvenanceError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.record_derivation(derivation(&output, &output, 1)),
            Err(ProvenanceError::SelfLineage(_))
        ));
    }

    #[rstest]
    fn rejects_cycles_and_sorts_ancestors() {
        let mut store = MemoryProvenanceStore::new();
        let a = artifact("a", b"a");
        let b = artifact("b", b"b");
        let c = artifact("c", b"c");
        for descriptor in [&a, &b, &c] {
            store.register_artifact(descriptor.clone()).unwrap();
        }
        store.record_derivation(derivation(&b, &a, 1)).unwrap();
        store.record_derivation(derivation(&c, &b, 2)).unwrap();

        assert!(matches!(
            store.record_derivation(derivation(&a, &c, 3)),
            Err(ProvenanceError::Cycle { .. })
        ));
        assert_eq!(
            store.ancestors(a.id()).unwrap(),
            vec![b.artifact_ref(), c.artifact_ref()]
        );
    }

    #[rstest]
    fn rejects_reference_with_wrong_digest() {
        let mut store = MemoryProvenanceStore::new();
        let source = artifact("source", b"source");
        let output = artifact("output", b"output");
        store.register_artifact(source.clone()).unwrap();
        store.register_artifact(output.clone()).unwrap();
        let wrong = ArtifactRef::new(source.id().clone(), ContentDigest::digest_bytes(b"wrong"));
        let record = DerivationRecord::new(
            vec![wrong],
            output.artifact_ref(),
            ProcessIdentity::new("test/v1").unwrap(),
            1,
        );
        assert!(matches!(
            store.record_derivation(record),
            Err(ProvenanceError::ReferenceDigestMismatch { .. })
        ));
    }

    fn artifact(id: &str, content: &[u8]) -> ArtifactDescriptor {
        ArtifactDescriptor::new(
            ArtifactId::new(id).unwrap(),
            ArtifactKind::Dataset,
            ContentDigest::digest_bytes(content),
        )
    }

    fn derivation(
        input: &ArtifactDescriptor,
        output: &ArtifactDescriptor,
        timestamp_ns: u64,
    ) -> DerivationRecord {
        DerivationRecord::new(
            vec![input.artifact_ref()],
            output.artifact_ref(),
            ProcessIdentity::new("test/v1").unwrap(),
            timestamp_ns,
        )
    }
}
