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

//! Durable [`redb`] provenance store.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
    fs::OpenOptions,
    io::Cursor,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    ArtifactDescriptor, ArtifactId, ArtifactRef, ContentDigest, DerivationRecord, ProvenanceError,
    ProvenanceStore,
    store::{collect_ancestors, validate_derivation, validate_snapshot},
};

const ARTIFACTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("artifacts");
const DIGEST_INDEX_TABLE: TableDefinition<&[u8], &str> = TableDefinition::new("digest_index");
const DERIVATIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("derivations");
const METADATA_TABLE: TableDefinition<&str, u32> = TableDefinition::new("metadata");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const PROVENANCE_STORE_SCHEMA_VERSION: u32 = 1;

type ArtifactMap = BTreeMap<ArtifactId, ArtifactDescriptor>;
type DigestIndexMap = BTreeMap<ContentDigest, ArtifactId>;
type DerivationMap = BTreeMap<ArtifactId, DerivationRecord>;

/// A durable [`ProvenanceStore`] backed by a single [`redb`] database file.
pub struct RedbProvenanceStore {
    db: Database,
    path: PathBuf,
}

impl Debug for RedbProvenanceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbProvenanceStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RedbProvenanceStore {
    /// Creates or opens a provenance database at `path` and initializes its schema.
    ///
    /// # Errors
    ///
    /// Returns [`ProvenanceError::Backend`] when the database cannot be opened or initialized, or
    /// a typed validation/corruption error when persisted artifacts or lineage violate invariants.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ProvenanceError> {
        let path = path.as_ref().to_path_buf();
        let (db, created) = create_or_open_database(&path)?;
        if created {
            initialize_store_schema(&db)?;
        } else {
            verify_store_schema(&db)?;
        }
        let store = Self { db, path };
        store.snapshots()?;
        Ok(store)
    }

    /// Returns the backing database path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn snapshots(&self) -> Result<(ArtifactMap, DerivationMap), ProvenanceError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let artifacts = {
            let table = txn.open_table(ARTIFACTS_TABLE).map_err(backend)?;
            load_artifacts(&table)?
        };
        let digest_index = {
            let table = txn.open_table(DIGEST_INDEX_TABLE).map_err(backend)?;
            load_digest_index(&table)?
        };
        let derivations = {
            let table = txn.open_table(DERIVATIONS_TABLE).map_err(backend)?;
            load_derivations(&table)?
        };
        validate_persisted_snapshot(&artifacts, &digest_index, &derivations)?;
        Ok((artifacts, derivations))
    }
}

fn create_or_open_database(path: &Path) -> Result<(Database, bool), ProvenanceError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Database::builder()
            .create_file(file)
            .map(|db| (db, true))
            .map_err(backend),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            Database::open(path).map(|db| (db, false)).map_err(backend)
        }
        Err(error) => Err(backend(format!(
            "atomically create provenance store {}: {error}",
            path.display()
        ))),
    }
}

fn initialize_store_schema(db: &Database) -> Result<(), ProvenanceError> {
    let mut txn = db.begin_write().map_err(backend)?;
    txn.set_durability(Durability::Immediate).map_err(backend)?;
    {
        txn.open_table(ARTIFACTS_TABLE).map_err(backend)?;
        txn.open_table(DIGEST_INDEX_TABLE).map_err(backend)?;
        txn.open_table(DERIVATIONS_TABLE).map_err(backend)?;
        let mut metadata = txn.open_table(METADATA_TABLE).map_err(backend)?;
        metadata
            .insert(SCHEMA_VERSION_KEY, PROVENANCE_STORE_SCHEMA_VERSION)
            .map_err(backend)?;
    }
    txn.commit().map_err(backend)
}

impl ProvenanceStore for RedbProvenanceStore {
    fn register_artifact(&mut self, descriptor: ArtifactDescriptor) -> Result<(), ProvenanceError> {
        let mut txn = self.db.begin_write().map_err(backend)?;
        txn.set_durability(Durability::Immediate).map_err(backend)?;

        let artifacts = {
            let table = txn.open_table(ARTIFACTS_TABLE).map_err(backend)?;
            load_artifacts(&table)?
        };
        let digest_index = {
            let table = txn.open_table(DIGEST_INDEX_TABLE).map_err(backend)?;
            load_digest_index(&table)?
        };
        let derivations = {
            let table = txn.open_table(DERIVATIONS_TABLE).map_err(backend)?;
            load_derivations(&table)?
        };
        validate_persisted_snapshot(&artifacts, &digest_index, &derivations)?;

        let existing_descriptor = artifacts.get(descriptor.id()).cloned();
        if let Some(existing) = &existing_descriptor
            && existing != &descriptor
        {
            return Err(ProvenanceError::ArtifactIdConflict(descriptor.id().clone()));
        }

        let digest_owner = digest_index.get(descriptor.content_digest()).cloned();
        if let Some(existing) = digest_owner {
            if &existing != descriptor.id() {
                return Err(ProvenanceError::DigestConflict {
                    digest: *descriptor.content_digest(),
                    existing,
                    requested: descriptor.id().clone(),
                });
            }
            if existing_descriptor.is_some() {
                return Ok(());
            }
            return Err(ProvenanceError::Corrupt(format!(
                "digest index points to missing artifact `{}`",
                descriptor.id()
            )));
        }
        if existing_descriptor.is_some() {
            return Err(ProvenanceError::Corrupt(format!(
                "artifact `{}` is missing its digest index",
                descriptor.id()
            )));
        }

        let bytes = encode(&descriptor, "artifact descriptor")?;
        {
            let mut artifacts = txn.open_table(ARTIFACTS_TABLE).map_err(backend)?;
            artifacts
                .insert(descriptor.id().as_str(), bytes.as_slice())
                .map_err(backend)?;
        }
        {
            let mut digests = txn.open_table(DIGEST_INDEX_TABLE).map_err(backend)?;
            digests
                .insert(
                    descriptor.content_digest().as_bytes().as_slice(),
                    descriptor.id().as_str(),
                )
                .map_err(backend)?;
        }
        txn.commit().map_err(backend)
    }

    fn get_artifact(&self, id: &ArtifactId) -> Result<Option<ArtifactDescriptor>, ProvenanceError> {
        let (artifacts, _) = self.snapshots()?;
        Ok(artifacts.get(id).cloned())
    }

    fn record_derivation(&mut self, record: DerivationRecord) -> Result<(), ProvenanceError> {
        let mut txn = self.db.begin_write().map_err(backend)?;
        txn.set_durability(Durability::Immediate).map_err(backend)?;

        let artifacts = {
            let table = txn.open_table(ARTIFACTS_TABLE).map_err(backend)?;
            load_artifacts(&table)?
        };
        let digest_index = {
            let table = txn.open_table(DIGEST_INDEX_TABLE).map_err(backend)?;
            load_digest_index(&table)?
        };
        let derivations = {
            let table = txn.open_table(DERIVATIONS_TABLE).map_err(backend)?;
            load_derivations(&table)?
        };
        validate_persisted_snapshot(&artifacts, &digest_index, &derivations)?;
        if let Some(existing) = derivations.get(record.output().id()) {
            return if existing == &record {
                Ok(())
            } else {
                Err(ProvenanceError::DerivationConflict(
                    record.output().id().clone(),
                ))
            };
        }
        validate_derivation(&artifacts, &derivations, &record)?;

        let bytes = encode(&record, "derivation record")?;
        {
            let mut table = txn.open_table(DERIVATIONS_TABLE).map_err(backend)?;
            table
                .insert(record.output().id().as_str(), bytes.as_slice())
                .map_err(backend)?;
        }
        txn.commit().map_err(backend)
    }

    fn get_derivation(
        &self,
        output: &ArtifactId,
    ) -> Result<Option<DerivationRecord>, ProvenanceError> {
        let (_, derivations) = self.snapshots()?;
        Ok(derivations.get(output).cloned())
    }

    fn ancestors(&self, artifact: &ArtifactId) -> Result<Vec<ArtifactRef>, ProvenanceError> {
        let (artifacts, derivations) = self.snapshots()?;
        collect_ancestors(&artifacts, &derivations, artifact)
    }
}

fn load_artifacts<T>(table: &T) -> Result<BTreeMap<ArtifactId, ArtifactDescriptor>, ProvenanceError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let mut artifacts = BTreeMap::new();
    for item in table.iter().map_err(backend)? {
        let (key, value) = item.map_err(backend)?;
        let key = ArtifactId::new(key.value())
            .map_err(|error| ProvenanceError::Corrupt(error.to_string()))?;
        let descriptor: ArtifactDescriptor = decode(value.value(), "artifact descriptor")?;
        if descriptor.id() != &key {
            return Err(ProvenanceError::Corrupt(format!(
                "artifact table key `{key}` disagrees with stored descriptor"
            )));
        }
        artifacts.insert(key, descriptor);
    }
    Ok(artifacts)
}

fn load_digest_index<T>(table: &T) -> Result<DigestIndexMap, ProvenanceError>
where
    T: ReadableTable<&'static [u8], &'static str>,
{
    let mut digest_index = BTreeMap::new();
    for item in table.iter().map_err(backend)? {
        let (key, value) = item.map_err(backend)?;
        let digest_bytes: [u8; 32] = key.value().try_into().map_err(|_| {
            ProvenanceError::Corrupt(format!(
                "digest index key has {} bytes; expected 32",
                key.value().len()
            ))
        })?;
        let digest = ContentDigest::from_array(digest_bytes);
        let owner = ArtifactId::new(value.value())
            .map_err(|error| ProvenanceError::Corrupt(error.to_string()))?;
        digest_index.insert(digest, owner);
    }
    Ok(digest_index)
}

fn load_derivations<T>(table: &T) -> Result<BTreeMap<ArtifactId, DerivationRecord>, ProvenanceError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let mut derivations = BTreeMap::new();
    for item in table.iter().map_err(backend)? {
        let (key, value) = item.map_err(backend)?;
        let key = ArtifactId::new(key.value())
            .map_err(|error| ProvenanceError::Corrupt(error.to_string()))?;
        let record: DerivationRecord = decode(value.value(), "derivation record")?;
        if record.output().id() != &key {
            return Err(ProvenanceError::Corrupt(format!(
                "derivation table key `{key}` disagrees with stored output"
            )));
        }
        derivations.insert(key, record);
    }
    Ok(derivations)
}

fn validate_persisted_snapshot(
    artifacts: &ArtifactMap,
    digest_index: &DigestIndexMap,
    derivations: &DerivationMap,
) -> Result<(), ProvenanceError> {
    validate_snapshot(artifacts, derivations)?;

    if digest_index.len() != artifacts.len() {
        return Err(ProvenanceError::Corrupt(format!(
            "digest index contains {} entries for {} artifacts",
            digest_index.len(),
            artifacts.len()
        )));
    }
    for (id, descriptor) in artifacts {
        match digest_index.get(descriptor.content_digest()) {
            Some(owner) if owner == id => {}
            Some(owner) => {
                return Err(ProvenanceError::Corrupt(format!(
                    "digest `{}` index points to `{owner}`, not `{id}`",
                    descriptor.content_digest()
                )));
            }
            None => {
                return Err(ProvenanceError::Corrupt(format!(
                    "artifact `{id}` is missing its digest index entry"
                )));
            }
        }
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T, kind: &str) -> Result<Vec<u8>, ProvenanceError> {
    rmp_serde::to_vec_named(value)
        .map_err(|error| ProvenanceError::Backend(format!("encode {kind}: {error}")))
}

fn decode<T: DeserializeOwned>(bytes: &[u8], kind: &str) -> Result<T, ProvenanceError> {
    let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(bytes));
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| ProvenanceError::Corrupt(format!("decode {kind}: {error}")))?;
    let position = usize::try_from(deserializer.position()).map_err(|error| {
        ProvenanceError::Corrupt(format!("decode {kind}: invalid cursor position: {error}"))
    })?;
    if position != bytes.len() {
        return Err(ProvenanceError::Corrupt(format!(
            "decode {kind}: {} trailing byte(s)",
            bytes.len() - position
        )));
    }
    Ok(value)
}

fn verify_store_schema(db: &Database) -> Result<(), ProvenanceError> {
    let txn = db.begin_read().map_err(backend)?;
    let tables: BTreeSet<String> = txn
        .list_tables()
        .map_err(backend)?
        .map(|table| table.name().to_string())
        .collect();
    for required in [
        ARTIFACTS_TABLE.name(),
        DIGEST_INDEX_TABLE.name(),
        DERIVATIONS_TABLE.name(),
        METADATA_TABLE.name(),
    ] {
        if !tables.contains(required) {
            return Err(ProvenanceError::Corrupt(format!(
                "missing required provenance store table `{required}`"
            )));
        }
    }
    let table = txn.open_table(METADATA_TABLE).map_err(|error| {
        ProvenanceError::Corrupt(format!("missing provenance store schema metadata: {error}"))
    })?;
    let version = table
        .get(SCHEMA_VERSION_KEY)
        .map_err(backend)?
        .ok_or_else(|| {
            ProvenanceError::Corrupt("missing provenance store schema version".to_string())
        })?
        .value();
    if version != PROVENANCE_STORE_SCHEMA_VERSION {
        return Err(ProvenanceError::Corrupt(format!(
            "unsupported provenance store schema version {version}; supported version is {PROVENANCE_STORE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn backend(error: impl Display) -> ProvenanceError {
    ProvenanceError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Serialize;
    use tempfile::TempDir;

    use super::*;
    use crate::{ArtifactKind, ContentDigest, ProcessIdentity};

    #[rstest]
    fn persists_and_reopens_lineage() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("provenance.redb");
        let source = artifact("source", b"source");
        let output = artifact("output", b"output");
        let record = DerivationRecord::new(
            vec![source.artifact_ref()],
            output.artifact_ref(),
            ProcessIdentity::new("pipeline/v1").unwrap(),
            42,
        );
        {
            let mut store = RedbProvenanceStore::open(&path).unwrap();
            store.register_artifact(source.clone()).unwrap();
            store.register_artifact(output.clone()).unwrap();
            store.record_derivation(record.clone()).unwrap();
        }

        let store = RedbProvenanceStore::open(&path).unwrap();
        assert_eq!(store.get_artifact(output.id()).unwrap(), Some(output));
        assert_eq!(
            store.get_derivation(record.output().id()).unwrap(),
            Some(record)
        );
        let output_id = ArtifactId::new("output").unwrap();
        assert_eq!(
            store.ancestors(&output_id).unwrap(),
            vec![source.artifact_ref()]
        );
    }

    #[rstest]
    fn rejects_missing_or_future_store_schema_metadata() {
        let temp = TempDir::new().unwrap();
        let missing_path = temp.path().join("missing-schema.redb");
        {
            let db = Database::create(&missing_path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(ARTIFACTS_TABLE).unwrap();
            txn.commit().unwrap();
        }
        assert!(matches!(
            RedbProvenanceStore::open(&missing_path),
            Err(ProvenanceError::Corrupt(message)) if message.contains("missing required provenance store table")
        ));
        let db = Database::open(&missing_path).unwrap();
        let txn = db.begin_read().unwrap();
        assert!(
            txn.open_table(METADATA_TABLE).is_err(),
            "failed open must not backfill schema metadata"
        );
        drop(txn);
        drop(db);

        let future_path = temp.path().join("future-schema.redb");
        drop(RedbProvenanceStore::open(&future_path).unwrap());
        {
            let db = Database::create(&future_path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(METADATA_TABLE).unwrap();
                table
                    .insert(SCHEMA_VERSION_KEY, PROVENANCE_STORE_SCHEMA_VERSION + 1)
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        assert!(matches!(
            RedbProvenanceStore::open(&future_path),
            Err(ProvenanceError::Corrupt(message)) if message.contains("unsupported provenance store schema")
        ));
    }

    #[rstest]
    fn rejects_precreated_empty_file_without_initializing_it() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("precreated-empty.redb");
        std::fs::File::create(&path).unwrap();

        assert!(RedbProvenanceStore::open(&path).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[rstest]
    fn concurrent_open_cannot_reinitialize_an_owned_store() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("concurrent.redb");
        let first = RedbProvenanceStore::open(&path).unwrap();

        assert!(matches!(
            RedbProvenanceStore::open(&path),
            Err(ProvenanceError::Backend(message)) if message.to_ascii_lowercase().contains("already open")
        ));

        drop(first);
        RedbProvenanceStore::open(&path).expect("store remains valid after lock release");
    }

    #[rstest]
    fn rejects_trailing_messagepack_bytes() {
        let descriptor = artifact("trailing", b"payload");
        let mut bytes = encode(&descriptor, "artifact").unwrap();
        bytes.push(0xc0);

        assert!(matches!(
            decode::<ArtifactDescriptor>(&bytes, "artifact"),
            Err(ProvenanceError::Corrupt(message)) if message.contains("trailing byte")
        ));
    }

    #[rstest]
    fn enforces_conflicts_in_persistent_store() {
        let temp = TempDir::new().unwrap();
        let mut store = RedbProvenanceStore::open(temp.path().join("provenance.redb")).unwrap();
        let first = artifact("first", b"same");
        store.register_artifact(first.clone()).unwrap();
        store.register_artifact(first.clone()).unwrap();
        let conflicting = ArtifactDescriptor::new(
            ArtifactId::new("second").unwrap(),
            ArtifactKind::Dataset,
            *first.content_digest(),
        );
        assert!(matches!(
            store.register_artifact(conflicting),
            Err(ProvenanceError::DigestConflict { .. })
        ));
    }

    #[rstest]
    fn preserves_order_and_multiplicity_of_persisted_derivation() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("provenance.redb");
        let mut store = RedbProvenanceStore::open(&path).unwrap();
        let first = artifact("first", b"first");
        let second = artifact("second", b"second");
        let output = artifact("output", b"output");
        for descriptor in [&first, &second, &output] {
            store.register_artifact(descriptor.clone()).unwrap();
        }

        let process = ProcessIdentity::new("pipeline/v1").unwrap();
        let untrusted = UntrustedDerivationRecord {
            inputs: vec![
                second.artifact_ref(),
                first.artifact_ref(),
                second.artifact_ref(),
            ],
            output: output.artifact_ref(),
            process: process.clone(),
            timestamp_ns: 42,
        };
        let bytes = rmp_serde::to_vec_named(&untrusted).unwrap();
        {
            let txn = store.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(DERIVATIONS_TABLE).unwrap();
                table
                    .insert(output.id().as_str(), bytes.as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }

        drop(store);
        let store = RedbProvenanceStore::open(&path).unwrap();
        let loaded = store.get_derivation(output.id()).unwrap().unwrap();
        let canonical = DerivationRecord::new(
            vec![
                second.artifact_ref(),
                first.artifact_ref(),
                second.artifact_ref(),
            ],
            output.artifact_ref(),
            process,
            42,
        );
        assert_eq!(loaded, canonical);
        assert_eq!(loaded.canonical_digest(), canonical.canonical_digest());
        assert_eq!(loaded.inputs().len(), 3);
    }

    #[rstest]
    fn rejects_raw_persisted_self_lineage_across_read_apis() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("provenance.redb");
        let mut store = RedbProvenanceStore::open(&path).unwrap();
        let artifact = artifact("self", b"self");
        store.register_artifact(artifact.clone()).unwrap();
        let record = DerivationRecord::new(
            vec![artifact.artifact_ref()],
            artifact.artifact_ref(),
            ProcessIdentity::new("tampered/raw").unwrap(),
            1,
        );
        inject_derivations(&store, &[record]);

        for error in [
            store.get_artifact(artifact.id()).unwrap_err(),
            store.get_derivation(artifact.id()).unwrap_err(),
            store.ancestors(artifact.id()).unwrap_err(),
        ] {
            assert_eq!(error, ProvenanceError::SelfLineage(artifact.id().clone()));
        }

        drop(store);
        assert!(matches!(
            RedbProvenanceStore::open(path),
            Err(ProvenanceError::SelfLineage(id)) if id == artifact.id().clone()
        ));
    }

    #[rstest]
    fn rejects_raw_persisted_two_node_cycle() {
        let temp = TempDir::new().unwrap();
        let mut store = RedbProvenanceStore::open(temp.path().join("provenance.redb")).unwrap();
        let first = artifact("first", b"first");
        let second = artifact("second", b"second");
        for descriptor in [&first, &second] {
            store.register_artifact(descriptor.clone()).unwrap();
        }
        let process = ProcessIdentity::new("tampered/raw").unwrap();
        inject_derivations(
            &store,
            &[
                DerivationRecord::new(
                    vec![second.artifact_ref()],
                    first.artifact_ref(),
                    process.clone(),
                    1,
                ),
                DerivationRecord::new(
                    vec![first.artifact_ref()],
                    second.artifact_ref(),
                    process,
                    2,
                ),
            ],
        );

        assert!(matches!(
            store.ancestors(first.id()),
            Err(ProvenanceError::Cycle { .. })
        ));
    }

    #[rstest]
    fn rejects_raw_persisted_reference_digest_mismatch() {
        let temp = TempDir::new().unwrap();
        let mut store = RedbProvenanceStore::open(temp.path().join("provenance.redb")).unwrap();
        let source = artifact("source", b"source");
        let output = artifact("output", b"output");
        for descriptor in [&source, &output] {
            store.register_artifact(descriptor.clone()).unwrap();
        }
        let received = ContentDigest::digest_bytes(b"substituted source");
        inject_derivations(
            &store,
            &[DerivationRecord::new(
                vec![ArtifactRef::new(source.id().clone(), received)],
                output.artifact_ref(),
                ProcessIdentity::new("tampered/raw").unwrap(),
                1,
            )],
        );

        assert!(matches!(
            store.ancestors(output.id()),
            Err(ProvenanceError::ReferenceDigestMismatch {
                id,
                expected,
                received: actual,
            }) if id == source.id().clone()
                && expected == *source.content_digest()
                && actual == received
        ));
    }

    #[rstest]
    fn rejects_raw_persisted_digest_index_rebinding() {
        let temp = TempDir::new().unwrap();
        let mut store = RedbProvenanceStore::open(temp.path().join("provenance.redb")).unwrap();
        let first = artifact("first", b"first");
        let second = artifact("second", b"second");
        for descriptor in [&first, &second] {
            store.register_artifact(descriptor.clone()).unwrap();
        }
        {
            let txn = store.db.begin_write().unwrap();
            {
                let mut table = txn.open_table(DIGEST_INDEX_TABLE).unwrap();
                table
                    .insert(
                        first.content_digest().as_bytes().as_slice(),
                        second.id().as_str(),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }

        assert!(matches!(
            store.get_artifact(second.id()),
            Err(ProvenanceError::Corrupt(message))
                if message.contains("index points to `second`, not `first`")
        ));
    }

    #[rstest]
    fn rejects_raw_persisted_missing_reference() {
        let temp = TempDir::new().unwrap();
        let mut store = RedbProvenanceStore::open(temp.path().join("provenance.redb")).unwrap();
        let output = artifact("output", b"output");
        store.register_artifact(output.clone()).unwrap();
        let missing_id = ArtifactId::new("missing").unwrap();
        inject_derivations(
            &store,
            &[DerivationRecord::new(
                vec![ArtifactRef::new(
                    missing_id.clone(),
                    ContentDigest::digest_bytes(b"missing"),
                )],
                output.artifact_ref(),
                ProcessIdentity::new("tampered/raw").unwrap(),
                1,
            )],
        );

        assert_eq!(
            store.ancestors(output.id()).unwrap_err(),
            ProvenanceError::MissingArtifact(missing_id)
        );
    }

    #[derive(Serialize)]
    struct UntrustedDerivationRecord {
        inputs: Vec<ArtifactRef>,
        output: ArtifactRef,
        process: ProcessIdentity,
        timestamp_ns: u64,
    }

    fn inject_derivations(store: &RedbProvenanceStore, records: &[DerivationRecord]) {
        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(DERIVATIONS_TABLE).unwrap();
            for record in records {
                let bytes = rmp_serde::to_vec_named(record).unwrap();
                table
                    .insert(record.output().id().as_str(), bytes.as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }

    fn artifact(id: &str, content: &[u8]) -> ArtifactDescriptor {
        ArtifactDescriptor::new(
            ArtifactId::new(id).unwrap(),
            ArtifactKind::Dataset,
            ContentDigest::digest_bytes(content),
        )
    }
}
