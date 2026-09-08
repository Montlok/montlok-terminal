#![forbid(unsafe_code)]

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use montlok_contracts::v2;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Duplicate,
}

pub struct ProjectionStore {
    db: Mutex<Connection>,
    archiving: Mutex<()>,
    parquet_root: PathBuf,
}

impl ProjectionStore {
    pub fn open(database: impl AsRef<Path>, parquet_root: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = database.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir_all(parquet_root.as_ref())?;
        let db = Connection::open(database)?;
        Self::initialize(db, parquet_root.as_ref().to_path_buf())
    }

    pub fn memory(parquet_root: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(parquet_root.as_ref())?;
        Self::initialize(
            Connection::open_in_memory()?,
            parquet_root.as_ref().to_path_buf(),
        )
    }

    fn initialize(db: Connection, parquet_root: PathBuf) -> Result<Self> {
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS streams (
                stream TEXT PRIMARY KEY,
                last_seq INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS events (
                stream TEXT NOT NULL,
                stream_seq INTEGER NOT NULL,
                event_id TEXT NOT NULL UNIQUE,
                event_type INTEGER NOT NULL,
                occurred_at_ns INTEGER NOT NULL,
                received_at_ns INTEGER NOT NULL,
                account_id TEXT,
                strategy_group_id TEXT,
                run_id TEXT,
                instrument_id TEXT,
                correlation_id TEXT,
                causation_id TEXT,
                source TEXT NOT NULL,
                envelope BLOB NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(stream, stream_seq)
             );
             CREATE INDEX IF NOT EXISTS events_run_time ON events(run_id, occurred_at_ns);
             CREATE INDEX IF NOT EXISTS events_correlation ON events(correlation_id, occurred_at_ns);
             CREATE INDEX IF NOT EXISTS events_type_time ON events(event_type, occurred_at_ns);"
        )?;
        Ok(Self {
            db: Mutex::new(db),
            archiving: Mutex::new(()),
            parquet_root,
        })
    }

    pub fn insert(&self, event: &v2::EventEnvelope) -> Result<InsertOutcome> {
        Ok(self.insert_batch(std::slice::from_ref(event))?.remove(0))
    }
    pub fn insert_batch(&self, events: &[v2::EventEnvelope]) -> Result<Vec<InsertOutcome>> {
        let mut db = self.db.lock().expect("projection database mutex");
        let transaction = db.transaction()?;
        let mut outcomes = Vec::with_capacity(events.len());
        for event in events {
            outcomes.push(Self::insert_in_transaction(&transaction, event)?);
        }
        transaction.commit()?;
        Ok(outcomes)
    }
    fn insert_in_transaction(
        transaction: &rusqlite::Transaction<'_>,
        event: &v2::EventEnvelope,
    ) -> Result<InsertOutcome> {
        if event.schema_version != 2 {
            bail!("unsupported event schema {}", event.schema_version);
        }
        if event.stream.is_empty() || event.stream_seq == 0 || event.event_id.is_empty() {
            bail!("event identity is incomplete");
        }
        let mut encoded = Vec::with_capacity(event.encoded_len());
        event.encode(&mut encoded)?;
        let prior: Option<(String, u64,Vec<u8>)> = transaction.query_row(
            "SELECT event_id, stream_seq,envelope FROM events WHERE event_id=?1 OR (stream=?2 AND stream_seq=?3) LIMIT 1",
            params![event.event_id, event.stream, event.stream_seq],
            |row| Ok((row.get(0)?, row.get(1)?,row.get(2)?)),
        ).optional()?;
        if let Some((event_id, sequence, prior_bytes)) = prior {
            if event_id == event.event_id && sequence == event.stream_seq && prior_bytes == encoded
            {
                return Ok(InsertOutcome::Duplicate);
            }
            bail!("event identity collides with a different stream sequence");
        }
        let last: Option<u64> = transaction
            .query_row(
                "SELECT last_seq FROM streams WHERE stream=?1",
                [&event.stream],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(last) = last {
            if event.stream_seq != last + 1 {
                bail!(
                    "sequence gap for {}: expected {}, received {}",
                    event.stream,
                    last + 1,
                    event.stream_seq
                );
            }
        } else if event.stream_seq != 1 {
            bail!("new stream {} must begin at sequence 1", event.stream);
        }
        transaction.execute(
            "INSERT INTO events(stream,stream_seq,event_id,event_type,occurred_at_ns,received_at_ns,account_id,strategy_group_id,run_id,instrument_id,correlation_id,causation_id,source,envelope)
             VALUES(?1,?2,?3,?4,?5,?6,NULLIF(?7,''),NULLIF(?8,''),NULLIF(?9,''),NULLIF(?10,''),NULLIF(?11,''),NULLIF(?12,''),?13,?14)",
            params![event.stream, event.stream_seq, event.event_id, event.event_type, event.occurred_at_ns,
                event.received_at_ns, event.account_id, event.strategy_group_id, event.run_id, event.instrument_id,
                event.correlation_id, event.causation_id, event.source, encoded],
        )?;
        transaction.execute(
            "INSERT INTO streams(stream,last_seq) VALUES(?1,?2) ON CONFLICT(stream) DO UPDATE SET last_seq=excluded.last_seq",
            params![event.stream, event.stream_seq],
        )?;
        Ok(InsertOutcome::Inserted)
    }

    pub fn events_after(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<v2::EventEnvelope>> {
        if limit == 0 || limit > 2_048 {
            bail!("event limit must be between 1 and 2048");
        }
        let db = self.db.lock().expect("projection database mutex");
        let mut statement = db.prepare(
            "SELECT envelope FROM events WHERE stream=?1 AND stream_seq>?2 ORDER BY stream_seq LIMIT ?3"
        )?;
        let encoded = statement
            .query_map(params![stream, after, limit], |row| {
                row.get::<_, Vec<u8>>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        encoded
            .into_iter()
            .map(|bytes| v2::EventEnvelope::decode(bytes.as_slice()).context("decode stored event"))
            .collect()
    }

    pub fn watermark(&self, stream: &str) -> Result<u64> {
        Ok(self
            .db
            .lock()
            .expect("projection database mutex")
            .query_row(
                "SELECT last_seq FROM streams WHERE stream=?1",
                [stream],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn archive_unarchived(&self, maximum_rows: usize) -> Result<Option<PathBuf>> {
        if maximum_rows == 0 || maximum_rows > 100_000 {
            bail!("archive row limit is invalid");
        }
        let _archive = self.archiving.lock().expect("archive mutex");
        let records = {
            let db = self.db.lock().expect("projection database mutex");
            let mut statement=db.prepare("SELECT rowid,event_type,occurred_at_ns,envelope FROM events WHERE archived=0 ORDER BY rowid LIMIT ?1")?;
            statement
                .query_map([maximum_rows], |row| {
                    Ok(StoredEvent {
                        rowid: row.get(0)?,
                        event_type: row.get(1)?,
                        occurred_at_ns: row.get(2)?,
                        envelope: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        if records.is_empty() {
            return Ok(None);
        }
        let mut partitions = std::collections::BTreeMap::<String, Vec<StoredEvent>>::new();
        for record in records {
            let market = matches!(
                v2::EventType::try_from(record.event_type),
                Ok(v2::EventType::MarketQuote | v2::EventType::OrderBookUpdated)
            );
            let kind = if market { "market" } else { "critical" };
            let date =
                chrono::DateTime::from_timestamp_nanos(record.occurred_at_ns).format("%Y-%m-%d");
            partitions
                .entry(format!("{kind}-date={date}"))
                .or_default()
                .push(record);
        }
        let mut first_path = None;
        for (partition, records) in partitions {
            let directory = self.parquet_root.join(partition);
            std::fs::create_dir_all(&directory)?;
            let first = records.first().expect("archive batch").rowid;
            let last = records.last().expect("archive batch").rowid;
            let path = directory.join(format!("events-{first}-{last}.parquet"));
            // Compression and disk writes run outside the ingestion database lock.
            write_parquet(&path, &records)?;
            let mut db = self.db.lock().expect("projection database mutex");
            let tx = db.transaction()?;
            for record in &records {
                tx.execute(
                    "UPDATE events SET archived=1 WHERE rowid=?1",
                    [record.rowid],
                )?;
            }
            tx.commit()?;
            if first_path.is_none() {
                first_path = Some(path);
            }
        }
        Ok(first_path)
    }

    pub fn delete_raw_market_before(&self, cutoff_ns: i64) -> Result<usize> {
        let quote = v2::EventType::MarketQuote as i32;
        let book = v2::EventType::OrderBookUpdated as i32;
        Ok(self.db.lock().expect("projection database mutex").execute(
            "DELETE FROM events WHERE archived=1 AND occurred_at_ns<?1 AND event_type IN (?2,?3)",
            params![cutoff_ns, quote, book],
        )?)
    }
}

#[derive(Debug)]
struct StoredEvent {
    rowid: i64,
    event_type: i32,
    occurred_at_ns: i64,
    envelope: Vec<u8>,
}

fn write_parquet(path: &Path, records: &[StoredEvent]) -> Result<()> {
    let envelopes = records
        .iter()
        .map(|record| {
            v2::EventEnvelope::decode(record.envelope.as_slice()).map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = montlok_contracts::columnar::event_batch(&envelopes)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let temporary = path.with_extension("parquet.partial");
    let file = File::create(&temporary)?;
    let sync = file.try_clone()?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    sync.sync_all()?;
    std::fs::rename(&temporary, path)?;
    File::open(path.parent().expect("archive parent"))?.sync_all()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub fields: Vec<String>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub time_from_ns: Option<i64>,
    #[serde(default)]
    pub time_to_ns: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    1_000
}

const QUERY_FIELDS: &[&str] = &[
    "stream",
    "stream_seq",
    "event_id",
    "event_type",
    "occurred_at_ns",
    "received_at_ns",
    "account_id",
    "strategy_group_id",
    "run_id",
    "instrument_id",
    "correlation_id",
    "causation_id",
    "source",
];

pub async fn query_archive(root: &Path, spec: &QuerySpec) -> Result<Vec<RecordBatch>> {
    if spec.fields.is_empty() || spec.fields.len() > 128 || spec.limit == 0 || spec.limit > 100_000
    {
        bail!("query bounds are invalid");
    }
    if spec
        .fields
        .iter()
        .any(|field| !QUERY_FIELDS.contains(&field.as_str()))
    {
        bail!("query contains an unknown field");
    }
    let mut paths = Vec::new();
    for partition in std::fs::read_dir(root)? {
        let partition = partition?;
        if !partition.file_type()?.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(partition.path())? {
            let file = file?;
            if file.file_type()?.is_file()
                && file
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
            {
                paths.push(file.path().canonicalize()?.to_string_lossy().to_string());
            }
        }
    }
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let context = SessionContext::new();
    let frame = context
        .read_parquet(paths, ParquetReadOptions::default())
        .await?;
    context.register_table("events", frame.into_view())?;
    let mut predicates = Vec::new();
    if let Some(stream) = &spec.stream {
        predicates.push(format!("stream = '{}'", sql_literal(stream)));
    }
    if let Some(run) = &spec.run_id {
        predicates.push(format!("run_id = '{}'", sql_literal(run)));
    }
    if let Some(from) = spec.time_from_ns {
        predicates.push(format!(
            "occurred_at_ns >= arrow_cast({from}, 'Timestamp(Nanosecond, Some(\"UTC\"))')"
        ));
    }
    if let Some(to) = spec.time_to_ns {
        predicates.push(format!(
            "occurred_at_ns <= arrow_cast({to}, 'Timestamp(Nanosecond, Some(\"UTC\"))')"
        ));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let sql = format!(
        "SELECT {} FROM events{} ORDER BY occurred_at_ns, stream_seq LIMIT {}",
        spec.fields.join(","),
        where_clause,
        spec.limit
    );
    Ok(context.sql(&sql).await?.collect().await?)
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn event(sequence: u64, id: &str) -> v2::EventEnvelope {
        v2::EventEnvelope {
            schema_version: 2,
            stream: "run.a".into(),
            stream_seq: sequence,
            snapshot_seq: 0,
            event_id: id.into(),
            event_type: v2::EventType::PnlUpdated as i32,
            occurred_at_ns: 1_800_000_000_000_000_000 + sequence as i64,
            received_at_ns: 1_800_000_000_000_000_100 + sequence as i64,
            account_id: "account".into(),
            strategy_group_id: "group".into(),
            run_id: "a".into(),
            instrument_id: String::new(),
            correlation_id: String::new(),
            causation_id: String::new(),
            source: "test".into(),
            payload: None,
            source_payload_json: Vec::new(),
        }
    }

    #[test]
    fn store_enforces_sequence_and_idempotency() {
        let directory = tempdir().unwrap();
        let store = ProjectionStore::memory(directory.path()).unwrap();
        assert_eq!(
            store.insert(&event(1, "one")).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            store.insert(&event(1, "one")).unwrap(),
            InsertOutcome::Duplicate
        );
        assert!(store.insert(&event(3, "three")).is_err());
        assert_eq!(
            store.insert(&event(2, "two")).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(store.watermark("run.a").unwrap(), 2);
        assert_eq!(store.events_after("run.a", 1, 100).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn archive_is_arrow_queryable() {
        let directory = tempdir().unwrap();
        let store = ProjectionStore::memory(directory.path()).unwrap();
        store.insert(&event(1, "one")).unwrap();
        store.insert(&event(2, "two")).unwrap();
        assert!(store.archive_unarchived(100).unwrap().is_some());
        let batches = query_archive(
            directory.path(),
            &QuerySpec {
                fields: vec!["stream_seq".into(), "run_id".into()],
                stream: Some("run.a".into()),
                run_id: None,
                time_from_ns: None,
                time_to_ns: None,
                limit: 100,
            },
        )
        .await
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let filtered = query_archive(
            directory.path(),
            &QuerySpec {
                fields: vec!["stream_seq".into()],
                stream: Some("run.a".into()),
                run_id: None,
                time_from_ns: Some(1_800_000_000_000_000_002),
                time_to_ns: Some(1_800_000_000_000_000_002),
                limit: 100,
            },
        )
        .await
        .unwrap();
        assert_eq!(filtered.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}
