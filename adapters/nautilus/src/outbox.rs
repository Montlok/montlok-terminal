//! Durable source offset, stream sequence, and pending payloads commit together.
//! A crash after a venue-bus acknowledgement replays the same event identifier.
use std::path::Path;

use anyhow::{Result, bail};
use montlok_contracts::v2::EventEnvelope;
use prost::Message;
use rusqlite::{Connection, params};

use crate::{Checkpoint, PersistedEvent, map_event};

pub struct Outbox {
    db: Connection,
    source: String,
}

pub struct PendingEvent {
    pub id: i64,
    pub envelope: EventEnvelope,
    pub bytes: Vec<u8>,
}

impl Outbox {
    pub fn open(path: &Path, source: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS source_offsets (
            source TEXT PRIMARY KEY, offset INTEGER NOT NULL, file_identity TEXT
        );
        CREATE TABLE IF NOT EXISTS stream_sequences(stream TEXT PRIMARY KEY, sequence INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(
            id INTEGER PRIMARY KEY, source TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
            envelope BLOB NOT NULL
        );")?;
        let source = source.to_string_lossy().to_string();
        db.execute(
            "INSERT OR IGNORE INTO source_offsets(source,offset) VALUES(?1,0)",
            [&source],
        )?;
        Ok(Self { db, source })
    }

    pub fn offset(&self) -> Result<u64> {
        Ok(self.db.query_row(
            "SELECT offset FROM source_offsets WHERE source=?1",
            [&self.source],
            |r| r.get(0),
        )?)
    }

    pub fn bind_file(&self, identity: &str) -> Result<()> {
        let previous: Option<String> = self.db.query_row(
            "SELECT file_identity FROM source_offsets WHERE source=?1",
            [&self.source],
            |r| r.get(0),
        )?;
        if previous.as_deref().is_some_and(|p| p != identity) {
            bail!(
                "source file identity changed; keep the original log until its pending events are drained"
            );
        }
        self.db.execute(
            "UPDATE source_offsets SET file_identity=?2 WHERE source=?1",
            params![self.source, identity],
        )?;
        Ok(())
    }

    pub fn stage(&mut self, mut records: Vec<(PersistedEvent, u64)>, now_ns: i64) -> Result<usize> {
        if records.len() > 256 {
            bail!("source batch exceeds 256 events");
        }
        let transaction = self.db.transaction()?;
        let mut checkpoint = Checkpoint::default();
        let mut statement = transaction.prepare("SELECT stream,sequence FROM stream_sequences")?;
        checkpoint.streams = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        drop(statement);
        let mut offset: u64 = transaction.query_row(
            "SELECT offset FROM source_offsets WHERE source=?1",
            [&self.source],
            |r| r.get(0),
        )?;
        let count = records.len();
        for (record, next_offset) in records.drain(..) {
            if next_offset <= offset {
                bail!("source offset did not advance");
            }
            let envelope = map_event(record, &mut checkpoint, now_ns)?;
            let bytes = envelope.encode_to_vec();
            transaction.execute(
                "INSERT INTO outbox(source,event_id,envelope) VALUES(?1,?2,?3)",
                params![self.source, envelope.event_id, bytes],
            )?;
            offset = next_offset;
        }
        for (stream, sequence) in checkpoint.streams {
            transaction.execute(
                "INSERT INTO stream_sequences(stream,sequence) VALUES(?1,?2)
                ON CONFLICT(stream) DO UPDATE SET sequence=excluded.sequence",
                params![stream, sequence],
            )?;
        }
        transaction.execute(
            "UPDATE source_offsets SET offset=?2 WHERE source=?1",
            params![self.source, offset],
        )?;
        transaction.commit()?;
        Ok(count)
    }

    pub fn pending(&self, limit: usize) -> Result<Vec<PendingEvent>> {
        let mut statement = self
            .db
            .prepare("SELECT id,envelope FROM outbox WHERE source=?1 ORDER BY id LIMIT ?2")?;
        let rows = statement
            .query_map(params![self.source, limit.min(256)], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(id, bytes)| {
                Ok(PendingEvent {
                    id,
                    envelope: EventEnvelope::decode(bytes.as_slice())?,
                    bytes,
                })
            })
            .collect()
    }

    pub fn acknowledge(&mut self, ids: &[i64]) -> Result<()> {
        let tx = self.db.transaction()?;
        for id in ids {
            tx.execute(
                "DELETE FROM outbox WHERE id=?1 AND source=?2",
                params![id, self.source],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn record() -> PersistedEvent {
        serde_json::from_value(json!({"type":"submitted","orderId":"a","time":1800000000000000000i64,
            "runId":"run1","instrument":"XTSLA-USDT.OKX","side":"BUY","quantity":"0.001","price":"355.37"})).unwrap()
    }

    #[test]
    fn restart_after_publish_reuses_identical_bytes_and_preserves_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("outbox.db");
        let log = dir.path().join("log.jsonl");
        let mut store = Outbox::open(&path, &log).unwrap();
        store
            .stage(vec![(record(), 100)], 1800000000000000100)
            .unwrap();
        let pending = store.pending(256).unwrap();
        let bytes = pending[0].bytes.clone();
        drop(store);
        let mut reopened = Outbox::open(&path, &log).unwrap();
        assert_eq!(reopened.offset().unwrap(), 100);
        assert_eq!(reopened.pending(256).unwrap()[0].bytes, bytes);
        reopened.acknowledge(&[pending[0].id]).unwrap();
        assert!(reopened.pending(256).unwrap().is_empty());
        reopened
            .stage(vec![(record(), 200)], 1800000000000000200)
            .unwrap();
        assert_eq!(reopened.pending(1).unwrap()[0].envelope.stream_seq, 2);
    }

    #[test]
    fn a_bad_batch_cannot_advance_the_source() {
        let dir = tempdir().unwrap();
        let mut store =
            Outbox::open(&dir.path().join("outbox.db"), &dir.path().join("log")).unwrap();
        assert!(
            store
                .stage(vec![(record(), 100), (record(), 90)], 10)
                .is_err()
        );
        assert_eq!(store.offset().unwrap(), 0);
        assert!(store.pending(1).unwrap().is_empty());
    }
}
