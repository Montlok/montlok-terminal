//! Durable replay state. SQLite work runs on the blocking pool in EventHub.
use anyhow::{Result, bail};
use montlok_contracts::v2::{self, event_envelope::Payload};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{collections::HashMap, path::Path, time::Duration};

pub struct Journal {
    connection: Connection,
}

impl Journal {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        Self::initialize(Connection::open(path)?)
    }
    pub fn memory() -> Result<Self> {
        Self::initialize(Connection::open_in_memory()?)
    }
    fn initialize(connection: Connection) -> Result<Self> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS event_log(
            stream TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
            account_id TEXT NOT NULL, group_id TEXT NOT NULL, run_id TEXT NOT NULL, instrument_id TEXT NOT NULL,
            entity_key TEXT NOT NULL, envelope BLOB NOT NULL, PRIMARY KEY(stream,sequence)
        );
        CREATE TABLE IF NOT EXISTS stream_positions(stream TEXT PRIMARY KEY,sequence INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS initial_state(
            stream TEXT NOT NULL,entity_key TEXT NOT NULL,sequence INTEGER NOT NULL,PRIMARY KEY(stream,entity_key)
        );
        CREATE INDEX IF NOT EXISTS event_log_run ON event_log(run_id,stream,sequence);")?;
        Ok(Self { connection })
    }
    #[cfg(test)]
    pub fn append(&mut self, event: &v2::EventEnvelope) -> Result<bool> {
        Ok(self.append_many(std::slice::from_ref(event))?[0])
    }
    pub fn append_many(&mut self, events: &[v2::EventEnvelope]) -> Result<Vec<bool>> {
        let tx = self.connection.transaction()?;
        let mut inserted = Vec::with_capacity(events.len());
        for event in events {
            inserted.push(Self::append_in_transaction(&tx, event)?);
        }
        tx.commit()?;
        Ok(inserted)
    }
    fn append_in_transaction(
        tx: &rusqlite::Transaction<'_>,
        event: &v2::EventEnvelope,
    ) -> Result<bool> {
        if event.schema_version != 2
            || event.stream_seq == 0
            || event.stream_seq > i64::MAX as u64
            || event.event_id.is_empty()
            || event.stream.is_empty()
        {
            bail!("event identity or schema is invalid");
        }
        let bytes = event.encode_to_vec();
        let prior: Option<Vec<u8>> = tx
            .query_row(
                "SELECT envelope FROM event_log WHERE event_id=?1 OR (stream=?2 AND sequence=?3)",
                params![event.event_id, event.stream, event.stream_seq],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            if prior == bytes {
                return Ok(false);
            }
            bail!("event identifier or stream sequence was reused with different content");
        }
        let last: u64 = tx
            .query_row(
                "SELECT sequence FROM stream_positions WHERE stream=?1",
                [&event.stream],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if event.stream_seq != last + 1 {
            bail!(
                "sequence gap for {}: expected {}, received {}",
                event.stream,
                last + 1,
                event.stream_seq
            );
        }
        let key = entity_key(event);
        tx.execute(
            "INSERT INTO event_log VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                event.stream,
                event.stream_seq,
                event.event_id,
                event.account_id,
                event.strategy_group_id,
                event.run_id,
                event.instrument_id,
                key,
                bytes
            ],
        )?;
        tx.execute("INSERT INTO stream_positions VALUES(?1,?2) ON CONFLICT(stream) DO UPDATE SET sequence=excluded.sequence",params![event.stream,event.stream_seq])?;
        tx.execute("INSERT INTO initial_state VALUES(?1,?2,?3) ON CONFLICT(stream,entity_key) DO UPDATE SET sequence=excluded.sequence",
            params![event.stream,key,event.stream_seq])?;
        Ok(true)
    }
    pub fn watermarks(&self) -> Result<HashMap<String, u64>> {
        let mut statement = self
            .connection
            .prepare("SELECT stream,sequence FROM stream_positions")?;
        Ok(statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn after(
        &self,
        stream: &str,
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<v2::EventEnvelope>> {
        let mut statement=self.connection.prepare("SELECT envelope FROM event_log WHERE stream=?1 AND sequence>?2 AND sequence<=?3 ORDER BY sequence LIMIT ?4")?;
        let rows = statement
            .query_map(
                params![stream, after, through.min(i64::MAX as u64), limit.min(2048)],
                |r| r.get::<_, Vec<u8>>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|bytes| Ok(v2::EventEnvelope::decode(bytes.as_slice())?))
            .collect()
    }
    pub fn snapshot(
        &mut self,
        topics: &[String],
        context: &v2::TerminalContext,
    ) -> Result<(HashMap<String, u64>, Vec<v2::EventEnvelope>)> {
        let tx = self.connection.transaction()?;
        let mut positions = tx.prepare("SELECT stream,sequence FROM stream_positions")?;
        let watermarks = positions
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<HashMap<_, _>>>()?;
        drop(positions);
        let mut query=tx.prepare("SELECT e.envelope FROM initial_state i JOIN event_log e ON e.stream=i.stream AND e.sequence=i.sequence
            WHERE (?1='' OR e.account_id=?1) AND (?2='' OR e.group_id=?2) AND (?3='' OR e.run_id=?3) AND (?4='' OR e.instrument_id=?4)
            ORDER BY e.stream,e.sequence")?;
        let bytes = query
            .query_map(
                params![
                    context.account_id,
                    context.strategy_group_id,
                    context.run_id,
                    context.instrument_id
                ],
                |r| r.get::<_, Vec<u8>>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(query);
        tx.commit()?;
        let mut events = Vec::new();
        for bytes in bytes {
            let event = v2::EventEnvelope::decode(bytes.as_slice())?;
            if topics
                .iter()
                .any(|topic| topic == "*" || topic == &event.stream)
            {
                events.push(event);
            }
        }
        Ok((
            watermarks
                .into_iter()
                .filter(|(name, _)| topics.iter().any(|topic| topic == "*" || topic == name))
                .collect(),
            events,
        ))
    }
}

fn entity_key(event: &v2::EventEnvelope) -> String {
    match event.payload.as_ref() {
        Some(Payload::OrderEvent(order)) => format!("order:{}", order.client_order_id),
        Some(Payload::RouteUpdated(route)) => format!("route:{}", route.route_id),
        Some(Payload::FillReceived(fill)) => format!("fill:{}", fill.trade_id),
        Some(Payload::PositionUpdated(_)) => format!("position:{}", event.instrument_id),
        Some(Payload::MarketQuote(_)) => format!("quote:{}", event.instrument_id),
        Some(Payload::OrderBookUpdated(_)) => format!("book:{}", event.instrument_id),
        Some(Payload::RunStateChanged(_)) => format!("state:{}", event.run_id),
        Some(Payload::PnlUpdated(_)) => format!("pnl:{}:{}", event.run_id, event.instrument_id),
        Some(Payload::AlertChanged(alert)) => format!("alert:{}", alert.alert_id),
        Some(Payload::OperationReceiptUpdated(receipt)) => {
            format!("operation:{}", receipt.operation_id)
        }
        _ => event.event_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(n: u64) -> v2::EventEnvelope {
        v2::EventEnvelope {
            schema_version: 2,
            stream: "run.a".into(),
            stream_seq: n,
            event_id: format!("id-{n}"),
            run_id: "a".into(),
            ..Default::default()
        }
    }
    #[test]
    fn same_id_with_changed_content_cannot_hide_a_gap() {
        let mut journal = Journal::memory().unwrap();
        let first = event(1);
        assert!(journal.append(&first).unwrap());
        assert!(!journal.append(&first).unwrap());
        let mut altered = first;
        altered.run_id = "b".into();
        assert!(journal.append(&altered).is_err());
        assert!(journal.append(&event(3)).is_err());
        assert!(journal.append(&event(2)).unwrap());
        assert_eq!(journal.after("run.a", 0, 2, 2048).unwrap().len(), 2);
    }
}
