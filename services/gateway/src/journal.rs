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
        connection.execute_batch("CREATE TABLE IF NOT EXISTS detail_index(event_id TEXT PRIMARY KEY, occurred_at_ns INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS event_links(kind TEXT NOT NULL,value TEXT NOT NULL,account_id TEXT NOT NULL,run_id TEXT NOT NULL,event_id TEXT NOT NULL,
                PRIMARY KEY(kind,value,account_id,run_id,event_id));
            CREATE INDEX IF NOT EXISTS event_links_id ON event_links(event_id);")?;
        let mut journal = Self { connection };
        // Incremental migration of an existing replay journal. Never scan or decode
        // the full history on an interactive detail request.
        loop {
            let batch = {
                let mut query = journal.connection.prepare("SELECT envelope FROM event_log e WHERE NOT EXISTS(SELECT 1 FROM detail_index d WHERE d.event_id=e.event_id) LIMIT 512")?;
                query
                    .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if batch.is_empty() {
                break;
            }
            let tx = journal.connection.transaction()?;
            for bytes in batch {
                Self::index_detail(&tx, &v2::EventEnvelope::decode(bytes.as_slice())?)?;
            }
            tx.commit()?;
        }
        Ok(journal)
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
        Self::index_detail(tx, event)?;
        Ok(true)
    }
    fn index_detail(tx: &rusqlite::Transaction<'_>, event: &v2::EventEnvelope) -> Result<()> {
        tx.prepare_cached("INSERT INTO detail_index VALUES(?1,?2)")?
            .execute(params![event.event_id, event.occurred_at_ns])?;
        let mut insert =
            tx.prepare_cached("INSERT OR IGNORE INTO event_links VALUES(?1,?2,?3,?4,?5)")?;
        for (kind, value) in links(event) {
            if !value.is_empty() {
                insert.execute(params![
                    kind,
                    value,
                    event.account_id,
                    event.run_id,
                    event.event_id
                ])?;
            }
        }
        Ok(())
    }
    pub fn related(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<Option<(Vec<v2::EventEnvelope>, bool)>> {
        let _snapshot = self.connection.unchecked_transaction()?;
        let seed: Option<Vec<u8>> = self
            .connection
            .query_row(
                "SELECT envelope FROM event_log WHERE event_id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(seed) = seed else {
            return Ok(None);
        };
        let seed = v2::EventEnvelope::decode(seed.as_slice())?;
        let limit = limit.clamp(1, 2048);
        // Visit each linkage key once. A broad correlation must not expand an
        // O(n^2) recursive join or hold the event-ingestion writer lock.
        let mut events = vec![seed];
        let mut seen = std::collections::HashSet::from([id.to_owned()]);
        let mut visited = std::collections::HashSet::new();
        let mut cursor = 0;
        let mut truncated = false;
        let mut query = self.connection.prepare_cached(
            "SELECT e.envelope FROM event_links l JOIN event_log e ON e.event_id=l.event_id
            WHERE l.kind=?1 AND l.value=?2 AND l.account_id=?3 AND l.run_id=?4 AND e.group_id=?5 LIMIT ?6",
        )?;
        'walk: while cursor < events.len() {
            let event = &events[cursor];
            let keys = links(event)
                .into_iter()
                .filter(|(_, value)| !value.is_empty())
                .map(|(kind, value)| (kind, value.to_owned()))
                .collect::<Vec<_>>();
            let account = event.account_id.clone();
            let run = event.run_id.clone();
            let group = event.strategy_group_id.clone();
            cursor += 1;
            for (kind, value) in keys {
                if !visited.insert((kind, value.clone())) {
                    continue;
                }
                let matches = query
                    .query_map(
                        params![kind, value, account, run, group, limit + 1],
                        |row| row.get::<_, Vec<u8>>(0),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for bytes in matches {
                    let event = v2::EventEnvelope::decode(bytes.as_slice())?;
                    if !seen.insert(event.event_id.clone()) {
                        continue;
                    }
                    if events.len() == limit {
                        truncated = true;
                        break 'walk;
                    }
                    events.push(event);
                }
            }
        }
        events.sort_by(|left, right| {
            left.occurred_at_ns
                .cmp(&right.occurred_at_ns)
                .then_with(|| left.stream.cmp(&right.stream))
                .then_with(|| left.stream_seq.cmp(&right.stream_seq))
        });
        Ok(Some((events, truncated)))
    }
    pub fn related_readonly(
        path: &Path,
        id: &str,
        limit: usize,
    ) -> Result<Option<(Vec<v2::EventEnvelope>, bool)>> {
        let connection = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_millis(250))?;
        Self { connection }.related(id, limit)
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

fn links(event: &v2::EventEnvelope) -> Vec<(&'static str, &str)> {
    let mut values = vec![
        ("correlation", event.correlation_id.as_str()),
        ("event", event.event_id.as_str()),
        ("event", event.causation_id.as_str()),
    ];
    match event.payload.as_ref() {
        Some(Payload::OrderEvent(order)) => values.extend([
            ("client_order", order.client_order_id.as_str()),
            ("venue_order", order.venue_order_id.as_str()),
            ("operation", order.operation_id.as_str()),
            ("risk", order.risk_decision_id.as_str()),
        ]),
        Some(Payload::RouteUpdated(route)) => values.extend([
            ("client_order", route.client_order_id.as_str()),
            ("venue_order", route.venue_order_id.as_str()),
            ("route", route.route_id.as_str()),
        ]),
        Some(Payload::FillReceived(fill)) => values.extend([
            ("venue_order", fill.venue_order_id.as_str()),
            ("route", fill.route_id.as_str()),
            ("trade", fill.trade_id.as_str()),
        ]),
        Some(Payload::PositionUpdated(position)) => values.extend([
            ("trade", position.trade_id.as_str()),
            ("position", position.position_event_id.as_str()),
        ]),
        Some(Payload::PnlUpdated(pnl)) => values.push(("position", pnl.position_event_id.as_str())),
        Some(Payload::ModelInferenceCompleted(model)) => {
            values.push(("inference", model.model_inference_id.as_str()))
        }
        Some(Payload::SignalGenerated(signal)) => values.extend([
            ("inference", signal.model_inference_id.as_str()),
            ("signal", signal.signal_id.as_str()),
        ]),
        Some(Payload::TargetPositionChanged(target)) => values.extend([
            ("signal", target.signal_id.as_str()),
            ("decision", target.decision_id.as_str()),
        ]),
        Some(Payload::RiskDecision(risk)) => values.extend([
            ("decision", risk.decision_id.as_str()),
            ("risk", risk.risk_decision_id.as_str()),
        ]),
        Some(Payload::OperationReceiptUpdated(receipt)) => {
            values.push(("operation", receipt.operation_id.as_str()))
        }
        _ => {}
    }
    values
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
    #[test]
    fn detail_traverses_routes_fills_positions_without_crossing_accounts_or_runs() {
        let mut journal = Journal::memory().unwrap();
        let mut order = event(1);
        order.account_id = "account-a".into();
        order.payload = Some(Payload::OrderEvent(v2::OrderEvent {
            client_order_id: "client".into(),
            ..Default::default()
        }));
        let mut route = event(2);
        route.account_id = "account-a".into();
        route.payload = Some(Payload::RouteUpdated(v2::RouteUpdated {
            client_order_id: "client".into(),
            route_id: "route".into(),
            ..Default::default()
        }));
        let mut fill = event(3);
        fill.account_id = "account-a".into();
        fill.payload = Some(Payload::FillReceived(v2::FillReceived {
            route_id: "route".into(),
            trade_id: "trade".into(),
            ..Default::default()
        }));
        let mut position = event(4);
        position.account_id = "account-a".into();
        position.payload = Some(Payload::PositionUpdated(v2::PositionUpdated {
            trade_id: "trade".into(),
            ..Default::default()
        }));
        let mut other = event(5);
        other.account_id = "account-b".into();
        other.payload = route.payload.clone();
        let mut other_run = event(6);
        other_run.account_id = "account-a".into();
        other_run.run_id = "different".into();
        other_run.payload = route.payload.clone();
        journal
            .append_many(&[order, route, fill, position, other, other_run])
            .unwrap();
        let (events, truncated) = journal.related("id-1", 500).unwrap().unwrap();
        assert_eq!(events.len(), 4);
        assert!(!truncated);
        assert!(
            events
                .iter()
                .all(|e| e.account_id == "account-a" && e.run_id == "a")
        );
        let (events, truncated) = journal.related("id-4", 2).unwrap().unwrap();
        assert_eq!(events.len(), 2);
        assert!(truncated);
        assert!(events.iter().any(|e| e.event_id == "id-4"));
        assert!(journal.related("missing", 20).unwrap().is_none());
    }
    #[test]
    fn model_signal_risk_order_chain_joins_without_a_shared_correlation() {
        let mut journal = Journal::memory().unwrap();
        let payloads = [
            Payload::ModelInferenceCompleted(v2::ModelInferenceCompleted {
                model_inference_id: "inference".into(),
                ..Default::default()
            }),
            Payload::SignalGenerated(v2::SignalGenerated {
                model_inference_id: "inference".into(),
                signal_id: "signal".into(),
                ..Default::default()
            }),
            Payload::TargetPositionChanged(v2::TargetPositionChanged {
                signal_id: "signal".into(),
                decision_id: "decision".into(),
                ..Default::default()
            }),
            Payload::RiskDecision(v2::RiskDecision {
                decision_id: "decision".into(),
                risk_decision_id: "risk".into(),
                ..Default::default()
            }),
            Payload::OrderEvent(v2::OrderEvent {
                risk_decision_id: "risk".into(),
                client_order_id: "client".into(),
                ..Default::default()
            }),
        ];
        for (index, payload) in payloads.into_iter().enumerate() {
            let mut event = event(index as u64 + 1);
            event.payload = Some(payload);
            journal.append(&event).unwrap();
        }
        let (events, truncated) = journal.related("id-5", 500).unwrap().unwrap();
        assert_eq!(events.len(), 5);
        assert!(!truncated);
    }
}
