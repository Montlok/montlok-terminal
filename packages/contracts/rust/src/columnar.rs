//! One Arrow schema shared by snapshot encoding and archive projection.
use crate::{MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_ROWS, v2};
use anyhow::{Result, bail};
use arrow::{
    array::{
        ArrayRef, BinaryArray, Int32Array, StringArray, TimestampNanosecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use prost::Message;
use std::sync::Arc;

pub fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::UInt32, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("stream_seq", DataType::UInt64, false),
        Field::new("snapshot_seq", DataType::UInt64, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Int32, false),
        Field::new(
            "occurred_at_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "received_at_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("account_id", DataType::Utf8, false),
        Field::new("strategy_group_id", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("correlation_id", DataType::Utf8, false),
        Field::new("causation_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("payload_protobuf", DataType::Binary, false),
    ]))
}
pub fn event_batch(events: &[v2::EventEnvelope]) -> Result<RecordBatch> {
    let strings = |get: fn(&v2::EventEnvelope) -> &str| {
        Arc::new(StringArray::from(
            events.iter().map(get).collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let bytes = events
        .iter()
        .map(Message::encode_to_vec)
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        event_schema(),
        vec![
            Arc::new(UInt32Array::from(
                events.iter().map(|e| e.schema_version).collect::<Vec<_>>(),
            )),
            strings(|e| &e.stream),
            Arc::new(UInt64Array::from(
                events.iter().map(|e| e.stream_seq).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                events.iter().map(|e| e.snapshot_seq).collect::<Vec<_>>(),
            )),
            strings(|e| &e.event_id),
            Arc::new(Int32Array::from(
                events.iter().map(|e| e.event_type).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampNanosecondArray::from(
                    events.iter().map(|e| e.occurred_at_ns).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampNanosecondArray::from(
                    events.iter().map(|e| e.received_at_ns).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            strings(|e| &e.account_id),
            strings(|e| &e.strategy_group_id),
            strings(|e| &e.run_id),
            strings(|e| &e.instrument_id),
            strings(|e| &e.correlation_id),
            strings(|e| &e.causation_id),
            strings(|e| &e.source),
            Arc::new(BinaryArray::from_iter_values(
                bytes.iter().map(Vec::as_slice),
            )),
        ],
    )?)
}
pub fn encode_ipc(events: &[v2::EventEnvelope]) -> Result<Vec<u8>> {
    let batch = event_batch(events)?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}
pub fn snapshot_batches(events: &[v2::EventEnvelope]) -> Result<Vec<(usize, Vec<u8>)>> {
    let mut batches = Vec::new();
    for chunk in events.chunks(MAX_SNAPSHOT_ROWS) {
        split_batch(chunk, &mut batches)?;
    }
    Ok(batches)
}
fn split_batch(events: &[v2::EventEnvelope], out: &mut Vec<(usize, Vec<u8>)>) -> Result<()> {
    let bytes = encode_ipc(events)?;
    if bytes.len() <= MAX_SNAPSHOT_BYTES {
        out.push((events.len(), bytes));
    } else if events.len() > 1 {
        let (left, right) = events.split_at(events.len() / 2);
        split_batch(left, out)?;
        split_batch(right, out)?;
    } else {
        bail!("one snapshot event exceeds 1 MiB");
    }
    Ok(())
}
