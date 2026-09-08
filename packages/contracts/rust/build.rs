fn main() {
    let proto = "../proto/montlok/v2/terminal.proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = prost_build::Config::new();
    config.boxed(".montlok.v2.ServerMessage.message.event");
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    for field in [
        "EventEnvelope.occurred_at_ns",
        "EventEnvelope.received_at_ns",
        "MarketQuote.venue_time_ns",
        "OrderBookUpdated.venue_time_ns",
        "ModelInferenceCompleted.input_as_of_ns",
        "FillReceived.venue_time_ns",
        "PnlUpdated.valuation_at_ns",
        "OperationReceiptUpdated.expires_at_ns",
        "Heartbeat.server_time_ns",
        "TerminalContext.time_from_ns",
        "TerminalContext.time_to_ns",
    ] {
        config.field_attribute(
            format!(".montlok.v2.{field}"),
            "#[serde(with = \"crate::wire_json::i64_string\")]",
        );
    }
    for field in [
        "EventEnvelope.stream_seq",
        "EventEnvelope.snapshot_seq",
        "ResumePosition.last_stream_seq",
        "PnlUpdated.source_event_seq",
        "ModelInferenceCompleted.latency_ns",
        "SnapshotBegin.snapshot_seq",
        "SnapshotEnd.snapshot_seq",
        "SnapshotEnd.last_stream_seq",
        "SequenceGap.expected",
        "SequenceGap.received",
    ] {
        config.field_attribute(
            format!(".montlok.v2.{field}"),
            "#[serde(with = \"crate::wire_json::u64_string\")]",
        );
    }
    config
        .compile_protos(&[proto], &["../proto"])
        .expect("compile Montlok v2 protobuf contract");
}
