use arrow::{
    array::{Array, BinaryArray},
    ipc::reader::StreamReader,
};
use futures_util::{SinkExt, StreamExt};
use montlok_contracts::v2::{self, client_message, server_message};
use montlok_gateway::{GatewayState, router};
use prost::Message;
use std::io::Cursor;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest},
};

fn event(n: u64) -> v2::EventEnvelope {
    v2::EventEnvelope {
        schema_version: 2,
        stream: "run.live".into(),
        stream_seq: n,
        event_id: format!("event-{n}"),
        run_id: "live".into(),
        occurred_at_ns: 1_800_000_000_000_000_000 + n as i64,
        received_at_ns: 1_800_000_000_000_001_000 + n as i64,
        event_type: v2::EventType::FillReceived as i32,
        payload: Some(v2::event_envelope::Payload::FillReceived(
            v2::FillReceived {
                trade_id: format!("trade-{n}"),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[tokio::test]
async fn initial_arrow_snapshot_replay_and_server_restart_preserve_every_event() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("gateway.sqlite");
    let mut state = GatewayState::new(&db, None).unwrap();
    state.auth.read_token = Some("r".repeat(64));
    for n in 1..=4100 {
        state.events.publish(event(n)).await.unwrap();
    }
    drop(state);
    let mut restored = GatewayState::new(&db, None).unwrap();
    restored.auth.read_token = Some("r".repeat(64));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(restored)).await.unwrap();
    });
    let public = reqwest::Client::new()
        .get(format!("http://{address}/api/v2/events?stream=run.live"))
        .send()
        .await
        .unwrap();
    assert_eq!(public.status(), 401);
    let mut request = format!("ws://{address}/api/v2/stream")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", "r".repeat(64)).parse().unwrap(),
    );
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "montlok.protobuf.v2".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request.clone()).await.unwrap();
    socket
        .send(WsMessage::Binary(
            v2::ClientMessage {
                message: Some(client_message::Message::Hello(v2::ClientHello {
                    protocol_version: 2,
                    ..Default::default()
                })),
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .unwrap();
    let subscribe = v2::ClientMessage {
        message: Some(client_message::Message::Subscribe(v2::Subscribe {
            request_id: "one".into(),
            topics: vec!["run.live".into()],
            ..Default::default()
        })),
    };
    socket
        .send(WsMessage::Binary(subscribe.encode_to_vec().into()))
        .await
        .unwrap();
    let mut rows = Vec::new();
    let mut batches = 0;
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let WsMessage::Binary(bytes) = frame {
            match v2::ServerMessage::decode(bytes).unwrap().message.unwrap() {
                server_message::Message::SnapshotBatch(batch) => {
                    assert!(batch.arrow_ipc.len() <= 1_048_576);
                    assert!(batch.row_count <= 2048);
                    batches += 1;
                    let reader = StreamReader::try_new(Cursor::new(batch.arrow_ipc), None).unwrap();
                    for record in reader {
                        let record = record.unwrap();
                        let values = record
                            .column_by_name("payload_protobuf")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap();
                        for i in 0..values.len() {
                            rows.push(
                                v2::EventEnvelope::decode(values.value(i))
                                    .unwrap()
                                    .stream_seq,
                            );
                        }
                    }
                }
                server_message::Message::SnapshotEnd(end) => {
                    assert_eq!(end.watermarks[0].last_stream_seq, 4100);
                    break;
                }
                _ => {}
            }
        }
    }
    assert_eq!(batches, 3);
    assert_eq!(rows, (1..=4100).collect::<Vec<_>>());
    socket.close(None).await.unwrap();
    let (mut replay, _) = connect_async(request).await.unwrap();
    replay
        .send(WsMessage::Binary(
            v2::ClientMessage {
                message: Some(client_message::Message::Hello(v2::ClientHello {
                    protocol_version: 2,
                    resume_positions: vec![v2::ResumePosition {
                        stream: "run.live".into(),
                        last_stream_seq: 4096,
                    }],
                    ..Default::default()
                })),
            }
            .encode_to_vec()
            .into(),
        ))
        .await
        .unwrap();
    replay
        .send(WsMessage::Binary(subscribe.encode_to_vec().into()))
        .await
        .unwrap();
    let mut sequences = Vec::new();
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), replay.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let WsMessage::Binary(bytes) = frame {
            match v2::ServerMessage::decode(bytes).unwrap().message.unwrap() {
                server_message::Message::Event(event) => sequences.push(event.stream_seq),
                server_message::Message::SnapshotEnd(_) => break,
                server_message::Message::SnapshotBegin(_) => {
                    panic!("retained events should replay without replacing the snapshot")
                }
                _ => {}
            }
        }
    }
    assert_eq!(sequences, vec![4097, 4098, 4099, 4100]);
    replay.close(None).await.unwrap();
    server.abort();
}
