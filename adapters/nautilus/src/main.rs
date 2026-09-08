use std::{
    io::SeekFrom,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use async_nats::jetstream;
use clap::Parser;
use montlok_nautilus_adapter::{PersistedEvent, outbox::Outbox};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    time,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(about = "Persisted Nautilus log to JetStream, with a durable outbox")]
struct Arguments {
    #[arg(long, env = "MONTLOK_EVENT_LOG")]
    event_log: PathBuf,
    #[arg(long, env = "MONTLOK_ADAPTER_DB", default_value = "var/adapter.sqlite")]
    database: PathBuf,
    #[arg(
        long,
        env = "MONTLOK_NATS_URL",
        default_value = "nats://127.0.0.1:4222"
    )]
    nats_url: String,
    #[arg(long, env = "MONTLOK_NATS_STREAM", default_value = "MONTLOK_EVENTS")]
    stream: String,
    #[arg(long, env = "MONTLOK_ACCOUNT_ID")]
    account_id: String,
    #[arg(long, env = "MONTLOK_GROUP_ID")]
    group_id: String,
    #[arg(long, env = "MONTLOK_RUN_ID")]
    run_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "montlok_nautilus_adapter=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();
    let args = Arguments::parse();
    let source = std::fs::canonicalize(&args.event_log).context("locate persisted run log")?;
    let mut outbox = Outbox::open(&args.database, &source)?;
    let file = File::open(&source).await?;
    let metadata = file.metadata().await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        outbox.bind_file(&format!("{}:{}", metadata.dev(), metadata.ino()))?;
    }
    if metadata.len() < outbox.offset()? {
        anyhow::bail!("source log shrank below its durable cursor");
    }
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(outbox.offset()?)).await?;
    let client = async_nats::connect(&args.nats_url).await?;
    let jetstream = jetstream::new(client);
    montlok_event_transport::ensure_event_stream(&jetstream, &args.stream).await?;
    loop {
        let pending = outbox.pending(256)?;
        if !pending.is_empty() {
            let mut acknowledged = Vec::with_capacity(pending.len());
            let mut receipts = Vec::with_capacity(pending.len());
            for event in pending {
                let mut headers = async_nats::HeaderMap::new();
                headers.insert("Nats-Msg-Id", event.envelope.event_id.as_str());
                let subject = format!(
                    "montlok.events.{}",
                    event
                        .envelope
                        .stream
                        .chars()
                        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                            c
                        } else {
                            '_'
                        })
                        .collect::<String>()
                );
                match jetstream
                    .publish_with_headers(subject, headers, event.bytes.into())
                    .await
                {
                    Ok(ack) => receipts.push((event.id, ack)),
                    Err(error) => {
                        tracing::warn!(%error,"JetStream publication pending");
                        break;
                    }
                }
            }
            for (id, result) in futures_util::future::join_all(
                receipts
                    .into_iter()
                    .map(|(id, ack)| async move { (id, ack.await) }),
            )
            .await
            {
                match result {
                    Ok(_) => acknowledged.push(id),
                    Err(error) => tracing::warn!(%error,"JetStream acknowledgement pending"),
                }
            }
            outbox.acknowledge(&acknowledged)?;
            if acknowledged.is_empty() {
                time::sleep(Duration::from_millis(250)).await;
            }
            continue;
        }

        let mut records = Vec::with_capacity(256);
        let mut cursor = outbox.offset()?;
        let mut batch_bytes = 0;
        while records.len() < 256 && batch_bytes < 1_048_576 {
            let mut line = Vec::new();
            let count = (&mut reader)
                .take(1_048_577)
                .read_until(b'\n', &mut line)
                .await?;
            if count == 0 {
                break;
            }
            if count > 1_048_576 {
                anyhow::bail!("persisted line exceeds 1 MiB at {cursor}");
            }
            if !line.ends_with(b"\n") {
                reader.seek(SeekFrom::Start(cursor)).await?;
                break;
            }
            let mut record: PersistedEvent =
                serde_json::from_slice(&line).with_context(|| format!("event at byte {cursor}"))?;
            record
                .account_id
                .get_or_insert_with(|| args.account_id.clone());
            record
                .strategy_group_id
                .get_or_insert_with(|| args.group_id.clone());
            record.run_id.get_or_insert_with(|| args.run_id.clone());
            cursor += count as u64;
            batch_bytes += count;
            records.push((record, cursor));
        }
        if records.is_empty() {
            tokio::select! { _ = tokio::signal::ctrl_c() => break, _ = time::sleep(Duration::from_millis(20)) => {} }
        } else {
            outbox.stage(records, now_ns())?;
        }
    }
    Ok(())
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
