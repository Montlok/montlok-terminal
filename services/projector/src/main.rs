use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull};
use clap::Parser;
use futures_util::StreamExt;
use montlok_contracts::v2;
use montlok_projector::ProjectionStore;
use prost::Message;
use tokio::time;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(about = "Montlok event projector")]
struct Arguments {
    #[arg(
        long,
        env = "MONTLOK_NATS_URL",
        default_value = "nats://127.0.0.1:4222"
    )]
    nats_url: String,
    #[arg(
        long,
        env = "MONTLOK_PROJECTOR_DB",
        default_value = "var/projector.sqlite"
    )]
    database: PathBuf,
    #[arg(long, env = "MONTLOK_PARQUET_ROOT", default_value = "var/parquet")]
    parquet_root: PathBuf,
    #[arg(long, env = "MONTLOK_NATS_STREAM", default_value = "MONTLOK_EVENTS")]
    stream: String,
    #[arg(
        long,
        env = "MONTLOK_NATS_CONSUMER",
        default_value = "montlok-projector-v2"
    )]
    consumer: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "montlok_projector=info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();
    let args = Arguments::parse();
    let store = Arc::new(ProjectionStore::open(&args.database, &args.parquet_root)?);
    let client = async_nats::connect(&args.nats_url).await?;
    let jetstream = jetstream::new(client);
    let events = montlok_event_transport::ensure_event_stream(&jetstream, &args.stream).await?;
    let consumer = events
        .get_or_create_consumer(
            &args.consumer,
            pull::Config {
                durable_name: Some(args.consumer.clone()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await?;
    let messages = tokio_stream::StreamExt::chunks_timeout(
        consumer.messages().await?,
        256,
        Duration::from_millis(1),
    );
    tokio::pin!(messages);
    let archive_store = store.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let worker_store = archive_store.clone();
            match tokio::task::spawn_blocking(move || worker_store.archive_unarchived(50_000))
                .await
                .unwrap_or_else(|error| Err(error.into()))
            {
                Ok(Some(path)) => {
                    tracing::info!(path = %path.display(), "archived projected events")
                }
                Ok(None) => {}
                Err(error) => tracing::error!(?error, "event archive failed"),
            }
        }
    });
    while let Some(batch) = messages.next().await {
        let messages = batch.into_iter().collect::<Result<Vec<_>, _>>()?;
        let events = messages
            .iter()
            .map(|message| v2::EventEnvelope::decode(message.payload.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let worker_store = store.clone();
        match tokio::task::spawn_blocking(move || worker_store.insert_batch(&events)).await? {
            Ok(_) => {
                for message in messages {
                    message
                        .ack()
                        .await
                        .map_err(|error| anyhow::anyhow!("JetStream acknowledgement: {error}"))?;
                }
            }
            Err(error) => {
                tracing::error!(?error, "projection rejected event batch");
                return Err(error);
            }
        }
    }
    Ok(())
}
