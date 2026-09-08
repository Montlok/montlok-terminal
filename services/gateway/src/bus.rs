use crate::EventHub;
use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull};
use futures_util::StreamExt;
use montlok_contracts::v2::EventEnvelope;
use prost::Message;
use std::time::Duration;

/// Each gateway deployment has its own durable consumer and journal.
pub async fn follow(hub: EventHub, url: &str, consumer_name: &str) -> Result<()> {
    let client = async_nats::connect(url).await?;
    let jetstream = jetstream::new(client);
    let events = montlok_event_transport::ensure_event_stream(
        &jetstream,
        montlok_event_transport::STREAM_NAME,
    )
    .await?;
    let consumer = events
        .get_or_create_consumer(
            consumer_name,
            pull::Config {
                durable_name: Some(consumer_name.into()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_ack_pending: 2048,
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
    while let Some(batch) = messages.next().await {
        let messages = batch.into_iter().collect::<Result<Vec<_>, _>>()?;
        let envelopes = messages
            .iter()
            .map(|message| EventEnvelope::decode(message.payload.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        hub.publish_batch(envelopes).await?;
        for message in messages {
            message
                .ack()
                .await
                .map_err(|error| anyhow::anyhow!("gateway bus acknowledgement: {error}"))?;
        }
    }
    anyhow::bail!("gateway event subscription ended");
}
