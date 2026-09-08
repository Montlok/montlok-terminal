//! All producers and consumers use this exact stream policy, regardless of startup order.
use async_nats::jetstream::{self, stream};
use std::time::Duration;
pub const STREAM_NAME: &str = "MONTLOK_EVENTS";
pub fn event_stream_config(name: &str) -> stream::Config {
    stream::Config {
        name: name.into(),
        subjects: vec!["montlok.events.>".into()],
        storage: stream::StorageType::File,
        discard: stream::DiscardPolicy::New,
        max_age: Duration::from_secs(30 * 24 * 60 * 60),
        ..Default::default()
    }
}
pub async fn ensure_event_stream(
    context: &jetstream::Context,
    name: &str,
) -> anyhow::Result<stream::Stream> {
    Ok(context
        .get_or_create_stream(event_stream_config(name))
        .await?)
}
