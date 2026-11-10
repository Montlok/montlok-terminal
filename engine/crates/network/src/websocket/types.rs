// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Shared transport halves, message handlers, and writer commands.
//!
//! [`MessageReader`] and the internal writer use [`BoxedWsTransport`], keeping lifecycle code
//! independent of the selected transport backend. [`MessageHandler`] receives messages without
//! connection identity, while [`EpochMessageHandler`] attaches the owning reader's connection epoch
//! to application messages and reconnect notifications.
//!
//! The channel handlers convert neutral [`Message`] values to Tungstenite messages at the adapter
//! compatibility boundary. `WriterCommand` remains internal so all sink access, connection swaps,
//! and reconnect buffering stay serialized in the writer task.

use std::{fmt::Debug, sync::Arc};

use futures_util::stream::{SplitSink, SplitStream};
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    error::SendError,
    transport::{BoxedWsTransport, Message},
};

/// Sink half of the active WebSocket transport.
///
/// Backed by [`BoxedWsTransport`], so the writer is decoupled from the concrete
/// backend stream type. Sends are keyed off the neutral [`Message`] enum.
pub(crate) type MessageWriter = SplitSink<BoxedWsTransport, Message>;

/// Stream half of the active WebSocket transport.
///
/// Backed by [`BoxedWsTransport`], yielding neutral [`Message`] values regardless
/// of the underlying backend.
pub type MessageReader = SplitStream<BoxedWsTransport>;

/// Function type for handling WebSocket messages.
///
/// When provided, the client will spawn an internal task to read messages and pass them
/// to this handler. This enables automatic reconnection where the client can replace the
/// reader internally.
///
/// When `None`, the client returns a `MessageReader` stream (via `connect_stream`) that
/// the caller owns and reads from directly. This disables automatic reconnection because
/// the reader cannot be replaced - the caller must manually reconnect.
pub type MessageHandler = Arc<dyn Fn(Message) + Send + Sync>;

/// Function type for handling WebSocket messages with connection ownership.
///
/// The initial connection has epoch `0`. Each replacement connection increments the epoch, and
/// every application or reconnect-control message carries the epoch of its originating reader.
pub type EpochMessageHandler = Arc<dyn Fn(u64, Message) + Send + Sync>;

/// Function type for handling WebSocket ping messages.
pub type PingHandler = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

/// Failure reported by a bounded channel message handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelMessageHandlerFailure {
    /// The receiver did not drain the channel before its fixed capacity was reached.
    Full,
    /// The receiver was dropped.
    Closed,
    /// A neutral transport message could not be represented by the legacy message type.
    Conversion,
}

/// Callback invoked when a bounded channel handler cannot forward a message.
pub type ChannelMessageHandlerFailureCallback =
    Arc<dyn Fn(ChannelMessageHandlerFailure) + Send + Sync>;

/// Failure reported by the optional bounded WebSocket writer admission gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterQueueFailure {
    /// All fixed writer slots were occupied.
    Full,
    /// The writer task had already closed its receiver.
    Closed,
}

/// Callback invoked when bounded writer admission fails.
pub type WriterQueueFailureCallback = Arc<dyn Fn(WriterQueueFailure) + Send + Sync>;

/// Creates a channel-based message handler.
///
/// Returns a tuple containing the message handler and a receiver for messages.
///
/// During the migration to the [`crate::transport`] abstraction the receiver still
/// yields `tokio_tungstenite::tungstenite::Message` so the 18 adapter crates keep
/// compiling unchanged. The neutral [`Message`] is converted at the channel
/// boundary; phase 3 of the migration switches both ends to the neutral type and
/// removes this shim.
#[must_use]
pub fn channel_message_handler() -> (
    MessageHandler,
    tokio::sync::mpsc::UnboundedReceiver<tokio_tungstenite::tungstenite::Message>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handler: MessageHandler = Arc::new(move |msg: Message| {
        match tokio_tungstenite::tungstenite::Message::try_from(msg) {
            Ok(legacy) => {
                if let Err(e) = tx.send(legacy) {
                    log::debug!("Failed to send message to channel: {e}");
                }
            }
            Err(e) => log::debug!("Dropping message that failed legacy conversion: {e}"),
        }
    });
    (handler, rx)
}

/// Creates a bounded channel-based message handler.
///
/// Unlike [`channel_message_handler`], this boundary never accumulates messages without a hard
/// limit. Full, closed, and conversion failures are synchronously reported to `on_failure`; the
/// caller is responsible for treating the callback as terminal when message loss is unsafe.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[must_use]
pub fn bounded_channel_message_handler(
    capacity: usize,
    on_failure: ChannelMessageHandlerFailureCallback,
) -> (
    MessageHandler,
    tokio::sync::mpsc::Receiver<tokio_tungstenite::tungstenite::Message>,
) {
    assert!(capacity > 0, "message channel capacity must be non-zero");

    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let handler: MessageHandler = Arc::new(move |msg: Message| {
        if failed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }

        let legacy = match tokio_tungstenite::tungstenite::Message::try_from(msg) {
            Ok(legacy) => legacy,
            Err(e) => {
                if failed
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_ok()
                {
                    log::error!("Failed to convert bounded channel message: {e}");
                    on_failure(ChannelMessageHandlerFailure::Conversion);
                }
                return;
            }
        };

        if let Err(e) = tx.try_send(legacy) {
            let failure = match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    ChannelMessageHandlerFailure::Full
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    ChannelMessageHandlerFailure::Closed
                }
            };
            if failed
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                log::error!("Failed to forward bounded channel message: {failure:?}");
                on_failure(failure);
            }
        }
    });
    (handler, rx)
}

/// Creates a channel-based message handler carrying each reader's connection epoch.
///
/// The receiver preserves the handler's `(connection_epoch, message)` order.
#[must_use]
pub fn channel_epoch_message_handler() -> (
    EpochMessageHandler,
    tokio::sync::mpsc::UnboundedReceiver<(u64, tokio_tungstenite::tungstenite::Message)>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handler: EpochMessageHandler =
        Arc::new(
            move |epoch, msg: Message| match tokio_tungstenite::tungstenite::Message::try_from(msg)
            {
                Ok(legacy) => {
                    if let Err(e) = tx.send((epoch, legacy)) {
                        log::debug!("Failed to send connection message to channel: {e}");
                    }
                }
                Err(e) => log::debug!("Dropping message that failed legacy conversion: {e}"),
            },
        );
    (handler, rx)
}

/// Represents a command for the writer task.
pub(crate) enum WriterCommand {
    /// Update the writer reference with a new one after reconnection.
    Update(MessageWriter, tokio::sync::oneshot::Sender<u64>),
    /// Send message to the server.
    Send(Message),
    /// Send a message admitted through a bounded writer gate.
    BoundedSend {
        message: Message,
        permit: OwnedSemaphorePermit,
    },
    /// Send once if the active connection still owns the message.
    SendOnConnection {
        message: Message,
        connection_epoch: u64,
        response_tx: tokio::sync::oneshot::Sender<Result<(), SendError>>,
    },
    /// Send once on the owning connection through a bounded writer gate.
    BoundedSendOnConnection {
        message: Message,
        connection_epoch: u64,
        response_tx: tokio::sync::oneshot::Sender<Result<(), SendError>>,
        _permit: OwnedSemaphorePermit,
    },
}

impl Debug for WriterCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Update(_, _) => f.debug_tuple("Update").field(&"<writer>").finish(),
            Self::Send(msg) => f.debug_tuple("Send").field(msg).finish(),
            Self::BoundedSend { message, .. } => {
                f.debug_tuple("BoundedSend").field(message).finish()
            }
            Self::SendOnConnection { message, .. } => {
                f.debug_tuple("SendOnConnection").field(message).finish()
            }
            Self::BoundedSendOnConnection { message, .. } => f
                .debug_tuple("BoundedSendOnConnection")
                .field(message)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use rstest::rstest;
    use tokio_tungstenite::tungstenite::Message as TgMessage;

    use super::*;
    use crate::RECONNECTED;

    #[rstest]
    fn channel_handler_drops_invalid_utf8_text_without_panic() {
        let (handler, mut rx) = channel_message_handler();

        // Invalid UTF-8 in a Text frame must not propagate or panic,
        // the shim logs and drops it when Utf8Bytes::try_from fails.
        handler(Message::Text(Bytes::from_static(&[0xFF, 0xFE])));
        handler(Message::Binary(Bytes::from_static(b"ok")));

        let received = rx.try_recv().expect("binary should arrive");
        assert!(matches!(received, TgMessage::Binary(ref b) if b.as_ref() == b"ok"));
        assert!(rx.try_recv().is_err(), "no further messages expected");
    }

    #[rstest]
    fn channel_handler_forwards_valid_text() {
        let (handler, mut rx) = channel_message_handler();
        handler(Message::text("hello"));

        let received = rx.try_recv().expect("text should arrive");
        match received {
            TgMessage::Text(t) => assert_eq!(t.as_str(), "hello"),
            other => panic!("expected text, was {other:?}"),
        }
    }

    #[rstest]
    fn epoch_message_handler_forwards_epoch_and_text() {
        let (handler, mut rx) = channel_epoch_message_handler();
        handler(7, Message::text("hello"));

        let (connection_epoch, received) = rx.try_recv().expect("text should arrive");
        assert_eq!(connection_epoch, 7);
        match received {
            TgMessage::Text(text) => assert_eq!(text.as_str(), "hello"),
            other => panic!("expected text, was {other:?}"),
        }
    }

    #[rstest]
    fn channel_handler_forwards_reconnected_text_to_rust_consumers() {
        let (handler, mut rx) = channel_message_handler();
        handler(Message::text(RECONNECTED));

        let received = rx.try_recv().expect("reconnect text should arrive");
        match received {
            TgMessage::Text(t) => assert_eq!(t.as_str(), RECONNECTED),
            other => panic!("expected text, was {other:?}"),
        }
    }

    #[rstest]
    fn bounded_channel_handler_reports_full_without_dropping_silently() {
        let failures = Arc::new(Mutex::new(Vec::new()));
        let callback_failures = Arc::clone(&failures);
        let callback: ChannelMessageHandlerFailureCallback = Arc::new(move |failure| {
            callback_failures.lock().unwrap().push(failure);
        });
        let (handler, mut rx) = bounded_channel_message_handler(1, callback);

        handler(Message::text("first"));
        handler(Message::text("overflow"));

        assert_eq!(
            failures.lock().unwrap().as_slice(),
            &[ChannelMessageHandlerFailure::Full]
        );
        assert!(matches!(rx.try_recv(), Ok(TgMessage::Text(text)) if text == "first"));
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn bounded_channel_handler_reports_closed_receiver() {
        let failures = Arc::new(Mutex::new(Vec::new()));
        let callback_failures = Arc::clone(&failures);
        let callback: ChannelMessageHandlerFailureCallback = Arc::new(move |failure| {
            callback_failures.lock().unwrap().push(failure);
        });
        let (handler, rx) = bounded_channel_message_handler(1, callback);
        drop(rx);

        handler(Message::text("after-close"));

        assert_eq!(
            failures.lock().unwrap().as_slice(),
            &[ChannelMessageHandlerFailure::Closed]
        );
    }

    #[rstest]
    fn writer_command_send_debug_includes_message() {
        let cmd = WriterCommand::Send(Message::text("hi"));
        let formatted = format!("{cmd:?}");
        assert!(
            formatted.starts_with("Send("),
            "unexpected debug output: {formatted}"
        );
        assert!(
            formatted.contains("Text"),
            "debug output should retain the message variant: {formatted}"
        );
        assert!(
            formatted.contains("hi"),
            "debug output should retain the payload bytes: {formatted}"
        );
    }
}
