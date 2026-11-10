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

//! WebSocket message handler for OKX.
//!
//! The handler is a thin I/O boundary between the network layer and the client. It owns the
//! `WebSocketClient`, deserializes raw venue messages into `OKXWsMessage` events, and handles
//! subscription management, authentication, and retry logic.
//!
//! All domain parsing (venue types to Nautilus types) occurs outside the handler:
//! - Data parsing in `PyOKXWebSocketClient` (uses an instruments cache)
//! - Execution parsing in `execution.rs` (uses the system Cache)

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use nautilus_model::identifiers::ClientOrderId;
use nautilus_network::{
    RECONNECTED,
    error::SendError,
    retry::{RetryManager, create_websocket_retry_manager},
    websocket::{AuthTracker, SubscriptionState, TEXT_PING, TEXT_PONG, WebSocketClient},
};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use super::{
    enums::{OKXSubscriptionEvent, OKXWsChannel, OKXWsOperation},
    error::OKXWsError,
    messages::{
        OKXOrderMsg, OKXSubscription, OKXSubscriptionArg, OKXWebSocketArg, OKXWebSocketError,
        OKXWsFrame, OKXWsMessage,
    },
    subscription::topic_from_websocket_arg,
};
use crate::websocket::client::{
    OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE, OKXBudgetedMessage, OKXWebSocketQueue,
    OKXWebSocketQueueFailureGuard, OKXWebSocketQueueFailureKind,
};
use crate::{
    common::{
        consts::{OKX_FIELD_SMSG, OKX_SUCCESS_CODE, should_retry_error_code},
        enums::{OKXOrderStatus, OKXOrderType},
        parse::prefer_rpi_response_fields,
    },
    websocket::client::OKX_RATE_LIMIT_KEY_SUBSCRIPTION,
};

/// Commands sent from the outer client to the inner message handler.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Set the WebSocketClient for the handler to use.
    SetClient(WebSocketClient),
    /// Disconnect the WebSocket connection.
    Disconnect,
    /// Send authentication payload to the WebSocket.
    Authenticate { payload: String },
    /// Subscribe to the given channels.
    Subscribe { args: Vec<OKXSubscriptionArg> },
    /// Unsubscribe from the given channels.
    Unsubscribe { args: Vec<OKXSubscriptionArg> },
    /// Send a pre-serialized payload (used for order operations).
    Send {
        payload: String,
        rate_limit_keys: Option<Vec<Ustr>>,
        request_id: Option<String>,
        client_order_id: Option<ClientOrderId>,
        op: Option<OKXWsOperation>,
        connection_epoch: u64,
    },
}

pub(super) struct OKXWsFeedHandler {
    signal: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    queue_failure: OKXWebSocketQueueFailureGuard,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::Receiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::Receiver<OKXBudgetedMessage<Message>>,
    out_tx: tokio::sync::mpsc::Sender<OKXBudgetedMessage<OKXWsMessage>>,
    auth_tracker: AuthTracker,
    subscriptions_state: SubscriptionState,
    retry_manager: RetryManager<OKXWsError>,
}

impl OKXWsFeedHandler {
    /// Creates a new [`OKXWsFeedHandler`] instance.
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor receives the three bounded channel endpoints and their shared terminal controls"
    )]
    pub(super) fn new(
        signal: Arc<AtomicBool>,
        shutdown_notify: Arc<tokio::sync::Notify>,
        queue_failure: OKXWebSocketQueueFailureGuard,
        cmd_rx: tokio::sync::mpsc::Receiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::Receiver<OKXBudgetedMessage<Message>>,
        out_tx: tokio::sync::mpsc::Sender<OKXBudgetedMessage<OKXWsMessage>>,
        auth_tracker: AuthTracker,
        subscriptions_state: SubscriptionState,
    ) -> Self {
        Self {
            signal,
            shutdown_notify,
            queue_failure,
            inner: None,
            cmd_rx,
            raw_rx,
            out_tx,
            auth_tracker,
            subscriptions_state,
            retry_manager: create_websocket_retry_manager(),
        }
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(Ordering::Acquire)
    }

    pub(super) fn send(
        &self,
        msg: OKXBudgetedMessage<OKXWsMessage>,
    ) -> Result<(), OKXWebSocketQueueFailureKind> {
        self.queue_failure
            .try_send(OKXWebSocketQueue::Output, &self.out_tx, msg)
    }

    async fn send_with_retry(
        &self,
        payload: String,
        rate_limit_keys: Option<&[Ustr]>,
    ) -> Result<(), OKXWsError> {
        validate_outbound_payload(&payload)?;
        if let Some(client) = &self.inner {
            let keys_owned: Option<Vec<Ustr>> = rate_limit_keys.map(|k| k.to_vec());
            self.retry_manager
                .execute_with_retry(
                    "websocket_send",
                    || {
                        let payload = payload.clone();
                        let keys = keys_owned.clone();
                        async move {
                            client
                                .send_text(payload, keys.as_deref())
                                .await
                                .map_err(|e| {
                                    match &e {
                                        SendError::BrokenPipe(_) => self.queue_failure.fail(
                                            OKXWebSocketQueue::Writer,
                                            OKXWebSocketQueueFailureKind::Closed,
                                        ),
                                        SendError::Closed
                                        | SendError::Timeout
                                        | SendError::WriteTimeout
                                        | SendError::ConnectionChanged => {}
                                    }
                                    OKXWsError::ClientError(format!("Send failed: {e}"))
                                })
                        }
                    },
                    should_retry_okx_error,
                    create_okx_timeout_error,
                )
                .await
        } else {
            Err(OKXWsError::ClientError(
                "No active WebSocket client".to_string(),
            ))
        }
    }

    /// Sends a trading command exactly once on the connection that owned it at admission time.
    ///
    /// This path deliberately bypasses the generic retry/reconnect replay machinery. A timeout
    /// after the transport write starts has indeterminate delivery and must be reconciled from
    /// venue state, never retried blindly.
    async fn send_trading_command_once(
        &self,
        payload: String,
        rate_limit_keys: Option<&[Ustr]>,
        expected_connection_epoch: u64,
    ) -> Result<(), OKXWsError> {
        validate_outbound_payload(&payload)?;
        let _admission = self
            .queue_failure
            .try_acquire_trading_admission()
            .ok_or_else(|| {
                OKXWsError::ClientError(
                    "Terminal WebSocket queue failure rejects trading command".to_string(),
                )
            })?;
        if self.signal.load(Ordering::Acquire) {
            return Err(OKXWsError::ClientError(
                "WebSocket shutdown rejects trading command".to_string(),
            ));
        }
        let client = self
            .inner
            .as_ref()
            .ok_or_else(|| OKXWsError::ClientError("No active WebSocket client".to_string()))?;
        let connection_epoch = client.connection_epoch();
        if connection_epoch != expected_connection_epoch {
            return Err(OKXWsError::ClientError(format!(
                "One-shot trading command connection epoch changed before write: \
                 admitted={expected_connection_epoch}, current={connection_epoch}"
            )));
        }
        client
            .send_text_on_connection(payload, rate_limit_keys, expected_connection_epoch)
            .await
            .map_err(|error| {
                match &error {
                    SendError::BrokenPipe(_) => self.queue_failure.fail(
                        OKXWebSocketQueue::Writer,
                        OKXWebSocketQueueFailureKind::Closed,
                    ),
                    SendError::Closed
                    | SendError::Timeout
                    | SendError::WriteTimeout
                    | SendError::ConnectionChanged => {}
                }
                OKXWsError::ClientError(format!(
                    "One-shot trading command failed on connection epoch {connection_epoch}: {error}"
                ))
            })
    }

    pub(super) async fn send_pong(&self) -> anyhow::Result<()> {
        match self.send_with_retry(TEXT_PONG.to_string(), None).await {
            Ok(()) => {
                log::trace!("Sent pong response to OKX text ping");
                Ok(())
            }
            Err(e) => {
                log::warn!("Failed to send pong after retries: error={e}");
                Err(anyhow::anyhow!("Failed to send pong: {e}"))
            }
        }
    }

    pub(super) async fn next(&mut self) -> Option<OKXBudgetedMessage<OKXWsMessage>> {
        if self.signal.load(Ordering::Acquire) {
            if let Some(client) = self.inner.take() {
                client.disconnect().await;
            }
            return None;
        }

        loop {
            tokio::select! {
                biased;

                () = self.shutdown_notify.notified() => {
                    if self.signal.load(Ordering::Acquire) {
                        if let Some(client) = self.inner.take() {
                            client.disconnect().await;
                        }
                        return None;
                    }
                }

                Some(cmd) = self.cmd_rx.recv() => {
                    if self.signal.load(Ordering::Acquire) || self.queue_failure.is_failed() {
                        if let Some(client) = self.inner.take() {
                            client.disconnect().await;
                        }
                        return None;
                    }
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            log::debug!("Handler received WebSocket client");
                            self.inner = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            log::debug!("Handler disconnecting WebSocket client");
                            if let Some(client) = self.inner.take() {
                                client.disconnect().await;
                            }
                            return None;
                        }
                        HandlerCommand::Authenticate { payload } => {
                            if let Err(e) = self.send_with_retry(
                                payload,
                                Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice()),
                            ).await {
                                log::error!(
                                    "Failed to send authentication message after retries: error={e}"
                                );
                                self.auth_tracker.fail(e.to_string());
                                self.queue_failure.fail(
                                    OKXWebSocketQueue::Writer,
                                    OKXWebSocketQueueFailureKind::Closed,
                                );
                                if let Some(client) = self.inner.take() {
                                    client.disconnect().await;
                                }
                                return None;
                            }
                        }
                        HandlerCommand::Subscribe { args } => {
                            if let Err(e) = self.handle_subscribe(args).await {
                                log::error!("Failed to handle subscribe command: error={e}");
                                self.queue_failure.fail(
                                    OKXWebSocketQueue::Writer,
                                    OKXWebSocketQueueFailureKind::Closed,
                                );
                                if let Some(client) = self.inner.take() {
                                    client.disconnect().await;
                                }
                                return None;
                            }
                        }
                        HandlerCommand::Unsubscribe { args } => {
                            if let Err(e) = self.handle_unsubscribe(args).await {
                                log::error!("Failed to handle unsubscribe command: error={e}");
                                self.queue_failure.fail(
                                    OKXWebSocketQueue::Writer,
                                    OKXWebSocketQueueFailureKind::Closed,
                                );
                                if let Some(client) = self.inner.take() {
                                    client.disconnect().await;
                                }
                                return None;
                            }
                        }
                        HandlerCommand::Send {
                            payload,
                            rate_limit_keys,
                            request_id,
                            client_order_id,
                            op,
                            connection_epoch,
                        } => {
                            if let Err(e) = self.send_trading_command_once(
                                payload,
                                rate_limit_keys.as_deref(),
                                connection_epoch,
                            ).await {
                                log::error!("One-shot trading command failed: error={e}");

                                self.queue_failure.fail(
                                    OKXWebSocketQueue::PendingOperations,
                                    OKXWebSocketQueueFailureKind::PendingAmbiguous,
                                );

                                if let Some(request_id) = request_id {
                                    return Some(OKXBudgetedMessage::unbudgeted(OKXWsMessage::SendFailed {
                                        request_id,
                                        client_order_id,
                                        op,
                                        error: format!("{e}"),
                                    }));
                                }
                            }
                        }
                    }
                }

                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    if self.signal.load(Ordering::Acquire) {
                        log::debug!("Stop signal received during idle period");
                        if let Some(client) = self.inner.take() {
                            client.disconnect().await;
                        }
                        return None;
                    }
                }

                msg = self.raw_rx.recv() => {
                    let (event, permit) = match msg {
                        Some(OKXBudgetedMessage { message, permit }) => {
                            match Self::parse_raw_message(message) {
                                Ok(Some(event)) => (event, permit),
                                Ok(None) => continue,
                                Err(error) => {
                                    log::error!("Critical OKX WebSocket frame parse failed: {error}");
                                    self.queue_failure.fail(
                                        OKXWebSocketQueue::Raw,
                                        OKXWebSocketQueueFailureKind::Conversion,
                                    );
                                    if let Some(client) = self.inner.take() {
                                        client.disconnect().await;
                                    }
                                    return None;
                                }
                            }
                        }
                        None => {
                            log::debug!("WebSocket stream closed");
                            return None;
                        }
                    };

                    match event {
                        OKXWsFrame::Ping => {
                            if let Err(e) = self.send_pong().await {
                                log::warn!("Failed to send pong response: error={e}");
                            }
                        }
                        OKXWsFrame::Login {
                            code, msg, conn_id, ..
                        } => {
                            if code == OKX_SUCCESS_CODE {
                                self.auth_tracker.succeed();
                                return Some(OKXBudgetedMessage {
                                    message: OKXWsMessage::Authenticated,
                                    permit,
                                });
                            }

                            log::error!("WebSocket authentication failed: error={msg}");
                            self.auth_tracker.fail(msg.clone());

                            let error = OKXWebSocketError {
                                code,
                                message: msg,
                                conn_id: Some(conn_id),
                                timestamp: nautilus_core::time::get_atomic_clock_realtime()
                                    .get_time_ns()
                                    .as_u64(),
                            };
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::Error(error),
                                permit,
                            });
                        }
                        OKXWsFrame::BookData { arg, action, data } => {
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::BookData { arg, action, data },
                                permit,
                            });
                        }
                        OKXWsFrame::RpiBookData { arg, action, data } => {
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::RpiBookData { arg, action, data },
                                permit,
                            });
                        }
                        OKXWsFrame::OrderResponse {
                            id, op, code, msg, data,
                        } => {
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::OrderResponse { id, op, code, msg, data },
                                permit,
                            });
                        }
                        OKXWsFrame::Data { arg, data } => {
                            match self.route_data_message(arg, data) {
                                Ok(Some(output)) => {
                                    return Some(OKXBudgetedMessage {
                                        message: output,
                                        permit,
                                    });
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    log::error!(
                                        "Critical OKX private/business payload parse failed: {error}"
                                    );
                                    self.queue_failure.fail(
                                        OKXWebSocketQueue::Raw,
                                        OKXWebSocketQueueFailureKind::Conversion,
                                    );
                                    if let Some(client) = self.inner.take() {
                                        client.disconnect().await;
                                    }
                                    return None;
                                }
                            }
                        }
                        OKXWsFrame::Error { code, msg } => {
                            let error = OKXWebSocketError {
                                code,
                                message: msg,
                                conn_id: None,
                                timestamp: nautilus_core::time::get_atomic_clock_realtime()
                                    .get_time_ns()
                                    .as_u64(),
                            };
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::Error(error),
                                permit,
                            });
                        }
                        OKXWsFrame::Reconnected => {
                            self.auth_tracker.invalidate();
                            return Some(OKXBudgetedMessage {
                                message: OKXWsMessage::Reconnected,
                                permit,
                            });
                        }
                        OKXWsFrame::Subscription {
                            event, arg, code, msg, ..
                        } => {
                            self.handle_subscription_ack(&event, &arg, code.as_deref(), msg.as_deref());
                        }
                        OKXWsFrame::ChannelConnCount { .. } => {}
                    }
                }

                else => {
                    log::debug!("Handler shutting down: stream ended or command channel closed");
                    return None;
                }
            }
        }
    }

    fn route_data_message(
        &self,
        arg: OKXWebSocketArg,
        mut data: Value,
    ) -> anyhow::Result<Option<OKXWsMessage>> {
        let OKXWebSocketArg {
            channel, inst_id, ..
        } = arg;

        match channel {
            OKXWsChannel::Account => Ok(Some(OKXWsMessage::Account(data))),
            OKXWsChannel::Positions => Ok(Some(OKXWsMessage::Positions(data))),
            OKXWsChannel::Orders => parse_critical_array(data, "orders")
                .map(|messages| Some(OKXWsMessage::Orders(messages))),
            OKXWsChannel::SprdOrders => parse_critical_array(data, "spread orders")
                .map(|messages| Some(OKXWsMessage::SpreadOrders(messages))),
            OKXWsChannel::OrdersAlgo | OKXWsChannel::AlgoAdvance => {
                parse_critical_array(data, "algo orders")
                    .map(|messages| Some(OKXWsMessage::AlgoOrders(messages)))
            }
            OKXWsChannel::Instruments => {
                prefer_rpi_response_fields(&mut data);
                Ok(parse_best_effort_array(data, "instruments").map(OKXWsMessage::Instruments))
            }
            _ => Ok(Some(OKXWsMessage::ChannelData {
                channel,
                inst_id,
                data,
            })),
        }
    }

    fn handle_subscription_ack(
        &self,
        event: &OKXSubscriptionEvent,
        arg: &OKXWebSocketArg,
        code: Option<&str>,
        msg: Option<&str>,
    ) {
        let topic = topic_from_websocket_arg(arg);
        let success = code.is_none_or(|c| c == OKX_SUCCESS_CODE);

        match event {
            OKXSubscriptionEvent::Subscribe => {
                if success {
                    self.subscriptions_state.confirm_subscribe(&topic);
                } else {
                    log::warn!(
                        "Subscription failed: topic={topic:?}, error={msg:?}, code={code:?}"
                    );
                    self.subscriptions_state.mark_failure(&topic);
                    self.queue_failure.report_subscription_failure(format!(
                        "okx_data_subscription_rejected: topic={topic:?}, code={code:?}, message={msg:?}"
                    ));
                }
            }
            OKXSubscriptionEvent::Unsubscribe => {
                if success {
                    self.subscriptions_state.confirm_unsubscribe(&topic);
                } else {
                    log::warn!(
                        "Unsubscription failed - restoring subscription: \
                         topic={topic:?}, error={msg:?}, code={code:?}"
                    );
                    self.subscriptions_state.confirm_unsubscribe(&topic);
                    self.subscriptions_state.mark_subscribe(&topic);
                    self.subscriptions_state.confirm_subscribe(&topic);
                }
            }
        }
    }

    async fn handle_subscribe(&self, args: Vec<OKXSubscriptionArg>) -> anyhow::Result<()> {
        for arg in &args {
            log::debug!(
                "Subscribing to channel: channel={:?}, inst_id={:?}",
                arg.channel,
                arg.inst_id
            );
        }

        let message = OKXSubscription {
            op: OKXWsOperation::Subscribe,
            args,
        };

        let json_txt = serde_json::to_string(&message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize subscription: {e}"))?;

        self.send_with_retry(json_txt, Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send subscription after retries: {e}"))?;
        Ok(())
    }

    async fn handle_unsubscribe(&self, args: Vec<OKXSubscriptionArg>) -> anyhow::Result<()> {
        for arg in &args {
            log::debug!(
                "Unsubscribing from channel: channel={:?}, inst_id={:?}",
                arg.channel,
                arg.inst_id
            );
        }

        let message = OKXSubscription {
            op: OKXWsOperation::Unsubscribe,
            args,
        };

        let json_txt = serde_json::to_string(&message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize unsubscription: {e}"))?;

        self.send_with_retry(json_txt, Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send unsubscription after retries: {e}"))?;
        Ok(())
    }

    pub(crate) fn parse_raw_message(
        msg: tokio_tungstenite::tungstenite::Message,
    ) -> anyhow::Result<Option<OKXWsFrame>> {
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                if text == TEXT_PONG {
                    log::trace!("Received pong from OKX");
                    return Ok(None);
                }

                if text == TEXT_PING {
                    log::trace!("Received ping from OKX (text)");
                    return Ok(Some(OKXWsFrame::Ping));
                }

                if text == RECONNECTED {
                    log::debug!("Received WebSocket reconnection signal");
                    return Ok(Some(OKXWsFrame::Reconnected));
                }
                log::trace!("Received WebSocket message: {text}");

                match serde_json::from_str(&text) {
                    Ok(ws_event) => match &ws_event {
                        OKXWsFrame::Error { code, msg } => {
                            if should_retry_error_code(code) {
                                log::warn!("WebSocket error: {code} - {msg}");
                            } else {
                                log::error!("WebSocket error: {code} - {msg}");
                            }
                            Ok(Some(ws_event))
                        }
                        OKXWsFrame::Login {
                            event,
                            code,
                            msg,
                            conn_id,
                        } => {
                            if code == OKX_SUCCESS_CODE {
                                log::debug!("WebSocket authenticated: conn_id={conn_id}");
                            } else {
                                log::error!(
                                    "WebSocket authentication failed: \
                                     event={event}, code={code}, error={msg}"
                                );
                            }
                            Ok(Some(ws_event))
                        }
                        OKXWsFrame::Subscription {
                            event,
                            arg,
                            conn_id,
                            ..
                        } => {
                            let channel_str = serde_json::to_string(&arg.channel)
                                .expect("Invalid OKX websocket channel")
                                .trim_matches('"')
                                .to_string();
                            log::debug!("{event}d: channel={channel_str}, conn_id={conn_id}");
                            Ok(Some(ws_event))
                        }
                        OKXWsFrame::ChannelConnCount {
                            channel,
                            conn_count,
                            conn_id,
                            ..
                        } => {
                            let channel_str = serde_json::to_string(channel)
                                .expect("Invalid OKX websocket channel")
                                .trim_matches('"')
                                .to_string();
                            log::debug!(
                                "Channel connection status: \
                                 channel={channel_str}, connections={conn_count}, conn_id={conn_id}",
                            );
                            Ok(None)
                        }
                        OKXWsFrame::Ping => {
                            log::trace!("Ignoring ping event parsed from text payload");
                            Ok(None)
                        }
                        OKXWsFrame::Data { .. }
                        | OKXWsFrame::BookData { .. }
                        | OKXWsFrame::RpiBookData { .. } => Ok(Some(ws_event)),
                        OKXWsFrame::OrderResponse {
                            id, op, code, data, ..
                        } => {
                            if code == OKX_SUCCESS_CODE {
                                log::debug!(
                                    "Order operation successful: id={id:?}, op={op}, code={code}"
                                );

                                if let Some(order_data) = data.first() {
                                    let success_msg = order_data
                                        .get(OKX_FIELD_SMSG)
                                        .and_then(|s| s.as_str())
                                        .unwrap_or("Order operation successful");
                                    log::debug!("Order success details: {success_msg}");
                                }
                            }
                            Ok(Some(ws_event))
                        }
                        OKXWsFrame::Reconnected => {
                            log::warn!("Unexpected Reconnected event from deserialization");
                            Ok(None)
                        }
                    },
                    Err(error) => Err(anyhow::anyhow!(
                        "invalid OKX WebSocket JSON envelope ({} bytes): {error}",
                        text.len()
                    )),
                }
            }
            Message::Ping(_payload) => {
                log::trace!("Received binary ping frame from OKX");
                Ok(Some(OKXWsFrame::Ping))
            }
            Message::Pong(payload) => {
                log::trace!("Received pong frame from OKX ({} bytes)", payload.len());
                Ok(None)
            }
            Message::Binary(msg) => {
                log::debug!("Raw binary frame ({} bytes)", msg.len());
                log::trace!("Raw binary: {msg:?}");
                Ok(None)
            }
            Message::Close(_) => {
                log::debug!("Received close message");
                Ok(None)
            }
            msg => {
                log::warn!("Unexpected message: {msg}");
                Ok(None)
            }
        }
    }
}

/// Returns `true` when an OKX WebSocket order message represents a post-only auto-cancel.
pub fn is_post_only_auto_cancel(msg: &OKXOrderMsg) -> bool {
    use crate::common::{consts::OKX_POST_ONLY_CANCEL_SOURCE, enums::OKXOrderStatus};

    if msg.state != OKXOrderStatus::Canceled {
        return false;
    }

    let cancel_source_matches = matches!(
        msg.cancel_source.as_deref(),
        Some(source) if source == OKX_POST_ONLY_CANCEL_SOURCE
    );

    let reason_matches = matches!(
        msg.cancel_source_reason.as_deref(),
        Some(reason) if reason.contains("POST_ONLY")
    );

    if !(cancel_source_matches || reason_matches) {
        return false;
    }

    msg.acc_fill_sz
        .as_ref()
        .is_none_or(|filled| filled == "0" || filled.is_empty())
}

/// Returns `true` when an RPI order update is canceled without any fill.
pub fn is_unfilled_rpi_cancel(msg: &OKXOrderMsg) -> bool {
    msg.ord_type == OKXOrderType::Rpi
        && msg.state == OKXOrderStatus::Canceled
        && msg
            .acc_fill_sz
            .as_ref()
            .is_none_or(|filled| filled == "0" || filled.is_empty())
}

/// Atomically parses a private/business execution array.
///
/// One malformed item invalidates the whole venue update; returning a partial batch would make
/// order state impossible to prove.
fn parse_critical_array<T: serde::de::DeserializeOwned>(
    data: Value,
    label: &str,
) -> anyhow::Result<Vec<T>> {
    let Value::Array(items) = data else {
        anyhow::bail!("expected critical {label} payload to be a JSON array");
    };

    let mut parsed = Vec::with_capacity(items.len());
    for (idx, item) in items.into_iter().enumerate() {
        parsed.push(serde_json::from_value::<T>(item).map_err(|error| {
            anyhow::anyhow!("failed to parse critical {label} item at index {idx}: {error}")
        })?);
    }
    Ok(parsed)
}

/// Best-effort parsing retained for public instrument-definition updates.
fn parse_best_effort_array<T: serde::de::DeserializeOwned>(
    data: Value,
    label: &str,
) -> Option<Vec<T>> {
    let Value::Array(items) = data else {
        log::warn!("Expected {label} payload to be a JSON array");
        return None;
    };

    let mut parsed = Vec::with_capacity(items.len());
    for (idx, item) in items.into_iter().enumerate() {
        match serde_json::from_value::<T>(item) {
            Ok(value) => parsed.push(value),
            Err(error) => log::warn!("Failed to parse {label} item at index {idx}: {error}"),
        }
    }

    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

#[inline]
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

// Specific phrases rather than bare "connection"/"network", which appear
// in permanent errors too (e.g. "no active WebSocket client connection").
const RETRYABLE_CLIENT_ERROR_PHRASES: &[&str] = &[
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "connection closed",
    "connection aborted",
    "broken pipe",
    "network unreachable",
    "network is unreachable",
    "no route to host",
];

fn should_retry_okx_error(error: &OKXWsError) -> bool {
    match error {
        OKXWsError::OkxError { error_code, .. } => should_retry_error_code(error_code),
        OKXWsError::TungsteniteError(_) => true,
        OKXWsError::ClientError(msg) => RETRYABLE_CLIENT_ERROR_PHRASES
            .iter()
            .any(|phrase| contains_ignore_ascii_case(msg, phrase)),
        OKXWsError::AuthenticationError(_)
        | OKXWsError::JsonError(_)
        | OKXWsError::ParsingError(_) => false,
    }
}

fn create_okx_timeout_error(msg: String) -> OKXWsError {
    OKXWsError::ClientError(msg)
}

fn validate_outbound_payload(payload: &str) -> Result<(), OKXWsError> {
    if payload.len() > OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE {
        return Err(OKXWsError::ClientError(format!(
            "Serialized OKX WebSocket payload is {} bytes, limit is {} bytes",
            payload.len(),
            OKX_WS_MAX_OUTBOUND_MESSAGE_SIZE
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicBool};

    use nautilus_network::websocket::{AuthTracker, SubscriptionState};
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::common::{
        consts::OKX_WS_TOPIC_DELIMITER, enums::OKXRpiPermission, testing::load_test_json,
    };

    fn create_handler() -> OKXWsFeedHandler {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(16);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(16);

        OKXWsFeedHandler::new(
            signal,
            shutdown_notify,
            queue_failure,
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        )
    }

    #[test]
    fn subscription_ack_default_policy_does_not_close_or_notify() {
        let handler = create_handler();
        let arg: OKXWebSocketArg =
            serde_json::from_value(json!({"channel":"books5","instId":"BTC-USDT"})).unwrap();
        handler.handle_subscription_ack(
            &OKXSubscriptionEvent::Subscribe,
            &arg,
            Some("60012"),
            Some("rejected"),
        );
        assert!(!handler.is_stopped());
        assert!(handler.queue_failure.failure().is_none());
        assert!(
            handler
                .queue_failure
                .state_is_subscription_default_for_test()
        );
    }

    #[test]
    fn subscription_ack_opt_in_reports_rejection_while_socket_remains_open() {
        let handler = create_handler();
        let failures = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = failures.clone();
        handler
            .queue_failure
            .set_subscription_failure_callback(Arc::new(move |reason| {
                observed.lock().unwrap().push(reason)
            }));
        let arg: OKXWebSocketArg =
            serde_json::from_value(json!({"channel":"books5","instId":"BTC-USDT"})).unwrap();
        handler.handle_subscription_ack(&OKXSubscriptionEvent::Subscribe, &arg, Some("0"), None);
        assert!(failures.lock().unwrap().is_empty());
        handler.handle_subscription_ack(
            &OKXSubscriptionEvent::Subscribe,
            &arg,
            Some("60012"),
            Some("rejected"),
        );
        assert_eq!(failures.lock().unwrap().len(), 1);
        assert!(failures.lock().unwrap()[0].contains("60012"));
        assert!(!handler.is_stopped());
        assert!(handler.queue_failure.failure().is_none());
    }

    #[cfg(feature = "python")]
    #[test]
    fn subscription_ack_reaches_real_bridge_and_normal_close_ignores_late_ack() {
        use nautilus_common::python::data_ingress::PyDataIngress;
        use pyo3::{
            prelude::*,
            types::{PyList, PyModule},
        };
        Python::initialize();
        Python::attach(|py| {
            for close_first in [false, true] {
                let module = PyModule::from_code(py, c"class Loop:\n def __init__(self): self.wakes=[]\n def is_closed(self): return False\n def call_soon_threadsafe(self, cb): self.wakes.append(cb)\n def step(self): self.wakes.pop(0)()\n", c"ack_bridge.py", c"ack_bridge").unwrap();
                let loop_ = module.getattr("Loop").unwrap().call0().unwrap();
                let failures = PyList::empty(py);
                let owner = format!("ack-real-bridge-{close_first}");
                let bridge = py
                    .get_type::<PyDataIngress>()
                    .call1((&loop_, owner, failures.getattr("append").unwrap(), 8, 2))
                    .unwrap();
                let handler = create_handler();
                let capability = bridge.extract::<PyRef<'_, PyDataIngress>>().unwrap();
                handler
                    .queue_failure
                    .set_subscription_failure_callback(capability.failure_callback());
                drop(capability);
                if close_first {
                    bridge.call_method0("close").unwrap();
                }
                let arg: OKXWebSocketArg =
                    serde_json::from_value(json!({"channel":"books5","instId":"BTC-USDT"}))
                        .unwrap();
                handler.handle_subscription_ack(
                    &OKXSubscriptionEvent::Subscribe,
                    &arg,
                    Some("60012"),
                    Some("rejected"),
                );
                assert!(!handler.is_stopped());
                if close_first {
                    assert!(bridge.getattr("failure").unwrap().is_none());
                    assert_eq!(failures.len(), 0);
                } else {
                    assert!(
                        bridge
                            .getattr("failure")
                            .unwrap()
                            .extract::<String>()
                            .unwrap()
                            .contains("subscription_rejected")
                    );
                    loop_.call_method0("step").unwrap();
                    assert_eq!(failures.len(), 1);
                }
                bridge.call_method0("close").unwrap();
            }
        });
    }

    #[rstest]
    fn output_queue_full_is_terminal_and_never_silent() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(1);
        let handler = OKXWsFeedHandler::new(
            signal.clone(),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );

        handler
            .send(OKXBudgetedMessage::unbudgeted(OKXWsMessage::Reconnected))
            .unwrap();
        assert_eq!(
            handler.send(OKXBudgetedMessage::unbudgeted(OKXWsMessage::Reconnected)),
            Err(OKXWebSocketQueueFailureKind::Full)
        );
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            queue_failure.failure(),
            Some((
                OKXWebSocketQueue::Output,
                OKXWebSocketQueueFailureKind::Full
            ))
        );
        assert!(matches!(
            out_rx.try_recv(),
            Ok(OKXBudgetedMessage {
                message: OKXWsMessage::Reconnected,
                ..
            })
        ));
        assert!(out_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn raw_budget_permit_survives_parse_and_output_queue_until_consumed() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            signal,
            shutdown_notify,
            queue_failure,
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );
        let budget = Arc::new(tokio::sync::Semaphore::new(16));
        let permit = Arc::clone(&budget)
            .try_acquire_many_owned(16)
            .expect("test budget available");
        raw_tx
            .try_send(OKXBudgetedMessage {
                message: Message::Text(RECONNECTED.into()),
                permit: Some(permit),
            })
            .unwrap();

        let event = handler.next().await.expect("parsed reconnect event");
        assert_eq!(budget.available_permits(), 0);
        handler.send(event).unwrap();
        assert_eq!(budget.available_permits(), 0);

        let consumed = out_rx.recv().await.expect("queued output event");
        assert_eq!(budget.available_permits(), 0);
        drop(consumed);
        assert_eq!(budget.available_permits(), 16);
    }

    #[tokio::test]
    async fn malformed_json_envelope_latches_conversion_and_stops_handler() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            Arc::clone(&signal),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );
        raw_tx
            .try_send(OKXBudgetedMessage::unbudgeted(Message::Text("{".into())))
            .unwrap();

        assert!(handler.next().await.is_none());
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            queue_failure.failure(),
            Some((
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::Conversion,
            ))
        );
    }

    #[tokio::test]
    async fn malformed_private_item_rejects_whole_batch_and_stops_handler() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            Arc::clone(&signal),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );
        let mut frame: Value =
            serde_json::from_str(&load_test_json("ws_orders.json")).expect("valid fixture");
        let items = frame["data"].as_array_mut().expect("orders array");
        let mut invalid = items[0].clone();
        invalid["instType"] = json!(7);
        items.push(invalid);
        raw_tx
            .try_send(OKXBudgetedMessage::unbudgeted(Message::Text(
                serde_json::to_string(&frame).unwrap().into(),
            )))
            .unwrap();

        assert!(handler.next().await.is_none());
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            queue_failure.failure(),
            Some((
                OKXWebSocketQueue::Raw,
                OKXWebSocketQueueFailureKind::Conversion,
            ))
        );
    }

    #[tokio::test]
    async fn subscribe_send_failure_is_terminal_instead_of_silently_continuing() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            Arc::clone(&signal),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );
        cmd_tx
            .try_send(HandlerCommand::Subscribe { args: Vec::new() })
            .unwrap();

        assert!(handler.next().await.is_none());
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            queue_failure.failure(),
            Some((
                OKXWebSocketQueue::Writer,
                OKXWebSocketQueueFailureKind::Closed,
            ))
        );
    }

    #[tokio::test]
    async fn trading_send_failure_returns_once_then_fail_stops_the_handler() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(2);
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            Arc::clone(&signal),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );

        cmd_tx
            .try_send(HandlerCommand::Send {
                payload: "{}".to_string(),
                rate_limit_keys: None,
                request_id: Some("first".to_string()),
                client_order_id: None,
                op: Some(OKXWsOperation::Order),
                connection_epoch: 0,
            })
            .unwrap();

        let message = handler.next().await.expect("immediate SendFailed").message;
        assert!(matches!(
            message,
            OKXWsMessage::SendFailed {
                request_id,
                error,
                ..
            } if request_id == "first" && error.contains("No active WebSocket client")
        ));
        assert!(signal.load(Ordering::Acquire));
        assert_eq!(
            queue_failure.failure(),
            Some((
                OKXWebSocketQueue::PendingOperations,
                OKXWebSocketQueueFailureKind::PendingAmbiguous,
            )),
        );

        cmd_tx
            .try_send(HandlerCommand::Send {
                payload: "{}".to_string(),
                rate_limit_keys: None,
                request_id: Some("second".to_string()),
                client_order_id: None,
                op: Some(OKXWsOperation::Order),
                connection_epoch: 0,
            })
            .unwrap();
        assert!(
            handler.next().await.is_none(),
            "terminal send ambiguity must reject later commands without another send attempt",
        );
    }

    #[tokio::test]
    async fn shutdown_signal_bypasses_a_full_command_queue() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        cmd_tx.try_send(HandlerCommand::Disconnect).unwrap();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            signal.clone(),
            shutdown_notify.clone(),
            queue_failure,
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );

        signal.store(true, Ordering::Release);
        shutdown_notify.notify_waiters();

        assert!(handler.next().await.is_none());
    }

    #[tokio::test]
    async fn latched_queue_failure_rejects_queued_trading_command_before_transport() {
        let signal = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let queue_failure =
            OKXWebSocketQueueFailureGuard::new(Arc::clone(&signal), Arc::clone(&shutdown_notify));
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        cmd_tx
            .try_send(HandlerCommand::Send {
                payload: r#"{"op":"order"}"#.to_string(),
                rate_limit_keys: None,
                request_id: Some("terminal-order".to_string()),
                client_order_id: None,
                op: Some(OKXWsOperation::Order),
                connection_epoch: 0,
            })
            .unwrap();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
        let mut handler = OKXWsFeedHandler::new(
            signal.clone(),
            shutdown_notify,
            queue_failure.clone(),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            SubscriptionState::new(OKX_WS_TOPIC_DELIMITER),
        );

        queue_failure.fail(
            OKXWebSocketQueue::Output,
            OKXWebSocketQueueFailureKind::Full,
        );

        assert!(signal.load(Ordering::Acquire));
        let error = handler
            .send_trading_command_once(r#"{"op":"order"}"#.to_string(), None, 0)
            .await
            .expect_err("terminal latch must reject one-shot transport admission");
        assert!(error.to_string().contains("queue failure rejects"));
        assert!(handler.next().await.is_none());
        assert!(queue_failure.try_acquire_trading_admission().is_none());
    }

    #[rstest]
    #[case("Connection reset by peer", true)]
    #[case("send timeout after 30s", true)]
    #[case("Connection closed unexpectedly", true)]
    #[case("Broken pipe", true)]
    #[case("Network unreachable", true)]
    #[case("No active WebSocket client connection", false)]
    #[case("network protocol upgrade required", false)]
    #[case("invalid frame format", false)]
    fn test_should_retry_client_error(#[case] msg: &str, #[case] expected: bool) {
        let err = OKXWsError::ClientError(msg.to_string());
        assert_eq!(should_retry_okx_error(&err), expected);
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct ParseArrayItem {
        value: i64,
    }

    #[rstest]
    fn critical_array_rejects_whole_batch_when_one_item_fails() {
        let data = json!([
            {"value": 1},
            {"value": "not a number"},
            {"value": 3},
        ]);

        let error = parse_critical_array::<ParseArrayItem>(data, "test").unwrap_err();
        assert!(error.to_string().contains("index 1"));
    }

    #[rstest]
    fn critical_array_rejects_non_array_payload() {
        let data = json!({"not": "an array"});
        assert!(parse_critical_array::<ParseArrayItem>(data, "test").is_err());
    }

    #[rstest]
    fn best_effort_array_keeps_valid_public_items() {
        let data = json!([{"value": 1}, {"value": "bad"}, {"value": 3}]);
        let parsed = parse_best_effort_array::<ParseArrayItem>(data, "test").unwrap();
        assert_eq!(
            parsed,
            vec![ParseArrayItem { value: 1 }, ParseArrayItem { value: 3 }],
        );
    }

    #[rstest]
    fn test_route_instruments_keeps_valid_items_when_one_item_fails() {
        let handler = create_handler();
        let mut frame: Value =
            serde_json::from_str(&load_test_json("ws_instruments.json")).expect("valid fixture");
        let data = frame
            .get_mut("data")
            .and_then(Value::as_array_mut)
            .expect("data array");
        let mut invalid_item = data[0].clone();
        invalid_item["tickSz"] = json!(7);
        data.insert(0, invalid_item);

        let arg: OKXWebSocketArg = serde_json::from_value(frame["arg"].clone()).expect("valid arg");
        let msg = handler
            .route_data_message(arg, frame["data"].clone())
            .expect("route succeeds")
            .expect("instruments message");

        match msg {
            OKXWsMessage::Instruments(instruments) => {
                assert_eq!(instruments.len(), 1);
                assert_eq!(instruments[0].inst_id.as_str(), "BTC-USDT-SWAP");
            }
            other => panic!("Expected Instruments, was {other:?}"),
        }
    }

    #[rstest]
    fn test_route_instruments_prefers_rpi_over_legacy_alias() {
        let handler = create_handler();
        let mut frame: Value =
            serde_json::from_str(&load_test_json("ws_instruments.json")).expect("valid fixture");
        let instrument = &mut frame["data"][0];
        instrument["rpi"] = json!("2");
        instrument["elp"] = json!("1");

        let arg: OKXWebSocketArg = serde_json::from_value(frame["arg"].clone()).expect("valid arg");
        let msg = handler
            .route_data_message(arg, frame["data"].clone())
            .expect("route succeeds")
            .expect("instruments message");

        match msg {
            OKXWsMessage::Instruments(instruments) => {
                assert_eq!(instruments.len(), 1);
                assert_eq!(instruments[0].rpi, Some(OKXRpiPermission::Permitted));
            }
            other => panic!("Expected Instruments, was {other:?}"),
        }
    }
}
