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

//! Bounded cumulative execution state retained across OKX reconnect replays.

use std::{error::Error, fmt::Display};

use ahash::{AHashMap, AHashSet};
use nautilus_model::types::{Money, Quantity};
use ustr::Ustr;

#[cfg(test)]
use super::client::{
    OKXWebSocketQueue, OKXWebSocketQueueFailureGuard, OKXWebSocketQueueFailureKind,
};
use super::messages::{OKXOrderMsg, OKXWsMessage};
use crate::http::models::OKXSpreadOrder;

/// Maximum distinct venue order IDs retained by one production WebSocket consumer.
///
/// Entries are never silently evicted. Once a new order ID cannot be admitted, the caller must
/// latch the WebSocket queue failure domain and stop consuming. This is an entry-count bound, not
/// a process RSS bound.
pub const OKX_EXECUTION_REPLAY_CACHE_CAPACITY: usize = 65_536;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ReplayTotals {
    fee: Option<Money>,
    filled_qty: Option<Quantity>,
}

/// A terminal capacity error from the OKX execution replay cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplayCacheCapacityError {
    capacity: usize,
    len: usize,
    requested_new: usize,
}

impl ReplayCacheCapacityError {
    #[cfg(test)]
    pub(crate) const fn capacity(self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(crate) const fn len(self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) const fn requested_new(self) -> usize {
        self.requested_new
    }
}

impl Display for ReplayCacheCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OKX execution replay cache capacity exhausted: len={}, requested_new={}, capacity={}",
            self.len, self.requested_new, self.capacity,
        )
    }
}

impl Error for ReplayCacheCapacityError {}

/// Latches the shared OKX/runner fail-stop domain for a production replay-cache overflow.
#[cfg(test)]
pub(crate) fn halt_on_replay_cache_capacity(
    queue_failure: &OKXWebSocketQueueFailureGuard,
    error: ReplayCacheCapacityError,
    consumer: &str,
) {
    log::error!("{error}; halting the OKX {consumer} execution consumer");
    queue_failure.fail(
        OKXWebSocketQueue::Output,
        OKXWebSocketQueueFailureKind::Full,
    );
}

/// Cache operations used by the common execution dispatcher.
///
/// The legacy map-backed implementation exists only to preserve the public offline parser and
/// dispatcher API. Production consumers use [`BoundedExecutionReplayCache`].
pub(crate) trait ExecutionReplayCache {
    fn preflight_message(&mut self, message: &OKXWsMessage)
    -> Result<(), ReplayCacheCapacityError>;

    fn previous_fee(&self, venue_order_id: &Ustr) -> Option<Money>;

    fn previous_filled_qty(&self, venue_order_id: &Ustr) -> Option<Quantity>;

    fn update(&mut self, venue_order_id: Ustr, fee: Option<Money>, filled_qty: Option<Quantity>);

    fn remove_explicitly_safe_rejection(&mut self, venue_order_id: &Ustr);
}

/// Fixed-capacity replay state used by native and Python production consumers.
#[derive(Debug)]
pub(crate) struct BoundedExecutionReplayCache {
    entries: AHashMap<Ustr, ReplayTotals>,
    capacity: usize,
}

impl Default for BoundedExecutionReplayCache {
    fn default() -> Self {
        Self::new(OKX_EXECUTION_REPLAY_CACHE_CAPACITY)
    }
}

impl BoundedExecutionReplayCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: AHashMap::with_capacity(capacity),
            capacity,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn preflight_order_ids<I>(&mut self, order_ids: I) -> Result<(), ReplayCacheCapacityError>
    where
        I: IntoIterator<Item = Ustr>,
    {
        let mut new_ids = AHashSet::new();
        for order_id in order_ids {
            if !self.entries.contains_key(&order_id) {
                new_ids.insert(order_id);
            }
        }

        if self.entries.len().saturating_add(new_ids.len()) > self.capacity {
            return Err(ReplayCacheCapacityError {
                capacity: self.capacity,
                len: self.entries.len(),
                requested_new: new_ids.len(),
            });
        }

        for order_id in new_ids {
            self.entries.insert(order_id, ReplayTotals::default());
        }
        Ok(())
    }

    pub(crate) fn preflight_orders(
        &mut self,
        messages: &[OKXOrderMsg],
    ) -> Result<(), ReplayCacheCapacityError> {
        self.preflight_order_ids(messages.iter().map(|message| message.ord_id))
    }

    pub(crate) fn preflight_spread_orders(
        &mut self,
        messages: &[OKXSpreadOrder],
    ) -> Result<(), ReplayCacheCapacityError> {
        self.preflight_order_ids(messages.iter().map(|message| message.ord_id))
    }

    pub(crate) fn previous_fee(&self, venue_order_id: &Ustr) -> Option<Money> {
        self.entries.get(venue_order_id).and_then(|entry| entry.fee)
    }

    pub(crate) fn previous_filled_qty(&self, venue_order_id: &Ustr) -> Option<Quantity> {
        self.entries
            .get(venue_order_id)
            .and_then(|entry| entry.filled_qty)
    }

    pub(crate) fn update(
        &mut self,
        venue_order_id: Ustr,
        fee: Option<Money>,
        filled_qty: Option<Quantity>,
    ) {
        let entry = self
            .entries
            .get_mut(&venue_order_id)
            .expect("preflight must reserve every order ID before cache update");
        if let Some(fee) = fee {
            entry.fee = Some(fee);
        }
        if let Some(filled_qty) = filled_qty {
            entry.filled_qty = Some(filled_qty);
        }
    }

    pub(crate) fn remove_explicitly_safe_rejection(&mut self, venue_order_id: &Ustr) {
        self.entries.remove(venue_order_id);
    }
}

impl ExecutionReplayCache for BoundedExecutionReplayCache {
    fn preflight_message(
        &mut self,
        message: &OKXWsMessage,
    ) -> Result<(), ReplayCacheCapacityError> {
        match message {
            OKXWsMessage::Orders(messages) => self.preflight_orders(messages),
            OKXWsMessage::SpreadOrders(messages) => self.preflight_spread_orders(messages),
            _ => Ok(()),
        }
    }

    fn previous_fee(&self, venue_order_id: &Ustr) -> Option<Money> {
        self.previous_fee(venue_order_id)
    }

    fn previous_filled_qty(&self, venue_order_id: &Ustr) -> Option<Quantity> {
        self.previous_filled_qty(venue_order_id)
    }

    fn update(&mut self, venue_order_id: Ustr, fee: Option<Money>, filled_qty: Option<Quantity>) {
        self.update(venue_order_id, fee, filled_qty);
    }

    fn remove_explicitly_safe_rejection(&mut self, venue_order_id: &Ustr) {
        self.remove_explicitly_safe_rejection(venue_order_id);
    }
}

/// Adapter for the existing unbounded public/offline API.
pub(crate) struct LegacyExecutionReplayCache<'a> {
    fee_cache: &'a mut AHashMap<Ustr, Money>,
    filled_qty_cache: &'a mut AHashMap<Ustr, Quantity>,
}

impl<'a> LegacyExecutionReplayCache<'a> {
    pub(crate) fn new(
        fee_cache: &'a mut AHashMap<Ustr, Money>,
        filled_qty_cache: &'a mut AHashMap<Ustr, Quantity>,
    ) -> Self {
        Self {
            fee_cache,
            filled_qty_cache,
        }
    }
}

impl ExecutionReplayCache for LegacyExecutionReplayCache<'_> {
    fn preflight_message(
        &mut self,
        _message: &OKXWsMessage,
    ) -> Result<(), ReplayCacheCapacityError> {
        Ok(())
    }

    fn previous_fee(&self, venue_order_id: &Ustr) -> Option<Money> {
        self.fee_cache.get(venue_order_id).copied()
    }

    fn previous_filled_qty(&self, venue_order_id: &Ustr) -> Option<Quantity> {
        self.filled_qty_cache.get(venue_order_id).copied()
    }

    fn update(&mut self, venue_order_id: Ustr, fee: Option<Money>, filled_qty: Option<Quantity>) {
        if let Some(fee) = fee {
            self.fee_cache.insert(venue_order_id, fee);
        }
        if let Some(filled_qty) = filled_qty {
            self.filled_qty_cache.insert(venue_order_id, filled_qty);
        }
    }

    fn remove_explicitly_safe_rejection(&mut self, venue_order_id: &Ustr) {
        self.fee_cache.remove(venue_order_id);
        self.filled_qty_cache.remove(venue_order_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn bounded_cache_rejects_over_capacity_without_partial_admission() {
        let mut cache = BoundedExecutionReplayCache::new(2);
        cache
            .preflight_order_ids([Ustr::from("1"), Ustr::from("2")])
            .unwrap();

        let error = cache
            .preflight_order_ids([Ustr::from("2"), Ustr::from("3"), Ustr::from("4")])
            .unwrap_err();

        assert_eq!(error.capacity(), 2);
        assert_eq!(error.len(), 2);
        assert_eq!(error.requested_new(), 2);
        assert_eq!(cache.len(), 2);
        assert!(cache.entries.contains_key(&Ustr::from("1")));
        assert!(cache.entries.contains_key(&Ustr::from("2")));
        assert!(!cache.entries.contains_key(&Ustr::from("3")));
    }

    #[rstest]
    fn duplicate_terminal_and_reconnect_replay_reuse_the_same_entry() {
        let mut cache = BoundedExecutionReplayCache::new(1);
        let order_id = Ustr::from("terminal-order");
        cache.preflight_order_ids([order_id]).unwrap();
        cache.update(
            order_id,
            Some(Money::from("-0.25 USDT")),
            Some(Quantity::from("2.5")),
        );

        // Duplicate terminal message before reconnect.
        cache.preflight_order_ids([order_id]).unwrap();
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.previous_filled_qty(&order_id),
            Some(Quantity::from("2.5"))
        );

        // Reconnect does not clear the replay ledger; a replay has the same cumulative baseline.
        cache.preflight_order_ids([order_id]).unwrap();
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.previous_fee(&order_id),
            Some(Money::from("-0.25 USDT"))
        );
    }

    #[rstest]
    fn removing_an_explicitly_safe_entry_releases_capacity() {
        let mut cache = BoundedExecutionReplayCache::new(1);
        let rejected_id = Ustr::from("post-only-rejected");
        cache.preflight_order_ids([rejected_id]).unwrap();
        cache.remove_explicitly_safe_rejection(&rejected_id);

        // A reconnect replay reserves the ID during atomic message preflight. The dispatcher's
        // bounded rejection marker recognizes it and releases the same proven zero-fill slot.
        cache.preflight_order_ids([rejected_id]).unwrap();
        cache.remove_explicitly_safe_rejection(&rejected_id);

        cache
            .preflight_order_ids([Ustr::from("replacement")])
            .unwrap();
        assert_eq!(cache.len(), 1);
    }

    #[rstest]
    fn capacity_error_latches_the_shared_queue_failure_guard() {
        let signal = Arc::new(AtomicBool::new(false));
        let guard = OKXWebSocketQueueFailureGuard::new(
            Arc::clone(&signal),
            Arc::new(tokio::sync::Notify::new()),
        );
        let mut cache = BoundedExecutionReplayCache::new(0);
        let error = cache
            .preflight_order_ids([Ustr::from("first")])
            .unwrap_err();

        halt_on_replay_cache_capacity(&guard, error, "test");

        assert!(signal.load(Ordering::Acquire));
        assert!(guard.is_failed());
        assert_eq!(
            guard.failure(),
            Some((
                OKXWebSocketQueue::Output,
                OKXWebSocketQueueFailureKind::Full,
            )),
        );
    }
}
