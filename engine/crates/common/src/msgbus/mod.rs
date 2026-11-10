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

//! In-memory message bus for intra-process communication.
//!
//! # Messaging patterns
//!
//! - **Point-to-point**: Send messages to named endpoints via `send_*` functions.
//! - **Pub/sub**: Publish messages to topics via `publish_*`, subscribers receive
//!   all messages matching their pattern.
//! - **Request/response**: Register correlation IDs for response sequence tracking.
//!
//! # Architecture
//!
//! The bus uses thread-local storage for single-threaded async runtimes. Each
//! thread gets its own `MessageBus` instance, avoiding synchronization overhead.
//!
//! Two routing mechanisms serve different needs:
//!
//! - **Typed routing** (`publish_quote`, `subscribe_quotes`): Zero-cost dispatch
//!   for known types. Handlers receive `&T` directly with no runtime type checking.
//! - **Any-based routing** (`publish_any`, `subscribe_any`): Flexible dispatch for
//!   custom types and Python interop. Handlers receive `&dyn Any`.
//!
//! See [`core`] module documentation for design decisions and performance details.

pub mod config;
pub mod core;
pub mod matching;
pub mod message;
pub mod mstr;
pub mod stubs;
pub mod switchboard;
pub mod typed_endpoints;
pub mod typed_handler;
pub mod typed_router;

pub(crate) mod external;

mod api;
mod backing;

use std::{
    any::Any,
    cell::{Cell, RefCell},
    marker::PhantomData,
    rc::Rc,
};

use nautilus_core::UUID4;
#[cfg(feature = "defi")]
use nautilus_model::defi::{Block, Pool, PoolFeeCollect, PoolFlash, PoolLiquidityUpdate, PoolSwap};
use nautilus_model::{
    data::{
        Bar, FundingRateUpdate, GreeksData, IndexPriceUpdate, MarkPriceUpdate, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick,
        option_chain::{OptionChainSlice, OptionGreeks},
    },
    events::{AccountState, OrderEventAny, PortfolioSnapshot, PositionEvent},
    instruments::InstrumentAny,
    orderbook::OrderBook,
};
use smallvec::SmallVec;

#[cfg(feature = "live")]
pub use self::backing::{
    MessageBusExternalIngress, MessageBusExternalReceiver, external_io_from_backing,
};
pub use self::{
    api::*,
    backing::{
        MessageBusBacking, MessageBusBackingFactory, MessageBusExternalEgress,
        external_egress_from_backing,
    },
    config::MessageBusConfig,
    core::{MessageBus, Subscription},
    message::{BusMessage, BusPayloadCategory, BusPayloadType},
    mstr::{Endpoint, MStr, Pattern, Topic},
    switchboard::MessagingSwitchboard,
    typed_endpoints::{EndpointMap, IntoEndpointMap},
    typed_handler::{
        CallbackHandler, Handler, IntoHandler, ShareableMessageHandler, TypedHandler,
        TypedIntoHandler,
    },
    typed_router::{TopicRouter, TypedSubscription},
};
use crate::timer::TimeEvent;

/// Inline capacity for handler buffers before heap allocation.
pub(super) const HANDLER_BUFFER_CAP: usize = 64;

// MessageBus is designed for single-threaded use within each async runtime.
// Thread-local storage ensures each thread gets its own instance, eliminating
// the need for unsafe Send/Sync implementations.
//
// Handler buffers provide zero-allocation publish on hot paths.
// Each buffer stores up to 64 handlers inline before spilling to heap.
// Publish functions use move-out/move-back to avoid holding RefCell borrows
// during handler calls (enabling re-entrant publishes).
thread_local! {
    pub(super) static MESSAGE_BUS: RefCell<Option<Rc<RefCell<MessageBus>>>> = const { RefCell::new(None) };
    pub(super) static HAS_EXTERNAL_EGRESS: Cell<bool> = const { Cell::new(false) };
    pub(super) static SUPPRESS_EXTERNAL_DEPTH: Cell<u32> = const { Cell::new(0) };

    pub(super) static ANY_HANDLERS: RefCell<SmallVec<[ShareableMessageHandler; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    pub(super) static DELTAS_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDeltas>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static DEPTH10_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDepth10>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static BOOK_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBook>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static QUOTE_HANDLERS: RefCell<SmallVec<[TypedHandler<QuoteTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static TRADE_HANDLERS: RefCell<SmallVec<[TypedHandler<TradeTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static BAR_HANDLERS: RefCell<SmallVec<[TypedHandler<Bar>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static MARK_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<MarkPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static INDEX_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<IndexPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static FUNDING_RATE_HANDLERS: RefCell<SmallVec<[TypedHandler<FundingRateUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static GREEKS_HANDLERS: RefCell<SmallVec<[TypedHandler<GreeksData>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static OPTION_GREEKS_HANDLERS: RefCell<SmallVec<[TypedHandler<OptionGreeks>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static OPTION_CHAIN_HANDLERS: RefCell<SmallVec<[TypedHandler<OptionChainSlice>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static ACCOUNT_STATE_HANDLERS: RefCell<SmallVec<[TypedHandler<AccountState>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static PORTFOLIO_SNAPSHOT_HANDLERS: RefCell<SmallVec<[TypedHandler<PortfolioSnapshot>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static ORDER_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderEventAny>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static POSITION_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<PositionEvent>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static INSTRUMENT_HANDLERS: RefCell<SmallVec<[TypedHandler<InstrumentAny>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    #[cfg(feature = "defi")]
    pub(super) static DEFI_BLOCK_HANDLERS: RefCell<SmallVec<[TypedHandler<Block>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_POOL_HANDLERS: RefCell<SmallVec<[TypedHandler<Pool>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_SWAP_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolSwap>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_LIQUIDITY_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolLiquidityUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_COLLECT_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFeeCollect>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_FLASH_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFlash>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
}

/// Guard that prevents republished external messages from being forwarded again.
#[derive(Debug)]
pub struct SuppressExternalGuard;

impl SuppressExternalGuard {
    #[must_use]
    pub fn new() -> Self {
        SUPPRESS_EXTERNAL_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Default for SuppressExternalGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SuppressExternalGuard {
    fn drop(&mut self) {
        SUPPRESS_EXTERNAL_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Sets the thread-local message bus, replacing any existing one.
pub fn set_message_bus(msgbus: Rc<RefCell<MessageBus>>) {
    HAS_EXTERNAL_EGRESS.with(|flag| flag.set(msgbus.borrow().has_external_egress()));
    MESSAGE_BUS.with(|bus| {
        *bus.borrow_mut() = Some(msgbus);
    });
}

/// Gets the thread-local message bus.
///
/// If no message bus has been set for this thread, a default one is created and initialized.
pub fn get_message_bus() -> Rc<RefCell<MessageBus>> {
    MESSAGE_BUS.with(|bus| {
        let mut slot = bus.borrow_mut();
        let rc = slot.get_or_insert_with(|| Rc::new(RefCell::new(MessageBus::default())));
        rc.clone()
    })
}

/// Returns the thread-local message bus if one has been set for this thread.
pub fn try_get_message_bus() -> Option<Rc<RefCell<MessageBus>>> {
    MESSAGE_BUS.with(|bus| bus.borrow().clone())
}

/// Observes dispatched bus traffic for the durable event store.
///
/// The bus invokes the registered tap (when present) before each publish, send, or
/// correlation response fanout, so subscribers cannot observe a message that has not
/// yet been handed to the tap. The tap callback runs on the engine thread and must be
/// cheap; it must not re-enter the bus (the bus is single-threaded and the call site
/// holds no live borrow of the bus, so any re-entrant publish would deadlock through
/// downstream `RefCell::borrow_mut` calls inside the registered tap).
pub trait BusTap: 'static {
    /// Invoked before a publish fanout dispatches to subscribers on `topic`.
    /// Returns `false` to reject the dispatch before any handler observes it.
    fn on_publish(&self, topic: MStr<Topic>, message: &dyn Any) -> bool;

    /// Invoked before a send dispatch reaches the endpoint handler.
    /// Returns `false` to reject the dispatch before the endpoint observes it.
    fn on_send(&self, endpoint: MStr<Endpoint>, message: &dyn Any) -> bool;

    /// Invoked before a correlation response dispatch reaches the response handler.
    fn on_response(&self, _correlation_id: &UUID4, _message: &dyn Any) -> bool {
        true
    }
}

/// Opaque ownership token for an exclusively installed bus tap.
///
/// Only the matching token may remove its tap. Tokens cannot cross threads because the message
/// bus and tap slots are thread-local.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BusTapLease {
    generation: u64,
    thread_bound: PhantomData<Rc<()>>,
}

/// Error returned when an exclusive bus tap cannot be installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BusTapInstallError {
    /// A legacy or exclusively owned tap is already installed.
    #[error("a bus tap is already installed on this thread")]
    Occupied,
    /// The monotonically increasing lease generation is exhausted.
    #[error("bus tap lease generation exhausted")]
    GenerationExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BusTapOwner {
    Legacy,
    Exclusive(BusTapLease),
}

struct InstalledBusTap {
    tap: Rc<dyn BusTap>,
    owner: BusTapOwner,
}

thread_local! {
    static BUS_TAP: RefCell<Option<InstalledBusTap>> = const { RefCell::new(None) };
    static NEXT_BUS_TAP_GENERATION: Cell<u64> = const { Cell::new(0) };
}

/// Registers `tap` as the thread-local bus tap, replacing any previously installed tap.
///
/// Legacy taps may replace other legacy taps, but cannot replace an exclusively owned tap.
/// Returns `false` without changing the slot when an exclusive tap is installed.
pub fn set_bus_tap(tap: Rc<dyn BusTap>) -> bool {
    BUS_TAP.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|installed| matches!(installed.owner, BusTapOwner::Exclusive(_)))
        {
            return false;
        }

        *slot = Some(InstalledBusTap {
            tap,
            owner: BusTapOwner::Legacy,
        });
        true
    })
}

/// Installs an exclusively owned thread-local bus tap.
///
/// Unlike [`set_bus_tap`], this never replaces an existing tap. The returned lease must be
/// supplied to [`clear_bus_tap_with_lease`] to remove the tap.
///
/// # Errors
///
/// Returns [`BusTapInstallError::Occupied`] when any tap is already installed and
/// [`BusTapInstallError::GenerationExhausted`] before a generation could be reused.
pub fn set_bus_tap_exclusive(tap: Rc<dyn BusTap>) -> Result<BusTapLease, BusTapInstallError> {
    BUS_TAP.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return Err(BusTapInstallError::Occupied);
        }

        let generation = NEXT_BUS_TAP_GENERATION.with(|next| {
            let generation = next
                .get()
                .checked_add(1)
                .ok_or(BusTapInstallError::GenerationExhausted)?;
            next.set(generation);
            Ok(generation)
        })?;
        let lease = BusTapLease {
            generation,
            thread_bound: PhantomData,
        };
        *slot = Some(InstalledBusTap {
            tap,
            owner: BusTapOwner::Exclusive(lease),
        });
        Ok(lease)
    })
}

/// Clears the registered bus tap on this thread.
///
/// A no-op returning `true` when no tap is installed. Returns `false` without changing the slot
/// when the installed tap is exclusively owned.
pub fn clear_bus_tap() -> bool {
    BUS_TAP.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|installed| matches!(installed.owner, BusTapOwner::Exclusive(_)))
        {
            return false;
        }

        *slot = None;
        true
    })
}

/// Clears an exclusively owned tap when `lease` matches its owner token.
///
/// Returns `false` without changing the slot for an empty slot, a legacy tap, or a stale/wrong
/// lease.
pub fn clear_bus_tap_with_lease(lease: BusTapLease) -> bool {
    BUS_TAP.with(|slot| {
        let mut slot = slot.borrow_mut();
        let matches = slot
            .as_ref()
            .is_some_and(|installed| installed.owner == BusTapOwner::Exclusive(lease));
        if matches {
            *slot = None;
        }
        matches
    })
}

/// Returns whether a bus tap is currently installed on this thread.
#[must_use]
pub fn is_bus_tap_installed() -> bool {
    BUS_TAP.with(|slot| slot.borrow().is_some())
}

/// Returns whether the currently installed bus tap is exclusively owned.
///
/// Legacy observation taps deliberately return `false`: callers should only treat an
/// exclusive tap as an authoritative admission-control topology component.
#[must_use]
pub fn is_exclusive_bus_tap_installed() -> bool {
    BUS_TAP.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|installed| matches!(installed.owner, BusTapOwner::Exclusive(_)))
    })
}

#[inline]
pub(super) fn dispatch_tap_publish(topic: MStr<Topic>, message: &dyn Any) -> bool {
    // Clone the Rc so the cell borrow is released before the tap runs. The tap is
    // single-threaded with the bus; a re-entrant `set_bus_tap` during dispatch would
    // otherwise panic on RefCell.
    let tap = BUS_TAP.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|installed| Rc::clone(&installed.tap))
    });
    tap.is_none_or(|tap| tap.on_publish(topic, message))
}

#[inline]
pub(super) fn dispatch_tap_send(endpoint: MStr<Endpoint>, message: &dyn Any) -> bool {
    let tap = BUS_TAP.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|installed| Rc::clone(&installed.tap))
    });
    tap.is_none_or(|tap| tap.on_send(endpoint, message))
}

#[inline]
pub(super) fn dispatch_tap_response(correlation_id: &UUID4, message: &dyn Any) -> bool {
    let tap = BUS_TAP.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|installed| Rc::clone(&installed.tap))
    });
    tap.is_none_or(|tap| tap.on_response(correlation_id, message))
}

#[inline]
pub(crate) fn dispatch_tap_time_event(event: &TimeEvent) -> bool {
    dispatch_tap_publish(MessagingSwitchboard::time_event_topic(), event)
}

#[cfg(test)]
mod lease_tests {
    use std::{any::Any, rc::Rc, thread};

    use rstest::rstest;

    use super::{
        BusTap, BusTapInstallError, NEXT_BUS_TAP_GENERATION, clear_bus_tap,
        clear_bus_tap_with_lease, is_bus_tap_installed, is_exclusive_bus_tap_installed,
        set_bus_tap, set_bus_tap_exclusive,
    };
    use crate::msgbus::mstr::{Endpoint, MStr, Topic};

    struct NoopTap;

    impl BusTap for NoopTap {
        fn on_publish(&self, _topic: MStr<Topic>, _message: &dyn Any) -> bool {
            true
        }

        fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) -> bool {
            true
        }
    }

    #[rstest]
    fn exclusive_presence_distinguishes_empty_legacy_and_exclusive_slots() {
        assert!(clear_bus_tap());
        assert!(!is_exclusive_bus_tap_installed());

        assert!(set_bus_tap(Rc::new(NoopTap)));
        assert!(is_bus_tap_installed());
        assert!(!is_exclusive_bus_tap_installed());
        assert!(clear_bus_tap());

        let lease = set_bus_tap_exclusive(Rc::new(NoopTap)).expect("install exclusive tap");
        assert!(is_bus_tap_installed());
        assert!(is_exclusive_bus_tap_installed());
        assert!(clear_bus_tap_with_lease(lease));
        assert!(!is_exclusive_bus_tap_installed());
    }

    #[rstest]
    fn exclusive_generation_exhaustion_never_reuses_a_token() {
        let result = thread::spawn(|| {
            assert!(clear_bus_tap());
            NEXT_BUS_TAP_GENERATION.with(|next| {
                next.set(u64::MAX);
            });
            let error = set_bus_tap_exclusive(Rc::new(NoopTap))
                .expect_err("an exhausted generation must reject installation");
            assert!(!is_bus_tap_installed());
            error
        })
        .join()
        .expect("generation exhaustion thread panicked");

        assert_eq!(result, BusTapInstallError::GenerationExhausted);
    }
}
