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

#![expect(
    clippy::too_many_arguments,
    reason = "rstest fixtures define broad test setup signatures"
)]

use std::{
    any::Any,
    cell::{Cell, RefCell},
    rc::Rc,
    str::FromStr,
    sync::Arc,
};

use ahash::AHashMap;
use nautilus_common::{
    cache::Cache,
    clock::{Clock, TestClock},
    messages::{
        execution::{
            BatchModifyOrders, CancelOrder, ModifyOrder, SubmitOrder, SubmitOrderList,
            TradingCommand,
        },
        system::{
            RiskMaxNotional, RiskStateChangeReason, RiskStateChanged, trading::TradingStateChanged,
        },
    },
    msgbus::{
        self, BusTap, BusTapLease, Endpoint, MStr, MessagingSwitchboard, Topic, TypedHandler,
        stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
    },
    runner::{
        SyncTradingCommandSender, drain_trading_cmd_queue, replace_exec_cmd_sender,
        trading_cmd_queue_is_empty,
    },
    throttler::RateLimit,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::{ExecutionEngine, config::ExecutionEngineConfig};
use nautilus_model::{
    accounts::{AccountAny, BettingAccount, CashAccount, MarginAccount, stubs::cash_account},
    data::{QuoteTick, TradeTick, stubs::quote_audusd},
    enums::{
        AccountType, AggressorSide, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSide, TimeInForce, TradingState, TrailingOffsetType, TriggerType,
    },
    events::{
        AccountState, OrderAccepted, OrderDeniedReason, OrderEventAny, OrderEventType, OrderFilled,
        OrderSubmitted, PositionEvent, PositionOpened,
        account::stubs::cash_account_state_million_usd,
        order::spec::{OrderAcceptedSpec, OrderFilledSpec, OrderSubmittedSpec},
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        Symbol, TradeId, TraderId, VenueOrderId,
        stubs::{
            account_id, client_id_binance, client_order_id, strategy_id_ema_cross, trader_id,
            uuid4, venue_order_id,
        },
    },
    instruments::{
        Commodity, CryptoPerpetual, CurrencyPair, FuturesSpread, Instrument, InstrumentAny,
        OptionSpread,
        stubs::{
            audusd_sim, betting, commodity_gold, crypto_perpetual_ethusdt, futures_spread_es,
            gbpusd_sim, option_spread, xbtusd_bitmex,
        },
    },
    orders::{Order, OrderAny, OrderList, OrderTestBuilder},
    position::Position,
    types::{AccountBalance, Currency, Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use nautilus_portfolio::Portfolio;
use rstest::{fixture, rstest};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use rust_decimal_macros::dec;
use ustr::Ustr;

// Helper that registers message collectors for ExecEngine.process events and
// returns the shared handler so callers can later retrieve the collected
// OrderEventAny messages via `get_process_order_event_handler_messages`.
fn register_process_handler() -> TypedIntoMessageSavingHandler<OrderEventAny> {
    let (handler, saving_handler) = get_typed_into_message_saving_handler::<OrderEventAny>(Some(
        Ustr::from("ExecEngine.process"),
    ));
    msgbus::register_order_event_endpoint(MessagingSwitchboard::exec_engine_process(), handler);
    saving_handler
}

fn consume_fixture<T>(_: T) {}

#[rstest]
#[case::cash_sell_without_position("2 ETH", "0 ETH", "2 ETH", OrderSide::Sell, "1.000", true)]
#[case::cash_sell_exceeds_inventory("2 ETH", "0 ETH", "2 ETH", OrderSide::Sell, "3.000", false)]
#[case::cash_buy_is_not_reduction("2 ETH", "0 ETH", "2 ETH", OrderSide::Buy, "1.000", false)]
#[case::external_locks_are_preserved(
    "2 ETH",
    "1.5 ETH",
    "0.5 ETH",
    OrderSide::Sell,
    "0.600",
    false
)]
#[case::free_inventory_can_reduce("2 ETH", "1.5 ETH", "0.5 ETH", OrderSide::Sell, "0.400", true)]
fn test_reducing_cash_inventory_without_derivative_position(
    #[case] total: &str,
    #[case] locked: &str,
    #[case] free: &str,
    #[case] side: OrderSide,
    #[case] quantity: &str,
    #[case] accepted: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    let instrument_eth_usdt = InstrumentAny::CurrencyPair(
        nautilus_model::instruments::stubs::currency_pair_ethusdt(),
    );
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    let account = AccountState::new(
        AccountId::from("BINANCE-001"),
        AccountType::Cash,
        vec![
            AccountBalance::new(
                Money::from("10000 USDT"),
                Money::from("0 USDT"),
                Money::from("10000 USDT"),
            ),
            AccountBalance::new(Money::from(total), Money::from(locked), Money::from(free)),
        ],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        None,
    );
    simple_cache
        .add_account(AccountAny::Cash(CashAccount::new(account, true, false)))
        .unwrap();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .side(side)
        .quantity(Quantity::from(quantity))
        .price(Price::from("3000.00"))
        .build();
    simple_cache
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    let mut risk = get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk.set_trading_state(TradingState::Reducing);
    let submit = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk.clock().borrow().timestamp_ns(),
        None,
    );
    risk.execute(TradingCommand::SubmitOrder(submit));
    assert_eq!(
        get_execute_order_event_handler_messages(&execute_order_event_handler).len(),
        usize::from(accepted),
        "{:?}", get_process_order_event_handler_messages(&process_order_event_handler)
    );
    assert_eq!(
        get_process_order_event_handler_messages(&process_order_event_handler).len(),
        usize::from(!accepted)
    );
}

#[rstest]
fn test_deny_order_on_price_precision_exceeded(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
) {
    // Register collector for denied events
    let process_handler = register_process_handler();

    // Build a RiskEngine with default (non-bypassed) settings and an account with ample balance
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    // Add large cash account so balance checks pass (focus is price precision)
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
        )))
        .unwrap();

    // Add a last quote so notional calculation can proceed if needed
    cache.add_quote(quote_audusd()).unwrap();

    let mut risk_engine = get_risk_engine(Some(Rc::new(RefCell::new(cache))), None, None, false);

    // AUD/USD price precision is 5 - create a Limit order with 6-dp price (invalid)
    let bad_price = Price::from("1.000001"); // precision 6
    assert!(bad_price.precision > instrument_audusd.price_precision());

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(bad_price)
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // Expect an OrderDenied to be emitted
    let saved_events = get_process_order_event_handler_messages(&process_handler);
    assert_eq!(saved_events.len(), 1);
    matches!(saved_events[0], OrderEventAny::Denied(_));
}

#[rstest]
fn test_deny_order_exceeding_max_notional(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
) {
    let process_handler = register_process_handler();

    // Prepare small max_notional setting (1 USD)
    let mut max_notional_map = AHashMap::new();
    max_notional_map.insert(instrument_audusd.id(), Decimal::from_i64(1).unwrap());

    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
        )))
        .unwrap();
    cache.add_quote(quote_audusd()).unwrap();

    let risk_config = RiskEngineConfig {
        debug: true,
        bypass: false,
        max_order_submit: RateLimit::new(10, 1000),
        max_order_modify: RateLimit::new(5, 1000),
        max_notional_per_order: AHashMap::new(),
        max_market_data_age_ns: Some(5_000_000_000),
        market_order_price_bound_bps: Some(1),
    };

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(risk_config),
        None,
        false,
    );

    risk_engine.set_max_notional_per_order(instrument_audusd.id(), Decimal::from_i64(1).unwrap());

    // Build an order with notional ~100 USD (price 1, qty 100) > max 1 USD
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from("1"))
        .quantity(Quantity::from("100"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_events = get_process_order_event_handler_messages(&process_handler);
    assert_eq!(saved_events.len(), 1);
    matches!(saved_events[0], OrderEventAny::Denied(_));
}

#[rstest]
fn test_submit_market_order_with_no_order_side_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    simple_cache.add_quote(quote_audusd()).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::NoOrderSide)
        .quantity(Quantity::from("100"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].event_type(), OrderEventType::Denied);
    assert_eq!(
        saved[0].message().unwrap(),
        Ustr::from("INVALID_ORDER_SIDE: NO_ORDER_SIDE")
    );
}

#[rstest]
fn test_submit_limit_order_with_no_order_side_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::NoOrderSide)
        .price(Price::from("1.00000"))
        .quantity(Quantity::from("100"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].event_type(), OrderEventType::Denied);
    assert_eq!(
        saved[0].message().unwrap(),
        Ustr::from("INVALID_ORDER_SIDE: NO_ORDER_SIDE")
    );
}

use nautilus_risk::engine::{
    AuthorizedRiskStateChange, RiskEngine, RiskMutationError, RiskMutationOutcome,
    config::RiskEngineConfig,
};

#[derive(Debug)]
struct RejectRiskStateTap;

impl BusTap for RejectRiskStateTap {
    fn on_publish(&self, topic: MStr<Topic>, _message: &dyn Any) -> bool {
        topic != MessagingSwitchboard::risk_state_topic()
    }

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) -> bool {
        true
    }
}

struct BusTapGuard;

impl Drop for BusTapGuard {
    fn drop(&mut self) {
        msgbus::clear_bus_tap();
    }
}

#[derive(Debug)]
struct AllowBusTap;

impl BusTap for AllowBusTap {
    fn on_publish(&self, _topic: MStr<Topic>, _message: &dyn Any) -> bool {
        true
    }

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) -> bool {
        true
    }
}

struct ExclusiveBusTapGuard {
    lease: BusTapLease,
}

impl Drop for ExclusiveBusTapGuard {
    fn drop(&mut self) {
        assert!(msgbus::clear_bus_tap_with_lease(self.lease));
    }
}

fn install_exclusive_allow_tap() -> ExclusiveBusTapGuard {
    let lease =
        msgbus::set_bus_tap_exclusive(Rc::new(AllowBusTap)).expect("install exclusive allow tap");
    ExclusiveBusTapGuard { lease }
}

fn reject_risk_state_admission() -> BusTapGuard {
    msgbus::set_bus_tap(Rc::new(RejectRiskStateTap));
    BusTapGuard
}

#[derive(Debug, Default)]
struct RiskStateTypeObservations {
    authorized: Cell<usize>,
    raw: Cell<usize>,
    snapshots: RefCell<Vec<RiskStateChanged>>,
}

#[derive(Debug)]
struct ObserveRiskStateTypeTap {
    observations: Rc<RiskStateTypeObservations>,
}

impl BusTap for ObserveRiskStateTypeTap {
    fn on_publish(&self, topic: MStr<Topic>, message: &dyn Any) -> bool {
        if topic == MessagingSwitchboard::risk_state_topic() {
            if let Some(authorized) = message.downcast_ref::<AuthorizedRiskStateChange>() {
                self.observations
                    .authorized
                    .set(self.observations.authorized.get() + 1);
                assert_eq!(authorized.event_id(), authorized.snapshot().event_id);
                self.observations
                    .snapshots
                    .borrow_mut()
                    .push(authorized.snapshot().clone());
            }
            if message.is::<RiskStateChanged>() {
                self.observations.raw.set(self.observations.raw.get() + 1);
            }
        }
        true
    }

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) -> bool {
        true
    }
}

fn observe_risk_state_admission_types() -> (Rc<RiskStateTypeObservations>, BusTapGuard) {
    let observations = Rc::new(RiskStateTypeObservations::default());
    msgbus::set_bus_tap(Rc::new(ObserveRiskStateTypeTap {
        observations: Rc::clone(&observations),
    }));
    (observations, BusTapGuard)
}

#[fixture]
fn process_order_event_handler() -> TypedIntoMessageSavingHandler<OrderEventAny> {
    let (handler, saving_handler) = get_typed_into_message_saving_handler::<OrderEventAny>(Some(
        Ustr::from("ExecEngine.process"),
    ));
    msgbus::register_order_event_endpoint(MessagingSwitchboard::exec_engine_process(), handler);
    saving_handler
}

#[fixture]
fn execute_order_event_handler() -> TypedIntoMessageSavingHandler<TradingCommand> {
    let (handler, saving_handler) = get_typed_into_message_saving_handler::<TradingCommand>(Some(
        Ustr::from("ExecEngine.queue_execute"),
    ));
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        handler,
    );
    saving_handler
}

#[fixture]
fn simple_cache() -> Cache {
    Cache::new(None, None)
}

#[fixture]
fn clock() -> TestClock {
    let clock = TestClock::new();
    clock.set_time(UnixNanos::from(1));
    clock
}

#[fixture]
fn max_order_submit() -> RateLimit {
    RateLimit::new(10, 1)
}

#[fixture]
fn max_order_modify() -> RateLimit {
    RateLimit::new(5, 1)
}

#[fixture]
fn max_notional_per_order() -> AHashMap<InstrumentId, Decimal> {
    AHashMap::new()
}

// Market buy order with corresponding fill
#[fixture]
fn market_order_buy(instrument_eth_usdt: InstrumentAny) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1"))
        .build()
}

// Market sell order
#[fixture]
fn market_order_sell(instrument_eth_usdt: InstrumentAny) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1"))
        .build()
}

#[fixture]
#[allow(dead_code)]
fn get_stub_submit_order(
    trader_id: TraderId,
    client_id_binance: ClientId,
    strategy_id_ema_cross: StrategyId,
    instrument_eth_usdt: InstrumentAny,
) -> (OrderAny, SubmitOrder) {
    let order = market_order_buy(instrument_eth_usdt.clone());
    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        UnixNanos::from(10),
        None, // correlation_id
    );
    (order, submit_order)
}

#[fixture]
fn config_fixture(
    max_order_submit: RateLimit,
    max_order_modify: RateLimit,
    max_notional_per_order: AHashMap<InstrumentId, Decimal>,
) -> RiskEngineConfig {
    RiskEngineConfig {
        debug: true,
        bypass: false,
        max_order_submit,
        max_order_modify,
        max_notional_per_order,
        max_market_data_age_ns: Some(5_000_000_000),
        market_order_price_bound_bps: Some(1),
    }
}

#[fixture]
pub fn bitmex_cash_account_state_multi() -> AccountState {
    let btc_account_balance = AccountBalance::new(
        Money::from("10 BTC"),
        Money::from("0 BTC"),
        Money::from("10 BTC"),
    );
    let eth_account_balance = AccountBalance::new(
        Money::from("20 ETH"),
        Money::from("0 ETH"),
        Money::from("20 ETH"),
    );
    AccountState::new(
        AccountId::from("BITMEX-001"),
        AccountType::Cash,
        vec![btc_account_balance, eth_account_balance],
        vec![],
        true,
        uuid4(),
        0.into(),
        0.into(),
        None, // multi cash account
    )
}

fn get_process_order_event_handler_messages(
    event_handler: &TypedIntoMessageSavingHandler<OrderEventAny>,
) -> Vec<OrderEventAny> {
    event_handler.get_messages()
}

fn get_execute_order_event_handler_messages(
    event_handler: &TypedIntoMessageSavingHandler<TradingCommand>,
) -> Vec<TradingCommand> {
    event_handler.get_messages()
}

#[fixture]
fn instrument_eth_usdt(crypto_perpetual_ethusdt: CryptoPerpetual) -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt)
}

#[fixture]
fn instrument_xbtusd_bitmex(xbtusd_bitmex: CryptoPerpetual) -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(xbtusd_bitmex)
}

#[fixture]
fn instrument_audusd(audusd_sim: CurrencyPair) -> InstrumentAny {
    InstrumentAny::CurrencyPair(audusd_sim)
}

#[fixture]
fn instrument_futures_spread(futures_spread_es: FuturesSpread) -> InstrumentAny {
    InstrumentAny::FuturesSpread(futures_spread_es)
}

#[fixture]
fn instrument_option_spread(option_spread: OptionSpread) -> InstrumentAny {
    InstrumentAny::OptionSpread(option_spread)
}

#[fixture]
fn instrument_commodity(commodity_gold: Commodity) -> InstrumentAny {
    InstrumentAny::Commodity(commodity_gold)
}

#[fixture]
pub fn instrument_xbtusd_with_high_size_precision() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
        InstrumentId::from("BTCUSDT.BITMEX"),
        Symbol::from("XBTUSD"),
        Currency::BTC(),
        Currency::USD(),
        Currency::BTC(),
        true,
        1,
        2,
        Price::from("0.5"),
        Quantity::from("0.01"),
        None,
        None,
        None,
        None,
        Some(Money::from("10000000 USD")),
        Some(Money::from("1 USD")),
        Some(Price::from("10000000")),
        Some(Price::from("0.01")),
        Some(dec!(0.01)),
        Some(dec!(0.0035)),
        Some(dec!(-0.00025)),
        Some(dec!(0.00075)),
        None,
        None, // info
        UnixNanos::default(),
        UnixNanos::default(),
    ))
}

// Helpers
fn get_risk_engine(
    cache: Option<Rc<RefCell<Cache>>>,
    config: Option<RiskEngineConfig>,
    clock: Option<Rc<RefCell<TestClock>>>,
    bypass: bool,
) -> RiskEngine {
    let cache = cache.unwrap_or(Rc::new(RefCell::new(Cache::default())));
    let config = config.unwrap_or(RiskEngineConfig {
        debug: true,
        bypass,
        max_order_submit: RateLimit::new(10, 1000),
        max_order_modify: RateLimit::new(5, 1000),
        max_notional_per_order: AHashMap::new(),
        max_market_data_age_ns: Some(5_000_000_000),
        market_order_price_bound_bps: Some(1),
    });
    let clock = clock.unwrap_or_else(|| {
        let clock = TestClock::new();
        clock.set_time(UnixNanos::from(1));
        Rc::new(RefCell::new(clock))
    });
    let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
    RiskEngine::new(config, portfolio, clock, cache)
}

fn cash_account_for_venue(
    instrument: &InstrumentAny,
    total: &str,
    locked: &str,
    free: &str,
    base_currency: Option<Currency>,
) -> CashAccount {
    CashAccount::new(
        AccountState::new(
            AccountId::new(format!("{}-001", instrument.id().venue)),
            AccountType::Cash,
            vec![AccountBalance::new(
                Money::from(total),
                Money::from(locked),
                Money::from(free),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            base_currency,
        ),
        true,
        false,
    )
}

fn execute_submit_for_test(
    risk_engine: &mut RiskEngine,
    order: &OrderAny,
    trader_id: TraderId,
    client_id: ClientId,
    strategy_id: StrategyId,
) {
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id), false)
        .unwrap();
    let submit = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );
    risk_engine.execute(TradingCommand::SubmitOrder(submit));
}

fn assert_denied_without_execution(
    process_handler: &TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_handler: &TypedIntoMessageSavingHandler<TradingCommand>,
    reason_fragment: &str,
) {
    let process_messages = get_process_order_event_handler_messages(process_handler);
    assert_eq!(process_messages.len(), 1);
    assert_eq!(process_messages[0].event_type(), OrderEventType::Denied);
    assert!(
        process_messages[0]
            .message()
            .is_some_and(|message| message.contains(reason_fragment)),
        "unexpected denial reason: {:?}",
        process_messages[0].message()
    );
    assert!(get_execute_order_event_handler_messages(execute_handler).is_empty());
}

fn add_open_position(
    cache: &mut Cache,
    instrument: &InstrumentAny,
    entry_side: OrderSide,
    quantity: Quantity,
    position_id: &str,
) {
    add_open_position_for_account(
        cache,
        instrument,
        entry_side,
        quantity,
        position_id,
        AccountId::from("SIM-001"),
    );
}

fn add_open_position_for_account(
    cache: &mut Cache,
    instrument: &InstrumentAny,
    entry_side: OrderSide,
    quantity: Quantity,
    position_id: &str,
    account_id: AccountId,
) {
    let entry_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id())
        .side(entry_side)
        .quantity(quantity)
        .build();
    let mut fill = order_filled(
        &entry_order,
        instrument,
        None,
        Some(account_id),
        Some(VenueOrderId::from("V-POSITION")),
        None,
        None,
        Some(Price::from("1.00000")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::new(position_id));
    cache
        .add_position(&Position::new(instrument, fill), OmsType::Hedging)
        .unwrap();
}

fn market_valuation_test_config() -> RiskEngineConfig {
    RiskEngineConfig {
        max_market_data_age_ns: Some(1_000_000_000),
        market_order_price_bound_bps: Some(500),
        ..Default::default()
    }
}

#[rstest]
fn test_market_with_configured_max_notional_and_no_price_evidence_denies_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    let config = RiskEngineConfig {
        max_notional_per_order: AHashMap::from([(instrument_audusd.id(), Decimal::from(1_000))]),
        max_market_data_age_ns: Some(1_000_000_000),
        market_order_price_bound_bps: Some(500),
        ..Default::default()
    };
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(config),
        None,
        false,
    );
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000000"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "quote evidence failed",
    );
}

#[rstest]
fn test_market_valuation_controls_default_to_disabled_and_deny_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache.add_quote(quote_audusd()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(RiskEngineConfig::default()),
        None,
        false,
    );
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "market-style valuation is disabled",
    );
}

#[rstest]
#[case::stale_quote(true, 2_000_000_000, "stale")]
#[case::stale_trade(false, 2_000_000_000, "stale")]
#[case::future_quote(true, 0, "in the future")]
fn test_market_with_unacceptable_price_evidence_age_denies_without_execution(
    #[case] use_quote: bool,
    #[case] clock_ns: u64,
    #[case] reason_fragment: &str,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    if use_quote {
        cache.add_quote(quote_audusd()).unwrap();
    } else {
        cache
            .add_trade(TradeTick::new(
                instrument_audusd.id(),
                Price::from("100.0000"),
                Quantity::from("1.00000000"),
                AggressorSide::Buyer,
                TradeId::new("STALE-1"),
                UnixNanos::from(1),
                UnixNanos::from(1),
            ))
            .unwrap();
    }
    let clock = Rc::new(RefCell::new(TestClock::new()));
    clock.borrow_mut().set_time(UnixNanos::from(clock_ns));
    let config = market_valuation_test_config();
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(config),
        Some(clock),
        false,
    );
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        reason_fragment,
    );
}

#[rstest]
fn test_missing_account_denies_limit_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    let mut risk_engine = get_risk_engine(Some(Rc::new(RefCell::new(cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.00000"))
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "account not found",
    );
}

#[rstest]
fn test_missing_cash_free_balance_denies_limit_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    let eur_balance = AccountBalance::new(
        Money::from("1000 EUR"),
        Money::from("0 EUR"),
        Money::from("1000 EUR"),
    );
    let state = AccountState::new(
        AccountId::from("SIM-001"),
        AccountType::Cash,
        vec![eur_balance],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    cache
        .add_account(AccountAny::Cash(CashAccount::new(state, true, false)))
        .unwrap();
    let mut risk_engine = get_risk_engine(Some(Rc::new(RefCell::new(cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.00000"))
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "no free USD balance",
    );
}

#[rstest]
fn test_missing_base_free_balance_denies_cash_sell_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    let usd_balance = AccountBalance::new(
        Money::from("1000000 USD"),
        Money::from("0 USD"),
        Money::from("1000000 USD"),
    );
    let account_state = AccountState::new(
        AccountId::from("SIM-001"),
        AccountType::Cash,
        vec![usd_balance],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    cache
        .add_account(AccountAny::Cash(CashAccount::new(
            account_state,
            true,
            false,
        )))
        .unwrap();
    let mut risk_engine = get_risk_engine(Some(Rc::new(RefCell::new(cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Sell)
        .price(Price::from("1.00000"))
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "no free base-currency balance for AUD",
    );
}

#[rstest]
fn test_missing_margin_collateral_denies_limit_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_eth_usdt.clone()).unwrap();
    let btc_balance = AccountBalance::new(
        Money::from("1 BTC"),
        Money::from("0 BTC"),
        Money::from("1 BTC"),
    );
    let state = AccountState::new(
        AccountId::from("BINANCE-001"),
        AccountType::Margin,
        vec![btc_balance],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    cache
        .add_account(AccountAny::Margin(MarginAccount::new(state, true)))
        .unwrap();
    let mut risk_engine = get_risk_engine(Some(Rc::new(RefCell::new(cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .price(Price::from("3000.00"))
        .quantity(Quantity::from("1.000"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "no free margin collateral balance",
    );
}

#[rstest]
fn test_stop_market_with_trigger_but_no_fresh_tick_denies_without_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    let config = market_valuation_test_config();
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(config),
        None,
        false,
    );
    let order = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .trigger_price(Price::from("1.10000"))
        .quantity(Quantity::from("100"))
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "quote evidence failed",
    );
}

#[rstest]
fn test_mixed_market_order_list_uses_side_specific_adverse_prices(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    cache
        .add_quote(QuoteTick::new(
            instrument_audusd.id(),
            Price::from("1.00000"),
            Price::from("2.00000"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        ))
        .unwrap();
    cache
        .add_trade(TradeTick::new(
            instrument_audusd.id(),
            Price::from("1.00000"),
            Quantity::from("1"),
            AggressorSide::Seller,
            TradeId::new("MIXED-TRADE"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        ))
        .unwrap();
    let mut config = market_valuation_test_config();
    config
        .max_notional_per_order
        .insert(instrument_audusd.id(), Decimal::from_str("150").unwrap());
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(config),
        None,
        false,
    );
    let buy = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("MIXED-BUY"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .build();
    let sell = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("MIXED-SELL"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .build();
    // Insert SELL first: the legacy cross-order price cache reused its bid for the later BUY and
    // would incorrectly admit the list below the configured max notional.
    let orders = [sell, buy];
    for order in &orders {
        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();
    }
    let order_list = OrderList::new(
        OrderListId::new("MIXED-1"),
        instrument_audusd.id(),
        strategy_id_ema_cross,
        orders.iter().map(Order::client_order_id).collect(),
        risk_engine.clock().borrow().timestamp_ns(),
    );
    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders
            .iter()
            .map(|order| order.init_event().clone())
            .collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    let process_messages = get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(!process_messages.is_empty());
    assert!(
        process_messages
            .iter()
            .all(|event| event.event_type() == OrderEventType::Denied)
    );
    assert!(process_messages.iter().any(|event| {
        event
            .message()
            .is_some_and(|message| message.contains("NOTIONAL_EXCEEDS_MAX_PER_ORDER"))
    }));
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[rstest]
fn test_trailing_stop_limit_with_explicit_limit_uses_limit_without_market_controls(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut cache = Cache::default();
    cache.add_instrument(instrument_audusd.clone()).unwrap();
    cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(cache))),
        Some(RiskEngineConfig::default()),
        None,
        false,
    );
    let order = OrderTestBuilder::new(OrderType::TrailingStopLimit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.00000"))
        .trigger_price(Price::from("1.10000"))
        .quantity(Quantity::from("100"))
        .limit_offset(dec!(0.01))
        .trailing_offset(dec!(0.01))
        .trailing_offset_type(TrailingOffsetType::Price)
        .trigger_type(TriggerType::BidAsk)
        .build();

    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert!(get_process_order_event_handler_messages(&process_order_event_handler).is_empty());
    assert_eq!(
        get_execute_order_event_handler_messages(&execute_order_event_handler).len(),
        1
    );
}

fn get_exec_engine(
    cache: Option<Rc<RefCell<Cache>>>,
    clock: Option<Rc<RefCell<TestClock>>>,
    config: Option<ExecutionEngineConfig>,
) -> ExecutionEngine {
    let cache = cache.unwrap_or(Rc::new(RefCell::new(Cache::default())));
    let clock = clock.unwrap_or(Rc::new(RefCell::new(TestClock::new())));
    ExecutionEngine::new(clock, cache, config)
}

#[rstest]
fn test_counters_increment_and_reset(get_stub_submit_order: (OrderAny, SubmitOrder)) {
    let (order, submit_order) = get_stub_submit_order;
    let mut risk_engine = get_risk_engine(None, None, None, true);

    assert_eq!(risk_engine.command_count(), 0);
    assert_eq!(risk_engine.event_count(), 0);

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    risk_engine.process(OrderEventAny::Submitted(order_submitted(&order)));

    assert_eq!(risk_engine.command_count(), 1);
    assert_eq!(risk_engine.event_count(), 1);

    risk_engine.reset();

    assert_eq!(risk_engine.command_count(), 0);
    assert_eq!(risk_engine.event_count(), 0);
}

#[rstest]
fn test_register_msgbus_handlers_registers_process_and_event_subscriptions(
    get_stub_submit_order: (OrderAny, SubmitOrder),
) {
    msgbus::get_message_bus().borrow_mut().dispose();

    let (order, _) = get_stub_submit_order;
    let risk_engine = Rc::new(RefCell::new(get_risk_engine(None, None, None, true)));
    RiskEngine::register_msgbus_handlers(&risk_engine);

    let submitted = OrderEventAny::Submitted(order_submitted(&order));
    msgbus::send_order_event(
        MessagingSwitchboard::risk_engine_process(),
        submitted.clone(),
    );

    let order_topic = format!("events.order.{}", order.strategy_id());
    msgbus::publish_order_event(order_topic.into(), &submitted);

    let position_event = PositionEvent::PositionOpened(position_opened(&order));
    let position_topic = format!("events.position.{}", order.strategy_id());
    msgbus::publish_position_event(position_topic.into(), &position_event);

    assert_eq!(risk_engine.borrow().event_count(), 3);
}

#[rstest]
fn test_deferred_risk_command_is_checked_before_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
) {
    std::thread::spawn(move || {
        msgbus::get_message_bus().borrow_mut().dispose();
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));

        let process_handler = register_process_handler();
        let (exec_handler, exec_saving_handler) = get_typed_into_message_saving_handler::<
            TradingCommand,
        >(Some(Ustr::from("ExecEngine.execute")));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            exec_handler,
        );

        let mut cache = Cache::default();
        cache.add_instrument(instrument_audusd.clone()).unwrap();
        cache
            .add_account(AccountAny::Cash(cash_account(
                cash_account_state_million_usd,
            )))
            .unwrap();
        let risk_engine = Rc::new(RefCell::new(get_risk_engine(
            Some(Rc::new(RefCell::new(cache))),
            None,
            None,
            false,
        )));
        RiskEngine::register_msgbus_handlers(&risk_engine);

        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_audusd.id())
            .side(OrderSide::NoOrderSide)
            .price(Price::from("1.00000"))
            .quantity(Quantity::from("100"))
            .build();
        risk_engine
            .borrow()
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();
        let submit_order = SubmitOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            risk_engine.borrow().clock().borrow().timestamp_ns(),
            None,
        );

        msgbus::send_trading_command(
            MessagingSwitchboard::risk_engine_queue_execute(),
            TradingCommand::SubmitOrder(submit_order),
        );
        assert_eq!(risk_engine.borrow().command_count(), 0);

        drain_trading_cmd_queue();

        let denied = get_process_order_event_handler_messages(&process_handler);
        assert_eq!(risk_engine.borrow().command_count(), 1);
        assert_eq!(denied.len(), 1);
        assert_eq!(denied[0].event_type(), OrderEventType::Denied);
        assert_eq!(
            denied[0].message().unwrap(),
            Ustr::from("INVALID_ORDER_SIDE: NO_ORDER_SIDE")
        );
        assert_eq!(exec_saving_handler.get_messages(), Vec::new());
    })
    .join()
    .unwrap();
}

#[rstest]
fn test_deferred_risk_denial_does_not_reenter_engine(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    cash_account_state_million_usd: AccountState,
) {
    std::thread::spawn(move || {
        msgbus::get_message_bus().borrow_mut().dispose();
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));

        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument_audusd.clone()).unwrap();
            cache
                .add_account(AccountAny::Cash(cash_account(
                    cash_account_state_million_usd,
                )))
                .unwrap();
        }

        let exec_engine = Rc::new(RefCell::new(get_exec_engine(
            Some(cache.clone()),
            Some(clock.clone()),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&exec_engine);
        let risk_engine = Rc::new(RefCell::new(get_risk_engine(
            Some(cache.clone()),
            None,
            Some(clock),
            false,
        )));
        RiskEngine::register_msgbus_handlers(&risk_engine);

        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_audusd.id())
            .side(OrderSide::NoOrderSide)
            .price(Price::from("1.00000"))
            .quantity(Quantity::from("100"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();
        let submit_order = SubmitOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            risk_engine.borrow().clock().borrow().timestamp_ns(),
            None,
        );

        msgbus::send_trading_command(
            MessagingSwitchboard::risk_engine_queue_execute(),
            TradingCommand::SubmitOrder(submit_order),
        );
        drain_trading_cmd_queue();

        assert!(trading_cmd_queue_is_empty());
        assert_eq!(risk_engine.borrow().command_count(), 1);
        assert_eq!(exec_engine.borrow().command_count(), 0);
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
    })
    .join()
    .unwrap();
}

#[rstest]
fn test_deferred_risk_approval_preserves_command_order(
    get_stub_submit_order: (OrderAny, SubmitOrder),
) {
    std::thread::spawn(move || {
        msgbus::get_message_bus().borrow_mut().dispose();
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));

        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let exec_engine = Rc::new(RefCell::new(get_exec_engine(
            Some(cache.clone()),
            Some(clock.clone()),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&exec_engine);

        let risk_engine = Rc::new(RefCell::new(get_risk_engine(
            Some(cache),
            None,
            Some(clock),
            true,
        )));
        RiskEngine::register_msgbus_handlers(&risk_engine);

        let (exec_handler, exec_saving_handler) = get_typed_into_message_saving_handler::<
            TradingCommand,
        >(Some(Ustr::from("ExecEngine.execute")));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            exec_handler,
        );

        let (order, submit_order) = get_stub_submit_order;
        let cancel_order = CancelOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            None,
            UUID4::new(),
            UnixNanos::from(11),
            None,
            None,
        );

        msgbus::send_trading_command(
            MessagingSwitchboard::risk_engine_queue_execute(),
            TradingCommand::SubmitOrder(submit_order),
        );
        msgbus::send_trading_command(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TradingCommand::CancelOrder(cancel_order),
        );

        drain_trading_cmd_queue();

        let commands = exec_saving_handler.get_messages();
        assert!(trading_cmd_queue_is_empty());
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], TradingCommand::SubmitOrder(_)));
        assert!(matches!(commands[1], TradingCommand::CancelOrder(_)));
    })
    .join()
    .unwrap();
}

#[rstest]
fn test_register_msgbus_handlers_subscribes_event_topics_at_priority_10(
    get_stub_submit_order: (OrderAny, SubmitOrder),
) {
    msgbus::get_message_bus().borrow_mut().dispose();

    let (order, _) = get_stub_submit_order;
    let risk_engine = Rc::new(RefCell::new(get_risk_engine(None, None, None, true)));
    let order_observations = Rc::new(RefCell::new(Vec::new()));
    let position_observations = Rc::new(RefCell::new(Vec::new()));

    let high_order_observations = order_observations.clone();
    let high_order_engine = risk_engine.clone();
    msgbus::subscribe_order_events(
        "events.order.*".into(),
        TypedHandler::from_with_id("order-high", move |_: &OrderEventAny| {
            high_order_observations
                .borrow_mut()
                .push(("high", high_order_engine.borrow().event_count()));
        }),
        Some(11),
    );

    let high_position_observations = position_observations.clone();
    let high_position_engine = risk_engine.clone();
    msgbus::subscribe_position_events(
        "events.position.*".into(),
        TypedHandler::from_with_id("position-high", move |_: &PositionEvent| {
            high_position_observations
                .borrow_mut()
                .push(("high", high_position_engine.borrow().event_count()));
        }),
        Some(11),
    );

    RiskEngine::register_msgbus_handlers(&risk_engine);

    let low_order_observations = order_observations.clone();
    let low_order_engine = risk_engine.clone();
    msgbus::subscribe_order_events(
        "events.order.*".into(),
        TypedHandler::from_with_id("order-low", move |_: &OrderEventAny| {
            low_order_observations
                .borrow_mut()
                .push(("low", low_order_engine.borrow().event_count()));
        }),
        Some(9),
    );

    let low_position_observations = position_observations.clone();
    let low_position_engine = risk_engine.clone();
    msgbus::subscribe_position_events(
        "events.position.*".into(),
        TypedHandler::from_with_id("position-low", move |_: &PositionEvent| {
            low_position_observations
                .borrow_mut()
                .push(("low", low_position_engine.borrow().event_count()));
        }),
        Some(9),
    );

    let submitted = OrderEventAny::Submitted(order_submitted(&order));
    let order_topic = format!("events.order.{}", order.strategy_id());
    msgbus::publish_order_event(order_topic.into(), &submitted);

    let position_event = PositionEvent::PositionOpened(position_opened(&order));
    let position_topic = format!("events.position.{}", order.strategy_id());
    msgbus::publish_position_event(position_topic.into(), &position_event);

    assert_eq!(
        order_observations.borrow().as_slice(),
        &[("high", 0), ("low", 1)]
    );
    assert_eq!(
        position_observations.borrow().as_slice(),
        &[("high", 1), ("low", 2)]
    );
    assert_eq!(risk_engine.borrow().event_count(), 2);
}

fn order_submitted(order: &OrderAny) -> OrderSubmitted {
    OrderSubmittedSpec::builder()
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(order.instrument_id())
        .client_order_id(order.client_order_id())
        .account_id(order.account_id().unwrap_or(account_id()))
        .build()
}

fn position_opened(order: &OrderAny) -> PositionOpened {
    PositionOpened {
        trader_id: order.trader_id(),
        strategy_id: order.strategy_id(),
        instrument_id: order.instrument_id(),
        position_id: PositionId::new("P-001"),
        account_id: account_id(),
        opening_order_id: order.client_order_id(),
        entry: OrderSide::Buy,
        side: PositionSide::Long,
        signed_qty: 1.0,
        quantity: order.quantity(),
        last_qty: order.quantity(),
        last_px: Price::from("1.0"),
        currency: Currency::USD(),
        avg_px_open: 1.0,
        event_id: UUID4::new(),
        ts_event: UnixNanos::from(1),
        ts_init: UnixNanos::from(1),
    }
}

fn order_accepted(
    order: &OrderAny,
    venue_order_id: Option<VenueOrderId>,
    account_id: Option<AccountId>,
) -> OrderAccepted {
    OrderAcceptedSpec::builder()
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(order.instrument_id())
        .client_order_id(order.client_order_id())
        .venue_order_id(venue_order_id.expect("venue_order_id required for order_accepted"))
        .account_id(account_id.unwrap_or_else(|| AccountId::new("SIM-001")))
        .build()
}

fn order_filled(
    order: &OrderAny,
    instrument: &InstrumentAny,
    strategy_id: Option<StrategyId>,
    account_id: Option<AccountId>,
    venue_order_id: Option<VenueOrderId>,
    trade_id: Option<TradeId>,
    last_qty: Option<Quantity>,
    last_px: Option<Price>,
    liquidity_side: Option<LiquiditySide>,
    account: Option<AccountAny>,
    ts_filled_ns: Option<UnixNanos>,
) -> OrderFilled {
    let strategy_id = strategy_id.unwrap_or(order.strategy_id());
    let account_id = account_id
        .or_else(|| order.account_id())
        .expect("account_id required for order_filled");
    let venue_order_id = venue_order_id
        .or_else(|| order.venue_order_id())
        .expect("venue_order_id required for order_filled");
    let trade_id = trade_id.unwrap_or(order.client_order_id().as_str().replace('O', "E").into());
    let last_qty = last_qty.unwrap_or(order.quantity());
    let last_px = last_px.unwrap_or(order.price().unwrap_or_default());
    let liquidity_side = liquidity_side.unwrap_or(LiquiditySide::Taker);
    let ts_filled_ns = ts_filled_ns.unwrap_or(0.into());
    let account = account.unwrap_or(AccountAny::Cash(cash_account(
        cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
    )));

    let commission = account
        .calculate_commission(instrument, order.quantity(), last_px, liquidity_side, None)
        .unwrap();

    OrderFilledSpec::builder()
        .trader_id(order.trader_id())
        .strategy_id(strategy_id)
        .instrument_id(instrument.id())
        .client_order_id(order.client_order_id())
        .venue_order_id(venue_order_id)
        .account_id(account_id)
        .trade_id(trade_id)
        .order_side(order.order_side())
        .order_type(order.order_type())
        .last_qty(last_qty)
        .last_px(last_px)
        .currency(instrument.quote_currency())
        .liquidity_side(liquidity_side)
        .ts_event(ts_filled_ns)
        .commission(commission)
        .build()
}

#[rstest]
fn test_bypass_config_risk_engine() {
    let risk_engine = get_risk_engine(
        None, None, None, true, // <-- Bypassing pre-trade risk checks for backtest
    );

    assert!(risk_engine.config().bypass);
}

#[rstest]
fn test_trading_state_after_instantiation_returns_active() {
    let risk_engine = get_risk_engine(None, None, None, false);

    assert_eq!(risk_engine.trading_state(), TradingState::Active);
}

#[rstest]
fn test_set_trading_state_when_no_change_logs_warning() {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    risk_engine.set_trading_state(TradingState::Active);

    assert_eq!(risk_engine.trading_state(), TradingState::Active);
}

#[rstest]
fn test_set_trading_state_changes_value_and_publishes_event() {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    risk_engine.set_trading_state(TradingState::Halted);

    assert_eq!(risk_engine.trading_state(), TradingState::Halted);
}

#[rstest]
fn test_max_order_submit_rate_when_no_risk_config_returns_10_per_second() {
    let risk_engine = get_risk_engine(None, None, None, false);

    assert_eq!(risk_engine.config().max_order_submit.limit(), 10);
    assert_eq!(risk_engine.config().max_order_submit.interval_ns(), 1000);
}

#[rstest]
fn test_max_order_modify_rate_when_no_risk_config_returns_5_per_second() {
    let risk_engine = get_risk_engine(None, None, None, false);

    assert_eq!(risk_engine.config().max_order_modify.limit(), 5);
    assert_eq!(risk_engine.config().max_order_modify.interval_ns(), 1000);
}

#[rstest]
fn test_max_notionals_per_order_when_no_risk_config_returns_empty_hashmap() {
    let risk_engine = get_risk_engine(None, None, None, false);

    assert_eq!(*risk_engine.max_notional_per_order(), AHashMap::new());
}

#[rstest]
fn test_set_max_notional_per_order_changes_setting(instrument_audusd: InstrumentAny) {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    risk_engine
        .set_max_notional_per_order(instrument_audusd.id(), Decimal::from_i64(100_000).unwrap());

    let mut expected = AHashMap::new();
    expected.insert(instrument_audusd.id(), Decimal::from_i64(100_000).unwrap());
    assert_eq!(*risk_engine.max_notional_per_order(), expected);
}

#[rstest]
fn test_terminal_failure_latch_rejects_public_rearm_and_survives_restore() {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    let instrument_id = InstrumentId::from("BTC/USDT.OKX");
    risk_engine.set_max_notional_per_order(instrument_id, dec!(50000));

    risk_engine.force_halt_for_terminal_failure("writer acknowledgement lost");

    assert_eq!(risk_engine.trading_state(), TradingState::Halted);
    assert!(risk_engine.is_terminal_failure_latched());
    assert!(!risk_engine.is_incomplete_recovery_interlocked());
    assert_eq!(
        risk_engine.max_notional_per_order().get(&instrument_id),
        Some(&dec!(50000))
    );
    assert_eq!(
        risk_engine
            .try_set_trading_state(TradingState::Active)
            .unwrap_err(),
        RiskMutationError::TerminalFailureLatched
    );
    assert_eq!(
        risk_engine
            .try_set_trading_state(TradingState::Reducing)
            .unwrap_err(),
        RiskMutationError::TerminalFailureLatched
    );
    assert_eq!(
        risk_engine.try_reset().unwrap_err(),
        RiskMutationError::TerminalFailureLatched
    );
    assert_eq!(
        risk_engine
            .try_set_trading_state(TradingState::Halted)
            .unwrap(),
        RiskMutationOutcome::Unchanged
    );
    assert_eq!(
        risk_engine.max_notional_per_order().get(&instrument_id),
        Some(&dec!(50000)),
        "rejected reset must not discard a dynamic limit"
    );

    let historical_active_checkpoint = RiskStateChanged::new(
        msgbus::get_message_bus().borrow().trader_id,
        TradingState::Active,
        [(instrument_id, dec!(50000))],
        RiskStateChangeReason::Initialized,
        UUID4::new(),
        UnixNanos::from(1),
        UnixNanos::from(1),
    );
    assert_eq!(
        risk_engine
            .restore_risk_state(&historical_active_checkpoint)
            .unwrap(),
        RiskMutationOutcome::Unchanged
    );
    assert_eq!(risk_engine.trading_state(), TradingState::Halted);
    assert!(risk_engine.is_terminal_failure_latched());
    assert_eq!(
        risk_engine
            .try_set_trading_state(TradingState::Active)
            .unwrap_err(),
        RiskMutationError::TerminalFailureLatched,
        "a complete historical checkpoint is not operator re-arm authorization"
    );
}

#[rstest]
fn test_incomplete_recovery_interlock_rejects_rearm_with_limits_or_empty_baseline() {
    let instrument_id = InstrumentId::from("ETH/USDT.OKX");
    let mut with_dynamic_limit = get_risk_engine(None, None, None, false);
    with_dynamic_limit.set_max_notional_per_order(instrument_id, dec!(125000));
    with_dynamic_limit
        .force_halt_for_incomplete_recovery("predecessor terminal outcome was unknown");

    assert!(with_dynamic_limit.is_incomplete_recovery_interlocked());
    assert_eq!(
        with_dynamic_limit
            .try_set_trading_state(TradingState::Active)
            .unwrap_err(),
        RiskMutationError::IncompleteRecoveryInterlock
    );
    assert_eq!(
        with_dynamic_limit.try_reset().unwrap_err(),
        RiskMutationError::IncompleteRecoveryInterlock
    );
    assert_eq!(
        with_dynamic_limit
            .max_notional_per_order()
            .get(&instrument_id),
        Some(&dec!(125000)),
        "the safety halt must not erase a recovered dynamic limit"
    );

    let mut empty_baseline = get_risk_engine(None, None, None, false);
    assert!(empty_baseline.max_notional_per_order().is_empty());
    empty_baseline.force_halt_for_incomplete_recovery("checkpoint was unavailable");
    assert_eq!(
        empty_baseline
            .try_set_trading_state(TradingState::Active)
            .unwrap_err(),
        RiskMutationError::IncompleteRecoveryInterlock
    );
    assert_eq!(
        empty_baseline.try_reset().unwrap_err(),
        RiskMutationError::IncompleteRecoveryInterlock
    );
    assert_eq!(empty_baseline.trading_state(), TradingState::Halted);
}

#[rstest]
fn test_risk_state_admission_rejection_is_atomic(instrument_audusd: InstrumentAny) {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    risk_engine.set_trading_state(TradingState::Halted);
    risk_engine.set_max_notional_per_order(instrument_audusd.id(), dec!(100000));
    let before_state = risk_engine.trading_state();
    let before_notionals = risk_engine.max_notional_per_order().clone();
    let before_command_count = risk_engine.command_count();
    let before_event_count = risk_engine.event_count();

    let legacy_handler = msgbus::stubs::get_message_saving_handler::<TradingStateChanged>(None);
    msgbus::subscribe_any("events.risk".into(), legacy_handler.clone(), None);
    let _tap_guard = reject_risk_state_admission();

    let state_error = risk_engine
        .try_set_trading_state(TradingState::Reducing)
        .unwrap_err();
    assert!(matches!(
        state_error,
        RiskMutationError::DurabilityRejected { .. }
    ));
    assert_eq!(risk_engine.trading_state(), before_state);
    assert_eq!(risk_engine.max_notional_per_order(), &before_notionals);
    assert!(
        msgbus::stubs::get_saved_messages::<TradingStateChanged>(&legacy_handler).is_empty(),
        "legacy observers must not see a rejected state change"
    );

    let max_error = risk_engine
        .try_set_max_notional_per_order(instrument_audusd.id(), dec!(200000))
        .unwrap_err();
    assert!(matches!(
        max_error,
        RiskMutationError::DurabilityRejected { .. }
    ));
    assert_eq!(risk_engine.trading_state(), before_state);
    assert_eq!(risk_engine.max_notional_per_order(), &before_notionals);

    let reset_error = risk_engine.try_reset().unwrap_err();
    assert!(matches!(
        reset_error,
        RiskMutationError::DurabilityRejected { .. }
    ));
    assert_eq!(risk_engine.trading_state(), before_state);
    assert_eq!(risk_engine.max_notional_per_order(), &before_notionals);
    assert_eq!(risk_engine.command_count(), before_command_count);
    assert_eq!(risk_engine.event_count(), before_event_count);

    let checkpoint_error = risk_engine.try_checkpoint_risk_state().unwrap_err();
    assert!(matches!(
        checkpoint_error,
        RiskMutationError::DurabilityRejected { .. }
    ));
}

#[rstest]
fn test_risk_state_snapshots_are_sorted_and_unchanged_mutations_do_not_publish() {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    let (observations, _tap_guard) = observe_risk_state_admission_types();

    assert_eq!(
        risk_engine
            .try_set_trading_state(TradingState::Active)
            .unwrap(),
        RiskMutationOutcome::Unchanged
    );
    assert_eq!(observations.authorized.get(), 0);

    let second_id = InstrumentId::from("ZZZ/USD.SIM");
    let first_id = InstrumentId::from("AAA/USD.SIM");
    assert_eq!(
        risk_engine
            .try_set_max_notional_per_order(second_id, dec!(200000.00))
            .unwrap(),
        RiskMutationOutcome::Applied
    );
    assert_eq!(
        risk_engine
            .try_set_max_notional_per_order(first_id, dec!(100000.0))
            .unwrap(),
        RiskMutationOutcome::Applied
    );
    let published_before_noop = observations.authorized.get();
    assert_eq!(
        risk_engine
            .try_set_max_notional_per_order(first_id, dec!(100000.00))
            .unwrap(),
        RiskMutationOutcome::Unchanged
    );

    assert_eq!(observations.authorized.get(), published_before_noop);
    assert_eq!(observations.authorized.get(), 2);
    assert_eq!(observations.raw.get(), 0);
    assert_eq!(
        risk_engine.try_checkpoint_risk_state().unwrap(),
        RiskMutationOutcome::Applied
    );
    assert_eq!(observations.authorized.get(), 3);
    let snapshots = observations.snapshots.borrow();
    let checkpoint = snapshots.last().unwrap();
    assert_eq!(
        checkpoint
            .max_notional_per_order
            .iter()
            .map(|entry| (entry.instrument_id.as_str(), entry.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("AAA/USD.SIM", "100000"), ("ZZZ/USD.SIM", "200000")]
    );
}

#[rstest]
fn test_risk_state_checkpoint_round_trips() {
    let mut source = get_risk_engine(None, None, None, false);
    source.set_trading_state(TradingState::Reducing);
    source.set_max_notional_per_order(InstrumentId::from("ETH/USDT.OKX"), dec!(123456.7800));
    let (observations, _tap_guard) = observe_risk_state_admission_types();
    assert_eq!(
        source.try_checkpoint_risk_state().unwrap(),
        RiskMutationOutcome::Applied
    );
    let checkpoint = observations.snapshots.borrow().last().unwrap().clone();

    let restore_clock = Rc::new(RefCell::new(TestClock::new()));
    let mut restored = get_risk_engine(None, None, Some(Rc::clone(&restore_clock)), false);
    assert_eq!(
        restored.restore_risk_state(&checkpoint).unwrap(),
        RiskMutationOutcome::Applied
    );
    assert_eq!(restored.trading_state(), TradingState::Reducing);
    assert!(restored.throttled_submit.is_limiting());
    assert!(restored.throttled_modify_order.is_limiting());
    assert_eq!(
        restored
            .max_notional_per_order()
            .get(&InstrumentId::from("ETH/USDT.OKX")),
        Some(&dec!(123456.78))
    );
    restore_clock
        .borrow_mut()
        .advance_time(UnixNanos::from(1_000), true);
    assert!(restored.throttled_submit.try_reserve(1));
    assert!(restored.throttled_modify_order.try_reserve(1));
    assert!(!restored.throttled_submit.is_limiting());
    assert!(!restored.throttled_modify_order.is_limiting());

    assert_eq!(
        restored.restore_risk_state(&checkpoint).unwrap(),
        RiskMutationOutcome::Unchanged
    );
    assert!(restored.throttled_submit.is_limiting());
    assert!(restored.throttled_modify_order.is_limiting());
}

#[rstest]
fn test_restore_risk_state_rejects_bad_snapshots_without_partial_mutation() {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    risk_engine.set_trading_state(TradingState::Halted);
    risk_engine.set_max_notional_per_order(InstrumentId::from("BTC/USDT.OKX"), dec!(50000));
    let before_state = risk_engine.trading_state();
    let before_notionals = risk_engine.max_notional_per_order().clone();
    let (observations, _tap_guard) = observe_risk_state_admission_types();
    risk_engine.try_checkpoint_risk_state().unwrap();
    let base = observations.snapshots.borrow().last().unwrap().clone();

    let invalid_entries = [
        (
            vec![RiskMaxNotional {
                instrument_id: "INVALID".to_string(),
                value: "1".to_string(),
            }],
            "invalid instrument ID",
        ),
        (
            vec![RiskMaxNotional {
                instrument_id: "ETH/USDT.OKX".to_string(),
                value: "not-a-decimal".to_string(),
            }],
            "invalid decimal",
        ),
        (
            vec![RiskMaxNotional {
                instrument_id: "ETH/USDT.OKX".to_string(),
                value: "0".to_string(),
            }],
            "non-positive decimal",
        ),
        (
            vec![
                RiskMaxNotional {
                    instrument_id: "ETH/USDT.OKX".to_string(),
                    value: "1".to_string(),
                },
                RiskMaxNotional {
                    instrument_id: "ETH/USDT.OKX".to_string(),
                    value: "2".to_string(),
                },
            ],
            "duplicate instrument ID",
        ),
    ];

    for (entries, case) in invalid_entries {
        let mut snapshot = base.clone();
        snapshot.state = TradingState::Active;
        snapshot.max_notional_per_order = entries;
        assert!(
            risk_engine.restore_risk_state(&snapshot).is_err(),
            "{case} must be rejected"
        );
        assert_eq!(risk_engine.trading_state(), before_state, "case: {case}");
        assert_eq!(
            risk_engine.max_notional_per_order(),
            &before_notionals,
            "case: {case}"
        );
        assert!(!risk_engine.throttled_submit.is_limiting(), "case: {case}");
        assert!(
            !risk_engine.throttled_modify_order.is_limiting(),
            "case: {case}"
        );
    }
}

#[rstest]
fn test_restore_risk_state_rejects_during_authoritative_capture_without_side_effects() {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    risk_engine.set_trading_state(TradingState::Halted);
    let instrument_id = InstrumentId::from("BTC/USDT.OKX");
    risk_engine.set_max_notional_per_order(instrument_id, dec!(50000));

    let before_state = risk_engine.trading_state();
    let before_notionals = risk_engine.max_notional_per_order().clone();
    let before_submit_limiting = risk_engine.throttled_submit.is_limiting();
    let before_modify_limiting = risk_engine.throttled_modify_order.is_limiting();
    let before_submit_delta = risk_engine.throttled_submit.delta_next();
    let before_modify_delta = risk_engine.throttled_modify_order.delta_next();
    let trader_id = msgbus::get_message_bus().borrow().trader_id;
    let forged = RiskStateChanged::new(
        trader_id,
        TradingState::Active,
        std::iter::empty::<(InstrumentId, Decimal)>(),
        nautilus_common::messages::system::RiskStateChangeReason::Initialized,
        UUID4::new(),
        UnixNanos::from(1),
        UnixNanos::from(1),
    );

    {
        let _tap_guard = install_exclusive_allow_tap();
        let error = risk_engine.restore_risk_state(&forged).unwrap_err();
        assert_eq!(error, RiskMutationError::RestoreDuringAuthoritativeCapture);
        assert_eq!(risk_engine.trading_state(), before_state);
        assert_eq!(risk_engine.max_notional_per_order(), &before_notionals);
        assert_eq!(
            risk_engine.throttled_submit.is_limiting(),
            before_submit_limiting
        );
        assert_eq!(
            risk_engine.throttled_modify_order.is_limiting(),
            before_modify_limiting
        );
        assert_eq!(
            risk_engine.throttled_submit.delta_next(),
            before_submit_delta
        );
        assert_eq!(
            risk_engine.throttled_modify_order.delta_next(),
            before_modify_delta
        );
    }

    assert!(!msgbus::is_exclusive_bus_tap_installed());
}

#[rstest]
fn test_try_set_max_notional_rejects_non_positive_before_admission() {
    let mut risk_engine = get_risk_engine(None, None, None, false);
    let instrument_id = InstrumentId::from("ETH/USDT.OKX");
    let _tap_guard = reject_risk_state_admission();

    let error = risk_engine
        .try_set_max_notional_per_order(instrument_id, Decimal::ZERO)
        .unwrap_err();
    assert!(matches!(
        error,
        RiskMutationError::InvalidMaxNotional { .. }
    ));
    assert!(
        !risk_engine
            .max_notional_per_order()
            .contains_key(&instrument_id)
    );
}

#[rstest]
fn test_given_random_command_then_logs_and_continues(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
) {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(100.0, 0))
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let random_command = TradingCommand::SubmitOrder(submit_order);

    risk_engine.execute(random_command);
}

// SUBMIT ORDER TESTS
#[rstest]
fn test_submit_order_with_default_settings_then_sends_to_client(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(100.0, 0))
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
fn test_submit_order_when_risk_bypassed_sends_to_execution_engine(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    consume_fixture(process_order_event_handler);
    let mut risk_engine = get_risk_engine(None, None, None, true);

    // TODO: Limit -> Market
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(100.0, 0))
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
fn test_submit_reduce_only_order_when_position_already_closed_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    clock: TestClock,
    simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    let clock = Rc::new(RefCell::new(clock));
    let simple_cache = Rc::new(RefCell::new(simple_cache));

    let mut risk_engine =
        get_risk_engine(Some(simple_cache.clone()), None, Some(clock.clone()), true);
    let mut exec_engine = get_exec_engine(Some(simple_cache), Some(clock), None);

    let order1 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000"))
        .build();

    let order2 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1000"))
        .reduce_only(true)
        .build();

    let order3 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1000"))
        .reduce_only(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order1 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order1.client_order_id(),
        order1.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let account_id = AccountId::new("SIM-001");
    let submitted = OrderEventAny::Submitted(order_submitted(&order1));
    let accepted = OrderEventAny::Accepted(order_accepted(
        &order1,
        Some(venue_order_id),
        Some(account_id),
    ));
    let filled = OrderEventAny::Filled(order_filled(
        &order1,
        &instrument_audusd,
        None,
        Some(account_id),
        Some(venue_order_id),
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order1));
    exec_engine.process(&submitted);
    exec_engine.process(&accepted);
    exec_engine.process(&filled);

    let submit_order2 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order2.client_order_id(),
        order2.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let venue_order_id2 = VenueOrderId::new("002");
    risk_engine.execute(TradingCommand::SubmitOrder(submit_order2));
    exec_engine.process(&OrderEventAny::Submitted(order_submitted(&order2)));
    exec_engine.process(&OrderEventAny::Filled(order_filled(
        &order2,
        &instrument_audusd,
        None,
        Some(account_id),
        Some(venue_order_id2),
        None,
        None,
        None,
        None,
        None,
        None,
    )));

    let submit_order3 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order3.client_order_id(),
        order3.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order3));

    // TODO
    // assert_eq!(order1.status(), OrderStatus::Filled);
    // assert_eq!(order2.status(), OrderStatus::Filled);
    // assert_eq!(order3.status(), OrderStatus::Denied);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 3);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
fn test_submit_reduce_only_order_when_position_would_be_increased_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    clock: TestClock,
    simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    let clock = Rc::new(RefCell::new(clock));
    let simple_cache = Rc::new(RefCell::new(simple_cache));

    let mut risk_engine =
        get_risk_engine(Some(simple_cache.clone()), None, Some(clock.clone()), true);
    let mut exec_engine = get_exec_engine(Some(simple_cache), Some(clock), None);

    let order1 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000"))
        .build();

    let order2 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("2000"))
        .reduce_only(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order1.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order1 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order1.client_order_id(),
        order1.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let account_id = AccountId::new("SIM-001");
    let submitted = OrderEventAny::Submitted(order_submitted(&order1));
    let accepted = OrderEventAny::Accepted(order_accepted(
        &order1,
        Some(venue_order_id),
        Some(account_id),
    ));
    let filled = OrderEventAny::Filled(order_filled(
        &order1,
        &instrument_audusd,
        None,
        Some(account_id),
        Some(venue_order_id),
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order1));
    exec_engine.process(&submitted);
    exec_engine.process(&accepted);
    exec_engine.process(&filled);

    let submit_order2 = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order2.client_order_id(),
        order2.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let venue_order_id2 = VenueOrderId::new("002");
    risk_engine.execute(TradingCommand::SubmitOrder(submit_order2));
    exec_engine.process(&OrderEventAny::Submitted(order_submitted(&order2)));
    exec_engine.process(&OrderEventAny::Accepted(order_accepted(
        &order2,
        Some(venue_order_id2),
        Some(account_id),
    )));
    exec_engine.process(&OrderEventAny::Filled(order_filled(
        &order2,
        &instrument_audusd,
        None,
        Some(account_id),
        Some(venue_order_id2),
        None,
        None,
        None,
        None,
        None,
        None,
    )));

    // TODO
    // assert_eq!(order1.status(), OrderStatus::Filled);
    // assert_eq!(order2.status(), OrderStatus::Denied);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 2);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
fn test_submit_order_reduce_only_order_with_custom_position_id_not_open_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(100.0, 0))
        .quantity(Quantity::from("1000"))
        .reduce_only(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        Some(PositionId::new("CUSTOM-001")), // <-- Custom position ID
        None,                                // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("POSITION_NOT_FOUND: position_id=CUSTOM-001")
    );
}

#[rstest]
fn test_submit_order_when_instrument_not_in_cache_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(100.0, 0))
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("INSTRUMENT_NOT_FOUND: instrument_id=AUD/USD.SIM")
    );
}

#[rstest]
fn test_submit_order_when_invalid_price_precision_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from_raw(1_000_000_000_000, FIXED_PRECISION)) // <- Invalid price
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages
            .first()
            .unwrap()
            .message()
            .unwrap()
            .contains(&format!("invalid (precision {FIXED_PRECISION} > 5)"))
    );
}

#[rstest]
fn test_submit_order_when_invalid_negative_price_and_not_option_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::new(-0.1, 1)) // <- Invalid price (negative)
        .quantity(Quantity::from("1000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("price -0.1 invalid (<= 0)")
    );
}

#[rstest]
fn test_submit_order_when_negative_price_for_futures_spread_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_futures_spread: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_futures_spread.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_futures_spread,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_futures_spread.id())
        .side(OrderSide::Buy)
        .price(Price::new(-17.0, 2)) // Negative price is valid for spreads
        .quantity(Quantity::from("1"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_futures_spread.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_futures_spread.id()
    );
}

#[rstest]
fn test_submit_order_when_negative_price_for_option_spread_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_option_spread: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_option_spread.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_option_spread,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_option_spread.id())
        .side(OrderSide::Buy)
        .price(Price::new(-2.50, 2)) // Negative price -2.50 is valid for spreads
        .quantity(Quantity::from("1"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_option_spread.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_option_spread.id()
    );
}

#[rstest]
fn test_submit_order_when_negative_price_for_commodity_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_commodity: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_commodity.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_commodity,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_commodity.id())
        .side(OrderSide::Buy)
        .price(Price::new(-5.0, 2)) // Negative price is valid for spot commodities
        .quantity(Quantity::from("1"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_commodity.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_commodity.id()
    );
}

#[rstest]
fn test_submit_order_when_zero_price_for_commodity_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_commodity: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_commodity.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_commodity,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_commodity.id())
        .side(OrderSide::Buy)
        .price(Price::new(0.0, 2)) // Zero price shares the negative price gate
        .quantity(Quantity::from("1"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_commodity.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_commodity.id()
    );
}

#[rstest]
fn test_submit_order_when_invalid_trigger_price_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::StopLimit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("1000").unwrap())
        .price(Price::new(0.1, 1))
        .trigger_price(Price::from_raw(1_000_000_000_000_000, FIXED_PRECISION)) // <- Invalid price
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    // assert!(saved_process_messages
    //     .first()
    //     .unwrap()
    //     .message()
    //     .unwrap()
    //     .contains(&format!("invalid (precision {PRECISION})")));
}

#[rstest]
fn test_submit_order_when_invalid_quantity_precision_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("0.1").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("quantity 0.1 invalid (precision 1 > 0)")
    );
}

#[rstest]
fn test_submit_order_when_invalid_quantity_exceeds_maximum_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100000000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("quantity 100000000 invalid (> maximum trade size of 1000000)")
    );
}

#[rstest]
fn test_submit_order_when_invalid_quantity_less_than_minimum_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("1").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from("quantity 1 invalid (< minimum trade size of 100)")
    );
}

#[rstest]
fn test_submit_order_when_market_order_and_fresh_market_then_sends_to_client(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_max_notional_per_order(
        instrument_audusd.id(),
        Decimal::from_i32(10_000_000).unwrap(),
    );

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
fn test_submit_order_when_less_than_min_notional_for_instrument_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_xbtusd_with_high_size_precision: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(execute_order_event_handler);
    simple_cache
        .add_instrument(instrument_xbtusd_with_high_size_precision.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_xbtusd_with_high_size_precision,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_xbtusd_with_high_size_precision.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.0"))
        .quantity(Quantity::from_str("0.9").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_xbtusd_with_high_size_precision.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_BELOW_MINIMUM: min_notional=Money(1.00, USD), notional=Money(0.90, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_when_greater_than_max_notional_for_instrument_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_xbtusd_bitmex: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_xbtusd_bitmex.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_xbtusd_bitmex,
            "100000000 USD",
            "0 USD",
            "100000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_max_notional_per_order(
        instrument_xbtusd_bitmex.id(),
        Decimal::from_i64(100_000_000).unwrap(),
    );

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.0"))
        .quantity(Quantity::from_str("10000001").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_xbtusd_bitmex.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_EXCEEDS_MAXIMUM: max_notional=Money(10000000.00, USD), notional=Money(10000001.00, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_when_buy_market_order_and_over_max_notional_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_audusd.id(),
        Price::from("0.75000"),
        Price::from("0.75005"),
        Quantity::from("500000"),
        Quantity::from("500000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine
        .set_max_notional_per_order(instrument_audusd.id(), Decimal::from_i64(100_000).unwrap());

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("1000000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_EXCEEDS_MAX_PER_ORDER: max_notional=Money(100000.00, USD), notional=Money(750120.00, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_when_notional_is_unrepresentable_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    simple_cache
        .add_quote(QuoteTick::new(
            instrument_audusd.id(),
            Price::from("1000000000.00000"),
            Price::from("1000000000.00000"),
            Quantity::from("1000000"),
            Quantity::from("1000000"),
            UnixNanos::default(),
            UnixNanos::default(),
        ))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1000000"))
        .build();
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].event_type(), OrderEventType::Denied);
    assert!(
        saved[0]
            .message()
            .unwrap()
            .as_str()
            .starts_with("Cannot calculate notional bounds:")
    );
}

#[rstest]
fn test_submit_order_when_sell_market_order_and_over_max_notional_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_audusd.id(),
        Price::from("0.75000"),
        Price::from("0.75005"),
        Quantity::from("500000"),
        Quantity::from("500000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine
        .set_max_notional_per_order(instrument_audusd.id(), Decimal::from_i64(100_000).unwrap());

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("1000000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_EXCEEDS_MAX_PER_ORDER: max_notional=Money(100000.00, USD), notional=Money(750070.00, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_when_market_order_and_over_free_balance_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_EXCEEDS_FREE_BALANCE: free=Money(1000000.00, USD), notional=Money(10101010.00, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_reduce_only_buy_over_free_balance_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100000").unwrap())
        .reduce_only(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);

    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "NOTIONAL_EXCEEDS_FREE_BALANCE: free=Money(1000000.00, USD), notional=Money(10101010.00, USD)"
        )
    );
}

#[rstest]
fn test_submit_order_reduce_only_buy_within_free_balance_then_sends_to_execution(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("1000").unwrap())
        .reduce_only(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_order_when_market_order_over_free_balance_with_borrowing_enabled_then_accepts(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    // Test that orders exceeding free balance are accepted when borrowing is enabled
    // (e.g. spot margin trading on Bybit)

    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    let cash_account_with_borrowing = CashAccount::new(cash_account_state_million_usd, true, true);
    simple_cache
        .add_account(AccountAny::Cash(cash_account_with_borrowing))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Create order that would exceed free balance (same as denied test above)
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // Should NOT be denied because borrowing is enabled
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(
        saved_process_messages.is_empty(),
        "Order should not be denied when borrowing is enabled, but got: {saved_process_messages:?}"
    );
}

#[rstest]
fn test_submit_order_list_buys_when_over_free_balance_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let order1 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("4920").unwrap())
        .build();

    let order2 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("5653").unwrap()) // <--- over free balance
        .build();

    simple_cache
        .add_order(order1.clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(order2.clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let orders = [order1, order2];
    let order_list = OrderList::new(
        OrderListId::new("1"),
        instrument_audusd.id(),
        StrategyId::new("S-001"),
        vec![orders[0].client_order_id(), orders[1].client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_order = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);

    assert_eq!(saved_process_messages.len(), 3);

    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
    }

    // The actual reason is in the first denial; the rest will show `OrderListID` as Denied.
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "CUM_NOTIONAL_EXCEEDS_FREE_BALANCE: free=1000000.00 USD, cum_notional=1067979.79 USD"
        )
    );
}

#[rstest]
fn test_submit_order_list_sells_when_over_free_balance_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let order1 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("4920").unwrap())
        .build();

    let order2 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("5653").unwrap()) // <--- over free balance
        .build();

    let orders = [order1, order2];

    simple_cache
        .add_order(orders[0].clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(orders[1].clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order_list = OrderList::new(
        OrderListId::new("1"),
        instrument_audusd.id(),
        StrategyId::new("S-001"),
        vec![orders[0].client_order_id(), orders[1].client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_order = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);

    assert_eq!(saved_process_messages.len(), 3);

    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
    }

    // Correct reason is in First deny, rest will show OrderList`ID` Denied.
    assert_eq!(
        saved_process_messages.first().unwrap().message().unwrap(),
        Ustr::from(
            "CUM_NOTIONAL_EXCEEDS_FREE_BALANCE: free=1000000.00 USD, cum_notional=1057405.73 USD"
        )
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_submit_order_when_trading_halted_then_denies_order(
    #[case] bypass: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(simple_cache))),
        None,
        None,
        bypass,
    );
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order.instrument_id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.set_trading_state(TradingState::Halted);

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // Get messages and test
    let saved_messages = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_messages.len(), 1);
    let first_message = saved_messages.first().unwrap();
    assert_eq!(first_message.event_type(), OrderEventType::Denied);
    assert_eq!(
        first_message.message().unwrap(),
        Ustr::from("TRADING_HALTED")
    );
}

#[expect(
    clippy::float_cmp,
    reason = "throttler usage is an integer counter represented as f64"
)]
#[rstest]
fn test_submit_order_beyond_rate_limit_then_denies_order(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    for i in 0..11 {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument_audusd.id())
            .client_order_id(ClientOrderId::new(format!("O-{i}")))
            .side(OrderSide::Buy)
            .quantity(Quantity::from_str("100").unwrap())
            .build();

        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();

        let submit_order = SubmitOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            order.instrument_id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None, // params
            UUID4::new(),
            risk_engine.clock().borrow().timestamp_ns(),
            None, // correlation_id
        );

        risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    }

    assert_eq!(risk_engine.throttled_submit.used(), 1.0);

    // Get messages and test
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    let first_message = saved_process_messages.first().unwrap();
    assert_eq!(first_message.event_type(), OrderEventType::Denied);
    assert_eq!(
        first_message.message().unwrap(),
        Ustr::from("RATE_LIMIT_EXCEEDED")
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_submit_order_list_when_trading_halted_then_denies_orders(
    #[case] bypass: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let entry = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(0.1, 1))
        .build();

    let take_profit = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-003"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::new(0.11, 2))
        .build();

    let orders = [entry, stop_loss, take_profit];

    simple_cache
        .add_order(orders[0].clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(orders[1].clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(orders[2].clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(simple_cache))),
        None,
        None,
        bypass,
    );

    let bracket = OrderList::new(
        OrderListId::new("1"),
        instrument_audusd.id(),
        StrategyId::new("S-001"),
        vec![
            orders[0].client_order_id(),
            orders[1].client_order_id(),
            orders[2].client_order_id(),
        ],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_bracket = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        bracket,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.set_trading_state(TradingState::Halted);
    risk_engine.execute(TradingCommand::SubmitOrderList(submit_bracket));

    // Get messages and test
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 3);

    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
        assert_eq!(event.message().unwrap(), Ustr::from("TRADING_HALTED"));
    }
}

#[rstest]
fn test_submit_order_list_denies_when_non_representative_instrument_missing(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    audusd_sim: CurrencyPair,
    gbpusd_sim: CurrencyPair,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    let instrument_a: InstrumentAny = audusd_sim.into();
    let instrument_b: InstrumentAny = gbpusd_sim.into();

    // Only register the representative instrument; non-representative is missing.
    simple_cache.add_instrument(instrument_a.clone()).unwrap();
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let order_a = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_a.id())
        .client_order_id(ClientOrderId::from("O-MISS-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();
    let order_b = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_b.id())
        .client_order_id(ClientOrderId::from("O-MISS-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let orders = [order_a.clone(), order_b.clone()];
    for order in &orders {
        simple_cache
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order_list = OrderList::new(
        OrderListId::new("L-MISS-001"),
        instrument_a.id(),
        StrategyId::new("S-001"),
        vec![order_a.client_order_id(), order_b.client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved.len(), orders.len());
    for event in &saved {
        assert_eq!(event.event_type(), OrderEventType::Denied);
        let msg = event.message().unwrap();
        assert!(
            msg.as_str().contains("INSTRUMENT_NOT_FOUND")
                && msg.as_str().contains(&instrument_b.id().to_string()),
            "unexpected denial reason: {msg}",
        );
    }
}

#[rstest]
fn test_submit_order_list_denies_when_representative_instrument_not_in_list(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    audusd_sim: CurrencyPair,
    gbpusd_sim: CurrencyPair,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    let instrument_a: InstrumentAny = audusd_sim.into();
    let instrument_b: InstrumentAny = gbpusd_sim.into();
    let representative_id = InstrumentId::from("USD/JPY.SIM");

    simple_cache.add_instrument(instrument_a.clone()).unwrap();
    simple_cache.add_instrument(instrument_b.clone()).unwrap();

    let order_a = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_a.id())
        .client_order_id(ClientOrderId::from("O-REP-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();
    let order_b = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_b.id())
        .client_order_id(ClientOrderId::from("O-REP-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let orders = [order_a.clone(), order_b.clone()];
    for order in &orders {
        simple_cache
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order_list = OrderList::new(
        OrderListId::new("L-REP-001"),
        representative_id,
        StrategyId::new("S-001"),
        vec![order_a.client_order_id(), order_b.client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved.len(), orders.len());
    for event in &saved {
        assert_eq!(event.event_type(), OrderEventType::Denied);
        assert_eq!(
            event.message().unwrap(),
            Ustr::from("INSTRUMENT_NOT_FOUND: instrument_id=USD/JPY.SIM")
        );
    }
}

#[rstest]
fn test_submit_order_list_check_order_uses_each_orders_own_instrument(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    audusd_sim: CurrencyPair,
    gbpusd_sim: CurrencyPair,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    let instrument_a: InstrumentAny = audusd_sim.into();
    let instrument_b: InstrumentAny = gbpusd_sim.into();

    simple_cache.add_instrument(instrument_a.clone()).unwrap();
    simple_cache.add_instrument(instrument_b.clone()).unwrap();
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    // AUD/USD price precision is 5. A 6-decimal-place limit price violates instrument_a's
    // precision regardless of which instrument the risk engine looks up. The first order
    // uses a valid 5-dp price; the second order's price is 6-dp and must be denied because
    // its own instrument's precision is checked, not the representative's.
    let order_a = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_a.id())
        .client_order_id(ClientOrderId::from("O-PREC-001"))
        .side(OrderSide::Buy)
        .price(Price::from("0.50000"))
        .quantity(Quantity::from_str("100").unwrap())
        .build();
    let bad_price = Price::from("1.000001"); // 6-dp, exceeds GBP/USD 5-dp precision
    assert!(bad_price.precision > instrument_b.price_precision());
    let order_b = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_b.id())
        .client_order_id(ClientOrderId::from("O-PREC-002"))
        .side(OrderSide::Buy)
        .price(bad_price)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let orders = [order_a.clone(), order_b.clone()];
    for order in &orders {
        simple_cache
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order_list = OrderList::new(
        OrderListId::new("L-PREC-001"),
        instrument_a.id(),
        StrategyId::new("S-001"),
        vec![order_a.client_order_id(), order_b.client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    // The second order is denied for price precision; the first order is not denied
    // (no event emitted for the still-pending entry).
    assert!(saved.iter().any(|event| {
        event.event_type() == OrderEventType::Denied
            && event.client_order_id() == order_b.client_order_id()
    }));
    assert!(
        !saved.iter().any(|event| {
            event.event_type() == OrderEventType::Denied
                && event.client_order_id() == order_a.client_order_id()
        }),
        "first order should not be denied; check_order is per-order: {saved:?}",
    );
}

// Test that order lists with BUY orders are denied when in REDUCING state and already LONG.
//
// This test verifies the risk engine correctly prevents adding to existing positions
// when the trading state is set to REDUCING (position reduction mode only).
//
// TODO: Complete implementation - similar to single order reducing tests but for order lists.
// The test logic needs to properly track portfolio position state through message bus updates.
#[ignore = "Under development - requires portfolio state tracking integration"]
#[rstest]
fn test_submit_order_list_buys_when_trading_reducing_then_denies_orders(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_xbtusd_bitmex: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    bitmex_cash_account_state_multi: AccountState,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_xbtusd_bitmex.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            bitmex_cash_account_state_multi,
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_xbtusd_bitmex.id(),
        Price::from("0.075000"),
        Price::from("0.075005"),
        Quantity::from("50000"),
        Quantity::from("50000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    risk_engine.set_max_notional_per_order(instrument_xbtusd_bitmex.id(), dec!(10000));

    let long = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(long.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_xbtusd_bitmex.id(),
        long.client_order_id(),
        long.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    risk_engine.set_trading_state(TradingState::Reducing);

    let entry = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(1.1, 1))
        .build();

    // TODO: attempt to add with overflow
    // let take_profit = OrderTestBuilder::new(OrderType::Limit)
    //     .instrument_id(instrument_xbtusd_bitmex.id())
    //     .side(OrderSide::Buy)
    //     .quantity(Quantity::from_str("100").unwrap())
    //     .price(Price::new(1.2, 1))
    //     .build();

    let orders = [entry, stop_loss];

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(orders[0].clone(), None, Some(client_id_binance), true)
        .unwrap();
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(orders[1].clone(), None, Some(client_id_binance), true)
        .unwrap();

    let bracket = OrderList::new(
        OrderListId::new("1"),
        instrument_xbtusd_bitmex.id(),
        StrategyId::new("S-001"),
        vec![orders[0].client_order_id(), orders[1].client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_order_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        bracket,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_order_list));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

// Test that order lists with SELL orders are denied when in REDUCING state and already SHORT.
//
// This test verifies the risk engine correctly prevents adding to existing short positions
// when the trading state is set to REDUCING (position reduction mode only).
//
// TODO: Re-enable after high-precision decimal work is merged and stable.
// The test may have precision-related issues with position calculations.
#[ignore = "Waiting on high-precision decimal merge"]
#[rstest]
fn test_submit_order_list_sells_when_trading_reducing_then_denies_orders(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_xbtusd_bitmex: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    bitmex_cash_account_state_multi: AccountState,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_xbtusd_bitmex.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            bitmex_cash_account_state_multi,
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_xbtusd_bitmex.id(),
        Price::from("0.075000"),
        Price::from("0.075005"),
        Quantity::from("50000"),
        Quantity::from("50000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    risk_engine.set_max_notional_per_order(instrument_xbtusd_bitmex.id(), dec!(10000));

    let short = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(short.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_xbtusd_bitmex.id(),
        short.client_order_id(),
        short.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    risk_engine.set_trading_state(TradingState::Reducing);

    let entry = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(1.1, 1))
        .build();

    let take_profit = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::new(1.2, 1))
        .build();

    let orders = [entry, stop_loss, take_profit];

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(orders[0].clone(), None, Some(client_id_binance), true)
        .unwrap();
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(orders[1].clone(), None, Some(client_id_binance), true)
        .unwrap();
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(orders[2].clone(), None, Some(client_id_binance), true)
        .unwrap();

    let bracket = OrderList::new(
        OrderListId::new("1"),
        instrument_xbtusd_bitmex.id(),
        StrategyId::new("S-001"),
        vec![
            orders[0].client_order_id(),
            orders[1].client_order_id(),
            orders[2].client_order_id(),
        ],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_order_list = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        bracket,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_order_list));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

// SUBMIT BRACKET ORDER TESTS

// Verify bracket orders with emulated orders are sent to emulator.
//
// This test requires the order emulator component to be implemented. The emulator
// handles client-side order management for conditional orders (stop-loss, take-profit, etc.)
// that need to be triggered locally before being sent to the venue.
//
// TODO: Re-enable once the emulator component is integrated with the risk engine.
// Dependencies: Order emulation infrastructure in execution engine
#[ignore = "Waiting on emulator implementation"]
#[rstest]
fn test_submit_bracket_with_emulated_orders_sends_to_emulator() {}

#[rstest]
fn test_submit_bracket_order_when_instrument_not_in_cache_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let entry = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(0.1, 1))
        .build();

    let take_profit = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-003"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::new(0.1001, 4))
        .build();

    let orders = [entry, stop_loss, take_profit];

    // Add orders to cache (but NOT the instrument - testing instrument not found case)
    simple_cache
        .add_order(orders[0].clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(orders[1].clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(orders[2].clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let bracket = OrderList::new(
        OrderListId::new("1"),
        instrument_audusd.id(),
        StrategyId::new("S-001"),
        vec![
            orders[0].client_order_id(),
            orders[1].client_order_id(),
            orders[2].client_order_id(),
        ],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_bracket = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        bracket,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_bracket));

    // Get messages and test
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 3);

    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
        assert_eq!(
            event.message().unwrap(),
            Ustr::from("INSTRUMENT_NOT_FOUND: instrument_id=AUD/USD.SIM")
        );
    }
}

// Verify that orders marked for emulation are correctly routed to the emulator.
//
// This test should verify that when an order is submitted with emulation flags,
// the risk engine routes it to the order emulator rather than directly to execution.
//
// TODO: Re-enable once the emulator component is integrated with the risk engine.
// Dependencies: Order emulation infrastructure in execution engine
#[ignore = "Waiting on emulator implementation"]
#[rstest]
fn test_submit_order_for_emulation_sends_command_to_emulator() {}

// MODIFY ORDER TESTS
#[rstest]
fn test_modify_order_when_no_order_found_logs_error(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);
}

#[expect(
    clippy::float_cmp,
    reason = "throttler usage is an integer counter represented as f64"
)]
#[rstest]
fn test_modify_order_beyond_rate_limit_then_rejects(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let mut order = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(1.0001, 4))
        .build();

    order
        .apply(OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();

    simple_cache
        .add_order(order, None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    for i in 0..11 {
        let modify_order = ModifyOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            client_order_id,
            Some(venue_order_id),
            Some(Quantity::from_str("100").unwrap()),
            Some(Price::new(1.00011 + f64::from(i) * 0.00001, 5)),
            None,
            UUID4::new(),
            risk_engine.clock().borrow().timestamp_ns(),
            None,
            None, // correlation_id
        );

        risk_engine.execute(TradingCommand::ModifyOrder(modify_order));
    }

    assert_eq!(risk_engine.throttled_modify_order.used(), 1.0);

    // Get messages and test
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 6);
    let first_message = saved_process_messages.first().unwrap();
    assert_eq!(first_message.event_type(), OrderEventType::ModifyRejected);
    assert_eq!(
        first_message.message().unwrap(),
        Ustr::from("Exceeded MAX_ORDER_MODIFY_RATE")
    );
    assert_eq!(first_message.account_id(), Some(account_id()));
}

#[rstest]
fn test_modify_order_with_default_settings_then_sends_to_client(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();
    simple_cache.add_quote(quote_audusd).unwrap();

    let order = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(1.0001, 4))
        .build();

    simple_cache
        .add_order(order.clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        Some(Quantity::from_str("100").unwrap()),
        Some(Price::new(1.00011, 5)),
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 2);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_modify_order_when_reducing_would_cross_flat_then_rejects(
    #[case] bypass: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("1.000"),
        "P-MODIFY-REDUCING",
    );
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.500"))
        .price(Price::from("3000.00"))
        .build();
    simple_cache
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Accepted(order_accepted(
            &order,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(simple_cache))),
        None,
        None,
        bypass,
    );
    risk_engine.set_trading_state(TradingState::Reducing);
    let modify = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        Some(venue_order_id),
        Some(Quantity::from("1.001")),
        Some(Price::from("3000.00")),
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrder(modify));

    let rejected = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].event_type(), OrderEventType::ModifyRejected);
    assert!(rejected[0].message().unwrap().contains("REDUCING"));
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[rstest]
fn test_modify_order_when_reducing_and_flat_then_rejects(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.500"))
        .price(Price::from("3000.00"))
        .build();
    simple_cache
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Accepted(order_accepted(
            &order,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_trading_state(TradingState::Reducing);
    let modify = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        Some(venue_order_id),
        Some(Quantity::from("0.250")),
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrder(modify));

    let rejected = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].event_type(), OrderEventType::ModifyRejected);
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[rstest]
fn test_modify_order_when_reducing_other_pending_consumes_position_then_rejects(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("1.000"),
        "P-MODIFY-PENDING",
    );
    let candidate = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-MODIFY-CANDIDATE"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.250"))
        .price(Price::from("3000.00"))
        .build();
    let pending = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-MODIFY-PENDING"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.750"))
        .price(Price::from("3001.00"))
        .build();
    for (order, venue_id) in [
        (&candidate, venue_order_id),
        (&pending, VenueOrderId::from("V-MODIFY-PENDING")),
    ] {
        simple_cache
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();
        simple_cache
            .update_order(&OrderEventAny::Submitted(order_submitted(order)))
            .unwrap();
        simple_cache
            .update_order(&OrderEventAny::Accepted(order_accepted(
                order,
                Some(venue_id),
                None,
            )))
            .unwrap();
    }

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_trading_state(TradingState::Reducing);
    let modify = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        candidate.client_order_id(),
        Some(venue_order_id),
        Some(Quantity::from("0.500")),
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrder(modify));

    let rejected = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].event_type(), OrderEventType::ModifyRejected);
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[expect(
    clippy::float_cmp,
    reason = "throttler usage is an integer counter represented as f64"
)]
#[rstest]
fn test_batch_modify_orders_counts_each_child_against_rate_limit(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let order1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-BATCH-MODIFY-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::new(1.0001, 4))
        .build();
    let order2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-BATCH-MODIFY-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("200").unwrap())
        .price(Price::new(1.0002, 4))
        .build();

    simple_cache
        .add_order(order1.clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(order2.clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let ts_init = risk_engine.clock().borrow().timestamp_ns();
    let modifies = vec![
        ModifyOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order1.client_order_id(),
            order1.venue_order_id(),
            Some(Quantity::from_str("101").unwrap()),
            Some(Price::new(1.0003, 4)),
            None,
            UUID4::new(),
            ts_init,
            None,
            None, // correlation_id
        ),
        ModifyOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order2.client_order_id(),
            order2.venue_order_id(),
            Some(Quantity::from_str("201").unwrap()),
            Some(Price::new(1.0004, 4)),
            None,
            UUID4::new(),
            ts_init,
            None,
            None, // correlation_id
        ),
    ];
    let command = BatchModifyOrders::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        modifies,
        UUID4::new(),
        ts_init,
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrders(command));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert!(matches!(
        saved_execute_messages.first(),
        Some(TradingCommand::ModifyOrders(command)) if command.modifies.len() == 2
    ));
    assert_eq!(risk_engine.throttled_modify_order.recv_count(), 2);
    assert_eq!(risk_engine.throttled_modify_order.sent_count(), 2);
    assert_eq!(risk_engine.throttled_modify_order.used(), 0.4);
}

#[rstest]
fn test_batch_modify_orders_rejects_all_children_when_one_child_fails_validation(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let order1 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-BATCH-MODIFY-PARTIAL-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::from("1.00010"))
        .build();
    let order2 = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-BATCH-MODIFY-PARTIAL-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("200").unwrap())
        .price(Price::from("1.00020"))
        .build();

    simple_cache
        .add_order(order1.clone(), None, Some(client_id_binance), true)
        .unwrap();
    simple_cache
        .add_order(order2.clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let ts_init = risk_engine.clock().borrow().timestamp_ns();
    let modifies = vec![
        ModifyOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order1.client_order_id(),
            order1.venue_order_id(),
            Some(Quantity::from_str("101").unwrap()),
            Some(Price::from("1.00030")),
            None,
            UUID4::new(),
            ts_init,
            None,
            None, // correlation_id
        ),
        ModifyOrder::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            instrument_audusd.id(),
            order2.client_order_id(),
            order2.venue_order_id(),
            Some(Quantity::from_str("201").unwrap()),
            Some(Price::from("1.000001")),
            None,
            UUID4::new(),
            ts_init,
            None,
            None, // correlation_id
        ),
    ];
    let command = BatchModifyOrders::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        modifies,
        UUID4::new(),
        ts_init,
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrders(command));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_process_messages.len(), 2);
    assert!(
        saved_process_messages
            .iter()
            .all(|event| { event.event_type() == OrderEventType::ModifyRejected })
    );
    assert!(saved_process_messages.iter().any(|event| {
        event.client_order_id() == order1.client_order_id()
            && event
                .message()
                .unwrap()
                .contains("one or more child modifications failed validation")
    }));
    assert!(
        saved_process_messages
            .iter()
            .any(|event| { event.client_order_id() == order2.client_order_id() })
    );
    assert_eq!(saved_execute_messages.len(), 0);
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_batch_modify_orders_when_reducing_accumulate_position_budget(
    #[case] bypass: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("1.000"),
        "P-BATCH-REDUCING",
    );
    let orders = [
        OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_eth_usdt.id())
            .client_order_id(ClientOrderId::from("O-BATCH-REDUCING-1"))
            .side(OrderSide::Sell)
            .quantity(Quantity::from("0.250"))
            .price(Price::from("3000.00"))
            .build(),
        OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_eth_usdt.id())
            .client_order_id(ClientOrderId::from("O-BATCH-REDUCING-2"))
            .side(OrderSide::Sell)
            .quantity(Quantity::from("0.250"))
            .price(Price::from("3001.00"))
            .build(),
    ];
    for (index, order) in orders.iter().enumerate() {
        simple_cache
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();
        simple_cache
            .update_order(&OrderEventAny::Submitted(order_submitted(order)))
            .unwrap();
        simple_cache
            .update_order(&OrderEventAny::Accepted(order_accepted(
                order,
                Some(VenueOrderId::new(format!("V-BATCH-REDUCING-{index}"))),
                None,
            )))
            .unwrap();
    }

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(simple_cache))),
        None,
        None,
        bypass,
    );
    risk_engine.set_trading_state(TradingState::Reducing);
    let ts_init = risk_engine.clock().borrow().timestamp_ns();
    let modifies = orders
        .iter()
        .map(|order| {
            ModifyOrder::new(
                trader_id,
                Some(client_id_binance),
                strategy_id_ema_cross,
                instrument_eth_usdt.id(),
                order.client_order_id(),
                order.venue_order_id(),
                Some(Quantity::from("0.600")),
                None,
                None,
                UUID4::new(),
                ts_init,
                None,
                None,
            )
        })
        .collect();
    let command = BatchModifyOrders::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        modifies,
        UUID4::new(),
        ts_init,
        None,
        None,
    );
    risk_engine.execute(TradingCommand::ModifyOrders(command));

    let rejected = get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(rejected.len(), 2);
    assert!(
        rejected
            .iter()
            .all(|event| event.event_type() == OrderEventType::ModifyRejected)
    );
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[rstest]
fn test_modify_order_when_negative_price_for_commodity_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_commodity: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_commodity.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_commodity,
            "1000000 USD",
            "0 USD",
            "1000000 USD",
            Some(Currency::USD()),
        )))
        .unwrap();

    simple_cache
        .add_quote(QuoteTick::new(
            instrument_commodity.id(),
            Price::from("5.00"),
            Price::from("5.01"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::default(),
            UnixNanos::default(),
        ))
        .unwrap();

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_commodity.id())
        .side(OrderSide::Buy)
        .price(Price::new(5.0, 2))
        .quantity(Quantity::from("1"))
        .build();

    simple_cache
        .add_order(order.clone(), None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_commodity.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_commodity.id(),
        order.client_order_id(),
        Some(venue_order_id),
        Some(Quantity::from("1")),
        Some(Price::new(-5.0, 2)), // Negative price is valid for spot commodities
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 2);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_commodity.id()
    );
}

#[rstest]
fn test_modify_order_when_risk_bypassed_sends_to_execution_engine(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
) {
    let mut risk_engine = get_risk_engine(None, None, None, true);

    // Order intentionally not in the cache: bypass skips all validation
    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        Some(Quantity::from("1000")),
        Some(Price::new(-100.0, 0)),
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument_audusd.id()
    );
}

// Verify that modifications to emulated orders are routed to the emulator.
//
// This test should verify that when modifying an order that's being managed by
// the emulator, the modification command is sent to the emulator rather than
// directly to the venue.
//
// TODO: Re-enable once the emulator component is integrated with the risk engine.
// Dependencies: Order emulation infrastructure in execution engine
#[ignore = "Waiting on emulator implementation"]
#[rstest]
fn test_modify_order_for_emulated_order_then_sends_to_emulator() {}

#[rstest]
fn test_submit_order_when_betting_back_order_liability_within_free_balance_then_accepts(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    let gbp = Currency::GBP();
    let instrument = InstrumentAny::Betting(betting());
    let account_state = AccountState::new(
        AccountId::new("BETFAIR-001"),
        AccountType::Betting,
        vec![AccountBalance::new(
            Money::new(1_000.0, gbp),
            Money::zero(gbp),
            Money::new(1_000.0, gbp),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(gbp),
    );

    simple_cache.add_instrument(instrument.clone()).unwrap();

    simple_cache
        .add_account(AccountAny::Betting(BettingAccount::new(
            account_state,
            true,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .price(Price::from("1.25"))
        .quantity(Quantity::from_str("1000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(saved_process_messages.is_empty());
}

#[rstest]
fn test_submit_order_when_betting_back_order_liability_exceeds_free_balance_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    let gbp = Currency::GBP();
    let instrument = InstrumentAny::Betting(betting());
    let account_state = AccountState::new(
        AccountId::new("BETFAIR-002"),
        AccountType::Betting,
        vec![AccountBalance::new(
            Money::new(999.0, gbp),
            Money::zero(gbp),
            Money::new(999.0, gbp),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(gbp),
    );

    simple_cache.add_instrument(instrument.clone()).unwrap();
    simple_cache
        .add_account(AccountAny::Betting(BettingAccount::new(
            account_state,
            true,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .price(Price::from("10.0"))
        .quantity(Quantity::from_str("1000").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages
            .first()
            .unwrap()
            .message()
            .unwrap()
            .as_str()
            .contains("NOTIONAL_EXCEEDS_FREE_BALANCE")
    );
}

#[rstest]
fn test_submit_order_when_betting_sell_reduces_long_position_then_accepts(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    let gbp = Currency::GBP();
    let instrument = InstrumentAny::Betting(betting());

    // Account with only 10 GBP free (not enough for a new bet)
    let account_state = AccountState::new(
        AccountId::new("BETFAIR-001"),
        AccountType::Betting,
        vec![AccountBalance::new(
            Money::new(10.0, gbp),
            Money::zero(gbp),
            Money::new(10.0, gbp),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(gbp),
    );

    simple_cache.add_instrument(instrument.clone()).unwrap();
    let betting_account = BettingAccount::new(account_state, true);
    simple_cache
        .add_account(AccountAny::Betting(betting_account.clone()))
        .unwrap();

    // Create a long position via a filled Buy order
    let entry_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .build();

    let mut fill = order_filled(
        &entry_order,
        &instrument,
        None,
        Some(AccountId::new("BETFAIR-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("2.0")),
        None,
        Some(AccountAny::Betting(betting_account)),
        None,
    );
    fill.position_id = Some(PositionId::from("P-001"));
    let position = Position::new(&instrument, fill);
    assert_eq!(position.side, PositionSide::Long);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Sell 50 to reduce the 100-qty long position (position-reducing, skips balance check)
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .side(OrderSide::Sell)
        .price(Price::from("2.5"))
        .quantity(Quantity::from("50"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    // Position-reducing sell should NOT be denied despite low free balance
    assert!(saved_process_messages.is_empty());
}

#[rstest]
fn test_submit_order_for_less_than_cash_balance_with_spot_crypto_account(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    let instrument = InstrumentAny::CurrencyPair(CurrencyPair::new(
        InstrumentId::from("ADAUSDT-SPOT.BINANCE"),
        Symbol::from("ADAUSDT"),
        Currency::from("ADA"),
        Currency::USDT(),
        4,
        0,
        Price::from("0.0001"),
        Quantity::from("1"),
        None,
        None,
        None,
        Some(Quantity::from("1")),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UnixNanos::default(),
        UnixNanos::default(),
    ));
    let quote = QuoteTick::new(
        instrument.id(),
        Price::from("0.6109"),
        Price::from("0.6110"),
        Quantity::from("1000"),
        Quantity::from("1000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache.add_instrument(instrument.clone()).unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument,
            "10000 USDT",
            "0 USDT",
            "10000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("440").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
    assert_eq!(
        saved_execute_messages.first().unwrap().instrument_id(),
        instrument.id()
    );
}

// Verify that account balances are correctly updated with partial and full order fills.
//
// This test should verify that when orders are partially or fully filled, the
// account balance tracking reflects the correct values including:
// - Reserved margin/capital being released
// - Commission being deducted
// - Realized P&L being applied to account balance
//
// TODO: Re-enable once real-time account balance tracking is implemented.
// Dependencies: Account balance tracking in portfolio/risk engine integration
// Related: Real-time position valuation and margin calculations
#[ignore = "Waiting on account balance tracking implementation"]
#[rstest]
fn test_partial_fill_and_full_fill_account_balance_correct() {}

#[rstest]
fn test_submit_order_with_gtd_expire_time_already_passed(
    clock: TestClock,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_xbtusd_bitmex: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    bitmex_cash_account_state_multi: AccountState,
    mut simple_cache: Cache,
) {
    consume_fixture((process_order_event_handler, execute_order_event_handler));
    let quote = QuoteTick::new(
        instrument_xbtusd_bitmex.id(),
        Price::from("0.6109"),
        Price::from("0.6110"),
        Quantity::from("1000"),
        Quantity::from("1000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );

    simple_cache
        .add_instrument(instrument_xbtusd_bitmex.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            bitmex_cash_account_state_multi,
        )))
        .unwrap();

    simple_cache.add_quote(quote).unwrap();

    let cache = Rc::new(RefCell::new(simple_cache));

    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_xbtusd_bitmex.id())
        .side(OrderSide::Buy)
        .price(Price::from("100_000.0"))
        .quantity(Quantity::from_str("440").unwrap())
        .time_in_force(TimeInForce::Gtd)
        .expire_time(UnixNanos::from(1_000)) // <-- Set expire time in the past
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_xbtusd_bitmex.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        clock.timestamp_ns(),
        None, // correlation_id
    );

    clock.set_time(UnixNanos::from(2_000)); // <-- Set time to 2,000 nanos past epoch

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // TODO: Change command messages to not require owned orders
}

#[rstest]
fn test_submit_order_with_quote_quantity_skips_min_max_quantity_check(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    // Create a BTCUSDT spot instrument with max_quantity = 83 BTC
    let btc_usdt = InstrumentAny::CurrencyPair(CurrencyPair::new(
        InstrumentId::from("BTCUSDT-SPOT.BYBIT"),
        Symbol::from("BTCUSDT"),
        Currency::BTC(),
        Currency::USDT(),
        1,
        6,
        Price::from("0.1"),
        Quantity::from("0.000001"),
        Some(Quantity::from("1")),         // multiplier
        Some(Quantity::from("0.000001")),  // lot_size
        Some(Quantity::from("83")),        // max_quantity = 83 BTC
        Some(Quantity::from("0.000011")),  // min_quantity
        Some(Money::from("8000000 USDT")), // max_notional
        Some(Money::from("5 USDT")),       // min_notional
        None,
        None,
        Some(dec!(0.1)),      // margin_init
        Some(dec!(0.1)),      // margin_maint
        Some(dec!(-0.00005)), // maker_fee
        Some(dec!(0.00015)),  // taker_fee
        None,                 // tick_scheme
        None,                 // info
        UnixNanos::default(),
        UnixNanos::default(),
    ));

    simple_cache.add_instrument(btc_usdt.clone()).unwrap();

    // Create a cash account with USDT balance (not USD) to match the instrument
    let usdt_account_state = AccountState::new(
        AccountId::from("BYBIT-001"), // Match the venue from the instrument
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("1000000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000000 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        Some(Currency::USDT()),
    );

    simple_cache
        .add_account(AccountAny::Cash(cash_account(usdt_account_state)))
        .unwrap();

    // Add a quote tick at $100,000 per BTC
    // This means 100 USDT quote quantity = 0.001 BTC base quantity
    let quote = QuoteTick::new(
        btc_usdt.id(),
        Price::from("100000.0"), // ask
        Price::from("99999.9"),  // bid
        Quantity::from("1.0"),   // ask_size
        Quantity::from("1.0"),   // bid_size
        UnixNanos::from(0),
        UnixNanos::from(0),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Create a market order with quote_quantity = 100 USDT
    // This should convert to 0.001 BTC which is well below max_quantity of 83 BTC
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(btc_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100")) // 100 USDT
        .quote_quantity(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        btc_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // The order should be accepted (not denied)
    // If the bug exists, it would compare 100 > 83 and deny the order
    // With the fix, it converts 100 USDT -> 0.001 BTC, then checks 0.001 < 83 (passes)
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);

    // Should have 1 event (submitted to exec engine, not denied)
    assert_eq!(
        saved_process_messages.len(),
        0,
        "Order should not be denied"
    );
}

#[rstest]
fn test_submit_order_with_quote_quantity_does_not_deny_on_base_max_quantity(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    // Base-quantity bounds do not apply to quote-denominated orders, so a
    // converted base quantity that would exceed `max_quantity` must still pass.
    let btc_usdt = InstrumentAny::CurrencyPair(CurrencyPair::new(
        InstrumentId::from("BTCUSDT-SPOT.BYBIT"),
        Symbol::from("BTCUSDT"),
        Currency::BTC(),
        Currency::USDT(),
        1,
        6,
        Price::from("0.1"),
        Quantity::from("0.000001"),
        Some(Quantity::from("1")),        // multiplier
        Some(Quantity::from("0.000001")), // lot_size
        Some(Quantity::from("0.5")),      // max_quantity = 0.5 BTC
        Some(Quantity::from("0.000011")), // min_quantity
        Some(Money::from("8000000 USDT")),
        Some(Money::from("5 USDT")),
        None,
        None,
        Some(dec!(0.1)),
        Some(dec!(0.1)),
        Some(dec!(-0.00005)),
        Some(dec!(0.00015)),
        None,
        None, // info
        UnixNanos::default(),
        UnixNanos::default(),
    ));

    simple_cache.add_instrument(btc_usdt.clone()).unwrap();

    let usdt_account_state = AccountState::new(
        AccountId::from("BYBIT-001"),
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("1000000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000000 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        Some(Currency::USDT()),
    );

    simple_cache
        .add_account(AccountAny::Cash(cash_account(usdt_account_state)))
        .unwrap();

    // Quote at $100k/BTC: 100,000 USDT would convert to 1 BTC > max 0.5 BTC.
    let quote = QuoteTick::new(
        btc_usdt.id(),
        Price::from("100000.0"),
        Price::from("99999.9"),
        Quantity::from("1.0"),
        Quantity::from("1.0"),
        UnixNanos::from(0),
        UnixNanos::from(0),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(btc_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100000"))
        .quote_quantity(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        btc_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None, // params
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(
        saved_process_messages.len(),
        0,
        "Order should not be denied for quote-quantity base bounds"
    );
}

#[rstest]
fn test_submit_order_with_quote_quantity_does_not_deny_on_base_min_quantity(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    // Mirrors the Polymarket scenario from #3874: a quote-denominated order whose
    // converted base quantity falls below a large `min_quantity` must still pass.
    let btc_usdt = InstrumentAny::CurrencyPair(CurrencyPair::new(
        InstrumentId::from("BTCUSDT-SPOT.BYBIT"),
        Symbol::from("BTCUSDT"),
        Currency::BTC(),
        Currency::USDT(),
        1,
        6,
        Price::from("0.1"),
        Quantity::from("0.000001"),
        Some(Quantity::from("1")),
        Some(Quantity::from("0.000001")),
        None,                      // max_quantity
        Some(Quantity::from("5")), // min_quantity = 5 base units
        None,                      // max_notional
        Some(Money::from("1 USDT")),
        None,
        None,
        Some(dec!(0.1)),
        Some(dec!(0.1)),
        Some(dec!(-0.00005)),
        Some(dec!(0.00015)),
        None,
        None,
        UnixNanos::default(),
        UnixNanos::default(),
    ));

    simple_cache.add_instrument(btc_usdt.clone()).unwrap();

    let usdt_account_state = AccountState::new(
        AccountId::from("BYBIT-001"),
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("1000000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000000 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        Some(Currency::USDT()),
    );

    simple_cache
        .add_account(AccountAny::Cash(cash_account(usdt_account_state)))
        .unwrap();

    // Quote at $100k/BTC: 10 USDT -> 0.0001 BTC, well below min_quantity of 5.
    let quote = QuoteTick::new(
        btc_usdt.id(),
        Price::from("100000.0"),
        Price::from("99999.9"),
        Quantity::from("1.0"),
        Quantity::from("1.0"),
        UnixNanos::from(0),
        UnixNanos::from(0),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(btc_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10"))
        .quote_quantity(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        btc_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(
        saved_process_messages.len(),
        0,
        "Order should not be denied for quote-quantity base bounds"
    );
}

#[rstest]
fn test_submit_order_with_quote_quantity_still_enforces_min_notional(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    // Base-quantity bounds are skipped for quote-denominated orders, but
    // `min_notional` still applies and must deny sub-minimum notionals.
    let btc_usdt = InstrumentAny::CurrencyPair(CurrencyPair::new(
        InstrumentId::from("BTCUSDT-SPOT.BYBIT"),
        Symbol::from("BTCUSDT"),
        Currency::BTC(),
        Currency::USDT(),
        1,
        6,
        Price::from("0.1"),
        Quantity::from("0.000001"),
        Some(Quantity::from("1")),
        Some(Quantity::from("0.000001")),
        None, // max_quantity
        None, // min_quantity
        None, // max_notional
        Some(Money::from("10 USDT")),
        None,
        None,
        Some(dec!(0.1)),
        Some(dec!(0.1)),
        Some(dec!(-0.00005)),
        Some(dec!(0.00015)),
        None,
        None,
        UnixNanos::default(),
        UnixNanos::default(),
    ));

    simple_cache.add_instrument(btc_usdt.clone()).unwrap();

    let usdt_account_state = AccountState::new(
        AccountId::from("BYBIT-001"),
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("1000000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000000 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        Some(Currency::USDT()),
    );

    simple_cache
        .add_account(AccountAny::Cash(cash_account(usdt_account_state)))
        .unwrap();

    let quote = QuoteTick::new(
        btc_usdt.id(),
        Price::from("100000.0"),
        Price::from("99999.9"),
        Quantity::from("1.0"),
        Quantity::from("1.0"),
        UnixNanos::from(0),
        UnixNanos::from(0),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // 1 USDT quote quantity, below the 10 USDT minimum notional.
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(btc_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1"))
        .quote_quantity(true)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        btc_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages.first().unwrap().event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages
            .first()
            .unwrap()
            .message()
            .unwrap()
            .contains("NOTIONAL_BELOW_MINIMUM")
    );
}

#[expect(
    clippy::float_cmp,
    reason = "throttler usage is an integer counter represented as f64"
)]
#[rstest]
fn test_submit_order_list_beyond_rate_limit_then_denies_all_orders(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    // Rate limit of 10 submissions per interval
    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Submit 10 order lists to fill the rate limit
    for i in 0..10 {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_audusd.id())
            .client_order_id(ClientOrderId::new(format!("O-{i}")))
            .side(OrderSide::Buy)
            .price(Price::new(1.0, 0))
            .quantity(Quantity::from_str("100").unwrap())
            .build();

        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();

        let order_list = OrderList::new(
            OrderListId::new(format!("OL-{i}")),
            instrument_audusd.id(),
            strategy_id_ema_cross,
            vec![order.client_order_id()],
            risk_engine.clock().borrow().timestamp_ns(),
        );

        let submit_order_list = SubmitOrderList::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            order_list,
            vec![order.init_event().clone()],
            None,
            None,
            None,
            UUID4::new(),
            risk_engine.clock().borrow().timestamp_ns(),
            None, // correlation_id
        );

        risk_engine.execute(TradingCommand::SubmitOrderList(submit_order_list));
    }

    // The 11th order list should be throttled
    let throttled_order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::new("O-THROTTLED"))
        .side(OrderSide::Buy)
        .price(Price::new(1.0, 0))
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(
            throttled_order.clone(),
            None,
            Some(client_id_binance),
            false,
        )
        .unwrap();

    let throttled_list = OrderList::new(
        OrderListId::new("OL-THROTTLED"),
        instrument_audusd.id(),
        strategy_id_ema_cross,
        vec![throttled_order.client_order_id()],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_throttled = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        throttled_list,
        vec![throttled_order.init_event().clone()],
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_throttled));

    assert_eq!(risk_engine.throttled_submit.used(), 1.0);

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    let first_message = saved_process_messages.first().unwrap();
    assert_eq!(first_message.event_type(), OrderEventType::Denied);
    assert_eq!(
        first_message.message().unwrap(),
        Ustr::from("RATE_LIMIT_EXCEEDED")
    );
}

#[rstest]
fn test_submit_order_list_beyond_rate_limit_denies_all_orders_in_list(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Fill rate limit with 10 single-order lists
    for i in 0..10 {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_audusd.id())
            .client_order_id(ClientOrderId::new(format!("O-{i}")))
            .side(OrderSide::Buy)
            .price(Price::new(1.0, 0))
            .quantity(Quantity::from_str("100").unwrap())
            .build();

        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), false)
            .unwrap();

        let order_list = OrderList::new(
            OrderListId::new(format!("OL-{i}")),
            instrument_audusd.id(),
            strategy_id_ema_cross,
            vec![order.client_order_id()],
            risk_engine.clock().borrow().timestamp_ns(),
        );

        let submit = SubmitOrderList::new(
            trader_id,
            Some(client_id_binance),
            strategy_id_ema_cross,
            order_list,
            vec![order.init_event().clone()],
            None,
            None,
            None,
            UUID4::new(),
            risk_engine.clock().borrow().timestamp_ns(),
            None, // correlation_id
        );

        risk_engine.execute(TradingCommand::SubmitOrderList(submit));
    }

    // Submit a bracket (3 orders) beyond the limit
    let entry = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-ENTRY"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-SL"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(0.9, 1))
        .build();

    let take_profit = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-TP"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .price(Price::new(1.1, 1))
        .build();

    let orders = [entry, stop_loss, take_profit];
    for order in &orders {
        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let bracket = OrderList::new(
        OrderListId::new("OL-BRACKET"),
        instrument_audusd.id(),
        strategy_id_ema_cross,
        orders.iter().map(Order::client_order_id).collect(),
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit_bracket = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        bracket,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit_bracket));

    // All 3 orders in the bracket should be denied
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 3);

    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
        assert_eq!(event.message().unwrap(), Ustr::from("RATE_LIMIT_EXCEEDED"));
    }
}

#[rstest]
fn test_set_trading_state_publishes_trading_state_changed_event() {
    let config = RiskEngineConfig {
        debug: true,
        bypass: false,
        max_order_submit: RateLimit::new(100, 1_000_000_000),
        max_order_modify: RateLimit::new(50, 1_000_000_000),
        max_notional_per_order: AHashMap::new(),
        max_market_data_age_ns: None,
        market_order_price_bound_bps: None,
    };

    let mut risk_engine = get_risk_engine(None, Some(config), None, false);
    risk_engine.set_max_notional_per_order(
        InstrumentId::from("AUD/USD.SIM"),
        Decimal::from_i64(500_000).unwrap(),
    );

    let handler = msgbus::stubs::get_message_saving_handler::<TradingStateChanged>(None);
    msgbus::subscribe_any("events.risk".into(), handler.clone(), None);

    risk_engine.set_trading_state(TradingState::Halted);

    let events = msgbus::stubs::get_saved_messages::<TradingStateChanged>(&handler);
    assert_eq!(events.len(), 1);

    let event = &events[0];
    assert_eq!(event.state, TradingState::Halted);
    assert_eq!(event.config["bypass"], "false");
    assert_eq!(event.config["max_order_submit_rate"], "100/00:00:01");
    assert_eq!(event.config["max_order_modify_rate"], "50/00:00:01");
    assert_eq!(event.config["debug"], "true");
    assert_eq!(event.config["max_notional_per_order.AUD/USD.SIM"], "500000");
}

#[rstest]
fn test_set_trading_state_from_halted_to_reducing() {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    risk_engine.set_trading_state(TradingState::Halted);
    assert_eq!(risk_engine.trading_state(), TradingState::Halted);

    risk_engine.set_trading_state(TradingState::Reducing);
    assert_eq!(risk_engine.trading_state(), TradingState::Reducing);
}

#[rstest]
fn test_set_trading_state_from_reducing_to_active() {
    let mut risk_engine = get_risk_engine(None, None, None, false);

    risk_engine.set_trading_state(TradingState::Reducing);
    assert_eq!(risk_engine.trading_state(), TradingState::Reducing);

    risk_engine.set_trading_state(TradingState::Active);
    assert_eq!(risk_engine.trading_state(), TradingState::Active);
}

#[rstest]
fn test_reset_restores_trading_state_and_config_notionals() {
    let instrument_id = InstrumentId::from("AUD/USD.SIM");
    let config_notional = Decimal::from_i64(50000).unwrap();

    let mut config_notionals = AHashMap::new();
    config_notionals.insert(instrument_id, config_notional);

    let config = RiskEngineConfig {
        debug: true,
        bypass: false,
        max_order_submit: RateLimit::new(10, 1000),
        max_order_modify: RateLimit::new(5, 1000),
        max_notional_per_order: config_notionals,
        max_market_data_age_ns: None,
        market_order_price_bound_bps: None,
    };

    let mut risk_engine = get_risk_engine(None, Some(config), None, false);

    risk_engine.set_trading_state(TradingState::Halted);
    risk_engine.set_max_notional_per_order(instrument_id, Decimal::from_i64(100_000).unwrap());

    risk_engine.reset();

    assert_eq!(risk_engine.trading_state(), TradingState::Active);
    assert_eq!(
        risk_engine.max_notional_per_order().get(&instrument_id),
        Some(&config_notional),
    );
}

#[rstest]
fn test_submit_order_list_within_rate_limit_passes_through(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    quote_audusd: QuoteTick,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let entry = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Buy)
        .price(Price::new(1.0, 0))
        .quantity(Quantity::from_str("100").unwrap())
        .build();

    let stop_loss = OrderTestBuilder::new(OrderType::StopMarket)
        .instrument_id(instrument_audusd.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_str("100").unwrap())
        .trigger_price(Price::new(0.9, 1))
        .build();

    let orders = [entry, stop_loss];
    for order in &orders {
        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let order_list = OrderList::new(
        OrderListId::new("OL-001"),
        instrument_audusd.id(),
        strategy_id_ema_cross,
        orders.iter().map(Order::client_order_id).collect(),
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    // No orders should be denied
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    // Order list should pass through to execution
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

fn margin_account_with_usdt_balance(total: &str, locked: &str, free: &str) -> MarginAccount {
    let state = AccountState::new(
        AccountId::from("BINANCE-001"),
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from(total),
            Money::from(locked),
            Money::from(free),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        Some(Currency::USDT()),
    );
    MarginAccount::new(state, true)
}

#[rstest]
#[case::unheld(Some("0 ETH"), "1.000", false, false, Some("0 ETH"), Some("1 ETH"))]
#[case::held_within_balance(Some("2 ETH"), "1.000", false, false, None, None)]
#[case::held_exceeding_balance(Some("2 ETH"), "3.000", false, false, Some("2 ETH"), Some("3 ETH"))]
#[case::borrowing_unheld(Some("0 ETH"), "1.000", true, false, None, None)]
#[case::reduce_only_unheld(Some("0 ETH"), "1.000", false, true, Some("0 ETH"), Some("1 ETH"))]
fn test_submit_order_cash_account_sell_checks_asset_balance(
    #[case] asset_balance: Option<&str>,
    #[case] quantity: &str,
    #[case] allow_borrowing: bool,
    #[case] reduce_only: bool,
    #[case] expected_free: Option<&str>,
    #[case] expected_cum_notional: Option<&str>,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    let mut balances = vec![AccountBalance::new(
        Money::from("10000 USDT"),
        Money::from("0 USDT"),
        Money::from("10000 USDT"),
    )];

    if let Some(asset_balance) = asset_balance {
        let balance = Money::from(asset_balance);
        balances.push(AccountBalance::new(
            balance,
            Money::zero(balance.currency),
            balance,
        ));
    }
    let account_state = AccountState::new(
        AccountId::from("BINANCE-001"),
        AccountType::Cash,
        balances,
        vec![],
        true,
        UUID4::new(),
        UnixNanos::from(0),
        UnixNanos::from(0),
        None,
    );
    simple_cache
        .add_account(AccountAny::Cash(CashAccount::new(
            account_state,
            true,
            allow_borrowing,
        )))
        .unwrap();
    simple_cache
        .add_quote(QuoteTick::new(
            instrument_eth_usdt.id(),
            Price::from("10000.00"),
            Price::from("10001.00"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        ))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from(quantity))
        .reduce_only(reduce_only)
        .build();
    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();
    let ts_init = risk_engine.clock().borrow().timestamp_ns();
    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        ts_init,
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let process_messages = get_process_order_event_handler_messages(&process_order_event_handler);
    let execute_messages = get_execute_order_event_handler_messages(&execute_order_event_handler);

    match (expected_free, expected_cum_notional) {
        (Some(expected_free), Some(expected_cum_notional)) => {
            assert_eq!(process_messages.len(), 1);
            assert_eq!(process_messages[0].event_type(), OrderEventType::Denied);
            assert_eq!(
                process_messages[0].message().unwrap(),
                Ustr::from(
                    &OrderDeniedReason::CumNotionalExceedsFreeBalance {
                        free: Money::from(expected_free),
                        cum_notional: Money::from(expected_cum_notional),
                    }
                    .to_string()
                )
            );
            assert_eq!(execute_messages.len(), 0);
        }
        (None, None) => {
            assert_eq!(process_messages.len(), 0);
            assert_eq!(execute_messages.len(), 1);
            assert_eq!(
                execute_messages[0].instrument_id(),
                instrument_eth_usdt.id()
            );
        }
        _ => unreachable!(),
    }
}

#[rstest]
#[case::buy(OrderSide::Buy)]
#[case::sell(OrderSide::Sell)]
fn test_submit_order_margin_account_within_free_balance(
    #[case] order_side: OrderSide,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // ETHUSDT margin_init=1.0, 10x leverage: 1 ETH @ $3000 requires $300 margin
    let mut margin_acct = margin_account_with_usdt_balance("100000 USDT", "0 USDT", "100000 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(order_side)
        .quantity(Quantity::from("1.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0); // No denial

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1); // Passed through
}

#[rstest]
fn test_submit_order_margin_account_buy_exceeds_free_balance(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // ETHUSDT margin_init=1.0, 10x leverage: margin = notional / 10
    // Buy 100 ETH @ $3000 -> notional = $300,000 -> margin = $30,000
    // Free balance = $20,000 -> denied
    let mut margin_acct = margin_account_with_usdt_balance("20000 USDT", "0 USDT", "20000 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert!(matches!(
        saved_process_messages[0].event_type(),
        OrderEventType::Denied
    ));
}

#[rstest]
fn test_submit_order_margin_account_sell_short_exceeds_free_balance(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Sell 100 ETH @ $3000 -> notional = $300,000 -> margin = $30,000
    // Free balance = $20,000 -> denied
    let mut margin_acct = margin_account_with_usdt_balance("20000 USDT", "0 USDT", "20000 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert!(matches!(
        saved_process_messages[0].event_type(),
        OrderEventType::Denied
    ));
}

#[rstest]
fn test_submit_order_margin_account_position_reducing_sell_passes(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Account with minimal free balance (can't afford new margin)
    let mut margin_acct = margin_account_with_usdt_balance("100 USDT", "0 USDT", "100 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create long position of 10 ETH via a fill
    let entry_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10.000"))
        .build();

    let mut fill = order_filled(
        &entry_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("BINANCE-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::from("P-001"));
    let position = Position::new(&instrument_eth_usdt, fill);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Sell 5 ETH to reduce position (within 10 ETH long)
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("5.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // Position-reducing sell passes despite insufficient free balance for new margin
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_order_margin_account_position_reducing_buy_passes(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Account with minimal free balance
    let mut margin_acct = margin_account_with_usdt_balance("100 USDT", "0 USDT", "100 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create short position of 10 ETH via a sell fill
    let entry_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("10.000"))
        .build();

    let mut fill = order_filled(
        &entry_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("BINANCE-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::from("P-002"));
    let position = Position::new(&instrument_eth_usdt, fill);
    assert_eq!(position.side, PositionSide::Short);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Buy 5 ETH to reduce short position (within 10 ETH short)
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("5.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    // Position-reducing buy passes despite insufficient free balance for new margin
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_order_list_margin_account_cum_margin_exceeds_free_balance(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Free = $500 USDT, 10x leverage
    // Each 1 ETH @ $3000 -> margin = $300
    // First order (1 ETH): cum_margin = $300 < $500 -> passes
    // Second order (1 ETH): cum_margin = $600 > $500 -> denied
    let mut margin_acct = margin_account_with_usdt_balance("500 USDT", "0 USDT", "500 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order1 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    let order2 = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-002"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    let orders = [order1, order2];
    for order in &orders {
        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let order_list = OrderList::new(
        OrderListId::new("OL-001"),
        instrument_eth_usdt.id(),
        strategy_id_ema_cross,
        orders.iter().map(Order::client_order_id).collect(),
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    // 1 denial from check_orders_risk (2nd order) + 2 from deny_order_list (both orders)
    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 3);
    for event in &saved_process_messages {
        assert_eq!(event.event_type(), OrderEventType::Denied);
    }
}

#[rstest]
fn test_submit_order_margin_account_limit_order_within_balance(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Limit buy 1 ETH @ $2500 -> notional = $2500 -> margin = $250 at 10x
    let mut margin_acct = margin_account_with_usdt_balance("1000 USDT", "0 USDT", "1000 USDT");
    margin_acct.set_default_leverage(dec!(10));
    simple_cache
        .add_account(AccountAny::Margin(margin_acct))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .price(Price::from("2500.00"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 0);

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_reduce_only_order_when_reducing_and_flat_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_trading_state(TradingState::Reducing);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.500"))
        .reduce_only(true)
        .build();
    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "TRADING_STATE_REDUCING",
    );
}

#[rstest]
fn test_submit_order_when_reducing_exceeds_position_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("1.000"),
        "P-REDUCING-LONG",
    );
    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_trading_state(TradingState::Reducing);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.001"))
        .build();
    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "TRADING_STATE_REDUCING",
    );
}

#[rstest]
fn test_submit_order_when_reducing_does_not_aggregate_positions_across_accounts(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position_for_account(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("0.500"),
        "P-REDUCING-ACCOUNT-A",
        AccountId::from("SIM-001"),
    );
    add_open_position_for_account(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("0.500"),
        "P-REDUCING-ACCOUNT-B",
        AccountId::from("SIM-002"),
    );
    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, true);
    risk_engine.set_trading_state(TradingState::Reducing);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.750"))
        .build();
    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "TRADING_STATE_REDUCING",
    );
}

#[rstest]
fn test_submit_order_when_reducing_pending_order_consumes_position_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_eth_usdt,
        OrderSide::Buy,
        Quantity::from("1.000"),
        "P-REDUCING-PENDING",
    );

    let pending = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-REDUCING-PENDING"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.750"))
        .price(Price::from("3000.00"))
        .build();
    simple_cache
        .add_order(pending.clone(), None, Some(client_id_binance), false)
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Submitted(order_submitted(&pending)))
        .unwrap();
    simple_cache
        .update_order(&OrderEventAny::Accepted(order_accepted(
            &pending,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);
    risk_engine.set_trading_state(TradingState::Reducing);

    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .client_order_id(ClientOrderId::from("O-REDUCING-CANDIDATE"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.500"))
        .build();
    execute_submit_for_test(
        &mut risk_engine,
        &order,
        trader_id,
        client_id_binance,
        strategy_id_ema_cross,
    );

    assert_denied_without_execution(
        &process_order_event_handler,
        &execute_order_event_handler,
        "TRADING_STATE_REDUCING",
    );
}

#[rstest]
fn test_submit_buy_when_reducing_and_net_long_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(execute_order_event_handler);
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "1000000 USDT",
            "0 USDT",
            "1000000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create a long position via a filled buy order
    let fill_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    let mut fill = order_filled(
        &fill_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("SIM-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::from("P-001"));
    let position = Position::new(&instrument_eth_usdt, fill);
    assert_eq!(position.side, PositionSide::Long);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let cache = Rc::new(RefCell::new(simple_cache));
    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);

    risk_engine.portfolio_mut().initialize_positions();
    risk_engine.set_trading_state(TradingState::Reducing);

    // Submit a buy order (increases long exposure) - should be denied
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("REDUCING")
    );
}

#[rstest]
fn test_submit_sell_when_reducing_and_net_short_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(execute_order_event_handler);
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "1000000 USDT",
            "0 USDT",
            "1000000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create a short position via a filled sell order
    let fill_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.000"))
        .build();

    let mut fill = order_filled(
        &fill_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("SIM-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::from("P-002"));
    let position = Position::new(&instrument_eth_usdt, fill);
    assert_eq!(position.side, PositionSide::Short);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let cache = Rc::new(RefCell::new(simple_cache));
    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);

    risk_engine.portfolio_mut().initialize_positions();
    risk_engine.set_trading_state(TradingState::Reducing);

    // Submit a sell order (increases short exposure) - should be denied
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("REDUCING")
    );
}

#[rstest]
fn test_submit_sell_when_reducing_and_net_long_then_allows(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "1000000 USDT",
            "0 USDT",
            "1000000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create a long position
    let fill_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    let mut fill = order_filled(
        &fill_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("SIM-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.position_id = Some(PositionId::from("P-003"));
    let position = Position::new(&instrument_eth_usdt, fill);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let cache = Rc::new(RefCell::new(simple_cache));
    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);

    risk_engine.portfolio_mut().initialize_positions();
    risk_engine.set_trading_state(TradingState::Reducing);

    // Submit a partial sell (strictly below the long position) - should pass.
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("0.500"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(
        saved_process_messages.is_empty(),
        "unexpected denial: {saved_process_messages:?}"
    );
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_order_list_reducing_uses_each_orders_own_instrument(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    audusd_sim: CurrencyPair,
    gbpusd_sim: CurrencyPair,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    let instrument_a: InstrumentAny = audusd_sim.into();
    let instrument_b: InstrumentAny = gbpusd_sim.into();

    simple_cache.add_instrument(instrument_a.clone()).unwrap();
    simple_cache.add_instrument(instrument_b.clone()).unwrap();
    add_open_position(
        &mut simple_cache,
        &instrument_a,
        OrderSide::Buy,
        Quantity::from("100"),
        "P-REDUCE-A",
    );
    add_open_position(
        &mut simple_cache,
        &instrument_b,
        OrderSide::Buy,
        Quantity::from("100"),
        "P-REDUCE-B",
    );

    let cache = Rc::new(RefCell::new(simple_cache));
    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);
    risk_engine.portfolio_mut().initialize_positions();
    risk_engine.set_trading_state(TradingState::Reducing);

    // Mixed-instrument lists receive an independent reduction budget per instrument. The first
    // A sell and the B sell fit, but the final A sell makes A's cumulative 110 > 100.
    let order_a = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_a.id())
        .client_order_id(ClientOrderId::from("O-REDUCE-001"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("60"))
        .build();
    let order_b = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_b.id())
        .client_order_id(ClientOrderId::from("O-REDUCE-002"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100"))
        .build();
    let order_a_overflow = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_a.id())
        .client_order_id(ClientOrderId::from("O-REDUCE-003"))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("50"))
        .build();

    let orders = [order_a.clone(), order_b.clone(), order_a_overflow.clone()];
    for order in &orders {
        risk_engine
            .cache()
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id_binance), true)
            .unwrap();
    }

    let order_list = OrderList::new(
        OrderListId::new("L-REDUCE-001"),
        instrument_a.id(),
        StrategyId::new("S-001"),
        vec![
            order_a.client_order_id(),
            order_b.client_order_id(),
            order_a_overflow.client_order_id(),
        ],
        risk_engine.clock().borrow().timestamp_ns(),
    );

    let submit = SubmitOrderList::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        order_list,
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
    );

    risk_engine.execute(TradingCommand::SubmitOrderList(submit));

    let saved = get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(
        !saved.is_empty(),
        "REDUCING should have produced denial events",
    );
    let denial_messages: Vec<String> = saved
        .iter()
        .filter(|e| e.event_type() == OrderEventType::Denied)
        .filter_map(|e| e.message().map(|m| m.as_str().to_string()))
        .collect();
    assert!(
        denial_messages
            .iter()
            .any(|m| m.contains(&instrument_a.id().to_string())),
        "expected cumulative denial reason to name instrument {}, found: {denial_messages:?}",
        instrument_a.id(),
    );
    assert!(
        !denial_messages
            .iter()
            .any(|m| m.contains(&instrument_b.id().to_string()) && m.contains("REDUCING")),
        "instrument_b should not appear in the cumulative REDUCING denial: {denial_messages:?}",
    );
    assert!(get_execute_order_event_handler_messages(&execute_order_event_handler).is_empty());
}

#[rstest]
fn test_submit_trailing_stop_market_buy_with_trigger_price_then_passes(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "1000000 USDT",
            "0 USDT",
            "1000000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Trailing stop buy with trigger_price and BidAsk trigger
    let order = OrderTestBuilder::new(OrderType::TrailingStopMarket)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .trigger_price(Price::from("3100.00"))
        .trailing_offset(dec!(100))
        .trailing_offset_type(TrailingOffsetType::Price)
        .trigger_type(TriggerType::BidAsk)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_trailing_stop_with_trigger_price_set_then_passes(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(process_order_event_handler);
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "1000000 USDT",
            "0 USDT",
            "1000000 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Trailing stop with trigger_price already set - skips calculation
    let order = OrderTestBuilder::new(OrderType::TrailingStopMarket)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.000"))
        .trigger_price(Price::from("2900.00"))
        .trailing_offset(dec!(100))
        .trailing_offset_type(TrailingOffsetType::Price)
        .trigger_type(TriggerType::BidAsk)
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}

#[rstest]
fn test_submit_order_with_zero_price_on_non_spread_instrument_then_denies(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_audusd: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    consume_fixture(execute_order_event_handler);
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd("1000000 USD", "0 USD", "1000000 USD"),
        )))
        .unwrap();

    simple_cache.add_quote(quote_audusd()).unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Limit order with price = 0 on a CurrencyPair (non-spread) - should be denied
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .price(Price::from("0.00000"))
        .quantity(Quantity::from("100"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::Denied
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("<= 0")
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_modify_order_when_trading_halted_then_rejects(
    #[case] bypass: bool,
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    // Create and accept a limit order so it has Accepted status
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00010"))
        .build();

    order
        .apply(OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();
    order
        .apply(OrderEventAny::Accepted(order_accepted(
            &order,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    simple_cache
        .add_order(order, None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine = get_risk_engine(
        Some(Rc::new(RefCell::new(simple_cache))),
        None,
        None,
        bypass,
    );

    risk_engine.set_trading_state(TradingState::Halted);

    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        Some(Quantity::from("200")),
        Some(Price::from("1.00020")),
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::ModifyRejected
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("HALTED")
    );

    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 0);
}

#[rstest]
fn test_modify_order_with_invalid_price_precision_then_rejects(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00010"))
        .build();

    order
        .apply(OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();
    order
        .apply(OrderEventAny::Accepted(order_accepted(
            &order,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    simple_cache
        .add_order(order, None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Modify with 6-dp price on a 5-dp instrument
    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        None,
        Some(Price::from("1.000001")),
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::ModifyRejected
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("precision")
    );
}

#[rstest]
fn test_modify_order_with_invalid_quantity_precision_then_rejects(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    client_order_id: ClientOrderId,
    instrument_audusd: InstrumentAny,
    venue_order_id: VenueOrderId,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    cash_account_state_million_usd: AccountState,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_audusd.clone())
        .unwrap();

    simple_cache
        .add_account(AccountAny::Cash(cash_account(
            cash_account_state_million_usd,
        )))
        .unwrap();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument_audusd.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("100"))
        .price(Price::from("1.00010"))
        .build();

    order
        .apply(OrderEventAny::Submitted(order_submitted(&order)))
        .unwrap();
    order
        .apply(OrderEventAny::Accepted(order_accepted(
            &order,
            Some(venue_order_id),
            None,
        )))
        .unwrap();

    simple_cache
        .add_order(order, None, Some(client_id_binance), true)
        .unwrap();

    let mut risk_engine =
        get_risk_engine(Some(Rc::new(RefCell::new(simple_cache))), None, None, false);

    // Modify with too-high quantity precision
    let modify_order = ModifyOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_audusd.id(),
        client_order_id,
        Some(venue_order_id),
        Some(Quantity::from("100.1")),
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None,
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::ModifyOrder(modify_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert_eq!(saved_process_messages.len(), 1);
    assert_eq!(
        saved_process_messages[0].event_type(),
        OrderEventType::ModifyRejected
    );
    assert!(
        saved_process_messages[0]
            .message()
            .unwrap()
            .contains("precision")
    );
}

#[rstest]
fn test_submit_sell_cash_account_with_long_position_reduces_then_passes(
    strategy_id_ema_cross: StrategyId,
    client_id_binance: ClientId,
    trader_id: TraderId,
    instrument_eth_usdt: InstrumentAny,
    process_order_event_handler: TypedIntoMessageSavingHandler<OrderEventAny>,
    execute_order_event_handler: TypedIntoMessageSavingHandler<TradingCommand>,
    mut simple_cache: Cache,
) {
    simple_cache
        .add_instrument(instrument_eth_usdt.clone())
        .unwrap();

    // Cash account with small free balance (not enough for a new buy)
    simple_cache
        .add_account(AccountAny::Cash(cash_account_for_venue(
            &instrument_eth_usdt,
            "100 USDT",
            "0 USDT",
            "100 USDT",
            Some(Currency::USDT()),
        )))
        .unwrap();

    let quote = QuoteTick::new(
        instrument_eth_usdt.id(),
        Price::from("3000.00"),
        Price::from("3000.01"),
        Quantity::from("100"),
        Quantity::from("100"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    simple_cache.add_quote(quote).unwrap();

    // Create a long position
    let fill_order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .build();

    let mut fill = order_filled(
        &fill_order,
        &instrument_eth_usdt,
        None,
        Some(AccountId::from("SIM-001")),
        Some(VenueOrderId::from("V-001")),
        None,
        None,
        Some(Price::from("3000.00")),
        None,
        None,
        None,
    );
    fill.account_id = AccountId::new(format!("{}-001", instrument_eth_usdt.id().venue));
    fill.position_id = Some(PositionId::from("P-004"));
    let position = Position::new(&instrument_eth_usdt, fill);
    assert_eq!(position.side, PositionSide::Long);

    simple_cache
        .add_position(&position, OmsType::Hedging)
        .unwrap();

    let cache = Rc::new(RefCell::new(simple_cache));
    let mut risk_engine = get_risk_engine(Some(cache), None, None, false);

    risk_engine.portfolio_mut().initialize_positions();

    // Sell 1 ETH (reduces long position) - should pass even with small balance
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_eth_usdt.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.000"))
        .build();

    risk_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id_binance), false)
        .unwrap();

    let submit_order = SubmitOrder::new(
        trader_id,
        Some(client_id_binance),
        strategy_id_ema_cross,
        instrument_eth_usdt.id(),
        order.client_order_id(),
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        risk_engine.clock().borrow().timestamp_ns(),
        None, // correlation_id
    );

    risk_engine.execute(TradingCommand::SubmitOrder(submit_order));

    let saved_process_messages =
        get_process_order_event_handler_messages(&process_order_event_handler);
    assert!(
        saved_process_messages.is_empty(),
        "unexpected denial: {saved_process_messages:?}"
    );
    let saved_execute_messages =
        get_execute_order_event_handler_messages(&execute_order_event_handler);
    assert_eq!(saved_execute_messages.len(), 1);
}
