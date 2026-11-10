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

//! Canonical, artifact-backed backtest execution environments.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use ahash::AHashMap;
use nautilus_model::{
    enums::{AccountType, BookType, CurrencyType, OmsType},
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money, Price},
};
use nautilus_provenance::{ArtifactId, ArtifactRef, ContentDigest};
use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor},
};

use crate::config::{BacktestEngineConfig, SimulatedVenueConfig};

/// The only environment schema accepted by the canonical JSON loader.
pub const BACKTEST_ENVIRONMENT_SCHEMA_VERSION: u32 = 3;

/// Stable process identity for the library-owned canonical environment loader.
pub const BACKTEST_ENVIRONMENT_LOADER_ID: &str = "nautilus-backtest/environment-canonical-json-v3";

/// Maximum accepted canonical environment artifact size (16 MiB).
pub const MAX_BACKTEST_ENVIRONMENT_BYTES: usize = 16 * 1024 * 1024;

const MAX_ENVIRONMENT_VENUES: usize = 256;
const MAX_ENVIRONMENT_INSTRUMENTS: usize = 100_000;
const MAX_ENVIRONMENT_CURRENCIES: usize = 1_024;
const MAX_VENUE_CONTEXTS: usize = 8;
const MAX_VENUE_STARTING_BALANCES: usize = 128;
const MAX_VENUE_INSTRUMENT_OVERRIDES: usize = MAX_ENVIRONMENT_INSTRUMENTS;
const MAX_CURRENCY_NAME_BYTES: usize = 128;

/// A market family whose semantics must be represented by the environment artifact.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketKind {
    /// Continuously traded centralized cryptocurrency markets.
    Crypto,
    /// Mainland China A-share cash equities.
    ChinaAShares,
    /// Hong Kong cash equities.
    HongKongEquities,
    /// United States cash equities.
    UnitedStatesEquities,
}

impl MarketKind {
    fn accepts(self, instrument: &InstrumentAny) -> bool {
        match self {
            // Schema v3's checked wire decoder only materializes crypto perpetuals. Expanding
            // this capability requires a schema bump and an equally strict decoder per variant.
            Self::Crypto => matches!(instrument, InstrumentAny::CryptoPerpetual(_)),
            Self::ChinaAShares | Self::HongKongEquities | Self::UnitedStatesEquities => {
                matches!(instrument, InstrumentAny::Equity(_))
            }
        }
    }

    fn required_contexts(self) -> &'static [MarketContextRole] {
        const EQUITY: &[MarketContextRole] = &[
            MarketContextRole::TradingCalendar,
            MarketContextRole::MarketRules,
            MarketContextRole::FeeSchedule,
            MarketContextRole::SettlementRules,
            MarketContextRole::Symbology,
            MarketContextRole::CorporateActions,
            MarketContextRole::ShortSaleRules,
            MarketContextRole::PriceLimits,
        ];

        match self {
            Self::Crypto => &[],
            Self::ChinaAShares | Self::HongKongEquities | Self::UnitedStatesEquities => EQUITY,
        }
    }

    fn accepts_venue(self, venue: Venue) -> bool {
        let venue = venue.to_string();
        match self {
            // Schema v3 is intentionally closed. Adding a crypto venue requires a schema review;
            // an unknown securities venue must never become crypto merely because it is absent
            // from an exclusion list. BINANCE remains explicit for the existing engine fixture.
            Self::Crypto => matches!(venue.as_str(), "OKX" | "BINANCE"),
            Self::ChinaAShares => matches!(venue.as_str(), "XSHG" | "XSHE"),
            Self::HongKongEquities => venue == "XHKG",
            Self::UnitedStatesEquities => {
                matches!(venue.as_str(), "XNAS" | "XNYS" | "XASE" | "ARCX" | "BATS")
            }
        }
    }
}

/// Versioned market profile selected by an environment venue.
///
/// Equity profiles in schema v3 describe structural requirements only. They are not executable
/// market-support claims. [`Self::GenericCryptoPerpetualV1`] deliberately represents generic
/// perpetual simulation and does not claim OKX-specific fees, funding, margin, or account rules.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketProfile {
    /// Generic crypto-perpetual simulation with no exchange-specific semantic applicator.
    GenericCryptoPerpetualV1,
    /// Structural contract for mainland China A-share cash equities.
    ChinaASharesSchemaV1,
    /// Structural contract for Hong Kong cash equities.
    HongKongEquitiesSchemaV1,
    /// Structural contract for United States cash equities.
    UnitedStatesEquitiesSchemaV1,
}

impl MarketProfile {
    const fn for_market(market: MarketKind) -> Self {
        match market {
            MarketKind::Crypto => Self::GenericCryptoPerpetualV1,
            MarketKind::ChinaAShares => Self::ChinaASharesSchemaV1,
            MarketKind::HongKongEquities => Self::HongKongEquitiesSchemaV1,
            MarketKind::UnitedStatesEquities => Self::UnitedStatesEquitiesSchemaV1,
        }
    }

    /// Returns the market family governed by this profile.
    #[must_use]
    pub const fn market(self) -> MarketKind {
        match self {
            Self::GenericCryptoPerpetualV1 => MarketKind::Crypto,
            Self::ChinaASharesSchemaV1 => MarketKind::ChinaAShares,
            Self::HongKongEquitiesSchemaV1 => MarketKind::HongKongEquities,
            Self::UnitedStatesEquitiesSchemaV1 => MarketKind::UnitedStatesEquities,
        }
    }

    /// Returns the library-owned stable identity used by provenance and promotion scopes.
    #[must_use]
    pub const fn stable_id(self) -> &'static str {
        match self {
            Self::GenericCryptoPerpetualV1 => "market-profile/generic-crypto-perpetual/v1",
            Self::ChinaASharesSchemaV1 => "market-profile/china-a-shares-schema/v1",
            Self::HongKongEquitiesSchemaV1 => "market-profile/hong-kong-equities-schema/v1",
            Self::UnitedStatesEquitiesSchemaV1 => "market-profile/united-states-equities-schema/v1",
        }
    }
}

/// Listing identity, kept separate from the route used to execute an order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ListingVenue {
    /// Canonical exchange or venue identifier carried by the instrument ID.
    Exchange(Venue),
}

impl ListingVenue {
    /// Returns the canonical listing venue identifier.
    #[must_use]
    pub const fn venue(self) -> Venue {
        match self {
            Self::Exchange(venue) => venue,
        }
    }

    /// Returns the library-owned stable identity used by provenance and promotion scopes.
    #[must_use]
    pub fn stable_id(self) -> String {
        match self {
            Self::Exchange(venue) => format!("listing/exchange/{venue}"),
        }
    }

    fn validate(self) -> anyhow::Result<()> {
        let venue = self.venue();
        anyhow::ensure!(
            venue != Venue::from("SMART"),
            "SMART is an execution router and must not be used as a listing venue"
        );
        Ok(())
    }
}

/// Typed execution route, independent of an instrument's listing identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionRoute {
    /// Orders are addressed directly to an exchange venue.
    DirectVenue { venue: Venue },
    /// Orders are sent to a broker-owned smart router.
    BrokerSmartRouter { broker: String },
    /// Mainland-listed shares are reached through a Stock Connect broker route.
    StockConnect { broker: String },
}

impl ExecutionRoute {
    /// Returns the library-owned stable identity used by provenance and promotion scopes.
    #[must_use]
    pub fn stable_id(&self) -> String {
        match self {
            Self::DirectVenue { venue } => format!("execution-route/direct-venue/{venue}"),
            Self::BrokerSmartRouter { broker } => {
                format!("execution-route/broker-smart-router/{broker}")
            }
            Self::StockConnect { broker } => {
                format!("execution-route/stock-connect/{broker}")
            }
        }
    }

    fn validate_for(&self, market: MarketKind, listing: ListingVenue) -> anyhow::Result<()> {
        match self {
            Self::DirectVenue { venue } => {
                anyhow::ensure!(
                    *venue != Venue::from("SMART"),
                    "SMART cannot be represented as a direct execution venue"
                );
                anyhow::ensure!(
                    market.accepts_venue(*venue),
                    "Direct execution venue {venue} is incompatible with declared market {market:?}"
                );
                if market != MarketKind::UnitedStatesEquities {
                    anyhow::ensure!(
                        *venue == listing.venue(),
                        "Direct execution for {market:?} must target its listing venue {}",
                        listing.venue()
                    );
                }
            }
            Self::BrokerSmartRouter { broker } => {
                validate_route_identity("smart-router broker", broker)?;
                anyhow::ensure!(
                    matches!(
                        market,
                        MarketKind::HongKongEquities | MarketKind::UnitedStatesEquities
                    ),
                    "BrokerSmartRouter is supported structurally only for Hong Kong or United States equities"
                );
            }
            Self::StockConnect { broker } => {
                validate_route_identity("Stock Connect broker", broker)?;
                anyhow::ensure!(
                    market == MarketKind::ChinaAShares,
                    "StockConnect is supported structurally only for mainland China A-shares"
                );
            }
        }
        Ok(())
    }
}

/// A market-semantic dependency which must be immutable and independently attestable.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketContextRole {
    /// Venue sessions, holidays, and special trading days.
    TradingCalendar,
    /// Order types, auctions, halts, and other venue microstructure rules.
    MarketRules,
    /// Commissions, taxes, levies, and rebates.
    FeeSchedule,
    /// Cash, asset, and position settlement rules.
    SettlementRules,
    /// Stable mapping between vendor, venue, and internal identifiers.
    Symbology,
    /// Splits, dividends, rights, mergers, and symbol changes.
    CorporateActions,
    /// Locate, borrow, uptick, and other short-sale restrictions.
    ShortSaleRules,
    /// Daily or board-specific price-limit rules.
    PriceLimits,
}

/// Exact semantic target governed by a market-context artifact.
///
/// Listing-market rules and execution-route rules are intentionally distinct: a US-listed equity
/// routed to another exchange, a broker smart router, or Stock Connect cannot reuse listing-only
/// evidence as proof that route-specific fees and controls were applied.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketContextTarget {
    /// Rules attached to the instrument's listing venue.
    ListingVenue(ListingVenue),
    /// Rules attached to the exact route used to execute orders.
    ExecutionRoute(ExecutionRoute),
}

/// Typed binding between a market-semantic role and its immutable artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketContextBinding {
    /// Role independently repeated inside the value to prevent map-value substitution.
    pub role: MarketContextRole,
    /// Market family whose semantics the artifact describes.
    pub market: MarketKind,
    /// Exact versioned market profile whose semantics the artifact describes.
    pub profile: MarketProfile,
    /// Exact venue whose semantics the artifact describes.
    pub venue: Venue,
    /// Listing or execution-route target governed by this artifact.
    pub target: MarketContextTarget,
    /// Source publication timestamp attested by the ingestion pipeline.
    pub published_at_ns: u64,
    /// Timestamp at which the authenticated pipeline observed the source.
    pub observed_at_ns: u64,
    /// Inclusive first nanosecond for which the artifact is effective.
    pub effective_from_ns: u64,
    /// Exclusive first nanosecond for which the artifact is no longer effective.
    pub effective_to_ns: u64,
    /// Immutable content-addressed semantic artifact.
    pub artifact: ArtifactRef,
}

impl MarketContextBinding {
    /// Creates a typed market-context binding.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "market identity, knowledge time, coverage, and artifact identity are all explicit security boundaries"
    )]
    pub const fn new(
        role: MarketContextRole,
        market: MarketKind,
        profile: MarketProfile,
        venue: Venue,
        published_at_ns: u64,
        observed_at_ns: u64,
        effective_from_ns: u64,
        effective_to_ns: u64,
        artifact: ArtifactRef,
    ) -> Self {
        Self {
            role,
            market,
            profile,
            venue,
            target: MarketContextTarget::ListingVenue(ListingVenue::Exchange(venue)),
            published_at_ns,
            observed_at_ns,
            effective_from_ns,
            effective_to_ns,
            artifact,
        }
    }

    /// Creates a binding for the exact execution route paired with a listing venue.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "market, route, point-in-time knowledge, coverage, and artifact identity are all security boundaries"
    )]
    pub fn new_for_execution_route(
        role: MarketContextRole,
        market: MarketKind,
        profile: MarketProfile,
        venue: Venue,
        route: ExecutionRoute,
        published_at_ns: u64,
        observed_at_ns: u64,
        effective_from_ns: u64,
        effective_to_ns: u64,
        artifact: ArtifactRef,
    ) -> Self {
        Self {
            role,
            market,
            profile,
            venue,
            target: MarketContextTarget::ExecutionRoute(route),
            published_at_ns,
            observed_at_ns,
            effective_from_ns,
            effective_to_ns,
            artifact,
        }
    }

    fn validate_for(
        &self,
        map_role: MarketContextRole,
        market: MarketKind,
        profile: MarketProfile,
        venue: Venue,
        target: &MarketContextTarget,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.role == map_role,
            "Market context map key {map_role:?} does not match bound role {:?}",
            self.role
        );
        anyhow::ensure!(
            self.market == market,
            "Market context {map_role:?} is bound to {:?}, not {market:?}",
            self.market
        );
        anyhow::ensure!(
            self.profile == profile,
            "Market context {map_role:?} is bound to {:?}, not {profile:?}",
            self.profile
        );
        anyhow::ensure!(
            self.venue == venue,
            "Market context {map_role:?} is bound to venue {}, not {venue}",
            self.venue
        );
        anyhow::ensure!(
            &self.target == target,
            "Market context {map_role:?} target {:?} does not match required target {target:?}",
            self.target
        );
        anyhow::ensure!(
            self.published_at_ns <= self.observed_at_ns,
            "Market context {map_role:?} on {venue} was observed before it was published"
        );
        anyhow::ensure!(
            self.effective_from_ns < self.effective_to_ns,
            "Market context {map_role:?} on {venue} has an empty or reversed effective range"
        );
        Ok(())
    }

    fn validate_knowledge_cutoff(&self, knowledge_cutoff_ns: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.observed_at_ns <= knowledge_cutoff_ns,
            "Market context {:?} on {} was observed at {} after knowledge cutoff {knowledge_cutoff_ns}",
            self.role,
            self.venue,
            self.observed_at_ns
        );
        Ok(())
    }

    fn validate_replay_coverage(
        &self,
        replay_start_ns: u64,
        replay_end_ns: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.effective_from_ns <= replay_start_ns && self.effective_to_ns >= replay_end_ns,
            "Market context {:?} on {} does not cover replay interval [{replay_start_ns}, {replay_end_ns})",
            self.role,
            self.venue
        );
        Ok(())
    }
}

/// FX quote side used for common-currency valuation.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValuationRateSide {
    Bid,
    Ask,
    Midpoint,
}

/// Whether cross rates may be formed through multiple deterministic legs.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValuationTriangulationPolicy {
    DirectOnly,
    DeterministicShortestPath,
}

/// Time cutoff applied when selecting an FX observation.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValuationCutoffPolicy {
    /// A quote must be no later than both replay time and the environment knowledge cutoff.
    ReplayAndKnowledgeCutoff,
}

/// Typed, immutable cross-currency valuation policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuationPolicyBinding {
    pub base_currency: Currency,
    pub rate_side: ValuationRateSide,
    pub max_quote_age_ns: u64,
    pub triangulation: ValuationTriangulationPolicy,
    pub cutoff: ValuationCutoffPolicy,
    /// Source publication timestamp attested by the valuation ingestion pipeline.
    pub published_at_ns: u64,
    /// Timestamp at which the authenticated pipeline observed the valuation source.
    pub observed_at_ns: u64,
    pub effective_from_ns: u64,
    pub effective_to_ns: u64,
    pub artifact: ArtifactRef,
}

impl ValuationPolicyBinding {
    /// Creates a typed valuation-policy binding.
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub const fn new(
        base_currency: Currency,
        rate_side: ValuationRateSide,
        max_quote_age_ns: u64,
        triangulation: ValuationTriangulationPolicy,
        cutoff: ValuationCutoffPolicy,
        published_at_ns: u64,
        observed_at_ns: u64,
        effective_from_ns: u64,
        effective_to_ns: u64,
        artifact: ArtifactRef,
    ) -> Self {
        Self {
            base_currency,
            rate_side,
            max_quote_age_ns,
            triangulation,
            cutoff,
            published_at_ns,
            observed_at_ns,
            effective_from_ns,
            effective_to_ns,
            artifact,
        }
    }

    fn validate(
        &self,
        replay_start_ns: u64,
        replay_end_ns: u64,
        knowledge_cutoff_ns: u64,
        currency_specs: &BTreeMap<&str, &CurrencySpec>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_quote_age_ns > 0,
            "Valuation policy max_quote_age_ns must be positive"
        );
        anyhow::ensure!(
            self.published_at_ns <= self.observed_at_ns,
            "Valuation policy was observed before it was published"
        );
        anyhow::ensure!(
            self.observed_at_ns <= knowledge_cutoff_ns,
            "Valuation policy was observed at {} after knowledge cutoff {knowledge_cutoff_ns}",
            self.observed_at_ns
        );
        anyhow::ensure!(
            self.effective_from_ns < self.effective_to_ns,
            "Valuation policy has an empty or reversed effective range"
        );
        anyhow::ensure!(
            self.effective_from_ns <= replay_start_ns && self.effective_to_ns >= replay_end_ns,
            "Valuation policy does not cover replay interval [{replay_start_ns}, {replay_end_ns})"
        );
        let spec = currency_specs
            .get(self.base_currency.code.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Valuation base currency {} has no bound definition",
                    self.base_currency.code
                )
            })?;
        anyhow::ensure!(
            spec.matches(self.base_currency),
            "Valuation base currency {} does not match its bound definition",
            self.base_currency.code
        );
        Ok(())
    }
}

/// Library-owned engine configuration profile.
///
/// Schema v3 intentionally exposes only one closed profile. This prevents an authenticated
/// artifact from selecting arbitrary trait-object factories which could ignore its bytes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BacktestEngineProfile {
    /// Exact [`BacktestEngineConfig::default`] semantics from the measured executable.
    SecureDefaultV1,
}

impl BacktestEngineProfile {
    fn into_config(self) -> BacktestEngineConfig {
        match self {
            Self::SecureDefaultV1 => BacktestEngineConfig::default(),
        }
    }
}

/// Library-owned fill, fee, margin, latency, and simulation-module profile.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationModelProfile {
    /// Built-in default fill and fee models, account-derived margin, no latency or modules.
    BuiltinDefaultV1,
}

/// Full currency semantics bound by the environment rather than inherited from the global map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrencySpec {
    pub code: String,
    pub precision: u8,
    pub iso4217: u16,
    pub name: String,
    pub currency_type: CurrencyType,
}

impl CurrencySpec {
    fn from_currency(currency: Currency) -> Self {
        Self {
            code: currency.code.to_string(),
            precision: currency.precision,
            iso4217: currency.iso4217,
            name: currency.name.to_string(),
            currency_type: currency.currency_type,
        }
    }

    fn validate(&self) -> anyhow::Result<Currency> {
        anyhow::ensure!(
            !self.code.is_empty()
                && self.code.len() <= 32
                && self.code.bytes().all(|byte| {
                    byte.is_ascii_uppercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')
                }),
            "Currency code '{}' is not canonical ASCII",
            self.code
        );
        anyhow::ensure!(
            !self.name.is_empty() && self.name.len() <= MAX_CURRENCY_NAME_BYTES,
            "Currency {} name must contain 1..={MAX_CURRENCY_NAME_BYTES} bytes",
            self.code
        );
        Ok(Currency::new_checked(
            &self.code,
            self.precision,
            self.iso4217,
            &self.name,
            self.currency_type,
        )?)
    }

    fn matches(&self, currency: Currency) -> bool {
        self.code == currency.code.as_str()
            && self.precision == currency.precision
            && self.iso4217 == currency.iso4217
            && self.name == currency.name.as_str()
            && self.currency_type == currency.currency_type
    }
}

/// Canonical venue configuration stored inside an environment artifact.
#[expect(
    clippy::struct_excessive_bools,
    reason = "every behavior-affecting simulated venue flag is explicit in the signed schema"
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestVenueSpec {
    /// Internal venue identifier.
    pub venue: Venue,
    /// Market family used for compatibility and context validation.
    pub market: MarketKind,
    /// Exact versioned market profile selected for this venue.
    pub profile: MarketProfile,
    /// Listing identity carried by instruments, distinct from execution routing.
    pub listing_venue: ListingVenue,
    /// Route used to send orders, distinct from listing identity.
    pub execution_route: ExecutionRoute,
    /// Market-semantic dependencies required by this venue.
    ///
    /// Schema v3 can describe equity dependencies, but the secure runtime loader refuses them
    /// until a library-owned applicator has decoded and installed their content.
    pub contexts: BTreeMap<MarketContextRole, MarketContextBinding>,
    /// Route-specific semantic dependencies when execution is not direct on the listing venue.
    ///
    /// These bindings prevent listing-market evidence from being reused for a broker router,
    /// Stock Connect, or a different US execution venue.
    pub execution_contexts: BTreeMap<MarketContextRole, MarketContextBinding>,
    /// Order-management mode.
    pub oms_type: OmsType,
    /// Account type.
    pub account_type: AccountType,
    /// Default order-book representation.
    pub book_type: BookType,
    /// Initial balances.
    pub starting_balances: Vec<Money>,
    /// Optional single account base currency.
    pub base_currency: Option<Currency>,
    /// Optional venue default leverage encoded as an exact decimal string.
    pub default_leverage: Option<String>,
    /// Instrument-specific leverage encoded as exact decimal strings.
    pub leverages: BTreeMap<InstrumentId, String>,
    /// Closed built-in simulation model profile.
    pub simulation_model: SimulationModelProfile,
    pub routing: bool,
    pub reject_stop_orders: bool,
    pub support_gtd_orders: bool,
    pub support_contingent_orders: bool,
    pub use_position_ids: bool,
    pub use_random_ids: bool,
    pub use_reduce_only: bool,
    pub use_message_queue: bool,
    pub use_market_order_acks: bool,
    pub bar_execution: bool,
    pub bar_adaptive_high_low_ordering: bool,
    pub trade_execution: bool,
    pub liquidity_consumption: bool,
    pub allow_cash_borrowing: bool,
    pub frozen_account: bool,
    pub queue_position: bool,
    pub oto_full_trigger: bool,
    pub price_protection_points: u32,
    /// Instrument settlement prices.
    pub settlement_prices: BTreeMap<InstrumentId, Price>,
    pub liquidation_enabled: bool,
    /// Positive finite ratio encoded as an exact string.
    pub liquidation_trigger_ratio: String,
    pub liquidation_cancel_open_orders: bool,
}

impl BacktestVenueSpec {
    /// Creates a venue spec with every flag set to the current built-in venue defaults.
    #[must_use]
    pub fn with_builtin_defaults(
        venue: Venue,
        market: MarketKind,
        contexts: BTreeMap<MarketContextRole, MarketContextBinding>,
        oms_type: OmsType,
        account_type: AccountType,
        book_type: BookType,
        mut starting_balances: Vec<Money>,
    ) -> Self {
        canonicalize_starting_balances(&mut starting_balances);
        Self {
            venue,
            market,
            profile: MarketProfile::for_market(market),
            listing_venue: ListingVenue::Exchange(venue),
            execution_route: ExecutionRoute::DirectVenue { venue },
            contexts,
            execution_contexts: BTreeMap::new(),
            oms_type,
            account_type,
            book_type,
            starting_balances,
            base_currency: None,
            default_leverage: None,
            leverages: BTreeMap::new(),
            simulation_model: SimulationModelProfile::BuiltinDefaultV1,
            routing: false,
            reject_stop_orders: true,
            support_gtd_orders: true,
            support_contingent_orders: true,
            use_position_ids: true,
            use_random_ids: false,
            use_reduce_only: true,
            use_message_queue: true,
            use_market_order_acks: false,
            bar_execution: true,
            bar_adaptive_high_low_ordering: false,
            trade_execution: true,
            liquidity_consumption: false,
            allow_cash_borrowing: false,
            frozen_account: false,
            queue_position: false,
            oto_full_trigger: false,
            price_protection_points: 0,
            settlement_prices: BTreeMap::new(),
            liquidation_enabled: false,
            liquidation_trigger_ratio: "1".to_string(),
            liquidation_cancel_open_orders: true,
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.listing_venue.validate()?;
        anyhow::ensure!(
            self.listing_venue.venue() == self.venue,
            "Environment venue {} does not match listing venue {}",
            self.venue,
            self.listing_venue.venue()
        );
        anyhow::ensure!(
            self.market.accepts_venue(self.listing_venue.venue()),
            "Venue {} is incompatible with declared market {:?}",
            self.listing_venue.venue(),
            self.market
        );
        anyhow::ensure!(
            self.profile.market() == self.market,
            "Market profile {:?} is incompatible with declared market {:?}",
            self.profile,
            self.market
        );
        self.execution_route
            .validate_for(self.market, self.listing_venue)?;
        if self.profile == MarketProfile::GenericCryptoPerpetualV1 {
            anyhow::ensure!(
                matches!(
                    &self.execution_route,
                    ExecutionRoute::DirectVenue { venue } if *venue == self.venue
                ),
                "GenericCryptoPerpetualV1 requires direct execution on its listing venue"
            );
        }
        anyhow::ensure!(
            !self.starting_balances.is_empty(),
            "Environment venue {} has no starting balance",
            self.venue
        );
        anyhow::ensure!(
            self.contexts.len() <= MAX_VENUE_CONTEXTS,
            "Environment venue {} exceeds the context limit of {MAX_VENUE_CONTEXTS}",
            self.venue
        );
        anyhow::ensure!(
            self.execution_contexts.len() <= MAX_VENUE_CONTEXTS,
            "Environment venue {} exceeds the execution-context limit of {MAX_VENUE_CONTEXTS}",
            self.venue
        );
        anyhow::ensure!(
            self.starting_balances.len() <= MAX_VENUE_STARTING_BALANCES,
            "Environment venue {} exceeds the starting balance limit of {MAX_VENUE_STARTING_BALANCES}",
            self.venue
        );
        anyhow::ensure!(
            self.leverages.len() <= MAX_VENUE_INSTRUMENT_OVERRIDES,
            "Environment venue {} exceeds the leverage override limit of {MAX_VENUE_INSTRUMENT_OVERRIDES}",
            self.venue
        );
        anyhow::ensure!(
            self.settlement_prices.len() <= MAX_VENUE_INSTRUMENT_OVERRIDES,
            "Environment venue {} exceeds the settlement price limit of {MAX_VENUE_INSTRUMENT_OVERRIDES}",
            self.venue
        );
        anyhow::ensure!(
            self.starting_balances
                .windows(2)
                .all(|pair| { pair[0].currency.code.as_str() < pair[1].currency.code.as_str() }),
            "Environment venue {} starting balances must have unique currency codes and be sorted canonically",
            self.venue
        );
        if let Some(base_currency) = self.base_currency {
            anyhow::ensure!(
                self.starting_balances.len() == 1
                    && self.starting_balances[0].currency.code == base_currency.code,
                "Environment venue {} base currency must match its single starting balance",
                self.venue
            );
        }
        let mut context_digests = BTreeSet::new();
        let listing_target = MarketContextTarget::ListingVenue(self.listing_venue);
        for (role, binding) in &self.contexts {
            binding.validate_for(
                *role,
                self.market,
                self.profile,
                self.venue,
                &listing_target,
            )?;
            anyhow::ensure!(
                context_digests.insert(*binding.artifact.digest()),
                "Environment venue {} reuses identical content for multiple market-context roles",
                self.venue
            );
        }
        let execution_target = MarketContextTarget::ExecutionRoute(self.execution_route.clone());
        for (role, binding) in &self.execution_contexts {
            binding.validate_for(
                *role,
                self.market,
                self.profile,
                self.venue,
                &execution_target,
            )?;
            anyhow::ensure!(
                context_digests.insert(*binding.artifact.digest()),
                "Environment venue {} reuses identical content across listing or execution-context roles",
                self.venue
            );
        }
        for role in self.market.required_contexts() {
            anyhow::ensure!(
                self.contexts.contains_key(role),
                "Environment venue {} ({:?}) is missing required {:?} context",
                self.venue,
                self.market,
                role
            );
        }
        let direct_on_listing = matches!(
            &self.execution_route,
            ExecutionRoute::DirectVenue { venue } if *venue == self.listing_venue.venue()
        );
        if direct_on_listing {
            anyhow::ensure!(
                self.execution_contexts.is_empty(),
                "Direct execution on listing venue {} must use the listing contexts instead of duplicating execution contexts",
                self.venue
            );
        } else {
            for role in self.market.required_contexts() {
                anyhow::ensure!(
                    self.execution_contexts.contains_key(role),
                    "Environment venue {} route {:?} is missing required {:?} execution context",
                    self.venue,
                    self.execution_route,
                    role
                );
            }
        }
        if self.market == MarketKind::Crypto {
            anyhow::ensure!(
                self.contexts.is_empty() && self.execution_contexts.is_empty(),
                "GenericCryptoPerpetualV1 must not claim unmaterialized external market contexts"
            );
        }
        anyhow::ensure!(
            !self.use_random_ids,
            "SecureDefaultV1 requires deterministic venue identifiers (use_random_ids=false)"
        );
        if let Some(value) = &self.default_leverage {
            parse_positive_decimal("default_leverage", value)?;
        }
        for (instrument_id, value) in &self.leverages {
            anyhow::ensure!(
                instrument_id.venue == self.venue,
                "Leverage instrument {instrument_id} does not belong to venue {}",
                self.venue
            );
            parse_positive_decimal("leverages", value)?;
        }
        for instrument_id in self.settlement_prices.keys() {
            anyhow::ensure!(
                instrument_id.venue == self.venue,
                "Settlement instrument {instrument_id} does not belong to venue {}",
                self.venue
            );
        }
        parse_positive_f64("liquidation_trigger_ratio", &self.liquidation_trigger_ratio)?;
        Ok(())
    }

    fn into_config(self) -> anyhow::Result<SimulatedVenueConfig> {
        self.validate()?;
        let default_leverage = self
            .default_leverage
            .as_deref()
            .map(|value| parse_positive_decimal("default_leverage", value))
            .transpose()?;
        let leverages = self
            .leverages
            .into_iter()
            .map(|(instrument_id, value)| {
                parse_positive_decimal("leverages", &value)
                    .map(|leverage| (instrument_id, leverage))
            })
            .collect::<anyhow::Result<AHashMap<_, _>>>()?;
        let liquidation_trigger_ratio =
            parse_positive_f64("liquidation_trigger_ratio", &self.liquidation_trigger_ratio)?;

        match self.simulation_model {
            SimulationModelProfile::BuiltinDefaultV1 => {}
        }

        Ok(SimulatedVenueConfig::builder()
            .venue(self.venue)
            .oms_type(self.oms_type)
            .account_type(self.account_type)
            .book_type(self.book_type)
            .starting_balances(self.starting_balances)
            .maybe_base_currency(self.base_currency)
            .maybe_default_leverage(default_leverage)
            .leverages(leverages)
            .routing(self.routing)
            .reject_stop_orders(self.reject_stop_orders)
            .support_gtd_orders(self.support_gtd_orders)
            .support_contingent_orders(self.support_contingent_orders)
            .use_position_ids(self.use_position_ids)
            .use_random_ids(self.use_random_ids)
            .use_reduce_only(self.use_reduce_only)
            .use_message_queue(self.use_message_queue)
            .use_market_order_acks(self.use_market_order_acks)
            .bar_execution(self.bar_execution)
            .bar_adaptive_high_low_ordering(self.bar_adaptive_high_low_ordering)
            .trade_execution(self.trade_execution)
            .liquidity_consumption(self.liquidity_consumption)
            .allow_cash_borrowing(self.allow_cash_borrowing)
            .frozen_account(self.frozen_account)
            .queue_position(self.queue_position)
            .oto_full_trigger(self.oto_full_trigger)
            .price_protection_points(self.price_protection_points)
            .settlement_prices(self.settlement_prices.into_iter().collect())
            .liquidation_enabled(self.liquidation_enabled)
            .liquidation_trigger_ratio(liquidation_trigger_ratio)
            .liquidation_cancel_open_orders(self.liquidation_cancel_open_orders)
            .build()?)
    }
}

/// Strict, versioned environment artifact schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestEnvironmentSpec {
    /// Must equal [`BACKTEST_ENVIRONMENT_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Closed engine profile.
    pub engine: BacktestEngineProfile,
    /// Inclusive first nanosecond replayed by this environment.
    pub replay_start_ns: u64,
    /// Exclusive first nanosecond not replayed by this environment.
    pub replay_end_ns: u64,
    /// Latest information-publication time admissible to this run.
    pub knowledge_cutoff_ns: u64,
    /// Complete simulated venue set.
    pub venues: Vec<BacktestVenueSpec>,
    /// Complete instrument universe.
    pub instruments: Vec<InstrumentAny>,
    /// Complete currency definitions used by balances and instruments.
    pub currencies: Vec<CurrencySpec>,
    /// Global cross-currency valuation policy required by a multi-currency environment.
    ///
    /// The secure runtime rejects this until a library-owned valuation applicator is installed.
    pub valuation_policy: Option<ValuationPolicyBinding>,
}

type RuntimeEnvironmentParts = (
    BacktestEngineConfig,
    Vec<SimulatedVenueConfig>,
    Vec<InstrumentAny>,
    Vec<ArtifactRef>,
);

impl BacktestEnvironmentSpec {
    /// Creates a schema-v3 environment covering the full representable replay interval.
    ///
    /// New production callers should use [`Self::new_for_replay`] to bind an exact interval. This
    /// compatibility constructor is intentionally stricter for contexts because they must cover
    /// `[0, u64::MAX)` and uses the Unix epoch as its knowledge cutoff.
    #[must_use]
    pub fn new(
        venues: Vec<BacktestVenueSpec>,
        instruments: Vec<InstrumentAny>,
        valuation_policy: Option<ValuationPolicyBinding>,
    ) -> Self {
        Self::new_for_replay(venues, instruments, valuation_policy, 0, u64::MAX, 0)
    }

    /// Creates a schema-v3 environment for an exact replay and knowledge interval.
    #[must_use]
    pub fn new_for_replay(
        mut venues: Vec<BacktestVenueSpec>,
        mut instruments: Vec<InstrumentAny>,
        valuation_policy: Option<ValuationPolicyBinding>,
        replay_start_ns: u64,
        replay_end_ns: u64,
        knowledge_cutoff_ns: u64,
    ) -> Self {
        for venue in &mut venues {
            canonicalize_starting_balances(&mut venue.starting_balances);
        }
        venues.sort_unstable_by_key(|venue| venue.venue);
        instruments.sort_unstable_by_key(Instrument::id);
        let currencies = collect_currency_specs(&venues, &instruments, valuation_policy.as_ref());
        Self {
            schema_version: BACKTEST_ENVIRONMENT_SCHEMA_VERSION,
            engine: BacktestEngineProfile::SecureDefaultV1,
            replay_start_ns,
            replay_end_ns,
            knowledge_cutoff_ns,
            venues,
            instruments,
            currencies,
            valuation_policy,
        }
    }

    /// Validates schema version, universe consistency, and required market contexts.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported version, empty or duplicate universe, a missing market
    /// context, a venue mismatch, or an instrument incompatible with its declared market family.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema_version == BACKTEST_ENVIRONMENT_SCHEMA_VERSION,
            "Unsupported backtest environment schema version {} (expected {})",
            self.schema_version,
            BACKTEST_ENVIRONMENT_SCHEMA_VERSION
        );
        anyhow::ensure!(
            self.replay_start_ns < self.replay_end_ns,
            "Environment replay interval must be non-empty and increasing"
        );
        anyhow::ensure!(
            self.knowledge_cutoff_ns <= self.replay_start_ns,
            "Environment knowledge cutoff must not be later than replay start"
        );
        anyhow::ensure!(!self.venues.is_empty(), "Environment has no venues");
        anyhow::ensure!(
            !self.instruments.is_empty(),
            "Environment has no instruments"
        );
        anyhow::ensure!(!self.currencies.is_empty(), "Environment has no currencies");
        anyhow::ensure!(
            self.venues.len() <= MAX_ENVIRONMENT_VENUES,
            "Environment exceeds the venue limit of {MAX_ENVIRONMENT_VENUES}"
        );
        anyhow::ensure!(
            self.instruments.len() <= MAX_ENVIRONMENT_INSTRUMENTS,
            "Environment exceeds the instrument limit of {MAX_ENVIRONMENT_INSTRUMENTS}"
        );
        anyhow::ensure!(
            self.currencies.len() <= MAX_ENVIRONMENT_CURRENCIES,
            "Environment exceeds the currency limit of {MAX_ENVIRONMENT_CURRENCIES}"
        );
        anyhow::ensure!(
            self.venues
                .windows(2)
                .all(|pair| pair[0].venue < pair[1].venue),
            "Environment venues must be unique and sorted canonically"
        );
        anyhow::ensure!(
            self.instruments
                .windows(2)
                .all(|pair| pair[0].id() < pair[1].id()),
            "Environment instruments must be unique and sorted canonically"
        );
        anyhow::ensure!(
            self.currencies
                .windows(2)
                .all(|pair| pair[0].code < pair[1].code),
            "Environment currencies must be unique and sorted canonically"
        );

        let mut venues = BTreeMap::new();
        for venue in &self.venues {
            venue.validate()?;
            for binding in venue
                .contexts
                .values()
                .chain(venue.execution_contexts.values())
            {
                binding.validate_replay_coverage(self.replay_start_ns, self.replay_end_ns)?;
                binding.validate_knowledge_cutoff(self.knowledge_cutoff_ns)?;
            }
            anyhow::ensure!(
                venues.insert(venue.venue, venue.market).is_none(),
                "Environment contains duplicate venue {}",
                venue.venue
            );
        }

        let currency_specs = self
            .currencies
            .iter()
            .map(|spec| {
                spec.validate()?;
                Ok((spec.code.as_str(), spec))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let used_currencies = collect_runtime_currencies(
            &self.venues,
            &self.instruments,
            self.valuation_policy.as_ref(),
        );
        for currency in &used_currencies {
            let spec = currency_specs.get(currency.code.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "Currency {} is used by the environment but has no bound definition",
                    currency.code
                )
            })?;
            anyhow::ensure!(
                spec.matches(*currency),
                "Currency {} resolved through mutable global state does not match its bound definition",
                currency.code
            );
        }
        let used_codes = used_currencies
            .iter()
            .map(|currency| currency.code.as_str())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            currency_specs.keys().copied().collect::<BTreeSet<_>>() == used_codes,
            "Environment contains an unused currency definition"
        );

        let mut instrument_ids = BTreeSet::new();
        for instrument in &self.instruments {
            validate_instrument_wire(instrument)?;
            let instrument_id = instrument.id();
            let published_at_ns = instrument.ts_event().as_u64();
            let observed_at_ns = instrument.ts_init().as_u64();
            anyhow::ensure!(
                published_at_ns <= observed_at_ns,
                "Instrument {instrument_id} was observed before its publication/effective timestamp"
            );
            anyhow::ensure!(
                observed_at_ns <= self.knowledge_cutoff_ns,
                "Instrument {instrument_id} was observed at {observed_at_ns} after knowledge cutoff {}",
                self.knowledge_cutoff_ns
            );
            anyhow::ensure!(
                instrument_ids.insert(instrument_id),
                "Environment contains duplicate instrument {instrument_id}"
            );
            let market = venues.get(&instrument_id.venue).ok_or_else(|| {
                anyhow::anyhow!(
                    "Instrument {instrument_id} references unknown venue {}",
                    instrument_id.venue
                )
            })?;
            anyhow::ensure!(
                market.accepts(instrument),
                "Instrument {instrument_id} is incompatible with declared market {market:?}"
            );
        }
        let valuation_currencies =
            collect_valuation_currency_codes(&self.venues, &self.instruments);
        anyhow::ensure!(
            valuation_currencies.len() <= 1 || self.valuation_policy.is_some(),
            "Multi-currency environment ({}) requires a bound valuation policy",
            valuation_currencies
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        );
        if let Some(policy) = &self.valuation_policy {
            policy.validate(
                self.replay_start_ns,
                self.replay_end_ns,
                self.knowledge_cutoff_ns,
                &currency_specs,
            )?;
        }
        for venue in venues.keys() {
            anyhow::ensure!(
                instrument_ids
                    .iter()
                    .any(|instrument_id| instrument_id.venue == *venue),
                "Environment venue {venue} has no instrument"
            );
        }
        for venue in &self.venues {
            for instrument_id in venue.leverages.keys().chain(venue.settlement_prices.keys()) {
                anyhow::ensure!(
                    instrument_ids.contains(instrument_id),
                    "Venue {} config references unknown instrument {instrument_id}",
                    venue.venue
                );
            }
        }
        validate_environment_artifact_bindings(&self.venues, self.valuation_policy.as_ref())?;
        Ok(())
    }

    /// Serializes the validated spec to its unique canonical JSON representation.
    ///
    /// # Errors
    ///
    /// Returns an error when validation or JSON serialization fails.
    pub fn to_canonical_json(&self) -> anyhow::Result<Vec<u8>> {
        self.validate()?;
        let value = serde_json::to_value(self)?;
        let content = serde_json::to_vec(&value)?;
        anyhow::ensure!(
            content.len() <= MAX_BACKTEST_ENVIRONMENT_BYTES,
            "Environment artifact exceeds the maximum size of {MAX_BACKTEST_ENVIRONMENT_BYTES} bytes"
        );
        Ok(content)
    }

    /// Parses only the unique canonical JSON representation of schema v3.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, non-canonical, unknown-field, unsupported, or internally
    /// inconsistent environment bytes.
    pub fn from_canonical_json(content: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            content.len() <= MAX_BACKTEST_ENVIRONMENT_BYTES,
            "Environment artifact exceeds the maximum size of {MAX_BACKTEST_ENVIRONMENT_BYTES} bytes"
        );
        reject_duplicate_json_keys(content)?;
        let value: Self = serde_json::from_slice(content)?;
        value.validate()?;
        let canonical = value.to_canonical_json()?;
        anyhow::ensure!(
            canonical == content,
            "Environment artifact is valid JSON but not canonical JSON"
        );
        Ok(value)
    }

    pub(crate) fn into_runtime(self) -> anyhow::Result<RuntimeEnvironmentParts> {
        self.validate()?;
        if let Some(venue) = self.venues.iter().find(|venue| {
            venue.market != MarketKind::Crypto
                || !venue.contexts.is_empty()
                || !venue.execution_contexts.is_empty()
        }) {
            anyhow::bail!(
                "Secure runtime support for {:?} on {} is not installed; refusing to claim its external market contexts were applied",
                venue.market,
                venue.venue
            );
        }
        anyhow::ensure!(
            self.valuation_policy.is_none(),
            "Secure cross-currency valuation runtime is not installed; refusing an unmaterialized valuation policy"
        );
        let engine = self.engine.into_config();
        let mut contexts = self
            .valuation_policy
            .into_iter()
            .map(|policy| policy.artifact)
            .collect::<Vec<_>>();
        contexts.extend(self.venues.iter().flat_map(|venue| {
            venue
                .contexts
                .values()
                .chain(venue.execution_contexts.values())
                .map(|binding| binding.artifact.clone())
        }));
        contexts.sort_unstable();
        contexts.dedup();
        let venues = self
            .venues
            .into_iter()
            .map(BacktestVenueSpec::into_config)
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((engine, venues, self.instruments, contexts))
    }
}

fn validate_environment_artifact_bindings(
    venues: &[BacktestVenueSpec],
    valuation_policy: Option<&ValuationPolicyBinding>,
) -> anyhow::Result<()> {
    let mut id_digests = BTreeMap::new();
    let mut bound_digests = BTreeSet::new();

    for venue in venues {
        for binding in venue
            .contexts
            .values()
            .chain(venue.execution_contexts.values())
        {
            register_environment_artifact(&binding.artifact, &mut id_digests, &mut bound_digests)?;
        }
    }
    if let Some(policy) = valuation_policy {
        register_environment_artifact(&policy.artifact, &mut id_digests, &mut bound_digests)?;
    }
    Ok(())
}

fn register_environment_artifact<'a>(
    artifact: &'a ArtifactRef,
    id_digests: &mut BTreeMap<&'a ArtifactId, &'a ContentDigest>,
    bound_digests: &mut BTreeSet<ContentDigest>,
) -> anyhow::Result<()> {
    if let Some(existing_digest) = id_digests.get(artifact.id()) {
        anyhow::ensure!(
            *existing_digest == artifact.digest(),
            "Artifact ID {} is bound to more than one content digest",
            artifact.id()
        );
    } else {
        id_digests.insert(artifact.id(), artifact.digest());
    }

    anyhow::ensure!(
        bound_digests.insert(*artifact.digest()),
        "Identical artifact content is reused by more than one environment binding"
    );
    Ok(())
}

fn canonicalize_starting_balances(balances: &mut Vec<Money>) {
    balances.sort_unstable_by(|left, right| {
        left.currency
            .code
            .cmp(&right.currency.code)
            .then(left.raw.cmp(&right.raw))
    });
    balances.dedup_by(|right, left| {
        left.raw == right.raw
            && left.currency.code == right.currency.code
            && left.currency.precision == right.currency.precision
            && left.currency.iso4217 == right.currency.iso4217
            && left.currency.name == right.currency.name
            && left.currency.currency_type == right.currency.currency_type
    });
}

#[derive(Copy, Clone)]
struct RejectDuplicateJsonKeys;

impl<'de> DeserializeSeed<'de> for RejectDuplicateJsonKeys {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RejectDuplicateJsonKeys {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element_seed(Self)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(A::Error::custom(format!(
                    "duplicate JSON object key '{key}'"
                )));
            }
            map.next_value_seed(Self)?;
        }
        Ok(())
    }
}

fn reject_duplicate_json_keys(content: &[u8]) -> anyhow::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(content);
    RejectDuplicateJsonKeys.deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(())
}

fn collect_currency_specs(
    venues: &[BacktestVenueSpec],
    instruments: &[InstrumentAny],
    valuation_policy: Option<&ValuationPolicyBinding>,
) -> Vec<CurrencySpec> {
    let mut currencies = BTreeMap::new();
    for currency in collect_runtime_currencies(venues, instruments, valuation_policy) {
        currencies
            .entry(currency.code.to_string())
            .or_insert_with(|| CurrencySpec::from_currency(currency));
    }
    currencies.into_values().collect()
}

fn collect_runtime_currencies(
    venues: &[BacktestVenueSpec],
    instruments: &[InstrumentAny],
    valuation_policy: Option<&ValuationPolicyBinding>,
) -> Vec<Currency> {
    let mut currencies = Vec::new();
    for venue in venues {
        currencies.extend(venue.starting_balances.iter().map(|money| money.currency));
        currencies.extend(venue.base_currency);
    }
    for instrument in instruments {
        currencies.extend(instrument.base_currency());
        currencies.push(instrument.quote_currency());
        currencies.push(instrument.settlement_currency());
        currencies.extend(instrument.max_notional().map(|money| money.currency));
        currencies.extend(instrument.min_notional().map(|money| money.currency));
    }
    currencies.extend(valuation_policy.map(|policy| policy.base_currency));
    currencies
}

fn collect_valuation_currency_codes(
    venues: &[BacktestVenueSpec],
    instruments: &[InstrumentAny],
) -> BTreeSet<String> {
    let mut currencies = BTreeSet::new();
    for venue in venues {
        currencies.extend(
            venue
                .starting_balances
                .iter()
                .map(|money| money.currency.code.to_string()),
        );
        currencies.extend(
            venue
                .base_currency
                .map(|currency| currency.code.to_string()),
        );
    }
    currencies.extend(
        instruments
            .iter()
            .map(|instrument| instrument.settlement_currency().code.to_string()),
    );
    currencies
}

fn validate_instrument_wire(instrument: &InstrumentAny) -> anyhow::Result<()> {
    let rebuilt = match instrument {
        InstrumentAny::CryptoPerpetual(value) => InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::CryptoPerpetual::new_checked(
                value.id,
                value.raw_symbol,
                value.base_currency,
                value.quote_currency,
                value.settlement_currency,
                value.is_inverse,
                value.price_precision,
                value.size_precision,
                value.price_increment,
                value.size_increment,
                Some(value.multiplier),
                Some(value.lot_size),
                value.max_quantity,
                value.min_quantity,
                value.max_notional,
                value.min_notional,
                value.max_price,
                value.min_price,
                Some(value.margin_init),
                Some(value.margin_maint),
                Some(value.maker_fee),
                Some(value.taker_fee),
                value.tick_scheme,
                value.info.clone(),
                value.ts_event,
                value.ts_init,
            )?,
        ),
        InstrumentAny::Equity(value) => {
            InstrumentAny::Equity(nautilus_model::instruments::Equity::new_checked(
                value.id,
                value.raw_symbol,
                value.isin,
                value.currency,
                value.price_precision,
                value.price_increment,
                value.lot_size,
                value.max_quantity,
                value.min_quantity,
                value.max_price,
                value.min_price,
                Some(value.margin_init),
                Some(value.margin_maint),
                Some(value.maker_fee),
                Some(value.taker_fee),
                value.tick_scheme,
                value.info.clone(),
                value.ts_event,
                value.ts_init,
            )?)
        }
        _ => anyhow::bail!(
            "Backtest environment schema v3 accepts only CryptoPerpetual and Equity instruments"
        ),
    };
    anyhow::ensure!(
        serde_json::to_value(instrument)? == serde_json::to_value(rebuilt)?,
        "Instrument {} changed while passing checked construction",
        instrument.id()
    );
    Ok(())
}

fn validate_route_identity(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            }),
        "{field} must contain 1..=64 canonical ASCII identifier bytes"
    );
    Ok(())
}

fn parse_positive_decimal(field: &str, raw: &str) -> anyhow::Result<Decimal> {
    let value = Decimal::from_str(raw)
        .map_err(|error| anyhow::anyhow!("Invalid {field} decimal '{raw}': {error}"))?;
    anyhow::ensure!(value > Decimal::ZERO, "{field} must be positive");
    anyhow::ensure!(
        value.normalize().to_string() == raw,
        "{field} must use canonical decimal notation"
    );
    Ok(value)
}

fn parse_positive_f64(field: &str, value: &str) -> anyhow::Result<f64> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| anyhow::anyhow!("Invalid {field} value '{value}': {error}"))?;
    anyhow::ensure!(
        parsed.is_finite() && parsed > 0.0,
        "{field} must be positive and finite"
    );
    anyhow::ensure!(
        parsed.to_string() == value,
        "{field} must use canonical numeric notation"
    );
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        identifiers::{InstrumentId, Symbol},
        instruments::stubs::{crypto_perpetual_ethusdt, currency_pair_btcusdt, equity_aapl},
        types::Quantity,
    };
    use nautilus_provenance::{ArtifactId, ContentDigest};
    use rstest::rstest;

    use super::*;
    use crate::engine::BacktestEngine;

    const TEST_REPLAY_START_NS: u64 = 1_700_000_000_000_000_000;
    const TEST_REPLAY_END_NS: u64 = 1_710_000_000_000_000_000;
    const TEST_KNOWLEDGE_CUTOFF_NS: u64 = 1_690_000_000_000_000_000;

    fn artifact(id: &str) -> ArtifactRef {
        ArtifactRef::new(
            ArtifactId::new(id).unwrap(),
            ContentDigest::digest_bytes(id.as_bytes()),
        )
    }

    fn contexts(
        venue: &str,
        market: MarketKind,
    ) -> BTreeMap<MarketContextRole, MarketContextBinding> {
        [
            MarketContextRole::TradingCalendar,
            MarketContextRole::MarketRules,
            MarketContextRole::FeeSchedule,
            MarketContextRole::SettlementRules,
            MarketContextRole::Symbology,
            MarketContextRole::CorporateActions,
            MarketContextRole::ShortSaleRules,
            MarketContextRole::PriceLimits,
        ]
        .into_iter()
        .map(|role| {
            (
                role,
                MarketContextBinding::new(
                    role,
                    market,
                    MarketProfile::for_market(market),
                    Venue::from(venue),
                    TEST_KNOWLEDGE_CUTOFF_NS - 2,
                    TEST_KNOWLEDGE_CUTOFF_NS - 1,
                    TEST_REPLAY_START_NS,
                    TEST_REPLAY_END_NS,
                    artifact(&format!("market-context/{venue}/{role:?}/v1")),
                ),
            )
        })
        .collect()
    }

    fn venue(
        venue: &str,
        market: MarketKind,
        account_type: AccountType,
        balance: &str,
    ) -> BacktestVenueSpec {
        let market_contexts = if market == MarketKind::Crypto {
            BTreeMap::new()
        } else {
            contexts(venue, market)
        };
        let mut venue = BacktestVenueSpec::with_builtin_defaults(
            Venue::from(venue),
            market,
            market_contexts,
            OmsType::Netting,
            account_type,
            BookType::L1_MBP,
            vec![Money::from(balance)],
        );
        venue.execution_route = match market {
            MarketKind::ChinaAShares => ExecutionRoute::StockConnect {
                broker: "TEST-BROKER".to_string(),
            },
            MarketKind::UnitedStatesEquities => ExecutionRoute::BrokerSmartRouter {
                broker: "TEST-BROKER".to_string(),
            },
            MarketKind::Crypto | MarketKind::HongKongEquities => {
                ExecutionRoute::DirectVenue { venue: venue.venue }
            }
        };
        if market != MarketKind::Crypto
            && !matches!(
                &venue.execution_route,
                ExecutionRoute::DirectVenue { venue: route_venue }
                    if *route_venue == venue.listing_venue.venue()
            )
        {
            let route = venue.execution_route.clone();
            venue.execution_contexts = contexts(venue.venue.as_str(), market)
                .into_iter()
                .map(|(role, listing_binding)| {
                    (
                        role,
                        MarketContextBinding::new_for_execution_route(
                            role,
                            market,
                            venue.profile,
                            venue.venue,
                            route.clone(),
                            listing_binding.published_at_ns,
                            listing_binding.observed_at_ns,
                            listing_binding.effective_from_ns,
                            listing_binding.effective_to_ns,
                            artifact(&format!(
                                "market-context/{}/{role:?}/execution/v1",
                                venue.venue
                            )),
                        ),
                    )
                })
                .collect();
        }
        venue
    }

    fn equity(id: &str, raw_symbol: &str, currency: &str, lot_size: u64) -> InstrumentAny {
        let mut instrument = equity_aapl();
        instrument.id = InstrumentId::from(id);
        instrument.raw_symbol = Symbol::from(raw_symbol);
        instrument.isin = None;
        instrument.currency = Currency::from(currency);
        instrument.lot_size = Some(Quantity::from(lot_size));
        InstrumentAny::Equity(instrument)
    }

    fn synthetic_okx_crypto_perpetual() -> InstrumentAny {
        // This is a synthetic schema fixture, not an attested exchange instrument snapshot. The
        // identifier and raw symbol follow the OKX swap symbology used by the repository adapter.
        let mut crypto = crypto_perpetual_ethusdt();
        crypto.id = InstrumentId::from("ETH-USDT-SWAP.OKX");
        crypto.raw_symbol = Symbol::from("ETH-USDT-SWAP");
        InstrumentAny::CryptoPerpetual(crypto)
    }

    fn synthetic_single_currency_okx_spec() -> BacktestEnvironmentSpec {
        BacktestEnvironmentSpec::new_for_replay(
            vec![venue(
                "OKX",
                MarketKind::Crypto,
                AccountType::Margin,
                "1_000_000 USDT",
            )],
            vec![synthetic_okx_crypto_perpetual()],
            None,
            TEST_REPLAY_START_NS,
            TEST_REPLAY_END_NS,
            TEST_KNOWLEDGE_CUTOFF_NS,
        )
    }

    fn valuation_policy(base_currency: Currency) -> ValuationPolicyBinding {
        ValuationPolicyBinding::new(
            base_currency,
            ValuationRateSide::Midpoint,
            60_000_000_000,
            ValuationTriangulationPolicy::DeterministicShortestPath,
            ValuationCutoffPolicy::ReplayAndKnowledgeCutoff,
            TEST_KNOWLEDGE_CUTOFF_NS - 2,
            TEST_KNOWLEDGE_CUTOFF_NS - 1,
            TEST_REPLAY_START_NS,
            TEST_REPLAY_END_NS,
            artifact("market-context/global/valuation-policy/v1"),
        )
    }

    fn structurally_modeled_four_market_spec() -> BacktestEnvironmentSpec {
        BacktestEnvironmentSpec::new_for_replay(
            vec![
                venue(
                    "OKX",
                    MarketKind::Crypto,
                    AccountType::Margin,
                    "1_000_000 USDT",
                ),
                venue(
                    "XSHG",
                    MarketKind::ChinaAShares,
                    AccountType::Cash,
                    "1_000_000 CNY",
                ),
                venue(
                    "XHKG",
                    MarketKind::HongKongEquities,
                    AccountType::Cash,
                    "1_000_000 HKD",
                ),
                venue(
                    "XNAS",
                    MarketKind::UnitedStatesEquities,
                    AccountType::Cash,
                    "1_000_000 USD",
                ),
            ],
            vec![
                synthetic_okx_crypto_perpetual(),
                equity("600000.XSHG", "600000", "CNY", 100),
                equity("0700.XHKG", "0700", "HKD", 100),
                equity("AAPL.XNAS", "AAPL", "USD", 1),
            ],
            Some(valuation_policy(Currency::USD())),
            TEST_REPLAY_START_NS,
            TEST_REPLAY_END_NS,
            TEST_KNOWLEDGE_CUTOFF_NS,
        )
    }

    #[rstest]
    fn canonical_environment_structurally_models_four_markets_and_runtime_fails_closed_without_adapters()
     {
        let content = structurally_modeled_four_market_spec()
            .to_canonical_json()
            .unwrap();
        let decoded = BacktestEnvironmentSpec::from_canonical_json(&content).unwrap();
        assert_eq!(decoded.to_canonical_json().unwrap(), content);
        assert_eq!(decoded.replay_start_ns, TEST_REPLAY_START_NS);
        assert_eq!(decoded.replay_end_ns, TEST_REPLAY_END_NS);
        assert_eq!(decoded.knowledge_cutoff_ns, TEST_KNOWLEDGE_CUTOFF_NS);
        let china = decoded
            .venues
            .iter()
            .find(|venue| venue.market == MarketKind::ChinaAShares)
            .unwrap();
        assert_eq!(china.profile, MarketProfile::ChinaASharesSchemaV1);
        assert!(matches!(
            china.execution_route,
            ExecutionRoute::StockConnect { .. }
        ));
        assert_eq!(china.execution_contexts.len(), 8);
        assert!(china.execution_contexts.values().all(|binding| {
            binding.target == MarketContextTarget::ExecutionRoute(china.execution_route.clone())
        }));
        let us = decoded
            .venues
            .iter()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap();
        assert_eq!(us.profile, MarketProfile::UnitedStatesEquitiesSchemaV1);
        assert!(matches!(
            us.execution_route,
            ExecutionRoute::BrokerSmartRouter { .. }
        ));
        assert_eq!(us.execution_contexts.len(), 8);

        let error = BacktestEngine::from_environment_artifact(
            ArtifactId::new("environment/four-market/v3").unwrap(),
            &content,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not installed"));
        assert!(error.to_string().contains("refusing to claim"));
    }

    #[rstest]
    fn generated_ffi_environment_schema_constants_match_rust() {
        let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let header =
            std::fs::read_to_string(workspace.join("nautilus_trader/core/includes/backtest.h"))
                .expect("workspace C header must be readable");
        let cython =
            std::fs::read_to_string(workspace.join("nautilus_trader/core/rust/backtest.pxd"))
                .expect("workspace Cython declarations must be readable");
        let c_declaration = format!(
            "#define BACKTEST_ENVIRONMENT_SCHEMA_VERSION {BACKTEST_ENVIRONMENT_SCHEMA_VERSION}"
        );
        let cython_declaration = format!(
            "const uint32_t BACKTEST_ENVIRONMENT_SCHEMA_VERSION # = {BACKTEST_ENVIRONMENT_SCHEMA_VERSION}"
        );

        assert!(
            header.contains(&c_declaration),
            "generated C header drifted from Rust environment schema"
        );
        assert!(
            cython.contains(&cython_declaration),
            "generated Cython declaration drifted from Rust environment schema"
        );
    }

    #[rstest]
    fn market_scope_stable_ids_are_library_owned() {
        assert_eq!(
            MarketProfile::GenericCryptoPerpetualV1.stable_id(),
            "market-profile/generic-crypto-perpetual/v1"
        );
        assert_eq!(
            MarketProfile::ChinaASharesSchemaV1.stable_id(),
            "market-profile/china-a-shares-schema/v1"
        );
        assert_eq!(
            ListingVenue::Exchange(Venue::from("XNAS")).stable_id(),
            "listing/exchange/XNAS"
        );
        assert_eq!(
            ExecutionRoute::DirectVenue {
                venue: Venue::from("ARCX")
            }
            .stable_id(),
            "execution-route/direct-venue/ARCX"
        );
        assert_eq!(
            ExecutionRoute::BrokerSmartRouter {
                broker: "IB".to_string()
            }
            .stable_id(),
            "execution-route/broker-smart-router/IB"
        );
        assert_eq!(
            ExecutionRoute::StockConnect {
                broker: "IB".to_string()
            }
            .stable_id(),
            "execution-route/stock-connect/IB"
        );
    }

    #[rstest]
    fn synthetic_single_settlement_currency_okx_environment_is_runtime_loadable() {
        let content = synthetic_single_currency_okx_spec()
            .to_canonical_json()
            .unwrap();

        BacktestEngine::from_environment_artifact(
            ArtifactId::new("environment/synthetic-okx-single-currency/v3").unwrap(),
            &content,
        )
        .unwrap();
    }

    #[rstest]
    fn multi_currency_environment_requires_valuation_policy() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.valuation_policy = None;

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires a bound valuation policy")
        );
        assert!(error.to_string().contains("CNY"));
        assert!(error.to_string().contains("HKD"));
        assert!(error.to_string().contains("USD"));
        assert!(error.to_string().contains("USDT"));
    }

    #[rstest]
    fn multi_currency_generic_crypto_runtime_rejects_uninstalled_valuation_policy() {
        let mut spec = synthetic_single_currency_okx_spec();
        spec.venues[0]
            .starting_balances
            .push(Money::from("1_000_000 USD"));
        canonicalize_starting_balances(&mut spec.venues[0].starting_balances);
        spec.valuation_policy = Some(valuation_policy(Currency::USDT()));
        spec.currencies = collect_currency_specs(
            &spec.venues,
            &spec.instruments,
            spec.valuation_policy.as_ref(),
        );
        let content = spec.to_canonical_json().unwrap();

        let error = BacktestEngine::from_environment_artifact(
            ArtifactId::new("environment/generic-crypto-multi-currency/v3").unwrap(),
            &content,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("valuation runtime is not installed")
        );
    }

    #[rstest]
    fn environment_rejects_zero_valuation_quote_age() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.valuation_policy.as_mut().unwrap().max_quote_age_ns = 0;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("max_quote_age_ns"));
    }

    #[rstest]
    fn environment_rejects_valuation_policy_that_does_not_cover_replay() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.valuation_policy.as_mut().unwrap().effective_to_ns = TEST_REPLAY_END_NS - 1;

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Valuation policy does not cover replay interval")
        );
    }

    #[rstest]
    fn environment_rejects_valuation_policy_observed_after_knowledge_cutoff() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.valuation_policy.as_mut().unwrap().observed_at_ns = TEST_KNOWLEDGE_CUTOFF_NS + 1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("after knowledge cutoff"));
    }

    #[rstest]
    fn environment_rejects_valuation_content_reused_as_market_context() {
        let mut spec = structurally_modeled_four_market_spec();
        let context_digest = *spec
            .venues
            .iter()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts[&MarketContextRole::TradingCalendar]
            .artifact
            .digest();
        spec.valuation_policy.as_mut().unwrap().artifact = ArtifactRef::new(
            ArtifactId::new("market-context/global/valuation-policy/alias-v1").unwrap(),
            context_digest,
        );

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("more than one environment binding")
        );
    }

    #[rstest]
    fn environment_rejects_smart_as_listing_venue() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap()
            .listing_venue = ListingVenue::Exchange(Venue::from("SMART"));

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must not be used as a listing venue")
        );
    }

    #[rstest]
    fn environment_rejects_stock_connect_for_non_a_share_market() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap()
            .execution_route = ExecutionRoute::StockConnect {
            broker: "TEST-BROKER".to_string(),
        };

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only for mainland China A-shares")
        );
    }

    #[rstest]
    fn environment_rejects_cross_listing_direct_route_for_a_shares() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::ChinaAShares)
            .unwrap()
            .execution_route = ExecutionRoute::DirectVenue {
            venue: Venue::from("XSHE"),
        };

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("must target its listing venue"));
    }

    #[rstest]
    fn environment_rejects_smart_router_for_generic_crypto() {
        let mut spec = synthetic_single_currency_okx_spec();
        spec.venues[0].execution_route = ExecutionRoute::BrokerSmartRouter {
            broker: "TEST-BROKER".to_string(),
        };

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only for Hong Kong or United States equities")
        );
    }

    #[rstest]
    fn environment_rejects_market_profile_substitution() {
        let mut spec = synthetic_single_currency_okx_spec();
        spec.venues[0].profile = MarketProfile::UnitedStatesEquitiesSchemaV1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("Market profile"));
    }

    #[rstest]
    fn unknown_securities_venue_cannot_be_laundered_as_crypto() {
        let venue = venue(
            "NASDAQ",
            MarketKind::Crypto,
            AccountType::Margin,
            "1_000_000 USDT",
        );

        let error = venue.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("incompatible with declared market Crypto")
        );
    }

    #[rstest]
    fn crypto_schema_v3_does_not_claim_unimplemented_instrument_variants() {
        let spot_pair = InstrumentAny::CurrencyPair(currency_pair_btcusdt());

        assert!(!MarketKind::Crypto.accepts(&spot_pair));
    }

    #[rstest]
    fn canonical_loader_rejects_equivalent_noncanonical_json() {
        let mut content = structurally_modeled_four_market_spec()
            .to_canonical_json()
            .unwrap();
        content.push(b'\n');

        let error = BacktestEnvironmentSpec::from_canonical_json(&content).unwrap_err();

        assert!(error.to_string().contains("not canonical JSON"));
    }

    #[rstest]
    fn canonical_loader_rejects_unknown_fields() {
        let spec = structurally_modeled_four_market_spec();
        let mut value = serde_json::to_value(spec).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("untrusted_factory".to_string(), serde_json::json!(true));
        let content = serde_json::to_vec(&value).unwrap();

        assert!(BacktestEnvironmentSpec::from_canonical_json(&content).is_err());
    }

    #[rstest]
    fn environment_rejects_missing_equity_corporate_actions() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .remove(&MarketContextRole::CorporateActions);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("CorporateActions"));
    }

    #[rstest]
    fn environment_rejects_market_context_role_substitution() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::CorporateActions)
            .unwrap()
            .role = MarketContextRole::Symbology;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("does not match bound role"));
    }

    #[rstest]
    fn environment_rejects_market_context_market_substitution() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::MarketRules)
            .unwrap()
            .market = MarketKind::UnitedStatesEquities;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("not HongKongEquities"));
    }

    #[rstest]
    fn environment_rejects_market_context_profile_substitution() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::MarketRules)
            .unwrap()
            .profile = MarketProfile::UnitedStatesEquitiesSchemaV1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("not HongKongEquitiesSchemaV1"));
    }

    #[rstest]
    fn environment_rejects_market_context_venue_substitution() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::TradingCalendar)
            .unwrap()
            .venue = Venue::from("XNAS");

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("not XHKG"));
    }

    #[rstest]
    fn environment_rejects_empty_market_context_effective_range() {
        let mut spec = structurally_modeled_four_market_spec();
        let binding = spec
            .venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::FeeSchedule)
            .unwrap();
        binding.effective_to_ns = binding.effective_from_ns;

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("empty or reversed effective range")
        );
    }

    #[rstest]
    fn environment_rejects_market_context_that_does_not_cover_replay() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::FeeSchedule)
            .unwrap()
            .effective_from_ns = TEST_REPLAY_START_NS + 1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("does not cover replay interval"));
    }

    #[rstest]
    fn environment_rejects_empty_replay_interval() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.replay_end_ns = spec.replay_start_ns;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("replay interval"));
    }

    #[rstest]
    fn environment_rejects_knowledge_cutoff_after_replay_start() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.knowledge_cutoff_ns = spec.replay_start_ns + 1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("knowledge cutoff"));
    }

    #[rstest]
    fn environment_rejects_instrument_observed_after_knowledge_cutoff() {
        let mut spec = synthetic_single_currency_okx_spec();
        let InstrumentAny::CryptoPerpetual(instrument) = &mut spec.instruments[0] else {
            panic!("fixture must contain a crypto perpetual");
        };
        instrument.ts_event = UnixNanos::from(TEST_KNOWLEDGE_CUTOFF_NS + 1);
        instrument.ts_init = UnixNanos::from(TEST_KNOWLEDGE_CUTOFF_NS + 1);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("after knowledge cutoff"));
    }

    #[rstest]
    fn environment_rejects_market_context_observed_after_knowledge_cutoff() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::TradingCalendar)
            .unwrap()
            .observed_at_ns = TEST_KNOWLEDGE_CUTOFF_NS + 1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("after knowledge cutoff"));
    }

    #[rstest]
    fn environment_rejects_context_observed_before_publication() {
        let mut spec = structurally_modeled_four_market_spec();
        let binding = spec
            .venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::TradingCalendar)
            .unwrap();
        binding.published_at_ns = binding.observed_at_ns + 1;

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("observed before it was published")
        );
    }

    #[rstest]
    fn environment_rejects_missing_execution_route_context() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap()
            .execution_contexts
            .remove(&MarketContextRole::FeeSchedule);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("execution context"));
        assert!(error.to_string().contains("FeeSchedule"));
    }

    #[rstest]
    fn environment_rejects_listing_evidence_substituted_for_execution_route_context() {
        let mut spec = structurally_modeled_four_market_spec();
        let venue = spec
            .venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap();
        venue
            .execution_contexts
            .get_mut(&MarketContextRole::MarketRules)
            .unwrap()
            .target = MarketContextTarget::ListingVenue(venue.listing_venue);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("does not match required target"));
    }

    #[rstest]
    fn environment_rejects_same_content_under_different_ids_for_multiple_context_roles() {
        let mut spec = structurally_modeled_four_market_spec();
        let venue = spec
            .venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap();
        let calendar_digest = *venue.contexts[&MarketContextRole::TradingCalendar]
            .artifact
            .digest();
        venue
            .contexts
            .get_mut(&MarketContextRole::MarketRules)
            .unwrap()
            .artifact = ArtifactRef::new(
            ArtifactId::new("market-context/XHKG/alias-calendar-as-rules/v1").unwrap(),
            calendar_digest,
        );

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("reuses identical content for multiple")
        );
    }

    #[rstest]
    fn environment_rejects_one_artifact_id_bound_to_different_digests() {
        let mut spec = structurally_modeled_four_market_spec();
        let venue = spec
            .venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap();
        let calendar_id = venue.contexts[&MarketContextRole::TradingCalendar]
            .artifact
            .id()
            .clone();
        venue
            .contexts
            .get_mut(&MarketContextRole::MarketRules)
            .unwrap()
            .artifact = ArtifactRef::new(
            calendar_id,
            ContentDigest::digest_bytes(b"different-market-rules"),
        );

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("more than one content digest"));
    }

    #[rstest]
    fn environment_rejects_same_context_content_reused_across_venues() {
        let mut spec = structurally_modeled_four_market_spec();
        let hong_kong_digest = *spec
            .venues
            .iter()
            .find(|venue| venue.market == MarketKind::HongKongEquities)
            .unwrap()
            .contexts[&MarketContextRole::TradingCalendar]
            .artifact
            .digest();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == MarketKind::UnitedStatesEquities)
            .unwrap()
            .contexts
            .get_mut(&MarketContextRole::TradingCalendar)
            .unwrap()
            .artifact = ArtifactRef::new(
            ArtifactId::new("market-context/XNAS/aliased-XHKG-calendar/v1").unwrap(),
            hong_kong_digest,
        );

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("more than one environment binding")
        );
    }

    #[rstest]
    #[case(MarketKind::HongKongEquities)]
    #[case(MarketKind::UnitedStatesEquities)]
    fn environment_requires_price_control_contexts_for_hk_and_us_equities(
        #[case] market: MarketKind,
    ) {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.market == market)
            .unwrap()
            .contexts
            .remove(&MarketContextRole::PriceLimits);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("PriceLimits"));
    }

    #[rstest]
    fn environment_rejects_market_instrument_mismatch() {
        let mut spec = structurally_modeled_four_market_spec();
        let mut equity = equity_aapl();
        equity.id = InstrumentId::from("AAPL.OKX");
        let crypto_index = spec
            .instruments
            .iter()
            .position(|instrument| matches!(instrument, InstrumentAny::CryptoPerpetual(_)))
            .unwrap();
        spec.instruments[crypto_index] = InstrumentAny::Equity(equity);
        spec.instruments.sort_unstable_by_key(Instrument::id);
        spec.currencies = collect_currency_specs(
            &spec.venues,
            &spec.instruments,
            spec.valuation_policy.as_ref(),
        );

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("incompatible"));
    }

    #[rstest]
    fn environment_revalidates_deserialized_instrument_fields() {
        let mut spec = structurally_modeled_four_market_spec();
        let instrument = spec
            .instruments
            .iter_mut()
            .find_map(|instrument| match instrument {
                InstrumentAny::CryptoPerpetual(instrument) => Some(instrument),
                _ => None,
            })
            .unwrap();
        instrument.price_precision = instrument.price_precision.saturating_add(1);

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("price_precision"));
    }

    #[rstest]
    fn environment_rejects_currency_definition_drift() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.currencies
            .iter_mut()
            .find(|currency| currency.code == "USDT")
            .unwrap()
            .precision = 1;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("mutable global state"));
    }

    #[rstest]
    fn environment_rejects_venue_market_spoofing() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.venue == Venue::from("XHKG"))
            .unwrap()
            .market = MarketKind::UnitedStatesEquities;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("Venue XHKG is incompatible"));
    }

    #[rstest]
    fn environment_rejects_noncanonical_numeric_strings() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.venue == Venue::from("OKX"))
            .unwrap()
            .liquidation_trigger_ratio = "1.0".to_string();

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("canonical numeric notation"));
    }

    #[rstest]
    fn environment_rejects_noncanonical_decimal_strings() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.venue == Venue::from("OKX"))
            .unwrap()
            .default_leverage = Some("1.0".to_string());

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("canonical decimal notation"));
    }

    #[rstest]
    fn secure_environment_rejects_random_venue_identifiers() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues
            .iter_mut()
            .find(|venue| venue.venue == Venue::from("OKX"))
            .unwrap()
            .use_random_ids = true;

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("use_random_ids=false"));
    }

    #[rstest]
    fn venue_constructor_canonically_sorts_and_deduplicates_exact_starting_balances() {
        let venue = BacktestVenueSpec::with_builtin_defaults(
            Venue::from("OKX"),
            MarketKind::Crypto,
            BTreeMap::new(),
            OmsType::Netting,
            AccountType::Margin,
            BookType::L1_MBP,
            vec![
                Money::from("2 USD"),
                Money::from("1 USDT"),
                Money::from("2 USD"),
            ],
        );

        let codes = venue
            .starting_balances
            .iter()
            .map(|money| money.currency.code.as_str())
            .collect::<Vec<_>>();
        assert_eq!(codes, vec!["USD", "USDT"]);
    }

    #[rstest]
    fn venue_rejects_conflicting_starting_balance_currency_aliases() {
        let mut venue = venue(
            "OKX",
            MarketKind::Crypto,
            AccountType::Margin,
            "1_000_000 USDT",
        );
        let aliased_usdt = Currency::new("USDT", 2, 0, "Aliased Tether", CurrencyType::Crypto);
        venue.starting_balances.push(Money::new(1.0, aliased_usdt));
        canonicalize_starting_balances(&mut venue.starting_balances);

        let error = venue.validate().unwrap_err();

        assert!(error.to_string().contains("unique currency codes"));
    }

    #[rstest]
    fn canonical_loader_rejects_duplicate_nested_json_keys() {
        let canonical = String::from_utf8(
            structurally_modeled_four_market_spec()
                .to_canonical_json()
                .unwrap(),
        )
        .unwrap();
        let content = canonical.replacen(
            "\"use_random_ids\":false",
            "\"use_random_ids\":false,\"use_random_ids\":false",
            1,
        );

        let error = BacktestEnvironmentSpec::from_canonical_json(content.as_bytes()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate JSON object key 'use_random_ids'")
        );
    }

    #[rstest]
    fn environment_rejects_duplicate_instrument_array_entries() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.instruments.push(spec.instruments[0].clone());
        spec.instruments.sort_unstable_by_key(Instrument::id);

        let error = spec.validate().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("instruments must be unique and sorted canonically")
        );
    }

    #[rstest]
    fn environment_rejects_excessive_starting_balance_collection() {
        let mut venue = venue(
            "OKX",
            MarketKind::Crypto,
            AccountType::Margin,
            "1_000_000 USDT",
        );
        venue.starting_balances = vec![Money::from("1 USDT"); MAX_VENUE_STARTING_BALANCES + 1];

        let error = venue.validate().unwrap_err();

        assert!(error.to_string().contains("starting balance limit"));
    }

    #[rstest]
    fn environment_rejects_excessive_top_level_collection() {
        let mut spec = structurally_modeled_four_market_spec();
        spec.venues = vec![spec.venues[0].clone(); MAX_ENVIRONMENT_VENUES + 1];

        let error = spec.validate().unwrap_err();

        assert!(error.to_string().contains("venue limit"));
    }

    #[rstest]
    fn canonical_loader_rejects_oversized_artifact_before_parsing() {
        let content = vec![b' '; MAX_BACKTEST_ENVIRONMENT_BYTES + 1];

        let error = BacktestEnvironmentSpec::from_canonical_json(&content).unwrap_err();

        assert!(error.to_string().contains("maximum size"));
    }
}
