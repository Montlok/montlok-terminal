// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License. You may obtain a copy of the
//  License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Cross-crate contract from a pinned OKX instrument response to a canonical backtest environment.

use nautilus_backtest::{
    engine::BacktestEngine,
    environment::{
        BacktestEnvironmentSpec, ExecutionRoute, ListingVenue, MarketKind, MarketProfile,
    },
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_okx::{
    common::{
        enums::{OKXInstrumentStatus, OKXInstrumentType},
        models::OKXInstrument,
        parse::parse_swap_instrument,
    },
    environment::{OKX_SWAP_ENVIRONMENT_MATERIALIZER_ID, materialize_swap_backtest_environment},
    http::client::OKXResponse,
};
use nautilus_provenance::{ArtifactId, ContentDigest};

const SWAP_FIXTURE: &[u8] = include_bytes!("../test_data/http_get_instruments_swap.json");
const SWAP_FIXTURE_DIGEST: &str =
    "422179791058c14fe62a3f570134f170723f4eb94525afb1b412350e24207b6f";
const TEST_INGEST_TS_NS: u64 = 1_786_147_200_000_000_000;
const TEST_REPLAY_START_NS: u64 = TEST_INGEST_TS_NS;
const TEST_REPLAY_END_NS: u64 = TEST_REPLAY_START_NS + 86_400_000_000_000;

#[test]
fn real_okx_swap_fixture_materializes_canonical_backtest_environment() {
    let fixture_digest = ContentDigest::digest_bytes(SWAP_FIXTURE);
    assert_eq!(fixture_digest.to_string(), SWAP_FIXTURE_DIGEST);

    let response: OKXResponse<OKXInstrument> =
        serde_json::from_slice(SWAP_FIXTURE).expect("pinned OKX swap fixture must deserialize");
    assert_eq!(response.code, "0");
    let definition = response
        .data
        .iter()
        .find(|instrument| instrument.inst_id == "ETH-USDT-SWAP")
        .expect("pinned OKX fixture must contain ETH-USDT-SWAP");
    assert_eq!(definition.inst_type, OKXInstrumentType::Swap);
    assert_eq!(definition.state, OKXInstrumentStatus::Live);
    assert_eq!(definition.list_time, Some(1_573_557_408_000));
    let ingest_ts = UnixNanos::from(TEST_INGEST_TS_NS);
    let instrument = parse_swap_instrument(definition, None, None, None, None, ingest_ts)
        .expect("pinned ETH-USDT-SWAP definition must parse");

    let InstrumentAny::CryptoPerpetual(perpetual) = &instrument else {
        panic!("ETH-USDT-SWAP must materialize as CryptoPerpetual");
    };
    assert_eq!(perpetual.id, InstrumentId::from("ETH-USDT-SWAP.OKX"));
    assert_eq!(perpetual.raw_symbol, Symbol::from("ETH-USDT-SWAP"));
    assert_eq!(perpetual.base_currency, Currency::ETH());
    assert_eq!(perpetual.quote_currency, Currency::USDT());
    assert_eq!(perpetual.settlement_currency, Currency::USDT());
    assert!(!perpetual.is_inverse);
    assert_eq!(perpetual.multiplier, Quantity::from("0.1"));
    assert_eq!(perpetual.price_precision, 2);
    assert_eq!(perpetual.size_precision, 2);
    assert_eq!(perpetual.price_increment, Price::from("0.01"));
    assert_eq!(perpetual.size_increment, Quantity::from("0.01"));
    assert_eq!(perpetual.lot_size, Quantity::from("0.01"));
    assert_eq!(perpetual.min_quantity, Some(Quantity::from("0.01")));
    assert_eq!(perpetual.max_quantity, Some(Quantity::from(20_000)));
    assert_eq!(perpetual.ts_event, ingest_ts);
    assert_eq!(perpetual.ts_init, ingest_ts);

    let source_id = ArtifactId::new("dataset/okx/http_get_instruments_swap.json")
        .expect("fixture artifact ID must be valid");
    let materialized = materialize_swap_backtest_environment(
        SWAP_FIXTURE,
        source_id.clone(),
        "ETH-USDT-SWAP",
        ingest_ts,
        UnixNanos::from(TEST_REPLAY_START_NS),
        UnixNanos::from(TEST_REPLAY_END_NS),
        Money::from("1_000_000 USDT"),
    )
    .expect("production OKX materializer must build the pinned environment");
    let canonical = materialized.canonical_bytes();
    let decoded = BacktestEnvironmentSpec::from_canonical_json(canonical)
        .expect("canonical real OKX environment must decode");
    assert_eq!(
        decoded
            .to_canonical_json()
            .expect("decoded environment must remain canonical"),
        canonical
    );
    assert_eq!(decoded.venues.len(), 1);
    assert_eq!(decoded.venues[0].venue, Venue::from("OKX"));
    assert_eq!(decoded.venues[0].market, MarketKind::Crypto);
    assert_eq!(
        decoded.venues[0].profile,
        MarketProfile::GenericCryptoPerpetualV1
    );
    assert_eq!(
        decoded.venues[0].listing_venue,
        ListingVenue::Exchange(Venue::from("OKX"))
    );
    assert_eq!(
        decoded.venues[0].execution_route,
        ExecutionRoute::DirectVenue {
            venue: Venue::from("OKX")
        }
    );
    assert_eq!(decoded.instruments.len(), 1);
    assert_eq!(
        decoded.instruments[0].id(),
        InstrumentId::from("ETH-USDT-SWAP.OKX")
    );
    assert!(decoded.valuation_policy.is_none());
    assert_eq!(decoded.replay_start_ns, TEST_REPLAY_START_NS);
    assert_eq!(decoded.replay_end_ns, TEST_REPLAY_END_NS);
    assert_eq!(decoded.knowledge_cutoff_ns, TEST_INGEST_TS_NS);

    let environment_digest = ContentDigest::digest_bytes(canonical);
    assert_eq!(materialized.artifact().digest(), &environment_digest);
    assert!(
        materialized
            .artifact()
            .id()
            .as_str()
            .ends_with(&environment_digest.to_string())
    );
    let engine =
        BacktestEngine::from_environment_artifact(materialized.artifact().id().clone(), canonical)
            .expect("canonical real OKX environment must load into the backtest engine");
    let binding = engine
        .run_provenance()
        .environment
        .as_ref()
        .expect("artifact loader must bind the execution environment");
    assert_eq!(binding.artifact().id(), materialized.artifact().id());
    assert_eq!(binding.artifact().digest(), &environment_digest);
    assert_eq!(binding.venues(), ["OKX"]);
    assert_eq!(binding.instruments(), ["ETH-USDT-SWAP.OKX"]);
    assert!(binding.market_contexts().is_empty());

    assert_eq!(materialized.source().id(), &source_id);
    assert_eq!(materialized.source().digest(), &fixture_digest);
    assert_eq!(
        materialized.derivation().inputs(),
        [materialized.source().clone()]
    );
    assert_eq!(materialized.derivation().output(), binding.artifact());
    assert_eq!(
        materialized.derivation().process().as_str(),
        OKX_SWAP_ENVIRONMENT_MATERIALIZER_ID
    );
    assert_eq!(materialized.derivation().timestamp_ns(), TEST_INGEST_TS_NS);
}

#[test]
fn okx_environment_materializer_rejects_failed_api_response() {
    let mut response: serde_json::Value = serde_json::from_slice(SWAP_FIXTURE).unwrap();
    response["code"] = serde_json::Value::String("51000".to_string());
    response["msg"] = serde_json::Value::String("test failure".to_string());
    let raw = serde_json::to_vec(&response).unwrap();

    let error = materialize_swap_backtest_environment(
        &raw,
        ArtifactId::new("dataset/okx/failed-response").unwrap(),
        "ETH-USDT-SWAP",
        UnixNanos::from(TEST_INGEST_TS_NS),
        UnixNanos::from(TEST_REPLAY_START_NS),
        UnixNanos::from(TEST_REPLAY_END_NS),
        Money::from("1_000_000 USDT"),
    )
    .unwrap_err();

    assert!(error.to_string().contains("code 51000"));
}

#[test]
fn okx_environment_materializer_rejects_missing_instrument() {
    let error = materialize_swap_backtest_environment(
        SWAP_FIXTURE,
        ArtifactId::new("dataset/okx/missing-instrument").unwrap(),
        "MISSING-USDT-SWAP",
        UnixNanos::from(TEST_INGEST_TS_NS),
        UnixNanos::from(TEST_REPLAY_START_NS),
        UnixNanos::from(TEST_REPLAY_END_NS),
        Money::from("1_000_000 USDT"),
    )
    .unwrap_err();

    assert!(error.to_string().contains("no definition"));
}

#[test]
fn okx_environment_materializer_rejects_wrong_settlement_balance() {
    let error = materialize_swap_backtest_environment(
        SWAP_FIXTURE,
        ArtifactId::new("dataset/okx/wrong-settlement-balance").unwrap(),
        "ETH-USDT-SWAP",
        UnixNanos::from(TEST_INGEST_TS_NS),
        UnixNanos::from(TEST_REPLAY_START_NS),
        UnixNanos::from(TEST_REPLAY_END_NS),
        Money::from("1_000_000 USD"),
    )
    .unwrap_err();

    assert!(error.to_string().contains("settlement currency"));
}
