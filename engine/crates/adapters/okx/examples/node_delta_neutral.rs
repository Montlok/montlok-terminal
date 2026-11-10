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

//! Example demonstrating the delta-neutral volatility strategy on OKX options.
//!
//! This runs the `DeltaNeutralVol` strategy which:
//! 1. Discovers BTC option instruments from the cache
//! 2. Selects OTM call and put strikes for a short strangle
//! 3. Subscribes to option greeks and hedge instrument quotes
//! 4. Enters the strangle with IV-based limit orders (via `px_vol` param)
//! 5. Delta-hedges with the underlying inverse perpetual on a periodic timer
//!
//! OKX BTC options are inverse (coin-margined, settled in BTC). The hedge
//! instrument is the BTC-USD-SWAP inverse perpetual, so both legs share the
//! same margin currency.
//!
//! The strategy uses `px_vol` to price option orders by implied volatility,
//! which the OKX adapter translates to the `pxVol` API field.
//!
//! The default invocation connects to demo without attaching the order-producing strategy. Use
//! `--demo-orders` to enable it on demo. Live connectivity requires `--live`; live orders
//! additionally require `--live-orders`, `--expected-account-id <id>`, and
//! `--live-confirmation OKX-LIVE-ORDERS:<id>`.
//!
//! Run with: `cargo run --example okx-delta-neutral --package nautilus-okx --features examples`
//!
//! Required credential environment variables:
//! - `OKX_API_KEY`.
//! - `OKX_API_SECRET`.
//! - `OKX_API_PASSPHRASE`.

use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};
use nautilus_okx::{
    common::{consts::OKX_CLIENT_ID, enums::OKXInstrumentType},
    config::{OKXDataClientConfig, OKXExecClientConfig},
    factories::{OKXDataClientFactory, OKXExecutionClientFactory},
    safety::{OKXLaunchEnvironment, parse_okx_execution_launch_policy},
};
use nautilus_trading::examples::strategies::delta_neutral_vol::{
    DeltaNeutralVol, DeltaNeutralVolConfig,
};

const TRADER_ID: &str = "TESTER-001";
const ACCOUNT_ID: &str = "OKX-001";
const NODE_NAME: &str = "OKX-DELTA-NEUTRAL-001";

const OPTION_FAMILY: &str = "BTC";
const INSTRUMENT_FAMILY: &str = "BTC-USD";
const HEDGE_INSTRUMENT_ID: &str = "BTC-USD-SWAP.OKX";

const TARGET_CALL_DELTA: f64 = 0.20;
const TARGET_PUT_DELTA: f64 = -0.20;
const CONTRACTS: u64 = 1;
const REHEDGE_DELTA_THRESHOLD: f64 = 0.5;
const REHEDGE_INTERVAL_SECS: u64 = 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let live = std::env::var("OKX_LIVE").ok();
    let demo_orders = std::env::var("OKX_DEMO_ORDERS").ok();
    let live_orders = std::env::var("OKX_LIVE_ORDERS").ok();
    let live_confirmation = std::env::var("OKX_LIVE_CONFIRMATION").ok();
    let expected_account_id = std::env::var("OKX_EXPECTED_ACCOUNT_ID").ok();
    let launch = parse_okx_execution_launch_policy(
        &args,
        OKXLaunchEnvironment {
            live: live.as_deref(),
            demo_orders: demo_orders.as_deref(),
            live_orders: live_orders.as_deref(),
            live_confirmation: live_confirmation.as_deref(),
            expected_account_id: expected_account_id.as_deref(),
            configured_account_id: Some(ACCOUNT_ID),
        },
    )?;
    log::warn!(
        "OKX delta-neutral example starting against {:?}; orders_enabled={}",
        launch.environment,
        launch.orders_enabled,
    );

    let environment = Environment::Live;
    let okx_environment = launch.environment;
    let trader_id = TraderId::from(TRADER_ID);
    let account_id = AccountId::from(ACCOUNT_ID);
    let client_id = *OKX_CLIENT_ID;
    let hedge_instrument_id = InstrumentId::from(HEDGE_INSTRUMENT_ID);

    let data_config = OKXDataClientConfig {
        api_key: None,        // Will use 'OKX_API_KEY' env var
        api_secret: None,     // Will use 'OKX_API_SECRET' env var
        api_passphrase: None, // Will use 'OKX_API_PASSPHRASE' env var
        instrument_types: vec![OKXInstrumentType::Option, OKXInstrumentType::Swap],
        instrument_families: Some(vec![INSTRUMENT_FAMILY.to_string()]),
        environment: okx_environment,
        ..Default::default()
    };

    let exec_config = OKXExecClientConfig {
        trader_id,
        account_id,
        api_key: None,        // Will use 'OKX_API_KEY' env var
        api_secret: None,     // Will use 'OKX_API_SECRET' env var
        api_passphrase: None, // Will use 'OKX_API_PASSPHRASE' env var
        instrument_types: vec![OKXInstrumentType::Option, OKXInstrumentType::Swap],
        instrument_families: Some(vec![INSTRUMENT_FAMILY.to_string()]),
        environment: okx_environment,
        ..Default::default()
    };

    let data_factory = OKXDataClientFactory::new();
    let exec_factory = OKXExecutionClientFactory::new();

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(NODE_NAME.to_string())
        .add_data_client(None, Box::new(data_factory), Box::new(data_config))?
        .add_exec_client(None, Box::new(exec_factory), Box::new(exec_config))?
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .build()?;

    if launch.orders_enabled {
        let mut strategy_config = DeltaNeutralVolConfig::builder()
            .option_family(OPTION_FAMILY.to_string())
            .hedge_instrument_id(hedge_instrument_id)
            .client_id(client_id)
            .target_call_delta(TARGET_CALL_DELTA)
            .target_put_delta(TARGET_PUT_DELTA)
            .contracts(CONTRACTS)
            .rehedge_delta_threshold(REHEDGE_DELTA_THRESHOLD)
            .rehedge_interval_secs(REHEDGE_INTERVAL_SECS)
            .enter_strangle(true)
            .build();

        // OKX forbids hyphens in client order IDs
        strategy_config.base.use_hyphens_in_client_order_ids = false;
        node.add_strategy(DeltaNeutralVol::new(strategy_config))?;
    } else {
        log::warn!("Dry-run: delta-neutral strategy is not attached");
    }
    node.run().await?;

    Ok(())
}
