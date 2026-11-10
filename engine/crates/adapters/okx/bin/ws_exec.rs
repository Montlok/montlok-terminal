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

//! Low-level OKX WebSocket execution example.
//!
//! The default invocation connects to demo and does not submit an order. Use `--demo-orders` to
//! submit on demo. Live connectivity requires `--live`; live order submission additionally
//! requires `--live-orders`, `--expected-account-id <id>`, and the exact confirmation token
//! `--live-confirmation OKX-LIVE-ORDERS:<id>`.

use std::time::Duration;

use futures_util::StreamExt;
use nautilus_model::{
    enums::{OrderSide, OrderType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    types::Quantity,
};
use nautilus_network::websocket::TransportBackend;
use nautilus_okx::{
    common::{
        enums::{OKXInstrumentType, OKXRegion, OKXTradeMode},
        urls::get_ws_base_url_private,
    },
    http::OKXHttpClient,
    safety::{OKXLaunchEnvironment, parse_okx_execution_launch_policy},
    websocket::client::OKXWebSocketClient,
};
use tokio::{pin, signal};

const ACCOUNT_ID: &str = "OKX-master";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    nautilus_common::logging::ensure_logging_initialized();

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
        "OKX WS execution example starting against {:?}; orders_enabled={}",
        launch.environment,
        launch.orders_enabled,
    );

    let rest_client = OKXHttpClient::with_credentials(
        None,
        None,
        None,
        None,
        60,
        3,
        1_000,
        10_000,
        launch.environment,
        None,
    )?;

    let inst_type = OKXInstrumentType::Swap;
    let (instruments, inst_id_codes) = rest_client.request_instruments(inst_type, None).await?;

    let ws_url = get_ws_base_url_private(OKXRegion::Global, launch.environment).to_string();
    let mut ws_client = OKXWebSocketClient::with_credentials(
        Some(ws_url),
        None,
        None,
        None,
        Some(AccountId::from(ACCOUNT_ID)),
        None,
        None,
        TransportBackend::default(),
        None,
    )?;
    ws_client.cache_instruments(&instruments);
    ws_client.cache_inst_id_codes(inst_id_codes);
    ws_client.connect().await?;

    // Subscribe to execution channels: orders and account updates
    ws_client.subscribe_orders(inst_type).await?;
    // ws_client.subscribe_account().await?;

    // Wait briefly to ensure subscriptions are active
    tokio::time::sleep(Duration::from_secs(1)).await;

    if launch.orders_enabled {
        let trader_id = TraderId::from("TRADER-001");
        let strategy_id = StrategyId::from("SCALPER-001");
        let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");
        let client_order_id = ClientOrderId::from("O20250711001");
        let order_side = OrderSide::Buy;
        let order_type = OrderType::Market;
        let quantity = Quantity::from("0.01");

        let response = ws_client
            .submit_order(
                trader_id,
                strategy_id,
                instrument_id,
                OKXTradeMode::Isolated,
                client_order_id,
                order_side,
                order_type,
                quantity,
                None, // time_in_force
                None, // price
                None, // trigger_price
                None, // post_only
                None, // reduce_only
                None, // quote_quantity
                None, // position_side
                None, // attach_algo_ords
                None, // px_usd
                None, // px_vol
                None, // speed_bump
                None, // outcome
                None, // slippage_pct
                None, // rpi
                None, // rpi_taker_access
                None, // rpi_px_round
            )
            .await;

        match response {
            Ok(response) => log::debug!("{response:?}"),
            Err(error) => log::error!("{error:?}"),
        }
    } else {
        log::warn!("Dry-run: order submission is disabled");
    }

    // Create a future that completes on CTRL+C
    let sigint = signal::ctrl_c();
    pin!(sigint);

    let stream = ws_client.stream();
    tokio::pin!(stream); // Pin the stream to allow polling in the loop

    loop {
        tokio::select! {
            Some(data) = stream.next() => {
                log::debug!("{data:?}");
            }
            _ = &mut sigint => {
                log::info!("Received SIGINT, closing connection...");
                ws_client.close().await?;
                break;
            }
            else => break,
        }
    }

    Ok(())
}
