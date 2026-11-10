// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! OKX demo CAA canary: arm, place one fixed SPOT order, and prove venue cancellation.

use std::time::Duration;

use anyhow::Context;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    types::{Price, Quantity},
};
use nautilus_okx::{
    common::{
        consts::OKX_NAUTILUS_BROKER_ID,
        enums::{OKXInstrumentType, OKXTradeMode},
    },
    http::client::OKXHttpClient,
};

const INSTRUMENT_ID: &str = "BTC-USDT.OKX";
const CLIENT_ORDER_ID: &str = "NCXDEMOCAA000001";

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX demo CAA runtime")?;
    runtime.block_on(async {
        let client = OKXHttpClient::from_env()?;
        let (instruments, _) = client
            .request_instruments(OKXInstrumentType::Spot, None)
            .await?;
        client.cache_instruments(&instruments);
        let instrument_id = InstrumentId::from(INSTRUMENT_ID);
        let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);

        client
            .arm_cancel_all_after(10, Some(OKX_NAUTILUS_BROKER_ID))
            .await?;
        println!("caa=armed timeout=10");
        let placed = client
            .place_order_with_domain_types(
                instrument_id,
                OKXTradeMode::Cash,
                client_order_id,
                OrderSide::Sell,
                OrderType::Limit,
                Quantity::from("0.0002"),
                Some(TimeInForce::Gtc),
                Some(Price::from("67000.0")),
                Some(true),
                Some(false),
                Some(false),
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
            )
            .await
            .map_err(anyhow::Error::new)?;
        let venue_order_id = placed
            .ord_id
            .filter(|value| !value.is_empty())
            .map(|value| VenueOrderId::from(value.as_str()))
            .context("OKX accepted the CAA canary without an order ID")?;
        println!("place=accepted");
        tokio::time::sleep(Duration::from_secs(13)).await;
        let report = client
            .request_order_status_report(
                AccountId::from("OKX-DEMO"),
                instrument_id,
                client_order_id,
                Some(venue_order_id),
            )
            .await?
            .context("CAA canary disappeared without an authoritative order report")?;
        println!("order_status={}", report.order_status);
        anyhow::ensure!(
            report.order_status.is_closed(),
            "CAA canary remains open after timeout: {}",
            report.order_status
        );

        let state = client
            .request_account_state(AccountId::from("OKX-DEMO"))
            .await?;
        let btc = state
            .balances
            .iter()
            .find(|balance| balance.currency.code.as_str() == "BTC")
            .context("BTC balance missing after CAA canary")?;
        println!("btc_locked={}", btc.locked);
        anyhow::ensure!(
            btc.locked.raw == 0,
            "BTC remains locked after CAA cancellation"
        );
        Ok(())
    })
}
