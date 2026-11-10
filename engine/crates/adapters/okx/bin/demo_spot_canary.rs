// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Fixed, non-configurable OKX demo SPOT place/cancel canary.

use anyhow::Context;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    types::{Price, Quantity},
};
use nautilus_okx::{
    common::enums::{OKXInstrumentType, OKXTradeMode},
    http::client::OKXHttpClient,
};

const INSTRUMENT_ID: &str = "BTC-USDT.OKX";
const CLIENT_ORDER_ID: &str = "NCXDEMO00000002";
const PRICE: &str = "67000.0";
const QUANTITY: &str = "0.0002";

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX demo canary runtime")?;
    runtime.block_on(async {
        let client = OKXHttpClient::from_env()?;
        let account = client.request_account_configuration().await?;
        let permissions = account.perm.split(',').map(str::trim).collect::<Vec<_>>();
        anyhow::ensure!(
            permissions.contains(&"trade"),
            "demo key lacks trade permission"
        );
        anyhow::ensure!(
            !permissions.contains(&"withdraw"),
            "demo key must not have withdraw permission"
        );

        let (instruments, _) = client
            .request_instruments(OKXInstrumentType::Spot, None)
            .await?;
        client.cache_instruments(&instruments);
        let instrument_id = InstrumentId::from(INSTRUMENT_ID);
        let client_order_id = ClientOrderId::from(CLIENT_ORDER_ID);
        let response = client
            .place_order_with_domain_types(
                instrument_id,
                OKXTradeMode::Cash,
                client_order_id,
                OrderSide::Sell,
                OrderType::Limit,
                Quantity::from(QUANTITY),
                Some(TimeInForce::Gtc),
                Some(Price::from(PRICE)),
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
        let venue_order_id = response
            .ord_id
            .filter(|value| !value.is_empty())
            .map(|value| VenueOrderId::from(value.as_str()))
            .context("OKX accepted the canary without returning an order ID")?;
        println!("place=accepted");
        println!("instrument=BTC-USDT");
        println!("mode=cash");
        println!("post_only=true");
        println!("quantity={QUANTITY}");
        println!("price={PRICE}");

        client
            .cancel_order(instrument_id, Some(client_order_id), Some(venue_order_id))
            .await
            .map_err(anyhow::Error::new)?;
        println!("cancel=accepted");
        Ok(())
    })
}
