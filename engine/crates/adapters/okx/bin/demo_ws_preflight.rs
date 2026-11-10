// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Authenticated OKX demo private WebSocket connect/subscribe/close canary.

use anyhow::Context;
use nautilus_model::identifiers::AccountId;
use nautilus_network::websocket::TransportBackend;
use nautilus_okx::{
    common::{
        enums::{OKXEnvironment, OKXInstrumentType, OKXRegion},
        urls::get_ws_base_url_private,
    },
    websocket::client::OKXWebSocketClient,
};

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX demo WS runtime")?;
    runtime.block_on(async {
        let url = get_ws_base_url_private(OKXRegion::Global, OKXEnvironment::Demo).to_string();
        let mut client = OKXWebSocketClient::with_credentials(
            Some(url),
            None,
            None,
            None,
            Some(AccountId::from("OKX-DEMO")),
            Some(20),
            Some(10),
            TransportBackend::default(),
            None,
        )?;
        client.connect().await?;
        client.wait_until_active(10.0).await?;
        println!("private_ws=authenticated");
        client.subscribe_orders(OKXInstrumentType::Spot).await?;
        client.subscribe_account().await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        println!("subscriptions=orders_spot,account");
        client.close().await?;
        println!("close=clean");
        Ok(())
    })
}
