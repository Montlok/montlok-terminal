// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Cleans fixed OKX demo canary state: cancels BTC-USDT orders and disables CAA.

use anyhow::Context;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use nautilus_okx::{
    common::{consts::OKX_NAUTILUS_BROKER_ID, enums::OKXInstrumentType},
    http::client::OKXHttpClient,
};

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX demo cleanup runtime")?;
    runtime.block_on(async {
        let client = OKXHttpClient::from_env()?;
        let (instruments, _) = client
            .request_instruments(OKXInstrumentType::Spot, None)
            .await?;
        client.cache_instruments(&instruments);
        let canceled = client
            .cancel_all_orders(InstrumentId::from("BTC-USDT.OKX"))
            .await
            .map_err(anyhow::Error::new)?;
        println!("cancel_requests={}", canceled.len());
        client
            .disable_cancel_all_after(Some(OKX_NAUTILUS_BROKER_ID))
            .await?;
        println!("caa=disabled");
        let state = client
            .request_account_state(AccountId::from("OKX-DEMO"))
            .await?;
        let btc = state
            .balances
            .iter()
            .find(|balance| balance.currency.code.as_str() == "BTC")
            .context("BTC balance missing after cleanup")?;
        println!("btc_locked={}", btc.locked);
        anyhow::ensure!(btc.locked.raw == 0, "BTC remains locked after cleanup");
        Ok(())
    })
}
