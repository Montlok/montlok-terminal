// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Read-only OKX demo balance query. Prints only non-zero currency balances.

use anyhow::Context;
use nautilus_model::identifiers::AccountId;
use nautilus_okx::http::client::OKXHttpClient;

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX demo balance runtime")?;
    runtime.block_on(async {
        let client = OKXHttpClient::from_env()?;
        let state = client
            .request_account_state(AccountId::from("OKX-DEMO"))
            .await?;
        println!("environment=DEMO");
        println!("account_type={}", state.account_type);
        for balance in state.balances {
            if balance.total.raw != 0 || balance.locked.raw != 0 || balance.free.raw != 0 {
                println!(
                    "currency={} total={} free={} locked={}",
                    balance.currency, balance.total, balance.free, balance.locked
                );
            }
        }
        Ok(())
    })
}
