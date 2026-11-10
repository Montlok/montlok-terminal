// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Read-only OKX credential preflight. Defaults to demo and never submits or cancels orders.

use anyhow::Context;
use nautilus_okx::http::client::OKXHttpClient;

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build the OKX preflight runtime")?;
    runtime.block_on(async {
        let client = OKXHttpClient::from_env()?;
        let account = client.request_account_configuration().await?;
        let permissions = account
            .perm
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            permissions.contains(&"read_only") && permissions.contains(&"trade"),
            "credential must have read_only and trade permissions"
        );
        anyhow::ensure!(
            !permissions.contains(&"withdraw"),
            "credential must not have withdraw permission"
        );
        println!("environment=DEMO");
        println!("uid={}", account.uid);
        println!("main_uid={}", account.main_uid);
        println!("account_type={}", account.account_type);
        println!("account_level={}", account.acct_lv);
        println!("position_mode={}", account.pos_mode);
        println!("api_label={}", account.label);
        println!("permissions={}", account.perm);
        println!("ip_bound={}", !account.ip.trim().is_empty());
        Ok(())
    })
}
