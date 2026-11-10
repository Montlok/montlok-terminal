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

//! Typed materialization from raw OKX instrument responses into canonical backtest environments.
//!
//! The adapter owns the whole transformation from authenticated-or-pinned response bytes through
//! the existing OKX parser to the canonical Nautilus environment schema. The returned derivation
//! proves which bytes were transformed; it does not by itself authenticate the remote OKX server.

use std::collections::BTreeMap;

use nautilus_backtest::environment::{BacktestEnvironmentSpec, BacktestVenueSpec, MarketKind};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AccountType, BookType, OmsType},
    identifiers::Venue,
    instruments::InstrumentAny,
    types::Money,
};
use nautilus_provenance::{
    ArtifactId, ArtifactRef, ContentDigest, DerivationRecord, ProcessIdentity,
};

use crate::{
    common::{
        enums::{OKXInstrumentStatus, OKXInstrumentType},
        models::OKXInstrument,
        parse::parse_swap_instrument,
    },
    http::client::OKXResponse,
};

/// Versioned identity for the library-owned OKX swap environment materializer.
pub const OKX_SWAP_ENVIRONMENT_MATERIALIZER_ID: &str =
    "nautilus-okx/parse-swap-to-backtest-environment/v3";

/// Canonical environment bytes plus their exact source-to-output lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OKXBacktestEnvironmentArtifact {
    canonical_bytes: Vec<u8>,
    source: ArtifactRef,
    artifact: ArtifactRef,
    derivation: DerivationRecord,
}

impl OKXBacktestEnvironmentArtifact {
    /// Returns the unique canonical environment JSON bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns the content-addressed raw OKX response reference.
    #[must_use]
    pub const fn source(&self) -> &ArtifactRef {
        &self.source
    }

    /// Returns the content-addressed canonical environment reference.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    /// Returns the immutable source-to-environment derivation record.
    #[must_use]
    pub const fn derivation(&self) -> &DerivationRecord {
        &self.derivation
    }

    /// Consumes the materialization and returns its canonical JSON bytes.
    #[must_use]
    pub fn into_canonical_bytes(self) -> Vec<u8> {
        self.canonical_bytes
    }
}

/// Materializes one live OKX swap definition from an exact raw HTTP response.
///
/// `observed_at` is the local authenticated-ingestion observation time. The current OKX
/// instrument response has no response-level publication timestamp, so the parsed instrument's
/// event and initialization timestamps are both set to this value. The environment cannot replay
/// before that knowledge cutoff.
///
/// # Errors
///
/// Returns an error when the response is not an OKX success, the requested definition is missing,
/// duplicated, non-live, or not a swap, parsing fails, the starting currency differs from the
/// settlement currency, replay bounds precede observation, or canonical environment validation
/// fails.
pub fn materialize_swap_backtest_environment(
    raw_response: &[u8],
    source_id: ArtifactId,
    instrument_id: &str,
    observed_at: UnixNanos,
    replay_start: UnixNanos,
    replay_end: UnixNanos,
    starting_balance: Money,
) -> anyhow::Result<OKXBacktestEnvironmentArtifact> {
    anyhow::ensure!(
        !instrument_id.is_empty(),
        "OKX instrument ID must not be empty"
    );
    anyhow::ensure!(
        observed_at <= replay_start,
        "OKX environment observation time must not be after replay start"
    );
    anyhow::ensure!(
        replay_start < replay_end,
        "OKX environment replay interval must be non-empty"
    );

    let source = ArtifactRef::new(source_id, ContentDigest::digest_bytes(raw_response));
    let response: OKXResponse<OKXInstrument> = serde_json::from_slice(raw_response)?;
    anyhow::ensure!(
        response.code == "0",
        "OKX instrument response failed with code {}: {}",
        response.code,
        response.msg
    );

    let mut definitions = response
        .data
        .iter()
        .filter(|definition| definition.inst_id.as_str() == instrument_id);
    let definition = definitions
        .next()
        .ok_or_else(|| anyhow::anyhow!("OKX response has no definition for {instrument_id}"))?;
    anyhow::ensure!(
        definitions.next().is_none(),
        "OKX response contains duplicate definitions for {instrument_id}"
    );
    anyhow::ensure!(
        definition.inst_type == OKXInstrumentType::Swap,
        "OKX definition {instrument_id} is {:?}, not SWAP",
        definition.inst_type
    );
    anyhow::ensure!(
        definition.state == OKXInstrumentStatus::Live,
        "OKX definition {instrument_id} is {:?}, not LIVE",
        definition.state
    );

    let instrument = parse_swap_instrument(definition, None, None, None, None, observed_at)?;
    let InstrumentAny::CryptoPerpetual(perpetual) = &instrument else {
        anyhow::bail!("OKX swap parser did not produce a crypto perpetual");
    };
    anyhow::ensure!(
        starting_balance.currency == perpetual.settlement_currency,
        "Starting balance currency {} does not match {} settlement currency {}",
        starting_balance.currency,
        perpetual.id,
        perpetual.settlement_currency
    );

    let venue = BacktestVenueSpec::with_builtin_defaults(
        Venue::from("OKX"),
        MarketKind::Crypto,
        BTreeMap::new(),
        OmsType::Netting,
        AccountType::Margin,
        BookType::L1_MBP,
        vec![starting_balance],
    );
    let environment = BacktestEnvironmentSpec::new_for_replay(
        vec![venue],
        vec![instrument],
        None,
        replay_start.as_u64(),
        replay_end.as_u64(),
        observed_at.as_u64(),
    );
    let canonical_bytes = environment.to_canonical_json()?;
    let environment_digest = ContentDigest::digest_bytes(&canonical_bytes);
    let artifact = ArtifactRef::new(
        ArtifactId::new(format!(
            "environment/okx/{}/{environment_digest}",
            instrument_id.to_ascii_lowercase()
        ))?,
        environment_digest,
    );
    let derivation = DerivationRecord::new(
        vec![source.clone()],
        artifact.clone(),
        ProcessIdentity::new(OKX_SWAP_ENVIRONMENT_MATERIALIZER_ID)?,
        observed_at.as_u64(),
    );

    Ok(OKXBacktestEnvironmentArtifact {
        canonical_bytes,
        source,
        artifact,
        derivation,
    })
}
