// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

use std::{collections::BTreeMap, fs, path::Path};

use indexmap::IndexMap;
use nautilus_analysis::{EvaluationSuite, Gate};
use nautilus_backtest::{
    engine::{ArtifactDataLoadOptions, BacktestEngine, encode_backtest_data_artifact},
    environment::{BacktestEnvironmentSpec, BacktestVenueSpec, MarketKind},
    result::BacktestResultEvidenceMaterial,
};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_event_store::{
    EventStoreConfig, EventStoreLifecycleOptions, IsolatedVerifier, RedbBackend, RunIdentity,
    RunStatus, VerifyLimits,
};
use nautilus_model::{
    data::{Data, QuoteTick},
    enums::{AccountType, BookType, OmsType},
    identifiers::{InstrumentId, StrategyId, Venue},
    instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
    types::{Money, Price, Quantity},
};
use nautilus_provenance::{ArtifactId, ContentDigest, RunProvenance};
use nautilus_system::EventStoreTerminalStatus;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{config::StrategyConfig, core::StrategyCore},
};

#[derive(Debug)]
struct IsolatedReceiptStrategy {
    core: StrategyCore,
}

impl DataActor for IsolatedReceiptStrategy {}

nautilus_strategy!(IsolatedReceiptStrategy);

impl nautilus_system::trader::ArtifactConfiguredStrategy for IsolatedReceiptStrategy {
    type ArtifactConfig = StrategyConfig;

    fn from_artifact_config(config: Self::ArtifactConfig) -> anyhow::Result<Self> {
        Ok(Self {
            core: StrategyCore::new(config),
        })
    }

    fn artifact_config(&self) -> &Self::ArtifactConfig {
        self.core.config()
    }
}

fn deploy_isolated_helper() -> (tempfile::TempDir, IsolatedVerifier, ContentDigest) {
    let source = Path::new(env!("CARGO_BIN_EXE_nautilus-backtest-event-store-verify"));
    let deployment = tempfile::TempDir::new().expect("helper deployment tempdir");
    let deployed = deployment.path().join(format!(
        "nautilus-backtest-event-store-verify{}",
        std::env::consts::EXE_SUFFIX
    ));
    fs::copy(source, &deployed).expect("copy helper into immutable test deployment");
    fs::set_permissions(
        &deployed,
        fs::metadata(source)
            .expect("read helper permissions")
            .permissions(),
    )
    .expect("preserve helper executable permissions");
    let digest = ContentDigest::digest_bytes(&fs::read(&deployed).expect("read deployed helper"));
    let verifier =
        IsolatedVerifier::new(&deployed, digest).expect("measure private isolated helper snapshot");
    (deployment, verifier, digest)
}

#[test]
fn authenticated_backtest_seals_through_measured_isolated_helper() {
    let (_helper_deployment, isolated, helper_digest) = deploy_isolated_helper();
    let lifecycle_options =
        EventStoreLifecycleOptions::new().with_isolated_verifier(isolated.clone());
    let event_store_dir = tempfile::TempDir::new().expect("event-store tempdir");

    let venue = BacktestVenueSpec::with_builtin_defaults(
        Venue::from("BINANCE"),
        MarketKind::Crypto,
        BTreeMap::new(),
        OmsType::Netting,
        AccountType::Margin,
        BookType::L1_MBP,
        vec![Money::from("1_000_000 USDT")],
    );
    let environment = BacktestEnvironmentSpec::new_for_replay(
        vec![venue],
        vec![InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())],
        None,
        0,
        10,
        0,
    );
    let artifact = environment
        .to_canonical_json()
        .expect("canonical environment artifact");
    let event_store = EventStoreConfig {
        base_dir: event_store_dir.path().to_path_buf(),
        identity: RunIdentity {
            binary_hash: "caller-forged-binary".to_string(),
            schema_version: u32::MAX,
            crate_versions: "caller-forged-lockfile".to_string(),
            feature_flags: vec!["caller-forged-feature".to_string()],
            adapter_versions: IndexMap::from([(
                "caller-forged-adapter".to_string(),
                "999".to_string(),
            )]),
            config_hash: "caller-forged-config".to_string(),
            provenance: RunProvenance::default(),
            seed: Some(u64::MAX),
        },
        ..Default::default()
    };
    let mut engine = BacktestEngine::from_environment_artifact_with_event_store_options(
        ArtifactId::new("environment/test/isolated-redb-v1").expect("artifact id"),
        &artifact,
        event_store,
        lifecycle_options,
    )
    .expect("construct authenticated engine with isolated verifier");
    let instance_id = engine.instance_id().to_string();

    let quote = QuoteTick::new(
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        Price::from("1000.00"),
        Price::from("1001.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::from(1),
        UnixNanos::from(1),
    );
    let data_artifact =
        encode_backtest_data_artifact(&[Data::Quote(quote)]).expect("encode quote artifact");
    engine
        .add_data_from_artifact(
            ArtifactId::new("dataset/test/isolated-redb-v1").expect("dataset id"),
            &data_artifact,
            ArtifactDataLoadOptions::default(),
        )
        .expect("install measured quote");
    let strategy_config = serde_json::to_vec(&StrategyConfig {
        strategy_id: Some(StrategyId::from("IsolatedReceipt-001")),
        ..Default::default()
    })
    .expect("encode strategy config");
    engine
        .add_strategy_from_artifact::<IsolatedReceiptStrategy>(
            StrategyId::from("IsolatedReceipt-001"),
            ArtifactId::new("strategy/test/isolated-receipt-v1").expect("strategy artifact id"),
            ArtifactId::new("config/test/isolated-receipt-v1").expect("config artifact id"),
            &strategy_config,
        )
        .expect("install measured strategy");
    engine
        .require_run_provenance()
        .expect("lock run provenance before start");

    engine
        .run(None, None, None, false)
        .expect("run authenticated backtest");

    let result = engine.get_result();
    let receipt = result
        .event_store_receipt
        .as_ref()
        .expect("isolated verifier must issue event-store receipt");
    assert_eq!(receipt.terminal_status(), EventStoreTerminalStatus::Ended);
    assert!(!receipt.run_id().is_empty());
    assert!(receipt.high_watermark() >= 3);

    let run_path = event_store_dir
        .path()
        .join(&instance_id)
        .join(format!("{}.redb", receipt.run_id()));
    let isolated_receipt = isolated
        .verify_file(&run_path, VerifyLimits::default())
        .expect("independently verify sealed run in helper");
    assert_eq!(isolated_receipt.run_id, receipt.run_id());
    assert_eq!(isolated_receipt.status, RunStatus::Ended);
    assert_eq!(isolated_receipt.high_watermark, receipt.high_watermark());
    assert_eq!(isolated_receipt.store_digest, receipt.store_digest());
    assert_eq!(isolated_receipt.helper_measurement.digest, helper_digest);

    let manifests =
        RedbBackend::list_runs(event_store_dir.path(), &instance_id).expect("list manifests");
    let manifest = manifests
        .iter()
        .find(|manifest| manifest.run_id == receipt.run_id())
        .expect("receipt run manifest");
    let executable_digest = ContentDigest::digest_bytes(
        &fs::read(std::env::current_exe().expect("current test executable path"))
            .expect("measure current test executable"),
    );
    let workspace_lock_digest = ContentDigest::digest_bytes(include_bytes!("../../../Cargo.lock"));
    assert_eq!(manifest.binary_hash, executable_digest.to_string());
    assert_eq!(manifest.crate_versions, workspace_lock_digest.to_string());
    assert_ne!(manifest.config_hash, "caller-forged-config");
    for digest in [
        &manifest.binary_hash,
        &manifest.crate_versions,
        &manifest.config_hash,
    ] {
        assert!(!digest.is_empty());
        digest
            .parse::<ContentDigest>()
            .expect("library-measured manifest digest");
    }

    let suite = EvaluationSuite::new(
        "real-redb-release",
        "1",
        vec![Gate::Minimum {
            metric: "backtest.total_orders".to_string(),
            minimum: 0.0,
        }],
    )
    .expect("release evaluation suite");
    assert!(result.evaluate_for_promotion(&suite, None).passed());
    let error = result
        .promotion_attestation_for_release(&suite, None, "test-release-scope", UnixNanos::new(20))
        .expect_err("generic crypto profile is not production release eligible");
    assert!(matches!(
        error,
        nautilus_backtest::evidence::BacktestEvidenceError::ReleaseIneligibleMarketProfile {
            ref market_profile_id,
            ..
        } if market_profile_id == "market-profile/generic-crypto-perpetual/v1"
    ));
}

#[test]
fn authenticated_inputs_without_preexecution_lock_cannot_promote() {
    let (_helper_deployment, isolated, _helper_digest) = deploy_isolated_helper();
    let lifecycle_options = EventStoreLifecycleOptions::new().with_isolated_verifier(isolated);
    let event_store_dir = tempfile::TempDir::new().expect("event-store tempdir");
    let venue = BacktestVenueSpec::with_builtin_defaults(
        Venue::from("BINANCE"),
        MarketKind::Crypto,
        BTreeMap::new(),
        OmsType::Netting,
        AccountType::Margin,
        BookType::L1_MBP,
        vec![Money::from("1_000_000 USDT")],
    );
    let environment = BacktestEnvironmentSpec::new_for_replay(
        vec![venue],
        vec![InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())],
        None,
        0,
        10,
        0,
    );
    let artifact = environment
        .to_canonical_json()
        .expect("canonical environment artifact");
    let mut engine = BacktestEngine::from_environment_artifact_with_event_store_options(
        ArtifactId::new("environment/test/isolated-unlocked-v1").expect("artifact id"),
        &artifact,
        EventStoreConfig {
            base_dir: event_store_dir.path().to_path_buf(),
            ..Default::default()
        },
        lifecycle_options,
    )
    .expect("construct authenticated engine with isolated verifier");
    let quote = QuoteTick::new(
        InstrumentId::from("ETHUSDT-PERP.BINANCE"),
        Price::from("1000.00"),
        Price::from("1001.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::from(1),
        UnixNanos::from(1),
    );
    let data_artifact =
        encode_backtest_data_artifact(&[Data::Quote(quote)]).expect("encode quote artifact");
    engine
        .add_data_from_artifact(
            ArtifactId::new("dataset/test/isolated-unlocked-v1").expect("dataset id"),
            &data_artifact,
            ArtifactDataLoadOptions::default(),
        )
        .expect("install measured quote");
    let strategy_config = serde_json::to_vec(&StrategyConfig {
        strategy_id: Some(StrategyId::from("IsolatedUnlocked-001")),
        ..Default::default()
    })
    .expect("encode strategy config");
    engine
        .add_strategy_from_artifact::<IsolatedReceiptStrategy>(
            StrategyId::from("IsolatedUnlocked-001"),
            ArtifactId::new("strategy/test/isolated-unlocked-v1").expect("strategy artifact id"),
            ArtifactId::new("config/test/isolated-unlocked-v1").expect("config artifact id"),
            &strategy_config,
        )
        .expect("install measured strategy");

    engine
        .run(None, None, None, false)
        .expect("run unlocked authenticated backtest");

    let result = engine.get_result();
    let evidence = BacktestResultEvidenceMaterial::from(&result);
    assert!(!evidence.run_inputs_verified);
    assert!(result.event_store_receipt.is_some());
    let suite = EvaluationSuite::new(
        "unenforced-research",
        "1",
        vec![Gate::Minimum {
            metric: "backtest.total_orders".to_string(),
            minimum: 0.0,
        }],
    )
    .expect("research evaluation suite");
    assert!(result.evaluate_for_promotion(&suite, None).passed());
    assert_eq!(
        result
            .promotion_attestation_for_release(
                &suite,
                None,
                "must-not-release",
                UnixNanos::new(20),
            )
            .expect_err("unlocked inputs must not promote"),
        nautilus_backtest::evidence::BacktestEvidenceError::UnverifiedRunInputs
    );
}
