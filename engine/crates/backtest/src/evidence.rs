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

//! Canonical backtest evidence and native promotion evaluation.

use std::{
    fs,
    path::{Path, PathBuf},
};

use ahash::AHashMap;
use nautilus_analysis::{
    ArtifactReference, AttestationError, EvaluationGate, EvaluationOutcome, EvaluationSuite,
    Observations, PromotionAttestation, SignedAttestationError, SignedPromotionAttestation,
};
use nautilus_core::{UUID4, UnixNanos};
#[cfg(test)]
use nautilus_event_store::{RunStatus, Verifier, VerifyError};
use nautilus_event_store::{VerifyLimitResource, VerifyLimits};
use nautilus_provenance::ContentDigest;
use nautilus_system::{EventStoreSealReceipt, EventStoreTerminalStatus};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::result::{BacktestResult, BacktestResultEvidenceMaterial};

const EVIDENCE_DOMAIN: &[u8] = b"nautilus-backtest/result-evidence/v1";

// Release eligibility is library policy, not a runner-supplied capability claim. The current
// profiles are either schema-only equities contracts or generic crypto simulation without venue
// fidelity, so the allowlist intentionally remains empty until an executable profile earns
// release-grade acceptance coverage.
const RELEASE_ELIGIBLE_MARKET_PROFILE_IDS: &[&str] = &[];

#[derive(Clone, Copy, Debug)]
enum ReleaseEligibilityPolicy {
    Production,
    #[cfg(test)]
    CurrentProfilesForPipelineTests,
}

impl ReleaseEligibilityPolicy {
    fn accepts(self, market_profile_id: &str) -> bool {
        match self {
            Self::Production => RELEASE_ELIGIBLE_MARKET_PROFILE_IDS.contains(&market_profile_id),
            #[cfg(test)]
            Self::CurrentProfilesForPipelineTests => matches!(
                market_profile_id,
                "market-profile/generic-crypto-perpetual/v1"
                    | "market-profile/china-a-shares-schema/v1"
                    | "market-profile/hong-kong-equities-schema/v1"
                    | "market-profile/united-states-equities-schema/v1"
            ),
        }
    }
}

/// Current wire version for an independently verifiable backtest evidence bundle.
pub const BACKTEST_EVIDENCE_BUNDLE_VERSION: u32 = 1;

/// Current wire version for an external evaluator signing request.
pub const RELEASE_PROMOTION_SIGNING_REQUEST_VERSION: u32 = 1;

/// Errors which prevent a backtest result from becoming promotion evidence.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BacktestEvidenceError {
    /// The result was not issued and sealed by a backtest engine.
    #[error("backtest evidence was not sealed by the engine")]
    Unsealed,
    /// Public result fields changed after the engine issued the result.
    #[error("backtest evidence was modified after the engine sealed it")]
    Tampered,
    /// The engine did not measure and match the run's environment, data, strategy, and
    /// configuration inputs.
    #[error("backtest release promotion requires engine-verified run inputs")]
    UnverifiedRunInputs,
    /// The engine did not assign a run identity.
    #[error("backtest evidence requires a completed run ID")]
    MissingRunId,
    /// The run was not bound to a cleanly sealed authoritative event store.
    #[error("backtest release promotion requires a verified event-store seal receipt")]
    MissingEventStoreReceipt,
    /// The attached event-store receipt is not a clean terminal run commitment.
    #[error("backtest event-store receipt is not a verified clean Ended run")]
    InvalidEventStoreReceipt,
    /// The run did not bind the exact strategy release artifact under evaluation.
    #[error("backtest promotion requires a bound strategy artifact")]
    MissingStrategyArtifact,
    /// The run did not bind any immutable dataset artifact.
    #[error("backtest release promotion requires at least one bound dataset artifact")]
    MissingDataArtifact,
    /// The run did not bind the exact execution environment used by the simulation.
    #[error("backtest release promotion requires a bound execution-environment artifact")]
    MissingEnvironmentArtifact,
    /// The environment binding omitted its exact paired market scope.
    #[error("backtest release promotion requires a scoped execution environment")]
    MissingEnvironmentScope,
    /// A comparative release evaluation used a baseline from another exact market scope.
    #[error(
        "backtest release baseline market scope mismatch: candidate {candidate}, baseline {baseline}"
    )]
    BaselineMarketScopeMismatch {
        /// Candidate paired market-scope commitment.
        candidate: ContentDigest,
        /// Baseline paired market-scope commitment.
        baseline: ContentDigest,
    },
    /// The run did not bind the exact immutable runtime configuration.
    #[error("backtest release promotion requires a bound configuration artifact")]
    MissingConfigArtifact,
    /// Evaluation provenance may only be produced after the sealed run.
    #[error("backtest release promotion rejects a preclaimed evaluation artifact")]
    PreclaimedEvaluationArtifact,
    /// Promotion provenance may only be produced after evaluation.
    #[error("backtest release promotion rejects a preclaimed promotion artifact")]
    PreclaimedPromotionArtifact,
    /// A bound market profile has not earned library-owned strong-release eligibility.
    #[error(
        "market profile `{market_profile_id}` for materialized venue `{materialized_venue_id}` is not eligible for strong release promotion"
    )]
    ReleaseIneligibleMarketProfile {
        /// Materialized runtime venue bound by the run provenance.
        materialized_venue_id: String,
        /// Exact versioned profile rejected by the library-owned allowlist.
        market_profile_id: String,
    },
    /// Evaluation or artifact binding did not satisfy attestation invariants.
    #[error(transparent)]
    Attestation(#[from] AttestationError),
    /// A candidate or baseline event-store base directory was not canonical and absolute.
    #[error("invalid {role} event-store base directory: {reason}")]
    InvalidEventStoreBaseDir {
        /// Whether the path belongs to the candidate or baseline.
        role: &'static str,
        /// Operator-readable validation failure.
        reason: String,
    },
    /// A baseline result was supplied without its authoritative event-store directory.
    #[error("baseline release evidence requires a baseline event-store base directory")]
    MissingBaselineEventStoreBaseDir,
    /// A baseline directory was supplied despite there being no baseline result.
    #[error("baseline event-store base directory was supplied without a baseline result")]
    UnexpectedBaselineEventStoreBaseDir,
    /// The external evaluator signing identity or payload could not be constructed.
    #[error("release signing request is invalid: {0}")]
    SigningRequest(String),
}

/// Failures detected while independently rebuilding a release signing request.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ExternalEvaluatorError {
    /// The request does not carry an evaluator-policy trusted authentication of the runner and
    /// its complete result semantics.
    #[error(
        "production release signing requires an authenticated runner signature binding the runner binary, run inputs, provenance, complete result evidence, and exact event-store receipt, or evaluator-owned semantic reexecution"
    )]
    AuthenticatedRunnerRequired,
    /// JSON did not decode into the strict versioned request schema.
    #[error("invalid release signing request JSON: {0}")]
    Deserialize(String),
    /// The top-level request version is not supported.
    #[error(
        "unsupported release signing request version {found}; supported version is {supported}"
    )]
    UnsupportedRequestVersion { found: u32, supported: u32 },
    /// A candidate or baseline bundle version is not supported.
    #[error("unsupported {role} evidence bundle version {found}; supported version is {supported}")]
    UnsupportedBundleVersion {
        role: &'static str,
        found: u32,
        supported: u32,
    },
    /// A request locator is missing, relative, non-canonical, or outside evaluator policy.
    #[error("invalid {role} event-store locator: {reason}")]
    InvalidEventStoreLocator { role: &'static str, reason: String },
    /// Baseline bundle and evaluator baseline-root policy do not agree.
    #[error("baseline bundle/root mismatch: {0}")]
    BaselinePolicy(String),
    /// Canonical backtest evidence failed its release checks.
    #[error("invalid {role} backtest evidence: {reason}")]
    InvalidBacktestEvidence { role: &'static str, reason: String },
    /// The serialized evidence digest does not match the complete result material.
    #[error("{role} canonical backtest evidence digest mismatch")]
    EvidenceDigestMismatch { role: &'static str },
    /// The duplicated bundle and result receipts disagree.
    #[error("{role} event-store receipt disagrees with canonical result material")]
    ReceiptMismatch { role: &'static str },
    /// Result, locator, and receipt identities do not agree.
    #[error("{role} event-store identity mismatch: {reason}")]
    EventStoreIdentityMismatch { role: &'static str, reason: String },
    /// The independently trusted event-store file could not be opened.
    #[error("could not open {role} event store: {reason}")]
    EventStoreOpen { role: &'static str, reason: String },
    /// Full off-trader event-store verification did not produce a clean Ended commitment.
    #[error("{role} event-store verification failed: {reason}")]
    EventStoreVerification { role: &'static str, reason: String },
    /// Evaluator-owned resource policy rejected an untrusted event store before a full scan.
    #[error(
        "{role} event-store verification {resource} limit exceeded: observed {observed}, limit {limit}"
    )]
    VerificationLimitExceeded {
        role: &'static str,
        resource: VerifyLimitResource,
        observed: u64,
        limit: u64,
    },
    /// The full serialized evaluation suite is invalid.
    #[error("invalid evaluation suite definition: {0}")]
    InvalidSuite(String),
    /// Rebuilt release claims did not match the request's claimed attestation.
    #[error("release attestation does not match independently rebuilt evidence and outcome")]
    AttestationMismatch,
    /// The request's claimed HSM payload does not match the rebuilt attestation.
    #[error("evaluator signing payload does not match independently rebuilt attestation")]
    SigningPayloadMismatch,
    /// The rebuilt attestation or signing payload violated release invariants.
    #[error("could not rebuild release attestation: {0}")]
    AttestationRebuild(String),
}

/// Canonical absolute location of one authoritative redb event-store run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventStoreEvidenceLocator {
    pub base_dir: PathBuf,
    pub instance_id: String,
    pub run_id: String,
}

/// Complete, versioned evidence needed to independently inspect one backtest result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestEvidenceBundle {
    pub version: u32,
    pub result: BacktestResultEvidenceMaterial,
    pub event_store_locator: EventStoreEvidenceLocator,
    pub event_store_receipt: EventStoreSealReceipt,
    pub evidence_digest: ContentDigest,
}

/// Serializable full ordered evaluation-suite definition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationSuiteDefinition {
    pub suite_id: String,
    pub suite_version: String,
    pub gates: Vec<EvaluationGate>,
}

/// Versioned external evaluator request carrying all evidence needed before signing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePromotionSigningRequest {
    pub version: u32,
    pub evaluator_key_id: String,
    pub suite: EvaluationSuiteDefinition,
    pub promotion_scope: String,
    pub evaluated_at: UnixNanos,
    pub candidate: BacktestEvidenceBundle,
    pub baseline: Option<BacktestEvidenceBundle>,
    pub attestation: PromotionAttestation,
    pub signing_payload_hex: String,
}

/// Output of an evaluator-authorized reconstruction; only this type exposes signable bytes.
///
/// Production entry points do not currently construct this type because the wire request has no
/// evaluator-policy trusted runner authentication. The private `cfg(test)` pipeline exercises the
/// consistency mechanism only; it is not a production trust decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedReleasePromotionSigningRequest {
    evaluator_key_id: String,
    attestation: PromotionAttestation,
    signing_payload: [u8; 32],
}

impl EvaluationSuiteDefinition {
    fn from_suite(suite: &EvaluationSuite) -> Self {
        Self {
            suite_id: suite.suite_id().to_string(),
            suite_version: suite.version().to_string(),
            gates: suite.gates().to_vec(),
        }
    }

    #[cfg(test)]
    fn to_suite(&self) -> Result<EvaluationSuite, ExternalEvaluatorError> {
        EvaluationSuite::new(
            self.suite_id.clone(),
            self.suite_version.clone(),
            self.gates.clone(),
        )
        .map_err(|error| ExternalEvaluatorError::InvalidSuite(error.to_string()))
    }
}

impl BacktestEvidenceBundle {
    fn from_engine_result_with_policy(
        result: &BacktestResult,
        base_dir: &Path,
        role: &'static str,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<Self, BacktestEvidenceError> {
        result.verify_engine_evidence_seal()?;
        result.validate_release_provenance_for_policy(release_policy)?;
        let receipt = result
            .event_store_receipt
            .clone()
            .ok_or(BacktestEvidenceError::MissingEventStoreReceipt)?;
        let base_dir = canonicalize_engine_store_base_dir(role, base_dir)?;
        Ok(Self {
            version: BACKTEST_EVIDENCE_BUNDLE_VERSION,
            result: BacktestResultEvidenceMaterial::from(result),
            event_store_locator: EventStoreEvidenceLocator {
                base_dir,
                instance_id: result.instance_id.to_string(),
                run_id: receipt.run_id().to_string(),
            },
            event_store_receipt: receipt,
            evidence_digest: result.evidence_digest(),
        })
    }

    #[cfg(test)]
    fn verify_for_external_evaluator(
        &self,
        role: &'static str,
        trusted_base_dir: &Path,
        limits: VerifyLimits,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<BacktestResult, ExternalEvaluatorError> {
        if self.version != BACKTEST_EVIDENCE_BUNDLE_VERSION {
            return Err(ExternalEvaluatorError::UnsupportedBundleVersion {
                role,
                found: self.version,
                supported: BACKTEST_EVIDENCE_BUNDLE_VERSION,
            });
        }

        let trusted_base_dir = canonicalize_external_store_base_dir(role, trusted_base_dir)?;
        let locator_base_dir =
            canonicalize_external_store_base_dir(role, &self.event_store_locator.base_dir)?;
        if locator_base_dir != self.event_store_locator.base_dir {
            return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
                role,
                reason: format!(
                    "serialized base directory `{}` is not canonical (`{}`)",
                    self.event_store_locator.base_dir.display(),
                    locator_base_dir.display(),
                ),
            });
        }
        if locator_base_dir != trusted_base_dir {
            return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
                role,
                reason: format!(
                    "serialized base directory `{}` is outside the independently trusted root `{}`",
                    locator_base_dir.display(),
                    trusted_base_dir.display(),
                ),
            });
        }

        let result = self.result.to_result();
        result
            .validate_release_provenance_for_policy(release_policy)
            .map_err(|error| ExternalEvaluatorError::InvalidBacktestEvidence {
                role,
                reason: error.to_string(),
            })?;
        if result.evidence_digest() != self.evidence_digest {
            return Err(ExternalEvaluatorError::EvidenceDigestMismatch { role });
        }
        if result.event_store_receipt.as_ref() != Some(&self.event_store_receipt) {
            return Err(ExternalEvaluatorError::ReceiptMismatch { role });
        }

        let expected_instance_id = result.instance_id.to_string();
        if self.event_store_locator.instance_id != expected_instance_id {
            return Err(ExternalEvaluatorError::EventStoreIdentityMismatch {
                role,
                reason: format!(
                    "locator instance_id `{}` disagrees with result instance_id `{expected_instance_id}`",
                    self.event_store_locator.instance_id,
                ),
            });
        }
        if self.event_store_locator.run_id != self.event_store_receipt.run_id() {
            return Err(ExternalEvaluatorError::EventStoreIdentityMismatch {
                role,
                reason: format!(
                    "locator run_id `{}` disagrees with receipt run_id `{}`",
                    self.event_store_locator.run_id,
                    self.event_store_receipt.run_id(),
                ),
            });
        }

        validate_external_run_path(role, &locator_base_dir, &self.event_store_locator)?;

        let verifier = Verifier::open_redb_with_limits(
            &locator_base_dir,
            &self.event_store_locator.instance_id,
            &self.event_store_locator.run_id,
            limits,
        )
        .map_err(|error| map_external_verifier_open_error(role, error))?;
        let manifest = verifier.backend().manifest().map_err(|error| {
            ExternalEvaluatorError::EventStoreVerification {
                role,
                reason: error.to_string(),
            }
        })?;
        if manifest.instance_id != self.event_store_locator.instance_id
            || manifest.run_id != self.event_store_locator.run_id
        {
            return Err(ExternalEvaluatorError::EventStoreIdentityMismatch {
                role,
                reason: format!(
                    "manifest identity ({}, {}) disagrees with locator ({}, {})",
                    manifest.instance_id,
                    manifest.run_id,
                    self.event_store_locator.instance_id,
                    self.event_store_locator.run_id,
                ),
            });
        }
        if manifest.provenance.canonical_digest() != result.provenance.canonical_digest() {
            return Err(ExternalEvaluatorError::EventStoreIdentityMismatch {
                role,
                reason: format!(
                    "sealed manifest provenance {} disagrees with canonical result provenance {}",
                    manifest.provenance.canonical_digest(),
                    result.provenance.canonical_digest(),
                ),
            });
        }
        // Recheck after the open to narrow (but not eliminate) path substitution races. A
        // production evaluator should ingest immutable evidence into evaluator-owned storage and
        // open with platform no-follow semantics.
        validate_external_run_path(role, &locator_base_dir, &self.event_store_locator)?;

        let report = verifier
            .verify()
            .map_err(|error| map_external_verifier_verify_error(role, error))?;
        if !report.is_clean()
            || report.status != RunStatus::Ended
            || report.run_id != self.event_store_receipt.run_id()
            || report.high_watermark != self.event_store_receipt.high_watermark()
            || report.store_digest != Some(self.event_store_receipt.store_digest())
        {
            return Err(ExternalEvaluatorError::EventStoreVerification {
                role,
                reason: format!(
                    "expected clean Ended receipt (run_id={}, hwm={}, digest={}); report was status={:?}, run_id={}, hwm={}, digest={:?}, findings={:?}",
                    self.event_store_receipt.run_id(),
                    self.event_store_receipt.high_watermark(),
                    self.event_store_receipt.store_digest(),
                    report.status,
                    report.run_id,
                    report.high_watermark,
                    report.store_digest,
                    report.findings,
                ),
            });
        }

        Ok(result.issue_engine_evidence_seal())
    }
}

impl ReleasePromotionSigningRequest {
    /// Strictly deserializes a typed signing request.
    ///
    /// # Errors
    ///
    /// Returns an error for missing fields, unknown fields, malformed values, or invalid JSON.
    pub fn from_json(value: &str) -> Result<Self, ExternalEvaluatorError> {
        serde_json::from_str(value)
            .map_err(|error| ExternalEvaluatorError::Deserialize(error.to_string()))
    }

    /// Refuses to turn a self-authored runner request into signable bytes.
    ///
    /// `trusted_candidate_base_dir` and `trusted_baseline_base_dir` are insufficient authorization:
    /// an intact event store proves its own receipt and provenance but does not derive the supplied
    /// portfolio statistics or prove that `run_inputs_verified` was issued by trusted runner code.
    /// Until an evaluator-policy trusted runner signature binds the measured runner binary, exact
    /// run inputs/provenance, full result evidence, and exact event-store receipt (or the evaluator
    /// performs a deterministic semantic rerun), this method fails closed without exposing bytes
    /// suitable for signing.
    ///
    /// # Errors
    ///
    /// Always returns [`ExternalEvaluatorError::AuthenticatedRunnerRequired`] in production.
    pub fn verify_for_external_evaluator(
        &self,
        trusted_candidate_base_dir: &Path,
        trusted_baseline_base_dir: Option<&Path>,
    ) -> Result<VerifiedReleasePromotionSigningRequest, ExternalEvaluatorError> {
        self.verify_for_external_evaluator_with_limits(
            trusted_candidate_base_dir,
            trusted_baseline_base_dir,
            VerifyLimits::default(),
        )
    }

    /// Refuses to rebuild this unauthenticated request under evaluator-owned resource budgets.
    ///
    /// Resource limits do not authenticate caller-supplied result semantics. This entry point
    /// therefore has the same production fail-closed behavior as
    /// [`Self::verify_for_external_evaluator`].
    ///
    /// # Errors
    ///
    /// Always returns [`ExternalEvaluatorError::AuthenticatedRunnerRequired`] in production.
    pub fn verify_for_external_evaluator_with_limits(
        &self,
        trusted_candidate_base_dir: &Path,
        trusted_baseline_base_dir: Option<&Path>,
        limits: VerifyLimits,
    ) -> Result<VerifiedReleasePromotionSigningRequest, ExternalEvaluatorError> {
        let _ = (
            trusted_candidate_base_dir,
            trusted_baseline_base_dir,
            limits,
        );
        Err(ExternalEvaluatorError::AuthenticatedRunnerRequired)
    }

    #[cfg(test)]
    fn verify_for_external_evaluator_for_pipeline_test(
        &self,
        trusted_candidate_base_dir: &Path,
        trusted_baseline_base_dir: Option<&Path>,
    ) -> Result<VerifiedReleasePromotionSigningRequest, ExternalEvaluatorError> {
        self.verify_for_external_evaluator_with_policy(
            trusted_candidate_base_dir,
            trusted_baseline_base_dir,
            VerifyLimits::default(),
            ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests,
        )
    }

    #[cfg(test)]
    fn verify_for_external_evaluator_with_limits_for_pipeline_test(
        &self,
        trusted_candidate_base_dir: &Path,
        trusted_baseline_base_dir: Option<&Path>,
        limits: VerifyLimits,
    ) -> Result<VerifiedReleasePromotionSigningRequest, ExternalEvaluatorError> {
        self.verify_for_external_evaluator_with_policy(
            trusted_candidate_base_dir,
            trusted_baseline_base_dir,
            limits,
            ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests,
        )
    }

    #[cfg(test)]
    fn verify_for_external_evaluator_with_policy(
        &self,
        trusted_candidate_base_dir: &Path,
        trusted_baseline_base_dir: Option<&Path>,
        limits: VerifyLimits,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<VerifiedReleasePromotionSigningRequest, ExternalEvaluatorError> {
        if self.version != RELEASE_PROMOTION_SIGNING_REQUEST_VERSION {
            return Err(ExternalEvaluatorError::UnsupportedRequestVersion {
                found: self.version,
                supported: RELEASE_PROMOTION_SIGNING_REQUEST_VERSION,
            });
        }
        let suite = self.suite.to_suite()?;
        let candidate = self.candidate.verify_for_external_evaluator(
            "candidate",
            trusted_candidate_base_dir,
            limits,
            release_policy,
        )?;
        let baseline = match (&self.baseline, trusted_baseline_base_dir) {
            (Some(bundle), Some(base_dir)) => Some(bundle.verify_for_external_evaluator(
                "baseline",
                base_dir,
                limits,
                release_policy,
            )?),
            (Some(_), None) => {
                return Err(ExternalEvaluatorError::BaselinePolicy(
                    "request carries baseline evidence but evaluator supplied no trusted baseline root"
                        .to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(ExternalEvaluatorError::BaselinePolicy(
                    "evaluator supplied a trusted baseline root but request carries no baseline evidence"
                        .to_string(),
                ));
            }
            (None, None) => None,
        };

        let rebuilt = candidate
            .promotion_attestation_for_release_with_policy(
                &suite,
                baseline.as_ref(),
                self.promotion_scope.clone(),
                self.evaluated_at,
                release_policy,
            )
            .map_err(|error| ExternalEvaluatorError::AttestationRebuild(error.to_string()))?;
        if rebuilt != self.attestation {
            return Err(ExternalEvaluatorError::AttestationMismatch);
        }

        let signing_payload = rebuilt
            .evaluator_signing_payload(&self.evaluator_key_id)
            .map_err(|error| ExternalEvaluatorError::AttestationRebuild(error.to_string()))?;
        let claimed_payload = decode_signing_payload_hex(&self.signing_payload_hex)?;
        if signing_payload != claimed_payload {
            return Err(ExternalEvaluatorError::SigningPayloadMismatch);
        }

        Ok(VerifiedReleasePromotionSigningRequest {
            evaluator_key_id: self.evaluator_key_id.clone(),
            attestation: rebuilt,
            signing_payload,
        })
    }
}

impl VerifiedReleasePromotionSigningRequest {
    /// Returns the evaluator key identity bound into the payload.
    #[must_use]
    pub fn evaluator_key_id(&self) -> &str {
        &self.evaluator_key_id
    }

    /// Returns the fully rebuilt unsigned release claims.
    #[must_use]
    pub const fn attestation(&self) -> &PromotionAttestation {
        &self.attestation
    }

    /// Returns the canonical 32 bytes an authorized evaluator may now sign.
    #[must_use]
    pub const fn signing_payload(&self) -> &[u8; 32] {
        &self.signing_payload
    }

    /// Assembles a detached evaluator signature with the independently rebuilt attestation.
    ///
    /// # Errors
    ///
    /// Returns an error if `signature` is not a valid signed-attestation envelope value.
    pub fn into_signed(
        self,
        signature: impl Into<Vec<u8>>,
    ) -> Result<SignedPromotionAttestation, SignedAttestationError> {
        self.attestation
            .into_signed(self.evaluator_key_id, signature)
    }
}

#[cfg(test)]
fn map_external_verifier_open_error(
    role: &'static str,
    error: VerifyError,
) -> ExternalEvaluatorError {
    match error {
        VerifyError::LimitExceeded {
            resource,
            observed,
            limit,
        } => ExternalEvaluatorError::VerificationLimitExceeded {
            role,
            resource,
            observed,
            limit,
        },
        other => ExternalEvaluatorError::EventStoreOpen {
            role,
            reason: other.to_string(),
        },
    }
}

#[cfg(test)]
fn map_external_verifier_verify_error(
    role: &'static str,
    error: VerifyError,
) -> ExternalEvaluatorError {
    match error {
        VerifyError::LimitExceeded {
            resource,
            observed,
            limit,
        } => ExternalEvaluatorError::VerificationLimitExceeded {
            role,
            resource,
            observed,
            limit,
        },
        other => ExternalEvaluatorError::EventStoreVerification {
            role,
            reason: other.to_string(),
        },
    }
}

fn canonicalize_engine_store_base_dir(
    role: &'static str,
    path: &Path,
) -> Result<PathBuf, BacktestEvidenceError> {
    if !path.is_absolute() {
        return Err(BacktestEvidenceError::InvalidEventStoreBaseDir {
            role,
            reason: format!("`{}` is not absolute", path.display()),
        });
    }
    fs::canonicalize(path).map_err(|error| BacktestEvidenceError::InvalidEventStoreBaseDir {
        role,
        reason: format!("could not canonicalize `{}`: {error}", path.display()),
    })
}

#[cfg(test)]
fn canonicalize_external_store_base_dir(
    role: &'static str,
    path: &Path,
) -> Result<PathBuf, ExternalEvaluatorError> {
    if !path.is_absolute() {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!("`{}` is not absolute", path.display()),
        });
    }
    fs::canonicalize(path).map_err(|error| ExternalEvaluatorError::InvalidEventStoreLocator {
        role,
        reason: format!("could not canonicalize `{}`: {error}", path.display()),
    })
}

#[cfg(test)]
fn validate_external_run_path(
    role: &'static str,
    trusted_base_dir: &Path,
    locator: &EventStoreEvidenceLocator,
) -> Result<PathBuf, ExternalEvaluatorError> {
    if !is_single_normal_path_component(&locator.instance_id) {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "invalid instance_id path component `{}`",
                locator.instance_id
            ),
        });
    }
    if !is_single_normal_path_component(&locator.run_id) {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!("invalid run_id path component `{}`", locator.run_id),
        });
    }

    let instance_dir = trusted_base_dir.join(&locator.instance_id);
    let expected_filename = format!("{}.redb", locator.run_id);
    let run_file = instance_dir.join(&expected_filename);
    let instance_metadata = fs::symlink_metadata(&instance_dir).map_err(|error| {
        ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "could not inspect instance directory `{}`: {error}",
                instance_dir.display(),
            ),
        }
    })?;
    if instance_metadata.file_type().is_symlink() {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "instance directory `{}` is a symbolic link",
                instance_dir.display(),
            ),
        });
    }
    let file_metadata = fs::symlink_metadata(&run_file).map_err(|error| {
        ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "could not inspect run file `{}`: {error}",
                run_file.display()
            ),
        }
    })?;
    if file_metadata.file_type().is_symlink() {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!("run file `{}` is a symbolic link", run_file.display()),
        });
    }

    let canonical_run_file = fs::canonicalize(&run_file).map_err(|error| {
        ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "could not canonicalize run file `{}`: {error}",
                run_file.display()
            ),
        }
    })?;
    if canonical_run_file != run_file
        || canonical_run_file.parent() != Some(instance_dir.as_path())
        || !canonical_run_file.starts_with(trusted_base_dir)
        || canonical_run_file
            .file_name()
            .and_then(|name| name.to_str())
            != Some(expected_filename.as_str())
    {
        return Err(ExternalEvaluatorError::InvalidEventStoreLocator {
            role,
            reason: format!(
                "run file `{}` does not resolve exactly to trusted root/instance/run.redb (`{}`)",
                run_file.display(),
                canonical_run_file.display(),
            ),
        });
    }
    Ok(canonical_run_file)
}

#[cfg(test)]
fn is_single_normal_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

fn encode_signing_payload_hex(payload: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in payload {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
fn decode_signing_payload_hex(value: &str) -> Result<[u8; 32], ExternalEvaluatorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ExternalEvaluatorError::SigningPayloadMismatch);
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

#[cfg(test)]
fn decode_hex_nibble(value: u8) -> Result<u8, ExternalEvaluatorError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ExternalEvaluatorError::SigningPayloadMismatch),
    }
}

impl BacktestResult {
    /// Converts native portfolio statistics into stable quantitative gate observations.
    ///
    /// Metric names are namespaced to prevent collisions across general, returns, and
    /// currency-specific PnL statistics.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "evaluation observations use f64 and backtest counts cannot realistically exceed its integer precision"
    )]
    pub fn evaluation_observations(&self) -> Observations {
        let mut observations = Observations::from([
            (
                "backtest.elapsed_time_secs".to_string(),
                self.elapsed_time_secs,
            ),
            ("backtest.iterations".to_string(), self.iterations as f64),
            (
                "backtest.total_events".to_string(),
                self.total_events as f64,
            ),
            (
                "backtest.total_orders".to_string(),
                self.total_orders as f64,
            ),
            (
                "backtest.total_positions".to_string(),
                self.total_positions as f64,
            ),
        ]);

        extend_observations(&mut observations, "returns", &self.stats_returns);
        extend_observations(&mut observations, "general", &self.stats_general);
        for (currency, metrics) in &self.stats_pnls {
            extend_observations(&mut observations, &format!("pnl.{currency}"), metrics);
        }
        observations
    }

    /// Evaluates this result against `suite`, optionally comparing a baseline result.
    #[must_use]
    pub fn evaluate_for_promotion(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
    ) -> EvaluationOutcome {
        let candidate_observations = self.evaluation_observations();
        let baseline_observations = baseline.map(Self::evaluation_observations);
        suite.evaluate(&candidate_observations, baseline_observations.as_ref())
    }

    /// Computes a canonical digest over run identity, provenance, statistics, and returns.
    ///
    /// The encoding is independent of Serde map ordering and binds the result to all immutable
    /// input artifacts through [`Self::provenance`].
    #[must_use]
    pub fn evidence_digest(&self) -> ContentDigest {
        let mut payload = Vec::new();
        write_bytes(&mut payload, EVIDENCE_DOMAIN);
        write_str(&mut payload, &self.trader_id);
        write_str(&mut payload, &self.machine_id);
        write_uuid(&mut payload, self.instance_id);
        write_optional_str(&mut payload, self.run_config_id.as_deref());
        write_optional_uuid(&mut payload, self.run_id);
        write_bytes(&mut payload, self.provenance.canonical_digest().as_bytes());
        write_event_store_receipt(&mut payload, self.event_store_receipt.as_ref());
        payload.push(u8::from(self.run_inputs_verified));
        write_optional_nanos(&mut payload, self.run_started);
        write_optional_nanos(&mut payload, self.run_finished);
        write_optional_nanos(&mut payload, self.backtest_start);
        write_optional_nanos(&mut payload, self.backtest_end);
        write_f64(&mut payload, self.elapsed_time_secs);
        write_usize(&mut payload, self.iterations);
        write_usize(&mut payload, self.total_events);
        write_usize(&mut payload, self.total_orders);
        write_usize(&mut payload, self.total_positions);
        write_string_map(&mut payload, &self.summary);
        write_nested_f64_map(&mut payload, &self.stats_pnls);
        write_f64_map(&mut payload, &self.stats_returns);
        write_f64_map(&mut payload, &self.stats_general);
        write_usize(&mut payload, self.returns_series.len());
        for (timestamp, value) in &self.returns_series {
            write_u64(&mut payload, timestamp.as_u64());
            write_f64(&mut payload, *value);
        }
        ContentDigest::digest_bytes(&payload)
    }

    /// Returns the immutable artifact identity of this exact backtest result.
    ///
    /// # Errors
    ///
    /// Returns an error if the result was not sealed by an engine, changed after sealing, or has
    /// no assigned run ID.
    pub fn evidence_artifact_reference(&self) -> Result<ArtifactReference, BacktestEvidenceError> {
        self.verify_engine_evidence_seal()?;
        let run_id = self.run_id.ok_or(BacktestEvidenceError::MissingRunId)?;
        ArtifactReference::new(
            format!("backtest-result:{}:{run_id}", self.instance_id),
            self.evidence_digest().to_string(),
        )
        .map_err(BacktestEvidenceError::from)
    }

    /// Evaluates and attests this exact result for promotion.
    ///
    /// The attestation candidate is the canonical backtest evidence artifact, rather than the
    /// strategy alone. Therefore evidence cannot be reused across different datasets, configs,
    /// run identities, statistics, or return series.
    ///
    /// # Errors
    ///
    /// Returns an error when either result was not sealed by an engine, changed after sealing,
    /// lacks a run ID, the suite fails, or a required baseline artifact is absent.
    pub fn promotion_attestation(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        evaluated_at: UnixNanos,
    ) -> Result<PromotionAttestation, BacktestEvidenceError> {
        let candidate = self.evidence_artifact_reference()?;
        let baseline_artifact = baseline
            .map(Self::evidence_artifact_reference)
            .transpose()?;
        let outcome = self.evaluate_for_promotion(suite, baseline);
        PromotionAttestation::from_passing_outcome(
            &outcome,
            candidate,
            baseline_artifact,
            evaluated_at,
        )
        .map_err(BacktestEvidenceError::from)
    }

    /// Creates a deployment-scoped attestation for the exact strategy release bound to this run.
    ///
    /// Unlike [`Self::promotion_attestation`], this strong form separates the release candidate
    /// from its sealed backtest evidence. The returned claims remain unsigned; an external
    /// evaluator must independently authenticate the runner and evidence before signing them for
    /// strategy artifact trust admission.
    ///
    /// # Errors
    ///
    /// Returns an error for unsealed or modified results, incomplete or preclaimed run provenance,
    /// failed gates, missing baseline evidence, or an empty promotion scope.
    pub fn promotion_attestation_for_release(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        promotion_scope: impl Into<String>,
        evaluated_at: UnixNanos,
    ) -> Result<PromotionAttestation, BacktestEvidenceError> {
        self.promotion_attestation_for_release_with_policy(
            suite,
            baseline,
            promotion_scope,
            evaluated_at,
            ReleaseEligibilityPolicy::Production,
        )
    }

    #[cfg(test)]
    fn promotion_attestation_for_release_for_pipeline_test(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        promotion_scope: impl Into<String>,
        evaluated_at: UnixNanos,
    ) -> Result<PromotionAttestation, BacktestEvidenceError> {
        self.promotion_attestation_for_release_with_policy(
            suite,
            baseline,
            promotion_scope,
            evaluated_at,
            ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests,
        )
    }

    fn promotion_attestation_for_release_with_policy(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        promotion_scope: impl Into<String>,
        evaluated_at: UnixNanos,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<PromotionAttestation, BacktestEvidenceError> {
        let evidence_artifact = self.evidence_artifact_reference()?;
        self.validate_release_provenance_for_policy(release_policy)?;
        let strategy = self
            .provenance
            .strategy
            .as_ref()
            .ok_or(BacktestEvidenceError::MissingStrategyArtifact)?;
        let candidate = ArtifactReference::new(
            strategy.artifact().id().to_string(),
            strategy.artifact().digest().to_string(),
        )?;
        let config = self
            .provenance
            .config
            .as_ref()
            .ok_or(BacktestEvidenceError::MissingConfigArtifact)?;
        let strategy_config_artifact =
            ArtifactReference::new(config.id().to_string(), config.digest().to_string())?;
        let market_scope_commitment = self
            .provenance
            .environment
            .as_ref()
            .and_then(|environment| environment.scope())
            .ok_or(BacktestEvidenceError::MissingEnvironmentScope)?
            .market_scope_commitment();
        if let Some(baseline) = baseline {
            baseline.validate_release_provenance_for_policy(release_policy)?;
            let baseline_market_scope_commitment = baseline
                .provenance
                .environment
                .as_ref()
                .and_then(|environment| environment.scope())
                .ok_or(BacktestEvidenceError::MissingEnvironmentScope)?
                .market_scope_commitment();
            if baseline_market_scope_commitment != market_scope_commitment {
                return Err(BacktestEvidenceError::BaselineMarketScopeMismatch {
                    candidate: market_scope_commitment,
                    baseline: baseline_market_scope_commitment,
                });
            }
        }
        let baseline_artifact = baseline
            .map(Self::evidence_artifact_reference)
            .transpose()?;
        let outcome = self.evaluate_for_promotion(suite, baseline);
        PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            candidate,
            evidence_artifact,
            self.provenance.canonical_digest().to_string(),
            market_scope_commitment.to_string(),
            strategy_config_artifact,
            strategy.entrypoint(),
            strategy.runtime_id(),
            baseline_artifact,
            promotion_scope,
            evaluated_at,
        )
        .map_err(BacktestEvidenceError::from)
    }

    /// Builds the complete typed request an external evaluator must independently verify.
    ///
    /// The serialized candidate and optional baseline bundles contain every field in the
    /// canonical result digest, an explicit caller-serialized `run_inputs_verified` bit, a
    /// canonical absolute redb locator, the verifier-issued receipt, and the claimed digest. This
    /// request is an audit/transport envelope, not authorization to sign. The production external
    /// verifier intentionally returns [`ExternalEvaluatorError::AuthenticatedRunnerRequired`]
    /// until trusted runner authentication or evaluator-owned semantic reexecution is integrated.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid result evidence, a missing or extra baseline directory,
    /// non-canonical store roots, failed gates, or an invalid evaluator key identity.
    #[expect(
        clippy::too_many_arguments,
        reason = "every external signing input and evidence location is named explicitly"
    )]
    pub fn release_promotion_signing_request(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        candidate_event_store_base_dir: &Path,
        baseline_event_store_base_dir: Option<&Path>,
        promotion_scope: &str,
        evaluated_at: UnixNanos,
        evaluator_key_id: &str,
    ) -> Result<ReleasePromotionSigningRequest, BacktestEvidenceError> {
        self.release_promotion_signing_request_with_policy(
            suite,
            baseline,
            candidate_event_store_base_dir,
            baseline_event_store_base_dir,
            promotion_scope,
            evaluated_at,
            evaluator_key_id,
            ReleaseEligibilityPolicy::Production,
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "test helper mirrors the fully explicit production signing API"
    )]
    fn release_promotion_signing_request_for_pipeline_test(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        candidate_event_store_base_dir: &Path,
        baseline_event_store_base_dir: Option<&Path>,
        promotion_scope: &str,
        evaluated_at: UnixNanos,
        evaluator_key_id: &str,
    ) -> Result<ReleasePromotionSigningRequest, BacktestEvidenceError> {
        self.release_promotion_signing_request_with_policy(
            suite,
            baseline,
            candidate_event_store_base_dir,
            baseline_event_store_base_dir,
            promotion_scope,
            evaluated_at,
            evaluator_key_id,
            ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "private policy injection keeps production release eligibility library-owned"
    )]
    fn release_promotion_signing_request_with_policy(
        &self,
        suite: &EvaluationSuite,
        baseline: Option<&Self>,
        candidate_event_store_base_dir: &Path,
        baseline_event_store_base_dir: Option<&Path>,
        promotion_scope: &str,
        evaluated_at: UnixNanos,
        evaluator_key_id: &str,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<ReleasePromotionSigningRequest, BacktestEvidenceError> {
        let baseline_bundle = match (baseline, baseline_event_store_base_dir) {
            (Some(result), Some(base_dir)) => {
                Some(BacktestEvidenceBundle::from_engine_result_with_policy(
                    result,
                    base_dir,
                    "baseline",
                    release_policy,
                )?)
            }
            (Some(_), None) => {
                return Err(BacktestEvidenceError::MissingBaselineEventStoreBaseDir);
            }
            (None, Some(_)) => {
                return Err(BacktestEvidenceError::UnexpectedBaselineEventStoreBaseDir);
            }
            (None, None) => None,
        };
        let candidate_bundle = BacktestEvidenceBundle::from_engine_result_with_policy(
            self,
            candidate_event_store_base_dir,
            "candidate",
            release_policy,
        )?;
        let attestation = self.promotion_attestation_for_release_with_policy(
            suite,
            baseline,
            promotion_scope,
            evaluated_at,
            release_policy,
        )?;
        let signing_payload = attestation
            .evaluator_signing_payload(evaluator_key_id)
            .map_err(|error| BacktestEvidenceError::SigningRequest(error.to_string()))?;

        Ok(ReleasePromotionSigningRequest {
            version: RELEASE_PROMOTION_SIGNING_REQUEST_VERSION,
            evaluator_key_id: evaluator_key_id.to_string(),
            suite: EvaluationSuiteDefinition::from_suite(suite),
            promotion_scope: promotion_scope.to_string(),
            evaluated_at,
            candidate: candidate_bundle,
            baseline: baseline_bundle,
            attestation,
            signing_payload_hex: encode_signing_payload_hex(&signing_payload),
        })
    }

    fn validate_release_provenance(&self) -> Result<(), BacktestEvidenceError> {
        self.validate_release_provenance_with_policy(ReleaseEligibilityPolicy::Production)
    }

    fn validate_release_provenance_for_policy(
        &self,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<(), BacktestEvidenceError> {
        match release_policy {
            ReleaseEligibilityPolicy::Production => self.validate_release_provenance(),
            #[cfg(test)]
            ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests => {
                self.validate_release_provenance_with_policy(release_policy)
            }
        }
    }

    fn validate_release_provenance_with_policy(
        &self,
        release_policy: ReleaseEligibilityPolicy,
    ) -> Result<(), BacktestEvidenceError> {
        if !self.run_inputs_verified {
            return Err(BacktestEvidenceError::UnverifiedRunInputs);
        }
        let receipt = self
            .event_store_receipt
            .as_ref()
            .ok_or(BacktestEvidenceError::MissingEventStoreReceipt)?;
        if receipt.terminal_status() != EventStoreTerminalStatus::Ended
            || receipt.run_id().is_empty()
            || receipt.high_watermark() == 0
        {
            return Err(BacktestEvidenceError::InvalidEventStoreReceipt);
        }
        if self.provenance.data.is_empty() {
            return Err(BacktestEvidenceError::MissingDataArtifact);
        }
        let environment = self
            .provenance
            .environment
            .as_ref()
            .ok_or(BacktestEvidenceError::MissingEnvironmentArtifact)?;
        let scope = environment
            .scope()
            .ok_or(BacktestEvidenceError::MissingEnvironmentScope)?;
        if self.provenance.strategy.is_none() {
            return Err(BacktestEvidenceError::MissingStrategyArtifact);
        }
        if self.provenance.config.is_none() {
            return Err(BacktestEvidenceError::MissingConfigArtifact);
        }
        if self.provenance.evaluation.is_some() {
            return Err(BacktestEvidenceError::PreclaimedEvaluationArtifact);
        }
        if self.provenance.promotion.is_some() {
            return Err(BacktestEvidenceError::PreclaimedPromotionArtifact);
        }
        for market_scope in scope.market_scopes() {
            if !release_policy.accepts(market_scope.market_profile_id()) {
                return Err(BacktestEvidenceError::ReleaseIneligibleMarketProfile {
                    materialized_venue_id: market_scope.materialized_venue_id().to_string(),
                    market_profile_id: market_scope.market_profile_id().to_string(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn verify_engine_evidence_seal(&self) -> Result<(), BacktestEvidenceError> {
        let sealed_digest = self
            .engine_evidence_seal_digest()
            .ok_or(BacktestEvidenceError::Unsealed)?;
        if sealed_digest != self.evidence_digest() {
            return Err(BacktestEvidenceError::Tampered);
        }
        Ok(())
    }
}

fn write_event_store_receipt(payload: &mut Vec<u8>, receipt: Option<&EventStoreSealReceipt>) {
    let Some(receipt) = receipt else {
        payload.push(0);
        return;
    };
    payload.push(1);
    write_str(payload, receipt.run_id());
    write_u64(payload, receipt.high_watermark());
    payload.push(match receipt.terminal_status() {
        EventStoreTerminalStatus::Ended => 1,
        EventStoreTerminalStatus::CrashedRecovered => 2,
        EventStoreTerminalStatus::Quarantined => 3,
    });
    write_bytes(payload, receipt.store_digest().as_bytes());
}

fn extend_observations(
    observations: &mut Observations,
    namespace: &str,
    values: &AHashMap<String, f64>,
) {
    observations.extend(
        values
            .iter()
            .map(|(metric, value)| (format!("{namespace}.{metric}"), *value)),
    );
}

fn write_optional_str(payload: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            payload.push(1);
            write_str(payload, value);
        }
        None => payload.push(0),
    }
}

fn write_optional_uuid(payload: &mut Vec<u8>, value: Option<UUID4>) {
    match value {
        Some(value) => {
            payload.push(1);
            write_uuid(payload, value);
        }
        None => payload.push(0),
    }
}

fn write_uuid(payload: &mut Vec<u8>, value: UUID4) {
    write_str(payload, &value.to_string());
}

fn write_optional_nanos(payload: &mut Vec<u8>, value: Option<UnixNanos>) {
    match value {
        Some(value) => {
            payload.push(1);
            write_u64(payload, value.as_u64());
        }
        None => payload.push(0),
    }
}

fn write_string_map(payload: &mut Vec<u8>, values: &AHashMap<String, String>) {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    write_usize(payload, entries.len());
    for (key, value) in entries {
        write_str(payload, key);
        write_str(payload, value);
    }
}

fn write_f64_map(payload: &mut Vec<u8>, values: &AHashMap<String, f64>) {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    write_usize(payload, entries.len());
    for (key, value) in entries {
        write_str(payload, key);
        write_f64(payload, *value);
    }
}

fn write_nested_f64_map(payload: &mut Vec<u8>, values: &AHashMap<String, AHashMap<String, f64>>) {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    write_usize(payload, entries.len());
    for (key, value) in entries {
        write_str(payload, key);
        write_f64_map(payload, value);
    }
}

fn write_f64(payload: &mut Vec<u8>, value: f64) {
    let bits = if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    };
    payload.extend_from_slice(&bits.to_be_bytes());
}

fn write_str(payload: &mut Vec<u8>, value: &str) {
    write_bytes(payload, value.as_bytes());
}

fn write_bytes(payload: &mut Vec<u8>, value: &[u8]) {
    write_usize(payload, value.len());
    payload.extend_from_slice(value);
}

fn write_usize(payload: &mut Vec<u8>, value: usize) {
    payload.extend_from_slice(&(value as u128).to_be_bytes());
}

fn write_u64(payload: &mut Vec<u8>, value: u64) {
    payload.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use indexmap::IndexMap;
    use nautilus_analysis::{AttestationError, Gate};
    use nautilus_cryptography::signing::ed25519_sign_bytes;
    use nautilus_event_store::{
        AppendEntry, EventStore, EventStoreEntry, Headers, RedbBackend, RegisteredComponents,
        RunManifest, RunStartedPayload, Topic, codec, compute_entry_hash,
    };
    use nautilus_provenance::{
        ArtifactId, ArtifactRef, DatasetBinding, ExecutionEnvironmentBinding,
        ExecutionEnvironmentScope, ExecutionMarketScope, ProcessIdentity, StrategyBinding,
    };
    use rstest::rstest;
    use tempfile::TempDir;
    use ustr::Ustr;

    use super::*;

    fn artifact(id: &str, content: &[u8]) -> ArtifactRef {
        ArtifactRef::new(
            ArtifactId::new(id).unwrap(),
            ContentDigest::digest_bytes(content),
        )
    }

    fn dataset(id: &str, content: &[u8]) -> DatasetBinding {
        DatasetBinding::new(
            artifact(id, content),
            ContentDigest::digest_bytes([b"materialized/".as_slice(), content].concat().as_slice()),
            ProcessIdentity::new("test-decoder/v1").unwrap(),
            0,
            1,
        )
        .unwrap()
    }

    fn strategy(id: &str, content: &[u8]) -> StrategyBinding {
        StrategyBinding::new(
            artifact(id, content),
            "test::MomentumStrategy",
            "MomentumStrategy-001",
        )
        .unwrap()
    }

    fn environment() -> ExecutionEnvironmentBinding {
        environment_binding(true)
    }

    fn unscoped_environment() -> ExecutionEnvironmentBinding {
        environment_binding(false)
    }

    fn environment_binding(scoped: bool) -> ExecutionEnvironmentBinding {
        environment_binding_with_us_route(scoped, "execution-route/broker-smart-router/us-broker")
    }

    fn environment_binding_with_us_route(
        scoped: bool,
        us_execution_route_id: &str,
    ) -> ExecutionEnvironmentBinding {
        let environment_artifact = artifact("environment/cross-market", b"environment-v1");
        let loader = ProcessIdentity::new("environment-loader/v1").unwrap();
        let contexts = vec![
            artifact("market-context/okx", b"okx-context-v1"),
            artifact("market-context/xhkg", b"xhkg-context-v1"),
            artifact("market-context/xnas", b"xnas-context-v1"),
            artifact("market-context/xshg", b"xshg-context-v1"),
            artifact("market-context/fx", b"fx-context-v1"),
        ];
        let venues = vec![
            "OKX".to_string(),
            "XHKG".to_string(),
            "XNAS".to_string(),
            "XSHG".to_string(),
        ];
        let instruments = vec![
            "BTC-USDT.OKX".to_string(),
            "0700.XHKG".to_string(),
            "AAPL.XNAS".to_string(),
            "600000.XSHG".to_string(),
        ];
        if scoped {
            let market_scopes = vec![
                ExecutionMarketScope::new(
                    "OKX",
                    "market-profile/generic-crypto-perpetual/v1",
                    "listing/exchange/OKX",
                    "execution-route/direct-venue/OKX",
                )
                .unwrap(),
                ExecutionMarketScope::new(
                    "XHKG",
                    "market-profile/hong-kong-equities-schema/v1",
                    "listing/exchange/XHKG",
                    "execution-route/broker-smart-router/hk-broker",
                )
                .unwrap(),
                ExecutionMarketScope::new(
                    "XNAS",
                    "market-profile/united-states-equities-schema/v1",
                    "listing/exchange/XNAS",
                    us_execution_route_id,
                )
                .unwrap(),
                ExecutionMarketScope::new(
                    "XSHG",
                    "market-profile/china-a-shares-schema/v1",
                    "listing/exchange/XSHG",
                    "execution-route/direct-venue/XSHG",
                )
                .unwrap(),
            ];
            ExecutionEnvironmentBinding::new_scoped(
                environment_artifact,
                loader,
                contexts,
                venues,
                instruments,
                ExecutionEnvironmentScope::new(3, 5, 2, market_scopes).unwrap(),
            )
            .unwrap()
        } else {
            ExecutionEnvironmentBinding::new(
                environment_artifact,
                loader,
                contexts,
                venues,
                instruments,
            )
            .unwrap()
        }
    }

    fn environment_binding_for_profile(profile_id: &str) -> ExecutionEnvironmentBinding {
        ExecutionEnvironmentBinding::new_scoped(
            artifact("environment/single-market", b"environment-single-market-v1"),
            ProcessIdentity::new("environment-loader/v1").unwrap(),
            vec![artifact("market-context/test", b"test-context-v1")],
            vec!["TEST".to_string()],
            vec!["INSTRUMENT.TEST".to_string()],
            ExecutionEnvironmentScope::new(
                3,
                5,
                2,
                vec![
                    ExecutionMarketScope::new(
                        "TEST",
                        profile_id,
                        "listing/exchange/TEST",
                        "execution-route/direct-venue/TEST",
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn unsealed_result(run_id: Option<UUID4>) -> BacktestResult {
        BacktestResult {
            trader_id: "TRADER-001".to_string(),
            machine_id: "machine-1".to_string(),
            instance_id: UUID4::from("11111111-1111-4111-8111-111111111111"),
            run_config_id: Some("config-1".to_string()),
            run_id,
            provenance: nautilus_provenance::RunProvenance {
                data: vec![dataset("dataset/btc-usdt", b"ticks")],
                environment: Some(environment()),
                strategy: Some(strategy("strategy/momentum", b"strategy")),
                config: Some(artifact("config/research", b"config")),
                ..Default::default()
            },
            run_started: Some(UnixNanos::new(1)),
            run_finished: Some(UnixNanos::new(2)),
            backtest_start: Some(UnixNanos::new(3)),
            backtest_end: Some(UnixNanos::new(4)),
            elapsed_time_secs: 1.5,
            iterations: 10,
            total_events: 20,
            total_orders: 2,
            total_positions: 1,
            summary: AHashMap::from([("PnL (total)".to_string(), "10 USD".to_string())]),
            stats_pnls: AHashMap::new(),
            stats_returns: AHashMap::from([("Sharpe Ratio".to_string(), 1.25)]),
            stats_general: AHashMap::new(),
            returns_series: BTreeMap::from([(UnixNanos::new(3), 0.25)]),
            event_store_receipt: Some(EventStoreSealReceipt::new(
                "event-store-run-1",
                42,
                EventStoreTerminalStatus::Ended,
                ContentDigest::digest_bytes(b"verified event store"),
            )),
            evidence_seal: None,
            run_inputs_verified: true,
        }
    }

    fn result(run_id: Option<UUID4>) -> BacktestResult {
        unsealed_result(run_id).issue_engine_evidence_seal()
    }

    fn suite(minimum_orders: f64) -> EvaluationSuite {
        EvaluationSuite::new(
            "research-promotion",
            "1",
            vec![Gate::Minimum {
                metric: "backtest.total_orders".to_string(),
                minimum: minimum_orders,
            }],
        )
        .unwrap()
    }

    fn store_entry(
        seq: u64,
        ts_init: u64,
        topic: &str,
        payload_type: &str,
        payload: Vec<u8>,
    ) -> EventStoreEntry {
        let topic: Topic = topic.into();
        let payload_type = Ustr::from(payload_type);
        let payload = payload.into();
        let headers = Headers::empty();
        let ts_init = UnixNanos::from(ts_init);
        let ts_publish = UnixNanos::from(ts_init.as_u64() + 1);
        let hash = compute_entry_hash(
            seq,
            ts_init,
            ts_publish,
            topic.as_ref(),
            payload_type.as_str(),
            &payload,
            &headers,
        );
        EventStoreEntry::new(
            hash,
            seq,
            headers,
            topic,
            payload_type,
            payload,
            ts_init,
            ts_publish,
        )
    }

    fn result_with_real_store(
        base_dir: &Path,
        result_run_id: UUID4,
        store_run_id: &str,
    ) -> BacktestResult {
        let mut result = unsealed_result(Some(result_run_id));
        let manifest = RunManifest {
            run_id: store_run_id.to_string(),
            parent_run_id: None,
            instance_id: result.instance_id.to_string(),
            binary_hash: "backtest-test-binary".to_string(),
            schema_version: nautilus_event_store::EVENT_STORE_ENTRY_SCHEMA_VERSION,
            crate_versions: "backtest-test-crates".to_string(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: "backtest-test-config".to_string(),
            registered_components: RegisteredComponents::default(),
            seed: Some(7),
            start_ts_init: UnixNanos::from(1),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
            provenance: result.provenance.clone(),
        };
        let run_started = store_entry(
            1,
            1,
            "run.lifecycle.RunStarted",
            "RunStarted",
            codec::encode_to_vec(&RunStartedPayload::from_manifest(&manifest))
                .expect("encode RunStarted"),
        );
        let run_ended = store_entry(2, 2, "run.lifecycle.RunEnded", "RunEnded", Vec::new());
        let mut backend = RedbBackend::new(base_dir);
        backend.open_run(manifest).expect("open event store");
        backend
            .append_batch(&[
                AppendEntry::without_indices(run_started),
                AppendEntry::without_indices(run_ended),
            ])
            .expect("append lifecycle entries");
        backend.seal(RunStatus::Ended).expect("seal event store");
        drop(backend);

        let report = Verifier::open_redb(base_dir, &result.instance_id.to_string(), store_run_id)
            .expect("open verifier")
            .verify()
            .expect("verify store");
        assert!(report.is_clean(), "findings: {:?}", report.findings);
        result.event_store_receipt = Some(EventStoreSealReceipt::new(
            report.run_id,
            report.high_watermark,
            EventStoreTerminalStatus::Ended,
            report.store_digest.expect("store digest"),
        ));
        result.issue_engine_evidence_seal()
    }

    #[rstest]
    fn evidence_digest_is_deterministic_and_binds_provenance() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let first = result(Some(run_id));
        let mut second = result(Some(run_id));
        assert_eq!(first.evidence_digest(), second.evidence_digest());

        second.provenance.data = vec![dataset("dataset/btc-usdt", b"different ticks")];
        assert_ne!(first.evidence_digest(), second.evidence_digest());
    }

    #[rstest]
    fn evidence_digest_binds_verified_event_store_receipt() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let first = result(Some(run_id));
        let mut second = unsealed_result(Some(run_id));
        second.event_store_receipt = Some(EventStoreSealReceipt::new(
            "event-store-run-2",
            43,
            EventStoreTerminalStatus::Ended,
            ContentDigest::digest_bytes(b"different verified event store"),
        ));
        let second = second.issue_engine_evidence_seal();

        assert_ne!(first.evidence_digest(), second.evidence_digest());
    }

    #[rstest]
    fn release_promotion_rejects_missing_or_crash_recovered_event_store_receipt() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut missing = unsealed_result(Some(run_id));
        missing.event_store_receipt = None;
        let missing = missing.issue_engine_evidence_seal();
        assert_eq!(
            missing.validate_release_provenance().unwrap_err(),
            BacktestEvidenceError::MissingEventStoreReceipt,
        );

        let mut crashed = unsealed_result(Some(run_id));
        crashed.event_store_receipt = Some(EventStoreSealReceipt::new(
            "event-store-crashed-run",
            42,
            EventStoreTerminalStatus::CrashedRecovered,
            ContentDigest::digest_bytes(b"crashed recovered"),
        ));
        let crashed = crashed.issue_engine_evidence_seal();
        assert_eq!(
            crashed.validate_release_provenance().unwrap_err(),
            BacktestEvidenceError::InvalidEventStoreReceipt,
        );
    }

    #[rstest]
    fn promotion_attestation_is_bound_to_exact_backtest_evidence() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let first = result(Some(run_id));
        let mut second = unsealed_result(Some(run_id));
        second.provenance.config = Some(artifact("config/research", b"changed"));
        let second = second.issue_engine_evidence_seal();

        let first_attestation = first
            .promotion_attestation(&suite(1.0), None, UnixNanos::new(10))
            .unwrap();
        let second_attestation = second
            .promotion_attestation(&suite(1.0), None, UnixNanos::new(10))
            .unwrap();

        assert_ne!(
            first_attestation.attestation_digest(),
            second_attestation.attestation_digest()
        );
        assert_ne!(
            first_attestation.candidate().digest(),
            second_attestation.candidate().digest()
        );
    }

    #[rstest]
    fn release_attestation_binds_strategy_evidence_and_deployment_scope() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let result = result(Some(run_id));

        let attestation = result
            .promotion_attestation_for_release_for_pipeline_test(
                &suite(1.0),
                None,
                "okx-production-a",
                UnixNanos::new(10),
            )
            .unwrap();

        let strategy = result.provenance.strategy.as_ref().unwrap();
        assert_eq!(
            attestation.candidate().artifact_id(),
            strategy.artifact().id().as_str()
        );
        assert_eq!(
            attestation.candidate().digest(),
            strategy.artifact().digest().to_string()
        );
        assert_eq!(
            attestation.evidence_artifact().unwrap(),
            &result.evidence_artifact_reference().unwrap()
        );
        assert_eq!(attestation.promotion_scope(), Some("okx-production-a"));
        let provenance_digest = result.provenance.canonical_digest().to_string();
        assert_eq!(
            attestation.run_provenance_digest(),
            Some(provenance_digest.as_str())
        );
        let expected_market_scope_commitment = result
            .provenance
            .environment
            .as_ref()
            .unwrap()
            .scope()
            .unwrap()
            .market_scope_commitment()
            .to_string();
        assert_eq!(
            attestation.market_scope_commitment(),
            Some(expected_market_scope_commitment.as_str())
        );
        let config = result.provenance.config.as_ref().unwrap();
        assert_eq!(
            attestation
                .strategy_config_artifact()
                .unwrap()
                .artifact_id(),
            config.id().as_str()
        );
        assert_eq!(
            attestation.strategy_config_artifact().unwrap().digest(),
            config.digest().to_string()
        );
        assert_eq!(
            attestation.strategy_entrypoint(),
            Some(strategy.entrypoint())
        );
        assert_eq!(
            attestation.strategy_runtime_id(),
            Some(strategy.runtime_id())
        );
        assert_eq!(
            attestation.suite_definition_digest(),
            suite(1.0).canonical_digest()
        );
        assert!(attestation.is_internally_consistent());
    }

    #[rstest]
    fn release_attestation_can_be_signed_by_an_external_evaluator() {
        const EVALUATOR_KEY: [u8; 32] = [29; 32];
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let result = result(Some(run_id));
        let suite = suite(1.0);

        let unsigned = result
            .promotion_attestation_for_release_for_pipeline_test(
                &suite,
                None,
                "okx-production-a",
                UnixNanos::new(10),
            )
            .unwrap();
        let evaluator_key_id = "evaluator/release-2026-01";
        let signing_payload = unsigned
            .evaluator_signing_payload(evaluator_key_id)
            .unwrap();
        let signature = ed25519_sign_bytes(&EVALUATOR_KEY, &signing_payload).unwrap();
        let signed = unsigned
            .clone()
            .into_signed(evaluator_key_id, signature)
            .unwrap();

        assert_eq!(signed.evaluator_key_id(), evaluator_key_id);
        assert_eq!(signed.attestation(), &unsigned);
        assert_eq!(signed.signature().as_bytes().len(), 64);
        assert!(signed.is_internally_consistent());
    }

    #[rstest]
    fn test_policy_signing_request_is_independently_rebuilt_from_redb_evidence() {
        const EVALUATOR_KEY: [u8; 32] = [31; 32];
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let baseline_dir = TempDir::new().expect("baseline tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let baseline = result_with_real_store(
            baseline_dir.path(),
            UUID4::from("33333333-3333-4333-8333-333333333333"),
            "baseline-store-run",
        );
        let request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                Some(&baseline),
                candidate_dir.path(),
                Some(baseline_dir.path()),
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let json = serde_json::to_string(&request).expect("serialize request");
        assert!(json.contains("\"run_inputs_verified\":true"));
        assert!(json.contains("\"event_store_locator\""));
        assert!(json.contains("\"gates\""));
        let decoded = ReleasePromotionSigningRequest::from_json(&json).expect("decode request");
        let verified = decoded
            .verify_for_external_evaluator_for_pipeline_test(
                candidate_dir.path(),
                Some(baseline_dir.path()),
            )
            .expect("independently verify request");

        let signature = ed25519_sign_bytes(&EVALUATOR_KEY, verified.signing_payload())
            .expect("sign verified payload");
        let signed = verified.into_signed(signature).expect("assemble signature");
        assert_eq!(signed.evaluator_key_id(), "evaluator/release-2026-01");
        assert!(signed.is_internally_consistent());
    }

    #[rstest]
    fn real_redb_evidence_verifies_and_research_promotes_but_strong_release_is_blocked() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );

        candidate
            .promotion_attestation(&suite(1.0), None, UnixNanos::new(10))
            .expect("research promotion remains available");
        let expected = BacktestEvidenceError::ReleaseIneligibleMarketProfile {
            materialized_venue_id: "OKX".to_string(),
            market_profile_id: "market-profile/generic-crypto-perpetual/v1".to_string(),
        };
        assert_eq!(
            candidate
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            expected,
        );
        assert_eq!(
            candidate
                .release_promotion_signing_request(
                    &suite(1.0),
                    None,
                    candidate_dir.path(),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                    "evaluator/release-2026-01",
                )
                .unwrap_err(),
            expected,
        );
    }

    #[rstest]
    fn strong_release_allowlist_rejects_every_current_profile_and_unknown_ids() {
        let profiles = [
            "market-profile/generic-crypto-perpetual/v1",
            "market-profile/china-a-shares-schema/v1",
            "market-profile/hong-kong-equities-schema/v1",
            "market-profile/united-states-equities-schema/v1",
            "market-profile/caller-claimed-release-grade/v999",
        ];

        for profile_id in profiles {
            let mut candidate =
                unsealed_result(Some(UUID4::from("22222222-2222-4222-8222-222222222222")));
            candidate.provenance.environment = Some(environment_binding_for_profile(profile_id));
            let candidate = candidate.issue_engine_evidence_seal();

            assert_eq!(
                candidate
                    .promotion_attestation_for_release(
                        &suite(1.0),
                        None,
                        "production",
                        UnixNanos::new(10),
                    )
                    .unwrap_err(),
                BacktestEvidenceError::ReleaseIneligibleMarketProfile {
                    materialized_venue_id: "TEST".to_string(),
                    market_profile_id: profile_id.to_string(),
                },
                "profile {profile_id} must not self-authorize strong release",
            );
        }
    }

    #[rstest]
    fn external_evaluator_enforces_file_byte_budget_before_store_scan() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("construct pipeline request");
        let store_path = candidate_dir
            .path()
            .join(candidate.instance_id.to_string())
            .join("candidate-store-run.redb");
        let observed = fs::metadata(store_path).expect("store metadata").len();
        let limit = observed.checked_sub(1).expect("redb file is non-empty");
        let limits = VerifyLimits {
            max_file_bytes: limit,
            ..VerifyLimits::default()
        };

        assert_eq!(
            request
                .verify_for_external_evaluator_with_limits_for_pipeline_test(
                    candidate_dir.path(),
                    None,
                    limits,
                )
                .unwrap_err(),
            ExternalEvaluatorError::VerificationLimitExceeded {
                role: "candidate",
                resource: VerifyLimitResource::FileBytes,
                observed,
                limit,
            },
        );
    }

    #[rstest]
    fn production_external_rebuild_requires_authenticated_runner_before_policy_checks() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        // Build a structurally complete request through the private pipeline-test policy to model
        // a stale or hostile request arriving from outside this library. Production must reject
        // it before profile or semantic checks can yield signable bytes.
        let request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("construct hostile request fixture");

        assert_eq!(
            request
                .verify_for_external_evaluator(candidate_dir.path(), None)
                .unwrap_err(),
            ExternalEvaluatorError::AuthenticatedRunnerRequired,
        );
    }

    #[rstest]
    fn production_external_rebuild_rejects_self_consistent_forged_result_semantics() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("construct pipeline request");

        // Model the exact semantic forgery the wire format cannot authenticate: a caller claims
        // verified inputs, substitutes favorable statistics, and recomputes every dependent
        // digest, attestation, and payload while retaining the real store receipt/provenance.
        request.candidate.result.run_inputs_verified = true;
        request.candidate.result.total_orders = 1_000_000;
        request
            .candidate
            .result
            .stats_returns
            .insert("Sharpe Ratio".to_string(), 99.0);
        let forged_result = request.candidate.result.to_result();
        request.candidate.evidence_digest = forged_result.evidence_digest();
        let forged_result = forged_result.issue_engine_evidence_seal();
        let forged_attestation = forged_result
            .promotion_attestation_for_release_with_policy(
                &request.suite.to_suite().expect("valid suite"),
                None,
                request.promotion_scope.clone(),
                request.evaluated_at,
                ReleaseEligibilityPolicy::CurrentProfilesForPipelineTests,
            )
            .expect("self-consistent forged attestation");
        let forged_payload = forged_attestation
            .evaluator_signing_payload(&request.evaluator_key_id)
            .expect("self-consistent forged payload");
        request.attestation = forged_attestation;
        request.signing_payload_hex = encode_signing_payload_hex(&forged_payload);

        // The private mechanism deliberately proves only consistency. This assertion pins the
        // attack fixture so the production rejection cannot accidentally become a shallow digest
        // mismatch test.
        request
            .verify_for_external_evaluator_for_pipeline_test(candidate_dir.path(), None)
            .expect("private consistency pipeline accepts the fully recomputed fixture");
        assert_eq!(
            request
                .verify_for_external_evaluator(candidate_dir.path(), None)
                .unwrap_err(),
            ExternalEvaluatorError::AuthenticatedRunnerRequired,
        );
        assert_eq!(
            request
                .verify_for_external_evaluator_with_limits(
                    candidate_dir.path(),
                    None,
                    VerifyLimits::default(),
                )
                .unwrap_err(),
            ExternalEvaluatorError::AuthenticatedRunnerRequired,
        );
    }

    #[rstest]
    fn signing_request_rejects_tampered_result_material() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        request.candidate.result.total_orders += 1;

        assert_eq!(
            request
                .verify_for_external_evaluator_for_pipeline_test(candidate_dir.path(), None)
                .unwrap_err(),
            ExternalEvaluatorError::EvidenceDigestMismatch { role: "candidate" },
        );
    }

    #[rstest]
    fn signing_request_rejects_tampered_suite_and_payload() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let mut suite_tampered = request.clone();
        suite_tampered.suite.gates = suite(0.0).gates().to_vec();
        assert_eq!(
            suite_tampered
                .verify_for_external_evaluator_for_pipeline_test(candidate_dir.path(), None)
                .unwrap_err(),
            ExternalEvaluatorError::AttestationMismatch,
        );

        let mut payload_tampered = request;
        payload_tampered.signing_payload_hex = "00".repeat(32);
        assert_eq!(
            payload_tampered
                .verify_for_external_evaluator_for_pipeline_test(candidate_dir.path(), None)
                .unwrap_err(),
            ExternalEvaluatorError::SigningPayloadMismatch,
        );
    }

    #[rstest]
    fn signing_request_rejects_result_manifest_provenance_substitution() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                candidate_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        request.candidate.result.provenance.config =
            Some(artifact("config/substituted", b"substituted configuration"));
        let substituted = request.candidate.result.to_result();
        request.candidate.evidence_digest = substituted.evidence_digest();

        assert!(matches!(
            request.verify_for_external_evaluator_for_pipeline_test(candidate_dir.path(), None),
            Err(ExternalEvaluatorError::EventStoreIdentityMismatch {
                role: "candidate",
                reason,
            }) if reason.contains("manifest provenance")
        ));
    }

    #[rstest]
    fn signing_request_rejects_candidate_baseline_bundle_substitution() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let baseline_dir = TempDir::new().expect("baseline tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let baseline = result_with_real_store(
            baseline_dir.path(),
            UUID4::from("33333333-3333-4333-8333-333333333333"),
            "baseline-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                Some(&baseline),
                candidate_dir.path(),
                Some(baseline_dir.path()),
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let baseline_bundle = request.baseline.as_mut().expect("baseline bundle");
        std::mem::swap(&mut request.candidate, baseline_bundle);

        assert_eq!(
            request
                .verify_for_external_evaluator_for_pipeline_test(
                    baseline_dir.path(),
                    Some(candidate_dir.path()),
                )
                .unwrap_err(),
            ExternalEvaluatorError::AttestationMismatch,
        );
    }

    #[rstest]
    fn signing_request_rejects_missing_candidate_or_baseline_locator() {
        let candidate_dir = TempDir::new().expect("candidate tempdir");
        let baseline_dir = TempDir::new().expect("baseline tempdir");
        let candidate = result_with_real_store(
            candidate_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let baseline = result_with_real_store(
            baseline_dir.path(),
            UUID4::from("33333333-3333-4333-8333-333333333333"),
            "baseline-store-run",
        );
        assert_eq!(
            candidate
                .release_promotion_signing_request_for_pipeline_test(
                    &suite(1.0),
                    Some(&baseline),
                    candidate_dir.path(),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                    "evaluator/release-2026-01",
                )
                .unwrap_err(),
            BacktestEvidenceError::MissingBaselineEventStoreBaseDir,
        );

        let request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                Some(&baseline),
                candidate_dir.path(),
                Some(baseline_dir.path()),
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let mut missing_candidate = serde_json::to_value(&request).expect("request JSON");
        missing_candidate["candidate"]
            .as_object_mut()
            .expect("candidate object")
            .remove("event_store_locator");
        assert!(matches!(
            ReleasePromotionSigningRequest::from_json(
                &serde_json::to_string(&missing_candidate).expect("JSON")
            ),
            Err(ExternalEvaluatorError::Deserialize(_))
        ));

        let mut missing_baseline = serde_json::to_value(&request).expect("request JSON");
        missing_baseline["baseline"]
            .as_object_mut()
            .expect("baseline object")
            .remove("event_store_locator");
        assert!(matches!(
            ReleasePromotionSigningRequest::from_json(
                &serde_json::to_string(&missing_baseline).expect("JSON")
            ),
            Err(ExternalEvaluatorError::Deserialize(_))
        ));
    }

    #[cfg(unix)]
    #[rstest]
    fn signing_request_rejects_symlinked_instance_directory() {
        use std::os::unix::fs::symlink;

        let real_dir = TempDir::new().expect("real tempdir");
        let redirected_dir = TempDir::new().expect("redirected tempdir");
        let candidate = result_with_real_store(
            real_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                real_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let instance_id = candidate.instance_id.to_string();
        symlink(
            real_dir.path().join(&instance_id),
            redirected_dir.path().join(&instance_id),
        )
        .expect("symlink instance directory");
        request.candidate.event_store_locator.base_dir =
            fs::canonicalize(redirected_dir.path()).expect("canonical redirected root");

        assert!(matches!(
            request.verify_for_external_evaluator_for_pipeline_test(redirected_dir.path(), None),
            Err(ExternalEvaluatorError::InvalidEventStoreLocator {
                role: "candidate",
                reason,
            }) if reason.contains("symbolic link")
        ));
    }

    #[cfg(unix)]
    #[rstest]
    fn signing_request_rejects_symlinked_run_file() {
        use std::os::unix::fs::symlink;

        let real_dir = TempDir::new().expect("real tempdir");
        let redirected_dir = TempDir::new().expect("redirected tempdir");
        let candidate = result_with_real_store(
            real_dir.path(),
            UUID4::from("22222222-2222-4222-8222-222222222222"),
            "candidate-store-run",
        );
        let mut request = candidate
            .release_promotion_signing_request_for_pipeline_test(
                &suite(1.0),
                None,
                real_dir.path(),
                None,
                "okx-production-a",
                UnixNanos::new(10),
                "evaluator/release-2026-01",
            )
            .expect("build request");
        let instance_id = candidate.instance_id.to_string();
        let redirected_instance_dir = redirected_dir.path().join(&instance_id);
        fs::create_dir(&redirected_instance_dir).expect("create redirected instance directory");
        symlink(
            real_dir
                .path()
                .join(&instance_id)
                .join("candidate-store-run.redb"),
            redirected_instance_dir.join("candidate-store-run.redb"),
        )
        .expect("symlink run file");
        request.candidate.event_store_locator.base_dir =
            fs::canonicalize(redirected_dir.path()).expect("canonical redirected root");

        assert!(matches!(
            request.verify_for_external_evaluator_for_pipeline_test(redirected_dir.path(), None),
            Err(ExternalEvaluatorError::InvalidEventStoreLocator {
                role: "candidate",
                reason,
            }) if reason.contains("symbolic link")
        ));
    }

    #[rstest]
    fn external_signing_request_requires_passing_evidence() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let error = result(Some(run_id))
            .promotion_attestation_for_release_for_pipeline_test(
                &suite(3.0),
                None,
                "okx-production-a",
                UnixNanos::new(10),
            )
            .unwrap_err();

        assert_eq!(
            error,
            BacktestEvidenceError::Attestation(AttestationError::OutcomeFailed)
        );
    }

    #[rstest]
    fn release_attestation_requires_complete_run_provenance() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut missing_data = unsealed_result(Some(run_id));
        missing_data.provenance.data.clear();
        let missing_data = missing_data.issue_engine_evidence_seal();
        let mut missing_config = unsealed_result(Some(run_id));
        missing_config.provenance.config = None;
        let missing_config = missing_config.issue_engine_evidence_seal();
        let mut missing_environment = unsealed_result(Some(run_id));
        missing_environment.provenance.environment = None;
        let missing_environment = missing_environment.issue_engine_evidence_seal();
        let mut missing_scope = unsealed_result(Some(run_id));
        missing_scope.provenance.environment = Some(unscoped_environment());
        let missing_scope = missing_scope.issue_engine_evidence_seal();

        assert_eq!(
            missing_data
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::MissingDataArtifact
        );
        assert_eq!(
            missing_config
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::MissingConfigArtifact
        );
        assert_eq!(
            missing_environment
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::MissingEnvironmentArtifact
        );
        assert_eq!(
            missing_scope
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::MissingEnvironmentScope
        );
    }

    #[rstest]
    fn release_attestation_rejects_declared_but_unmeasured_inputs() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut declared_only = unsealed_result(Some(run_id));
        declared_only.run_inputs_verified = false;
        let declared_only = declared_only.issue_engine_evidence_seal();

        assert_eq!(
            declared_only
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::UnverifiedRunInputs
        );
    }

    #[rstest]
    fn release_attestation_rejects_baseline_from_another_market_scope() {
        let candidate = result(Some(UUID4::from("22222222-2222-4222-8222-222222222222")));
        let mut baseline =
            unsealed_result(Some(UUID4::from("33333333-3333-4333-8333-333333333333")));
        baseline.provenance.environment = Some(environment_binding_with_us_route(
            true,
            "execution-route/direct-venue/XNAS",
        ));
        let baseline = baseline.issue_engine_evidence_seal();

        assert!(matches!(
            candidate.promotion_attestation_for_release_for_pipeline_test(
                &suite(1.0),
                Some(&baseline),
                "okx-production-a",
                UnixNanos::new(10),
            ),
            Err(BacktestEvidenceError::BaselineMarketScopeMismatch { .. })
        ));
    }

    #[rstest]
    fn release_attestation_rejects_preclaimed_post_run_artifacts() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut preclaimed = unsealed_result(Some(run_id));
        preclaimed.provenance.promotion = Some(artifact("promotion/forged", b"forged"));
        let preclaimed = preclaimed.issue_engine_evidence_seal();

        assert_eq!(
            preclaimed
                .promotion_attestation_for_release(
                    &suite(1.0),
                    None,
                    "okx-production-a",
                    UnixNanos::new(10),
                )
                .unwrap_err(),
            BacktestEvidenceError::PreclaimedPromotionArtifact
        );
    }

    #[rstest]
    fn promotion_fails_closed_for_failed_suite() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let error = result(Some(run_id))
            .promotion_attestation(&suite(3.0), None, UnixNanos::new(10))
            .unwrap_err();

        assert_eq!(
            error,
            BacktestEvidenceError::Attestation(AttestationError::OutcomeFailed)
        );
    }

    #[rstest]
    fn evidence_requires_completed_run_id() {
        assert_eq!(
            result(None).evidence_artifact_reference().unwrap_err(),
            BacktestEvidenceError::MissingRunId
        );
    }

    #[rstest]
    fn unsealed_result_cannot_be_used_as_promotion_evidence() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let result = unsealed_result(Some(run_id));

        assert_eq!(
            result.evidence_artifact_reference().unwrap_err(),
            BacktestEvidenceError::Unsealed
        );
        assert_eq!(
            result
                .promotion_attestation(&suite(1.0), None, UnixNanos::new(10))
                .unwrap_err(),
            BacktestEvidenceError::Unsealed
        );
    }

    #[rstest]
    fn modifying_public_result_fields_invalidates_engine_seal() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut result = result(Some(run_id));
        result.total_orders += 1;
        result
            .stats_returns
            .insert("Sharpe Ratio".to_string(), 99.0);

        assert_eq!(
            result.evidence_artifact_reference().unwrap_err(),
            BacktestEvidenceError::Tampered
        );
        assert_eq!(
            result
                .promotion_attestation(&suite(1.0), None, UnixNanos::new(10))
                .unwrap_err(),
            BacktestEvidenceError::Tampered
        );
    }

    #[rstest]
    fn engine_seal_cannot_be_reissued_over_modified_evidence() {
        let run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let mut result = result(Some(run_id));
        result.total_orders += 1;
        let result = result.issue_engine_evidence_seal();

        assert_eq!(
            result.evidence_artifact_reference().unwrap_err(),
            BacktestEvidenceError::Tampered
        );
    }

    #[rstest]
    fn modifying_baseline_invalidates_promotion_evidence() {
        let candidate_run_id = UUID4::from("22222222-2222-4222-8222-222222222222");
        let baseline_run_id = UUID4::from("33333333-3333-4333-8333-333333333333");
        let candidate = result(Some(candidate_run_id));
        let mut baseline = result(Some(baseline_run_id));
        baseline.returns_series.insert(UnixNanos::new(4), 0.5);

        assert_eq!(
            candidate
                .promotion_attestation(&suite(1.0), Some(&baseline), UnixNanos::new(10))
                .unwrap_err(),
            BacktestEvidenceError::Tampered
        );
    }
}
