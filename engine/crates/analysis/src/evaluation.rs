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

//! Deterministic quantitative evaluation and promotion attestations.
//!
//! Evaluation is deliberately fail-closed: absent and non-finite observations produce failed
//! evidence instead of being ignored. Promotion attestations can only be created from a passing
//! outcome and bind that outcome to explicit artifact identities and an evaluation timestamp.

use std::{collections::BTreeMap, fmt::Display};

use nautilus_core::UnixNanos;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

const OBSERVATIONS_DOMAIN: &[u8] = b"nautilus-analysis/evaluation-observations/v1";
const SUITE_DOMAIN: &[u8] = b"nautilus-analysis/evaluation-suite/v1";
const EVIDENCE_DOMAIN: &[u8] = b"nautilus-analysis/evaluation-evidence/v1";
const OUTCOME_SEAL_DOMAIN: &[u8] = b"nautilus-analysis/evaluation-outcome-seal/v2";
const ATTESTATION_DOMAIN: &[u8] = b"nautilus-analysis/promotion-attestation/v5";
const EVALUATOR_SIGNATURE_DOMAIN: &[u8] =
    b"nautilus-analysis/promotion-attestation-evaluator-signature/v2";
const SIGNED_ATTESTATION_DOMAIN: &[u8] = b"nautilus-analysis/signed-promotion-attestation/v2";

/// Current serialization and signing-payload version for signed promotion attestations.
pub const SIGNED_PROMOTION_ATTESTATION_VERSION: u32 = 2;

const ED25519_SIGNATURE_LENGTH: usize = 64;
const MAX_EVALUATOR_KEY_ID_LENGTH: usize = 255;

/// Metric observations keyed by stable metric name.
pub type Observations = BTreeMap<String, f64>;

/// A deterministic 32-byte BLAKE3 digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EvaluationDigest([u8; 32]);

impl EvaluationDigest {
    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns a lowercase hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            output.push(nibble_to_hex(byte >> 4));
            output.push(nibble_to_hex(byte & 0x0f));
        }
        output
    }
}

impl Display for EvaluationDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// A quantitative promotion gate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvaluationGate {
    /// Requires the candidate value to be greater than or equal to `minimum`.
    Minimum { metric: String, minimum: f64 },
    /// Requires the candidate value to be less than or equal to `maximum`.
    Maximum { metric: String, maximum: f64 },
    /// Requires the candidate value to lie inside the closed interval.
    InclusiveRange {
        metric: String,
        minimum: f64,
        maximum: f64,
    },
    /// Requires `candidate >= baseline - maximum_regression`.
    MaxRegression {
        metric: String,
        maximum_regression: f64,
    },
}

impl EvaluationGate {
    fn metric(&self) -> &str {
        match self {
            Self::Minimum { metric, .. }
            | Self::Maximum { metric, .. }
            | Self::InclusiveRange { metric, .. }
            | Self::MaxRegression { metric, .. } => metric,
        }
    }

    const fn kind_tag(&self) -> u8 {
        match self {
            Self::Minimum { .. } => 0,
            Self::Maximum { .. } => 1,
            Self::InclusiveRange { .. } => 2,
            Self::MaxRegression { .. } => 3,
        }
    }

    const fn needs_baseline(&self) -> bool {
        matches!(self, Self::MaxRegression { .. })
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        if self.metric().is_empty() {
            return Err(EvaluationError::EmptyMetric);
        }

        match self {
            Self::Minimum { minimum, .. } => validate_threshold(self.metric(), *minimum),
            Self::Maximum { maximum, .. } => validate_threshold(self.metric(), *maximum),
            Self::InclusiveRange {
                minimum, maximum, ..
            } => {
                validate_threshold(self.metric(), *minimum)?;
                validate_threshold(self.metric(), *maximum)?;
                if minimum > maximum {
                    return Err(EvaluationError::InvalidRange {
                        metric: self.metric().to_owned(),
                        minimum: *minimum,
                        maximum: *maximum,
                    });
                }
                Ok(())
            }
            Self::MaxRegression {
                maximum_regression, ..
            } => {
                validate_threshold(self.metric(), *maximum_regression)?;
                if *maximum_regression < 0.0 {
                    return Err(EvaluationError::NegativeMaximumRegression {
                        metric: self.metric().to_owned(),
                        maximum_regression: *maximum_regression,
                    });
                }
                Ok(())
            }
        }
    }
}

/// Alias for [`EvaluationGate`].
pub type Gate = EvaluationGate;

/// Why a gate passed or failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateReason {
    Passed,
    MissingObservation,
    MissingBaseline,
    NonFiniteObservation,
    NonFiniteBaseline,
    BelowMinimum,
    AboveMaximum,
    OutsideInclusiveRange,
    RegressionExceeded,
}

impl Display for GateReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::Passed => "passed",
            Self::MissingObservation => "missing candidate observation",
            Self::MissingBaseline => "missing baseline observation",
            Self::NonFiniteObservation => "candidate observation is not finite",
            Self::NonFiniteBaseline => "baseline observation is not finite",
            Self::BelowMinimum => "candidate observation is below the minimum",
            Self::AboveMaximum => "candidate observation is above the maximum",
            Self::OutsideInclusiveRange => "candidate observation is outside the inclusive range",
            Self::RegressionExceeded => "candidate regression exceeds the maximum",
        };
        f.write_str(reason)
    }
}

/// Evidence recorded for one evaluated gate.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GateEvidence {
    pub gate: EvaluationGate,
    pub observed: Option<f64>,
    pub baseline: Option<f64>,
    pub passed: bool,
    pub reason: GateReason,
}

impl GateEvidence {
    /// Returns the metric evaluated by this evidence.
    #[must_use]
    pub fn metric(&self) -> &str {
        self.gate.metric()
    }
}

/// A validated, versioned set of deterministic evaluation gates.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EvaluationSuite {
    suite_id: String,
    version: String,
    gates: Vec<EvaluationGate>,
}

impl EvaluationSuite {
    /// Creates a validated evaluation suite.
    ///
    /// # Errors
    ///
    /// Returns an error when an identifier is empty, no gates are provided, or a gate definition
    /// is invalid or non-finite.
    pub fn new(
        suite_id: impl Into<String>,
        version: impl Into<String>,
        gates: Vec<EvaluationGate>,
    ) -> Result<Self, EvaluationError> {
        let suite_id = suite_id.into();
        let version = version.into();
        if suite_id.is_empty() {
            return Err(EvaluationError::EmptySuiteId);
        }
        if version.is_empty() {
            return Err(EvaluationError::EmptySuiteVersion);
        }
        if gates.is_empty() {
            return Err(EvaluationError::EmptyGateSet);
        }
        for gate in &gates {
            gate.validate()?;
        }
        Ok(Self {
            suite_id,
            version,
            gates,
        })
    }

    /// Returns the stable suite identifier.
    #[must_use]
    pub fn suite_id(&self) -> &str {
        &self.suite_id
    }

    /// Returns the stable suite version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the suite gates in evaluation order.
    #[must_use]
    pub fn gates(&self) -> &[EvaluationGate] {
        &self.gates
    }

    /// Returns the canonical digest of the complete ordered gate definition.
    #[must_use]
    pub fn canonical_digest(&self) -> EvaluationDigest {
        digest_suite(&self.suite_id, &self.version, &self.gates)
    }

    /// Evaluates candidate observations and an optional explicit baseline.
    ///
    /// Missing or non-finite input fails its gate closed and remains visible in the returned
    /// evidence. Extra observations are retained in the canonical observation digest.
    #[must_use]
    pub fn evaluate(
        &self,
        candidate: &Observations,
        baseline: Option<&Observations>,
    ) -> EvaluationOutcome {
        let evidence = self
            .gates
            .iter()
            .cloned()
            .map(|gate| evaluate_gate(gate, candidate, baseline))
            .collect::<Vec<_>>();
        let observations_digest = digest_observations(candidate, baseline);
        EvaluationOutcome::new(
            self.suite_id.clone(),
            self.version.clone(),
            self.canonical_digest(),
            evidence,
            observations_digest,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OutcomeSeal(EvaluationDigest);

/// Complete result of evaluating a suite.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EvaluationOutcome {
    suite_id: String,
    suite_version: String,
    suite_definition_digest: EvaluationDigest,
    passed: bool,
    baseline_required: bool,
    evidence: Vec<GateEvidence>,
    observations_digest: EvaluationDigest,
    evidence_digest: EvaluationDigest,
    #[serde(skip)]
    seal: OutcomeSeal,
}

impl EvaluationOutcome {
    fn new(
        suite_id: String,
        suite_version: String,
        suite_definition_digest: EvaluationDigest,
        evidence: Vec<GateEvidence>,
        observations_digest: EvaluationDigest,
    ) -> Self {
        let passed = evidence.iter().all(|item| item.passed);
        let baseline_required = evidence.iter().any(|item| item.gate.needs_baseline());
        let evidence_digest = digest_evidence(&suite_id, &suite_version, &evidence);
        let seal = seal_outcome(
            &suite_id,
            &suite_version,
            suite_definition_digest,
            passed,
            baseline_required,
            observations_digest,
            evidence_digest,
        );
        Self {
            suite_id,
            suite_version,
            suite_definition_digest,
            passed,
            baseline_required,
            evidence,
            observations_digest,
            evidence_digest,
            seal,
        }
    }

    /// Returns the stable suite identifier.
    #[must_use]
    pub fn suite_id(&self) -> &str {
        &self.suite_id
    }

    /// Returns the stable suite version.
    #[must_use]
    pub fn suite_version(&self) -> &str {
        &self.suite_version
    }

    /// Returns the canonical digest of the exact ordered suite definition.
    #[must_use]
    pub const fn suite_definition_digest(&self) -> EvaluationDigest {
        self.suite_definition_digest
    }

    /// Returns whether every gate passed.
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    /// Returns whether this evaluation included a gate requiring baseline provenance.
    #[must_use]
    pub const fn baseline_required(&self) -> bool {
        self.baseline_required
    }

    /// Returns the immutable per-gate evaluation evidence.
    #[must_use]
    pub fn evidence(&self) -> &[GateEvidence] {
        &self.evidence
    }

    /// Returns the digest of candidate and baseline observations.
    #[must_use]
    pub const fn observations_digest(&self) -> EvaluationDigest {
        self.observations_digest
    }

    /// Returns the digest of the suite's per-gate evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvaluationDigest {
        self.evidence_digest
    }

    fn is_internally_consistent(&self) -> bool {
        if self.suite_id.is_empty()
            || self.suite_version.is_empty()
            || self.evidence.is_empty()
            || self
                .evidence
                .iter()
                .any(|item| item.gate.validate().is_err())
        {
            return false;
        }

        let evidence_consistent = self.evidence.iter().all(|item| {
            let reason = evaluate_gate_reason(&item.gate, item.observed, item.baseline);
            item.reason == reason && item.passed == (reason == GateReason::Passed)
        });
        let passed = self.evidence.iter().all(|item| item.passed);
        let baseline_required = self.evidence.iter().any(|item| item.gate.needs_baseline());
        let evidence_digest = digest_evidence(&self.suite_id, &self.suite_version, &self.evidence);
        let gates: Vec<_> = self.evidence.iter().map(|item| item.gate.clone()).collect();
        let suite_definition_digest = digest_suite(&self.suite_id, &self.suite_version, &gates);
        let seal = seal_outcome(
            &self.suite_id,
            &self.suite_version,
            self.suite_definition_digest,
            self.passed,
            self.baseline_required,
            self.observations_digest,
            self.evidence_digest,
        );

        evidence_consistent
            && self.passed == passed
            && self.baseline_required == baseline_required
            && self.suite_definition_digest == suite_definition_digest
            && self.evidence_digest == evidence_digest
            && self.seal == seal
    }
}

/// Immutable artifact identity used by a promotion attestation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ArtifactReference {
    artifact_id: String,
    digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactReferenceWire {
    artifact_id: String,
    digest: String,
}

impl<'de> Deserialize<'de> for ArtifactReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ArtifactReferenceWire::deserialize(deserializer)?;
        Self::new(wire.artifact_id, wire.digest).map_err(de::Error::custom)
    }
}

impl ArtifactReference {
    /// Creates a non-empty artifact reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the artifact ID or digest is empty.
    pub fn new(
        artifact_id: impl Into<String>,
        digest: impl Into<String>,
    ) -> Result<Self, AttestationError> {
        let artifact_id = artifact_id.into();
        let digest = digest.into();
        if artifact_id.is_empty() {
            return Err(AttestationError::EmptyArtifactId);
        }
        if digest.is_empty() {
            return Err(AttestationError::EmptyArtifactDigest);
        }
        Ok(Self {
            artifact_id,
            digest,
        })
    }

    /// Returns the artifact identifier.
    #[must_use]
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Returns the artifact digest supplied by the caller.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Cryptographic proof that a particular artifact passed a particular evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionAttestation {
    suite_id: String,
    suite_version: String,
    suite_definition_digest: EvaluationDigest,
    candidate: ArtifactReference,
    evidence_artifact: Option<ArtifactReference>,
    run_provenance_digest: Option<String>,
    market_scope_commitment: Option<String>,
    strategy_config_artifact: Option<ArtifactReference>,
    strategy_entrypoint: Option<String>,
    strategy_runtime_id: Option<String>,
    observations_digest: EvaluationDigest,
    evidence_digest: EvaluationDigest,
    baseline: Option<ArtifactReference>,
    promotion_scope: Option<String>,
    evaluated_at: UnixNanos,
    attestation_digest: EvaluationDigest,
}

impl PromotionAttestation {
    /// Creates an attestation from a passing outcome.
    ///
    /// # Errors
    ///
    /// Returns an error if the outcome failed or a baseline-dependent suite has no baseline
    /// artifact identity.
    pub fn from_passing_outcome(
        outcome: &EvaluationOutcome,
        candidate: ArtifactReference,
        baseline: Option<ArtifactReference>,
        evaluated_at: UnixNanos,
    ) -> Result<Self, AttestationError> {
        Self::build(
            outcome,
            candidate,
            None,
            None,
            None,
            None,
            None,
            None,
            baseline,
            None,
            evaluated_at,
        )
    }

    /// Creates a deployment-scoped attestation for a release artifact from exact run evidence.
    ///
    /// This is the strong form consumed by strategy artifact verification: `candidate` identifies
    /// the exact release bytes, while `evidence_artifact` identifies the sealed backtest result
    /// whose observations passed the suite.
    ///
    /// # Errors
    ///
    /// Returns an error if the outcome failed, is inconsistent, requires a missing baseline, or
    /// the promotion scope or paired market-scope commitment is absent or malformed.
    #[expect(
        clippy::too_many_arguments,
        reason = "the strong constructor explicitly binds every independently audited artifact dimension"
    )]
    pub fn from_passing_outcome_with_evidence(
        outcome: &EvaluationOutcome,
        candidate: ArtifactReference,
        evidence_artifact: ArtifactReference,
        run_provenance_digest: impl Into<String>,
        market_scope_commitment: impl Into<String>,
        strategy_config_artifact: ArtifactReference,
        strategy_entrypoint: impl Into<String>,
        strategy_runtime_id: impl Into<String>,
        baseline: Option<ArtifactReference>,
        promotion_scope: impl Into<String>,
        evaluated_at: UnixNanos,
    ) -> Result<Self, AttestationError> {
        validate_release_artifact_reference("candidate", &candidate)?;
        validate_release_artifact_reference("evidence_artifact", &evidence_artifact)?;
        validate_release_artifact_reference("strategy_config_artifact", &strategy_config_artifact)?;
        if let Some(baseline) = baseline.as_ref() {
            validate_release_artifact_reference("baseline", baseline)?;
        }
        let promotion_scope = promotion_scope.into();
        if promotion_scope.is_empty() {
            return Err(AttestationError::EmptyPromotionScope);
        }
        let run_provenance_digest = run_provenance_digest.into();
        if run_provenance_digest.is_empty() {
            return Err(AttestationError::EmptyRunProvenanceDigest);
        }
        validate_release_digest("run_provenance_digest", &run_provenance_digest)?;
        let market_scope_commitment = market_scope_commitment.into();
        if market_scope_commitment.is_empty() {
            return Err(AttestationError::EmptyMarketScopeCommitment);
        }
        validate_release_digest("market_scope_commitment", &market_scope_commitment)?;
        let strategy_entrypoint = strategy_entrypoint.into();
        if strategy_entrypoint.is_empty() {
            return Err(AttestationError::EmptyStrategyEntrypoint);
        }
        let strategy_runtime_id = strategy_runtime_id.into();
        if strategy_runtime_id.is_empty() {
            return Err(AttestationError::EmptyStrategyRuntimeId);
        }
        Self::build(
            outcome,
            candidate,
            Some(evidence_artifact),
            Some(run_provenance_digest),
            Some(market_scope_commitment),
            Some(strategy_config_artifact),
            Some(strategy_entrypoint),
            Some(strategy_runtime_id),
            baseline,
            Some(promotion_scope),
            evaluated_at,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the private builder mirrors the two attestation constructor variants"
    )]
    fn build(
        outcome: &EvaluationOutcome,
        candidate: ArtifactReference,
        evidence_artifact: Option<ArtifactReference>,
        run_provenance_digest: Option<String>,
        market_scope_commitment: Option<String>,
        strategy_config_artifact: Option<ArtifactReference>,
        strategy_entrypoint: Option<String>,
        strategy_runtime_id: Option<String>,
        baseline: Option<ArtifactReference>,
        promotion_scope: Option<String>,
        evaluated_at: UnixNanos,
    ) -> Result<Self, AttestationError> {
        if !outcome.is_internally_consistent() {
            return Err(AttestationError::InconsistentOutcome);
        }
        if !outcome.passed() {
            return Err(AttestationError::OutcomeFailed);
        }
        if outcome.baseline_required() && baseline.is_none() {
            return Err(AttestationError::BaselineArtifactRequired);
        }

        let attestation_digest = digest_attestation(AttestationDigestInput {
            suite_id: outcome.suite_id(),
            suite_version: outcome.suite_version(),
            suite_definition_digest: outcome.suite_definition_digest(),
            candidate: &candidate,
            evidence_artifact: evidence_artifact.as_ref(),
            run_provenance_digest: run_provenance_digest.as_deref(),
            market_scope_commitment: market_scope_commitment.as_deref(),
            strategy_config_artifact: strategy_config_artifact.as_ref(),
            strategy_entrypoint: strategy_entrypoint.as_deref(),
            strategy_runtime_id: strategy_runtime_id.as_deref(),
            observations_digest: outcome.observations_digest(),
            evidence_digest: outcome.evidence_digest(),
            baseline: baseline.as_ref(),
            promotion_scope: promotion_scope.as_deref(),
            evaluated_at,
        });
        Ok(Self {
            suite_id: outcome.suite_id.clone(),
            suite_version: outcome.suite_version.clone(),
            suite_definition_digest: outcome.suite_definition_digest,
            candidate,
            evidence_artifact,
            run_provenance_digest,
            market_scope_commitment,
            strategy_config_artifact,
            strategy_entrypoint,
            strategy_runtime_id,
            observations_digest: outcome.observations_digest,
            evidence_digest: outcome.evidence_digest,
            baseline,
            promotion_scope,
            evaluated_at,
            attestation_digest,
        })
    }

    /// Returns the suite identifier.
    #[must_use]
    pub fn suite_id(&self) -> &str {
        &self.suite_id
    }

    /// Returns the suite version.
    #[must_use]
    pub fn suite_version(&self) -> &str {
        &self.suite_version
    }

    /// Returns the canonical digest of the exact ordered suite definition.
    #[must_use]
    pub const fn suite_definition_digest(&self) -> EvaluationDigest {
        self.suite_definition_digest
    }

    /// Returns the candidate artifact identity.
    #[must_use]
    pub const fn candidate(&self) -> &ArtifactReference {
        &self.candidate
    }

    /// Returns the exact evaluation-evidence artifact for strong release attestations.
    #[must_use]
    pub const fn evidence_artifact(&self) -> Option<&ArtifactReference> {
        self.evidence_artifact.as_ref()
    }

    /// Returns the canonical provenance digest of the evaluated run.
    #[must_use]
    pub fn run_provenance_digest(&self) -> Option<&str> {
        self.run_provenance_digest.as_deref()
    }

    /// Returns the canonical commitment to the exact paired profile/listing/route market scope.
    #[must_use]
    pub fn market_scope_commitment(&self) -> Option<&str> {
        self.market_scope_commitment.as_deref()
    }

    /// Returns the exact strategy configuration artifact evaluated by the run.
    #[must_use]
    pub const fn strategy_config_artifact(&self) -> Option<&ArtifactReference> {
        self.strategy_config_artifact.as_ref()
    }

    /// Returns the runtime entrypoint evaluated by a strong release attestation.
    #[must_use]
    pub fn strategy_entrypoint(&self) -> Option<&str> {
        self.strategy_entrypoint.as_deref()
    }

    /// Returns the concrete strategy identity evaluated by a strong release attestation.
    #[must_use]
    pub fn strategy_runtime_id(&self) -> Option<&str> {
        self.strategy_runtime_id.as_deref()
    }

    /// Returns the digest of candidate and baseline observations.
    #[must_use]
    pub const fn observations_digest(&self) -> EvaluationDigest {
        self.observations_digest
    }

    /// Returns the digest of the suite's per-gate evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvaluationDigest {
        self.evidence_digest
    }

    /// Returns the optional baseline artifact identity.
    #[must_use]
    pub const fn baseline(&self) -> Option<&ArtifactReference> {
        self.baseline.as_ref()
    }

    /// Returns the deployment or release-channel scope for strong attestations.
    #[must_use]
    pub fn promotion_scope(&self) -> Option<&str> {
        self.promotion_scope.as_deref()
    }

    /// Returns the explicitly supplied evaluation timestamp.
    #[must_use]
    pub const fn evaluated_at(&self) -> UnixNanos {
        self.evaluated_at
    }

    /// Returns the canonical digest binding all attestation fields.
    #[must_use]
    pub const fn attestation_digest(&self) -> EvaluationDigest {
        self.attestation_digest
    }

    /// Recomputes the canonical digest from the attestation fields.
    ///
    /// Unlike [`Self::attestation_digest`], this value does not trust the stored digest field and
    /// is therefore suitable as input to a signature payload.
    #[must_use]
    pub fn canonical_digest(&self) -> EvaluationDigest {
        digest_attestation(AttestationDigestInput {
            suite_id: &self.suite_id,
            suite_version: &self.suite_version,
            suite_definition_digest: self.suite_definition_digest,
            candidate: &self.candidate,
            evidence_artifact: self.evidence_artifact.as_ref(),
            run_provenance_digest: self.run_provenance_digest.as_deref(),
            market_scope_commitment: self.market_scope_commitment.as_deref(),
            strategy_config_artifact: self.strategy_config_artifact.as_ref(),
            strategy_entrypoint: self.strategy_entrypoint.as_deref(),
            strategy_runtime_id: self.strategy_runtime_id.as_deref(),
            observations_digest: self.observations_digest,
            evidence_digest: self.evidence_digest,
            baseline: self.baseline.as_ref(),
            promotion_scope: self.promotion_scope.as_deref(),
            evaluated_at: self.evaluated_at,
        })
    }

    /// Builds the canonical 32-byte payload an evaluator must sign for `evaluator_key_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the attestation is not release-complete, is inconsistent, or the
    /// evaluator key ID is invalid.
    pub fn evaluator_signing_payload(
        &self,
        evaluator_key_id: &str,
    ) -> Result<[u8; 32], SignedAttestationError> {
        validate_evaluator_key_id(evaluator_key_id)?;
        if !self.is_release_complete() {
            return Err(SignedAttestationError::IncompleteReleaseAttestation);
        }
        if !self.is_internally_consistent() {
            return Err(SignedAttestationError::InconsistentAttestation);
        }
        Ok(*digest_evaluator_signature(evaluator_key_id, self.canonical_digest()).as_bytes())
    }

    /// Wraps this attestation and its detached evaluator signature in a validated envelope.
    ///
    /// # Errors
    ///
    /// Returns an error if the attestation, evaluator key ID, or signature shape is invalid.
    pub fn into_signed(
        self,
        evaluator_key_id: impl Into<String>,
        signature: impl Into<Vec<u8>>,
    ) -> Result<SignedPromotionAttestation, SignedAttestationError> {
        SignedPromotionAttestation::new(evaluator_key_id, self, signature)
    }

    /// Returns whether all fields still match the canonical attestation digest.
    #[must_use]
    pub fn is_internally_consistent(&self) -> bool {
        self.attestation_digest == self.canonical_digest()
    }

    /// Returns whether every production release field is present and content-digest shaped.
    #[must_use]
    pub fn is_release_complete(&self) -> bool {
        self.evidence_artifact.as_ref().is_some_and(|reference| {
            validate_release_artifact_reference("evidence_artifact", reference).is_ok()
        }) && self
            .run_provenance_digest
            .as_deref()
            .is_some_and(|digest| validate_release_digest("run_provenance_digest", digest).is_ok())
            && self
                .market_scope_commitment
                .as_deref()
                .is_some_and(|digest| {
                    validate_release_digest("market_scope_commitment", digest).is_ok()
                })
            && self
                .strategy_config_artifact
                .as_ref()
                .is_some_and(|reference| {
                    validate_release_artifact_reference("strategy_config_artifact", reference)
                        .is_ok()
                })
            && self
                .strategy_entrypoint
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && self
                .strategy_runtime_id
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && self
                .promotion_scope
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && validate_release_artifact_reference("candidate", &self.candidate).is_ok()
            && self.baseline.as_ref().is_none_or(|reference| {
                validate_release_artifact_reference("baseline", reference).is_ok()
            })
    }
}

/// A validated raw Ed25519 evaluator signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EvaluatorSignature(Vec<u8>);

impl EvaluatorSignature {
    /// Creates a signature value from exactly 64 raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `signature` is not exactly 64 bytes long.
    pub fn new(signature: impl Into<Vec<u8>>) -> Result<Self, SignedAttestationError> {
        let signature = signature.into();
        if signature.len() != ED25519_SIGNATURE_LENGTH {
            return Err(SignedAttestationError::InvalidEvaluatorSignatureLength {
                actual: signature.len(),
            });
        }
        Ok(Self(signature))
    }

    /// Returns the raw detached signature bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for EvaluatorSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let signature = Vec::<u8>::deserialize(deserializer)?;
        Self::new(signature).map_err(de::Error::custom)
    }
}

/// A versioned, release-complete promotion attestation carrying a detached evaluator signature.
///
/// This type defines and validates the canonical bytes to verify, but intentionally performs no
/// cryptographic verification. A trust-policy layer must resolve [`Self::evaluator_key_id`] to an
/// authorized public key and verify [`Self::signing_payload`] against [`Self::signature`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SignedPromotionAttestation {
    envelope_version: u32,
    evaluator_key_id: String,
    attestation: PromotionAttestation,
    signature: EvaluatorSignature,
}

impl SignedPromotionAttestation {
    /// Creates a validated signed-attestation envelope.
    ///
    /// This validates structure and digest consistency, not signature authenticity.
    ///
    /// # Errors
    ///
    /// Returns an error if the evaluator key ID, signature shape, or release attestation is
    /// incomplete or invalid.
    pub fn new(
        evaluator_key_id: impl Into<String>,
        attestation: PromotionAttestation,
        signature: impl Into<Vec<u8>>,
    ) -> Result<Self, SignedAttestationError> {
        let evaluator_key_id = evaluator_key_id.into();
        validate_evaluator_key_id(&evaluator_key_id)?;
        if !attestation.is_release_complete() {
            return Err(SignedAttestationError::IncompleteReleaseAttestation);
        }
        if !attestation.is_internally_consistent() {
            return Err(SignedAttestationError::InconsistentAttestation);
        }
        Ok(Self {
            envelope_version: SIGNED_PROMOTION_ATTESTATION_VERSION,
            evaluator_key_id,
            attestation,
            signature: EvaluatorSignature::new(signature)?,
        })
    }

    /// Returns the signed-envelope protocol version.
    #[must_use]
    pub const fn envelope_version(&self) -> u32 {
        self.envelope_version
    }

    /// Returns the trust-store identifier of the evaluator key.
    #[must_use]
    pub fn evaluator_key_id(&self) -> &str {
        &self.evaluator_key_id
    }

    /// Returns the enclosed promotion attestation.
    #[must_use]
    pub const fn attestation(&self) -> &PromotionAttestation {
        &self.attestation
    }

    /// Returns the raw detached evaluator signature.
    #[must_use]
    pub const fn signature(&self) -> &EvaluatorSignature {
        &self.signature
    }

    /// Returns the canonical 32-byte payload whose signature must be verified.
    #[must_use]
    pub fn signing_payload(&self) -> [u8; 32] {
        *digest_evaluator_signature(&self.evaluator_key_id, self.attestation.canonical_digest())
            .as_bytes()
    }

    /// Returns a canonical digest of the complete envelope, including its signature bytes.
    #[must_use]
    pub fn envelope_digest(&self) -> EvaluationDigest {
        digest_signed_attestation(
            self.envelope_version,
            &self.evaluator_key_id,
            self.attestation.canonical_digest(),
            self.signature.as_bytes(),
        )
    }

    /// Returns whether the envelope shape and enclosed attestation digest remain consistent.
    ///
    /// This is not a cryptographic signature verification result.
    #[must_use]
    pub fn is_internally_consistent(&self) -> bool {
        self.envelope_version == SIGNED_PROMOTION_ATTESTATION_VERSION
            && validate_evaluator_key_id(&self.evaluator_key_id).is_ok()
            && self.signature.as_bytes().len() == ED25519_SIGNATURE_LENGTH
            && self.attestation.is_release_complete()
            && self.attestation.is_internally_consistent()
    }
}

impl<'de> Deserialize<'de> for SignedPromotionAttestation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireEnvelope {
            envelope_version: u32,
            evaluator_key_id: String,
            attestation: PromotionAttestation,
            signature: EvaluatorSignature,
        }

        let envelope = WireEnvelope::deserialize(deserializer)?;
        if envelope.envelope_version != SIGNED_PROMOTION_ATTESTATION_VERSION {
            return Err(de::Error::custom(
                SignedAttestationError::UnsupportedEnvelopeVersion {
                    expected: SIGNED_PROMOTION_ATTESTATION_VERSION,
                    actual: envelope.envelope_version,
                },
            ));
        }
        Self::new(
            envelope.evaluator_key_id,
            envelope.attestation,
            envelope.signature.0,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid suite or gate definitions.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum EvaluationError {
    #[error("evaluation suite ID must not be empty")]
    EmptySuiteId,
    #[error("evaluation suite version must not be empty")]
    EmptySuiteVersion,
    #[error("evaluation suite must contain at least one gate")]
    EmptyGateSet,
    #[error("evaluation metric name must not be empty")]
    EmptyMetric,
    #[error("threshold for metric '{metric}' must be finite, received {value}")]
    NonFiniteThreshold { metric: String, value: f64 },
    #[error(
        "inclusive range for metric '{metric}' is invalid: minimum {minimum} exceeds maximum {maximum}"
    )]
    InvalidRange {
        metric: String,
        minimum: f64,
        maximum: f64,
    },
    #[error(
        "maximum regression for metric '{metric}' must be non-negative, received {maximum_regression}"
    )]
    NegativeMaximumRegression {
        metric: String,
        maximum_regression: f64,
    },
}

/// Errors preventing creation of a promotion attestation.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AttestationError {
    #[error("evaluation outcome failed its internal integrity checks")]
    InconsistentOutcome,
    #[error("promotion attestation requires a passing evaluation outcome")]
    OutcomeFailed,
    #[error("artifact ID must not be empty")]
    EmptyArtifactId,
    #[error("artifact digest must not be empty")]
    EmptyArtifactDigest,
    #[error("baseline artifact identity is required for a baseline-dependent evaluation")]
    BaselineArtifactRequired,
    #[error("promotion scope must not be empty")]
    EmptyPromotionScope,
    #[error("run provenance digest must not be empty")]
    EmptyRunProvenanceDigest,
    #[error("market scope commitment must not be empty")]
    EmptyMarketScopeCommitment,
    #[error("release field '{field}' must be a canonical 64-character lowercase hex digest")]
    InvalidReleaseDigest { field: &'static str },
    #[error("strategy entrypoint must not be empty")]
    EmptyStrategyEntrypoint,
    #[error("strategy runtime ID must not be empty")]
    EmptyStrategyRuntimeId,
}

/// Errors preventing construction or deserialization of a signed promotion attestation.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SignedAttestationError {
    #[error("unsupported signed-attestation envelope version {actual}; expected {expected}")]
    UnsupportedEnvelopeVersion { expected: u32, actual: u32 },
    #[error("evaluator key ID must not be empty")]
    EmptyEvaluatorKeyId,
    #[error(
        "evaluator key ID must be at most 255 ASCII bytes and contain only alphanumeric characters or '-', '_', '.', ':', '/'"
    )]
    InvalidEvaluatorKeyId,
    #[error("invalid evaluator signature length: expected 64 bytes, got {actual}")]
    InvalidEvaluatorSignatureLength { actual: usize },
    #[error("promotion attestation failed its internal integrity checks")]
    InconsistentAttestation,
    #[error("only complete release promotion attestations can be evaluator-signed")]
    IncompleteReleaseAttestation,
}

fn validate_release_artifact_reference(
    field: &'static str,
    reference: &ArtifactReference,
) -> Result<(), AttestationError> {
    validate_release_digest(field, reference.digest())
}

fn validate_release_digest(field: &'static str, digest: &str) -> Result<(), AttestationError> {
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(AttestationError::InvalidReleaseDigest { field })
    }
}

fn validate_evaluator_key_id(evaluator_key_id: &str) -> Result<(), SignedAttestationError> {
    if evaluator_key_id.is_empty() {
        return Err(SignedAttestationError::EmptyEvaluatorKeyId);
    }
    if evaluator_key_id.len() > MAX_EVALUATOR_KEY_ID_LENGTH
        || !evaluator_key_id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(SignedAttestationError::InvalidEvaluatorKeyId);
    }
    Ok(())
}

fn validate_threshold(metric: &str, value: f64) -> Result<(), EvaluationError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(EvaluationError::NonFiniteThreshold {
            metric: metric.to_owned(),
            value,
        })
    }
}

fn evaluate_gate(
    gate: EvaluationGate,
    candidate: &Observations,
    baseline: Option<&Observations>,
) -> GateEvidence {
    let observed = candidate.get(gate.metric()).copied();
    let baseline_value = baseline
        .and_then(|values| values.get(gate.metric()))
        .copied();
    let reason = evaluate_gate_reason(&gate, observed, baseline_value);

    GateEvidence {
        gate,
        observed,
        baseline: baseline_value,
        passed: reason == GateReason::Passed,
        reason,
    }
}

fn evaluate_gate_reason(
    gate: &EvaluationGate,
    observed: Option<f64>,
    baseline: Option<f64>,
) -> GateReason {
    match observed {
        None => GateReason::MissingObservation,
        Some(value) if !value.is_finite() => GateReason::NonFiniteObservation,
        Some(value) if gate.needs_baseline() => match baseline {
            None => GateReason::MissingBaseline,
            Some(baseline) if !baseline.is_finite() => GateReason::NonFiniteBaseline,
            Some(baseline) => compare_gate(gate, value, baseline),
        },
        Some(value) => compare_gate(gate, value, 0.0),
    }
}

fn compare_gate(gate: &EvaluationGate, observed: f64, baseline: f64) -> GateReason {
    match gate {
        EvaluationGate::Minimum { minimum, .. } if observed < *minimum => GateReason::BelowMinimum,
        EvaluationGate::Maximum { maximum, .. } if observed > *maximum => GateReason::AboveMaximum,
        EvaluationGate::InclusiveRange {
            minimum, maximum, ..
        } if observed < *minimum || observed > *maximum => GateReason::OutsideInclusiveRange,
        EvaluationGate::MaxRegression {
            maximum_regression, ..
        } => compare_regression(observed, baseline, *maximum_regression),
        _ => GateReason::Passed,
    }
}

fn compare_regression(observed: f64, baseline: f64, maximum_regression: f64) -> GateReason {
    if observed >= baseline {
        return GateReason::Passed;
    }

    let regression = baseline - observed;
    if !regression.is_finite() || regression > maximum_regression {
        GateReason::RegressionExceeded
    } else {
        GateReason::Passed
    }
}

fn digest_observations(
    candidate: &Observations,
    baseline: Option<&Observations>,
) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBSERVATIONS_DOMAIN);
    write_observation_map(&mut hasher, candidate);
    write_optional_observation_map(&mut hasher, baseline);
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn digest_suite(suite_id: &str, version: &str, gates: &[EvaluationGate]) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SUITE_DOMAIN);
    write_str(&mut hasher, suite_id);
    write_str(&mut hasher, version);
    write_len(&mut hasher, gates.len());
    for gate in gates {
        write_gate_definition(&mut hasher, gate);
    }
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn write_gate_definition(hasher: &mut blake3::Hasher, gate: &EvaluationGate) {
    hasher.update(&[gate.kind_tag()]);
    write_str(hasher, gate.metric());
    match gate {
        EvaluationGate::Minimum { minimum, .. } => write_f64(hasher, *minimum),
        EvaluationGate::Maximum { maximum, .. } => write_f64(hasher, *maximum),
        EvaluationGate::InclusiveRange {
            minimum, maximum, ..
        } => {
            write_f64(hasher, *minimum);
            write_f64(hasher, *maximum);
        }
        EvaluationGate::MaxRegression {
            maximum_regression, ..
        } => write_f64(hasher, *maximum_regression),
    }
}

fn digest_evidence(suite_id: &str, version: &str, evidence: &[GateEvidence]) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVIDENCE_DOMAIN);
    write_str(&mut hasher, suite_id);
    write_str(&mut hasher, version);
    write_len(&mut hasher, evidence.len());
    for item in evidence {
        write_gate_definition(&mut hasher, &item.gate);
        write_optional_f64(&mut hasher, item.observed);
        write_optional_f64(&mut hasher, item.baseline);
        hasher.update(&[u8::from(item.passed), reason_tag(item.reason)]);
    }
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn seal_outcome(
    suite_id: &str,
    suite_version: &str,
    suite_definition_digest: EvaluationDigest,
    passed: bool,
    baseline_required: bool,
    observations_digest: EvaluationDigest,
    evidence_digest: EvaluationDigest,
) -> OutcomeSeal {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OUTCOME_SEAL_DOMAIN);
    write_str(&mut hasher, suite_id);
    write_str(&mut hasher, suite_version);
    hasher.update(suite_definition_digest.as_bytes());
    hasher.update(&[u8::from(passed), u8::from(baseline_required)]);
    hasher.update(observations_digest.as_bytes());
    hasher.update(evidence_digest.as_bytes());
    OutcomeSeal(EvaluationDigest(*hasher.finalize().as_bytes()))
}

#[derive(Clone, Copy)]
struct AttestationDigestInput<'a> {
    suite_id: &'a str,
    suite_version: &'a str,
    suite_definition_digest: EvaluationDigest,
    candidate: &'a ArtifactReference,
    evidence_artifact: Option<&'a ArtifactReference>,
    run_provenance_digest: Option<&'a str>,
    market_scope_commitment: Option<&'a str>,
    strategy_config_artifact: Option<&'a ArtifactReference>,
    strategy_entrypoint: Option<&'a str>,
    strategy_runtime_id: Option<&'a str>,
    observations_digest: EvaluationDigest,
    evidence_digest: EvaluationDigest,
    baseline: Option<&'a ArtifactReference>,
    promotion_scope: Option<&'a str>,
    evaluated_at: UnixNanos,
}

fn digest_attestation(input: AttestationDigestInput<'_>) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ATTESTATION_DOMAIN);
    write_str(&mut hasher, input.suite_id);
    write_str(&mut hasher, input.suite_version);
    hasher.update(input.suite_definition_digest.as_bytes());
    write_artifact(&mut hasher, input.candidate);
    write_optional_artifact(&mut hasher, input.evidence_artifact);
    match input.run_provenance_digest {
        Some(digest) => {
            hasher.update(&[1]);
            write_str(&mut hasher, digest);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    write_optional_str(&mut hasher, input.market_scope_commitment);
    write_optional_artifact(&mut hasher, input.strategy_config_artifact);
    write_optional_str(&mut hasher, input.strategy_entrypoint);
    write_optional_str(&mut hasher, input.strategy_runtime_id);
    hasher.update(input.observations_digest.as_bytes());
    hasher.update(input.evidence_digest.as_bytes());
    match input.baseline {
        Some(artifact) => {
            hasher.update(&[1]);
            write_artifact(&mut hasher, artifact);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    match input.promotion_scope {
        Some(scope) => {
            hasher.update(&[1]);
            write_str(&mut hasher, scope);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&input.evaluated_at.as_u64().to_be_bytes());
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn digest_evaluator_signature(
    evaluator_key_id: &str,
    attestation_digest: EvaluationDigest,
) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVALUATOR_SIGNATURE_DOMAIN);
    hasher.update(&SIGNED_PROMOTION_ATTESTATION_VERSION.to_be_bytes());
    write_str(&mut hasher, evaluator_key_id);
    hasher.update(attestation_digest.as_bytes());
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn digest_signed_attestation(
    envelope_version: u32,
    evaluator_key_id: &str,
    attestation_digest: EvaluationDigest,
    signature: &[u8],
) -> EvaluationDigest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SIGNED_ATTESTATION_DOMAIN);
    hasher.update(&envelope_version.to_be_bytes());
    write_str(&mut hasher, evaluator_key_id);
    hasher.update(attestation_digest.as_bytes());
    write_bytes(&mut hasher, signature);
    EvaluationDigest(*hasher.finalize().as_bytes())
}

fn write_optional_str(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            write_str(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn write_optional_artifact(hasher: &mut blake3::Hasher, artifact: Option<&ArtifactReference>) {
    match artifact {
        Some(artifact) => {
            hasher.update(&[1]);
            write_artifact(hasher, artifact);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn write_artifact(hasher: &mut blake3::Hasher, artifact: &ArtifactReference) {
    write_str(hasher, &artifact.artifact_id);
    write_str(hasher, &artifact.digest);
}

fn write_observation_map(hasher: &mut blake3::Hasher, observations: &Observations) {
    write_len(hasher, observations.len());
    for (metric, value) in observations {
        write_str(hasher, metric);
        write_f64(hasher, *value);
    }
}

fn write_optional_observation_map(
    hasher: &mut blake3::Hasher,
    observations: Option<&Observations>,
) {
    match observations {
        Some(observations) => {
            hasher.update(&[1]);
            write_observation_map(hasher, observations);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn write_optional_f64(hasher: &mut blake3::Hasher, value: Option<f64>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            write_f64(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn write_f64(hasher: &mut blake3::Hasher, value: f64) {
    let bits = if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    };
    hasher.update(&bits.to_be_bytes());
}

fn write_str(hasher: &mut blake3::Hasher, value: &str) {
    write_bytes(hasher, value.as_bytes());
}

fn write_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    write_len(hasher, value.len());
    hasher.update(value);
}

fn write_len(hasher: &mut blake3::Hasher, value: usize) {
    hasher.update(&(value as u64).to_be_bytes());
}

const fn reason_tag(reason: GateReason) -> u8 {
    match reason {
        GateReason::Passed => 0,
        GateReason::MissingObservation => 1,
        GateReason::MissingBaseline => 2,
        GateReason::NonFiniteObservation => 3,
        GateReason::NonFiniteBaseline => 4,
        GateReason::BelowMinimum => 5,
        GateReason::AboveMaximum => 6,
        GateReason::OutsideInclusiveRange => 7,
        GateReason::RegressionExceeded => 8,
    }
}

const fn nibble_to_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + nibble - 10) as char,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn observations(values: &[(&str, f64)]) -> Observations {
        values
            .iter()
            .map(|(metric, value)| ((*metric).to_owned(), *value))
            .collect()
    }

    fn suite(gates: Vec<EvaluationGate>) -> EvaluationSuite {
        EvaluationSuite::new("research-promotion", "2026.1", gates).unwrap()
    }

    fn strong_attestation(evaluated_at: u64) -> PromotionAttestation {
        strong_attestation_with_market_scope(evaluated_at, &"e".repeat(64))
    }

    fn strong_attestation_with_market_scope(
        evaluated_at: u64,
        market_scope_commitment: &str,
    ) -> PromotionAttestation {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);
        PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new("strategy:release", "a".repeat(64)).unwrap(),
            ArtifactReference::new("backtest-result:run", "b".repeat(64)).unwrap(),
            "c".repeat(64),
            market_scope_commitment,
            ArtifactReference::new("config:strategy", "d".repeat(64)).unwrap(),
            "weilan::Strategy",
            "weilan-release-001",
            None,
            "okx-production-a",
            UnixNanos::new(evaluated_at),
        )
        .unwrap()
    }

    #[rstest]
    fn evaluation_passes_all_gate_types_with_arbitrary_finite_values() {
        let suite = suite(vec![
            Gate::Minimum {
                metric: "pnl".into(),
                minimum: -25.0,
            },
            Gate::Maximum {
                metric: "latency".into(),
                maximum: 250.0,
            },
            Gate::InclusiveRange {
                metric: "exposure".into(),
                minimum: -2.0,
                maximum: 3.0,
            },
            Gate::MaxRegression {
                metric: "sharpe".into(),
                maximum_regression: 0.2,
            },
        ]);
        let candidate = observations(&[
            ("pnl", -20.0),
            ("latency", 240.0),
            ("exposure", -1.5),
            ("sharpe", 1.3),
        ]);
        let baseline = observations(&[("sharpe", 1.5)]);

        let outcome = suite.evaluate(&candidate, Some(&baseline));

        assert!(outcome.passed());
        assert!(outcome.evidence.iter().all(|item| item.passed));
    }

    #[rstest]
    #[case(
        Gate::Minimum { metric: "value".into(), minimum: 5.0 },
        4.9,
        GateReason::BelowMinimum
    )]
    #[case(
        Gate::Maximum { metric: "value".into(), maximum: -5.0 },
        -4.9,
        GateReason::AboveMaximum
    )]
    #[case(
        Gate::InclusiveRange { metric: "value".into(), minimum: -2.0, maximum: -1.0 },
        0.0,
        GateReason::OutsideInclusiveRange
    )]
    fn threshold_failures_record_evidence(
        #[case] gate: EvaluationGate,
        #[case] value: f64,
        #[case] expected: GateReason,
    ) {
        let outcome = suite(vec![gate]).evaluate(&observations(&[("value", value)]), None);

        assert!(!outcome.passed());
        assert_eq!(outcome.evidence[0].observed, Some(value));
        assert_eq!(outcome.evidence[0].reason, expected);
    }

    #[rstest]
    #[case(Observations::new(), None, GateReason::MissingObservation)]
    #[case(observations(&[("value", f64::NAN)]), None, GateReason::NonFiniteObservation)]
    #[case(observations(&[("value", f64::INFINITY)]), None, GateReason::NonFiniteObservation)]
    fn candidate_data_failures_are_closed(
        #[case] candidate: Observations,
        #[case] baseline: Option<Observations>,
        #[case] expected: GateReason,
    ) {
        let outcome = suite(vec![Gate::Minimum {
            metric: "value".into(),
            minimum: -100.0,
        }])
        .evaluate(&candidate, baseline.as_ref());

        assert!(!outcome.passed());
        assert_eq!(outcome.evidence[0].reason, expected);
    }

    #[rstest]
    #[case(None, GateReason::MissingBaseline)]
    #[case(Some(observations(&[("value", f64::NEG_INFINITY)])), GateReason::NonFiniteBaseline)]
    #[case(Some(observations(&[("value", 10.0)])), GateReason::RegressionExceeded)]
    fn regression_failures_are_closed(
        #[case] baseline: Option<Observations>,
        #[case] expected: GateReason,
    ) {
        let outcome = suite(vec![Gate::MaxRegression {
            metric: "value".into(),
            maximum_regression: 1.0,
        }])
        .evaluate(&observations(&[("value", 8.0)]), baseline.as_ref());

        assert!(!outcome.passed());
        assert_eq!(outcome.evidence[0].reason, expected);
    }

    #[rstest]
    fn failed_outcome_cannot_create_attestation() {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 0.0)]), None);
        let candidate = ArtifactReference::new("candidate-a", "sha256:a").unwrap();

        let error = PromotionAttestation::from_passing_outcome(
            &outcome,
            candidate,
            None,
            UnixNanos::new(42),
        )
        .unwrap_err();

        assert_eq!(error, AttestationError::OutcomeFailed);
    }

    #[rstest]
    fn attestation_digest_is_deterministic_and_candidate_bound() {
        let suite = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: -5.0,
        }]);
        let outcome_a = suite.evaluate(&observations(&[("z", 9.0), ("score", -4.0)]), None);
        let outcome_b = suite.evaluate(&observations(&[("score", -4.0), ("z", 9.0)]), None);
        let make = |outcome: &EvaluationOutcome, artifact_id: &str| {
            PromotionAttestation::from_passing_outcome(
                outcome,
                ArtifactReference::new(artifact_id, "sha256:abc").unwrap(),
                None,
                UnixNanos::new(123),
            )
            .unwrap()
        };

        let first = make(&outcome_a, "candidate-a");
        let repeated = make(&outcome_b, "candidate-a");
        let other_candidate = make(&outcome_a, "candidate-b");

        assert_eq!(first.attestation_digest(), repeated.attestation_digest());
        assert_ne!(
            first.attestation_digest(),
            other_candidate.attestation_digest()
        );
        assert_ne!(first.candidate(), other_candidate.candidate());
    }

    #[rstest]
    fn baseline_evaluation_requires_and_binds_baseline_artifact() {
        let outcome = suite(vec![Gate::MaxRegression {
            metric: "score".into(),
            maximum_regression: 1.0,
        }])
        .evaluate(
            &observations(&[("score", 9.5)]),
            Some(&observations(&[("score", 10.0)])),
        );
        let candidate = ArtifactReference::new("candidate", "sha256:candidate").unwrap();

        let error = PromotionAttestation::from_passing_outcome(
            &outcome,
            candidate.clone(),
            None,
            UnixNanos::new(7),
        )
        .unwrap_err();
        assert_eq!(error, AttestationError::BaselineArtifactRequired);

        let first = PromotionAttestation::from_passing_outcome(
            &outcome,
            candidate.clone(),
            Some(ArtifactReference::new("baseline-a", "sha256:a").unwrap()),
            UnixNanos::new(7),
        )
        .unwrap();
        let second = PromotionAttestation::from_passing_outcome(
            &outcome,
            candidate,
            Some(ArtifactReference::new("baseline-b", "sha256:b").unwrap()),
            UnixNanos::new(7),
        )
        .unwrap();

        assert_ne!(first.attestation_digest(), second.attestation_digest());
    }

    #[rstest]
    fn strong_attestation_round_trips_across_process_boundary() {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new("strategy:release", "a".repeat(64)).unwrap(),
            ArtifactReference::new("backtest-result:run", "b".repeat(64)).unwrap(),
            "c".repeat(64),
            "e".repeat(64),
            ArtifactReference::new("config:strategy", "d".repeat(64)).unwrap(),
            "weilan::Strategy",
            "weilan-release-001",
            None,
            "okx-production-a",
            UnixNanos::new(42),
        )
        .unwrap();

        let json = serde_json::to_string(&attestation).unwrap();
        let decoded: PromotionAttestation = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, attestation);
        assert!(decoded.is_internally_consistent());
        let expected_market_scope_commitment = "e".repeat(64);
        assert_eq!(
            decoded.market_scope_commitment(),
            Some(expected_market_scope_commitment.as_str())
        );
    }

    #[rstest]
    fn release_attestation_rejects_noncanonical_content_digests() {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);

        let error = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new("strategy:release", "A".repeat(64)).unwrap(),
            ArtifactReference::new("backtest-result:run", "b".repeat(64)).unwrap(),
            "c".repeat(64),
            "e".repeat(64),
            ArtifactReference::new("config:strategy", "d".repeat(64)).unwrap(),
            "weilan::Strategy",
            "weilan-release-001",
            None,
            "okx-production-a",
            UnixNanos::new(42),
        )
        .unwrap_err();

        assert_eq!(
            error,
            AttestationError::InvalidReleaseDigest { field: "candidate" }
        );
    }

    #[rstest]
    #[case("", AttestationError::EmptyMarketScopeCommitment)]
    #[case(
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        AttestationError::InvalidReleaseDigest { field: "market_scope_commitment" }
    )]
    #[case(
        "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
        AttestationError::InvalidReleaseDigest { field: "market_scope_commitment" }
    )]
    #[case(
        "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
        AttestationError::InvalidReleaseDigest { field: "market_scope_commitment" }
    )]
    fn release_attestation_rejects_invalid_market_scope_commitment(
        #[case] market_scope_commitment: &str,
        #[case] expected: AttestationError,
    ) {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);

        let error = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new("strategy:release", "a".repeat(64)).unwrap(),
            ArtifactReference::new("backtest-result:run", "b".repeat(64)).unwrap(),
            "c".repeat(64),
            market_scope_commitment,
            ArtifactReference::new("config:strategy", "d".repeat(64)).unwrap(),
            "weilan::Strategy",
            "weilan-release-001",
            None,
            "okx-production-a",
            UnixNanos::new(42),
        )
        .unwrap_err();

        assert_eq!(error, expected);
    }

    #[rstest]
    fn research_attestation_cannot_enter_signed_release_envelope() {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome(
            &outcome,
            ArtifactReference::new("research:candidate", "sha256:candidate").unwrap(),
            None,
            UnixNanos::new(42),
        )
        .unwrap();

        assert_eq!(
            attestation
                .evaluator_signing_payload("evaluator:key-1")
                .unwrap_err(),
            SignedAttestationError::IncompleteReleaseAttestation
        );
    }

    #[rstest]
    fn deserialized_attestation_tampering_fails_integrity_check() {
        let outcome = suite(vec![Gate::Minimum {
            metric: "score".into(),
            minimum: 1.0,
        }])
        .evaluate(&observations(&[("score", 2.0)]), None);
        let attestation = PromotionAttestation::from_passing_outcome_with_evidence(
            &outcome,
            ArtifactReference::new("strategy:release", "a".repeat(64)).unwrap(),
            ArtifactReference::new("backtest-result:run", "b".repeat(64)).unwrap(),
            "c".repeat(64),
            "e".repeat(64),
            ArtifactReference::new("config:strategy", "d".repeat(64)).unwrap(),
            "weilan::Strategy",
            "weilan-release-001",
            None,
            "okx-production-a",
            UnixNanos::new(42),
        )
        .unwrap();
        let mut value = serde_json::to_value(attestation).unwrap();
        value["promotion_scope"] = serde_json::Value::String("other-deployment".to_string());

        let decoded: PromotionAttestation = serde_json::from_value(value).unwrap();

        assert!(!decoded.is_internally_consistent());
    }

    #[rstest]
    fn market_scope_commitment_is_bound_by_attestation_and_evaluator_payload() {
        let first = strong_attestation_with_market_scope(42, &"e".repeat(64));
        let other_scope = strong_attestation_with_market_scope(42, &"f".repeat(64));

        assert_ne!(first.attestation_digest(), other_scope.attestation_digest());
        assert_ne!(
            first.evaluator_signing_payload("evaluator:key-1").unwrap(),
            other_scope
                .evaluator_signing_payload("evaluator:key-1")
                .unwrap()
        );

        let mut tampered = serde_json::to_value(first).unwrap();
        tampered["market_scope_commitment"] = serde_json::json!("f".repeat(64));
        let decoded: PromotionAttestation = serde_json::from_value(tampered).unwrap();
        assert!(!decoded.is_internally_consistent());
    }

    #[rstest]
    fn evaluator_signing_payload_is_deterministic_and_binds_identity_and_attestation() {
        let attestation = strong_attestation(42);

        let first = attestation
            .evaluator_signing_payload("evaluator:key-1")
            .unwrap();
        let second = attestation
            .evaluator_signing_payload("evaluator:key-1")
            .unwrap();
        let other_key = attestation
            .evaluator_signing_payload("evaluator:key-2")
            .unwrap();
        let other_attestation = strong_attestation(43)
            .evaluator_signing_payload("evaluator:key-1")
            .unwrap();

        assert_eq!(first, second);
        assert_ne!(first, other_key);
        assert_ne!(first, other_attestation);
    }

    #[rstest]
    fn signed_attestation_round_trips_and_exposes_verification_material() {
        let attestation = strong_attestation(42);
        let expected_payload = attestation
            .evaluator_signing_payload("evaluator:key-1")
            .unwrap();
        let envelope = attestation
            .into_signed("evaluator:key-1", [7_u8; 64])
            .unwrap();

        let json = serde_json::to_string(&envelope).unwrap();
        let decoded: SignedPromotionAttestation = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, envelope);
        assert_eq!(
            decoded.envelope_version(),
            SIGNED_PROMOTION_ATTESTATION_VERSION
        );
        assert_eq!(decoded.evaluator_key_id(), "evaluator:key-1");
        assert_eq!(decoded.signature().as_bytes(), &[7_u8; 64]);
        assert_eq!(decoded.signing_payload(), expected_payload);
        assert!(decoded.is_internally_consistent());
    }

    #[rstest]
    #[case("", SignedAttestationError::EmptyEvaluatorKeyId)]
    #[case("key with spaces", SignedAttestationError::InvalidEvaluatorKeyId)]
    #[case("keyø", SignedAttestationError::InvalidEvaluatorKeyId)]
    fn signed_attestation_rejects_invalid_evaluator_key_id(
        #[case] key_id: &str,
        #[case] expected: SignedAttestationError,
    ) {
        assert_eq!(
            SignedPromotionAttestation::new(key_id, strong_attestation(42), [0_u8; 64]),
            Err(expected)
        );
    }

    #[rstest]
    fn signed_attestation_rejects_invalid_signature_length() {
        assert_eq!(
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [0_u8; 63]),
            Err(SignedAttestationError::InvalidEvaluatorSignatureLength { actual: 63 })
        );
    }

    #[rstest]
    fn signed_attestation_deserialization_rejects_unknown_protocol_version() {
        let envelope =
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [0_u8; 64])
                .unwrap();
        let mut value = serde_json::to_value(envelope).unwrap();
        value["envelope_version"] = serde_json::json!(SIGNED_PROMOTION_ATTESTATION_VERSION + 1);

        let error = serde_json::from_value::<SignedPromotionAttestation>(value).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("expected {SIGNED_PROMOTION_ATTESTATION_VERSION}"))
        );
    }

    #[rstest]
    fn signed_attestation_deserialization_rejects_legacy_version_and_missing_market_scope() {
        let envelope =
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [0_u8; 64])
                .unwrap();

        let mut legacy = serde_json::to_value(&envelope).unwrap();
        legacy["envelope_version"] = serde_json::json!(1);
        assert!(serde_json::from_value::<SignedPromotionAttestation>(legacy).is_err());

        let mut missing_scope = serde_json::to_value(envelope).unwrap();
        missing_scope["attestation"]
            .as_object_mut()
            .unwrap()
            .remove("market_scope_commitment");
        let error =
            serde_json::from_value::<SignedPromotionAttestation>(missing_scope).unwrap_err();
        assert!(error.to_string().contains("complete release"));
    }

    #[rstest]
    fn signed_attestation_deserialization_rejects_tampered_attestation() {
        let envelope =
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [0_u8; 64])
                .unwrap();
        let mut value = serde_json::to_value(envelope).unwrap();
        value["attestation"]["promotion_scope"] = serde_json::json!("other-deployment");

        let error = serde_json::from_value::<SignedPromotionAttestation>(value).unwrap_err();

        assert!(error.to_string().contains("integrity checks"));
    }

    #[rstest]
    fn signed_attestation_envelope_digest_binds_signature_bytes() {
        let first =
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [0_u8; 64])
                .unwrap();
        let second =
            SignedPromotionAttestation::new("evaluator:key-1", strong_attestation(42), [1_u8; 64])
                .unwrap();

        assert_eq!(first.signing_payload(), second.signing_payload());
        assert_ne!(first.envelope_digest(), second.envelope_digest());
    }

    #[rstest]
    fn artifact_reference_deserialization_rejects_empty_identity() {
        let error = serde_json::from_str::<ArtifactReference>(
            r#"{"artifact_id":"","digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("artifact ID must not be empty"));
    }
}
