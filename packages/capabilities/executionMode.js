'use strict';

/**
 * Capability execution modes (SPEC-058 / ADR-042).
 *
 * Execution — decide whether work may run (boolean canRun).
 * Diagnostic — explain why work cannot run (diagnoseCanRun).
 */

const {
  CAPABILITY_EXECUTION_MODES,
  CAPABILITY_RESULT_STATUS,
  buildCapabilityResult,
} = require('./types');

const DIAGNOSTIC_INTENT_CATEGORIES = Object.freeze([
  'campaign_diagnostics',
  'discovery_investigation',
  'diagnostics',
]);

/**
 * Resolve CapabilityRunner execution mode from context / mission intent.
 * Does not change Mission Planning — only how blocked preconditions are reported.
 * @param {object} [context]
 * @param {object} [capability]
 * @returns {'execution'|'diagnostic'}
 */
function resolveCapabilityExecutionMode(context = {}, capability = null) {
  const explicit =
    context.executionMode ||
    context.mode ||
    (context.constraints && context.constraints.executionMode) ||
    (context.inputs && context.inputs.executionMode) ||
    null;
  if (
    explicit === CAPABILITY_EXECUTION_MODES.DIAGNOSTIC ||
    explicit === 'diagnostics'
  ) {
    return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
  }
  if (explicit === CAPABILITY_EXECUTION_MODES.EXECUTION) {
    return CAPABILITY_EXECUTION_MODES.EXECUTION;
  }

  const intent =
    context.missionIntent ||
    (context.missionPlan && context.missionPlan.missionIntent) ||
    (context.constraints && context.constraints.missionIntent) ||
    null;
  if (intent) {
    if (intent.diagnostics === true) {
      return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
    }
    if (
      intent.mode === 'diagnostics' ||
      intent.mode === CAPABILITY_EXECUTION_MODES.DIAGNOSTIC ||
      intent.mode === 'investigation'
    ) {
      return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
    }
    const category = intent.matchedIntent || intent.intentCategory || '';
    if (DIAGNOSTIC_INTENT_CATEGORIES.includes(String(category))) {
      return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
    }
  }

  if (capability && capability.diagnostic === true) {
    return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
  }

  return CAPABILITY_EXECUTION_MODES.EXECUTION;
}

/**
 * Normalize diagnoseCanRun() return value to SPEC-058 contract.
 * @param {object|boolean|null} raw
 * @param {string} capabilityId
 * @returns {object}
 */
function normalizeDiagnoseCanRun(raw, capabilityId) {
  if (raw === true) {
    return {
      runnable: true,
      reason: null,
      failedPrecondition: null,
      expectedArtifact: null,
      actualState: null,
      producer: null,
      recommendedNextAction: null,
    };
  }
  if (raw === false || raw == null) {
    return {
      runnable: false,
      reason: 'canRun precondition failed',
      failedPrecondition: 'canRun precondition failed',
      expectedArtifact: null,
      actualState: 'Not Present',
      producer: null,
      recommendedNextAction:
        'Inspect capability inputs and ensure required artifacts are present.',
      capabilityId,
    };
  }
  if (typeof raw !== 'object') {
    return normalizeDiagnoseCanRun(false, capabilityId);
  }

  const runnable =
    raw.runnable != null
      ? Boolean(raw.runnable)
      : raw.ok != null
        ? Boolean(raw.ok)
        : false;
  const failedPrecondition =
    raw.failedPrecondition ||
    raw.reason ||
    (runnable ? null : 'canRun precondition failed');
  const producer = raw.producer || raw.expectedProducer || null;

  return {
    ...raw,
    runnable,
    reason: raw.reason || failedPrecondition,
    failedPrecondition,
    expectedArtifact: raw.expectedArtifact || null,
    actualState: raw.actualState || (runnable ? null : 'Not Present'),
    producer,
    expectedProducer: raw.expectedProducer || producer,
    recommendedNextAction:
      raw.recommendedNextAction ||
      (runnable
        ? null
        : 'Inspect capability inputs and ensure required artifacts are present.'),
    capabilityId: raw.capabilityId || capabilityId,
  };
}

/**
 * Build structured blocked / failed precondition result (SPEC-058).
 * Never fabricates ReviewDecision / business outputs.
 * @param {object} input
 */
function buildPreconditionBlockedResult(input = {}) {
  const capabilityId = input.capabilityId || 'capability';
  const diagnosis = normalizeDiagnoseCanRun(input.diagnosis, capabilityId);
  const diagnosticMode = Boolean(input.diagnosticMode);
  const status = diagnosticMode
    ? CAPABILITY_RESULT_STATUS.BLOCKED
    : CAPABILITY_RESULT_STATUS.FAILED;

  const structured = {
    code: diagnosticMode
      ? 'can_run_precondition_blocked'
      : 'can_run_precondition_failed',
    message: diagnosis.failedPrecondition || 'canRun precondition blocked',
    capabilityId,
    failedPrecondition: diagnosis.failedPrecondition,
    expectedArtifact: diagnosis.expectedArtifact,
    actualState: diagnosis.actualState,
    producer: diagnosis.producer || diagnosis.expectedProducer,
    expectedProducer: diagnosis.expectedProducer || diagnosis.producer,
    recommendedNextAction: diagnosis.recommendedNextAction,
    diagnosis,
  };

  const preconditionDiagnostics = {
    artifactType: 'PreconditionDiagnostics',
    readOnly: true,
    diagnostic: true,
    mutatesBusinessState: false,
    capabilityId,
    status: 'Blocked',
    failedPrecondition: structured.failedPrecondition,
    expectedArtifact: structured.expectedArtifact,
    actualState: structured.actualState,
    producer: structured.producer,
    expectedProducer: structured.expectedProducer,
    recommendedNextAction: structured.recommendedNextAction,
  };

  return buildCapabilityResult({
    status,
    errors: [structured],
    outputs: {
      readOnly: true,
      mutatesBusinessState: false,
      executionMode: diagnosticMode
        ? CAPABILITY_EXECUTION_MODES.DIAGNOSTIC
        : CAPABILITY_EXECUTION_MODES.EXECUTION,
      preconditionDiagnostics,
      // Never fabricate business artifacts from blocked preconditions
      reviewDecision: null,
      reviewPackage: null,
    },
    evidence: [
      {
        kind: 'diagnostics',
        summary: structured.failedPrecondition,
        readOnly: true,
        failedPrecondition: structured.failedPrecondition,
        expectedArtifact: structured.expectedArtifact,
        actualState: structured.actualState,
        producer: structured.producer,
        recommendedNextAction: structured.recommendedNextAction,
      },
    ],
    // Diagnostic explanations stay in outputs — do not publish empty
    // stage produces (ReviewDecision). No fabricated bus artifacts.
    artifacts: [],
    warnings: [
      `Blocked precondition: ${structured.failedPrecondition}`,
      structured.expectedArtifact
        ? `Expected artifact: ${structured.expectedArtifact}`
        : null,
      structured.actualState
        ? `Actual state: ${structured.actualState}`
        : null,
      structured.producer ? `Producer: ${structured.producer}` : null,
      structured.recommendedNextAction
        ? `Recommended next action: ${structured.recommendedNextAction}`
        : null,
    ].filter(Boolean),
  });
}

module.exports = {
  CAPABILITY_EXECUTION_MODES,
  DIAGNOSTIC_INTENT_CATEGORIES,
  resolveCapabilityExecutionMode,
  normalizeDiagnoseCanRun,
  buildPreconditionBlockedResult,
};
