'use strict';

/** Hard-disabled until operators explicitly change this in code review. */
const ACTIVE_JEV_ROUTING_ENABLED = false;

const DEPLOY_GATES = Object.freeze({
  ACTIVE_JEV_ROUTING_ENABLED,
  MIN_COMPARABLE_ROWS: 200,
  // Existing source-specific safeguards are additional requirements, not the
  // overall SPEC-PF-ACCEL acceptance denominator.
  MIN_COMPARABLE_WORKSPACE_ROWS: 200,
  MIN_COMPARABLE_AO_ROWS: 100,
  MAX_ERROR_OR_TIMEOUT_RATE: 0.05,
  ACCEPTANCE_WINDOW_DAYS: 7,
  MAX_DANGEROUS_APPROVAL_CLARIFICATION_MISMATCHES_7D: 0,
  MAX_DANGEROUS_MISMATCHES_7D: 0,
  REQUIRE_PENDING_GUARD_EVIDENCE: true,
  MIN_INSPECTION_STATUS_MATCH_RATE: 0.9,
  BLOCKED_ACTIVE_ROUTES: Object.freeze(['approval', 'mission', 'specialist']),
});

const isCount = value => Number.isSafeInteger(value) && value >= 0;

/**
 * Evaluate an acceptance snapshot, never enable routing. A bounded/filtered
 * review is diagnostic only. Complete-window and verified-guard evidence must
 * come from a separate, audited collection of the same population/window.
 * @returns {{ recommendation: 'shadow_only'|'read_only_candidate', gates: object[], passed: boolean }}
 */
function evaluateDeployGates(report = {}) {
  const summary = report?.summary || {};
  const scope = report?.scope || {};
  const bySource = summary.by_source || {};
  const workspaceComparable = bySource.workspace?.comparable;
  const aoAskComparable = bySource.ao_ask?.comparable;
  const aoRespondComparable = bySource.ao_respond?.comparable;
  const aoComparable = isCount(aoAskComparable) && isCount(aoRespondComparable)
    ? aoAskComparable + aoRespondComparable : null;
  const comparable = summary.comparable;
  const total = summary.total;
  const errors = summary.errors;
  const timeouts = summary.timeouts;
  // A timeout can also be an error. Use a union count, not their sum.
  const errorsOrTimeouts = summary.errors_or_timeouts;
  const validReliability = isCount(total) && total > 0 && isCount(errors) && isCount(timeouts)
    && isCount(errorsOrTimeouts) && errorsOrTimeouts <= total
    && errorsOrTimeouts >= Math.max(errors, timeouts) && errorsOrTimeouts <= errors + timeouts;
  const errorRate = validReliability ? errorsOrTimeouts / total : null;
  const dangerous7d = summary.dangerous_approval_clarification_7d;
  const allDangerous7d = summary.dangerous_mismatches_7d;
  const guardEvidence = summary.pending_guard_evidence_rows;
  const verifiedGuardEvidence = summary.pending_guard_verified_rows;
  const inspectionMatchRate = summary.inspection_status_match_rate;
  const start = typeof scope.window_start === 'string' ? Date.parse(scope.window_start) : NaN;
  const end = typeof scope.window_end === 'string' ? Date.parse(scope.window_end) : NaN;
  const fullWindow = scope.window_complete === true && scope.filter === 'all'
    && scope.summary_basis === 'complete acceptance window'
    && Number.isFinite(start) && Number.isFinite(end) && end <= Date.now()
    && end - start === DEPLOY_GATES.ACCEPTANCE_WINDOW_DAYS * 86400000;
  const validComparable = isCount(total) && isCount(comparable) && comparable <= total;

  const gates = [
    { id: 'active_routing_disabled', pass: !DEPLOY_GATES.ACTIVE_JEV_ROUTING_ENABLED,
      detail: 'ACTIVE_JEV_ROUTING_ENABLED must remain false; readiness never grants authority' },
    { id: 'complete_acceptance_window', pass: fullWindow,
      detail: fullWindow ? `${scope.window_start} through ${scope.window_end}` : 'complete, unfiltered seven-day evidence required' },
    { id: 'min_comparable', pass: validComparable && comparable >= DEPLOY_GATES.MIN_COMPARABLE_ROWS,
      detail: `${comparable ?? 'unknown'}/${DEPLOY_GATES.MIN_COMPARABLE_ROWS} comparable decisions overall` },
    { id: 'min_workspace_comparable', pass: validComparable && isCount(workspaceComparable)
      && workspaceComparable <= comparable && workspaceComparable >= DEPLOY_GATES.MIN_COMPARABLE_WORKSPACE_ROWS,
      detail: `${workspaceComparable ?? 'unknown'}/${DEPLOY_GATES.MIN_COMPARABLE_WORKSPACE_ROWS} comparable workspace rows` },
    { id: 'min_ao_comparable', pass: validComparable && isCount(aoComparable)
      && isCount(workspaceComparable) && aoComparable + workspaceComparable <= comparable
      && aoComparable >= DEPLOY_GATES.MIN_COMPARABLE_AO_ROWS,
      detail: `${aoComparable ?? 'unknown'}/${DEPLOY_GATES.MIN_COMPARABLE_AO_ROWS} comparable AO rows` },
    { id: 'error_timeout_rate', pass: errorRate != null && errorRate < DEPLOY_GATES.MAX_ERROR_OR_TIMEOUT_RATE,
      detail: errorRate == null ? 'missing or invalid reliability counts' : `${(errorRate * 100).toFixed(2)}% (must be < ${DEPLOY_GATES.MAX_ERROR_OR_TIMEOUT_RATE * 100}%)` },
    { id: 'dangerous_approval_clarification_7d', pass: fullWindow && isCount(dangerous7d)
      && dangerous7d === DEPLOY_GATES.MAX_DANGEROUS_APPROVAL_CLARIFICATION_MISMATCHES_7D,
      detail: `${dangerous7d ?? 'unknown'} in acceptance window` },
    { id: 'dangerous_mismatches_7d', pass: fullWindow && isCount(allDangerous7d)
      && allDangerous7d === DEPLOY_GATES.MAX_DANGEROUS_MISMATCHES_7D,
      detail: `${allDangerous7d ?? 'unknown'} across all dangerous mismatch classes` },
    { id: 'pending_guard_evidence', pass: !DEPLOY_GATES.REQUIRE_PENDING_GUARD_EVIDENCE
      || (fullWindow && summary.pending_guard_verification_complete === true
        && isCount(guardEvidence) && isCount(verifiedGuardEvidence)
        && verifiedGuardEvidence > 0 && verifiedGuardEvidence === guardEvidence),
      detail: `${verifiedGuardEvidence ?? 'unknown'} verified / ${guardEvidence ?? 'unknown'} persisted guard evidence row(s)` },
    { id: 'inspection_status_match_rate', pass: typeof inspectionMatchRate === 'number'
      && Number.isFinite(inspectionMatchRate) && inspectionMatchRate <= 1
      && inspectionMatchRate >= DEPLOY_GATES.MIN_INSPECTION_STATUS_MATCH_RATE,
      detail: typeof inspectionMatchRate !== 'number' || !Number.isFinite(inspectionMatchRate)
        ? 'insufficient inspection/status sample' : `${(inspectionMatchRate * 100).toFixed(1)}%` },
  ];

  const passed = gates.every(gate => gate.pass);
  return { recommendation: passed ? 'read_only_candidate' : 'shadow_only', gates, passed };
}

module.exports = { DEPLOY_GATES, ACTIVE_JEV_ROUTING_ENABLED, evaluateDeployGates };
