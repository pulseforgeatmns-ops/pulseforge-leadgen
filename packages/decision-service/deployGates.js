'use strict';

/** Hard-disabled until operators explicitly change this in code review. */
const ACTIVE_JEV_ROUTING_ENABLED = false;

const DEPLOY_GATES = Object.freeze({
  ACTIVE_JEV_ROUTING_ENABLED,
  MIN_COMPARABLE_WORKSPACE_ROWS: 200,
  MIN_COMPARABLE_AO_ROWS: 100,
  MAX_ERROR_OR_TIMEOUT_RATE: 0.05,
  MAX_DANGEROUS_APPROVAL_CLARIFICATION_MISMATCHES_7D: 0,
  REQUIRE_PENDING_GUARD_EVIDENCE: true,
  MIN_INSPECTION_STATUS_MATCH_RATE: 0.9,
  BLOCKED_ACTIVE_ROUTES: Object.freeze(['approval', 'mission', 'specialist']),
});

function rate(numerator, denominator) {
  if (!denominator) return null;
  return numerator / denominator;
}

/**
 * Evaluate deploy readiness from an aggregated review report (read-only analytics).
 * @param {object} report
 * @returns {{ recommendation: 'shadow_only'|'read_only_candidate'|'broad_candidate', gates: object[], passed: boolean }}
 */
function evaluateDeployGates(report = {}) {
  const summary = report.summary || {};
  const bySource = summary.by_source || {};
  const workspaceComparable = bySource.workspace?.comparable || 0;
  const aoComparable = (bySource.ao_ask?.comparable || 0) + (bySource.ao_respond?.comparable || 0);
  const total = summary.total || 0;
  const errors = summary.errors || 0;
  const timeouts = summary.timeouts || 0;
  const errorRate = rate(errors + timeouts, total);
  const dangerous7d = summary.dangerous_approval_clarification_7d || 0;
  const guardEvidence = summary.pending_guard_evidence_rows || 0;
  const inspectionMatchRate = summary.inspection_status_match_rate ?? null;

  const gates = [
    { id: 'active_routing_disabled', pass: !DEPLOY_GATES.ACTIVE_JEV_ROUTING_ENABLED,
      detail: 'ACTIVE_JEV_ROUTING_ENABLED must remain false for shadow-only' },
    { id: 'min_workspace_comparable', pass: workspaceComparable >= DEPLOY_GATES.MIN_COMPARABLE_WORKSPACE_ROWS,
      detail: `${workspaceComparable}/${DEPLOY_GATES.MIN_COMPARABLE_WORKSPACE_ROWS} comparable workspace rows` },
    { id: 'min_ao_comparable', pass: aoComparable >= DEPLOY_GATES.MIN_COMPARABLE_AO_ROWS,
      detail: `${aoComparable}/${DEPLOY_GATES.MIN_COMPARABLE_AO_ROWS} comparable AO rows` },
    { id: 'error_timeout_rate', pass: errorRate == null || errorRate <= DEPLOY_GATES.MAX_ERROR_OR_TIMEOUT_RATE,
      detail: errorRate == null ? 'no rows' : `${(errorRate * 100).toFixed(1)}% (max ${DEPLOY_GATES.MAX_ERROR_OR_TIMEOUT_RATE * 100}%)` },
    { id: 'dangerous_approval_clarification_7d', pass: dangerous7d <= DEPLOY_GATES.MAX_DANGEROUS_APPROVAL_CLARIFICATION_MISMATCHES_7D,
      detail: `${dangerous7d} in last 7 days` },
    { id: 'pending_guard_evidence', pass: !DEPLOY_GATES.REQUIRE_PENDING_GUARD_EVIDENCE || guardEvidence > 0,
      detail: `${guardEvidence} persisted guard evidence row(s)` },
    { id: 'inspection_status_match_rate', pass: inspectionMatchRate == null || inspectionMatchRate >= DEPLOY_GATES.MIN_INSPECTION_STATUS_MATCH_RATE,
      detail: inspectionMatchRate == null ? 'insufficient inspection/status sample' : `${(inspectionMatchRate * 100).toFixed(1)}%` },
  ];

  const passed = gates.every(gate => gate.pass);
  let recommendation = 'shadow_only';
  if (passed && !DEPLOY_GATES.ACTIVE_JEV_ROUTING_ENABLED) {
    recommendation = 'read_only_candidate';
  }
  if (passed && DEPLOY_GATES.ACTIVE_JEV_ROUTING_ENABLED) {
    recommendation = 'broad_candidate';
  }

  return { recommendation, gates, passed };
}

module.exports = { DEPLOY_GATES, ACTIVE_JEV_ROUTING_ENABLED, evaluateDeployGates };
