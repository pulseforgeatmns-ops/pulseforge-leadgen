'use strict';

const { listShadowEvents, reviewOptions } = require('./ShadowEventRepository');
const { countShadowEvidenceSince } = require('./ShadowEvidenceRepository');
const { INSPECTION_THRESHOLD, classifyDecisionMismatch } = require('./mismatchClassifier');
const { likelyMissionInspection } = require('./shadowRoutingWarning');
const { evaluateDeployGates } = require('./deployGates');
const { isDangerousMismatchClassification } = require('./shadowMismatchClassification');

function buildShadowReview(rows, options, evidenceCounts = {}) {
  const scope = reviewOptions(options);
  const mismatches = rows.filter(row => row.comparison === 'mismatch');
  const errors = rows.filter(row => row.status === 'error' || row.errors?.length > 0);
  const likely = rows.filter(row => classifyDecisionMismatch(row) != null);
  const count = (items, key) => items.reduce((result, row) => {
    const label = row[key] ?? 'unknown';
    result[label] = (result[label] || 0) + 1;
    return result;
  }, Object.create(null));

  const comparableRows = rows.filter(row => {
    if (row.route_comparable === false) return false;
    if (row.route_comparable === true) return true;
    return row.comparison === 'match' || row.comparison === 'mismatch';
  });
  const comparable = comparableRows.length;
  const matches = comparableRows.filter(row => row.comparison === 'match').length;
  const mismatchCount = comparableRows.filter(row => row.comparison === 'mismatch').length;

  const mismatchPairs = mismatches.reduce((acc, row) => {
    const prod = row.prod_route || row.current_route?.route || '?';
    const jev = row.recommended_route || '?';
    const key = `${prod}->${jev}`;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, Object.create(null));

  const approvalWhileClarification = rows.filter(row => row.recommended_route === 'approval'
    && (row.prod_route === 'clarification' || row.current_route?.route === 'clarification'));
  const inspectionWhileConversation = rows.filter(row => row.recommended_route === 'inspection'
    && ['conversation', 'intelligence'].includes(row.prod_route || row.current_route?.route));

  const bySource = rows.reduce((acc, row) => {
    const source = row.source || 'unknown';
    acc[source] ||= { total: 0, evaluated: 0, comparable: 0, match: 0, mismatch: 0, unavailable: 0, errors: 0 };
    acc[source].total += 1;
    if (row.status === 'evaluated') acc[source].evaluated += 1;
    if (row.status === 'error') acc[source].errors += 1;
    if (row.comparison === 'match') { acc[source].comparable += 1; acc[source].match += 1; }
    else if (row.comparison === 'mismatch') { acc[source].comparable += 1; acc[source].mismatch += 1; }
    else if (row.comparison === 'unavailable') acc[source].unavailable += 1;
    return acc;
  }, Object.create(null));

  const timeouts = rows.filter(row => row.error_reason === 'timeout'
    || row.errors?.some(error => error.code === 'timeout')).length;
  const latencies = rows.map(row => row.latency_ms).filter(n => Number.isInteger(n) && n >= 0);
  const latency_ms = latencies.length
    ? { min: Math.min(...latencies), max: Math.max(...latencies), avg: Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) }
    : null;

  const inspectionStatusRows = rows.filter(row => row.intent === 'status_check'
    && ['match', 'mismatch'].includes(row.comparison));
  const inspectionStatusMatches = inspectionStatusRows.filter(row => row.comparison === 'match').length;
  const inspectionStatusComparable = inspectionStatusRows.filter(row => ['match', 'mismatch'].includes(row.comparison)).length;

  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const dangerousRecent = rows.filter(row => row.timestamp >= sevenDaysAgo
    && (row.mismatch_classification === 'dangerous_approval_vs_clarification'
      || (row.recommended_route === 'approval'
        && (row.prod_route === 'clarification' || row.current_route?.route === 'clarification'))));

  const dangerousMismatchRows = rows.filter(row => isDangerousMismatchClassification(row.mismatch_classification)
    || (row.recommended_route === 'approval'
      && (row.prod_route === 'clarification' || row.current_route?.route === 'clarification')));

  const summary = {
    total: rows.length,
    evaluated: rows.filter(row => row.status === 'evaluated').length,
    errors: errors.length,
    timeouts,
    comparable,
    unavailable: rows.filter(row => row.comparison === 'unavailable').length,
    matches,
    mismatches: mismatchCount,
    mismatch_rate: comparable ? mismatchCount / comparable : null,
    likely_mission_inspections: likely.length,
    mismatch_pairs: mismatchPairs,
    approval_recommended_while_prod_clarification: approvalWhileClarification.length,
    inspection_recommended_while_prod_conversation: inspectionWhileConversation.length,
    dangerous_mismatch_rows: dangerousMismatchRows.length,
    dangerous_approval_clarification_7d: dangerousRecent.length,
    pending_guard_evidence_rows: evidenceCounts.pending_guard || 0,
    shadow_warning_evidence_rows: evidenceCounts.shadow_warning || 0,
    inspection_status_match_rate: inspectionStatusComparable
      ? inspectionStatusMatches / inspectionStatusComparable
      : null,
    by_status: count(rows, 'status'),
    by_source: bySource,
    by_tenant: count(rows, 'tenant_id'),
    by_intent: count(rows.filter(row => row.status === 'evaluated'), 'intent'),
    mismatch_intents: count(mismatches, 'intent'),
    mismatch_classifications: count(mismatches, 'mismatch_classification'),
    error_codes: count(errors.flatMap(row => row.errors || []), 'code'),
    latency_ms,
    timeout_rate: rows.length ? timeouts / rows.length : null,
    error_rate: rows.length ? errors.length / rows.length : null,
  };

  const report = {
    spec: 'SPEC-JEV-006', mode: 'shadow_review',
    scope: { latest: scope.limit, tenant_id: scope.tenantId, filter: scope.filter,
      summary_basis: 'returned rows only' },
    thresholds: {
      confidence: 0.9,
      inspection_probability: INSPECTION_THRESHOLD,
    },
    summary,
    evaluations: scope.filter === 'warnings' ? likely : rows,
    mismatches,
    likely_mission_inspections: likely,
    operator_warnings: likely,
    errors,
    dangerous_mismatches: dangerousMismatchRows,
    approval_while_clarification: approvalWhileClarification,
    inspection_while_conversation: inspectionWhileConversation,
  };
  report.deploy = evaluateDeployGates(report);
  report.deploy_recommendation = report.deploy.passed && report.deploy.recommendation !== 'shadow_only'
    ? report.deploy.recommendation
    : 'shadow_only';
  return report;
}

async function queryShadowReview(db, options) {
  const rows = await listShadowEvents(db, options);
  let evidenceCounts = {};
  try {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    evidenceCounts = {
      pending_guard: await countShadowEvidenceSince(db, 'PENDING_DECISION_CAPTURE_GUARDED', since),
      shadow_warning: await countShadowEvidenceSince(db, 'DECISION_SHADOW_WARNING', since),
    };
  } catch (_) {
    evidenceCounts = { pending_guard: 0, shadow_warning: 0 };
  }
  return buildShadowReview(rows, options, evidenceCounts);
}

module.exports = { likelyMissionInspection, buildShadowReview, queryShadowReview };
