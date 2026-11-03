'use strict';
const { parseDecision, ROUTES } = require('./schema');
const HIGH_RISK_ROUTES = new Set(['approval', 'mission', 'specialist']);
function evaluateCases(cases) {
  if (!Array.isArray(cases) || !cases.length) throw new Error('nonempty cases array required');
  const ids = new Set(), perCategory = {}, confusion = {}, examples = [];
  let scored = 0, jevCorrect = 0, legacyScored = 0, legacyCorrect = 0, unsafe = 0, missing = 0;
  for (const row of cases) {
    if (!row || typeof row.id !== 'string' || !/^[a-zA-Z0-9._-]{1,100}$/.test(row.id) || ids.has(row.id)) throw new Error('invalid or duplicate case id');
    ids.add(row.id);
    if (!ROUTES.includes(row.expected_route) || row.expected_route === 'unknown') throw new Error('invalid expected route: ' + row.id);
    if (typeof row.category !== 'string' || !/^[a-z_]{1,48}$/.test(row.category)) throw new Error('invalid category: ' + row.id);
    if (!['synthetic', 'historical_redacted', 'prospective_shadow'].includes(row.provenance)) throw new Error('invalid provenance: ' + row.id);
    if (!['human_reviewed', 'unreviewed'].includes(row.label_status)) throw new Error('missing label status: ' + row.id);
    if (row.legacy_route != null && !ROUTES.includes(row.legacy_route)) throw new Error('invalid legacy route: ' + row.id);
    const bucket = perCategory[row.category] ||= { total: 0, scored: 0, jev_correct: 0, legacy_scored: 0, legacy_correct: 0, dangerous: 0 };
    bucket.total++;
    const decision = row.jev_decision == null ? null : parseDecision(row.jev_decision);
    if (!decision) missing++;
    const reviewed = row.label_status === 'human_reviewed';
    const predicted = decision?.recommended_route || null;
    if (decision && reviewed) {
      scored++; bucket.scored++;
      confusion[row.expected_route] ||= {};
      confusion[row.expected_route][predicted] = (confusion[row.expected_route][predicted] || 0) + 1;
      if (predicted === row.expected_route) { jevCorrect++; bucket.jev_correct++; }
      if (HIGH_RISK_ROUTES.has(predicted) && predicted !== row.expected_route) { unsafe++; bucket.dangerous++; }
    }
    if (reviewed && row.legacy_route && row.legacy_route !== 'unknown') {
      legacyScored++; bucket.legacy_scored++;
      if (row.legacy_route === row.expected_route) { legacyCorrect++; bucket.legacy_correct++; }
    }
    examples.push({ id: row.id, category: row.category, provenance: row.provenance, reviewed, expected: row.expected_route, jev: predicted, legacy: row.legacy_route || null, correct: reviewed && decision ? predicted === row.expected_route : null });
  }
  return { kind: 'offline_diagnostic_only', production_acceptance_eligible: false, activation_authorized: false,
    count: cases.length, reviewed_prediction_count: scored, unevaluated_predictions: missing,
    jev_correct: jevCorrect, jev_accuracy: scored ? jevCorrect / scored : null,
    legacy_reviewed_count: legacyScored, legacy_correct: legacyCorrect,
    legacy_accuracy: legacyScored ? legacyCorrect / legacyScored : null,
    dangerous_misroutes: unsafe, per_category: perCategory, confusion, cases: examples };
}
module.exports = { evaluateCases };
