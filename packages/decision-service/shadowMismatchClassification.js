'use strict';

const { classifyDecisionMismatch } = require('./mismatchClassifier');

function prodRoute(row = {}) {
  return row.prod_route || row.current_route?.route || null;
}

function classifyShadowMismatch(row = {}) {
  if (!row || row.status !== 'evaluated') return null;
  if (row.comparison === 'match' || row.route_matches === true) return 'match';
  if (row.comparison !== 'mismatch' && row.route_matches !== false) return null;

  const prod = prodRoute(row);
  const jev = row.recommended_route;
  if (!prod || !jev) return null;

  if (jev === 'approval' && prod === 'clarification') {
    return 'dangerous_approval_vs_clarification';
  }
  if (jev === 'approval' && prod === 'mission') {
    return 'dangerous_approval_vs_pending_mission';
  }
  if (classifyDecisionMismatch(row)) {
    return 'likely_mission_inspection_misroute';
  }
  const execRoutes = new Set(['mission', 'approval', 'specialist']);
  const readRoutes = new Set(['inspection', 'conversation', 'intelligence', 'clarification']);
  if (execRoutes.has(jev) && readRoutes.has(prod)) return 'safe_read_only_difference';
  if (readRoutes.has(jev) && execRoutes.has(prod)) return 'dangerous_execution_vs_read_only';
  return 'noisy_framing_difference';
}

function isDangerousMismatchClassification(classification) {
  return typeof classification === 'string' && classification.startsWith('dangerous_');
}

module.exports = {
  classifyShadowMismatch,
  isDangerousMismatchClassification,
};
