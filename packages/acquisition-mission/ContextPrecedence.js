'use strict';

/**
 * SPEC-130 — Context precedence for Mission Planning.
 * Blueprint informs. Mission decides. Nothing may override an approved Mission Plan.
 */

const CONTEXT_PRECEDENCE = Object.freeze([
  'operator_approval',
  'operator',
  'mission_plan',
  'workspace',
  'blueprint',
  'historical_memory',
  'general_knowledge',
]);

function sourceRank(source) {
  const index = CONTEXT_PRECEDENCE.indexOf(source);
  return index < 0 ? CONTEXT_PRECEDENCE.length : index;
}

/**
 * Higher-precedence sources may fill or replace lower-precedence values.
 * Approved mission plans and explicit operator fields never lose to Blueprint.
 */
function mayAssign(existingSource, incomingSource) {
  if (!existingSource) return true;
  return sourceRank(incomingSource) <= sourceRank(existingSource);
}

function pickByPrecedence(candidates = []) {
  const ranked = candidates
    .filter((row) => row && row.value != null && row.value !== '')
    .slice()
    .sort((a, b) => sourceRank(a.source) - sourceRank(b.source));
  return ranked[0] || null;
}

module.exports = {
  CONTEXT_PRECEDENCE,
  sourceRank,
  mayAssign,
  pickByPrecedence,
};
