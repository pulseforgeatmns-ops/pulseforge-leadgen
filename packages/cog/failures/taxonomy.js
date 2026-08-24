'use strict';

/**
 * COG Failure Taxonomy — extensible classification of reasoning failures.
 * Failures are classified, not simply marked "failed."
 */

/** @type {Record<string, {code: string, label: string, description: string, domainHints?: string[]}>} */
const BUILTIN_FAILURE_TYPES = Object.freeze({
  'R-001': {
    code: 'R-001',
    label: 'Retrieval Loop',
    description: 'Max repeatedly retrieves or delegates without advancing reasoning — stuck in a fetch cycle instead of synthesizing.',
    domainHints: ['COG-101', 'COG-102'],
  },
  'R-002': {
    code: 'R-002',
    label: 'Proposition Drift',
    description: 'A stated proposition changes across turns without explicit revision — the answer shifts but Max does not acknowledge the change.',
    domainHints: ['COG-102', 'COG-103', 'COG-109'],
  },
  'R-003': {
    code: 'R-003',
    label: 'Assumption Blindness',
    description: 'Max proceeds without surfacing implicit assumptions, or treats assumptions as facts.',
    domainHints: ['COG-103', 'COG-106', 'COG-108'],
  },
  'R-004': {
    code: 'R-004',
    label: 'Counterfactual Collapse',
    description: 'Max cannot maintain or reason about alternative scenarios — collapses to a single path or refuses to explore what-if.',
    domainHints: ['COG-104', 'COG-105'],
  },
  'R-005': {
    code: 'R-005',
    label: 'Conversation Reset',
    description: 'Max loses conversational context — responds as if prior turns did not occur.',
    domainHints: ['COG-102', 'COG-109'],
  },
  'R-006': {
    code: 'R-006',
    label: 'Identity Drift',
    description: 'Max loses or contradicts its operational identity, role boundaries, or tenant scope.',
    domainHints: ['COG-101'],
  },
  'R-007': {
    code: 'R-007',
    label: 'Confidence Mismatch',
    description: 'Stated confidence does not match available evidence — overconfident or underconfident relative to knowns/unknowns.',
    domainHints: ['COG-108', 'COG-106'],
  },
});

const FAILURE_TYPES = { ...BUILTIN_FAILURE_TYPES };

function listFailureTypes() {
  return Object.values(FAILURE_TYPES).map(f => ({ ...f }));
}

function getFailureType(code) {
  const found = FAILURE_TYPES[code];
  return found ? { ...found } : null;
}

function isKnownFailureCode(code) {
  return Boolean(FAILURE_TYPES[code]);
}

function registerFailureType(definition) {
  if (!definition?.code || !definition?.label) {
    throw new Error('Failure type requires code and label');
  }
  if (FAILURE_TYPES[definition.code]) {
    throw new Error(`Failure type ${definition.code} already registered`);
  }
  FAILURE_TYPES[definition.code] = Object.freeze({
    code: definition.code,
    label: definition.label,
    description: definition.description || '',
    domainHints: definition.domainHints || [],
  });
  return { ...FAILURE_TYPES[definition.code] };
}

/**
 * Build a classified failure record from taxonomy + runtime evidence.
 * @param {string} code
 * @param {object} [evidence]
 * @returns {import('../types').FailureClassification}
 */
function classifyFailure(code, evidence = {}) {
  const type = getFailureType(code);
  if (!type) {
    return {
      code,
      label: evidence.label || 'Unknown failure',
      description: evidence.description || '',
      turnIndex: evidence.turnIndex,
      behaviorId: evidence.behaviorId,
      evidence: evidence.evidence,
      requiresHumanReview: evidence.requiresHumanReview ?? true,
    };
  }
  return {
    code: type.code,
    label: type.label,
    description: evidence.description || type.description,
    turnIndex: evidence.turnIndex,
    behaviorId: evidence.behaviorId,
    evidence: evidence.evidence,
    requiresHumanReview: evidence.requiresHumanReview ?? false,
  };
}

module.exports = {
  BUILTIN_FAILURE_TYPES,
  FAILURE_TYPES,
  listFailureTypes,
  getFailureType,
  isKnownFailureCode,
  registerFailureType,
  classifyFailure,
};
