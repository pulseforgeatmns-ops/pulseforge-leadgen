'use strict';

/**
 * Core graph invariants (SPEC-017).
 * Every domain must use these permanent node categories.
 */

/** @typedef {'observation'|'evidence'|'claim'|'outcome'} CoreNodeCategory */

const CORE_NODE_CATEGORIES = Object.freeze({
  OBSERVATION: 'observation',
  EVIDENCE: 'evidence',
  CLAIM: 'claim',
  OUTCOME: 'outcome',
});

const CORE_NODE_CATEGORY_SET = new Set(Object.values(CORE_NODE_CATEGORIES));

/**
 * Universal graph rules from SPEC-017.
 */
const GRAPH_RULES = Object.freeze({
  OBSERVATIONS_IMMUTABLE: 'observations_are_immutable',
  EVIDENCE_REPRODUCIBLE: 'evidence_is_reproducible',
  CLAIMS_NO_BUSINESS_LOGIC: 'claims_accumulate_evidence_only',
  PROVENANCE_REQUIRED: 'everything_has_provenance',
  EDGES_HAVE_MEANING: 'every_edge_has_meaning',
  REPLAY_FROM_OBSERVATIONS: 'replay_reconstructs_from_observations',
});

/**
 * @param {string} category
 * @returns {category is CoreNodeCategory}
 */
function isCoreNodeCategory(category) {
  return CORE_NODE_CATEGORY_SET.has(category);
}

module.exports = {
  CORE_NODE_CATEGORIES,
  CORE_NODE_CATEGORY_SET,
  GRAPH_RULES,
  isCoreNodeCategory,
};
