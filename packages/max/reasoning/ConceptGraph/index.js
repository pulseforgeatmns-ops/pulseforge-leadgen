'use strict';

/**
 * SPEC-152 — Concept Graph Reasoning public API.
 */

const { ConceptGraph, RELATION_TYPES, CONCEPT_CATEGORIES, normalizeId } = require('./ConceptGraph');
const {
  buildOperatingConceptGraph,
  getOperatingConceptGraph,
  resetOperatingConceptGraphCache,
} = require('./seedFromOperatingModel');
const {
  REASONING_GOALS,
  planConceptQuery,
  shouldUseConceptGraphReasoning,
  parseResolvedQuestion,
  classifyGoal,
  mergeConcepts,
  extractSpecialists,
} = require('./ConceptPlanner');
const { reasonFromPlan, reasoningMetadata, describeEdge, joinSentences } = require('./ConceptReasoner');

function composeConceptGraphAnswer(input = {}, graph = null) {
  const activeGraph = graph || getOperatingConceptGraph();
  const plan = planConceptQuery(input);
  if (!plan) return null;
  const result = reasonFromPlan(plan, activeGraph);
  if (!result) return null;
  return {
    plan,
    result,
    metadata: reasoningMetadata(plan, result),
  };
}

module.exports = {
  ConceptGraph,
  RELATION_TYPES,
  CONCEPT_CATEGORIES,
  normalizeId,
  buildOperatingConceptGraph,
  getOperatingConceptGraph,
  resetOperatingConceptGraphCache,
  REASONING_GOALS,
  planConceptQuery,
  shouldUseConceptGraphReasoning,
  parseResolvedQuestion,
  classifyGoal,
  mergeConcepts,
  extractSpecialists,
  reasonFromPlan,
  reasoningMetadata,
  describeEdge,
  joinSentences,
  composeConceptGraphAnswer,
};
