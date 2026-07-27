'use strict';

/**
 * Capability Framework types (SPEC-023 / ADR-011).
 * Capabilities are the stable API. Agents are implementation details.
 */

const CAPABILITY_CATEGORIES = Object.freeze({
  DISCOVERY: 'discovery',
  ENRICHMENT: 'enrichment',
  INTELLIGENCE: 'intelligence',
  CAMPAIGN: 'campaign',
  MONITORING: 'monitoring',
  REPORTING: 'reporting',
});

const CAPABILITY_RESULT_STATUS = Object.freeze({
  COMPLETED: 'completed',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
  PARTIAL: 'partial',
});

const PROGRESS_KINDS = Object.freeze({
  QUEUED: 'queued',
  RUNNING: 'running',
  PROGRESS: 'progress',
  COMPLETED: 'completed',
  FAILED: 'failed',
  RETRYING: 'retrying',
  CANCELLED: 'cancelled',
});

/** Built-in capability ids (operator-facing names live on the descriptor). */
const BUILTIN_IDS = Object.freeze({
  PROSPECT_DISCOVERY: 'prospect_discovery',
  COMPANY_ENRICHMENT: 'company_enrichment',
  KNOWLEDGE_UPDATE: 'knowledge_update',
  OPPORTUNITY_RANKING: 'opportunity_ranking',
  CAMPAIGN_BUILDER: 'campaign_builder',
  PROPOSAL_GENERATOR: 'proposal_generator',
  MAIL_PACKAGE_GENERATOR: 'mail_package_generator',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCapabilityResult(partial = {}) {
  return {
    status: partial.status || CAPABILITY_RESULT_STATUS.COMPLETED,
    outputs: partial.outputs && typeof partial.outputs === 'object' ? partial.outputs : {},
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    artifacts: Array.isArray(partial.artifacts) ? partial.artifacts : [],
    duration: Number.isFinite(Number(partial.duration)) ? Number(partial.duration) : 0,
    warnings: Array.isArray(partial.warnings) ? partial.warnings.map(String) : [],
    errors: Array.isArray(partial.errors) ? partial.errors : [],
    nextRecommendations: Array.isArray(partial.nextRecommendations)
      ? partial.nextRecommendations
      : [],
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCapabilityEstimate(partial = {}) {
  return {
    durationMs: Number.isFinite(Number(partial.durationMs))
      ? Number(partial.durationMs)
      : 1000,
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0.8,
    costHint: partial.costHint != null ? String(partial.costHint) : null,
    notes: Array.isArray(partial.notes) ? partial.notes.map(String) : [],
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCapabilityContext(partial = {}) {
  return {
    missionId: String(partial.missionId || ''),
    tenantId: String(partial.tenantId || ''),
    clientId: partial.clientId != null ? partial.clientId : null,
    objective: partial.objective != null ? partial.objective : '',
    constraints:
      partial.constraints && typeof partial.constraints === 'object'
        ? partial.constraints
        : {},
    inputs: partial.inputs && typeof partial.inputs === 'object' ? partial.inputs : {},
    knowledge:
      partial.knowledge && typeof partial.knowledge === 'object' ? partial.knowledge : {},
  };
}

module.exports = {
  CAPABILITY_CATEGORIES,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  buildCapabilityContext,
};
