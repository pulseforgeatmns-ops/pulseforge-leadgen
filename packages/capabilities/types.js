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
  EXECUTION: 'execution',
  MONITORING: 'monitoring',
  REPORTING: 'reporting',
  DIAGNOSTICS: 'diagnostics',
});

const CAPABILITY_RESULT_STATUS = Object.freeze({
  COMPLETED: 'completed',
  FAILED: 'failed',
  /** SPEC-058 — diagnostic mode blocked precondition (not opaque failure) */
  BLOCKED: 'blocked',
  CANCELLED: 'cancelled',
  PARTIAL: 'partial',
});

/** SPEC-058 / ADR-042 — production vs diagnostic capability invocation */
const CAPABILITY_EXECUTION_MODES = Object.freeze({
  EXECUTION: 'execution',
  DIAGNOSTIC: 'diagnostic',
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
  /** SPEC-060 — provider-agnostic CandidateSet → ProspectList */
  PROSPECT_ACQUISITION: 'prospect_acquisition',
  COMPANY_ENRICHMENT: 'company_enrichment',
  KNOWLEDGE_UPDATE: 'knowledge_update',
  OPPORTUNITY_RANKING: 'opportunity_ranking',
  BUSINESS_INTELLIGENCE: 'business_intelligence',
  SALES_INTELLIGENCE: 'sales_intelligence',
  CAMPAIGN_BUILDER: 'campaign_builder',
  PROPOSAL_GENERATOR: 'proposal_generator',
  MAIL_PACKAGE_GENERATOR: 'mail_package_generator',
  CAMPAIGN_REVIEW: 'campaign_review',
  DIRECT_MAIL_EXECUTION: 'direct_mail_execution',
  OUTCOME_INTELLIGENCE: 'outcome_intelligence',
  OPERATOR_INBOX: 'operator_inbox',
  /** SPEC-056 — read-only diagnostic producer (never mutates business state) */
  DISCOVERY_DIAGNOSTICS: 'discovery_diagnostics',
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
    /** SPEC-058 — execution | diagnostic */
    executionMode:
      partial.executionMode === CAPABILITY_EXECUTION_MODES.DIAGNOSTIC ||
      partial.executionMode === 'diagnostics'
        ? CAPABILITY_EXECUTION_MODES.DIAGNOSTIC
        : partial.executionMode === CAPABILITY_EXECUTION_MODES.EXECUTION
          ? CAPABILITY_EXECUTION_MODES.EXECUTION
          : partial.executionMode || null,
    mode: partial.mode != null ? partial.mode : null,
    missionIntent:
      partial.missionIntent && typeof partial.missionIntent === 'object'
        ? partial.missionIntent
        : null,
    missionPlan:
      partial.missionPlan && typeof partial.missionPlan === 'object'
        ? partial.missionPlan
        : null,
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
  CAPABILITY_EXECUTION_MODES,
  PROGRESS_KINDS,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  buildCapabilityContext,
};
