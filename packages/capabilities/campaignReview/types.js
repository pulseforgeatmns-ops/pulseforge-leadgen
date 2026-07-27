'use strict';

/**
 * Campaign Review Workspace types (SPEC-034 / ADR-021).
 */

const CAMPAIGN_REVIEW_STATUS = Object.freeze({
  IN_REVIEW: 'in_review',
  READY_TO_PRINT: 'ready_to_print',
  REJECTED: 'rejected',
  BLOCKED: 'blocked',
});

const PROSPECT_REVIEW_STATUS = Object.freeze({
  NEEDS_REVIEW: 'needs_review',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  SKIPPED: 'skipped',
  BLOCKED: 'blocked',
});

const REVIEW_SORT = Object.freeze({
  SCORE: 'score',
  CONFIDENCE: 'confidence',
  NEEDS_REVIEW: 'needs_review',
  ALPHABETICAL: 'alphabetical',
});

const PROSPECT_ACTIONS = Object.freeze([
  'approve',
  'reject',
  'skip',
  'edit_letter',
  'regenerate',
  'replace_recipient',
  'update_address',
  'add_note',
]);

const BULK_ACTIONS = Object.freeze([
  'approve_selected',
  'reject_selected',
  'regenerate_selected',
  'export_selected',
  'print_selected',
]);

const CAMPAIGN_ACTIONS = Object.freeze([
  'approve_campaign',
  'restore_revision',
  'duplicate_revision',
  'compare_revisions',
]);

const OPERATOR_ACTIONS = Object.freeze([
  ...PROSPECT_ACTIONS,
  ...BULK_ACTIONS,
  ...CAMPAIGN_ACTIONS,
]);

const REVIEW_PROGRESS_STAGES = Object.freeze({
  GATHERING: 'Gathering campaign artifacts',
  ASSEMBLING: 'Assembling review workspace',
  APPLYING: 'Applying review actions',
  VALIDATING: 'Validating approval gates',
  COMPLETED: 'Completed',
});

/** Default personalization confidence threshold for approval. */
const DEFAULT_CONFIDENCE_THRESHOLD = 0.65;

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCampaignReviewSummary(partial = {}) {
  return {
    campaignName: String(partial.campaignName || 'Campaign').trim(),
    client: partial.client != null ? String(partial.client) : null,
    discoveryProfile:
      partial.discoveryProfile != null ? String(partial.discoveryProfile) : null,
    generatedAt: partial.generatedAt || new Date().toISOString(),
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : 1,
    prospectCount: Number(partial.prospectCount) || 0,
    readyCount: Number(partial.readyCount) || 0,
    needsReviewCount: Number(partial.needsReviewCount) || 0,
    blockedCount: Number(partial.blockedCount) || 0,
    status: partial.status || CAMPAIGN_REVIEW_STATUS.IN_REVIEW,
    mailPackageGenerated: Boolean(partial.mailPackageGenerated),
    activeRevision:
      partial.activeRevision != null
        ? Number(partial.activeRevision)
        : Number(partial.revision) || 1,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProspectQueueRow(partial = {}) {
  return {
    prospectId: String(partial.prospectId || ''),
    status: partial.status || PROSPECT_REVIEW_STATUS.NEEDS_REVIEW,
    company: String(partial.company || '').trim(),
    recipient: String(partial.recipient || '').trim(),
    score: Number.isFinite(Number(partial.score)) ? Number(partial.score) : 0,
    confidence: Number.isFinite(Number(partial.confidence))
      ? Number(partial.confidence)
      : 0,
    personalization: Array.isArray(partial.personalization)
      ? partial.personalization.map(String)
      : [],
    letterPreview: String(partial.letterPreview || '').trim(),
    address: String(partial.address || '').trim(),
    lastModified: partial.lastModified || new Date().toISOString(),
    validationErrors: Array.isArray(partial.validationErrors)
      ? partial.validationErrors.map(String)
      : [],
    operatorNote:
      partial.operatorNote != null ? String(partial.operatorNote) : null,
    insertChecklist: Array.isArray(partial.insertChecklist)
      ? partial.insertChecklist
      : [],
    letter: partial.letter && typeof partial.letter === 'object' ? partial.letter : null,
    envelope:
      partial.envelope && typeof partial.envelope === 'object'
        ? partial.envelope
        : null,
    companyIntelligence:
      partial.companyIntelligence && typeof partial.companyIntelligence === 'object'
        ? partial.companyIntelligence
        : null,
    opportunityBrief:
      partial.opportunityBrief && typeof partial.opportunityBrief === 'object'
        ? partial.opportunityBrief
        : null,
    signals: Array.isArray(partial.signals) ? partial.signals : [],
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    companySummary:
      partial.companySummary != null ? String(partial.companySummary) : null,
    mailPackageId:
      partial.mailPackageId != null ? String(partial.mailPackageId) : null,
    skipped: Boolean(partial.skipped),
    required: partial.required !== false && !partial.skipped,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildRevisionRecord(partial = {}) {
  return {
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : 1,
    timestamp: partial.timestamp || new Date().toISOString(),
    operator: partial.operator != null ? String(partial.operator) : 'system',
    changeSummary: String(partial.changeSummary || '').trim(),
    actions: Array.isArray(partial.actions) ? partial.actions : [],
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionDecision(partial = {}) {
  return {
    kind: 'mission_decision',
    decisionType: partial.decisionType || 'campaign_review',
    action: String(partial.action || ''),
    prospectId: partial.prospectId != null ? String(partial.prospectId) : null,
    operator: partial.operator != null ? String(partial.operator) : 'system',
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionRevision(partial = {}) {
  return {
    kind: 'mission_revision',
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : 1,
    reason: String(partial.reason || 'campaign_review_change').trim(),
    operator: partial.operator != null ? String(partial.operator) : 'system',
    timestamp: partial.timestamp || new Date().toISOString(),
    changeSummary: String(partial.changeSummary || '').trim(),
  };
}

module.exports = {
  CAMPAIGN_REVIEW_STATUS,
  PROSPECT_REVIEW_STATUS,
  REVIEW_SORT,
  PROSPECT_ACTIONS,
  BULK_ACTIONS,
  CAMPAIGN_ACTIONS,
  OPERATOR_ACTIONS,
  REVIEW_PROGRESS_STAGES,
  DEFAULT_CONFIDENCE_THRESHOLD,
  buildCampaignReviewSummary,
  buildProspectQueueRow,
  buildRevisionRecord,
  buildMissionDecision,
  buildMissionRevision,
};
