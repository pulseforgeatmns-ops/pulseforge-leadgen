'use strict';

/**
 * Direct Mail Execution types (SPEC-035 / ADR-022).
 */

const EXECUTION_STATUS = Object.freeze({
  DRAFT: 'draft',
  READY_TO_PRINT: 'ready_to_print',
  PRINTING: 'printing',
  PRINTED: 'printed',
  ASSEMBLING: 'assembling',
  READY_TO_MAIL: 'ready_to_mail',
  MAILED: 'mailed',
  DELIVERED: 'delivered',
  RESPONDED: 'responded',
  COMPLETED: 'completed',
});

/** Ordered primary path (Delivered is optional). */
const EXECUTION_STATUS_ORDER = Object.freeze([
  EXECUTION_STATUS.DRAFT,
  EXECUTION_STATUS.READY_TO_PRINT,
  EXECUTION_STATUS.PRINTING,
  EXECUTION_STATUS.PRINTED,
  EXECUTION_STATUS.ASSEMBLING,
  EXECUTION_STATUS.READY_TO_MAIL,
  EXECUTION_STATUS.MAILED,
  EXECUTION_STATUS.DELIVERED,
  EXECUTION_STATUS.RESPONDED,
  EXECUTION_STATUS.COMPLETED,
]);

const PRINT_SESSION_STATUS = Object.freeze({
  OPEN: 'open',
  COMPLETED: 'completed',
  CANCELLED: 'cancelled',
});

const ASSEMBLY_ACTIONS = Object.freeze(['complete', 'skip', 'reopen']);

const RESPONSE_STATUS = Object.freeze({
  NO_RESPONSE: 'no_response',
  RETURNED_MAIL: 'returned_mail',
  CALLED: 'called',
  EMAILED: 'emailed',
  WALKTHROUGH_SCHEDULED: 'walkthrough_scheduled',
  PROPOSAL_SENT: 'proposal_sent',
  CLOSED_WON: 'closed_won',
  CLOSED_LOST: 'closed_lost',
});

/** Responses that count toward "Responses" / meetings / proposals / wins metrics. */
const RESPONSE_METRIC_KINDS = Object.freeze({
  response: new Set([
    RESPONSE_STATUS.CALLED,
    RESPONSE_STATUS.EMAILED,
    RESPONSE_STATUS.WALKTHROUGH_SCHEDULED,
    RESPONSE_STATUS.PROPOSAL_SENT,
    RESPONSE_STATUS.CLOSED_WON,
    RESPONSE_STATUS.CLOSED_LOST,
  ]),
  meeting: new Set([RESPONSE_STATUS.WALKTHROUGH_SCHEDULED]),
  proposal: new Set([RESPONSE_STATUS.PROPOSAL_SENT, RESPONSE_STATUS.CLOSED_WON]),
  win: new Set([RESPONSE_STATUS.CLOSED_WON]),
});

const OPERATOR_ACTIONS = Object.freeze([
  'start_execution',
  'advance_state',
  'start_print_session',
  'complete_print_session',
  'assembly_complete',
  'assembly_skip',
  'assembly_reopen',
  'mark_selected_mailed',
  'mark_all_mailed',
  'mark_delivered',
  'set_response',
  'complete_campaign',
]);

const EXECUTION_PROGRESS_STAGES = Object.freeze({
  GATHERING: 'Gathering approved artifacts',
  VALIDATING: 'Validating approved revision',
  APPLYING: 'Applying execution actions',
  METRICS: 'Computing campaign metrics',
  COMPLETED: 'Completed',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildAssemblyChecklist(partial = {}) {
  return {
    letterInserted: Boolean(partial.letterInserted),
    envelopeAddressed: Boolean(partial.envelopeAddressed),
    insertsAdded: Boolean(partial.insertsAdded),
    sealed: Boolean(partial.sealed),
    postageApplied: Boolean(partial.postageApplied),
  };
}

/**
 * True when all assembly checklist items are done.
 * @param {object} checklist
 * @returns {boolean}
 */
function isAssemblyComplete(checklist) {
  if (!checklist || typeof checklist !== 'object') return false;
  return (
    checklist.letterInserted === true &&
    checklist.envelopeAddressed === true &&
    checklist.insertsAdded === true &&
    checklist.sealed === true &&
    checklist.postageApplied === true
  );
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildProspectExecution(partial = {}) {
  const assembly = buildAssemblyChecklist(partial.assembly || partial);
  return {
    prospectId: String(partial.prospectId || ''),
    company: String(partial.company || '').trim(),
    recipient: String(partial.recipient || '').trim(),
    address: String(partial.address || '').trim(),
    mailPackageId:
      partial.mailPackageId != null ? String(partial.mailPackageId) : null,
    skipped: Boolean(partial.skipped),
    assembly,
    assemblyComplete:
      partial.assemblyComplete != null
        ? Boolean(partial.assemblyComplete)
        : isAssemblyComplete(assembly),
    printed: Boolean(partial.printed),
    mailed: Boolean(partial.mailed),
    mailedAt: partial.mailedAt || null,
    delivered: Boolean(partial.delivered),
    deliveredAt: partial.deliveredAt || null,
    responseStatus: partial.responseStatus || RESPONSE_STATUS.NO_RESPONSE,
    responseNotes:
      partial.responseNotes != null ? String(partial.responseNotes) : null,
    responseAt: partial.responseAt || null,
    uspsBatchId:
      partial.uspsBatchId != null ? String(partial.uspsBatchId) : null,
    lastModified: partial.lastModified || new Date().toISOString(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildPrintSession(partial = {}) {
  return {
    id: String(partial.id || ''),
    campaignId: partial.campaignId != null ? String(partial.campaignId) : null,
    campaignName: String(partial.campaignName || '').trim(),
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : null,
    operator: partial.operator != null ? String(partial.operator) : 'operator',
    timestamp: partial.timestamp || new Date().toISOString(),
    prospectCount: Number(partial.prospectCount) || 0,
    printStatus: partial.printStatus || PRINT_SESSION_STATUS.OPEN,
    completedAt: partial.completedAt || null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildAuditEntry(partial = {}) {
  return Object.freeze({
    id: String(partial.id || ''),
    previousState: partial.previousState != null ? String(partial.previousState) : null,
    newState: String(partial.newState || ''),
    timestamp: partial.timestamp || new Date().toISOString(),
    operator: partial.operator != null ? String(partial.operator) : 'system',
    notes: String(partial.notes || '').trim(),
    action: partial.action != null ? String(partial.action) : null,
    prospectId: partial.prospectId != null ? String(partial.prospectId) : null,
  });
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCampaignLock(partial = {}) {
  return {
    locked: Boolean(partial.locked),
    lockedAt: partial.lockedAt || null,
    lockedBy: partial.lockedBy != null ? String(partial.lockedBy) : null,
    campaignRevision:
      partial.campaignRevision != null ? Number(partial.campaignRevision) : null,
    mailPackageBatchId:
      partial.mailPackageBatchId != null
        ? String(partial.mailPackageBatchId)
        : null,
    executionPackageId:
      partial.executionPackageId != null
        ? String(partial.executionPackageId)
        : null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildExecutionMetrics(partial = {}) {
  const printed = Number(partial.printed) || 0;
  const assembled = Number(partial.assembled) || 0;
  const mailed = Number(partial.mailed) || 0;
  const responses = Number(partial.responses) || 0;
  const meetings = Number(partial.meetings) || 0;
  const proposals = Number(partial.proposals) || 0;
  const wins = Number(partial.wins) || 0;
  const responseRate =
    mailed > 0 ? Math.round((responses / mailed) * 1000) / 1000 : 0;
  return {
    printed,
    assembled,
    mailed,
    responses,
    meetings,
    proposals,
    wins,
    responseRate:
      partial.responseRate != null ? Number(partial.responseRate) : responseRate,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildExecutionSummary(partial = {}) {
  return {
    campaignName: String(partial.campaignName || 'Campaign').trim(),
    campaignId: partial.campaignId != null ? String(partial.campaignId) : null,
    client: partial.client != null ? String(partial.client) : null,
    status: partial.status || EXECUTION_STATUS.DRAFT,
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : null,
    prospectCount: Number(partial.prospectCount) || 0,
    locked: Boolean(partial.locked),
    metrics: buildExecutionMetrics(partial.metrics || partial),
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

/**
 * Mission Memory event shape (SPEC-032 contract mirror).
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionExecutionEvent(partial = {}) {
  return {
    kind: 'mission_execution_event',
    eventType: String(partial.eventType || 'execution_transition'),
    previousState:
      partial.previousState != null ? String(partial.previousState) : null,
    newState: partial.newState != null ? String(partial.newState) : null,
    operator: partial.operator != null ? String(partial.operator) : 'system',
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    prospectId: partial.prospectId != null ? String(partial.prospectId) : null,
    revision: Number.isFinite(Number(partial.revision))
      ? Number(partial.revision)
      : null,
  };
}

/**
 * Mission timeline entry shape.
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionTimelineEntry(partial = {}) {
  return {
    kind: 'mission_timeline',
    stage: String(partial.stage || ''),
    status: partial.status != null ? String(partial.status) : null,
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    operator: partial.operator != null ? String(partial.operator) : null,
  };
}

module.exports = {
  EXECUTION_STATUS,
  EXECUTION_STATUS_ORDER,
  PRINT_SESSION_STATUS,
  ASSEMBLY_ACTIONS,
  RESPONSE_STATUS,
  RESPONSE_METRIC_KINDS,
  OPERATOR_ACTIONS,
  EXECUTION_PROGRESS_STAGES,
  buildAssemblyChecklist,
  isAssemblyComplete,
  buildProspectExecution,
  buildPrintSession,
  buildAuditEntry,
  buildCampaignLock,
  buildExecutionMetrics,
  buildExecutionSummary,
  buildMissionExecutionEvent,
  buildMissionTimelineEntry,
};
