'use strict';

/**
 * Operator Inbox types (SPEC-037 / ADR-024).
 * Coordination layer — does not perform business workflows.
 */

const INBOX_CATEGORIES = Object.freeze({
  APPROVAL_REQUIRED: 'approval_required',
  REVIEW_REQUIRED: 'review_required',
  ACTION_REQUIRED: 'action_required',
  DECISION_REQUIRED: 'decision_required',
  COMPLETED: 'completed',
});

const INBOX_KINDS = Object.freeze({
  // Approval Required
  CAMPAIGN_APPROVAL: 'campaign_approval',
  PROPOSAL_APPROVAL: 'proposal_approval',
  MAIL_PACKAGE_APPROVAL: 'mail_package_approval',
  // Review Required
  LOW_CONFIDENCE_INTELLIGENCE: 'low_confidence_intelligence',
  MISSING_RECIPIENT: 'missing_recipient',
  MISSING_ADDRESS: 'missing_address',
  VALIDATION_ISSUES: 'validation_issues',
  // Action Required
  PRINT_CAMPAIGN: 'print_campaign',
  ASSEMBLE_MAIL: 'assemble_mail',
  MAIL_CAMPAIGN: 'mail_campaign',
  CALL_PROSPECT: 'call_prospect',
  SEND_PROPOSAL: 'send_proposal',
  FOLLOW_UP: 'follow_up',
  // Decision Required
  APPLY_RECOMMENDATION: 'apply_recommendation',
  UPDATE_CLIENT_PLAYBOOK: 'update_client_playbook',
  APPLY_RANKING_CHANGES: 'apply_ranking_changes',
  RESOLVE_DUPLICATE_COMPANIES: 'resolve_duplicate_companies',
  // Completed
  CAMPAIGN_COMPLETED: 'campaign_completed',
  OUTCOME_SUMMARY_AVAILABLE: 'outcome_summary_available',
  WORKFLOW_COMPLETED: 'workflow_completed',
});

const KIND_CATEGORY = Object.freeze({
  [INBOX_KINDS.CAMPAIGN_APPROVAL]: INBOX_CATEGORIES.APPROVAL_REQUIRED,
  [INBOX_KINDS.PROPOSAL_APPROVAL]: INBOX_CATEGORIES.APPROVAL_REQUIRED,
  [INBOX_KINDS.MAIL_PACKAGE_APPROVAL]: INBOX_CATEGORIES.APPROVAL_REQUIRED,
  [INBOX_KINDS.LOW_CONFIDENCE_INTELLIGENCE]: INBOX_CATEGORIES.REVIEW_REQUIRED,
  [INBOX_KINDS.MISSING_RECIPIENT]: INBOX_CATEGORIES.REVIEW_REQUIRED,
  [INBOX_KINDS.MISSING_ADDRESS]: INBOX_CATEGORIES.REVIEW_REQUIRED,
  [INBOX_KINDS.VALIDATION_ISSUES]: INBOX_CATEGORIES.REVIEW_REQUIRED,
  [INBOX_KINDS.PRINT_CAMPAIGN]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.ASSEMBLE_MAIL]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.MAIL_CAMPAIGN]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.CALL_PROSPECT]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.SEND_PROPOSAL]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.FOLLOW_UP]: INBOX_CATEGORIES.ACTION_REQUIRED,
  [INBOX_KINDS.APPLY_RECOMMENDATION]: INBOX_CATEGORIES.DECISION_REQUIRED,
  [INBOX_KINDS.UPDATE_CLIENT_PLAYBOOK]: INBOX_CATEGORIES.DECISION_REQUIRED,
  [INBOX_KINDS.APPLY_RANKING_CHANGES]: INBOX_CATEGORIES.DECISION_REQUIRED,
  [INBOX_KINDS.RESOLVE_DUPLICATE_COMPANIES]: INBOX_CATEGORIES.DECISION_REQUIRED,
  [INBOX_KINDS.CAMPAIGN_COMPLETED]: INBOX_CATEGORIES.COMPLETED,
  [INBOX_KINDS.OUTCOME_SUMMARY_AVAILABLE]: INBOX_CATEGORIES.COMPLETED,
  [INBOX_KINDS.WORKFLOW_COMPLETED]: INBOX_CATEGORIES.COMPLETED,
});

const INBOX_PRIORITY = Object.freeze({
  CRITICAL: 'critical',
  HIGH: 'high',
  NORMAL: 'normal',
  LOW: 'low',
});

const PRIORITY_ORDER = Object.freeze({
  [INBOX_PRIORITY.CRITICAL]: 0,
  [INBOX_PRIORITY.HIGH]: 1,
  [INBOX_PRIORITY.NORMAL]: 2,
  [INBOX_PRIORITY.LOW]: 3,
});

const INBOX_STATUS = Object.freeze({
  OPEN: 'open',
  IN_PROGRESS: 'in_progress',
  SNOOZED: 'snoozed',
  COMPLETED: 'completed',
  REJECTED: 'rejected',
  ARCHIVED: 'archived',
});

/** Statuses that remain on the active (outstanding) list. */
const ACTIVE_STATUSES = Object.freeze(
  new Set([
    INBOX_STATUS.OPEN,
    INBOX_STATUS.IN_PROGRESS,
    INBOX_STATUS.SNOOZED,
  ])
);

const WORKSPACE_TARGETS = Object.freeze({
  CAMPAIGN_REVIEW: 'campaign_review',
  MAIL_PACKAGE: 'mail_package',
  COMPANY_INTELLIGENCE: 'company_intelligence',
  DIRECT_MAIL_EXECUTION: 'direct_mail_execution',
  OUTCOME_SUMMARY: 'outcome_summary',
  PROPOSAL: 'proposal',
  OPERATOR_INBOX: 'operator_inbox',
});

const OPERATOR_ACTIONS = Object.freeze([
  'open',
  'review',
  'approve',
  'reject',
  'complete',
  'snooze',
  'assign',
  'archive',
  'ingest',
]);

const INBOX_PROGRESS_STAGES = Object.freeze({
  INGESTING: 'Ingesting work items',
  DEDUPING: 'Deduplicating',
  PRIORITIZING: 'Prioritizing',
  APPLYING: 'Applying actions',
  COMPLETED: 'Completed',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildInboxItem(partial = {}) {
  const kind = String(partial.kind || INBOX_KINDS.FOLLOW_UP);
  const category =
    partial.category || KIND_CATEGORY[kind] || INBOX_CATEGORIES.ACTION_REQUIRED;
  const deepLink = buildDeepLink(partial.deepLink || partial);
  return {
    id: String(partial.id || ''),
    title: String(partial.title || defaultTitle(kind)).trim(),
    category,
    kind,
    priority: partial.priority || INBOX_PRIORITY.NORMAL,
    sourceCapability:
      partial.sourceCapability != null
        ? String(partial.sourceCapability)
        : null,
    missionId: partial.missionId != null ? String(partial.missionId) : null,
    clientId: partial.clientId != null ? partial.clientId : null,
    campaignId: partial.campaignId != null ? String(partial.campaignId) : null,
    subjectId: partial.subjectId != null ? String(partial.subjectId) : null,
    createdAt: partial.createdAt || new Date().toISOString(),
    dueDate: partial.dueDate || null,
    status: partial.status || INBOX_STATUS.OPEN,
    assignee: partial.assignee != null ? String(partial.assignee) : null,
    snoozedUntil: partial.snoozedUntil || null,
    deepLink,
    dedupeKey: partial.dedupeKey != null ? String(partial.dedupeKey) : null,
    sources: Array.isArray(partial.sources) ? partial.sources : [],
    notes: partial.notes != null ? String(partial.notes) : null,
    completedAt: partial.completedAt || null,
    completedBy: partial.completedBy != null ? String(partial.completedBy) : null,
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildDeepLink(partial = {}) {
  if (partial && partial.workspace && typeof partial === 'object' && partial.href) {
    return {
      workspace: String(partial.workspace),
      href: String(partial.href),
      label: partial.label != null ? String(partial.label) : null,
    };
  }
  const workspace =
    partial.workspace ||
    partial.target ||
    workspaceForKind(partial.kind) ||
    WORKSPACE_TARGETS.OPERATOR_INBOX;
  const missionId = partial.missionId != null ? String(partial.missionId) : null;
  const campaignId =
    partial.campaignId != null ? String(partial.campaignId) : null;
  const href =
    partial.href ||
    `/command-deck#${workspace}${missionId ? `:${missionId}` : ''}${
      campaignId ? `?campaign=${encodeURIComponent(campaignId)}` : ''
    }`;
  return {
    workspace: String(workspace),
    href,
    label: partial.label != null ? String(partial.label) : String(workspace),
  };
}

/**
 * @param {string} kind
 * @returns {string}
 */
function workspaceForKind(kind) {
  const map = {
    [INBOX_KINDS.CAMPAIGN_APPROVAL]: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
    [INBOX_KINDS.MAIL_PACKAGE_APPROVAL]: WORKSPACE_TARGETS.MAIL_PACKAGE,
    [INBOX_KINDS.PROPOSAL_APPROVAL]: WORKSPACE_TARGETS.PROPOSAL,
    [INBOX_KINDS.LOW_CONFIDENCE_INTELLIGENCE]:
      WORKSPACE_TARGETS.COMPANY_INTELLIGENCE,
    [INBOX_KINDS.MISSING_RECIPIENT]: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
    [INBOX_KINDS.MISSING_ADDRESS]: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
    [INBOX_KINDS.VALIDATION_ISSUES]: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
    [INBOX_KINDS.PRINT_CAMPAIGN]: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
    [INBOX_KINDS.ASSEMBLE_MAIL]: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
    [INBOX_KINDS.MAIL_CAMPAIGN]: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
    [INBOX_KINDS.APPLY_RECOMMENDATION]: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
    [INBOX_KINDS.UPDATE_CLIENT_PLAYBOOK]: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
    [INBOX_KINDS.APPLY_RANKING_CHANGES]: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
    [INBOX_KINDS.OUTCOME_SUMMARY_AVAILABLE]: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
    [INBOX_KINDS.CAMPAIGN_COMPLETED]: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
  };
  return map[kind] || WORKSPACE_TARGETS.OPERATOR_INBOX;
}

/**
 * @param {string} kind
 * @returns {string}
 */
function defaultTitle(kind) {
  return String(kind || 'work_item')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildAuditEntry(partial = {}) {
  return Object.freeze({
    id: String(partial.id || ''),
    itemId: partial.itemId != null ? String(partial.itemId) : null,
    action: String(partial.action || ''),
    previousStatus:
      partial.previousStatus != null ? String(partial.previousStatus) : null,
    newStatus: partial.newStatus != null ? String(partial.newStatus) : null,
    operator: partial.operator != null ? String(partial.operator) : 'system',
    timestamp: partial.timestamp || new Date().toISOString(),
    notes: String(partial.notes || '').trim(),
  });
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildCompletionEvent(partial = {}) {
  return {
    kind: 'inbox_completion_event',
    itemId: partial.itemId != null ? String(partial.itemId) : null,
    inboxKind: partial.inboxKind != null ? String(partial.inboxKind) : null,
    missionId: partial.missionId != null ? String(partial.missionId) : null,
    operator: partial.operator != null ? String(partial.operator) : 'system',
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    result: partial.result != null ? String(partial.result) : 'completed',
  };
}

/**
 * Mission Memory event shape (SPEC-032 contract mirror).
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionInboxEvent(partial = {}) {
  return {
    kind: 'mission_inbox_event',
    eventType: String(partial.eventType || 'inbox_updated'),
    timestamp: partial.timestamp || new Date().toISOString(),
    operator: partial.operator != null ? String(partial.operator) : 'system',
    summary: String(partial.summary || '').trim(),
    itemId: partial.itemId != null ? String(partial.itemId) : null,
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildMissionTimelineEntry(partial = {}) {
  return {
    kind: 'mission_timeline',
    stage: String(partial.stage || 'operator_inbox'),
    status: partial.status != null ? String(partial.status) : null,
    timestamp: partial.timestamp || new Date().toISOString(),
    summary: String(partial.summary || '').trim(),
    operator: partial.operator != null ? String(partial.operator) : null,
  };
}

module.exports = {
  INBOX_CATEGORIES,
  INBOX_KINDS,
  KIND_CATEGORY,
  INBOX_PRIORITY,
  PRIORITY_ORDER,
  INBOX_STATUS,
  ACTIVE_STATUSES,
  WORKSPACE_TARGETS,
  OPERATOR_ACTIONS,
  INBOX_PROGRESS_STAGES,
  buildInboxItem,
  buildDeepLink,
  workspaceForKind,
  defaultTitle,
  buildAuditEntry,
  buildCompletionEvent,
  buildMissionInboxEvent,
  buildMissionTimelineEntry,
};
