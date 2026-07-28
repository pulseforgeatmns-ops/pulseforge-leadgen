'use strict';

/**
 * Mission Engine types (SPEC-022 / ADR-010).
 */

const MISSION_STATUS = Object.freeze({
  REQUESTED: 'requested',
  PLANNING: 'planning',
  EXECUTING: 'executing',
  WAITING: 'waiting',
  REVIEW_REQUIRED: 'review_required',
  COMPLETED: 'completed',
  REVIEWED: 'reviewed',
  ARCHIVED: 'archived',
  FAILED: 'failed',
});

const MISSION_TYPES = Object.freeze({
  CAMPAIGN_CREATION: 'campaign_creation',
  PROSPECT_DISCOVERY: 'prospect_discovery',
  COMPETITOR_RESEARCH: 'competitor_research',
  OVERFLOW_PARTNER_SEARCH: 'overflow_partner_search',
  ACQUISITION_SEARCH: 'acquisition_search',
  MARKET_RESEARCH: 'market_research',
  WEEKLY_BRIEF: 'weekly_brief',
  KNOWLEDGE_REFRESH: 'knowledge_refresh',
  PROPOSAL_GENERATION: 'proposal_generation',
  MAIL_PACKAGE_GENERATION: 'mail_package_generation',
  CAMPAIGN_REVIEW: 'campaign_review',
  DIRECT_MAIL_EXECUTION: 'direct_mail_execution',
  OUTCOME_INTELLIGENCE: 'outcome_intelligence',
  OPERATOR_INBOX: 'operator_inbox',
});

const AUDIT_KINDS = Object.freeze({
  REQUEST: 'request',
  PLAN: 'plan',
  STEP_START: 'step_start',
  STEP_OK: 'step_ok',
  STEP_FAIL: 'step_fail',
  RETRY: 'retry',
  REVIEW: 'review',
  ARCHIVE: 'archive',
  STATUS: 'status',
  PROGRESS: 'progress',
});

const REVIEW_ACTIONS = Object.freeze({
  APPROVE: 'approve',
  REJECT: 'reject',
  EDIT: 'edit',
  RUN_AGAIN: 'run_again',
});

const ROUTE_KINDS = Object.freeze({
  MISSION: 'mission',
  INTELLIGENCE: 'intelligence',
});

/** Operator-facing stage labels (never agent module names). */
const STAGE_LABELS = Object.freeze({
  planning: 'Planning Mission',
  prospect_discovery: 'Discovering Prospects',
  company_enrichment: 'Enriching Companies',
  knowledge_update: 'Updating Knowledge',
  opportunity_ranking: 'Ranking Opportunities',
  campaign_builder: 'Building Campaign',
  proposal_generator: 'Generating Proposal',
  mail_package_generator: 'Generating Mail Packages',
  campaign_review: 'Campaign Review',
  direct_mail_execution: 'Direct Mail Execution',
  outcome_intelligence: 'Outcome Intelligence',
  operator_inbox: 'Operator Inbox',
  review_required: 'Ready for Review',
});

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function missionEnabled() {
  const flag = process.env.MISSION_ENGINE;
  if (flag === '0' || flag === 'false' || flag === 'off') return false;
  return true;
}

module.exports = {
  MISSION_STATUS,
  MISSION_TYPES,
  AUDIT_KINDS,
  REVIEW_ACTIONS,
  ROUTE_KINDS,
  STAGE_LABELS,
  newId,
  missionEnabled,
};
