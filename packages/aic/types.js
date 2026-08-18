'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler types.
 * Compiler output is not operating fact and never executes outreach.
 */

const WORKSPACE_STATUS = Object.freeze({
  NEW: 'new',
  INGESTING: 'ingesting',
  EXTRACTED: 'extracted',
  ONTOLOGY_READY: 'ontology_ready',
  IN_REVIEW: 'in_review',
  APPROVED: 'approved',
  PUBLISHED: 'published',
});

const CONCEPT_TYPES = Object.freeze({
  MISSION: 'mission',
  TRANSFORMATION: 'transformation',
  ICP: 'icp',
  PAIN_CATEGORY: 'pain_category',
  PAIN: 'pain',
  OBSERVABLE_SIGNAL: 'observable_signal',
  BUYING_TRIGGER: 'buying_trigger',
  OBJECTION: 'objection',
  LANGUAGE: 'language',
  EVIDENCE: 'evidence',
  DISQUALIFIER: 'disqualifier',
  CONFIDENCE_RULE: 'confidence_rule',
  MESSAGING: 'messaging',
  UNKNOWN: 'unknown',
});

const CONCEPT_STATUS = Object.freeze({
  PROPOSED: 'proposed',
  ACCEPTED: 'accepted',
  EDITED: 'edited',
  MERGED: 'merged',
  REMOVED: 'removed',
});

const RELATIONS = Object.freeze({
  SUPPORTED_BY: 'supported_by',
  OBSERVED_THROUGH: 'observed_through',
  MAPS_TO: 'maps_to',
  EXCLUDES: 'excludes',
  INCREASES_CERTAINTY: 'increases_certainty',
  DECREASES_CERTAINTY: 'decreases_certainty',
  BELONGS_TO: 'belongs_to',
});

const REVIEW_ACTIONS = Object.freeze({
  ACCEPT: 'accept',
  EDIT: 'edit',
  MERGE: 'merge',
  REMOVE: 'remove',
});

const DOCUMENT_KINDS = Object.freeze({
  PAIN_RESEARCH: 'pain_research',
  CUSTOMER_INTERVIEW: 'customer_interview',
  DISCOVERY_CALL: 'discovery_call',
  SALES_CALL: 'sales_call',
  FOUNDER_INTERVIEW: 'founder_interview',
  PLAYBOOK: 'playbook',
  LANDING_PAGE: 'landing_page',
  ICP_NOTES: 'icp_notes',
  OBJECTION_DOCUMENT: 'objection_document',
  CASE_STUDY: 'case_study',
  OUTCOME_REPORT: 'outcome_report',
  OTHER: 'other',
});

const RUNTIME_AIM_STATUSES = Object.freeze(['complete', 'published']);

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function asList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  return splitList(asText(value));
}

function splitList(text) {
  return asText(text)
    .split(/\s*(?:,|;|\n|\||\/)\s*/)
    .map((s) => s.replace(/^[-*•]\s*/, '').trim())
    .filter(Boolean);
}

function nowIso(now) {
  if (now instanceof Date) return now.toISOString();
  if (typeof now === 'number') return new Date(now).toISOString();
  return new Date().toISOString();
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function slugify(text) {
  const slug = asText(text)
    .toLowerCase()
    .replace(/['"]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 72);
  return slug || 'concept';
}

function newId(prefix = 'id') {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function isRuntimeAim(aim) {
  return Boolean(aim && RUNTIME_AIM_STATUSES.includes(aim.status));
}

module.exports = {
  WORKSPACE_STATUS,
  CONCEPT_TYPES,
  CONCEPT_STATUS,
  RELATIONS,
  REVIEW_ACTIONS,
  DOCUMENT_KINDS,
  RUNTIME_AIM_STATUSES,
  asText,
  asList,
  splitList,
  nowIso,
  isPlainObject,
  clone,
  slugify,
  newId,
  isRuntimeAim,
};
