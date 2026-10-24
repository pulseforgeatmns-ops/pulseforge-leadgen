'use strict';

/**
 * SPEC-113 Stage 1 — Ingestion.
 * Operator uploads documents. Compiler creates a draft workspace.
 */

const {
  WORKSPACE_STATUS,
  DOCUMENT_KINDS,
  asText,
  nowIso,
  newId,
  isPlainObject,
} = require('./types');

const KIND_ALIASES = Object.freeze({
  pain: DOCUMENT_KINDS.PAIN_RESEARCH,
  pain_points: DOCUMENT_KINDS.PAIN_RESEARCH,
  pain_research: DOCUMENT_KINDS.PAIN_RESEARCH,
  interview: DOCUMENT_KINDS.CUSTOMER_INTERVIEW,
  customer_interview: DOCUMENT_KINDS.CUSTOMER_INTERVIEW,
  discovery: DOCUMENT_KINDS.DISCOVERY_CALL,
  discovery_call: DOCUMENT_KINDS.DISCOVERY_CALL,
  sales: DOCUMENT_KINDS.SALES_CALL,
  sales_call: DOCUMENT_KINDS.SALES_CALL,
  founder: DOCUMENT_KINDS.FOUNDER_INTERVIEW,
  founder_interview: DOCUMENT_KINDS.FOUNDER_INTERVIEW,
  playbook: DOCUMENT_KINDS.PLAYBOOK,
  landing: DOCUMENT_KINDS.LANDING_PAGE,
  landing_page: DOCUMENT_KINDS.LANDING_PAGE,
  icp: DOCUMENT_KINDS.ICP_NOTES,
  icp_notes: DOCUMENT_KINDS.ICP_NOTES,
  objection: DOCUMENT_KINDS.OBJECTION_DOCUMENT,
  objections: DOCUMENT_KINDS.OBJECTION_DOCUMENT,
  case_study: DOCUMENT_KINDS.CASE_STUDY,
  outcome: DOCUMENT_KINDS.OUTCOME_REPORT,
  outcome_report: DOCUMENT_KINDS.OUTCOME_REPORT,
});

function normalizeKind(value, title = '', filename = '') {
  const raw = asText(value).toLowerCase().replace(/[\s-]+/g, '_');
  if (KIND_ALIASES[raw] || Object.values(DOCUMENT_KINDS).includes(raw)) {
    return KIND_ALIASES[raw] || raw;
  }
  const blob = `${title} ${filename}`.toLowerCase();
  if (/pain/.test(blob)) return DOCUMENT_KINDS.PAIN_RESEARCH;
  if (/interview/.test(blob)) return DOCUMENT_KINDS.CUSTOMER_INTERVIEW;
  if (/landing/.test(blob)) return DOCUMENT_KINDS.LANDING_PAGE;
  if (/sales/.test(blob)) return DOCUMENT_KINDS.SALES_CALL;
  if (/playbook/.test(blob)) return DOCUMENT_KINDS.PLAYBOOK;
  if (/icp/.test(blob)) return DOCUMENT_KINDS.ICP_NOTES;
  if (/objection/.test(blob)) return DOCUMENT_KINDS.OBJECTION_DOCUMENT;
  return DOCUMENT_KINDS.OTHER;
}

function buildDocument(partial = {}) {
  const src = isPlainObject(partial) ? partial : { body: String(partial || '') };
  const title = asText(src.title) || asText(src.filename) || 'Untitled document';
  const filename = asText(src.filename) || asText(src.title);
  return {
    id: asText(src.id) || newId('doc'),
    title,
    kind: normalizeKind(src.kind, title, filename),
    filename,
    body: asText(src.body || src.content || src.text),
    section: asText(src.section),
    uploadedAt: src.uploadedAt || nowIso(),
  };
}

function emptyWorkspace(partial = {}) {
  const clientKey = asText(partial.clientKey || partial.client_key);
  return {
    id: asText(partial.id) || newId('aic'),
    clientKey,
    clientName: asText(partial.clientName || partial.client_name) || clientKey,
    clientId: partial.clientId != null ? Number(partial.clientId) : null,
    spec: 'SPEC-113',
    status: WORKSPACE_STATUS.NEW,
    version: Number(partial.version) || 1,
    documents: [],
    concepts: [],
    edges: [],
    reviews: [],
    unknowns: [],
    aimId: null,
    publishedAim: null,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    compiledAt: null,
    approvedAt: null,
    publishedAt: null,
    approvedBy: null,
    isOperatingFact: false,
    executesOutreach: false,
  };
}

function ingestDocuments(workspace, documents = []) {
  const docs = (Array.isArray(documents) ? documents : [documents]).map(buildDocument);
  const empty = docs.filter((d) => !d.body);
  if (empty.length && !docs.some((d) => d.body)) {
    const err = new Error('Compiler ingestion requires at least one document with a text body.');
    err.code = 'aic_empty_document';
    throw err;
  }
  const resetCompile = [
    WORKSPACE_STATUS.EXTRACTED,
    WORKSPACE_STATUS.ONTOLOGY_READY,
    WORKSPACE_STATUS.IN_REVIEW,
    WORKSPACE_STATUS.APPROVED,
    WORKSPACE_STATUS.PUBLISHED,
  ].includes(workspace.status);
  const next = {
    ...workspace,
    documents: [...(workspace.documents || []), ...docs.filter((d) => d.body)],
    status: WORKSPACE_STATUS.INGESTING,
    updatedAt: nowIso(),
  };
  if (resetCompile) {
    next.concepts = [];
    next.edges = [];
    next.reviews = [];
    next.unknowns = [];
    next.publishedAim = null;
    next.aimId = null;
    next.publishedAt = null;
    next.compiledAt = null;
    next.approvedAt = null;
    next.approvedBy = null;
  }
  return next;
}

module.exports = {
  KIND_ALIASES,
  normalizeKind,
  buildDocument,
  emptyWorkspace,
  ingestDocuments,
};
