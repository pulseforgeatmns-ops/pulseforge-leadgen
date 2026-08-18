'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler.
 * Market knowledge → compile → human approval → published AIM.
 * Never executes outreach.
 */

const fs = require('fs');
const path = require('path');

const {
  WORKSPACE_STATUS,
  CONCEPT_TYPES,
  DOCUMENT_KINDS,
  asText,
  nowIso,
} = require('./types');
const { ingestDocuments, emptyWorkspace, buildDocument } = require('./Ingestion');
const { extractConcepts } = require('./Extraction');
const { constructOntology } = require('./Ontology');
const { reviewConcept, approveWorkspace } = require('./Review');
const { publishWorkspace } = require('./Publication');
const { createMemoryAicStore } = require('./Store');

const FIXTURE_DIR = path.join(__dirname, 'fixtures');

const OUTREACH_RE = /\b(send email|send sms|enroll sequence|place call|twilio|brevo|bland)\b/i;

function assertNoOutreach(input) {
  const blob = typeof input === 'string' ? input : JSON.stringify(input || {});
  if (OUTREACH_RE.test(blob)) {
    const err = new Error('Acquisition Intelligence Compiler cannot execute outreach.');
    err.code = 'aic_no_outreach';
    throw err;
  }
}

function loadFixtureDocuments() {
  return [
    buildDocument({
      title: 'Pain Points v3',
      kind: DOCUMENT_KINDS.PAIN_RESEARCH,
      filename: 'pain_points_v3.md',
      body: fs.readFileSync(path.join(FIXTURE_DIR, 'pain_points_v3.md'), 'utf8'),
    }),
    buildDocument({
      title: 'Customer Interview #12',
      kind: DOCUMENT_KINDS.CUSTOMER_INTERVIEW,
      filename: 'customer_interview_12.md',
      body: fs.readFileSync(path.join(FIXTURE_DIR, 'customer_interview_12.md'), 'utf8'),
    }),
    buildDocument({
      title: 'Landing Page',
      kind: DOCUMENT_KINDS.LANDING_PAGE,
      filename: 'landing_page.md',
      body: fs.readFileSync(path.join(FIXTURE_DIR, 'landing_page.md'), 'utf8'),
    }),
  ];
}

function compileWorkspace(workspace) {
  assertNoOutreach(workspace);
  if (!workspace.documents || !workspace.documents.length) {
    const err = new Error('Compile requires ingested documents.');
    err.code = 'aic_no_documents';
    throw err;
  }
  const extracted = extractConcepts(workspace.documents);
  const ontology = constructOntology(extracted.concepts, extracted.pendingEdges);
  const unknowns = ontology.concepts
    .filter((c) => c.type === CONCEPT_TYPES.UNKNOWN)
    .map((c) => c.statement);
  if (!ontology.concepts.some((c) => c.type === CONCEPT_TYPES.MISSION)) {
    unknowns.push('Mission was not found in source documents.');
  }
  if (!ontology.concepts.some((c) => c.type === CONCEPT_TYPES.ICP)) {
    unknowns.push('ICP reasoning was not found in source documents.');
  }
  return {
    ...workspace,
    concepts: ontology.concepts,
    edges: ontology.edges,
    unknowns,
    status: WORKSPACE_STATUS.IN_REVIEW,
    compiledAt: nowIso(),
    updatedAt: nowIso(),
    approvedAt: null,
    approvedBy: null,
    publishedAt: null,
    publishedAim: null,
    aimId: null,
  };
}

function createCompiler(opts = {}) {
  const store = opts.store || createMemoryAicStore();
  const aimStore = opts.aimStore || null;

  function save(workspace) {
    return store.putWorkspace(workspace);
  }

  function createWorkspace(partial = {}) {
    assertNoOutreach(partial);
    return store.createWorkspace(partial);
  }

  function addDocuments(workspaceId, documents) {
    assertNoOutreach(documents);
    const workspace = ingestDocuments(store.requireWorkspace(workspaceId), documents);
    return save(workspace);
  }

  function compile(workspaceId) {
    const compiled = compileWorkspace(store.requireWorkspace(workspaceId));
    return save(compiled);
  }

  function review(workspaceId, conceptId, input, reviewOpts = {}) {
    const workspace = store.requireWorkspace(workspaceId);
    reviewConcept(workspace, conceptId, input, reviewOpts);
    return save(workspace);
  }

  function approve(workspaceId, approveOpts = {}) {
    const workspace = approveWorkspace(store.requireWorkspace(workspaceId), approveOpts);
    return save(workspace);
  }

  function publish(workspaceId, publishOpts = {}) {
    const result = publishWorkspace(store.requireWorkspace(workspaceId), {
      ...publishOpts,
      aimStore: publishOpts.aimStore || aimStore,
    });
    save(result.workspace);
    return result;
  }

  function ingestAndCompile(partial, documents) {
    const created = createWorkspace(partial);
    addDocuments(created.id, documents);
    return compile(created.id);
  }

  return {
    store,
    aimStore,
    createWorkspace,
    addDocuments,
    compile,
    review,
    approve,
    publish,
    ingestAndCompile,
    getWorkspace: (id) => store.getWorkspace(id),
    listWorkspaces: (clientKey) => store.listWorkspaces(clientKey),
  };
}

module.exports = {
  FIXTURE_DIR,
  loadFixtureDocuments,
  compileWorkspace,
  createCompiler,
  emptyWorkspace,
  ingestDocuments,
};
