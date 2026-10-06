'use strict';

const pool = require('../db');
const {
  ingestOperationalUpdate,
  ingestSpreadsheet,
  PostgresStateStore,
  markOverdueExpectations,
  followUpPromptForExpectation,
} = require('../packages/max/stateIngestion');

async function createStore(clientId, db = pool) {
  const store = new PostgresStateStore(db, { clientId });
  await store.init();
  return store;
}

async function ingestOperationalEvidence(clientId, body = {}, { db = pool } = {}) {
  const store = await createStore(clientId, db);
  const conversationId = body.conversation_id || body.conversationId || null;
  let memoryRepository = null;
  if (conversationId) {
    const { PostgresConversationMemoryRepository } = require('../packages/max/understanding');
    memoryRepository = new PostgresConversationMemoryRepository(db);
    await memoryRepository.init();
  }
  return ingestOperationalUpdate({
    clientId,
    sourceType: body.source_type || body.sourceType || 'OPERATOR_REPORTED',
    sourceActor: body.source_actor || body.sourceActor || null,
    text: body.text,
    message: body.message,
    structured: body.structured,
    claims: body.claims,
    artifact: body.artifact,
    operatorCorrection: Boolean(body.operator_correction || body.operatorCorrection),
    conversationId,
    actor: body.actor || {
      userId: body.source_actor || body.sourceActor || null,
      role: body.actor_role || body.actorRole || null,
    },
    contextAccounts: body.context_accounts || body.contextAccounts,
    memoryRepository,
    store,
    now: body.now ? new Date(body.now) : new Date(),
  });
}

async function ingestSpreadsheetEvidence(clientId, body = {}, { db = pool } = {}) {
  const store = await createStore(clientId, db);
  const sheetName = body.sheet_name || body.sheetName || 'Prospects';
  const rows = Array.isArray(body.rows) ? body.rows : [];
  const sheets = Array.isArray(body.sheets) ? body.sheets : null;
  const batch = await ingestSpreadsheet({
    clientId,
    filename: body.filename || 'spreadsheet',
    sheetName,
    rows,
    sheets,
    instruction: body.instruction || body.text || null,
    sourceType: body.source_type || 'FILE_IMPORTED',
    sourceActor: body.source_actor || null,
    store,
    now: body.now ? new Date(body.now) : new Date(),
    commitMode: body.commit_mode || 'safe_only',
  });
  return batch;
}

async function listOverdueExpectationPrompts(clientId, { db = pool, now = new Date() } = {}) {
  const store = await createStore(clientId, db);
  const open = await store.listOpenExpectations({ clientId });
  const overdue = markOverdueExpectations(open, now);
  return overdue.map(exp => ({
    expectation_id: exp.id,
    prompt: followUpPromptForExpectation(exp, exp.source_evidence?.ao_name || 'AO'),
    expectation: exp,
  }));
}

module.exports = {
  ingestOperationalEvidence,
  ingestSpreadsheetEvidence,
  listOverdueExpectationPrompts,
};
