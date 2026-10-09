'use strict';

const pool = require('../db');
const {
  ingestOperationalUpdate,
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
  if ((body.source_type || body.sourceType) === 'FILE_IMPORTED' || body.artifact?.artifact_type === 'spreadsheet' || body.sheets || body.rows) {
    throw Object.assign(new Error('Spreadsheet files require a server-owned reviewed proposal'), { code: 'reviewed_spreadsheet_proposal_required', statusCode: 410 });
  }
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
  throw Object.assign(new Error('Legacy spreadsheet persistence is disabled; use an approved server proposal'), {
    code: 'reviewed_spreadsheet_proposal_required', statusCode: 410,
  });

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
