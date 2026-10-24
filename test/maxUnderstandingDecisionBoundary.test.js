'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const {
  interpretConversationalInput,
  ConversationMemory,
} = require('../packages/max/understanding');
const {
  ingestOperationalUpdate,
  MemoryStateStore,
} = require('../packages/max/stateIngestion');
const {
  reevaluateOnIngestion,
  MemoryDecisionStore,
} = require('../packages/max/decisionExecution');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-pack', name: 'Exeter Packaging', client_id: 1 },
    ],
    prospects: overrides.prospects || [
      {
        id: 'prospect-exeter',
        client_id: 1,
        company_id: 'co-exeter',
        company_name: 'Exeter Phillips',
        assigned_ao_id: 10,
        status: 'warm',
      },
    ],
    ...overrides,
  });
}

describe('MAX Understanding decision boundary', () => {
  it('blocks ambiguous pronoun from CRM commit and decision execution', async () => {
    const store = seedStore();
    const memory = new ConversationMemory();
    await ingestOperationalUpdate({ clientId: 1, text: 'Talked to Dave at Exeter Phillips.', store, memory });
    await ingestOperationalUpdate({ clientId: 1, text: 'Also spoke with Mike at Exeter Phillips.', store, memory });
    const blocked = await ingestOperationalUpdate({
      clientId: 1,
      text: 'He said call Thursday.',
      store,
      memory,
    });
    assert.equal(blocked.commit_blocked, true);
    assert.ok(blocked.clarification_required);
    assert.equal(blocked.mutations.length, 0);
    assert.equal(blocked.claims.length, 0);

    const decisionStore = new MemoryDecisionStore({ clientId: 1 });
    const followUp = await reevaluateOnIngestion({
      clientId: 1,
      stateStore: store,
      decisionStore,
      ingestionResult: blocked,
    });
    assert.equal(followUp.skipped, true);
    assert.equal(decisionStore.decisions.length, 0);
    assert.equal(decisionStore.aoTasks.length, 0);
  });

  it('blocks ambiguous account identity from commit', async () => {
    const store = seedStore();
    const blocked = await ingestOperationalUpdate({
      clientId: 1,
      text: 'Exeter said call Thursday.',
      store,
      contextAccounts: ['Exeter Phillips', 'Exeter Packaging'],
    });
    assert.equal(blocked.commit_blocked, true);
    assert.equal(blocked.mutations.length, 0);
  });

  it('blocks stale pronoun without durable mutations', async () => {
    const store = seedStore();
    const memory = new ConversationMemory();
    await ingestOperationalUpdate({ clientId: 1, text: 'Talked to Dave at Exeter Phillips.', store, memory });
    await ingestOperationalUpdate({ clientId: 1, text: 'Stopped at ABC Manufacturing.', store, memory });
    await ingestOperationalUpdate({ clientId: 1, text: 'Visited Granite State Daycare.', store, memory });
    const blocked = await ingestOperationalUpdate({
      clientId: 1,
      text: 'He said call Thursday.',
      store,
      memory,
    });
    assert.equal(blocked.commit_blocked, true);
    assert.equal(blocked.mutations.length, 0);
  });

  it('records understanding telemetry counters without raw text', async () => {
    const store = seedStore();
    const memory = new ConversationMemory();
    await ingestOperationalUpdate({ clientId: 1, text: 'Talked to Dave at Exeter Phillips.', store, memory });
    await ingestOperationalUpdate({ clientId: 1, text: 'Also spoke with Mike at Exeter Phillips.', store, memory });
    const blocked = await ingestOperationalUpdate({
      clientId: 1,
      text: 'He said call Thursday.',
      store,
      memory,
    });
    assert.ok(blocked.telemetry.understanding_commit_blocked_count >= 1);
    assert.ok(blocked.telemetry.understanding_reference_ambiguity_count >= 1);
    assert.equal(JSON.stringify(blocked.telemetry).includes('He said call'), false);
  });

  it('flags uncertain action target when follow-up account is ambiguous', () => {
    const { validation } = interpretConversationalInput({
      text: 'Put a follow-up on Monday for them.',
      contextAccounts: ['Exeter Phillips', 'Exeter Packaging'],
    });
    assert.equal(validation.blockCommit, true);
  });
});
