'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const {
  interpretWithDurableConversationContext,
  MemoryConversationMemoryRepository,
  ConversationMemory,
  validateSituationModel,
  SEMANTIC_TYPE,
} = require('../packages/max/understanding');

function durableInput(overrides = {}) {
  const repo = overrides.repository || new MemoryConversationMemoryRepository();
  return {
    tenantId: 1,
    clientId: 1,
    conversationId: overrides.conversationId || 'conv-durable-test',
    actor: overrides.actor || { userId: '10', role: 'manager' },
    memoryRepository: repo,
    contextAccounts: overrides.contextAccounts,
    now: overrides.now || new Date('2026-10-05T15:00:00.000Z'),
    ...overrides,
    repository: repo,
  };
}

async function turn(input) {
  const { repository, ...rest } = input;
  return interpretWithDurableConversationContext({ ...rest, memoryRepository: repository });
}

describe('MAX-MEMORY-001 durable conversational context', () => {
  it('M1 — restart pronoun recovery', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const base = durableInput({ repository, conversationId: 'm1-conv' });
    await turn({ ...base, text: 'Talked to Dave at Exeter Phillips.' });
    const second = await turn({
      ...base,
      text: 'Actually, he isn\'t the decision maker.',
    });
    assert.ok(second.situationModel.corrections.some(c => c.kind === 'decision_maker_role' || c.targetClaim === 'decision_maker_role'));
    assert.equal(second.conversationMemoryTelemetry.conversation_memory_hit_count, 1);
  });

  it('M2 — clarification survives restart', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const base = durableInput({
      repository,
      conversationId: 'm2-conv',
      contextAccounts: ['Exeter Phillips', 'Exeter Packaging'],
    });
    const blocked = await turn({ ...base, text: 'Exeter said call Thursday.' });
    assert.equal(blocked.validation.blockCommit, true);
    const continued = await turn({ ...base, text: 'Phillips.' });
    assert.equal(continued.validation.blockCommit, false);
    assert.match(continued.situationModel.threads[0].accountName, /Exeter Phillips/i);
    assert.ok(continued.situationModel.commitments.some(c => /thursday/i.test(c.windowPhrase || '')));
  });

  it('M3 — correction supersedes old binding', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const base = durableInput({ repository, conversationId: 'm3-conv' });
    await turn({ ...base, text: 'Dave is the decision maker at Exeter Phillips.' });
    const second = await turn({
      ...base,
      text: 'Actually Dave isn\'t the decision maker. Lisa handles vendors.',
    });
    assert.ok(second.situationModel.corrections.some(c => /Lisa|Dave/i.test(c.contactName || '')));
    const loaded = await repository.loadActive({
      tenantId: 1,
      conversationId: 'm3-conv',
    });
    const lisa = loaded.records.find(r =>
      r.semanticType === SEMANTIC_TYPE.ACTIVE_CONTACT && r.payload?.contact?.name === 'Lisa'
    );
    assert.ok(lisa);
  });

  it('M4 — stale memory does not over-resolve', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const base = durableInput({ repository, conversationId: 'm4-conv' });
    await turn({ ...base, text: 'Talked to Dave at Exeter Phillips.' });
    await turn({ ...base, text: 'Stopped at ABC Manufacturing for a quick visit.' });
    await turn({ ...base, text: 'Also checked in at Granite State Daycare.' });
    const third = await turn({ ...base, text: 'He said call Thursday.' });
    assert.equal(third.validation.blockCommit, true);
  });

  it('M5 — tenant isolation', async () => {
    const repository = new MemoryConversationMemoryRepository();
    await turn({
      ...durableInput({ repository, conversationId: 'tenant-a', tenantId: 1, clientId: 1 }),
      text: 'Talked to Dave at Exeter Phillips.',
    });
    const tenantB = await turn({
      ...durableInput({ repository, conversationId: 'tenant-b', tenantId: 2, clientId: 2 }),
      text: 'Actually, he isn\'t the decision maker.',
    });
    assert.equal(tenantB.validation.blockCommit, false);
    assert.equal(tenantB.situationModel.corrections.filter(c => c.kind === 'decision_maker_role').length, 0);
  });

  it('M6 — actor isolation', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const conv = 'm6-actors';
    await turn({
      ...durableInput({ repository, conversationId: conv, actor: { userId: 'tony', role: 'sales' } }),
      text: 'Talked to Dave at Exeter Phillips.',
    });
    const rory = await turn({
      ...durableInput({ repository, conversationId: conv, actor: { userId: 'rory', role: 'sales' } }),
      text: 'Actually, he isn\'t the decision maker.',
    });
    assert.equal(rory.validation.blockCommit, false);
    assert.equal(rory.situationModel.corrections.length, 0);
  });

  it('M7 — conversation isolation', async () => {
    const repository = new MemoryConversationMemoryRepository();
    await turn({
      ...durableInput({ repository, conversationId: 'conv-seven-a' }),
      text: 'Talked to Dave at Exeter Phillips.',
    });
    const other = await turn({
      ...durableInput({ repository, conversationId: 'conv-seven-b' }),
      text: 'Actually, he isn\'t the decision maker.',
    });
    assert.equal(other.situationModel.corrections.length, 0);
  });

  it('M8 — expired context is not used', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const conv = 'm8-expired';
    await turn({
      ...durableInput({ repository, conversationId: conv }),
      text: 'Talked to Dave at Exeter Phillips.',
    });
    for (const rec of repository.records) {
      rec.expiresAt = new Date('2020-01-01T00:00:00.000Z').toISOString();
    }
    const second = await turn({
      ...durableInput({ repository, conversationId: conv }),
      text: 'Actually, he isn\'t the decision maker.',
    });
    assert.equal(second.situationModel.corrections.length, 0);
  });

  it('M9 — memory DB unavailable blocks reference-dependent input', async () => {
    const repository = {
      async loadActive() {
        throw new Error('connection refused');
      },
      async upsertRecords() {
        return { written: 0, duplicates: 0 };
      },
      async supersedeActive() {
        return 0;
      },
    };
    const result = await turn({
      ...durableInput({ repository, conversationId: 'm9-fail' }),
      text: 'He said call Thursday.',
    });
    assert.equal(result.conversationMemoryTelemetry.conversation_memory_load_failed, true);
    assert.equal(result.validation.blockCommit, true);
  });

  it('M10 — idempotent replay does not duplicate active bindings', async () => {
    const repository = new MemoryConversationMemoryRepository();
    const base = durableInput({ repository, conversationId: 'm10-idempotent' });
    const inputId = 'in_fixed_replay';
    await turn({ ...base, text: 'Talked to Dave at Exeter Phillips.', inputId });
    await turn({ ...base, text: 'Talked to Dave at Exeter Phillips.', inputId });
    const activeContacts = repository.records.filter(r =>
      r.semanticType === SEMANTIC_TYPE.ACTIVE_CONTACT && !r.supersededAt
    );
    const daveRows = activeContacts.filter(r => r.payload?.contact?.name === 'Dave');
    assert.equal(daveRows.length, 1);
  });
});

describe('MAX-MEMORY-001 hydration unit', () => {
  it('ConversationMemory.fromDurableRecords rebuilds contacts', () => {
    const mem = ConversationMemory.fromDurableRecords({
      conversationId: 'x',
      records: [{
        semanticType: SEMANTIC_TYPE.ACTIVE_CONTACT,
        payload: {
          contact: { kind: 'contact', name: 'Dave', gender: 'male', accountName: 'Exeter Phillips' },
        },
      }],
    });
    const recent = mem.recentContacts({ genderHint: 'male' });
    assert.equal(recent[0].name, 'Dave');
  });
});
