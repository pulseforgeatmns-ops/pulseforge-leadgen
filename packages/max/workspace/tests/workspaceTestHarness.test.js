'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { hermeticWorkspaceOptions, createHermeticWorkspaceEngine, hermeticRetrievalInput } = require('./workspaceTestHarness');
const { createMemoryStore } = require('../../../../services/clientIntelligenceInterview');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { loadDurableBusinessUnderstanding, KNOWLEDGE_STATES } = require('../BusinessUnderstandingRetrieval');

test('unit defaults use fresh isolated memory stores and runtime', async () => {
  const a = hermeticWorkspaceOptions({ disableLlm: true });
  const b = hermeticWorkspaceOptions({ disableLlm: true });
  assert.equal(a.clientIntelligenceOpts.store.kind, 'memory');
  assert.notEqual(a.clientIntelligenceOpts.store, b.clientIntelligenceOpts.store);
  assert.notEqual(a.acquisitionMissionRuntime, b.acquisitionMissionRuntime);
  assert.equal(a.acquisitionMissionRuntime.persistOpts().persist, false);
  const engine = createHermeticWorkspaceEngine({ disableLlm: true });
  const opened = await engine.open({ tenantId: '10' });
  assert.ok(opened.sessionId);
});

test('explicit services, memory store, runtime and other options retain identity', () => {
  const store = createMemoryStore();
  const cieOpts = { store, marker: 'preserve' };
  const service = { getApprovedClientBlueprint: async () => null };
  const runtime = createTestAmoRuntime();
  const provider = () => runtime;
  const options = { clientIntelligenceOpts: cieOpts, clientIntelligenceService: service, runtimeProvider: provider, acquisitionMissionRuntime: runtime, disableLlm: false, anthropic: { fixture: true } };
  const actual = hermeticWorkspaceOptions(options);
  assert.equal(actual.clientIntelligenceOpts, cieOpts);
  assert.equal(actual.clientIntelligenceService, service);
  assert.equal(actual.runtimeProvider, provider);
  assert.equal(actual.acquisitionMissionRuntime, runtime);
  assert.equal(actual.disableLlm, false);
  assert.deepEqual(options, actual);
});

test('direct retrieval fixture distinguishes absent knowledge from a failing supplied service', async () => {
  const empty = await loadDurableBusinessUnderstanding(hermeticRetrievalInput({ context: { tenantId: '10' } }));
  assert.equal(empty.knowledgeState, KNOWLEDGE_STATES.NEVER_LEARNED);
  const failure = { getApprovedClientBlueprint: async () => { throw new Error('fixture unavailable'); } };
  const input = hermeticRetrievalInput({ context: { tenantId: '10' }, cieService: failure });
  assert.equal(input.cieService, failure);
  const failed = await loadDurableBusinessUnderstanding(input);
  assert.equal(failed.knowledgeState, KNOWLEDGE_STATES.RETRIEVAL_FAILURE);
});

test('unit harness refuses implicit live LLM dependency', () => {
  assert.throws(() => createHermeticWorkspaceEngine({ disableLlm: false }), /explicit fake LLM/);
});

// Unknown evidence is an explicit test premise, never fabricated success.
test('explicit unavailable operating fixture never reports verified records', async () => {
  const { unavailableOperatingEvidence } = require('./workspaceTestHarness');
  const opts = unavailableOperatingEvidence();
  assert.equal(Object.keys(opts).length, 9);
  for (const load of Object.values(opts)) {
    assert.deepEqual(await load(), { available: false, reason: 'unit_fixture_not_recorded' });
  }
});
