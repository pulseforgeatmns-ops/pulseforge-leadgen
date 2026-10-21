'use strict';
require('./helpers/denyWebsiteEvidenceNetwork.cjs');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createMaxReasoningRuntime } = require('../packages/max');
const { createTestAmoRuntime } = require('../packages/max/workspace/tests/amoTestRuntime');
const { createMemoryStore } = require('../services/clientIntelligenceInterview');

test('public runtime forwards explicit workspace dependencies by identity', () => {
  const runtime = createTestAmoRuntime();
  const provider = () => runtime;
  const service = { getApprovedClientBlueprint: async () => null };
  const opts = { store: createMemoryStore() };
  const max = createMaxReasoningRuntime({ disableLlm: true, acquisitionMissionRuntime: runtime, runtimeProvider: provider, clientIntelligenceService: service, clientIntelligenceOpts: opts });
  assert.equal(max.workspace._clientIntelligenceService, service);
  assert.equal(max.workspace._clientIntelligenceOpts, opts);
  assert.equal(max.workspace._resolveAmoRuntime(), runtime);
  assert.equal(max.workspace._runtimeProvider, provider);
});

test('public runtime omission retains workspace default dependency selection', () => {
  const max = createMaxReasoningRuntime({ disableLlm: true });
  assert.equal(max.workspace._clientIntelligenceService, null);
  assert.equal(max.workspace._clientIntelligenceOpts, null);
  assert.equal(max.workspace._runtimeProvider, null);
});

test('request context cannot replace construction-time service or runtime', async () => {
  const runtime = createTestAmoRuntime();
  const opts = { store: createMemoryStore() };
  const max = createMaxReasoningRuntime({ disableLlm: true, acquisitionMissionRuntime: runtime, clientIntelligenceOpts: opts });
  await max.askWorkspace({ question: 'Hello', context: { tenantId: '10', acquisitionMissionRuntime: { forged: true }, runtimeProvider: () => { throw new Error('forged'); }, clientIntelligenceService: { forged: true } } });
  assert.equal(max.workspace._resolveAmoRuntime(), runtime);
  assert.equal(max.workspace._clientIntelligenceService, null);
  assert.equal(max.workspace._clientIntelligenceOpts, opts);
});
