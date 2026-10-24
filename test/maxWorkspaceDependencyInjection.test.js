'use strict';
require('./helpers/denyWebsiteEvidenceNetwork.cjs');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createMaxReasoningRuntime } = require('../packages/max');
const { createTestAmoRuntime } = require('../packages/max/workspace/tests/amoTestRuntime');
const { hermeticWorkspaceOptions } = require('../packages/max/workspace/tests/workspaceTestHarness');
const { createMemoryStore } = require('../services/clientIntelligenceInterview');

test('public runtime forwards explicit workspace dependencies by identity', () => {
  const runtime = createTestAmoRuntime();
  const provider = () => runtime;
  const service = { getApprovedClientBlueprint: async () => null };
  const opts = { store: createMemoryStore() };
  const objectiveService = { fixture: true };
  const objectiveOpts = { store: { fixture: true } };
  const max = createMaxReasoningRuntime({ disableLlm: true, acquisitionMissionRuntime: runtime, runtimeProvider: provider, clientIntelligenceService: service, clientIntelligenceOpts: opts, operatorObjectiveService: objectiveService, operatorObjectiveOpts: objectiveOpts });
  assert.equal(max.workspace._clientIntelligenceService, service);
  assert.equal(max.workspace._clientIntelligenceOpts, opts);
  assert.equal(max.workspace._resolveAmoRuntime(), runtime);
  assert.equal(max.workspace._runtimeProvider, provider);
  assert.equal(max.workspace._operatorObjectiveService, objectiveService);
  assert.equal(max.workspace._operatorObjectiveOpts, objectiveOpts);
});

test('public runtime omission retains workspace default dependency selection', () => {
  const max = createMaxReasoningRuntime({ disableLlm: true });
  assert.equal(max.workspace._clientIntelligenceService, null);
  assert.equal(max.workspace._clientIntelligenceOpts, null);
  assert.equal(max.workspace._runtimeProvider, null);
  assert.equal(max.workspace._operatorObjectiveService, null);
  assert.equal(max.workspace._operatorObjectiveOpts, null);
});

test('request context cannot replace construction-time service or runtime', async () => {
  const runtime = createTestAmoRuntime();
  const opts = { store: createMemoryStore() };
  const options = hermeticWorkspaceOptions({ disableLlm: true, acquisitionMissionRuntime: runtime, clientIntelligenceOpts: opts });
  const max = createMaxReasoningRuntime(options);
  await max.askWorkspace({ question: 'Hello', context: { tenantId: '10', acquisitionMissionRuntime: { forged: true }, runtimeProvider: () => { throw new Error('forged'); }, clientIntelligenceService: { forged: true }, operatorObjectiveOpts: { forged: true }, operatorObjectiveService: { forged: true } } });
  assert.equal(max.workspace._resolveAmoRuntime(), runtime);
  assert.equal(max.workspace._clientIntelligenceService, null);
  assert.equal(max.workspace._clientIntelligenceOpts, opts);
  assert.equal(max.workspace._operatorObjectiveOpts, options.operatorObjectiveOpts);
  assert.equal(max.workspace._operatorObjectiveService, null);
});
