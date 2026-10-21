'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { hermeticWorkspaceOptions, createHermeticWorkspaceEngine, hermeticRetrievalInput } = require('./workspaceTestHarness');
const { createMemoryStore } = require('../../../../services/clientIntelligenceInterview');
const objectives = require('../../../../services/operatorObjectives');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { loadDurableBusinessUnderstanding, KNOWLEDGE_STATES } = require('../BusinessUnderstandingRetrieval');

test('unit defaults use fresh isolated memory stores and runtime', async () => {
  const a = hermeticWorkspaceOptions({ disableLlm: true });
  const b = hermeticWorkspaceOptions({ disableLlm: true });
  assert.equal(a.clientIntelligenceOpts.store.kind, 'memory');
  assert.notEqual(a.clientIntelligenceOpts.store, b.clientIntelligenceOpts.store);
  assert.notEqual(a.acquisitionMissionRuntime, b.acquisitionMissionRuntime);
  assert.equal(a.operatorObjectiveOpts.store.kind, 'memory');
  assert.notEqual(a.operatorObjectiveOpts.store, b.operatorObjectiveOpts.store);
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
  const objectiveOpts = { store: objectives.createMemoryStore() };
  const options = { operatorObjectiveOpts: objectiveOpts, clientIntelligenceOpts: cieOpts, clientIntelligenceService: service, runtimeProvider: provider, acquisitionMissionRuntime: runtime, disableLlm: false, anthropic: { fixture: true } };
  const actual = hermeticWorkspaceOptions(options);
  assert.equal(actual.clientIntelligenceOpts, cieOpts);
  assert.equal(actual.operatorObjectiveOpts, objectiveOpts);
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

test('objective fixtures preserve explicit stores, services and pools in both adapters', () => {
  for (const fixture of [{ store: objectives.createMemoryStore() }, { pool: { query() { throw new Error('explicit pool'); } } }]) {
    assert.equal(hermeticWorkspaceOptions({ operatorObjectiveOpts: fixture }).operatorObjectiveOpts, fixture);
    assert.equal(hermeticRetrievalInput({ objectiveOpts: fixture }).objectiveOpts, fixture);
  }
  const service = { fixture: true };
  assert.equal(hermeticWorkspaceOptions({ operatorObjectiveService: service }).operatorObjectiveService, service);
  assert.equal(hermeticWorkspaceOptions({ operatorObjectiveService: service }).operatorObjectiveOpts, undefined);
  assert.equal(hermeticRetrievalInput({ objectiveService: service }).objectiveService, service);
  assert.equal(hermeticRetrievalInput({ objectiveService: service }).objectiveOpts, undefined);
});

test('objective fixtures retain tenant boundaries and do not leak between engines', async () => {
  const a = hermeticWorkspaceOptions();
  const b = hermeticWorkspaceOptions();
  await a.operatorObjectiveOpts.store.insert({ id: 'fixture-objective', tenantId: '10', status: 'active', scope: 'operator', updatedAt: new Date().toISOString() });
  assert.equal((await objectives.getActiveObjectives({ tenantId: '10' }, a.operatorObjectiveOpts)).length, 1);
  assert.equal((await objectives.getActiveObjectives({ tenantId: '17' }, a.operatorObjectiveOpts)).length, 0);
  assert.equal((await objectives.getActiveObjectives({ tenantId: '10' }, b.operatorObjectiveOpts)).length, 0);
});

test('CI database configuration cannot turn hermetic workspace or retrieval fixtures into live dependencies', () => {
  const { spawnSync } = require('node:child_process');
  const path = require('node:path');
  const root = path.resolve(__dirname, '../../../..');
  const result = spawnSync(process.execPath, ['--require', './test/helpers/denyWebsiteEvidenceNetwork.cjs', '-e', `
    const assert = require('node:assert/strict');
    const { createHermeticWorkspaceEngine, hermeticRetrievalInput, hermeticWorkspaceOptions, unavailableOperatingEvidence } = require('./packages/max/workspace/tests/workspaceTestHarness');
    const { loadDurableBusinessUnderstanding } = require('./packages/max/workspace/BusinessUnderstandingRetrieval');
    const { createMaxReasoningRuntime } = require('./packages/max');
    (async () => {
      assert.ok(process.env.DATABASE_URL);
      await createHermeticWorkspaceEngine().ask({ question: 'Hello', context: { tenantId: '10' } });
      const retrieval = await createHermeticWorkspaceEngine({ operatingEvidenceOpts: unavailableOperatingEvidence() }).ask({ question: 'What prospects do we already have?', context: { tenantId: '10' } });
      assert.equal(retrieval.domainDecision.reason, 'operating_evidence_retrieval');
      await loadDurableBusinessUnderstanding(hermeticRetrievalInput({ context: { tenantId: '10' } }));
      await createMaxReasoningRuntime(hermeticWorkspaceOptions()).askWorkspace({ question: 'Hello', context: { tenantId: '10' } });
    })().catch(error => { console.error(error); process.exitCode = 1; });
  `], { cwd: root, env: { ...process.env, DATABASE_URL: 'postgresql://localhost:1/pulseforge_test_only', DATABASE_SSL: 'false' }, encoding: 'utf8', timeout: 15000 });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stdout + result.stderr);
});

test('legacy mission Scout dependencies are construction-time only and omitted defaults remain unchanged', () => {
  const { createMissionEngine } = require('../../../mission-engine');
  const dependencies = { companies: [], people: [] };
  assert.equal(createMissionEngine().scoutDiscoveryOpts, undefined);
  assert.equal(createMissionEngine({ scoutDiscoveryOpts: dependencies }).scoutDiscoveryOpts, dependencies);
});
