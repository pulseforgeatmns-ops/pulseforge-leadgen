'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { createWorkspaceEngine, composeResponse } = require('../../index');

function testMissionEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-022 Workspace Mission routing', () => {
  it('Build Campaign 001 creates a mission and skips Market Intelligence', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    let composeCalled = false;
    const originalCompose = composeResponse;
    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.equal(result.mission.status, 'review_required');
    assert.equal(result.mission.title, 'Campaign 001');
    assert.equal(result.structured.metadata.route, 'mission');
    assert.match(result.prose || result.structured.answer, /Mission created/i);
    assert.match(
      result.prose || result.structured.answer,
      /Discovery Profile/i
    );
    assert.doesNotMatch(
      result.structured.answer,
      /I can investigate .+ using only the intelligence/i
    );
    void originalCompose;
    void composeCalled;
  });

  it('Monitor Microsoft stays on intelligence path', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Monitor Microsoft.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
        },
      },
    });

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.notEqual(result.structured.metadata.route, 'mission');
  });

  it('disabled MISSION_ENGINE falls through to intelligence', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: false,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: { headline: 'Brief', summary: 'Summary' },
      },
    });

    assert.equal(result.route, 'mission');
    assert.equal(result.mission, null);
    assert.notEqual(result.structured.metadata.route, 'mission');
  });
});

describe('SPEC-022 Command Deck Operations section', () => {
  it('compose includes operations with mission cards', async () => {
    const { createMaxReasoningRuntime } = require('../../index');
    const missionEngine = testMissionEngine();
    await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    const cards = (await missionEngine.list({ tenantId: '10' })).map((m) =>
      missionEngine.toCard(m)
    );

    const max = createMaxReasoningRuntime({
      disableLlm: true,
      missionEngine,
    });
    const model = await max.compose({
      tenantId: '10',
      missions: cards,
      evaluatePolicy: false,
    });

    assert.ok(model.operations);
    assert.equal(model.operations.id, 'operations');
    assert.ok(model.operations.missions.length >= 1);
    assert.equal(model.operations.missions[0].title, 'Campaign 001');
    assert.ok(model.meta.missionCount >= 1);
  });
});
