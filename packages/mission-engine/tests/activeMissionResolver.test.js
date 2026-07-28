'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  classifyMessage,
  createMissionEngine,
  MESSAGE_CLASS,
  RESOLUTION_PATHS,
  MISSION_TYPES,
  AUDIT_KINDS,
} = require('..');
const { createBuiltinRegistry } = require('../../capabilities');
const { createWorkspaceEngine } = require('../../max/workspace');

function testEngine(opts = {}) {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: opts.resolverEnabled !== false,
  });
}

describe('SPEC-039 classifyMessage', () => {
  it('classifies explicit New Mission', () => {
    const r = classifyMessage('New Mission');
    assert.equal(r.classification, MESSAGE_CLASS.NEW_MISSION);
  });

  it('classifies diagnose against active Mission', () => {
    const active = {
      id: 'msn_1',
      objectiveText: 'Build Campaign 001 for Anchor Cleaning.',
      title: 'Campaign 001',
      status: 'waiting',
    };
    const r = classifyMessage(
      'Investigate why Campaign Review failed',
      active
    );
    assert.equal(r.classification, MESSAGE_CLASS.DIAGNOSE);
  });

  it('classifies modify', () => {
    const active = {
      id: 'msn_1',
      objectiveText: 'Build Campaign 001',
      title: 'Campaign 001',
      status: 'review_required',
    };
    const r = classifyMessage('Use Manchester instead of Boston', active);
    assert.equal(r.classification, MESSAGE_CLASS.MODIFY);
  });

  it('classifies Build Campaign 002 as new objective vs Campaign 001', () => {
    const active = {
      id: 'msn_1',
      objectiveText: 'Build Campaign 001 for Anchor Cleaning.',
      title: 'Campaign 001',
      status: 'review_required',
    };
    const r = classifyMessage('Build Campaign 002 for Anchor Cleaning.', active);
    assert.equal(r.classification, MESSAGE_CLASS.NEW_MISSION);
  });

  it('classifies resume', () => {
    const active = {
      id: 'msn_1',
      objectiveText: 'Build Campaign 001',
      title: 'Campaign 001',
      status: 'review_required',
    };
    const r = classifyMessage('Show progress', active);
    assert.equal(r.classification, MESSAGE_CLASS.RESUME);
  });
});

describe('SPEC-039 ActiveMissionResolver', () => {
  it('binds session on create and diagnoses without a new Mission', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-diagnose-1';

    const created = await resolver.resolve({
      sessionId,
      message: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(created.action, 'created');
    assert.equal(created.mission.type, MISSION_TYPES.CAMPAIGN_CREATION);
    const missionId = created.mission.id;

    const diagnosed = await resolver.resolve({
      sessionId,
      message: 'Investigate why Campaign Review failed',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(diagnosed.action, 'diagnosed');
    assert.equal(diagnosed.classification, MESSAGE_CLASS.DIAGNOSE);
    assert.equal(diagnosed.resolutionPath, RESOLUTION_PATHS.DIAGNOSE);
    assert.equal(diagnosed.mission.id, missionId);
    assert.match(diagnosed.diagnosis.summary, /Diagnosing active Mission/i);

    const list = await engine.list({ tenantId: '10' });
    assert.equal(list.length, 1, 'must not create a second Mission');

    const audit = await engine.listAudit(missionId);
    assert.ok(audit.some((e) => e.kind === AUDIT_KINDS.MESSAGE));
    assert.ok(audit.some((e) => e.kind === AUDIT_KINDS.DIAGNOSED));
  });

  it('resumes active Mission on follow-up', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-resume-1';

    const created = await resolver.resolve({
      sessionId,
      message: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    const missionId = created.mission.id;

    const resumed = await resolver.resolve({
      sessionId,
      message: 'Show progress',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(resumed.action, 'resumed');
    assert.equal(resumed.mission.id, missionId);
    const list = await engine.list({ tenantId: '10' });
    assert.equal(list.length, 1);
  });

  it('explicit New Mission creates a second Mission and rebinds', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-new-1';

    await resolver.resolve({
      sessionId,
      message: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    const second = await resolver.resolve({
      sessionId,
      message: 'New Mission. Build Campaign 002 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(second.action, 'created');
    assert.equal(second.mission.type, MISSION_TYPES.CAMPAIGN_CREATION);
    assert.match(second.mission.title, /002/);
    const list = await engine.list({ tenantId: '10' });
    assert.equal(list.length, 2);
    const active = await resolver.resolveActiveMission(sessionId);
    assert.equal(active.id, second.mission.id);
  });

  it('modifies constraints on the same Mission', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-mod-1';

    const created = await resolver.resolve({
      sessionId,
      message: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    const missionId = created.mission.id;

    const modified = await resolver.resolve({
      sessionId,
      message: 'Increase target count to 75',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(modified.action, 'modified');
    assert.equal(modified.mission.id, missionId);
    assert.equal(modified.mission.constraints.targetCount, 75);
    const list = await engine.list({ tenantId: '10' });
    assert.equal(list.length, 1);
  });
});

describe('SPEC-039 WorkspaceEngine Active Mission precedence', () => {
  it('follow-up diagnose does not spawn campaign_review Mission', async () => {
    const missionEngine = testEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    assert.equal(first.route, 'mission');
    assert.ok(first.mission);
    assert.equal(first.mission.type, MISSION_TYPES.CAMPAIGN_CREATION);

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Investigate why Campaign Review failed',
    });
    assert.equal(second.route, 'mission');
    assert.equal(second.mission.id, first.mission.id);
    assert.equal(second.resolution.action, 'diagnosed');
    assert.doesNotMatch(
      second.prose || second.structured.answer,
      /Mission created/i
    );
    assert.match(
      second.prose || second.structured.answer,
      /Diagnosing active Mission/i
    );

    const list = await missionEngine.list({ tenantId: '10' });
    assert.equal(list.length, 1);
    assert.equal(list[0].type, MISSION_TYPES.CAMPAIGN_CREATION);
  });

  it('resolver disabled falls back to create-on-intent', async () => {
    const missionEngine = testEngine({ resolverEnabled: false });
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: false,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Investigate why Campaign Review failed',
    });
    assert.equal(second.mission.type, MISSION_TYPES.CAMPAIGN_REVIEW);
    const list = await missionEngine.list({ tenantId: '10' });
    assert.equal(list.length, 2);
  });
});
