'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  classifyMessage,
  createMissionEngine,
  MESSAGE_CLASS,
  routeIntent,
  ROUTE_KINDS,
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

  it('classifies preparation-only canary correction as clarification, not resume', () => {
    const active = {
      id: 'msn_1',
      objectiveText: 'Direct Mail Execution — Campaign 001',
      title: 'Direct Mail Execution — Campaign 001',
      type: MISSION_TYPES.DIRECT_MAIL_EXECUTION,
      status: 'waiting',
    };
    const r = classifyMessage(
      'Please retry the canary as preparation only, not execution.',
      active
    );
    assert.equal(r.classification, MESSAGE_CLASS.CLARIFY);
    assert.equal(r.reason, 'execution_negated');
  });
});

describe('SPEC-055 canary routing constraints', () => {
  it('routes review-only canary package preparation away from Direct Mail Execution', () => {
    const routed = routeIntent(
      'We are not launching the full direct mail campaign yet. I want to run a canary batch first. Prepare a small Campaign 001 canary package for review only.'
    );
    assert.equal(routed.kind, ROUTE_KINDS.MISSION);
    assert.equal(routed.missionType, MISSION_TYPES.MAIL_PACKAGE_GENERATION);
    assert.notEqual(routed.missionType, MISSION_TYPES.DIRECT_MAIL_EXECUTION);
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

  it('does not resume a failed execution mission when operator says preparation only', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-clarify-1';

    const created = await resolver.resolve({
      sessionId,
      message: 'Execute Campaign 001 direct mail.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(created.action, 'created');
    assert.equal(created.mission.type, MISSION_TYPES.DIRECT_MAIL_EXECUTION);

    const clarified = await resolver.resolve({
      sessionId,
      message: 'Do not jump into Direct Mail Execution. Retry this as a canary package for preparation only, not execution.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(clarified.action, 'clarified');
    assert.equal(clarified.classification, MESSAGE_CLASS.CLARIFY);
    assert.equal(clarified.resolutionPath, RESOLUTION_PATHS.CLARIFY);
    assert.equal(clarified.mission.id, created.mission.id);
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

  it('answers execution-negated follow-up conversationally instead of exposing resolver internals first', async () => {
    const missionEngine = testEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Execute Campaign 001 direct mail.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    assert.equal(first.mission.type, MISSION_TYPES.DIRECT_MAIL_EXECUTION);

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question:
        'Do not jump into Direct Mail Execution. Retry this as a canary package for preparation only, not execution.',
    });

    assert.equal(second.resolution.action, 'clarified');
    const text = second.prose || second.structured.answer;
    assert.match(text, /I will not resume Direct Mail Execution/i);
    assert.match(text, /preparation\/review-only canary/i);
    assert.doesNotMatch(text, /Active Mission Resolver/i);
    assert.doesNotMatch(text, /IntentRouter not used/i);
  });
});
