'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  classifyMessage,
  createMissionEngine,
  MESSAGE_CLASS,
  RESOLUTION_PATHS,
  MISSION_TYPES,
  AUDIT_KINDS,
  resolveCurrentStage,
  executeCurrentStage,
  selectExecutorForStage,
  EXECUTOR_IDS,
  clearMissionStageAuditLog,
  listMissionStageAuditLog,
} = require('..');
const { createBuiltinRegistry } = require('../../capabilities');
const { createWorkspaceEngine } = require('../../max/workspace');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

describe('AUDIT-003 classifyMessage EXECUTE_STAGE', () => {
  const active = {
    id: 'msn_audit_1',
    objectiveText: 'Build Campaign 001 for Anchor Cleaning.',
    title: 'Campaign 001',
    status: 'planning',
    progress: { currentStage: 'Discovery' },
  };

  it('classifies Approved. Begin Scout discovery. as execute_stage', () => {
    const r = classifyMessage('Approved. Begin Scout discovery.', active);
    assert.equal(r.classification, MESSAGE_CLASS.EXECUTE_STAGE);
    assert.equal(r.reason, 'execute_stage_pattern');
  });

  it('classifies begin scout discovery as execute_stage', () => {
    const r = classifyMessage('Begin scout discovery', active);
    assert.equal(r.classification, MESSAGE_CLASS.EXECUTE_STAGE);
  });

  it('does not classify show progress as execute_stage', () => {
    const r = classifyMessage('Show progress', active);
    assert.equal(r.classification, MESSAGE_CLASS.RESUME);
  });
});

describe('AUDIT-003 stage resolution and executor selection', () => {
  it('resolves Discovery from queued prospect_discovery step', () => {
    const mission = {
      id: 'msn_1',
      status: 'planning',
      plan: {
        steps: [
          {
            stageId: 'prospect_discovery',
            capabilityId: 'prospect_discovery',
            name: 'Discovery',
            stageLabel: 'Discovery',
            status: 'queued',
          },
        ],
      },
    };
    const stage = resolveCurrentStage(mission);
    assert.equal(stage.stageId, 'prospect_discovery');
    assert.equal(stage.stageName, 'Discovery');
    assert.ok(stage.confidence >= 0.85);

    const sel = selectExecutorForStage(stage);
    assert.equal(sel.executorId, EXECUTOR_IDS.SCOUT_DISCOVERY);
    assert.match(sel.selectionReason, /stage_registry/);
  });

  it('falls back to RecommendationEngine when stage is unregistered', () => {
    const stage = {
      stageId: 'campaign_builder',
      stageName: 'Campaign Builder',
      capabilityId: 'campaign_builder',
    };
    const sel = selectExecutorForStage(stage);
    assert.equal(sel.executorId, null);
    assert.equal(sel.selectionReason, 'no_executor_registered');
  });
});

describe('AUDIT-003 stage execution instrumentation', () => {
  beforeEach(() => clearMissionStageAuditLog());

  it('emits MISSION_STAGE through MISSION_EXECUTOR_RESULT on Discovery execute', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    clearMissionStageAuditLog();

    const execution = await executeCurrentStage({
      mission,
      missionEngine: engine,
      operatorId: 'operator-1',
      message: 'Approved. Begin Scout discovery.',
    });

    assert.equal(execution.executorId, EXECUTOR_IDS.SCOUT_DISCOVERY);
    assert.equal(execution.fallback, false);
    assert.equal(execution.result.success, true);

    const logs = listMissionStageAuditLog();
    const events = logs.map((l) => l.event);
    assert.ok(events.includes('MISSION_STAGE'));
    assert.ok(events.includes('MISSION_EXECUTOR_SELECTED'));
    assert.ok(events.includes('MISSION_EXECUTOR_INVOKED'));
    assert.ok(events.includes('MISSION_EXECUTOR_RESULT'));
    assert.equal(events.includes('MISSION_EXECUTOR_FALLBACK'), false);

    const selected = logs.find((l) => l.event === 'MISSION_EXECUTOR_SELECTED');
    assert.equal(selected.executor, EXECUTOR_IDS.SCOUT_DISCOVERY);

    const invoked = logs.find((l) => l.event === 'MISSION_EXECUTOR_INVOKED');
    assert.equal(invoked.missionId, mission.id);

    assert.ok(execution.result.scoutPayload);
    assert.equal(execution.result.scoutPayload.missionId, mission.id);
    assert.equal(execution.result.scoutPayload.approvalState, 'approved');
  });

  it('emits MISSION_EXECUTOR_FALLBACK for unregistered stage', async () => {
    clearMissionStageAuditLog();
    const mission = {
      id: 'msn_fallback',
      status: 'executing',
      objectiveText: 'Test',
      plan: {
        steps: [
          {
            stageId: 'campaign_builder',
            capabilityId: 'campaign_builder',
            name: 'Campaign Builder',
            status: 'queued',
          },
        ],
      },
    };

    const execution = await executeCurrentStage({
      mission,
      missionEngine: testEngine(),
      message: 'Execute stage',
    });

    assert.equal(execution.fallback, true);
    assert.equal(execution.result.executorId, EXECUTOR_IDS.RECOMMENDATION_ENGINE);

    const logs = listMissionStageAuditLog();
    assert.ok(logs.some((l) => l.event === 'MISSION_EXECUTOR_FALLBACK'));
    const fallback = logs.find((l) => l.event === 'MISSION_EXECUTOR_FALLBACK');
    assert.equal(fallback.selected, EXECUTOR_IDS.RECOMMENDATION_ENGINE);
    assert.equal(fallback.reason, 'no_executor_registered');
  });
});

describe('AUDIT-003 ActiveMissionResolver execute_stage', () => {
  beforeEach(() => clearMissionStageAuditLog());

  it('Approved. Begin Scout discovery invokes ScoutDiscoveryExecutor via resolver', async () => {
    const engine = testEngine();
    const resolver = engine.activeMissionResolver;
    const sessionId = 'sess-audit-scout';

    const created = await resolver.resolve({
      sessionId,
      message: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      constraints: {},
    });
    assert.equal(created.action, 'created');

    await engine.store.update({
      id: created.mission.id,
      status: 'planning',
      plan: created.mission.plan,
    });

    clearMissionStageAuditLog();

    const executed = await resolver.resolve({
      sessionId,
      message: 'Approved. Begin Scout discovery.',
      tenantId: '10',
      clientId: 10,
    });

    assert.equal(executed.action, 'executed');
    assert.equal(executed.classification, MESSAGE_CLASS.EXECUTE_STAGE);
    assert.equal(executed.resolutionPath, RESOLUTION_PATHS.EXECUTE_STAGE);
    assert.equal(executed.stageExecution.executorId, EXECUTOR_IDS.SCOUT_DISCOVERY);
    assert.equal(executed.stageExecution.fallback, false);

    const audit = await engine.listAudit(created.mission.id);
    assert.ok(audit.some((e) => e.kind === AUDIT_KINDS.STAGE_EXECUTED));

    const logs = listMissionStageAuditLog();
    assert.ok(logs.some((l) => l.event === 'MISSION_EXECUTOR_INVOKED'));
  });
});

describe('AUDIT-003 workspace integration', () => {
  it('Approved. Begin Scout discovery routes Mission Engine and executes Discovery', async () => {
    clearMissionStageAuditLog();
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

    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    clearMissionStageAuditLog();

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Approved. Begin Scout discovery.',
    });

    assert.equal(second.route, 'mission');
    assert.equal(second.resolution.action, 'executed');
    assert.equal(second.resolution.classification, MESSAGE_CLASS.EXECUTE_STAGE);
    assert.match(
      second.prose || second.structured.answer,
      /Scout discovery executed|Discovery/i
    );
    assert.doesNotMatch(
      second.prose || second.structured.answer,
      /Continuing with the active Mission \(no new Mission created\)/i
    );

    const logs = listMissionStageAuditLog();
    assert.ok(logs.some((l) => l.event === 'MISSION_EXECUTOR_SELECTED'));
    assert.equal(
      logs.find((l) => l.event === 'MISSION_EXECUTOR_SELECTED').executor,
      EXECUTOR_IDS.SCOUT_DISCOVERY
    );
  });
});
