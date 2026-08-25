'use strict';

/**
 * SPEC-153 — Mission Workspace Modes.
 * Acceptance tests: exactly one workspace mode active; creation and inspection never simultaneous.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const amo = require('../index');
const {
  WORKSPACE_MODES,
  RENDER_MODES,
  COMPONENTS,
  deriveWorkspaceMode,
  deriveRenderMode,
  isComponentVisible,
  buildWorkspaceContext,
  PROGRESSION_STAGES,
  STAGES,
  SPECIALISTS,
  OPERATOR_DECISION_KINDS,
  runAutonomousProgression,
} = amo;

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function advanceMissionToExecution(amoEngine, missionId) {
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.SCOUT,
    kind: 'discovery',
    payload: {
      companies: [{ id: 1, name: 'Harbor Law' }],
      prospects: [{ id: 1, name: 'Jordan', title: 'Office Manager' }],
      evidence: ['Google Places'],
      qualifiedCount: 5,
      confidence: 0.8,
    },
  });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.UNDERSTAND });
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.MAX,
    kind: 'prioritization',
    payload: {
      priorities: [{ segment: 'law_firm', rank: 1 }],
      recommendations: ['Prioritize ops hires'],
    },
  });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.PLAN });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.PREPARE });
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.PAIGE,
    kind: 'variants',
    payload: { variants: [{ label: 'A' }], subjects: ['Walkthrough'] },
  });
  amoEngine.contribute(missionId, {
    specialist: SPECIALISTS.EMMETT,
    kind: 'capacity',
    payload: { capacity: { remaining: 18 }, health: { status: 'healthy' } },
  });
  amoEngine.progress(missionId, { role: 'max' }, { stage: STAGES.READY });
}

describe('SPEC-153 — Mission Workspace Modes', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('Test 1: no mission selected → CREATE mode', () => {
    assert.equal(deriveWorkspaceMode({}), WORKSPACE_MODES.CREATE);
    assert.equal(deriveWorkspaceMode({ missionId: null, snapshot: null }), WORKSPACE_MODES.CREATE);
    assert.equal(deriveRenderMode(WORKSPACE_MODES.CREATE), RENDER_MODES.CREATE);
    assert.ok(isComponentVisible('missionCreator', WORKSPACE_MODES.CREATE));
    assert.ok(!isComponentVisible('missionTimeline', WORKSPACE_MODES.CREATE));
    assert.ok(!isComponentVisible('executionWorkspace', WORKSPACE_MODES.CREATE));
  });

  it('Test 2: mission inspect → not CREATE; mission creator hidden', () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const ctx = buildWorkspaceContext({ missionId: mission.id, snapshot });
    assert.notEqual(ctx.workspaceMode, WORKSPACE_MODES.CREATE);
    assert.equal(ctx.renderMode, RENDER_MODES.INSPECT);
    assert.ok(!isComponentVisible('missionCreator', ctx.workspaceMode));
    assert.ok(isComponentVisible('missionTimeline', ctx.workspaceMode));
    assert.ok(isComponentVisible('missionWorkspace', ctx.workspaceMode));
    assert.ok(snapshot.workspaceContext);
    assert.equal(snapshot.workspaceContext.workspaceMode, ctx.workspaceMode);
  });

  it('Test 3: mission in execution → EXECUTION mode with execution controls', () => {
    advanceMissionToExecution(engine, mission.id);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const mode = deriveWorkspaceMode({ missionId: mission.id, snapshot });
    assert.equal(mode, WORKSPACE_MODES.EXECUTION);
    assert.equal(deriveRenderMode(mode), RENDER_MODES.EXECUTION);
    assert.ok(isComponentVisible('executionWorkspace', mode));
    assert.ok(!isComponentVisible('missionCreator', mode));
    assert.ok(isComponentVisible('operatorDecision', mode));
  });

  it('Test 4: mission in discovery review → REVIEW render mode with intelligence report', async () => {
    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });
    assert.equal(result.progressionStage, PROGRESSION_STAGES.DISCOVERY_REVIEW);

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const mode = deriveWorkspaceMode({ missionId: mission.id, snapshot });
    assert.equal(mode, WORKSPACE_MODES.DISCOVERY_REVIEW);
    assert.equal(deriveRenderMode(mode), RENDER_MODES.REVIEW);
    assert.ok(isComponentVisible('missionIntelligenceReport', mode));
    assert.ok(isComponentVisible('operatorDecision', mode));
    assert.ok(!isComponentVisible('missionCreator', mode));
    assert.ok(!isComponentVisible('executionWorkspace', mode));
  });

  it('runtime guarantee: mission creator and execution workspace never share a mode', () => {
    const createModes = COMPONENTS.missionCreator.supportedModes;
    const execModes = COMPONENTS.executionWorkspace.supportedModes;
    const overlap = createModes.filter((mode) => execModes.includes(mode));
    assert.deepEqual(overlap, []);
  });

  it('operator journey maps progression stages to workspace modes', () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(
      deriveWorkspaceMode({ missionId: mission.id, snapshot }),
      WORKSPACE_MODES.UNDERSTANDING
    );
    assert.equal(
      snapshot.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PLAN_APPROVAL
    );
  });

  it('HTML workspace uses mode-driven component contracts', () => {
    const ui = fs.readFileSync(
      path.join(__dirname, '../../../public/acquisition-missions.html'),
      'utf8'
    );
    assert.match(ui, /data-component="missionCreator"/);
    assert.match(ui, /data-component="executionWorkspace"/);
    assert.match(ui, /data-component="missionIntelligenceReport"/);
    assert.match(ui, /applyWorkspaceMode/);
    assert.match(ui, /Workspace Mode:/);
    assert.doesNotMatch(ui, /id="advanceBtn".*id="createBtn"/s);
    assert.match(ui, /executionPanel.*advanceBtn/s);
    assert.match(ui, /createPanel.*createBtn/s);
  });
});
