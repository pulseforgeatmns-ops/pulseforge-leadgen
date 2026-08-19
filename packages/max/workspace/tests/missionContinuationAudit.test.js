'use strict';

/**
 * AUDIT-002 — Mission Continuation & Delegation.
 * Observational: logs prove where progression stops. Does not change routing.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { createAcquisitionMissionEngine } = require('../../../acquisition-mission');
const {
  looksLikeAcquisitionMissionQuestion,
} = require('../AcquisitionMissionTurn');
const { classifyCognitiveMode } = require('../../specialistDelegation/CognitiveMode');
const { looksLikeAcquisitionQuestion } = require('../../scoutAcquisition/NeedAssessment');
const { selectExecutionDomain, EXECUTION_DOMAINS } = require('../ExecutionDomain');
const {
  CHECKPOINTS,
  PIPELINES,
  INTENTS,
  createAuditLog,
  classifyOperatorIntent,
  evaluateContinuation,
  loadActiveAcquisitionMission,
  evaluateWorkspaceMissionContinuation,
  inferActualPipeline,
} = require('../MissionContinuationAudit');
const {
  createMission,
  resetEngine,
  getEngine,
  attachScoutDiscovery,
  progressMission,
} = require('../../../../services/acquisitionMission');

const APPROVAL = 'Approved. Begin the mission...';

function kind(records, name) {
  return records.filter((row) => row.kind === name);
}

describe('AUDIT-002 operator intent classification', () => {
  it('classifies Approved. Begin the mission as Operator Approval, not a general question', () => {
    const classified = classifyOperatorIntent(APPROVAL, { id: 'mission_1', stage: 'discover' });
    assert.equal(classified.intent, INTENTS.OPERATOR_APPROVAL);
    assert.ok(classified.confidence >= 0.9);
    assert.equal(classified.reasoning, 'operator_approved_and_requested_mission_start');
  });

  it('does not match the SPEC-118 Max Ask question gate', () => {
    assert.equal(looksLikeAcquisitionMissionQuestion(APPROVAL), false);
  });

  it('is not classified as cognitive-mode execution', () => {
    const mode = classifyCognitiveMode(APPROVAL);
    assert.notEqual(mode.kind, 'execution');
  });
});

describe('AUDIT-002 continuation evaluation', () => {
  it('selects Mission Engine and Scout when an active discover Mission exists', () => {
    const mission = {
      id: 'mission_481',
      stage: 'discover',
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
    };
    const decision = evaluateContinuation(APPROVAL, mission);
    assert.equal(decision.continueMission, true);
    assert.equal(decision.pipelineSelected, PIPELINES.MISSION_ENGINE);
    assert.equal(decision.capability, 'scout');
    assert.equal(decision.classified.intent, INTENTS.OPERATOR_APPROVAL);
  });

  it('bypasses only on explicit new-objective evidence', () => {
    const mission = { id: 'mission_481', stage: 'discover' };
    const decision = evaluateContinuation('New mission. Start over.', mission);
    assert.equal(decision.continueMission, false);
    assert.equal(decision.pipelineSelected, PIPELINES.GENERAL_REASONING);
    assert.equal(decision.reason, 'explicit_new_objective');
  });

  it('selects General Reasoning when no Mission is loaded', () => {
    const decision = evaluateContinuation(APPROVAL, null);
    assert.equal(decision.continueMission, false);
    assert.equal(decision.pipelineSelected, PIPELINES.GENERAL_REASONING);
    assert.equal(decision.classified.intent, INTENTS.OPERATOR_APPROVAL);
    assert.equal(decision.reason, 'approval_detected_but_no_active_mission');
  });
});

describe('AUDIT-002 Step 1 — Mission persistence', () => {
  it('creates a Mission with id, persisted stage, and MISSION_CREATED log', async () => {
    resetEngine();
    const log = createAuditLog({ console: false });
    const mission = await createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
      campaign: 'Fall Outreach',
      owner: 'Operator',
    }, { persist: false, auditLog: log });

    assert.ok(mission.id);
    assert.equal(mission.stage, 'discover');
    assert.ok(mission.createdAt);
    assert.equal(mission.tenantId, '10');
    assert.equal(mission.owner, 'Operator');
    const stored = getEngine().get(mission.id, '10');
    assert.equal(stored.id, mission.id);
    assert.equal(stored.stage, 'discover');

    const created = kind(log.records, CHECKPOINTS.MISSION_CREATED);
    assert.equal(created.length, 1);
    assert.equal(created[0].missionId, mission.id);
    assert.equal(created[0].stage, 'discover');
    assert.equal(created[0].workspace, '10');
    assert.equal(created[0].operator, 'Operator');
    assert.ok(created[0].timestamp);
    assert.equal(created[0].outcome, 'created');
  });
});

describe('AUDIT-002 Step 2 — Mission retrieval', () => {
  it('loads the active Mission by id and logs MISSION_LOADED', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
    });
    const log = createAuditLog({ console: false });
    const loaded = await loadActiveAcquisitionMission({
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: engine,
      persist: false,
      log,
    });
    assert.ok(loaded.mission);
    assert.equal(loaded.mission.id, mission.id);
    assert.equal(loaded.mission.stage, 'discover');
    assert.equal(loaded.source, 'explicit_id');

    const evaluated = await evaluateWorkspaceMissionContinuation({
      question: APPROVAL,
      context: { tenantId: '10', missionId: mission.id },
      session: { id: 'sess-audit-002', operator: 'Operator', context: { tenantId: '10' } },
      acquisitionMissionEngine: engine,
      persist: false,
      log,
    });
    assert.equal(evaluated.snapshot.activeMissionFound, true);
    assert.equal(evaluated.snapshot.missionId, mission.id);
    assert.equal(evaluated.snapshot.stage, 'discover');
    assert.equal(kind(log.records, CHECKPOINTS.ACTIVE_MISSION_FOUND).length, 1);
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_LOADED)[0].missionId, mission.id);
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_STAGE)[0].stage, 'discover');
  });

  it('logs not_found when the store has no Mission', async () => {
    const engine = createAcquisitionMissionEngine();
    const log = createAuditLog({ console: false });
    const evaluated = await evaluateWorkspaceMissionContinuation({
      question: APPROVAL,
      context: { tenantId: '10' },
      session: { id: 'sess-empty', context: { tenantId: '10' } },
      acquisitionMissionEngine: engine,
      persist: false,
      log,
    });
    assert.equal(evaluated.snapshot.activeMissionFound, false);
    assert.equal(evaluated.snapshot.loadReason, 'no_mission_in_store');
    assert.equal(kind(log.records, CHECKPOINTS.ACTIVE_MISSION_FOUND)[0].outcome, 'not_found');
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_LOADED).length, 0);
  });
});

describe('AUDIT-002 Steps 3–8 — approval, transition, Scout, response', () => {
  it('records approval and recommended Scout, while ask() gates skip Mission/Scout', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
      campaign: 'Fall Outreach',
      owner: 'Operator',
    });
    const log = createAuditLog({ console: false });
    const session = {
      id: 'sess-audit-002',
      operator: 'Operator',
      context: { tenantId: '10', missionId: mission.id },
    };
    const evaluated = await evaluateWorkspaceMissionContinuation({
      question: APPROVAL,
      session,
      context: { tenantId: '10', missionId: mission.id, page: 'command-deck' },
      acquisitionMissionEngine: engine,
      persist: false,
      log,
    });

    const continuation = evaluated.snapshot;
    assert.equal(continuation.activeMissionFound, true);
    assert.equal(continuation.missionId, mission.id);
    assert.equal(continuation.stage, 'discover');
    assert.equal(continuation.continuationEvaluated, true);
    assert.equal(continuation.continueMission, true);
    assert.equal(continuation.recommendedPipeline, PIPELINES.MISSION_ENGINE);
    assert.equal(continuation.selectedCapability, 'scout');
    assert.equal(continuation.intent, INTENTS.OPERATOR_APPROVAL);
    assert.ok(continuation.confidence >= 0.9);
    assert.equal(continuation.intentReasoning, 'operator_approved_and_requested_mission_start');
    assert.equal(continuation.previousStage, 'discover');
    assert.equal(continuation.nextStage, 'understand');
    assert.equal(continuation.transitionReason, 'operator_approval');
    assert.ok(continuation.delegationPayload);
    assert.equal(continuation.delegationPayload.capability, 'scout');
    assert.equal(continuation.delegationPayload.missionId, mission.id);

    assert.equal(looksLikeAcquisitionMissionQuestion(APPROVAL), false);
    assert.notEqual(classifyCognitiveMode(APPROVAL).kind, 'execution');
    assert.equal(
      looksLikeAcquisitionQuestion(APPROVAL, { tenantId: '10' }),
      false
    );
    const domain = selectExecutionDomain(APPROVAL);
    assert.notEqual(domain.domain, EXECUTION_DOMAINS.MISSION_EXECUTION);
    assert.equal(domain.routeKind, 'intelligence');

    const simulatedAsk = {
      route: 'intelligence',
      mission: null,
      scoutLoop: undefined,
      domainDecision: { reason: domain.reason, domain: domain.domain },
    };
    assert.equal(inferActualPipeline(simulatedAsk), PIPELINES.GENERAL_REASONING);
    assert.equal(engine.get(mission.id, '10').stage, 'discover', 'stage did not advance');

    const approval = kind(log.records, CHECKPOINTS.MISSION_APPROVAL);
    assert.equal(approval[0].intent, INTENTS.OPERATOR_APPROVAL);
    assert.equal(approval[0].outcome, 'operator_approval');
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_TRANSITION).length, 0);
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_DELEGATE).length, 0);

    const { attachAskObservation } = require('../MissionContinuationAudit');
    const observed = attachAskObservation(evaluated, {
      route: 'intelligence',
      mission: null,
      context: session.context,
      domainDecision: { reason: domain.reason },
    });
    assert.equal(observed.context.missionContinuation.actualPipeline, PIPELINES.GENERAL_REASONING);
    assert.equal(observed.context.missionContinuation.scoutInvoked, false);
    assert.equal(observed.context.missionContinuation.responseFromMission, false);
    const delegate = kind(log.records, CHECKPOINTS.MISSION_DELEGATE);
    assert.equal(delegate[0].outcome, 'not_attempted');
    assert.equal(kind(log.records, CHECKPOINTS.MISSION_RESPONSE)[0].composedFrom, 'fresh_reasoning');
  });

  it('does not attach Scout results when Scout is never invoked', async () => {
    resetEngine();
    const log = createAuditLog({ console: false });
    const mission = await createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
    }, { persist: false, auditLog: log });
    const attached = await attachScoutDiscovery(
      { tenantId: '10' },
      { payload: { companies: [{ id: 1 }], prospects: [{ id: 2 }], evidence: ['places'] } },
      { persist: false, auditLog: log }
    );
    assert.equal(attached, null);
    const results = kind(log.records, CHECKPOINTS.MISSION_RESULT);
    assert.equal(results[0].attached, false);
    assert.equal(engineStillDiscover(mission.id), 'discover');
  });
});

function engineStillDiscover(id) {
  return getEngine().get(id, '10').stage;
}

describe('AUDIT-002 source order', () => {
  it('evaluates continuation before classifyCognitiveMode and retrieval', () => {
    const engineSrc = fs.readFileSync(
      path.join(__dirname, '..', 'WorkspaceEngine.js'),
      'utf8'
    );
    const auditAt = engineSrc.indexOf(
      'const missionAuditEval = await evaluateWorkspaceMissionContinuation'
    );
    const cognitiveAt = engineSrc.indexOf('const cognitive = classifyCognitiveMode(question');
    const retrieveAt = engineSrc.indexOf(
      'const retrievalTurn = await maybeHandleRetrievalBeforeDelegationTurn'
    );
    const scoutAt = engineSrc.indexOf('await maybeHandleScoutAcquisitionTurn');
    assert.ok(auditAt > 0);
    assert.ok(cognitiveAt > auditAt);
    assert.ok(retrieveAt > auditAt);
    assert.ok(scoutAt > retrieveAt);
  });
});

describe('AUDIT-002 transition log when Max actually progresses', () => {
  it('emits MISSION_TRANSITION only when progressMission is called', async () => {
    resetEngine();
    const log = createAuditLog({ console: false });
    const mission = await createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
    }, { persist: false, auditLog: log });
    await attachScoutDiscovery(
      { tenantId: '10', missionId: mission.id },
      {
        payload: {
          companies: [{ id: 1, name: 'Harbor Law' }],
          prospects: [{ id: 9 }],
          evidence: ['places'],
          confidence: 0.7,
        },
      },
      { persist: false, auditLog: log }
    );
    const advanced = await progressMission(
      mission.id,
      { role: 'max' },
      { stage: 'understand', reason: 'operator_approval' },
      { persist: false, tenantId: '10', auditLog: log }
    );
    assert.equal(advanced.stage, 'understand');
    const rows = kind(log.records, CHECKPOINTS.MISSION_TRANSITION);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].previousStage, 'discover');
    assert.equal(rows[0].nextStage, 'understand');
    assert.equal(rows[0].transitionReason, 'operator_approval');
    const results = kind(log.records, CHECKPOINTS.MISSION_RESULT);
    assert.equal(results[0].attached, true);
    assert.ok(results[0].prospectCount >= 1);
  });
});
