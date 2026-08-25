'use strict';

/**
 * ADR-077 — Decisions Must Be Executable.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  evaluatePrioritizationReadiness,
  canApprovePrioritization,
  buildPostDiscoveryPendingDecision,
  buildDecisionReadiness,
  rollbackStageLabel,
} = require('../DecisionReadiness');
const {
  hasPendingPrioritizationApproval,
  hasPendingDiscoveryInvestigation,
  presentableOperatorDecision,
  assertMissionStateConsistent,
} = require('../PendingOperatorDecision');
const { presentationFromDiscoveryPayload } = require('../DiscoveryPresentation');
const { formatRollbackProse } = require('../ExecutionErrors');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const COMPLETE_PRESENTATION = presentationFromDiscoveryPayload({
  rankedProspects: [{ rank: 1, name: 'Harbor Law Group' }],
  qualifiedCount: 1,
  summary: '1 qualified prospect with attributable signals.',
  buyingSignals: [{ label: 'Hiring operations manager', type: 'hiring' }],
  evidence: [{ label: 'Careers page posting', source: 'Company website' }],
  discoveryStatus: 'complete',
  coverage: {
    cities: { searched: 6, planned: 6 },
    concepts: { searched: 6, planned: 6 },
    sources: { searched: 1, planned: 1 },
    complete: true,
    warnings: [],
  },
});

const INCOMPLETE_PRESENTATION = presentationFromDiscoveryPayload({
  rankedProspects: [{ rank: 1, name: 'Test Co' }],
  qualifiedCount: 1,
  summary: 'Found one prospect.',
  buyingSignals: [{ label: 'Hiring manager', type: 'hiring' }],
  evidence: [{ label: 'Website', source: 'Company website' }],
  discoveryStatus: 'incomplete',
  coverage: {
    cities: { searched: 1, planned: 6 },
    concepts: { searched: 2, planned: 6 },
    sources: { searched: 1, planned: 1 },
    complete: false,
    warnings: ['Only 1 / 6 cities searched.', 'Discovery incomplete.'],
  },
  discoveryPlan: {
    missingSources: ['Airbnb', 'VRBO', 'Business Directories'],
  },
});

describe('ADR-077 — Decisions Must Be Executable', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
  });

  it('Scenario 1: coverage complete exposes Approve findings', () => {
    const pending = buildPostDiscoveryPendingDecision(COMPLETE_PRESENTATION);
    assert.equal(pending.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
    assert.match(pending.prompt, /Approve findings/i);
    assert.equal(pending.readiness.executable, true);
  });

  it('Scenario 2: coverage incomplete exposes Continue Investigation, not Approve findings', () => {
    const pending = buildPostDiscoveryPendingDecision(INCOMPLETE_PRESENTATION);
    assert.equal(pending.kind, OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION);
    assert.doesNotMatch(pending.prompt, /Approve findings/i);
    assert.ok((pending.actions || []).includes('Continue Investigation'));
    assert.ok((pending.missingEvidence || []).length > 0);
    assert.equal(pending.readiness.executable, false);
  });

  it('Scenario 3: displayed prioritization approval executes without precondition rollback', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    const discovery = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const snapshot = discovery.snapshot;
    assert.equal(hasPendingPrioritizationApproval(snapshot), true);
    assert.equal(snapshot.executableDecision.prompt, 'Approve findings?');

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approve findings.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });

    assert.equal(turn.action, 'prioritization_approved');
    assert.doesNotMatch(turn.prose, /could not execute/i);
    assert.equal(engine.get(mission.id, '10').stage, STAGES.UNDERSTAND);
  });

  it('Scenario 3b: incomplete coverage mission advertises investigation, not prioritization approval', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    engine.contribute(mission.id, {
      specialist: 'operator',
      kind: 'approval',
      payload: {
        approved: true,
        consumed: true,
        action: 'discovery_approved',
        stage: STAGES.DISCOVER,
      },
    }, { tenantId: '10' });

    engine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: INCOMPLETE_PRESENTATION,
    }, { tenantId: '10' });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryInvestigation(snapshot), true);
    assert.equal(hasPendingPrioritizationApproval(snapshot), false);
    assert.doesNotMatch(snapshot.executableDecision.prompt, /Approve findings/i);
    assert.ok((snapshot.executableDecision.actions || []).includes('Continue Investigation'));
    assertMissionStateConsistent(snapshot.mission, { contributions: snapshot.contributions });
  });

  it('Scenario 4: rollback prose identifies prioritization transaction', () => {
    assert.equal(rollbackStageLabel('prioritization_approved'), 'Prioritization');
    const prose = formatRollbackProse('Prioritization', {
      rollbackReason: 'Discovery evidence is insufficient.',
      message: 'Insufficient evidence for prioritization approval.',
    });
    assert.match(prose, /Prioritization could not execute/i);
    assert.match(prose, /Discovery evidence is insufficient/i);
    assert.match(prose, /Continue investigation/i);
    assert.doesNotMatch(prose, /Discovery could not execute/i);
  });

  it('Scenario 5: UI presentation and readiness contract agree', () => {
    const snapshot = {
      mission: {
        stage: STAGES.DISCOVER,
        structuredMissionApproved: true,
        pendingOperatorDecision: buildPostDiscoveryPendingDecision(INCOMPLETE_PRESENTATION),
      },
      contributions: [{
        specialist: 'scout',
        kind: 'discovery',
        payload: INCOMPLETE_PRESENTATION,
      }],
      discoveryArtifact: INCOMPLETE_PRESENTATION,
    };

    const readiness = buildDecisionReadiness(snapshot);
    const presented = presentableOperatorDecision(snapshot);
    assert.equal(readiness.executable, false);
    assert.equal(presented.kind, OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION);
    assert.equal(canApprovePrioritization(snapshot).executable, false);
  });

  it('rejects inconsistent prioritization approval when evidence is insufficient', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    engine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: INCOMPLETE_PRESENTATION,
    }, { tenantId: '10' });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    snapshot.mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
      prompt: 'Approve findings?',
    };

    assert.throws(
      () => assertMissionStateConsistent(snapshot.mission, { contributions: snapshot.contributions }),
      (err) => err.code === 'MISSION_STATE_INCONSISTENT'
        && (/insufficient|cannot consume/i.test(err.message))
    );
  });

  it('evaluatePrioritizationReadiness reports missing coverage details', () => {
    const readiness = evaluatePrioritizationReadiness(INCOMPLETE_PRESENTATION);
    assert.equal(readiness.executable, false);
    assert.match(readiness.blockingReason, /incomplete/i);
    assert.equal(readiness.recommendedAction, 'Continue investigation.');
    assert.ok(readiness.missingEvidence.length > 0);
    assert.ok(readiness.coveragePercent != null);
  });
});
