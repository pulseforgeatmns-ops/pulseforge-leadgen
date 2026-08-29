'use strict';

/**
 * SPEC-207 — Post-Prioritization Presentation Projection.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, SPECIALISTS, CONTRIBUTION_KINDS } = amo;
const {
  presentationFromPrioritizationPayload,
  formatPrioritizationResultsProse,
  resolvePrioritizationApprovedNextStep,
  findLatestPrioritizationContribution,
} = require('../../../acquisition-mission/PrioritizationPresentation');
const { buildExecutionMissionResponse } = require('../AcquisitionMissionExecution');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../AmoOperatorApproval');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function samplePrioritizationPayload() {
  return {
    priorities: [
      {
        rank: 1,
        name: 'Harbor Law Group',
        fit: 0.78,
        timing: 0.65,
        confidence: 0.74,
        rationale: 'Strong hiring signal and reachable office manager.',
      },
      {
        rank: 2,
        name: 'Granite Legal Partners',
        fit: 0.71,
        timing: 0.58,
        confidence: 0.68,
        rationale: 'Office coordinator hiring indicates receptivity window.',
      },
      {
        rank: 3,
        name: 'Summit Legal',
        fit: 0.66,
        timing: 0.52,
        confidence: 0.61,
        rationale: 'Qualified but weaker timing signal.',
      },
    ],
    objectives: [{ text: 'Book 3 walkthroughs with law firms in Manchester NH.' }],
    objectiveReason: 'Harbor Law Group leads on fit and timing for the first outreach wave.',
    recommendations: ['Prioritize Harbor Law Group in the first outreach wave.'],
    delegation: { paige: 'variants', emmett: 'capacity' },
    confidence: 0.72,
    evidence: [{ label: 'Operations manager job posting', source: 'job_board' }],
    buyingSignals: [{ label: 'Hiring operations manager', type: 'hiring' }],
  };
}

describe('SPEC-207 — Post-Prioritization Presentation Projection', () => {
  it('presentationFromPrioritizationPayload projects committed Max fields', () => {
    const presentation = presentationFromPrioritizationPayload(samplePrioritizationPayload());
    assert.equal(presentation.priorities.length, 3);
    assert.equal(presentation.priorities[0].name, 'Harbor Law Group');
    assert.match(presentation.objectiveReason, /Harbor Law Group/i);
    assert.equal(presentation.recommendations[0], 'Prioritize Harbor Law Group in the first outreach wave.');
    assert.deepEqual(presentation.delegation, { paige: 'variants', emmett: 'capacity' });
    assert.equal(presentation.confidence, 0.72);
  });

  it('formatPrioritizationResultsProse renders Max Prioritization artifact', () => {
    const prose = formatPrioritizationResultsProse(samplePrioritizationPayload());
    assert.match(prose, /Max Prioritization/i);
    assert.match(prose, /Top Priorities/i);
    assert.match(prose, /1\. Harbor Law Group/);
    assert.match(prose, /2\. Granite Legal Partners/);
    assert.match(prose, /Why these targets/i);
    assert.match(prose, /Recommended Next Action/i);
    assert.match(prose, /Delegation/i);
    assert.match(prose, /Paige: variants/i);
    assert.match(prose, /Emmett: capacity/i);
    assert.doesNotMatch(prose, /Scout Discovery/i);
  });

  it('findLatestPrioritizationContribution returns the newest Max row', () => {
    const contributions = [
      {
        id: 'max-old',
        specialist: SPECIALISTS.MAX,
        kind: CONTRIBUTION_KINDS.PRIORITIZATION,
        payload: { priorities: [{ rank: 1, name: 'Stale Target' }] },
      },
      {
        id: 'max-new',
        specialist: SPECIALISTS.MAX,
        kind: CONTRIBUTION_KINDS.PRIORITIZATION,
        payload: samplePrioritizationPayload(),
      },
    ];
    const row = findLatestPrioritizationContribution(contributions);
    assert.equal(row.id, 'max-new');
    assert.equal(row.payload.priorities[0].name, 'Harbor Law Group');
  });

  it('resolvePrioritizationApprovedNextStep points to outreach planning after understand', () => {
    const nextStep = resolvePrioritizationApprovedNextStep(
      {
        mission: { id: 'm-1', stage: STAGES.UNDERSTAND, pendingOperatorDecision: null },
        contributions: [
          {
            specialist: SPECIALISTS.MAX,
            kind: CONTRIBUTION_KINDS.PRIORITIZATION,
            payload: samplePrioritizationPayload(),
          },
        ],
      },
      { id: 'm-1', stage: STAGES.UNDERSTAND }
    );
    assert.equal(nextStep, 'Continue to outreach planning.');
  });

  it('buildExecutionMissionResponse renders Max prioritization after approval', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const before = engine.inspect(mission.id, { tenantId: '10' });
    const staleDiscovery = {
      specialist: SPECIALISTS.SCOUT,
      kind: CONTRIBUTION_KINDS.DISCOVERY,
      payload: {
        qualifiedCount: 1,
        rankedProspects: [{ rank: 1, name: 'Stale Prospect Only' }],
        summary: 'Stale first-pass discovery.',
      },
    };
    const snapshotWithStaleFirstDiscovery = {
      ...before,
      contributions: [staleDiscovery, ...(before.contributions || [])],
    };

    const prioritizationResult = await advancePrioritizationAfterApproval({
      engine,
      mission: before.mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });

    const response = buildExecutionMissionResponse({
      mission: prioritizationResult.snapshot.mission,
      snapshot: snapshotWithStaleFirstDiscovery,
      action: 'prioritization_approved',
      question: 'Approved prioritization.',
      executionResult: prioritizationResult,
    });

    assert.match(response.prose, /Max Prioritization/i);
    assert.match(response.prose, /Harbor Law Group/i);
    assert.match(response.prose, /Why these targets/i);
    assert.match(response.prose, /Continue to outreach planning/i);
    assert.doesNotMatch(response.prose, /Scout Discovery/i);
    assert.doesNotMatch(response.prose, /Stale Prospect Only/i);
    assert.doesNotMatch(response.prose, /Review mission workspace for Max prioritization/i);
    assert.equal(response.comm.operatorDecision, null);
    assert.ok(response.comm.prioritizationResults);
    assert.equal(response.comm.prioritizationResults.priorities[0].name, 'Harbor Law Group');
  });

  it('maybeHandleAcquisitionMissionExecution prioritization turn uses Max artifact', async () => {
    const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved prioritization.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });

    assert.equal(turn.action, 'prioritization_approved');
    assert.match(turn.prose, /Max Prioritization/i);
    assert.match(turn.prose, /Continue to outreach planning/i);
    assert.doesNotMatch(turn.prose, /Scout Discovery/i);
  });
});
