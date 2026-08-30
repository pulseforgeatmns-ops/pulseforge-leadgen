'use strict';

/**
 * SPEC-203 — Investigation Continuation Presentation Contract.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS, SPECIALISTS, CONTRIBUTION_KINDS } = amo;
const {
  presentationFromInvestigationContinuation,
  resolveInvestigationContinuationPayloads,
} = require('../../../acquisition-mission/InvestigationContinuationPresentation');
const { buildExecutionMissionResponse } = require('../AcquisitionMissionExecution');
const { TASK_STATUS } = require('../../../scout/investigation/CandidateInvestigation');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for short-term rental property managers.';

function anchorCandidate(id, name, qualificationStatus, extras = {}) {
  return {
    candidateId: id,
    candidate_id: id,
    name,
    qualification: {
      status: qualificationStatus,
      reason: extras.reason || null,
    },
    qualificationStatus,
    evidenceRefs: extras.evidenceRefs || [],
    ...extras,
  };
}

function priorDiscoveryPayload() {
  return {
    candidateUniverseCount: 24,
    qualifiedCount: 14,
    confidence: 0.72,
    discoveryStatus: 'complete',
    candidateUniverse: [
      anchorCandidate('candidate-blue-door', 'Blue Door Living', 'uncertain'),
      anchorCandidate('candidate-lot-202', 'Lot 202', 'uncertain'),
      anchorCandidate('candidate-mill-city', 'Mill City', 'uncertain'),
    ],
    evidence: [{ label: 'Google Places search', source: 'google_places' }],
    providerExecution: [{ providerId: 'google_places', succeeded: true, results: 24 }],
  };
}

function continuedDiscoveryPayload() {
  return {
    candidateUniverseCount: 24,
    qualifiedCount: 14,
    confidence: 0.74,
    discoveryStatus: 'complete',
    candidateUniverse: [
      anchorCandidate('candidate-blue-door', 'Blue Door Living', 'uncertain', {
        evidenceRefs: [
          {
            id: 'ev-blue-door-dm',
            label: 'Office manager contact page',
            snapshot: { companyName: 'Blue Door Living', source: 'website' },
            evidenceType: 'decision_maker',
          },
        ],
      }),
      anchorCandidate('candidate-lot-202', 'Lot 202', 'uncertain', {
        evidenceRefs: [
          {
            id: 'ev-lot-portfolio',
            label: 'Portfolio page lists managed units',
            snapshot: { companyName: 'Lot 202', source: 'website' },
            evidenceType: 'portfolio',
          },
        ],
      }),
      anchorCandidate('candidate-mill-city', 'Mill City', 'uncertain', {
        evidenceRefs: [
          {
            id: 'ev-mill-website',
            label: 'Company website confirms property management',
            snapshot: { companyName: 'Mill City', source: 'website' },
            evidenceType: 'website',
          },
        ],
      }),
    ],
    evidence: [
      { label: 'Google Places search', source: 'google_places' },
      {
        label: 'Office manager contact page',
        source: 'website',
        company: 'Blue Door Living',
        evidenceType: 'decision_maker',
      },
      {
        label: 'Portfolio page lists managed units',
        source: 'website',
        company: 'Lot 202',
        evidenceType: 'portfolio',
      },
      {
        label: 'Company website confirms property management',
        source: 'website',
        company: 'Mill City',
        evidenceType: 'website',
      },
    ],
    providerExecution: [
      { providerId: 'google_places', succeeded: true, results: 24 },
      { providerId: 'linkedin', succeeded: false, reason: 'LinkedIn unavailable' },
      { providerId: 'prospeo', succeeded: false, reason: 'Prospeo unavailable' },
    ],
    candidateInvestigation: {
      executedTasks: [
        {
          task: {
            candidateName: 'Blue Door Living',
            gap: 'decision_maker',
            evidenceType: 'decision_maker',
          },
        },
        {
          task: {
            candidateName: 'Lot 202',
            gap: 'portfolio_size',
            evidenceType: 'portfolio',
          },
        },
      ],
      queue: [
        { status: TASK_STATUS.BLOCKED, providers: [{ id: 'linkedin' }] },
        { status: TASK_STATUS.BLOCKED, providers: [{ id: 'prospeo' }] },
      ],
    },
  };
}

describe('SPEC-203 — Investigation Continuation Presentation', () => {
  it('projects committed before/after discovery payloads into operator-visible delta', () => {
    const presentation = presentationFromInvestigationContinuation({
      priorPayload: priorDiscoveryPayload(),
      currentPayload: continuedDiscoveryPayload(),
      mission: {
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
          prompt: 'Continue investigation?',
          reason: 'More evidence is still required before prioritization.',
        },
      },
    });

    assert.equal(presentation.candidateUniverseBefore, 24);
    assert.equal(presentation.candidateUniverseAfter, 24);
    assert.equal(presentation.qualifiedBefore, 14);
    assert.equal(presentation.qualifiedAfter, 14);
    assert.equal(presentation.confidenceBefore, 0.72);
    assert.equal(presentation.confidenceAfter, 0.74);
    assert.equal(presentation.qualificationChanges.newlyQualified.length, 0);
    assert.equal(presentation.qualificationChanges.disqualified.length, 0);
    assert.ok(presentation.evidenceAdded.some((line) => /Blue Door Living/i.test(line)));
    assert.ok(presentation.evidenceAdded.some((line) => /Lot 202/i.test(line)));
    assert.ok(presentation.evidenceAdded.some((line) => /Mill City/i.test(line)));
    assert.ok(presentation.blockedInvestigation.some((line) => /linkedin/i.test(line)));
    assert.ok(presentation.blockedInvestigation.some((line) => /prospeo/i.test(line)));
    assert.equal(presentation.operatorDecision, 'Continue investigation?');
    assert.match(presentation.nextStep, /More evidence is still required/i);
  });

  it('buildExecutionMissionResponse renders investigation delta instead of generic mission fallback', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'STR Property Managers',
    });

    const priorPayload = priorDiscoveryPayload();
    const currentPayload = continuedDiscoveryPayload();
    const executionResult = {
      alreadyExecuted: false,
      investigationContinuation: true,
      executionOutcome: 'completed',
      discovery: { payload: currentPayload },
    };

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    snapshot.contributions = [
      {
        id: 'disc-prior',
        specialist: SPECIALISTS.SCOUT,
        kind: CONTRIBUTION_KINDS.DISCOVERY,
        payload: priorPayload,
      },
      {
        id: 'disc-current',
        specialist: SPECIALISTS.SCOUT,
        kind: CONTRIBUTION_KINDS.DISCOVERY,
        payload: currentPayload,
      },
    ];
    snapshot.mission.stage = STAGES.DISCOVER;
    snapshot.mission.pendingOperatorDecision = {
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
      prompt: 'Continue investigation?',
      reason: 'More evidence is still required before prioritization.',
    };

    const response = buildExecutionMissionResponse({
      mission: snapshot.mission,
      snapshot,
      action: 'discovery_investigation_continued',
      question: 'continue the investigation please',
      executionResult,
    });

    assert.equal(response.comm.headline, 'Investigation Continued');
    assert.ok(response.comm.investigationContinuationResults);
    assert.match(response.prose, /Investigation Continued/i);
    assert.match(response.prose, /24 → 24/);
    assert.match(response.prose, /14 → 14/);
    assert.match(response.prose, /Blue Door Living/i);
    assert.match(response.prose, /Lot 202/i);
    assert.match(response.prose, /Mill City/i);
    assert.match(response.prose, /0 newly qualified/i);
    assert.match(response.prose, /0 disqualified/i);
    assert.match(response.prose, /LinkedIn unavailable/i);
    assert.match(response.prose, /Prospeo unavailable/i);
    assert.match(response.prose, /Continue investigation\?/);
    assert.doesNotMatch(response.prose, /^Mission Updated/m);
    assert.doesNotMatch(response.prose, /Continue in mission workspace/i);
    assert.doesNotMatch(response.prose, /Active mission — Discovering/i);
  });

  it('buildExecutionMissionResponse renders canonical READY execution review instead of generic fallback', () => {
    const mission = {
      id: 'm-ready-1',
      title: 'Ready execution review',
      objective: OBJECTIVE,
      stage: STAGES.READY,
      pendingOperatorDecision: {
        kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
        prompt: 'Authorize external execution of this prepared outreach?',
      },
    };

    const snapshot = {
      mission,
      contributions: [
        {
          specialist: SPECIALISTS.PAIGE,
          kind: CONTRIBUTION_KINDS.VARIANTS,
          payload: {
            variants: [{
              subject: 'Cutting-edge cleaning for your offices',
              body: 'Here is the prepared message body.',
              cta: 'Book a quick call',
            }],
          },
        },
        {
          specialist: SPECIALISTS.EMMETT,
          kind: CONTRIBUTION_KINDS.CAPACITY,
          payload: {
            queue: { items: [{ prospectId: 'p-1' }, { prospectId: 'p-2' }] },
            capacity: { recommended: 2, available: 2 },
            deliverability: { status: 'healthy' },
            governor: { outcome: 'clear' },
          },
        },
      ],
      executionReview: {
        artifactBinding: {
          maxContributionId: 'm-1',
          paigeContributionId: 'p-1',
          emmettContributionId: 'e-1',
        },
        targets: [{ company: 'North Street Law', priorityReason: 'Strong office fit' }],
        communication: {
          subject: 'Cutting-edge cleaning for your offices',
          body: 'Here is the prepared message body.',
          cta: 'Book a quick call',
        },
        infrastructure: {
          queue: [{ prospectId: 'p-1' }, { prospectId: 'p-2' }],
          safeCapacity: 2,
          deliverabilityStatus: 'healthy',
          governorOutcome: 'clear',
        },
        decision: {
          blockers: [],
          plannedSendCount: 2,
        },
      },
      health: { label: 'Healthy' },
    };

    const response = buildExecutionMissionResponse({
      mission,
      snapshot,
      action: 'pending_operator_decision',
      question: 'continue',
      executionResult: null,
    });

    assert.equal(response.comm.headline, 'Execution Ready');
    assert.match(response.prose, /Execution Ready/i);
    assert.match(response.prose, /Prepared targets/i);
    assert.match(response.prose, /Prepared message/i);
    assert.match(response.prose, /Channel/i);
    assert.match(response.prose, /Send\/capacity summary/i);
    assert.match(response.prose, /Delivery\/governor state/i);
    assert.match(response.prose, /Authorize external execution of this prepared outreach\?/i);
    assert.doesNotMatch(response.prose, /Continue in mission workspace/i);
    assert.doesNotMatch(response.prose, /Active mission — Ready/i);
  });

  it('resolveInvestigationContinuationPayloads reads prior/current from committed contributions', () => {
    const priorPayload = priorDiscoveryPayload();
    const currentPayload = continuedDiscoveryPayload();
    const resolved = resolveInvestigationContinuationPayloads({
      snapshot: {
        contributions: [
          {
            specialist: SPECIALISTS.SCOUT,
            kind: CONTRIBUTION_KINDS.DISCOVERY,
            payload: priorPayload,
          },
          {
            specialist: SPECIALISTS.SCOUT,
            kind: CONTRIBUTION_KINDS.DISCOVERY,
            payload: currentPayload,
          },
        ],
      },
      executionResult: { discovery: { payload: currentPayload } },
    });

    assert.equal(resolved.priorPayload.candidateUniverseCount, 24);
    assert.equal(resolved.currentPayload.qualifiedCount, 14);
  });
});
