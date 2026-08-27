'use strict';

/**
 * SPEC-193 — Post-Discovery Readiness Enforcement (ADR-077).
 * presentable decision === executable decision
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const { hasSufficientEvidenceForPrioritization, normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const {
  evaluatePrioritizationReadiness: evaluateReadiness,
  buildPostDiscoveryPendingDecision: buildPendingDecision,
} = require('../DecisionReadiness');
const { presentationFromDiscoveryPayload } = require('../DiscoveryPresentation');
const {
  hasPendingPrioritizationApproval,
  hasPendingDiscoveryInvestigation,
  assertMissionStateConsistent,
} = require('../PendingOperatorDecision');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advanceDiscoveryInvestigationAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function strongDiscoveryPayload() {
  return {
    status: 'completed',
    summary: '1 prospect matches mission objective.',
    confidence: 0.81,
    discoveryStatus: 'complete',
    payload: {
      opportunities: [
        {
          companyId: 'co-1',
          name: 'Summit STR Management',
          fit: 0.84,
          timing: 0.72,
          confidence: 0.81,
          signals: [
            {
              type: 'hiring',
              label: 'Hiring cleaning operations coordinator',
              source: 'job_board',
            },
          ],
          evidenceRefs: [
            {
              label: 'Job posting: cleaning operations coordinator',
              snapshot: { source: 'job_board', companyName: 'Summit STR Management' },
            },
          ],
        },
      ],
      qualifiedCount: 1,
    },
  };
}

describe('SPEC-193 — Post-Discovery Readiness Enforcement', () => {
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

  async function approvePlan() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
  }

  async function runDiscovery(runScout) {
    await approvePlan();
    return advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: false,
      runScout,
    });
  }

  function assertInvestigationPending(snapshot, label) {
    assert.equal(
      snapshot.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
      label
    );
    assert.equal(hasPendingDiscoveryInvestigation(snapshot), true, label);
    assert.equal(hasPendingPrioritizationApproval(snapshot), false, label);
    assert.doesNotThrow(() =>
      assertMissionStateConsistent(snapshot.mission, {
        contributions: snapshot.contributions,
      })
    );
  }

  function assertPrioritizationPending(snapshot, label) {
    assert.equal(
      snapshot.mission.pendingOperatorDecision.kind,
      OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
      label
    );
    assert.equal(hasPendingPrioritizationApproval(snapshot), true, label);
    assert.equal(hasPendingDiscoveryInvestigation(snapshot), false, label);
  }

  function assertPresentationMatchesTme(discoveryPayload, label) {
    const pending = buildPendingDecision(discoveryPayload);
    const presentation = presentationFromDiscoveryPayload(discoveryPayload);
    const sufficient = hasSufficientEvidenceForPrioritization(presentation);

    if (pending.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
      assert.equal(sufficient, true, `${label}: presentation must be sufficient`);
    } else {
      assert.equal(pending.kind, OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION, label);
      assert.equal(sufficient, false, `${label}: presentation must be insufficient`);
    }
  }

  it('Scenario A — provider failure advertises investigation, not prioritization', async () => {
    const result = await runDiscovery(async () => ({
      status: 'blocked',
      blocked: true,
      blockerCode: 'REQUEST_DENIED',
      summary: 'Google Places REQUEST_DENIED.',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
      },
      coverage: {
        complete: false,
        warnings: ['Coverage incomplete.'],
      },
      discoveryStatus: 'incomplete',
    }));

    assertInvestigationPending(result.snapshot, 'Scenario A');
    assertPresentationMatchesTme(result.discovery.payload, 'Scenario A');
    assert.match(
      result.snapshot.mission.pendingOperatorDecision.reason,
      /REQUEST_DENIED|Discovery Blocked|Coverage Incomplete/i
    );

    await assert.rejects(
      () =>
        advancePrioritizationAfterApproval({
          engine,
          mission: result.snapshot.mission,
          tenantId: '10',
          question: 'Approved prioritization.',
        }),
      (err) => err.code === 'tme_no_pending_prioritization'
    );
  });

  it('Scenario B — raw businesses with zero ranked prospects advertises investigation', async () => {
    const result = await runDiscovery(async () => ({
      status: 'completed',
      summary: 'Raw businesses found but none qualified.',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        candidateUniverseCount: 12,
        evidence: [{ label: 'Google Places search', source: 'google_places' }],
      },
      discoveryStatus: 'complete',
      providerExecution: [{ source: 'google_places', succeeded: true, resultCount: 12 }],
    }));

    assertInvestigationPending(result.snapshot, 'Scenario B');
    assertPresentationMatchesTme(result.discovery.payload, 'Scenario B');
    assert.match(
      result.snapshot.mission.pendingOperatorDecision.reason,
      /prioritizable prospects|No ranked prospects/i
    );
  });

  it('Scenario C — ranked prospect without typed buying signals advertises investigation', async () => {
    const result = await runDiscovery(async () => ({
      status: 'completed',
      summary: 'Found one business without sufficient signals.',
      payload: {
        companies: [{ id: 'c1', name: 'Harbor Law Group' }],
        opportunities: [],
        qualifiedCount: 1,
        buyingSignals: ['Hiring'],
        evidence: ['fixture'],
      },
      discoveryStatus: 'complete',
    }));

    assertInvestigationPending(result.snapshot, 'Scenario C');
    assertPresentationMatchesTme(result.discovery.payload, 'Scenario C');
  });

  it('Scenario D — sufficient discovery advertises prioritization approval', async () => {
    const result = await runDiscovery(async () => strongDiscoveryPayload());

    assertPrioritizationPending(result.snapshot, 'Scenario D');
    assertPresentationMatchesTme(result.discovery.payload, 'Scenario D');

    const advanced = await advancePrioritizationAfterApproval({
      engine,
      mission: result.snapshot.mission,
      tenantId: '10',
      question: 'Approved prioritization.',
    });
    assert.equal(advanced.alreadyExecuted, false);
    assert.equal(engine.get(mission.id, '10').stage, STAGES.UNDERSTAND);
  });

  it('Scenario E — investigation continuation re-runs Scout instead of short-circuiting', async () => {
    let scoutRuns = 0;
    const first = await runDiscovery(async () => {
      scoutRuns += 1;
      return {
        status: 'completed',
        summary: 'No qualified prospects yet.',
        payload: {
          opportunities: [],
          qualifiedCount: 0,
          candidateUniverseCount: 8,
          evidence: [{ label: 'Google Places search', source: 'google_places' }],
        },
        discoveryStatus: 'complete',
      };
    });
    assertInvestigationPending(first.snapshot, 'Scenario E initial');

    const shortCircuit = await advanceDiscoveryAfterApproval({
      engine,
      mission: first.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: false,
      runScout: async () => {
        scoutRuns += 1;
        return strongDiscoveryPayload();
      },
    });
    assert.equal(shortCircuit.alreadyExecuted, false, 'must not short-circuit incomplete investigation');
    assert.equal(scoutRuns, 2);

    assertPrioritizationPending(shortCircuit.snapshot, 'Scenario E after continuation');
  });

  it('canonical continue investigation executes Scout again via investigation handler', async () => {
    let scoutRuns = 0;
    const first = await runDiscovery(async () => {
      scoutRuns += 1;
      return {
        status: 'blocked',
        summary: 'Temporary provider failure.',
        payload: { opportunities: [], qualifiedCount: 0 },
        discoveryStatus: 'incomplete',
      };
    });
    assertInvestigationPending(first.snapshot, 'investigation handler initial');

    const continued = await advanceDiscoveryInvestigationAfterApproval({
      engine,
      mission: first.snapshot.mission,
      tenantId: '10',
      question: 'Continue investigation.',
      allowFixtureFallback: false,
      runScout: async () => {
        scoutRuns += 1;
        return strongDiscoveryPayload();
      },
    });

    assert.equal(continued.investigationContinuation, true);
    assert.equal(scoutRuns, 2);
    assertPrioritizationPending(continued.snapshot, 'investigation handler after Scout rerun');
  });

  it('DecisionReadiness and validatePrioritizationPreconditions stay aligned across fixtures', () => {
    const fixtures = [
      {
        label: 'blocked provider',
        payload: {
          blocked: true,
          summary: 'REQUEST_DENIED',
          discoveryStatus: 'incomplete',
          companies: [],
        },
      },
      {
        label: 'zero ranked',
        payload: {
          summary: 'No prospects.',
          discoveryStatus: 'complete',
          candidateUniverseCount: 12,
          companies: [],
        },
      },
      {
        label: 'weak signals',
        payload: {
          summary: 'Found 1.',
          discoveryStatus: 'complete',
          companies: [{ name: 'Harbor Law Group' }],
          buyingSignals: ['Hiring'],
          evidence: ['fixture'],
          qualifiedCount: 1,
        },
      },
      {
        label: 'strong discovery',
        payload: normalizeScoutDiscoveryPayload(strongDiscoveryPayload(), {
          missionObjective: OBJECTIVE,
        }),
      },
    ];

    for (const fixture of fixtures) {
      const pending = buildPendingDecision(fixture.payload);
      const readiness = evaluateReadiness(fixture.payload);
      const presentation = readiness.presentation;
      const sufficient = hasSufficientEvidenceForPrioritization(presentation);

      assert.equal(readiness.sufficient, sufficient, fixture.label);
      if (sufficient) {
        assert.equal(pending.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL, fixture.label);
      } else {
        assert.equal(pending.kind, OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION, fixture.label);
      }
    }
  });
});
