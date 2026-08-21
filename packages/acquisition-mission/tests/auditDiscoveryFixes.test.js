'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  deriveMissionTitle,
  inferTargetSegmentFromObjective,
  segmentToSearchKey,
} = require('../MissionNaming');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('../DiscoveryPayload');
const { presentationFromDiscoveryPayload } = require('../DiscoveryPresentation');
const {
  buildDelegationFromAmoMission,
  advanceDiscoveryAfterApproval,
  advancePlanAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Hooksett and Auburn.';

describe('AMO discovery audit fixes', () => {
  it('derives concise mission title from objective — not Blueprint ICP dump', () => {
    const title = deriveMissionTitle(STR_OBJECTIVE);
    assert.equal(title, 'Short-Term Rental Operators — Hooksett and Auburn');
    assert.doesNotMatch(title, /property manager.*facility manager/i);
  });

  it('infers target segment from mission objective only', () => {
    assert.equal(
      inferTargetSegmentFromObjective(STR_OBJECTIVE),
      'Short-Term Rental Operators'
    );
    assert.equal(
      inferTargetSegmentFromObjective('Acquire commercial cleaning customers in Manchester for law firms.'),
      'Law Firms'
    );
  });

  it('maps STR segment to search key for Scout delegation', () => {
    assert.equal(segmentToSearchKey('Short-Term Rental Operators'), 'short_term_rental');
  });

  it('buildDelegationFromAmoMission binds immutable mission objective', () => {
    const mission = {
      id: 'mission_str',
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Property Managers, Facility Managers, Professional Offices',
      constraints: ['Commercial only'],
    };
    const delegation = buildDelegationFromAmoMission(mission);
    assert.equal(delegation.businessContext.operatorDirection, STR_OBJECTIVE);
    assert.equal(delegation.businessContext.missionObjectiveImmutable, true);
    assert.equal(delegation.targetContext.missionBound, true);
    assert.deepEqual(delegation.targetContext.segments, ['short_term_rental']);
    assert.notEqual(delegation.targetContext.businessType, mission.targetSegment);
  });

  it('normalizes scout payload with attributable evidence and signal specificity', () => {
    const payload = normalizeScoutDiscoveryPayload({
      status: 'completed',
      summary: '1 prospect matches mission objective.',
      confidence: 0.81,
      payload: {
        opportunities: [
          {
            companyId: 'co-str-1',
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
            unknowns: [{ text: 'Current vendor unknown.' }],
          },
        ],
        qualifiedCount: 1,
      },
    }, { missionObjective: STR_OBJECTIVE });

    assert.equal(payload.companies[0].name, 'Summit STR Management');
    assert.equal(payload.buyingSignals[0].label, 'Hiring cleaning operations coordinator');
    assert.match(payload.evidence[0].source, /Job board/i);
    assert.ok(payload.summary);
    assert.ok(payload.confidenceBreakdown);
    assert.equal(payload.confidenceBreakdown.overall, payload.confidence);
    assert.ok(payload.rankedProspects[0].rationale);
  });

  it('does not approve prioritization without sufficient evidence', () => {
    const weak = presentationFromDiscoveryPayload({
      companies: [{ name: 'Harbor Law Group' }],
      buyingSignals: ['Hiring'],
      evidence: ['fixture'],
      qualifiedCount: 1,
      summary: 'Found 1 prospect.',
    });
    assert.equal(hasSufficientEvidenceForPrioritization(weak), false);

    const strong = presentationFromDiscoveryPayload(
      normalizeScoutDiscoveryPayload({
        status: 'completed',
        payload: {
          opportunities: [
            {
              name: 'Summit STR Management',
              signals: [{ type: 'hiring', label: 'Hiring cleaning coordinator', source: 'job_board' }],
              evidenceRefs: [{ label: 'Job board posting', snapshot: { source: 'job_board' } }],
            },
          ],
          qualifiedCount: 1,
        },
      }, { missionObjective: STR_OBJECTIVE })
    );
    assert.equal(hasSufficientEvidenceForPrioritization(strong), true);
  });

  it('does not use fixture fallback by default when discovery is blocked', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      planApproved: true,
    });

    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: false,
      runScout: async () => ({
        status: 'blocked',
        summary: 'No STR operators found in Hooksett and Auburn.',
        payload: { opportunities: [], qualifiedCount: 0 },
      }),
    });

    assert.equal(result.executionOutcome, 'blocked');
    assert.equal(result.discovery.payload.blocked, true);
    assert.equal(result.discovery.payload.companies.length, 0);
    assert.match(result.discovery.payload.summary, /No STR operators found/i);
  });

  it('updates mission confidence after scout discovery contribution', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      confidence: 0.5,
    });

    engine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: normalizeScoutDiscoveryPayload({
        status: 'completed',
        confidence: 0.79,
        payload: {
          companies: [{ id: '1', name: 'Summit STR Management' }],
          buyingSignals: [{ type: 'hiring', label: 'Hiring cleaning coordinator', source: 'job_board' }],
          evidence: [{ label: 'Job board', source: 'job_board' }],
          qualifiedCount: 1,
        },
      }),
    });

    const updated = engine.get(mission.id, '10');
    assert.equal(updated.confidence, 0.79);
  });

  it('prefers mission.objective over blueprint-sourced max contributions in shared context', () => {
    const mission = {
      id: 'm1',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
      constraints: [],
      title: 'STR — Hooksett',
      campaign: null,
      priority: 'normal',
      status: 'Discovering',
      stage: 'discover',
    };
    const contributions = [
      {
        specialist: 'max',
        kind: 'constraints',
        payload: {
          strategicContext: { icp: 'Property Managers, Facility Managers, Professional Offices' },
        },
      },
    ];
    const ctx = amo.buildSharedContext(mission, contributions);
    assert.equal(ctx.mission.objective, STR_OBJECTIVE);
  });
});
