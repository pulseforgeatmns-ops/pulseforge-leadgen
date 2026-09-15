'use strict';

/**
 * Anchor fresh mission audit — scope collapse + Scout evidence commit regression.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  resolveMarketScopeFromObjective,
  marketScopesCompatible,
  deriveMissionTitle,
  isExplicitStrPrimaryTarget,
} = require('../MissionNaming');
const { planFromObjective } = require('../MissionPlanner');
const { resolveCanonicalObjective } = require('../../max/workspace/ResolvedObjective');
const { freezeStructuredMission } = require('../StructuredMission');
const {
  buildDelegationFromAmoMission,
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const { assertEvidenceAttached } = require('../TransactionalExecution');
const { findResumableMission } = require('../../max/workspace/AcquisitionOwnership');
const { resetEngine } = require('../../../services/acquisitionMission');

const BROAD_ANCHOR_OBJECTIVE =
  'Acquire one new recurring cleaning client for Anchor Cleaning in Greater Manchester. ' +
  'Prioritize high-fit commercial and property-management opportunities, including ' +
  'short-term rental operators where appropriate.';

const STR_ONLY_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

const PRIOR_STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

describe('Anchor fresh mission audit', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('1 — broad commercial objective does not resolve exclusively to short_term_rental', () => {
    const scope = resolveMarketScopeFromObjective(BROAD_ANCHOR_OBJECTIVE);
    assert.equal(scope.primarySegment, 'property_management');
    assert.notEqual(scope.primarySegment, 'short_term_rental');
    assert.ok(scope.eligibleSubsegments.includes('property_management'));
    assert.ok(scope.eligibleSubsegments.includes('short_term_rental'));

    const resolved = resolveCanonicalObjective({ question: BROAD_ANCHOR_OBJECTIVE });
    assert.equal(resolved.segmentKey, 'property_management');
    assert.equal(resolved.marketMeta.segment, 'property_management');
    assert.equal(resolved.marketMeta.industry, 'real_estate');
    assert.equal(resolved.marketMeta.buyer, 'property_manager');

    const planned = planFromObjective(BROAD_ANCHOR_OBJECTIVE);
    assert.equal(planned.draft.market.segment, 'property_management');
    assert.ok(planned.draft.market.eligibleSubsegments.includes('short_term_rental'));
    assert.notEqual(planned.draft.market.segment, 'short_term_rental');
    assert.match(deriveMissionTitle(BROAD_ANCHOR_OBJECTIVE), /Commercial property management/i);
    assert.doesNotMatch(deriveMissionTitle(BROAD_ANCHOR_OBJECTIVE), /^Short-Term Rental/i);
  });

  it('2 — explicit STR-only objective still resolves to short_term_rental', () => {
    assert.equal(isExplicitStrPrimaryTarget(STR_ONLY_OBJECTIVE), true);
    const scope = resolveMarketScopeFromObjective(STR_ONLY_OBJECTIVE);
    assert.equal(scope.primarySegment, 'short_term_rental');
    const planned = planFromObjective(STR_ONLY_OBJECTIVE);
    assert.equal(planned.draft.market.segment, 'short_term_rental');
    assert.equal(planned.draft.market.industry, 'hospitality');
    assert.equal(planned.draft.market.buyer, 'property_operator');
  });

  it('3 — historical STR mission context cannot override broader operator objective', () => {
    assert.equal(
      marketScopesCompatible(PRIOR_STR_OBJECTIVE, BROAD_ANCHOR_OBJECTIVE),
      false
    );
    const missions = [
      {
        id: 'mission_82e8102f-249c-4f44-b88e-2de76b13898e',
        objective: PRIOR_STR_OBJECTIVE,
        stage: 'discover',
        structuredMission: { market: { segment: 'short_term_rental' } },
      },
    ];
    assert.equal(findResumableMission(missions, BROAD_ANCHOR_OBJECTIVE), null);
  });

  it('4 — fresh mission does not inherit prior mission artifacts', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const prior = engine.create({
      tenantId: '10',
      id: 'mission_82e8102f-249c-4f44-b88e-2de76b13898e',
      objective: PRIOR_STR_OBJECTIVE,
      planApproved: true,
    });
    engine.contribute(prior.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: normalizeScoutDiscoveryPayload({
        status: 'completed',
        payload: {
          opportunities: [{
            name: 'Old STR Co',
            evidenceRefs: [{ label: 'prior', snapshot: { source: 'google_places' } }],
          }],
          qualifiedCount: 1,
        },
      }),
    });

    const fresh = engine.create({
      tenantId: '10',
      objective: BROAD_ANCHOR_OBJECTIVE,
      resolvedObjective: resolveCanonicalObjective({ question: BROAD_ANCHOR_OBJECTIVE }),
    });

    assert.notEqual(fresh.id, prior.id);
    assert.equal(fresh.resolvedObjective.segmentKey, 'property_management');
    const freshSnapshot = engine.inspect(fresh.id, { tenantId: '10' });
    assert.equal(freshSnapshot.contributions.length, 0);
    assert.notEqual(fresh.missionPlanDraft.market.segment, 'short_term_rental');
  });

  it('5-7 — APPROVE_DISCOVERY runs Scout and TME accepts contribution with evidence', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: BROAD_ANCHOR_OBJECTIVE,
      resolvedObjective: resolveCanonicalObjective({ question: BROAD_ANCHOR_OBJECTIVE }),
    });

    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Proceed with this plan.',
    });
    assert.equal(planResult.snapshot.mission.structuredMissionApproved, true);

    const scoutResult = {
      status: 'completed',
      confidence: 0.76,
      payload: {
        opportunities: [{
          companyId: 'co-pm-1',
          name: 'Harbor Property Management',
          fit: 0.81,
          signals: [{ type: 'portfolio', label: 'Multi-unit portfolio', source: 'website' }],
          evidenceRefs: [{
            id: 'ev-harbor',
            label: 'Portfolio page lists managed units',
            snapshot: { source: 'website', companyName: 'Harbor Property Management' },
          }],
        }],
        qualifiedCount: 1,
      },
    };

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      runScout: async () => scoutResult,
    });

    assert.equal(discoveryResult.alreadyExecuted, false);
    assert.ok(discoveryResult.discovery);
    assert.ok(discoveryResult.discovery.payload.evidence.length > 0);
    assert.ok(discoveryResult.discovery.payload.buyingSignals.length > 0);
    assert.doesNotThrow(() =>
      assertEvidenceAttached(discoveryResult.discovery.payload, { required: true })
    );
  });

  it('8 — zero-result Scout with provider telemetry is blocked, not evidence-validation failure', () => {
    const payload = normalizeScoutDiscoveryPayload({
      status: 'completed',
      payload: {
        opportunities: [],
        fitCandidates: [],
        qualifiedCount: 0,
        providerExecution: [{
          provider: 'Google Maps',
          status: 'empty',
          googleStatus: 'ZERO_RESULTS',
          results: 0,
        }],
      },
    });
    assert.equal(payload.blocked, true);
    assert.doesNotThrow(() =>
      assertEvidenceAttached(payload, { required: !payload.blocked })
    );
  });

  it('9-11 — no execution approval, no outbound, autosend false on broad mission plan', async () => {
    const resolved = resolveCanonicalObjective({ question: BROAD_ANCHOR_OBJECTIVE });
    assert.notEqual(resolved.executionPolicy.autonomy, 'autonomous');

    const planned = planFromObjective(BROAD_ANCHOR_OBJECTIVE);
    const frozen = freezeStructuredMission(planned.draft, { approvedBy: 'operator' });
    const delegation = buildDelegationFromAmoMission({
      id: 'mission-fresh',
      tenantId: '10',
      objective: BROAD_ANCHOR_OBJECTIVE,
      structuredMission: frozen,
      structuredMissionApproved: true,
    });

    assert.deepEqual(delegation.targetContext.segments, [
      'property_management',
      'short_term_rental',
      'law_firm',
      'accounting',
    ]);
    assert.equal(delegation.targetContext.primarySegment, 'property_management');
    assert.notEqual(delegation.businessContext.autosend, true);
    assert.ok(!JSON.stringify(delegation).includes('EXECUTE_OUTBOUND'));
    assert.ok(!JSON.stringify(delegation).includes('APPROVE_EXECUTION'));
  });
});
