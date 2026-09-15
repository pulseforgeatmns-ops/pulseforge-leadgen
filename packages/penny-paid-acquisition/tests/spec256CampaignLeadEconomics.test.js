'use strict';

/**
 * SPEC-256 — Campaign-to-lead economics alignment for canonical Penny.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  ACQUISITION_APPROACHES,
  buildExecutionInput,
} = amo;
const {
  deriveCampaignLeadEconomics,
  buildFirstPartyAttributionRetrieval,
  extractFirstPartyAttributionEvidence,
  resolveSharedObservationWindow,
  isProviderCompatible,
  WINDOW_ALIGNMENT,
  ECONOMICS_STATUS,
  AVAILABILITY,
  PLATFORM,
  mapAgentActionToEvidence,
} = require('../index');
const {
  buildPaidAcquisitionRecommendationPayload,
  runPennyPaidAcquisition,
  runPennyForAmoMission,
  loadFirstPartyAttributionEvidenceForPenny,
} = require('../../max/workspace/PennyPaidAcquisitionExecutor');
const { SOURCE_KIND } = require('../../../lib/walkthroughAttribution');

const ANCHOR_CAMPAIGN_ID = 'cmpn_6aa6bacd604881a383dc4ce465f9995d';
const WINDOW = {
  start: '2026-09-01',
  end: '2026-09-14',
  days: 14,
  label: 'LAST_14_DAYS',
};

function anchorPlatformEvidence(overrides = {}) {
  return {
    spec: 'SPEC-253',
    platform: PLATFORM.CHATGPT_ADS,
    channel: 'ChatGPT Ads',
    availability: AVAILABILITY.AVAILABLE,
    observationWindow: WINDOW,
    campaigns: [{
      externalCampaignId: ANCHOR_CAMPAIGN_ID,
      name: 'Anchor Residential – Recurring Home Cleaning',
      spend: 43.61,
      impressions: 1022,
      clicks: 7,
      platformConversions: 0,
      ...(overrides.campaign || {}),
    }],
    aggregates: {
      spend: 43.61,
      impressions: 1022,
      clicks: 7,
      platformConversions: 0,
    },
    provenance: { sourceKind: 'PLATFORM_API', readOnly: true },
    ...overrides.platform,
  };
}

function anchorFirstPartyEvidence(overrides = {}) {
  return mapAgentActionToEvidence({
    id: overrides.evidenceId ?? 88001,
    created_at: new Date('2026-09-10T12:00:00.000Z'),
    payload: {
      source: 'website_walkthrough',
      attribution: {
        raw: {
          campaign_id: ANCHOR_CAMPAIGN_ID,
          ad_group_id: 'grp_1',
          ad_id: 'ad_1',
        },
        normalized: {
          lead_source: 'chatgpt_ads',
          attribution_status: overrides.attributionStatus || 'deterministic',
        },
        provenance: { sourceKind: SOURCE_KIND },
      },
    },
  });
}

function availableRetrieval(observedCount = 1) {
  return {
    availability: AVAILABILITY.AVAILABLE,
    observedCount,
    reason: null,
    observationWindow: WINDOW,
  };
}

describe('SPEC-256 — CampaignLeadEconomics derivation', () => {
  it('matches exact campaign ID deterministically (Anchor ChatGPT Ads fixture)', () => {
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.equal(result.campaignLeadEconomics.length, 1);
    const row = result.campaignLeadEconomics[0];
    assert.equal(row.externalCampaignId, ANCHOR_CAMPAIGN_ID);
    assert.equal(row.firstPartyAttributedLeadCount, 1);
    assert.equal(row.costPerFirstPartyLead, 43.61);
    assert.equal(row.platformConversions, 0);
    assert.equal(row.observedPlatformSpend, 43.61);
    assert.equal(row.windowAlignment, WINDOW_ALIGNMENT.ALIGNED);
    assert.equal(row.evidenceOnly, true);
    assert.equal(row.cashExposure, null);
    assert.equal(row.cashExposureStatus, 'UNKNOWN');
    assert.equal(row.platformConversionsAreSeparateFromFirstPartyLeads, true);
    assert.equal(
      row.firstPartyAttributedLeadCount + row.platformConversions,
      1,
      'platformConversions must not be added to first-party lead count'
    );
  });

  it('does not join when campaign ID is missing', () => {
    const lead = anchorFirstPartyEvidence();
    lead.campaignId = null;
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [lead],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.equal(result.campaignLeadEconomics[0].firstPartyAttributedLeadCount, 0);
    assert.equal(result.campaignLeadEconomics[0].costPerFirstPartyLead, null);
    assert.equal(result.unmatchedFirstPartyAttribution.missingCampaignIdCount, 1);
  });

  it('does not join when campaign ID is unmatched', () => {
    const lead = anchorFirstPartyEvidence();
    lead.campaignId = 'cmpn_unknown';
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [lead],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.equal(result.campaignLeadEconomics[0].firstPartyAttributedLeadCount, 0);
    assert.equal(result.unmatchedFirstPartyAttribution.unmatchedCampaignIdCount, 1);
  });

  it('does not join on provider conflict', () => {
    const lead = anchorFirstPartyEvidence();
    lead.leadSource = 'google_ads';
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [lead],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.equal(result.campaignLeadEconomics[0].firstPartyAttributedLeadCount, 0);
    assert.equal(result.unmatchedFirstPartyAttribution.providerConflictCount, 1);
    assert.equal(isProviderCompatible(PLATFORM.CHATGPT_ADS, 'google_ads'), false);
  });

  it('counts deterministic attribution and keeps inferred separately inspectable', () => {
    const deterministic = anchorFirstPartyEvidence({ evidenceId: 1 });
    const inferred = anchorFirstPartyEvidence({ evidenceId: 2, attributionStatus: 'inferred' });
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [deterministic, inferred],
      firstPartyAttributionRetrieval: availableRetrieval(2),
      observationWindow: WINDOW,
    });

    const row = result.campaignLeadEconomics[0];
    assert.equal(row.attribution.deterministicLeadCount, 1);
    assert.equal(row.attribution.inferredLeadCount, 1);
    assert.equal(row.firstPartyAttributedLeadCount, 1);
    assert.equal(row.costPerFirstPartyLead, 43.61);
  });

  it('counts duplicate evidenceId once', () => {
    const lead = anchorFirstPartyEvidence({ evidenceId: 42 });
    const duplicate = { ...lead, label: 'duplicate copy' };
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [lead, duplicate],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.equal(result.campaignLeadEconomics[0].firstPartyAttributedLeadCount, 1);
  });

  it('returns CPL null when spend > 0 and zero first-party leads', () => {
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [],
      firstPartyAttributionRetrieval: availableRetrieval(0),
      observationWindow: WINDOW,
    });

    const row = result.campaignLeadEconomics[0];
    assert.equal(row.firstPartyAttributedLeadCount, 0);
    assert.equal(row.costPerFirstPartyLead, null);
    assert.equal(row.economicsStatus, ECONOMICS_STATUS.NO_FIRST_PARTY_LEADS);
  });

  it('returns CPL null when spend is zero with attributed leads', () => {
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence({ campaign: { spend: 0 } })],
      firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    const row = result.campaignLeadEconomics[0];
    assert.equal(row.costPerFirstPartyLead, null);
    assert.equal(
      row.economicsStatus,
      ECONOMICS_STATUS.SPEND_UNAVAILABLE_OR_ZERO_WITH_ATTRIBUTED_LEADS
    );
  });

  it('returns no economics rows when platform evidence is unavailable', () => {
    const unavailable = {
      ...anchorPlatformEvidence(),
      availability: AVAILABILITY.UNAVAILABLE,
      campaigns: [],
    };
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [unavailable],
      firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    assert.deepEqual(result.campaignLeadEconomics, []);
  });

  it('returns no economics rows when first-party retrieval errors', () => {
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [],
      firstPartyAttributionRetrieval: {
        availability: AVAILABILITY.ERROR,
        observedCount: null,
        reason: 'QUERY_FAILED',
        observationWindow: WINDOW,
      },
      observationWindow: WINDOW,
    });

    assert.deepEqual(result.campaignLeadEconomics, []);
  });

  it('returns CPL null on mismatched observation windows', () => {
    const mismatchedWindow = {
      start: '2026-08-01',
      end: '2026-08-14',
      days: 14,
      label: 'LAST_14_DAYS',
    };
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence({ platform: { observationWindow: mismatchedWindow } })],
      firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
      firstPartyAttributionRetrieval: availableRetrieval(1),
      observationWindow: WINDOW,
    });

    const row = result.campaignLeadEconomics[0];
    assert.equal(row.windowAlignment, WINDOW_ALIGNMENT.MISMATCHED);
    assert.equal(row.costPerFirstPartyLead, null);
    assert.equal(row.economicsStatus, ECONOMICS_STATUS.WINDOW_MISMATCHED);
  });

  it('preserves unmatched first-party leads in summary', () => {
    const unmatched = anchorFirstPartyEvidence({ evidenceId: 99 });
    unmatched.campaignId = 'cmpn_orphan';
    const result = deriveCampaignLeadEconomics({
      platformEvidence: [anchorPlatformEvidence()],
      firstPartyAttributionEvidence: [anchorFirstPartyEvidence(), unmatched],
      firstPartyAttributionRetrieval: availableRetrieval(2),
      observationWindow: WINDOW,
    });

    assert.equal(result.unmatchedFirstPartyAttribution.unmatchedCampaignIdCount, 1);
  });
});

describe('SPEC-256 — retrieval semantics', () => {
  it('distinguishes AVAILABLE zero leads from ERROR', () => {
    const availableZero = buildFirstPartyAttributionRetrieval({
      availability: AVAILABILITY.AVAILABLE,
      observedCount: 0,
      observationWindow: WINDOW,
    });
    const error = buildFirstPartyAttributionRetrieval({
      availability: AVAILABILITY.ERROR,
      observedCount: 0,
      reason: 'QUERY_FAILED',
      observationWindow: WINDOW,
    });

    assert.equal(availableZero.availability, AVAILABILITY.AVAILABLE);
    assert.equal(availableZero.observedCount, 0);
    assert.equal(error.availability, AVAILABILITY.ERROR);
    assert.equal(error.observedCount, null);
  });
});

describe('SPEC-256 — Penny output contract', () => {
  it('persists typed firstPartyAttributionEvidence and campaignLeadEconomics on recommendation', async () => {
    const platformEvidence = [anchorPlatformEvidence()];
    const firstPartyAttributionEvidence = [anchorFirstPartyEvidence({ evidenceId: 7001 })];
    const firstPartyAttributionRetrieval = availableRetrieval(1);
    const economics = deriveCampaignLeadEconomics({
      platformEvidence,
      firstPartyAttributionEvidence,
      firstPartyAttributionRetrieval,
      observationWindow: WINDOW,
    });

    const input = buildExecutionInput({
      mission: {
        tenantId: '10',
        id: 'mission-256',
        objective: 'Acquire recurring commercial cleaning customers.',
        structuredMission: {
          objective: 'Acquire recurring commercial cleaning customers.',
          market: { segment: 'Law Firms', buyer: 'Office manager' },
          geography: { region: 'Manchester NH' },
        },
      },
      specialist: SPECIALISTS.PENNY,
      contributions: [],
      platformEvidence,
      acquisitionEvidence: firstPartyAttributionEvidence,
      firstPartyAttributionEvidence,
      firstPartyAttributionRetrieval,
      campaignLeadEconomics: economics.campaignLeadEconomics,
      unmatchedFirstPartyAttribution: economics.unmatchedFirstPartyAttribution,
      observationWindow: WINDOW,
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
      availableBudget: { amount: 500, currency: 'USD' },
      conversionReadiness: { ready: true },
      measurementReadiness: { ready: true },
    });

    const typed = input.specialistInput.firstPartyAttributionEvidence;
    assert.equal(typed.length, 1);
    assert.equal(typed[0].kind, 'first_party_attributed_lead');
    assert.equal(typed[0].sourceKind, SOURCE_KIND);
    assert.ok(input.specialistInput.evidence.some((row) => row.kind === 'first_party_attributed_lead'));

    const pennyResult = await runPennyPaidAcquisition(input);
    const recommendation = pennyResult.contributions.paidAcquisitionRecommendation;

    assert.equal(recommendation.firstPartyAttributionEvidence.length, 1);
    assert.equal(recommendation.firstPartyAttributionRetrieval.availability, AVAILABILITY.AVAILABLE);
    assert.equal(recommendation.campaignLeadEconomics.length, 1);
    assert.equal(recommendation.campaignLeadEconomics[0].costPerFirstPartyLead, 43.61);
    assert.equal(recommendation.observationWindow.start, WINDOW.start);
    assert.ok(recommendation.unmatchedFirstPartyAttribution);
    assert.equal(recommendation.platformEvidence[0].aggregates.platformConversions, 0);
  });

  it('resolveSharedObservationWindow is persisted once for Penny execution', () => {
    const resolved = resolveSharedObservationWindow({ observationWindow: WINDOW });
    assert.equal(resolved.start, WINDOW.start);
    assert.equal(resolved.end, WINDOW.end);
  });

  it('extractFirstPartyAttributionEvidence filters typed rows only', () => {
    const merged = [
      anchorFirstPartyEvidence(),
      { kind: 'first_party_attribution_retrieval_unavailable', sourceKind: SOURCE_KIND },
      { label: 'scout note', source: 'scout' },
    ];
    const typed = extractFirstPartyAttributionEvidence(merged);
    assert.equal(typed.length, 1);
    assert.equal(typed[0].kind, 'first_party_attributed_lead');
  });
});

describe('SPEC-256 — scoreChannel unchanged', () => {
  it('does not change recommendation viability solely because economics exist', async () => {
    const withoutEconomics = buildPaidAcquisitionRecommendationPayload({
      specialistInput: {
        acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
        availableBudget: { amount: 500, currency: 'USD' },
        conversionReadiness: { ready: true },
        measurementReadiness: { ready: true },
        candidatePaidChannels: [{ name: 'Yelp', fit: 'strong' }],
        platformEvidence: [],
        evidence: [],
      },
    });

    const withEconomics = buildPaidAcquisitionRecommendationPayload({
      specialistInput: {
        acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
        availableBudget: { amount: 500, currency: 'USD' },
        conversionReadiness: { ready: true },
        measurementReadiness: { ready: true },
        candidatePaidChannels: [{ name: 'Yelp', fit: 'strong' }],
        platformEvidence: [anchorPlatformEvidence()],
        firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
        firstPartyAttributionRetrieval: availableRetrieval(1),
        campaignLeadEconomics: deriveCampaignLeadEconomics({
          platformEvidence: [anchorPlatformEvidence()],
          firstPartyAttributionEvidence: [anchorFirstPartyEvidence()],
          firstPartyAttributionRetrieval: availableRetrieval(1),
          observationWindow: WINDOW,
        }).campaignLeadEconomics,
        observationWindow: WINDOW,
        evidence: [],
      },
    });

    assert.equal(withoutEconomics.viability, withEconomics.viability);
    assert.equal(withoutEconomics.paidAcquisitionRecommendation.viability, withEconomics.paidAcquisitionRecommendation.viability);
  });
});
