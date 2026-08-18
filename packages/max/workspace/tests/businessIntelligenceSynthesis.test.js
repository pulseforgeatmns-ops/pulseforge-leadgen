'use strict';

/**
 * SPEC-110 — business intelligence synthesis.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  maybeHandleRetrievalBeforeDelegationTurn,
} = require('../RetrievalBeforeDelegationContext');
const {
  CONTRACT_IDS,
  selectResponseContract,
} = require('../ResponseContract');
const {
  CATEGORIES,
  CONFIDENCE,
  synthesizeBusinessIntelligence,
  isChannelEffectivenessQuestion,
  intelligenceObject,
} = require('../BusinessIntelligence');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../../specialistDelegation/RetrievalGate');
const { shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { shouldRetrieveOperatingEvidence } = require('../OperatingEvidenceRetrieval');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');

const RETRIEVAL_OUTREACH = 'What outreach has already been sent?';
const SUMMARY_ANCHOR = 'How is Anchor Cleaning doing?';
const RECOMMENDATION_NEXT = 'What should we do next?';
const UNKNOWN_YELP = 'Are Yelp Ads working?';
const BOTTLENECK_FOCUS = 'Where should we focus next?';

function operatingOpts(overrides = {}) {
  const prospects = overrides.prospects || { total: 72, qualified: 54 };
  const scoutMatched = overrides.scoutMatched != null ? overrides.scoutMatched : 69;
  const aoLeads = overrides.aoLeads != null ? overrides.aoLeads : 20;
  const walkthroughs = overrides.walkthroughs != null ? overrides.walkthroughs : 0;
  const jobs = overrides.jobs != null ? overrides.jobs : 0;
  const touchpointCount = overrides.touchpointCount != null ? overrides.touchpointCount : 25;
  return {
    now: overrides.now || NOW,
    loadCampaignAo: async ({ clientId }) => {
      assert.equal(Number(clientId), ANCHOR_ID);
      return {
        available: true,
        campaignName: 'Campaign 001',
        mailExecuted: overrides.mailExecuted !== false,
        progress: {
          campaign_name: 'Campaign 001',
          target_total: 20,
          seeded_in_ao: aoLeads,
          visited: 0,
          walkthrough_requests: walkthroughs,
          remaining_route_queue: 20,
        },
        leads: Array.from({ length: aoLeads }, (_, i) => ({
          id: i + 1,
          client_id: ANCHOR_ID,
          campaign_name: 'Campaign 001',
          operational_state: 'not_started',
          mail_status: overrides.mailExecuted === false ? undefined : 'mailed',
        })),
      };
    },
    loadProspects: async () => ({ available: true, counts: prospects }),
    loadScout: async () => ({
      available: true,
      launchedNewWork: false,
      intelligence: {
        counts: { considered: scoutMatched, matched: scoutMatched },
        companies: Array.from({ length: Math.min(scoutMatched, 8) }, (_, i) => ({
          id: `co-${i + 1}`,
          tenantId: '10',
          name: `Company ${i + 1}`,
        })),
      },
      state: { tenantId: '10', opportunityCount: scoutMatched },
    }),
    loadMissions: async ({ clientId }) => ({
      available: true,
      rows: [
        {
          id: 'msn-1',
          clientId,
          title: 'Campaign 001 preparation',
          status: 'planned',
        },
      ],
    }),
    loadObjectives: async () => ({
      available: true,
      rows: [
        {
          id: 'obj-1',
          clientId: ANCHOR_ID,
          title: 'Grow commercial cleaning in Greater Manchester',
          status: 'active',
        },
      ],
    }),
    loadActivity: async ({ clientId }) => ({
      available: true,
      touchpoints: Array.from({ length: touchpointCount }, (_, i) => ({
        id: i + 1,
        client_id: clientId,
        channel: i === 0 ? 'mail' : 'phone',
        action_type: i === 0 ? 'delivery_log' : 'call',
      })),
      activity: [],
    }),
    loadOutcomes: async () => ({ available: true, jobs, payments: 0 }),
    loadOperatorAttested: async () => ({
      available: true,
      claims: [
        {
          status: 'active',
          statement: 'Campaign 001 was physically mailed on August 6.',
          metadata: {
            operatingUpdate: true,
            predicate: 'physical_mail_execution',
            occurredAt: '2026-08-06',
          },
        },
      ],
    }),
    loadCapability: async () => ({
      available: true,
      enabled_agents: ['scout'],
      autosend_enabled: false,
    }),
  };
}

function sessionContext() {
  return {
    tenantId: '10',
    clientId: ANCHOR_ID,
    clientIntelligence: {
      approved: true,
      businessName: 'Anchor Cleaning',
      identity: 'Anchor Cleaning — commercial cleaning for professional offices.',
      geography: 'Greater Manchester including Bedford and Hooksett',
      idealCustomers: 'property managers and professional offices',
      goals: 'Grow commercial cleaning in Greater Manchester.',
      unknowns: ['Which commercial segment will respond first'],
    },
  };
}

async function retrieveTurn(question, extras = {}) {
  const context = sessionContext();
  return maybeHandleRetrievalBeforeDelegationTurn({
    question,
    session: { id: 'spec-110', context },
    context,
    operatingEvidenceOpts: operatingOpts(extras),
    now: NOW,
    ...extras,
  });
}

function biIndex(prose) {
  return String(prose || '').search(/Business Intelligence/i);
}

function evidenceIndex(prose) {
  return String(prose || '').search(/\nEvidence\n|\nSupporting Evidence\n/i);
}

describe('SPEC-110 intelligence objects', () => {
  it('rejects findings that have no supporting claims', () => {
    const obj = intelligenceObject({
      id: 'invented',
      finding: 'Yelp Ads are working.',
      category: CATEGORIES.MOMENTUM,
      confidence: CONFIDENCE.HIGH,
      supportingClaims: [],
      operatorImpact: 'Scale ads.',
    });
    assert.equal(obj, null);
  });

  it('produces first-class objects with required fields from grounded inventory', () => {
    const synthesis = synthesizeBusinessIntelligence({
      question: SUMMARY_ANCHOR,
      bundle: {
        items: [
          {
            epistemic: 'verified',
            claim: '72 prospects are recorded.',
            provenance: 'prospect repository',
            sourceKind: 'prospects',
          },
          {
            epistemic: 'verified',
            claim: '69 Scout companies are on file.',
            provenance: 'Scout acquisition state',
            sourceKind: 'scout',
          },
          {
            epistemic: 'verified',
            claim: '20 AO leads are attributed.',
            provenance: 'Campaign 001 AO records',
            sourceKind: 'campaign',
          },
          {
            epistemic: 'not_recorded',
            claim: 'No durable conversion, job, or payment outcomes are recorded for this tenant.',
            provenance: 'revenue outcome',
            sourceKind: 'outcome',
          },
        ],
        prospects: { counts: { total: 72, qualified: 54 } },
        scout: { intelligence: { counts: { matched: 69 } } },
        campaign: {
          campaignName: 'Campaign 001',
          mailExecuted: true,
          progress: { seeded_in_ao: 20, walkthrough_requests: 0 },
        },
        outcomes: { jobs: 0, payments: 0 },
      },
      extras: { now: NOW },
    });
    assert.ok(synthesis.objects.length >= 1);
    for (const obj of synthesis.objects) {
      assert.ok(obj.finding);
      assert.ok(Object.values(CATEGORIES).includes(obj.category));
      assert.ok(Object.values(CONFIDENCE).includes(obj.confidence));
      assert.ok(Array.isArray(obj.supporting_claims) && obj.supporting_claims.length >= 1);
      assert.equal('operator_impact' in obj, true);
    }
    assert.ok(synthesis.objects.some((obj) => obj.category === CATEGORIES.BOTTLENECK));
    assert.match(synthesis.prose, /sufficient|execution|walkthroughs|recurring clients/i);
    assert.doesNotMatch(synthesis.prose, /because the market is slow|will close \d+/i);
  });
});

describe('SPEC-110 Inventory — What outreach has already been sent?', () => {
  it('summarizes verified outreach in business intelligence before listing evidence', async () => {
    const turn = await retrieveTurn(RETRIEVAL_OUTREACH);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RETRIEVAL);
    const bi = biIndex(turn.prose);
    const evidence = evidenceIndex(turn.prose);
    const inventory = turn.prose.search(/What I can verify|Recently completed/i);
    assert.ok(bi >= 0, 'business intelligence heading missing');
    assert.ok(inventory >= 0);
    assert.ok(bi < inventory, 'intelligence must precede inventory');
    if (evidence >= 0) assert.ok(bi < evidence);
    assert.match(turn.prose, /verified outreach|physical mail|Campaign 001/i);
    assert.doesNotMatch(turn.prose, /I'd recommend proving a repeatable/i);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.some((obj) => obj.id === 'verified_outreach' || obj.finding));
    assert.ok(objects.every((obj) => obj.supporting_claims.length >= 1));
  });
});

describe('SPEC-110 Summary — How is Anchor Cleaning doing?', () => {
  it('gives the operator understanding before inventory', async () => {
    const turn = await retrieveTurn(SUMMARY_ANCHOR);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.SUMMARY);
    const bi = biIndex(turn.prose);
    const observed = turn.prose.search(/Observed operating state/i);
    const evidence = evidenceIndex(turn.prose);
    assert.ok(bi >= 0);
    assert.ok(observed >= 0);
    assert.ok(bi < observed, 'intelligence must precede observed state');
    if (evidence >= 0) assert.ok(bi < evidence);
    assert.match(turn.prose, /sufficient|execution|walkthroughs|recurring clients/i);
    assert.match(turn.prose, /Goals/i);
    assert.match(turn.prose, /Unknowns/i);
    const recIdx = turn.prose.search(/\nRecommendation\n/i);
    if (recIdx >= 0) assert.ok(bi < recIdx);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.some((obj) => obj.category === CATEGORIES.BOTTLENECK));
    assert.ok(objects.some((obj) => obj.category === CATEGORIES.UNKNOWN));
  });
});

describe('SPEC-110 Recommendation — What should we do next?', () => {
  it('references synthesized findings rather than isolated facts', async () => {
    const turn = await retrieveTurn(RECOMMENDATION_NEXT);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RECOMMENDATION);
    const bi = biIndex(turn.prose);
    const rec = turn.prose.search(/RECOMMENDATION/i);
    const evidence = evidenceIndex(turn.prose);
    assert.ok(bi >= 0);
    assert.ok(rec >= 0);
    assert.ok(bi < rec, 'intelligence must precede recommendation');
    if (evidence >= 0) assert.ok(rec < evidence || bi < evidence);
    const primary = turn.structured.metadata.businessIntelligence.primary;
    assert.ok(primary && primary.finding);
    assert.match(turn.prose, new RegExp(primary.finding.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').slice(0, 40), 'i'));
    assert.match(turn.prose, /operating finding|execution|outbound|inventory/i);
    assert.doesNotMatch(turn.prose, /^72 prospects\n69 Scout/m);
  });
});

describe('SPEC-110 Unknown — Are Yelp Ads working?', () => {
  it('returns insufficient evidence and does not speculate', async () => {
    assert.equal(isChannelEffectivenessQuestion(UNKNOWN_YELP), true);
    assert.equal(shouldRetrieveOperatingEvidence(UNKNOWN_YELP), true);
    const mode = classifyCognitiveMode(UNKNOWN_YELP);
    assert.equal(mode.kind, COGNITIVE_MODES.RETRIEVAL);
    assert.equal(selectResponseContract(UNKNOWN_YELP, mode).id, CONTRACT_IDS.RETRIEVAL);
    assert.equal(shouldInvokeSpecialist(UNKNOWN_YELP), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(UNKNOWN_YELP, null, { approvedBlueprint: true }),
      false
    );
    const turn = await retrieveTurn(UNKNOWN_YELP);
    assert.ok(turn);
    assert.match(turn.prose, /Insufficient evidence to determine(?: Yelp Ads)? effectiveness/i);
    assert.doesNotMatch(turn.prose, /Yelp Ads are working|probably working|likely converting/i);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.every((obj) => obj.category === CATEGORIES.UNKNOWN));
    assert.ok(objects.every((obj) => obj.confidence === CONFIDENCE.UNKNOWN));
  });
});

describe('SPEC-110 Bottleneck — Where should we focus next?', () => {
  it('identifies the current bottleneck from grounded evidence', async () => {
    const mode = classifyCognitiveMode(BOTTLENECK_FOCUS);
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(selectResponseContract(BOTTLENECK_FOCUS, mode).id, CONTRACT_IDS.RECOMMENDATION);
    const turn = await retrieveTurn(BOTTLENECK_FOCUS);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RECOMMENDATION);
    const bi = biIndex(turn.prose);
    assert.ok(bi >= 0);
    assert.match(turn.prose, /execution|bottleneck|outbound|conversion/i);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.some((obj) => obj.category === CATEGORIES.BOTTLENECK));
    assert.ok(objects[0].supporting_claims.length >= 1);
  });
});

describe('SPEC-110 spec file exists', () => {
  it('keeps SPEC-110 in docs/specs', () => {
    const spec = path.join(__dirname, '../../../../docs/specs/SPEC-110_Business_Intelligence_Synthesis.md');
    assert.equal(fs.existsSync(spec), true);
  });
});
