'use strict';

/**
 * SPEC-111 — operator intent taxonomy.
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
  listResponseContracts,
  containsForbidden,
} = require('../ResponseContract');
const {
  OPERATOR_INTENTS,
  looksLikeDiagnosis,
  looksLikeUnknownAnalysis,
  looksLikeRisk,
  looksLikeProgress,
} = require('../OperatorIntentRegistry');
const { CATEGORIES } = require('../BusinessIntelligence');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../../specialistDelegation/RetrievalGate');
const { shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { shouldHandleScoutAcquisition } = require('../ScoutAcquisitionContext');
const { shouldRetrieveOperatingEvidence } = require('../OperatingEvidenceRetrieval');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');

const DIAGNOSIS = "What's preventing us from growing faster?";
const UNKNOWN_ANALYSIS = "What don't we know yet that matters?";
const RISK = "What's our biggest operational risk?";
const PROGRESS = 'How much progress have we made?';
const RECOMMENDATION = 'What should we do next?';

function operatingOpts() {
  return {
    now: NOW,
    loadCampaignAo: async ({ clientId }) => {
      assert.equal(Number(clientId), ANCHOR_ID);
      return {
        available: true,
        campaignName: 'Campaign 001',
        mailExecuted: true,
        progress: {
          campaign_name: 'Campaign 001',
          target_total: 20,
          seeded_in_ao: 20,
          visited: 0,
          walkthrough_requests: 0,
          remaining_route_queue: 20,
        },
        leads: Array.from({ length: 20 }, (_, i) => ({
          id: i + 1,
          client_id: ANCHOR_ID,
          campaign_name: 'Campaign 001',
          operational_state: 'not_started',
          mail_status: 'mailed',
        })),
      };
    },
    loadProspects: async () => ({
      available: true,
      counts: { total: 72, qualified: 54, cold: 40, warm: 10, hot: 4 },
    }),
    loadScout: async () => ({
      available: true,
      launchedNewWork: false,
      intelligence: {
        counts: { considered: 69, matched: 69 },
        companies: Array.from({ length: 8 }, (_, i) => ({
          id: `co-${i + 1}`,
          tenantId: '10',
          name: `Company ${i + 1}`,
        })),
      },
      state: { tenantId: '10', opportunityCount: 69 },
    }),
    loadMissions: async ({ clientId }) => ({
      available: true,
      rows: [{ id: 'msn-1', clientId, title: 'Campaign 001 preparation', status: 'planned' }],
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
      touchpoints: Array.from({ length: 25 }, (_, i) => ({
        id: i + 1,
        client_id: clientId,
        channel: i === 0 ? 'mail' : 'phone',
        action_type: i === 0 ? 'delivery_log' : 'call',
      })),
      activity: [],
    }),
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
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

async function retrieveTurn(question) {
  const context = sessionContext();
  return maybeHandleRetrievalBeforeDelegationTurn({
    question,
    session: { id: 'spec-111', context },
    context,
    operatingEvidenceOpts: operatingOpts(),
    now: NOW,
  });
}

describe('SPEC-111 intent registry', () => {
  it('registers the nine operator intents and matching contracts', () => {
    const ids = listResponseContracts().map((c) => c.id);
    assert.deepEqual(ids, [
      CONTRACT_IDS.RETRIEVAL,
      CONTRACT_IDS.SUMMARY,
      CONTRACT_IDS.RECOMMENDATION,
      CONTRACT_IDS.DIAGNOSIS,
      CONTRACT_IDS.UNKNOWN_ANALYSIS,
      CONTRACT_IDS.RISK,
      CONTRACT_IDS.PROGRESS,
      CONTRACT_IDS.CHALLENGE,
      CONTRACT_IDS.INVESTIGATION,
    ]);
    assert.deepEqual(Object.values(OPERATOR_INTENTS), ids);
  });

  it('classifies diagnosis before recommendation', () => {
    assert.equal(looksLikeDiagnosis(DIAGNOSIS), true);
    const mode = classifyCognitiveMode(DIAGNOSIS);
    assert.equal(mode.kind, COGNITIVE_MODES.DIAGNOSIS);
    assert.equal(mode.intent, OPERATOR_INTENTS.DIAGNOSIS);
    assert.equal(selectResponseContract(DIAGNOSIS, mode).id, CONTRACT_IDS.DIAGNOSIS);
    assert.equal(shouldInvokeSpecialist(DIAGNOSIS), false);
    assert.equal(shouldRetrieveOperatingEvidence(DIAGNOSIS), true);
    assert.equal(
      shouldClaimClientIntelligenceTurn(DIAGNOSIS, null, { approvedBlueprint: true }),
      false
    );
  });

  it('does not send unknown analysis to Scout acquisition', () => {
    assert.equal(looksLikeUnknownAnalysis(UNKNOWN_ANALYSIS), true);
    const mode = classifyCognitiveMode(UNKNOWN_ANALYSIS);
    assert.equal(mode.kind, COGNITIVE_MODES.UNKNOWN_ANALYSIS);
    assert.equal(selectResponseContract(UNKNOWN_ANALYSIS, mode).id, CONTRACT_IDS.UNKNOWN_ANALYSIS);
    assert.equal(
      shouldHandleScoutAcquisition({ question: UNKNOWN_ANALYSIS, context: { tenantId: '10' } }),
      false
    );
    assert.equal(shouldInvokeSpecialist(UNKNOWN_ANALYSIS), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(UNKNOWN_ANALYSIS, null, { approvedBlueprint: true }),
      false
    );
  });

  it('classifies risk and progress independently of recommendation', () => {
    assert.equal(looksLikeRisk(RISK), true);
    assert.equal(looksLikeProgress(PROGRESS), true);
    assert.equal(classifyCognitiveMode(RISK).kind, COGNITIVE_MODES.RISK);
    assert.equal(selectResponseContract(RISK).id, CONTRACT_IDS.RISK);
    assert.equal(classifyCognitiveMode(PROGRESS).kind, COGNITIVE_MODES.PROGRESS);
    assert.equal(selectResponseContract(PROGRESS).id, CONTRACT_IDS.PROGRESS);
  });
});

describe('SPEC-111 Diagnosis — What\'s preventing us from growing faster?', () => {
  it('identifies execution as the bottleneck and does not give generic acquisition advice', async () => {
    const turn = await retrieveTurn(DIAGNOSIS);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.DIAGNOSIS);
    assert.match(turn.prose, /Current bottleneck/i);
    assert.match(turn.prose, /execution|bottleneck/i);
    assert.match(turn.prose, /Confidence/i);
    assert.match(turn.prose, /Supporting evidence|Evidence/i);
    assert.doesNotMatch(turn.prose, /I'd recommend proving a repeatable/i);
    assert.doesNotMatch(turn.prose, /I'd recommend a focused first campaign/i);
    assert.doesNotMatch(turn.prose, /I'd start by proving a repeatable commercial acquisition/i);
    assert.equal(containsForbidden(turn.prose, turn.responseContract).length, 0);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.some((obj) => obj.category === CATEGORIES.BOTTLENECK));
    assert.ok(objects.some((obj) => /execution/i.test(obj.finding) || obj.id === 'execution_bottleneck'));
  });
});

describe('SPEC-111 Unknown Analysis — What don\'t we know yet that matters?', () => {
  it('returns conversions, walkthroughs, Yelp performance, and campaign execution — not acquisition rumors', async () => {
    const turn = await retrieveTurn(UNKNOWN_ANALYSIS);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.UNKNOWN_ANALYSIS);
    assert.match(turn.prose, /Critical unknowns/i);
    assert.match(turn.prose, /conversions/i);
    assert.match(turn.prose, /walkthroughs/i);
    assert.match(turn.prose, /Yelp/i);
    assert.match(turn.prose, /campaign execution/i);
    assert.match(turn.prose, /Evidence gaps/i);
    assert.match(turn.prose, /Why they matter/i);
    assert.doesNotMatch(turn.prose, /acquisition rumors/i);
    assert.doesNotMatch(turn.prose, /I'd recommend proving a repeatable/i);
    assert.doesNotMatch(turn.prose, /probably working|likely converting/i);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.every((obj) => obj.category === CATEGORIES.UNKNOWN));
    assert.ok(objects.every((obj) => obj.supporting_claims.length >= 1));
  });
});

describe('SPEC-111 Risk — What\'s our biggest operational risk?', () => {
  it('returns grounded risks only', async () => {
    const turn = await retrieveTurn(RISK);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RISK);
    assert.match(turn.prose, /Risks/i);
    assert.match(turn.prose, /Evidence/i);
    assert.match(turn.prose, /Confidence/i);
    assert.match(turn.prose, /Potential impact/i);
    assert.doesNotMatch(turn.prose, /probably working|likely converting|I believe the market/i);
    const objects = turn.structured.metadata.businessIntelligence.objects;
    assert.ok(objects.length >= 1);
    assert.ok(objects.every((obj) => obj.category === CATEGORIES.RISK));
    assert.ok(objects.every((obj) => obj.supporting_claims.length >= 1));
  });
});

describe('SPEC-111 Progress — How much progress have we made?', () => {
  it('measures progress against goals', async () => {
    const turn = await retrieveTurn(PROGRESS);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.PROGRESS);
    assert.match(turn.prose, /Progress/i);
    assert.match(turn.prose, /Remaining work/i);
    assert.match(turn.prose, /Confidence/i);
    assert.match(turn.prose, /goal|Campaign 001|approved Blueprint/i);
    assert.doesNotMatch(turn.prose, /I'd recommend proving a repeatable/i);
  });
});

describe('SPEC-111 Recommendation — What should we do next?', () => {
  it('stays recommendation-first with no regression', async () => {
    const mode = classifyCognitiveMode(RECOMMENDATION);
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(selectResponseContract(RECOMMENDATION, mode).id, CONTRACT_IDS.RECOMMENDATION);
    const turn = await retrieveTurn(RECOMMENDATION);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RECOMMENDATION);
    assert.match(turn.prose, /RECOMMENDATION/i);
    const rec = turn.prose.search(/RECOMMENDATION/i);
    const bi = turn.prose.search(/Business Intelligence/i);
    assert.ok(rec >= 0);
    assert.ok(bi >= 0 && bi < rec);
  });
});

describe('SPEC-111 spec file exists', () => {
  it('keeps SPEC-111 in docs/specs', () => {
    const spec = path.join(__dirname, '../../../../docs/specs/SPEC-111_Operator_Intent_Taxonomy.md');
    assert.equal(fs.existsSync(spec), true);
  });
});
