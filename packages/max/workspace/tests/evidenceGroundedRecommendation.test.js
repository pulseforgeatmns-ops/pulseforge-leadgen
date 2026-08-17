'use strict';

/**
 * SPEC-107 — evidence-grounded recommendation orchestration.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
} = require('../RetrievalBeforeDelegationContext');
const {
  shouldClaimClientIntelligenceTurn,
} = require('../ClientIntelligenceContext');
const {
  isOperatingEvidenceQuestion,
  isOperatingGroundedRecommendation,
  shouldRetrieveOperatingEvidence,
  isInventoryOnlyRequest,
  loadOperatingEvidence,
  composeOperatingEvidenceAnswer,
} = require('../OperatingEvidenceRetrieval');
const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
  looksLikeRecommendation,
} = require('../../specialistDelegation/CognitiveMode');
const { shouldHandleScoutAcquisition } = require('../ScoutAcquisitionContext');
const { isOperatorOperatingUpdate } = require('../OperatorOperatingUpdate');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');

const PURE_RETRIEVAL = "What's the current state of Campaign 001?";
const PRODUCTION_RECOMMEND =
  'Given that update and what PulseForge already knows about Campaign 001, what should I focus on next to build the repeatable commercial pipeline?';
const HIGHEST_LEVERAGE =
  "Given what's already in motion for Anchor, where is the highest-leverage constraint or opportunity I should focus on next to establish a repeatable commercial pipeline?";
const BARE_NEXT = 'What should I focus on next?';
const CIE_FOCUS = 'Based on what you know about my business, what should we focus on first?';

function attestedClaims(overrides = {}) {
  return [
    {
      status: 'active',
      statement: 'Campaign 001 was physically mailed on August 6.',
      metadata: {
        operatingUpdate: true,
        predicate: 'physical_mail_execution',
        occurredAt: overrides.mailedAt || '2026-08-06',
      },
    },
    {
      status: 'active',
      statement: 'Follow-up expected August 18.',
      metadata: {
        operatingUpdate: true,
        predicate: 'campaign_follow_up_expected',
        expectedAt: overrides.followUpAt || '2026-08-18',
        temporalClass: 'expected',
        value: overrides.followUpValue || 'expected',
      },
    },
  ];
}

function operatingOpts(overrides = {}) {
  const prospects = overrides.prospects || { total: 71, qualified: 54 };
  const scoutMatched = overrides.scoutMatched != null ? overrides.scoutMatched : 69;
  const capability = overrides.capability || {
    available: true,
    enabled_agents: ['scout'],
    autosend_enabled: false,
  };
  const walkthroughs = overrides.walkthroughs != null ? overrides.walkthroughs : 0;
  return {
    now: overrides.now || NOW,
    loadCampaignAo: async ({ clientId }) => {
      assert.equal(Number(clientId), ANCHOR_ID);
      return {
        available: true,
        campaignName: 'Campaign 001',
        progress: {
          campaign_name: 'Campaign 001',
          target_total: 20,
          seeded_in_ao: overrides.aoLeads != null ? overrides.aoLeads : 20,
          visited: 0,
          walkthrough_requests: walkthroughs,
          escalations: 0,
          remaining_route_queue: 20,
        },
        leads: Array.from({ length: overrides.aoLeads != null ? overrides.aoLeads : 20 }, (_, i) => ({
          id: i + 1,
          client_id: ANCHOR_ID,
          campaign_name: 'Campaign 001',
          operational_state: 'not_started',
        })),
      };
    },
    loadProspects: async ({ clientId }) => {
      assert.equal(Number(clientId), ANCHOR_ID);
      return { available: true, counts: prospects };
    },
    loadScout: async () => ({
      available: scoutMatched > 0,
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
          tenantId: String(clientId),
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
          tenantId: '10',
          title: 'Build a repeatable commercial pipeline',
          status: 'active',
        },
      ],
    }),
    loadActivity: async ({ clientId }) => ({
      available: true,
      touchpoints: overrides.emailMotion
        ? [{ id: 1, client_id: clientId, channel: 'email', action_type: 'email_sent' }]
        : [{ id: 1, client_id: clientId, channel: 'phone', action_type: 'call' }],
      activity: [],
    }),
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
    loadOperatorAttested: async () => ({
      available: true,
      claims: overrides.omitAttested ? [] : attestedClaims(overrides),
    }),
    capability,
  };
}

async function retrieveTurn(question, overrides = {}) {
  const opts = operatingOpts(overrides);
  return maybeHandleRetrievalBeforeDelegationTurn({
    question,
    session: { id: 'spec-107', context: { tenantId: '10', clientId: ANCHOR_ID } },
    context: { tenantId: '10', clientId: ANCHOR_ID },
    operatingEvidenceOpts: opts,
    now: opts.now,
  });
}

describe('SPEC-107 classification', () => {
  it('keeps pure retrieval as retrieval, not recommendation', () => {
    const mode = classifyCognitiveMode(PURE_RETRIEVAL);
    assert.equal(isOperatingEvidenceQuestion(PURE_RETRIEVAL), true);
    assert.equal(isOperatingGroundedRecommendation(PURE_RETRIEVAL), false);
    assert.equal(isInventoryOnlyRequest(PURE_RETRIEVAL), false);
    assert.equal(mode.kind, COGNITIVE_MODES.RETRIEVAL);
    assert.equal(mode.requiresOperatingRetrieval, true);
    assert.equal(looksLikeRecommendation(PURE_RETRIEVAL), false);
  });

  it('classifies the production prompt as recommendation that requires retrieval', () => {
    const mode = classifyCognitiveMode(PRODUCTION_RECOMMEND);
    assert.equal(shouldRetrieveOperatingEvidence(PRODUCTION_RECOMMEND), true);
    assert.equal(isOperatingGroundedRecommendation(PRODUCTION_RECOMMEND), true);
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(mode.requiresOperatingRetrieval, true);
    assert.equal(isOperatorOperatingUpdate(PRODUCTION_RECOMMEND), false);
    assert.equal(shouldHandleScoutAcquisition({ question: PRODUCTION_RECOMMEND, context: { tenantId: '10' } }), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(PRODUCTION_RECOMMEND, null, { approvedBlueprint: true }),
      false
    );
  });

  it('does not treat CIE business-framed focus as operating-grounded recommendation', () => {
    assert.equal(isOperatingGroundedRecommendation(CIE_FOCUS), false);
    assert.equal(shouldRetrieveOperatingEvidence(CIE_FOCUS), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(CIE_FOCUS, null, { approvedBlueprint: true }),
      true
    );
  });

  it('does not treat a new-campaign ask as operating-grounded recommendation', () => {
    assert.equal(isOperatingEvidenceQuestion('What campaign would you recommend?'), false);
    assert.equal(isOperatingGroundedRecommendation('What campaign would you recommend?'), false);
  });

  it('leaves bare focus questions available to CIE when no operating signal exists', () => {
    assert.equal(isOperatingGroundedRecommendation('What should we focus on first?'), false);
    assert.equal(shouldRetrieveOperatingEvidence('What should we focus on first?'), false);
    assert.equal(classifyCognitiveMode('What should we focus on first?').kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(
      shouldClaimClientIntelligenceTurn('What should we focus on first?', null, { approvedBlueprint: true }),
      true
    );
  });
});

describe('SPEC-107 TEST A — pure retrieval', () => {
  it('returns operating inventory without forcing a recommendation', async () => {
    const turn = await retrieveTurn(PURE_RETRIEVAL);
    assert.ok(turn);
    assert.equal(turn.mode.kind, COGNITIVE_MODES.RETRIEVAL);
    assert.match(turn.prose, /What I can verify/i);
    assert.match(turn.prose, /operator-reported as physically mailed on 2026-08-06/i);
    assert.match(turn.prose, /expected to begin 2026-08-18/i);
    assert.doesNotMatch(turn.prose, /^RECOMMENDATION\n/m);
    assert.notEqual(turn.structured.metadata.evidenceGroundedRecommendation, true);
    assert.match(String(turn.structured.nextInvestigations || []), /Ask for a recommendation only after reviewing this inventory/i);
  });
});

describe('SPEC-107 TEST B — compound recommendation', () => {
  it('retrieves then recommends instead of terminating on inventory', async () => {
    const turn = await retrieveTurn(PRODUCTION_RECOMMEND);
    assert.ok(turn);
    assert.equal(turn.mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(turn.launchedScout, false);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /RECOMMENDATION/i);
    assert.match(turn.prose, /WHY NOW/i);
    assert.match(turn.prose, /Campaign 001/i);
    assert.match(turn.prose, /20 AO lead/i);
    assert.doesNotMatch(turn.prose, /Ask for a recommendation only after reviewing this inventory/i);
    assert.doesNotMatch(turn.prose, /What I can verify/i);
    assert.equal(turn.structured.metadata.evidenceGroundedRecommendation, true);
    assert.equal(turn.structured.metadata.executed, false);
    assert.deepEqual(turn.structured.nextInvestigations, []);
    assert.match(String(turn.structured.reasoning || []), /reasoned to a recommendation/i);
  });

  it('WorkspaceEngine does not return inventory-only for the production prompt', async () => {
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatingEvidenceOpts: operatingOpts(),
    });
    const opened = engine.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: PRODUCTION_RECOMMEND,
    });
    assert.equal(result.domainDecision.reason, 'operating_evidence_retrieval');
    assert.equal(result.retrieval.mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.match(result.prose, /RECOMMENDATION/i);
    assert.doesNotMatch(result.prose, /Ask for a recommendation only after reviewing this inventory/i);
    assert.equal(result.structured.metadata.executed, false);
  });
});

describe('SPEC-107 TEST C — highest leverage from healthy supply', () => {
  it('does not default to finding more prospects and respects disabled outbound policy', async () => {
    const turn = await retrieveTurn(HIGHEST_LEVERAGE);
    assert.ok(turn);
    assert.match(turn.prose, /71 prospect/i);
    assert.match(turn.prose, /54/i);
    assert.match(turn.prose, /already scheduled|already in motion|has an owner/i);
    assert.match(turn.prose, /OPERATOR ATTESTED/i);
    assert.match(turn.prose, /PLANNED/i);
    assert.match(turn.prose, /INFERENCE/i);
    assert.doesNotMatch(turn.prose, /find more prospects/i);
    assert.doesNotMatch(turn.prose, /Begin following up with Campaign 001/i);
    assert.match(turn.prose, /disabled|not in enabled agents/i);
    assert.match(turn.prose, /will not enable|does not enable|will not launch/i);
    assert.doesNotMatch(turn.prose, /start sending immediately/i);
    assert.doesNotMatch(turn.prose, /Max can start sending/i);
    assert.equal(turn.structured.metadata.executed, false);
  });
});

describe('SPEC-107 TEST D — low prospect supply is not hard-coded Emmett', () => {
  it('treats thin qualified inventory as the bottleneck', async () => {
    const turn = await retrieveTurn(HIGHEST_LEVERAGE, {
      prospects: { total: 3, qualified: 2 },
      scoutMatched: 0,
    });
    assert.match(turn.prose, /prospect-supply|thin|scarcity|qualified inventory is thin/i);
    assert.doesNotMatch(turn.prose, /Evaluate and prepare controlled Emmett activation/i);
    assert.doesNotMatch(turn.prose, /start sending immediately/i);
    assert.match(turn.prose, /would not be the highest-leverage move/i);
  });
});

describe('SPEC-107 TEST E — Emmett already active', () => {
  it('seeks another constraint instead of recommending activation again', async () => {
    const turn = await retrieveTurn(HIGHEST_LEVERAGE, {
      emailMotion: true,
      capability: {
        available: true,
        enabled_agents: ['scout', 'emmett'],
        autosend_enabled: true,
        emailMotionActive: true,
      },
    });
    assert.match(turn.prose, /already active/i);
    assert.doesNotMatch(turn.prose, /Evaluate and prepare controlled Emmett activation/i);
    assert.match(turn.prose, /walkthrough|conversion|measure/i);
  });
});

describe('SPEC-107 TEST F — planned is not completed', () => {
  it('does not say overdue follow-up occurred', async () => {
    const turn = await retrieveTurn(HIGHEST_LEVERAGE, {
      followUpAt: '2026-08-16',
      now: new Date('2026-08-17T15:00:00.000Z'),
    });
    assert.match(turn.prose, /planned\/expected|expected 2026-08-16/i);
    assert.match(turn.prose, /will not say it occurred|not recorded execution/i);
    assert.doesNotMatch(turn.prose, /follow-up (?:has )?occurred/i);
    assert.doesNotMatch(turn.prose, /follow-up actually began/i);
  });
});

describe('SPEC-107 TEST G — policy boundary', () => {
  it('may propose activation evaluation but never claims immediate sending', async () => {
    const turn = await retrieveTurn(BARE_NEXT);
    assert.ok(turn);
    assert.equal(turn.mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.match(turn.prose, /RECOMMENDATION/i);
    assert.match(turn.prose, /disabled|operator decision|readiness/i);
    assert.doesNotMatch(turn.prose, /start sending immediately/i);
    assert.doesNotMatch(turn.prose, /I will start Emmett/i);
    assert.equal(turn.structured.metadata.executed, false);
  });
});

describe('SPEC-107 safety', () => {
  it('does not hard-code Anchor or Emmett in the reasoner', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'OperatingStateRecommendation.js'),
      'utf8'
    );
    assert.doesNotMatch(src, /client_id\s*===?\s*10/);
    assert.doesNotMatch(src, /clientId\s*===?\s*10/);
    assert.doesNotMatch(src, /if\s*\(\s*clientId\s*==\s*10/);
    assert.doesNotMatch(src, /recommend Emmett/);
  });

  it('loadOperatingEvidence exposes capability without executing', async () => {
    const bundle = await loadOperatingEvidence({
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: operatingOpts(),
    });
    assert.equal(bundle.capability.agents.emmett.status, 'disabled');
    assert.equal(bundle.capability.agents.scout.status, 'available');
    assert.equal(bundle.capability.autosendEnabled, false);
    assert.equal(bundle.launchedScout, false);
    const composed = composeOperatingEvidenceAnswer(PRODUCTION_RECOMMEND, bundle, {
      recommend: true,
      now: NOW,
    });
    assert.equal(composed.executed, false);
    assert.equal(composed.recommend, true);
  });
});
