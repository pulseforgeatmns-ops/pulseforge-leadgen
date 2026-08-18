'use strict';

/**
 * SPEC-108 — claim grounding competency transfer across domains.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
} = require('../RetrievalBeforeDelegationContext');
const {
  shouldClaimClientIntelligenceTurn,
} = require('../ClientIntelligenceContext');
const { isOperatorOperatingUpdate } = require('../OperatorOperatingUpdate');
const {
  isClaimChallenge,
  isOperatorClaimCorrection,
  identifyChallengedClaim,
} = require('../RecommendationClaimChallenge');
const {
  composeEvidenceGroundedRecommendation,
} = require('../OperatingStateRecommendation');
const {
  SUPPORT,
  VERDICT,
  TOPICS,
  evaluateOperatingStateClaim,
} = require('../ClaimGrounding');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');
const HIGHEST_LEVERAGE =
  "Given what's already in motion for Anchor, where is the highest-leverage constraint or opportunity I should focus on next to establish a repeatable commercial pipeline?";
const EMAIL_CHALLENGE =
  'You said outbound email is already active. What evidence tells you that?';
const PLANNED_CHALLENGE = "You said the follow-up already occurred. That isn't true.";
const INVENTORY_CHALLENGE = "You said outreach has begun. That isn't true.";
const GOAL_CHALLENGE = "You said we are expanding our commercial business. That isn't true.";
const SUPPORTED_CHALLENGE = "That's incorrect.";

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
  const objectives =
    overrides.objectives ||
    [
      {
        id: 'obj-1',
        clientId: ANCHOR_ID,
        tenantId: '10',
        title: 'Build a repeatable commercial pipeline',
        status: 'active',
      },
    ];
  return {
    now: overrides.now || NOW,
    loadCampaignAo: async ({ clientId }) => {
      assert.equal(Number(clientId), ANCHOR_ID);
      return {
        available: true,
        campaignName: 'Campaign 001',
        mailExecuted: overrides.mailExecuted === true,
        progress: {
          campaign_name: 'Campaign 001',
          target_total: 20,
          seeded_in_ao: overrides.aoLeads != null ? overrides.aoLeads : 20,
          visited: 0,
          walkthrough_requests: 0,
          escalations: 0,
          remaining_route_queue: 20,
        },
        leads: Array.from({ length: overrides.aoLeads != null ? overrides.aoLeads : 20 }, (_, i) => ({
          id: i + 1,
          client_id: ANCHOR_ID,
          campaign_name: 'Campaign 001',
          operational_state: 'not_started',
          mail_status: overrides.mailExecuted ? 'mailed' : undefined,
        })),
      };
    },
    loadProspects: async () => ({ available: true, counts: prospects }),
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
      rows: (overrides.missions || []).map((row) => ({ ...row, clientId: row.clientId || clientId })),
    }),
    loadObjectives: async () => ({ available: true, rows: objectives }),
    loadActivity: async ({ clientId }) => ({
      available: true,
      touchpoints: overrides.touchpoints || [
        overrides.deliveryLogs
          ? {
              id: 9,
              client_id: clientId,
              channel: 'mail',
              action_type: 'delivery_log',
              status: 'delivered',
            }
          : { id: 1, client_id: clientId, channel: 'phone', action_type: 'call' },
      ],
      activity: overrides.activity || [],
    }),
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
    loadOperatorAttested: async () => ({
      available: true,
      claims: overrides.omitAttested ? [] : attestedClaims(overrides),
    }),
    capability,
  };
}

async function conversation(questions, overrides = {}) {
  const opts = operatingOpts(overrides);
  const session = {
    id: 'spec-108',
    context: {
      tenantId: '10',
      clientId: ANCHOR_ID,
      ...(overrides.sessionContext || {}),
    },
  };
  const turns = [];
  for (const question of questions) {
    turns.push(
      await maybeHandleRetrievalBeforeDelegationTurn({
        question,
        session,
        context: { tenantId: '10', clientId: ANCHOR_ID },
        operatingEvidenceOpts: opts,
        now: opts.now,
      })
    );
  }
  return { session, turns, opts };
}

describe('SPEC-108 classification', () => {
  it('treats generic operator denials as claim challenges, not SPEC-106 writes', () => {
    for (const question of ["That isn't true.", "That's incorrect.", "That's not true."]) {
      assert.equal(isClaimChallenge(question), true, question);
      assert.equal(isOperatorClaimCorrection(question), false, question);
      assert.equal(isOperatorOperatingUpdate(question), false, question);
      assert.equal(classifyCognitiveMode(question).via, 'claim_challenge', question);
      assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.EXPLANATION, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });
});

describe('SPEC-108 Scenario 1 — email activity regression', () => {
  it('still retracts an unsupported active-email claim and revises', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, EMAIL_CHALLENGE]);
    const challenge = turns[1];
    assert.equal(challenge.reason, 'recommendation_claim_challenge');
    assert.equal(challenge.claimVerdict, 'retract');
    assert.match(challenge.prose, /retract/i);
    assert.match(challenge.prose, /REVISED RECOMMENDATION/i);
    assert.doesNotMatch(challenge.prose, /What I can verify/i);
  });
});

describe('SPEC-108 Scenario 2 — planned does not equal completed', () => {
  it('retracts a completed-follow-up claim when only a future schedule exists', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, PLANNED_CHALLENGE], {
      followUpAt: '2026-08-18',
      now: NOW,
    });
    const rec = turns[0];
    const challenge = turns[1];
    assert.match(rec.prose, /scheduled|PLANNED/i);
    assert.doesNotMatch(rec.prose, /follow-up (?:has )?occurred/i);
    assert.equal(challenge.reason, 'recommendation_claim_challenge');
    assert.equal(challenge.claimVerdict, 'retract');
    assert.match(challenge.prose, /You're right/i);
    assert.match(challenge.prose, /planned work as completed/i);
    assert.match(challenge.prose, /no longer believe that claim is supported/i);
    assert.match(challenge.prose, /REVISED RECOMMENDATION/i);
    assert.doesNotMatch(challenge.prose, /follow-up has recorded execution/i);
    assert.doesNotMatch(challenge.prose, /\bsorry\b/i);
  });

  it('classifies a completed-follow-up assertion as unsupported against a schedule', () => {
    const evaluation = evaluateOperatingStateClaim(
      { topic: TOPICS.FOLLOW_UP, text: 'Follow-up occurred.' },
      {
        campaignName: 'Campaign 001',
        followUp: { kind: 'planned_future', expectedAt: '2026-08-18', executed: false },
      }
    );
    assert.equal(evaluation.support, SUPPORT.UNSUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.RETRACT);
    assert.match(evaluation.distinction, /planned work as completed/i);
  });
});

describe('SPEC-108 Scenario 3 — inventory does not equal execution', () => {
  it('retracts outreach-has-begun and recommends first outreach', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, INVENTORY_CHALLENGE], {
      omitAttested: true,
      prospects: { total: 67, qualified: 40 },
      scoutMatched: 67,
      missions: [],
    });
    const rec = turns[0];
    const challenge = turns[1];
    assert.doesNotMatch(rec.prose, /outreach has begun/i);
    assert.equal(challenge.claimVerdict, 'retract');
    assert.match(challenge.prose, /inventory as execution|discovery is not outreach/i);
    assert.match(challenge.prose, /REVISED RECOMMENDATION/i);
    assert.match(challenge.prose, /first outreach/i);
    assert.doesNotMatch(challenge.prose, /find more prospects/i);
    assert.doesNotMatch(challenge.prose, /\bsorry\b/i);
  });

  it('does not treat prospect inventory as outreach in a recommendation', () => {
    const composed = composeEvidenceGroundedRecommendation(
      {
        prospects: { counts: { total: 67, qualified: 40 } },
        scout: { intelligence: { counts: { matched: 67 } } },
        activity: { touchpoints: [], activity: [] },
        missions: [],
        capability: { available: true, enabled_agents: ['scout'], autosend_enabled: false },
      },
      { now: NOW }
    );
    assert.doesNotMatch(composed.prose, /outreach has begun/i);
    const outreach = composed.premises.find((p) => p.topic === TOPICS.OUTREACH_BEGUN);
    assert.ok(outreach);
    assert.equal(outreach.support, SUPPORT.UNSUPPORTED);
  });
});

describe('SPEC-108 Scenario 4 — goals do not equal operating state', () => {
  it('clarifies a Blueprint objective versus observed expansion', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, GOAL_CHALLENGE], {
      objectives: [
        {
          id: 'obj-goal',
          clientId: ANCHOR_ID,
          tenantId: '10',
          title: 'Acquire twenty commercial clients',
          status: 'active',
        },
      ],
    });
    const rec = turns[0];
    const challenge = turns[1];
    assert.doesNotMatch(rec.prose, /you are expanding your commercial business/i);
    assert.equal(challenge.claimVerdict, 'qualified');
    assert.match(challenge.prose, /overstated the current state/i);
    assert.match(challenge.prose, /Acquire twenty commercial clients/i);
    assert.match(challenge.prose, /stated objective/i);
    assert.match(challenge.prose, /not observed/i);
    assert.doesNotMatch(challenge.prose, /I retract that statement/i);
  });
});

describe('SPEC-108 Scenario 5 — supported evidence remains supported', () => {
  it('maintains a campaign-complete claim and presents evidence', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, SUPPORTED_CHALLENGE], {
      mailExecuted: true,
      deliveryLogs: true,
      omitAttested: false,
    });
    const rec = turns[0];
    const challenge = turns[1];
    assert.ok(rec);
    assert.equal(challenge.reason, 'recommendation_claim_challenge');
    assert.equal(challenge.claimVerdict, 'confirmed');
    assert.match(challenge.prose, /still believe this claim is supported/i);
    assert.match(challenge.prose, /Evidence:/i);
    assert.match(challenge.prose, /mailed|operator-attested|delivery/i);
    assert.doesNotMatch(challenge.prose, /I retract that statement/i);
    assert.doesNotMatch(challenge.prose, /REVISED RECOMMENDATION/i);
  });
});

describe('SPEC-108 safety', () => {
  it('does not default a follow-up challenge to the email evaluator', () => {
    const claim = identifyChallengedClaim(PLANNED_CHALLENGE, {
      premises: [
        { id: 'follow_up', topic: 'follow_up', text: 'Follow-up is planned.', support: 'partially_supported' },
        { id: 'email_motion', topic: 'email_motion', text: 'An outbound email motion is already active.' },
      ],
    });
    assert.equal(claim.topic, TOPICS.FOLLOW_UP);
    assert.match(claim.text, /occurred/i);
  });

  it('WorkspaceEngine answers a planned-vs-completed challenge without executing', async () => {
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
    await engine.ask({
      sessionId: opened.sessionId,
      question: HIGHEST_LEVERAGE,
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: PLANNED_CHALLENGE,
    });
    assert.equal(result.domainDecision.reason, 'recommendation_claim_challenge');
    assert.match(result.prose, /retract/i);
    assert.equal(result.structured.metadata.executed, false);
    assert.equal(result.retrieval.delegated, false);
  });
});
