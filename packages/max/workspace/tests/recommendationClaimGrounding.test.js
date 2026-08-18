'use strict';

/**
 * SPEC-107A — recommendation claim grounding and challenge.
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
  shouldRetrieveOperatingEvidence,
} = require('../OperatingEvidenceRetrieval');
const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
  looksLikeClaimChallenge,
} = require('../../specialistDelegation/CognitiveMode');
const { isOperatorOperatingUpdate } = require('../OperatorOperatingUpdate');
const {
  isClaimChallenge,
  isOperatorClaimCorrection,
  identifyChallengedClaim,
} = require('../RecommendationClaimChallenge');
const {
  assessEmailMotion,
  assembleOperatingState,
  composeEvidenceGroundedRecommendation,
} = require('../OperatingStateRecommendation');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');
const HIGHEST_LEVERAGE =
  "Given what's already in motion for Anchor, where is the highest-leverage constraint or opportunity I should focus on next to establish a repeatable commercial pipeline?";
const EVIDENCE_CHALLENGE = 'What evidence supports that?';
const EMAIL_CHALLENGE =
  'You said outbound email is already active. What evidence tells you that?';
const EMAIL_CORRECTION = "No, email outbound isn't running right now.";
const INVENTORY_EVIDENCE = 'What evidence do we already have?';

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
  const missions =
    overrides.missions ||
    [
      {
        id: 'msn-1',
        clientId: ANCHOR_ID,
        tenantId: '10',
        title: 'Campaign 001 preparation',
        status: 'planned',
      },
    ];
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
          walkthrough_requests: 0,
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
      rows: missions.map((row) => ({ ...row, clientId: row.clientId || clientId })),
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
      touchpoints: overrides.touchpoints || [
        { id: 1, client_id: clientId, channel: 'phone', action_type: 'call' },
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
    id: 'spec-107a',
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

function challengeBody(prose) {
  return String(prose || '').split(/REVISED RECOMMENDATION/i)[0];
}

describe('SPEC-107A classification', () => {
  it('treats claim challenges as targeted explanation, not inventory retrieval', () => {
    for (const question of [
      EVIDENCE_CHALLENGE,
      EMAIL_CHALLENGE,
      'Where did you get that?',
      'How do you know that?',
      'Can you verify that?',
      'Why do you think email is active?',
      "That's not right.",
    ]) {
      assert.equal(isClaimChallenge(question), true, question);
      assert.equal(isOperatingEvidenceQuestion(question), false, question);
      assert.equal(looksLikeClaimChallenge(question), true, question);
      assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.EXPLANATION, question);
      assert.equal(classifyCognitiveMode(question).via, 'claim_challenge', question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });

  it('keeps inventory evidence questions as SPEC-105 retrieval', () => {
    assert.equal(isClaimChallenge(INVENTORY_EVIDENCE), false);
    assert.equal(isOperatingEvidenceQuestion(INVENTORY_EVIDENCE), true);
    assert.equal(shouldRetrieveOperatingEvidence(INVENTORY_EVIDENCE), true);
    assert.equal(classifyCognitiveMode(INVENTORY_EVIDENCE).via, 'operating_evidence');
  });

  it('does not persist operator claim corrections as SPEC-106 campaign events', () => {
    assert.equal(isOperatorClaimCorrection(EMAIL_CORRECTION), true);
    assert.equal(isOperatorOperatingUpdate(EMAIL_CORRECTION), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(EMAIL_CORRECTION, null, { approvedBlueprint: true }),
      false
    );
  });
});

describe('SPEC-107A TEST A — supported claim', () => {
  it('traces the mailed-August-6 claim to SPEC-106 operator-attested evidence', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, EVIDENCE_CHALLENGE]);
    const rec = turns[0];
    const challenge = turns[1];
    assert.ok(rec);
    assert.match(rec.prose, /reported as physically mailed on 2026-08-06/i);
    assert.ok(challenge);
    assert.equal(challenge.reason, 'recommendation_claim_challenge');
    assert.equal(challenge.claimVerdict, 'confirmed');
    assert.match(challenge.prose, /operator-attested|operator report|SPEC-106/i);
    assert.match(challenge.prose, /2026-08-06|August 6/i);
    assert.doesNotMatch(challenge.prose, /I retract that statement/i);
    assert.doesNotMatch(challenge.prose, /What I can verify/i);
  });
});

describe('SPEC-107A TEST B — unsupported claim', () => {
  it('retrieves claim-relevant evidence, retracts, and does not dump inventory', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, EMAIL_CHALLENGE]);
    const challenge = turns[1];
    assert.ok(challenge);
    assert.equal(challenge.reason, 'recommendation_claim_challenge');
    assert.equal(challenge.claimVerdict, 'retract');
    assert.equal(challenge.executed, false);
    const body = challengeBody(challenge.prose);
    assert.match(body, /can't verify|cannot verify/i);
    assert.match(body, /retract/i);
    assert.match(body, /outbound email motion is currently active/i);
    assert.doesNotMatch(body, /What I can verify/i);
    assert.doesNotMatch(body, /Ask for a recommendation only after reviewing this inventory/i);
    assert.doesNotMatch(body, /71 prospect/i);
    assert.doesNotMatch(challenge.prose, /Ask for a recommendation only after reviewing this inventory/i);
    assert.equal(challenge.structured.metadata.claimChallenge, true);
    assert.deepEqual(challenge.structured.nextInvestigations, []);
  });
});

describe('SPEC-107A TEST C — historical != active', () => {
  it('does not call email motion active from historical sends', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE], {
      touchpoints: [
        { id: 1, client_id: ANCHOR_ID, channel: 'email', action_type: 'email_sent', status: 'sent' },
        { id: 2, client_id: ANCHOR_ID, channel: 'emmett', action_type: 'email_sent' },
      ],
    });
    const rec = turns[0];
    assert.ok(rec);
    assert.match(rec.prose, /historical/i);
    assert.doesNotMatch(rec.prose, /outbound email motion is already active/i);
    const motion = assessEmailMotion({
      activity: {
        touchpoints: [
          { channel: 'email', action_type: 'email_sent', status: 'sent' },
        ],
        activity: [],
      },
    }, { enabled_agents: ['scout'], autosend_enabled: false });
    assert.equal(motion.current, false);
    assert.equal(motion.kind, 'historical');
  });
});

describe('SPEC-107A TEST D — mission != execution', () => {
  it('labels an email mission as planned or intended work', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE], {
      missions: [
        {
          id: 'msn-email',
          clientId: ANCHOR_ID,
          tenantId: '10',
          title: 'Email outbound campaign for qualified prospects',
          status: 'planned',
        },
      ],
    });
    const rec = turns[0];
    assert.match(rec.prose, /planned\/intent|planned or intended|mission is not recorded execution/i);
    assert.doesNotMatch(rec.prose, /outbound email motion is already active/i);
    const motion = assessEmailMotion({
      missions: [{ title: 'Email outbound campaign', status: 'planned' }],
      activity: { touchpoints: [], activity: [] },
    }, { enabled_agents: ['scout'] });
    assert.equal(motion.current, false);
    assert.equal(motion.kind, 'planned');
  });
});

describe('SPEC-107A TEST E — disabled capability', () => {
  it('describes Emmett as disabled, not active', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE]);
    const rec = turns[0];
    assert.match(rec.prose, /disabled|not in enabled agents/i);
    assert.doesNotMatch(rec.prose, /outbound email motion is already active/i);
    const state = assembleOperatingState(
      {
        capability: { available: true, enabled_agents: ['scout'], autosend_enabled: false },
        activity: { touchpoints: [], activity: [] },
        missions: [],
        prospects: { counts: { total: 71, qualified: 54 } },
      },
      {}
    );
    assert.equal(state.emailMotion.kind, 'disabled');
    assert.equal(state.emailMotionActive, false);
  });
});

describe('SPEC-107A TEST F — reasoning revision', () => {
  it('reassesses the highest-leverage constraint after retracting active email', async () => {
    const { turns } = await conversation([HIGHEST_LEVERAGE, EMAIL_CHALLENGE]);
    const challenge = turns[1];
    assert.equal(challenge.claimVerdict, 'retract');
    assert.match(challenge.prose, /REVISED RECOMMENDATION/i);
    assert.match(challenge.prose, /Evaluate and prepare controlled|outbound email|activation/i);
    assert.doesNotMatch(challenge.prose, /find more prospects/i);
    assert.doesNotMatch(challenge.prose, /a current outbound email motion is executing/i);
  });
});

describe('SPEC-107A TEST G — operator correction', () => {
  it('accepts an operator-attested working-model correction and stops asserting active email', async () => {
    const { session, turns } = await conversation([
      HIGHEST_LEVERAGE,
      EMAIL_CORRECTION,
      HIGHEST_LEVERAGE,
    ]);
    const correction = turns[1];
    const revised = turns[2];
    assert.ok(correction);
    assert.equal(correction.reason, 'recommendation_claim_challenge');
    assert.match(correction.prose, /operator-attested correction/i);
    assert.match(correction.prose, /will not continue asserting/i);
    assert.equal(session.context.operatorDeniedEmailActive, true);
    assert.ok(session.context.retractedPremises.includes('email_motion'));
    assert.ok(revised);
    assert.match(revised.prose, /RECOMMENDATION/i);
    assert.doesNotMatch(revised.prose, /outbound email motion is already active/i);
    assert.equal(isOperatorOperatingUpdate(EMAIL_CORRECTION), false);
  });
});

describe('SPEC-107A safety', () => {
  it('does not hard-code Anchor or Emmett conclusions in the challenge path', () => {
    const recSrc = fs.readFileSync(
      path.join(__dirname, '..', 'RecommendationClaimChallenge.js'),
      'utf8'
    );
    const stateSrc = fs.readFileSync(
      path.join(__dirname, '..', 'OperatingStateRecommendation.js'),
      'utf8'
    );
    assert.doesNotMatch(recSrc, /client_id\s*===?\s*10/);
    assert.doesNotMatch(stateSrc, /clientId\s*===?\s*10/);
    assert.doesNotMatch(recSrc, /recommend Emmett/);
  });

  it('does not treat Max-generated statements as evidence when identifying a claim', () => {
    const claim = identifyChallengedClaim(EMAIL_CHALLENGE, {
      premises: [
        {
          id: 'email_motion',
          topic: 'email_motion',
          text: 'An outbound email motion is already active.',
          kind: 'inferred',
          support: 'unknown',
        },
      ],
    });
    assert.equal(claim.topic, 'email_motion');
    const composed = composeEvidenceGroundedRecommendation(
      {
        prospects: { counts: { total: 71, qualified: 54 } },
        scout: { intelligence: { counts: { matched: 69 } } },
        activity: {
          touchpoints: [{ channel: 'email', action_type: 'email_sent' }],
          activity: [],
        },
        missions: [],
        capability: { available: true, enabled_agents: ['scout'], autosend_enabled: false },
        operatorAttested: {
          mail: { occurredAt: '2026-08-06' },
          followUp: { expectedAt: '2026-08-18' },
        },
      },
      { now: NOW }
    );
    const emailPremise = composed.premises.find((p) => p.topic === 'email_motion');
    assert.equal(emailPremise.support, 'unknown');
    assert.doesNotMatch(composed.prose, /outbound email motion is already active/i);
  });

  it('WorkspaceEngine answers a challenge without executing', async () => {
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
      question: EMAIL_CHALLENGE,
    });
    assert.equal(result.domainDecision.reason, 'recommendation_claim_challenge');
    assert.match(result.prose, /retract/i);
    assert.equal(result.structured.metadata.executed, false);
    assert.equal(result.retrieval.delegated, false);
  });
});
