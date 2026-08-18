'use strict';

/**
 * SPEC-109 — intent-bound response selection.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
  isOperatorDeskWorkflowQuestion,
} = require('../RetrievalBeforeDelegationContext');
const {
  shouldClaimClientIntelligenceTurn,
} = require('../ClientIntelligenceContext');
const { shouldHandleScoutAcquisition } = require('../ScoutAcquisitionContext');
const { isOperatorOperatingUpdate } = require('../OperatorOperatingUpdate');
const {
  CONTRACT_IDS,
  selectResponseContract,
  listResponseContracts,
  containsForbidden,
  looksLikeSummary,
  looksLikeCompletedRetrieval,
} = require('../ResponseContract');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../../specialistDelegation/RetrievalGate');
const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../../../../services/specialistDelegation');
const { createMemoryAcquisitionState } = require('../../../../services/scoutAcquisitionIntelligence');

const ANCHOR_ID = 10;
const NOW = new Date('2026-08-17T15:00:00.000Z');

const RETRIEVAL_OUTREACH = 'What outreach has already been sent?';
const RETRIEVAL_COMPLETED = 'What have we completed recently?';
const SUMMARY_ANCHOR = 'How is Anchor Cleaning doing?';
const RECOMMENDATION_NEXT = 'What should we do next?';
const CHALLENGE = "That's incorrect.";
const INVESTIGATION = 'Investigate commercial prospects.';

function operatingOpts(overrides = {}) {
  return {
    now: overrides.now || NOW,
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
          visited: 6,
          walkthrough_requests: 1,
          remaining_route_queue: 14,
        },
        leads: [
          {
            id: 1,
            client_id: ANCHOR_ID,
            campaign_name: 'Campaign 001',
            operational_state: 'walkthrough_requested',
            mail_status: 'mailed',
          },
        ],
      };
    },
    loadProspects: async () => ({
      available: true,
      counts: { total: 18, qualified: 7, cold: 11, warm: 5, hot: 2 },
    }),
    loadScout: async () => ({
      available: true,
      launchedNewWork: false,
      intelligence: { counts: { considered: 4, matched: 2 } },
      state: { tenantId: '10', opportunityCount: 2 },
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
      touchpoints: [{ id: 1, client_id: clientId, channel: 'mail', action_type: 'delivery_log' }],
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

async function retrieveTurn(question, extras = {}) {
  const context = sessionContext();
  return maybeHandleRetrievalBeforeDelegationTurn({
    question,
    session: { id: 'spec-109', context },
    context,
    operatingEvidenceOpts: operatingOpts(),
    now: NOW,
    ...extras,
  });
}

describe('SPEC-109 response contract registry', () => {
  it('registers the five operator-facing contracts with required/optional/forbidden sections', () => {
    const ids = listResponseContracts().map((c) => c.id);
    assert.deepEqual(ids, [
      CONTRACT_IDS.RETRIEVAL,
      CONTRACT_IDS.SUMMARY,
      CONTRACT_IDS.RECOMMENDATION,
      CONTRACT_IDS.CHALLENGE,
      CONTRACT_IDS.INVESTIGATION,
    ]);
    const retrieval = listResponseContracts().find((c) => c.id === CONTRACT_IDS.RETRIEVAL);
    assert.ok(retrieval.required.includes('verified_state'));
    assert.ok(retrieval.required.includes('unknowns'));
    assert.ok(retrieval.forbidden.includes('unsolicited_strategy'));
    assert.equal(retrieval.permitsRecommendation, false);
  });
});

describe('SPEC-109 contract selection before delegation', () => {
  it('selects Retrieval for completed/sent operating-state questions', () => {
    for (const question of [RETRIEVAL_OUTREACH, RETRIEVAL_COMPLETED]) {
      const mode = classifyCognitiveMode(question);
      const contract = selectResponseContract(question, mode);
      assert.equal(mode.kind, COGNITIVE_MODES.RETRIEVAL, question);
      assert.equal(contract.id, CONTRACT_IDS.RETRIEVAL, question);
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
      assert.equal(isOperatorOperatingUpdate(question), false, question);
    }
    assert.equal(looksLikeCompletedRetrieval(RETRIEVAL_COMPLETED), true);
    assert.equal(looksLikeCompletedRetrieval(RETRIEVAL_OUTREACH), true);
  });

  it('selects Summary for how-is-the-business-doing questions', () => {
    const mode = classifyCognitiveMode(SUMMARY_ANCHOR);
    const contract = selectResponseContract(SUMMARY_ANCHOR, mode);
    assert.equal(looksLikeSummary(SUMMARY_ANCHOR), true);
    assert.equal(mode.via, 'summary');
    assert.equal(contract.id, CONTRACT_IDS.SUMMARY);
    assert.equal(shouldInvokeSpecialist(SUMMARY_ANCHOR), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(SUMMARY_ANCHOR, null, { approvedBlueprint: true }),
      false
    );
  });

  it('selects Recommendation when the operator asks what to do next', () => {
    const mode = classifyCognitiveMode(RECOMMENDATION_NEXT);
    const contract = selectResponseContract(RECOMMENDATION_NEXT, mode);
    assert.equal(mode.kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.equal(contract.id, CONTRACT_IDS.RECOMMENDATION);
    assert.equal(contract.recommendationPrimary, true);
  });

  it('selects Challenge for that-is-incorrect', () => {
    const mode = classifyCognitiveMode(CHALLENGE);
    const contract = selectResponseContract(CHALLENGE, mode);
    assert.equal(mode.via, 'claim_challenge');
    assert.equal(contract.id, CONTRACT_IDS.CHALLENGE);
    assert.equal(shouldInvokeSpecialist(CHALLENGE), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(CHALLENGE, null, { approvedBlueprint: true }),
      false
    );
  });

  it('selects Investigation and does not answer from CIE memory', () => {
    const mode = classifyCognitiveMode(INVESTIGATION);
    const contract = selectResponseContract(INVESTIGATION, mode);
    assert.equal(mode.kind, COGNITIVE_MODES.INVESTIGATION);
    assert.equal(contract.id, CONTRACT_IDS.INVESTIGATION);
    assert.equal(shouldHandleScoutAcquisition({ question: INVESTIGATION, context: { tenantId: '10' } }), true);
    const session = { context: { tenantId: '10', responseContract: contract } };
    assert.equal(
      shouldClaimClientIntelligenceTurn(INVESTIGATION, session, { approvedBlueprint: true }),
      false
    );
  });
});

describe('SPEC-109 Retrieval — What outreach has already been sent?', () => {
  it('returns operating state and does not immediately recommend strategy', async () => {
    const turn = await retrieveTurn(RETRIEVAL_OUTREACH);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RETRIEVAL);
    assert.match(turn.prose, /What I can verify|Recently completed|Verified state/i);
    assert.match(turn.prose, /Unknown|What I cannot verify/i);
    assert.match(turn.prose, /Campaign 001|outreach|mailed|sent/i);
    assert.doesNotMatch(turn.prose, /I'd recommend proving a repeatable/i);
    assert.doesNotMatch(turn.prose, /I'd recommend a focused first campaign/i);
    assert.equal(containsForbidden(turn.prose, turn.responseContract).length, 0);
    const recIdx = turn.prose.search(/I'd recommend|RECOMMENDATION:/i);
    const stateIdx = turn.prose.search(/What I can verify|Recently completed|Campaign 001/i);
    if (recIdx >= 0) assert.ok(stateIdx >= 0 && stateIdx < recIdx);
  });
});

describe('SPEC-109 Retrieval — What have we completed recently?', () => {
  it('answers with completed operating state instead of Blueprint advisory', async () => {
    const turn = await retrieveTurn(RETRIEVAL_COMPLETED);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RETRIEVAL);
    assert.match(turn.prose, /Recently completed/i);
    assert.match(turn.prose, /Campaign 001/i);
    assert.doesNotMatch(turn.prose, /I'd start by proving a repeatable/i);
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
      question: RETRIEVAL_COMPLETED,
    });
    assert.match(result.prose, /Recently completed|What I can verify|Campaign 001/i);
    assert.doesNotMatch(result.prose, /I'd recommend proving a repeatable acquisition motion/i);
    assert.equal(result.structured.metadata.responseContract, CONTRACT_IDS.RETRIEVAL);
  });
});

describe('SPEC-109 Summary — How is Anchor Cleaning doing?', () => {
  it('separates observed state, goals, and unknowns; recommendation is last and optional', async () => {
    const turn = await retrieveTurn(SUMMARY_ANCHOR);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.SUMMARY);
    assert.match(turn.prose, /Observed operating state/i);
    assert.match(turn.prose, /Goals/i);
    assert.match(turn.prose, /Unknowns/i);
    const stateIdx = turn.prose.search(/Observed operating state/i);
    const recIdx = turn.prose.search(/\nRecommendation\n/i);
    assert.ok(stateIdx >= 0);
    if (recIdx >= 0) assert.ok(stateIdx < recIdx);
    assert.doesNotMatch(turn.prose, /^I'd start by proving a repeatable/i);
  });
});

describe('SPEC-109 Recommendation — What should we do next?', () => {
  it('makes recommendation primary after retrieving current state', async () => {
    const turn = await retrieveTurn(RECOMMENDATION_NEXT);
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RECOMMENDATION);
    assert.match(turn.prose, /RECOMMENDATION/i);
    assert.match(turn.prose, /Current state|WHAT'S ALREADY IN MOTION/i);
    assert.match(turn.prose, /WHY NOW|Reasoning/i);
    assert.match(turn.prose, /Confidence/i);
    assert.match(turn.prose, /Evidence/i);
    assert.equal(turn.structured.metadata.evidenceGroundedRecommendation, true);
  });
});

describe('SPEC-109 Challenge — That\'s incorrect.', () => {
  it('revises reasoning under the challenge contract', async () => {
    const context = {
      ...sessionContext(),
      lastRecommendation: {
        lastClaim: {
          id: 'email_motion',
          topic: 'email_motion',
          text: 'An outbound email motion is already active.',
          kind: 'inferred',
          support: 'unsupported',
        },
        premises: [
          {
            id: 'email_motion',
            topic: 'email_motion',
            text: 'An outbound email motion is already active.',
            kind: 'inferred',
            support: 'unsupported',
          },
        ],
        recommendation: 'Activate email.',
        prose: 'I would activate email.',
      },
    };
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: CHALLENGE,
      session: { id: 'spec-109-challenge', context },
      context,
      operatingEvidenceOpts: operatingOpts(),
      now: NOW,
    });
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.CHALLENGE);
    assert.match(turn.prose, /Claim identified/i);
    assert.match(turn.prose, /Evidence reviewed/i);
    assert.match(turn.prose, /Revision/i);
    assert.match(turn.prose, /Updated recommendation/i);
    assert.match(turn.prose, /retract|qualify|confirm|You're right|still believe/i);
  });
});

describe('SPEC-109 Investigation — Investigate commercial prospects.', () => {
  it('creates investigation structure and does not answer from unsupported memory', async () => {
    const store = createMemoryStore();
    const service = createSpecialistDelegationService({ store });
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatingEvidenceOpts: operatingOpts(),
      specialistDelegationService: service,
      specialistDelegationOpts: { store },
      scoutAcquisitionOpts: {
        aoStore: createMemoryAcquisitionState(),
        companies: [
          {
            id: 'co-granite',
            tenantId: '10',
            name: 'Granite State Property Management',
            industry: 'property_management',
            location: 'Manchester, NH',
          },
        ],
      },
    });
    const opened = engine.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
      domainId: 'acquisition',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: INVESTIGATION,
    });
    assert.match(result.prose, /Known/i);
    assert.match(result.prose, /Need specialist\?/i);
    assert.match(result.prose, /Expected outputs/i);
    assert.doesNotMatch(result.prose, /I'd start by proving a repeatable acquisition motion/i);
    assert.doesNotMatch(result.prose, /I'd recommend a focused first campaign/i);
    const delegated = result.structured && result.structured.metadata && result.structured.metadata.scoutDelegated;
    const contract = result.structured && result.structured.metadata && result.structured.metadata.responseContract;
    assert.ok(delegated === true || contract === CONTRACT_IDS.INVESTIGATION);
  });
});

describe('SPEC-109 does not replace other operator contracts', () => {
  it('keeps focused mail-status retrieval instead of a generic inventory dump', async () => {
    const turn = await retrieveTurn('When was Campaign 001 mailed?');
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RETRIEVAL);
    assert.match(turn.prose, /operator-reported as mailed 2026-08-06/i);
    assert.match(turn.prose, /What I can verify/i);
  });

  it('does not let retrieval composition intercept preparation-only canary desk work', async () => {
    const question =
      'Summarize the Campaign 001 preparation-only canary status across PM-001, PM-002, and PM-003.';
    assert.equal(isOperatorDeskWorkflowQuestion(question), true);
    const turn = await retrieveTurn(question);
    assert.equal(turn, null);
  });
});

describe('SPEC-109 spec file exists', () => {
  it('keeps SPEC-109 in docs/specs', () => {
    const spec = path.join(__dirname, '../../../../docs/specs/SPEC-109_Intent_Bound_Response_Selection.md');
    assert.equal(fs.existsSync(spec), true);
  });
});
