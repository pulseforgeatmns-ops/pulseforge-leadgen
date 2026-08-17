'use strict';

/**
 * SPEC-105 — Max operating evidence retrieval.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
  isHardRetrievalQuestion,
} = require('../RetrievalBeforeDelegationContext');
const {
  maybeHandleClientIntelligenceTurn,
  shouldClaimClientIntelligenceTurn,
} = require('../ClientIntelligenceContext');
const {
  maybeHandleScoutAcquisitionTurn,
  shouldHandleScoutAcquisition,
} = require('../ScoutAcquisitionContext');
const {
  isOperatingEvidenceQuestion,
  isOperatingGroundedRecommendation,
  shouldRetrieveOperatingEvidence,
  isInventoryOnlyRequest,
  loadOperatingEvidence,
  composeOperatingEvidenceAnswer,
  hasDurableMailExecution,
  EPISTEMIC,
  CAMPAIGN_LAYER,
} = require('../OperatingEvidenceRetrieval');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');
const {
  createMemoryStore: createDelegationStore,
  createSpecialistDelegationService,
} = require('../../../../services/specialistDelegation');
const { createMemoryAcquisitionState } = require('../../../../services/scoutAcquisitionIntelligence');

const ANCHOR_ID = 10;
const PULSEFORGE_ID = 1;
const AS_CLEANING_ID = 11;

const ANCHOR_ANSWERS = [
  'Anchor Cleaning — commercial cleaning for professional offices.',
  'Recurring commercial cleaning and weekly office cleans.',
  'Property managers, facility managers, and professional offices.',
  'Lowest-price bargain hunters.',
  'Greater Manchester including Bedford and Hooksett.',
  'Reliable crews that do the work right without chasing.',
  'Calm professional reliable voice.',
  'Grow commercial cleaning in Greater Manchester.',
  'Clearer path to commercial opportunities in 90 days.',
];

const PILOT_INVENTORY =
  "Don't recommend a new acquisition motion yet. First investigate what PulseForge already knows about Anchor's past and current acquisition activity. I want an evidence-based inventory of campaigns, prospects, outreach, leads, walkthroughs, and outcomes already recorded in the system. Tell me what you can verify, what you cannot verify, and where each piece of evidence came from.";

const PILOT_RECOMMEND =
  "Given what we've already tried and what PulseForge can actually verify, what should I focus on next to build the repeatable commercial pipeline?";

async function approveAnchor(store) {
  const opts = { store };
  const started = await startClientInterview({ clientId: ANCHOR_ID, forceNew: true }, opts);
  let turn = started;
  for (const answer of ANCHOR_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  await approveBlueprint(turn.blueprint.id, opts);
  return opts;
}

function anchorOperatingOpts() {
  let scoutLaunched = 0;
  return {
    scoutLaunched: () => scoutLaunched,
    opts: {
      loadCampaignAo: async ({ clientId }) => {
        assert.equal(Number(clientId), ANCHOR_ID);
        return {
          available: true,
          campaignName: 'Campaign 001',
          progress: {
            campaign_name: 'Campaign 001',
            target_total: 20,
            seeded_in_ao: 20,
            visited: 6,
            walkthrough_requests: 1,
            escalations: 2,
            remaining_route_queue: 14,
          },
          leads: [
            {
              id: 1,
              client_id: ANCHOR_ID,
              campaign_name: 'Campaign 001',
              operational_state: 'walkthrough_requested',
              original_visit_note: 'Seeded for Campaign 001 direct-mail follow-up.',
            },
            {
              id: 2,
              client_id: ANCHOR_ID,
              campaign_name: 'Campaign 001',
              operational_state: 'not_started',
            },
          ],
        };
      },
      loadProspects: async ({ clientId }) => {
        assert.equal(Number(clientId), ANCHOR_ID);
        return {
          available: true,
          counts: { total: 18, qualified: 7, cold: 11, warm: 5, hot: 2, setter_visible: 7, booked: 0, closed: 0 },
          segments: [{ vertical: 'property_management', count: 8 }],
        };
      },
      loadScout: async () => {
        scoutLaunched += 1;
        return {
          available: true,
          launchedNewWork: false,
          intelligence: {
            counts: { considered: 4, matched: 2, rejected: 2 },
            companies: [{ id: 'co-1', tenantId: '10', name: 'Granite State Property Management' }],
          },
          state: { tenantId: '10', opportunityCount: 2, summary: 'Existing Scout state' },
        };
      },
      loadMissions: async ({ clientId }) => ({
        available: true,
        rows: [
          {
            id: 'msn-1',
            clientId,
            tenantId: String(clientId),
            title: 'Campaign 001 preparation',
            status: 'planned',
            createdAt: '2026-06-01T00:00:00.000Z',
          },
        ],
      }),
      loadObjectives: async ({ clientId }) => ({
        available: true,
        rows: [
          {
            id: 'obj-1',
            clientId,
            tenantId: String(clientId),
            title: 'Build a repeatable commercial pipeline',
            status: 'active',
          },
        ],
      }),
      loadActivity: async ({ clientId }) => ({
        available: true,
        touchpoints: [
          { id: 1, client_id: clientId, channel: 'phone', action_type: 'call', outcome: 'voicemail' },
        ],
        activity: [
          { id: 2, client_id: clientId, action_type: 'call', notes: 'AO follow-up' },
        ],
      }),
      loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
    },
  };
}

describe('SPEC-105 classification / routing', () => {
  const retrievalPrompts = [
    'What campaigns have we run?',
    'What evidence do we already have?',
    'What acquisition activity is already recorded?',
    'What happened with Campaign 001?',
    'Before recommending anything, tell me what we\'ve already tried.',
    PILOT_INVENTORY,
  ];

  for (const question of retrievalPrompts) {
    it(`classifies as retrieval: ${question.slice(0, 64)}`, () => {
      assert.equal(isOperatingEvidenceQuestion(question), true, question);
      assert.equal(shouldRetrieveOperatingEvidence(question), true, question);
      const mode = classifyCognitiveMode(question);
      assert.equal(mode.kind, COGNITIVE_MODES.RETRIEVAL, question);
      assert.equal(isHardRetrievalQuestion(question, mode), true, question);
    });
  }

  it('CIE does not claim explicit operating-evidence inventory turns', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const turn = await maybeHandleClientIntelligenceTurn({
      question: PILOT_INVENTORY,
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      cieOpts,
    });
    assert.equal(turn.handled, false);
    assert.equal(turn.skipReason, 'operating_evidence');
    assert.equal(shouldClaimClientIntelligenceTurn(PILOT_INVENTORY, null, { approvedBlueprint: true }), false);
    assert.doesNotMatch(String(turn.prose || ''), /KNOWN|INFERENCE|UNKNOWN|EVIDENCE NEEDED/);
  });

  it('does not treat business-understanding questions as operating evidence', () => {
    assert.equal(
      isOperatingEvidenceQuestion('What do you currently understand about Anchor Cleaning?'),
      false
    );
    assert.equal(isOperatingEvidenceQuestion('Who are our ideal customers?'), false);
    assert.equal(isOperatingEvidenceQuestion('What campaign would you recommend?'), false);
    assert.equal(
      isOperatingEvidenceQuestion(
        'Which property managers in the GTA are showing buying signals right now?'
      ),
      false
    );
  });
});

describe('SPEC-105 existing evidence', () => {
  let session;
  let loaders;

  beforeEach(() => {
    loaders = anchorOperatingOpts();
    session = { id: 'sess-105', context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID } };
  });

  it('existing AO Campaign 001 evidence reaches Workspace Max', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What happened with Campaign 001?',
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /Campaign 001/i);
    assert.match(turn.prose, /20 AO lead/i);
    assert.match(turn.prose, /Verified from Campaign 001 AO records/i);
    assert.equal(turn.delegated, false);
    assert.equal(turn.launchedScout, false);
  });

  it('existing prospects can be retrieved without launching Scout', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What prospects do we already have?',
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /18 existing prospect/i);
    assert.match(turn.prose, /prospect repository/i);
    assert.equal(turn.launchedScout, false);
    assert.notEqual(turn.structured.metadata.scoutDelegated, true);
    assert.equal(turn.structured.metadata.specialistDelegated, false);
    assert.match(turn.prose, /do not need Scout to rediscover/i);
  });

  it('existing missions are distinguishable from executed external actions', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What acquisition activity is already recorded?',
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.match(turn.prose, /Campaign 001 preparation/i);
    assert.match(turn.prose, /not proof that an external action occurred/i);
  });

  it('existing objectives are distinguishable from outcomes', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'Show me our existing acquisition activity.',
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.match(turn.prose, /operator objective/i);
    assert.match(turn.prose, /not evidence that activity occurred/i);
  });

  it('existing activity/touchpoints are tenant-scoped', async () => {
    const leakOpts = {
      ...loaders.opts,
      loadActivity: async ({ clientId }) => ({
        available: true,
        touchpoints: [
          { id: 1, client_id: clientId, channel: 'phone', action_type: 'call' },
          { id: 99, client_id: PULSEFORGE_ID, channel: 'email', action_type: 'open' },
        ],
        activity: [{ id: 2, client_id: AS_CLEANING_ID, action_type: 'call' }],
      }),
    };
    const bundle = await loadOperatingEvidence({
      context: session.context,
      operatingEvidenceOpts: leakOpts,
    });
    assert.equal(bundle.activity.touchpoints.length, 1);
    assert.equal(bundle.activity.touchpoints[0].client_id, ANCHOR_ID);
    assert.equal(bundle.activity.activity.length, 0);
  });

  it('existing Scout intelligence can be retrieved without new Scout work', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What acquisition activity is already recorded?',
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.match(turn.prose, /Existing Scout intelligence/i);
    assert.match(turn.prose, /not a new investigation/i);
    assert.equal(turn.launchedScout, false);
  });
});

describe('SPEC-105 epistemic safety', () => {
  it('campaign intent does not imply campaign execution', () => {
    assert.equal(hasDurableMailExecution([{ original_visit_note: 'mail this week' }]), false);
    assert.equal(hasDurableMailExecution([{ operational_state: 'not_started' }]), false);
  });

  it('AO seed note does not prove physical mail execution', async () => {
    const loaders = anchorOperatingOpts();
    const composed = composeOperatingEvidenceAnswer(
      'What happened with Campaign 001?',
      await loadOperatingEvidence({
        context: { tenantId: '10', clientId: 10 },
        operatingEvidenceOpts: loaders.opts,
      })
    );
    assert.match(composed.prose, /physically mailed/i);
    assert.doesNotMatch(composed.prose, /Campaign 001 was mailed/i);
    const mailItem = composed.items.find((item) => /physically mailed/i.test(item.claim));
    assert.ok(mailItem);
    assert.equal(mailItem.epistemic, EPISTEMIC.NOT_RECORDED);
    assert.equal(mailItem.layer, CAMPAIGN_LAYER.INTENT);
  });

  it('missing mail execution produces NOT RECORDED', async () => {
    const loaders = anchorOperatingOpts();
    const bundle = await loadOperatingEvidence({
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: loaders.opts,
    });
    const mail = bundle.items.find((item) => /physically mailed/i.test(item.claim));
    assert.equal(mail.epistemic, EPISTEMIC.NOT_RECORDED);
  });

  it('missing Yelp data is not invented', async () => {
    const loaders = anchorOperatingOpts();
    const composed = composeOperatingEvidenceAnswer(
      'What evidence do we already have?',
      await loadOperatingEvidence({
        context: { tenantId: '10', clientId: 10 },
        operatingEvidenceOpts: loaders.opts,
      })
    );
    assert.match(composed.prose, /No durable Yelp activity/i);
    assert.doesNotMatch(composed.prose, /Yelp campaign produced/i);
  });

  it('missing walkthrough/conversion records are reported as missing when absent', async () => {
    const loaders = anchorOperatingOpts();
    loaders.opts.loadCampaignAo = async () => ({
      available: true,
      campaignName: 'Campaign 001',
      progress: {
        campaign_name: 'Campaign 001',
        seeded_in_ao: 20,
        visited: 0,
        walkthrough_requests: 0,
        escalations: 0,
      },
      leads: [{ id: 1, client_id: 10, operational_state: 'not_started' }],
    });
    const composed = composeOperatingEvidenceAnswer(
      PILOT_INVENTORY,
      await loadOperatingEvidence({
        context: { tenantId: '10', clientId: 10 },
        operatingEvidenceOpts: loaders.opts,
      })
    );
    assert.match(composed.prose, /No walkthrough-request/i);
    assert.match(composed.prose, /No durable conversion, job, or payment outcomes/i);
  });

  it('inference is labeled separately from verified fact', async () => {
    const loaders = anchorOperatingOpts();
    const composed = composeOperatingEvidenceAnswer(
      PILOT_INVENTORY,
      await loadOperatingEvidence({
        context: { tenantId: '10', clientId: 10 },
        operatingEvidenceOpts: loaders.opts,
      })
    );
    assert.match(composed.prose, /What I can verify/i);
    assert.match(composed.prose, /labeling interpretation separately from verified fact/i);
    const inferred = composed.items.filter((item) => item.epistemic === EPISTEMIC.INFERRED);
    assert.ok(inferred.length >= 1);
  });
});

describe('SPEC-105 delegation', () => {
  it('existing-evidence question does not invoke Scout unnecessarily', async () => {
    const loaders = anchorOperatingOpts();
    const session = { id: 's', context: { tenantId: '10', clientId: 10, domainId: 'acquisition' } };
    assert.equal(shouldHandleScoutAcquisition({ question: PILOT_INVENTORY, session, context: session.context }), false);
    const store = createDelegationStore();
    const service = createSpecialistDelegationService({ store });
    const before = (await store.listDelegations({ tenantId: '10' })).length;
    const scoutTurn = await maybeHandleScoutAcquisitionTurn({
      question: PILOT_INVENTORY,
      session,
      context: session.context,
      delegationService: service,
      delegationOpts: { store },
      aoStore: createMemoryAcquisitionState(),
      companies: [],
    });
    assert.equal(scoutTurn, null);
    assert.equal((await store.listDelegations({ tenantId: '10' })).length, before);
    const retrieve = await maybeHandleRetrievalBeforeDelegationTurn({
      question: PILOT_INVENTORY,
      session,
      context: session.context,
      operatingEvidenceOpts: loaders.opts,
    });
    assert.ok(retrieve);
    assert.notEqual(retrieve.structured.metadata.scoutDelegated, true);
    assert.equal(retrieve.structured.metadata.specialistDelegated, false);
    assert.doesNotMatch(retrieve.prose, /I'd recommend a focused first campaign/i);
  });

  it('new-market investigation still delegates to Scout', () => {
    const question = 'Find 20 additional property managers matching what we learned from Campaign 001.';
    assert.equal(isOperatingEvidenceQuestion(question), false);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), true);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.INVESTIGATION);
  });

  it('recommendation question retrieves operating evidence before recommendation', async () => {
    const loaders = anchorOperatingOpts();
    assert.equal(isOperatingGroundedRecommendation(PILOT_RECOMMEND), true);
    assert.equal(isInventoryOnlyRequest(PILOT_RECOMMEND), false);
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: PILOT_RECOMMEND,
      session: { id: 's', context: { tenantId: '10', clientId: 10 } },
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: loaders.opts,
    });
    assert.ok(turn);
    assert.equal(classifyCognitiveMode(PILOT_RECOMMEND).kind, COGNITIVE_MODES.RECOMMENDATION);
    assert.match(turn.prose, /RECOMMENDATION/i);
    assert.match(turn.prose, /Campaign 001/i);
    assert.match(turn.prose, /20 AO lead/i);
    assert.doesNotMatch(turn.prose, /Ask for a recommendation only after reviewing this inventory/i);
    assert.doesNotMatch(turn.prose, /I would treat the next move as a small operator loop, not a city-wide push/i);
    assert.equal(turn.structured.metadata.evidenceGroundedRecommendation, true);
    assert.equal(turn.structured.metadata.executed, false);
  });

  it('specialist delegation remains subject to enabled_agents policy', () => {
    const question = 'Find commercial cleaning opportunities.';
    assert.equal(
      shouldHandleScoutAcquisition({
        question,
        context: { tenantId: '10', enabled_agents: ['scout'] },
      }),
      true
    );
    assert.equal(
      shouldHandleScoutAcquisition({
        question,
        context: { tenantId: '10', enabled_agents: [] },
      }),
      false
    );
  });
});

describe('SPEC-105 tenant isolation', () => {
  it('Anchor retrieval only returns client 10 operating evidence', async () => {
    const seen = [];
    const bundle = await loadOperatingEvidence({
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: {
        loadCampaignAo: async ({ clientId }) => {
          seen.push(clientId);
          return {
            available: true,
            progress: { campaign_name: 'Campaign 001', seeded_in_ao: 3 },
            leads: [
              { id: 1, client_id: 10, campaign_name: 'Campaign 001' },
              { id: 2, client_id: 1, campaign_name: 'Pulseforge' },
            ],
          };
        },
        loadProspects: async ({ clientId }) => ({ available: true, counts: { total: clientId === 10 ? 5 : 99 } }),
        loadScout: async () => ({ available: true, intelligence: { counts: { matched: 0 } } }),
        loadMissions: async () => ({
          available: true,
          rows: [
            { id: 'a', clientId: 10, tenantId: '10', title: 'Anchor mission', status: 'planned' },
            { id: 'b', clientId: 1, tenantId: '1', title: 'PF mission', status: 'planned' },
          ],
        }),
        loadObjectives: async () => ({ available: true, rows: [] }),
        loadActivity: async () => ({ available: true, touchpoints: [], activity: [] }),
        loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
      },
    });
    assert.deepEqual(seen, [10]);
    assert.equal(bundle.clientId, 10);
    assert.equal(bundle.campaign.leads.every((l) => Number(l.client_id) === 10), true);
    assert.equal(bundle.missions.length, 1);
    assert.equal(bundle.missions[0].title, 'Anchor mission');
  });

  it('Client 1 evidence cannot leak into Anchor', async () => {
    const bundle = await loadOperatingEvidence({
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: {
        loadCampaignAo: async () => ({
          available: true,
          progress: { campaign_name: 'Other', seeded_in_ao: 9 },
          leads: [{ id: 1, client_id: PULSEFORGE_ID }],
        }),
        loadProspects: async () => ({ available: true, counts: { total: 0 } }),
        loadScout: async () => ({ available: true, intelligence: { counts: { matched: 0 } } }),
        loadMissions: async () => ({ available: true, rows: [{ id: 'x', clientId: 1, tenantId: '1' }] }),
        loadObjectives: async () => ({ available: true, rows: [{ id: 'o', clientId: 1, tenantId: '1' }] }),
        loadActivity: async () => ({
          available: true,
          touchpoints: [{ id: 1, client_id: 1 }],
          activity: [{ id: 2, client_id: 1 }],
        }),
        loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
      },
    });
    assert.equal(bundle.campaign.leads.length, 0);
    assert.equal(bundle.missions.length, 0);
    assert.equal(bundle.objectives.length, 0);
    assert.equal(bundle.activity.touchpoints.length, 0);
  });

  it('Client 11 evidence cannot leak into Anchor', async () => {
    const bundle = await loadOperatingEvidence({
      context: { tenantId: '10', clientId: 10 },
      operatingEvidenceOpts: {
        loadCampaignAo: async () => ({
          available: true,
          progress: { campaign_name: 'AS', seeded_in_ao: 4 },
          leads: [{ id: 11, client_id: AS_CLEANING_ID }],
        }),
        loadProspects: async () => ({ available: true, counts: { total: 0 } }),
        loadScout: async () => ({ available: true, intelligence: { counts: { matched: 0 } } }),
        loadMissions: async () => ({ available: true, rows: [{ id: 'c', clientId: 11, tenantId: '11' }] }),
        loadObjectives: async () => ({ available: true, rows: [] }),
        loadActivity: async () => ({ available: true, touchpoints: [], activity: [] }),
        loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
      },
    });
    assert.equal(bundle.campaign.leads.length, 0);
    assert.equal(bundle.missions.length, 0);
  });

  it('missing tenant context fails closed', async () => {
    const bundle = await loadOperatingEvidence({ context: {} });
    assert.equal(bundle.failClosed, true);
    assert.equal(bundle.reason, 'missing_tenant');
    const composed = composeOperatingEvidenceAnswer('What campaigns have we run?', bundle);
    assert.match(composed.prose, /tenant context/i);
    assert.doesNotMatch(composed.prose, /Campaign 001 has/i);
  });
});

describe('SPEC-105 regression', () => {
  it('What do you understand about Anchor Cleaning still uses durable CIE retrieval', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const session = { id: 'anchor-sess', context: { tenantId: String(ANCHOR_ID) } };
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you currently understand about Anchor Cleaning?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.equal(turn.reason, 'retrieval_before_delegation');
    assert.match(turn.prose, /Anchor Cleaning/i);
    assert.match(turn.prose, /Based on my current understanding/i);
    assert.doesNotMatch(turn.prose, /What I can verify/i);
  });

  it('Who are our ideal customers still uses CIE / business understanding', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const session = { id: 'anchor-sess', context: { tenantId: String(ANCHOR_ID) } };
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'Who are our ideal customers?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /property managers|professional offices/i);
    assert.equal(isOperatingEvidenceQuestion('Who are our ideal customers?'), false);
  });

  it('WorkspaceEngine places operating retrieval inside the existing retrieval gate', () => {
    const engineSrc = fs.readFileSync(path.join(__dirname, '..', 'WorkspaceEngine.js'), 'utf8');
    const retrieveAt = engineSrc.indexOf(
      'const retrievalTurn = await maybeHandleRetrievalBeforeDelegationTurn'
    );
    const updateAt = engineSrc.indexOf('await maybeHandleOperatorOperatingUpdate');
    const scoutAt = engineSrc.indexOf('await maybeHandleScoutAcquisitionTurn');
    const cieAt = engineSrc.indexOf('await maybeHandleClientIntelligenceTurn');
    assert.ok(retrieveAt > 0);
    assert.ok(updateAt > retrieveAt);
    assert.ok(scoutAt > updateAt);
    assert.ok(cieAt > scoutAt);
    assert.match(engineSrc, /operatingEvidenceOpts/);
  });

  it('Pilot 0 inventory prompt does not return Blueprint-only CIE advice', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const loaders = anchorOperatingOpts();
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: cieOpts,
      operatingEvidenceOpts: loaders.opts,
    });
    const opened = engine.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: PILOT_INVENTORY,
    });
    assert.equal(result.domainDecision.reason, 'operating_evidence_retrieval');
    assert.match(result.prose, /Campaign 001/i);
    assert.match(result.prose, /18 existing prospect/i);
    assert.match(result.prose, /physically mailed/i);
    assert.match(result.prose, /Verified from/i);
    assert.doesNotMatch(result.prose, /KNOWN\n/i);
    assert.doesNotMatch(result.prose, /EVIDENCE NEEDED/i);
    assert.notEqual(result.structured.metadata.scoutDelegated, true);
    assert.doesNotMatch(result.prose, /I'd recommend a focused first campaign/i);
  });
});
