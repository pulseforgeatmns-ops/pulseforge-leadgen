'use strict';

/**
 * SPEC-103 — Max Client-Context Business Reasoning
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleClientIntelligenceTurn,
  isClientContextReasoningRequest,
  isEvidenceDependentClientRequest,
  isClientContextExecutionRequest,
  composeClientContextReasoning,
  looksLikeBusinessUnderstandingAsk,
  loadApprovedClientIntelligence,
} = require('../ClientIntelligenceContext');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');

const AS_CLEANING_ID = 11;
const ANCHOR_ID = 10;

/** Commercial GTA-style AS Cleaning answers (matches production Blueprint shape). */
const AS_CLEANING_COMMERCIAL = [
  'AS Cleaning Co. — commercial cleaning preferred over residential.',
  'Commercial cleaning, apartment and multifamily cleaning, facility cleaning.',
  'Property managers and facility managers.',
  'One-off bargain hunters and lowest-price residential tire-kickers.',
  'Greater Toronto Area.',
  'Excellent quality with reliable on-time service.',
  'Professional and clear.',
  'Build a reliable prospect-to-client pipeline with recurring revenue.',
  'Walkthroughs booked and recurring revenue clients.',
];

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

async function approveClient(store, clientId, answers) {
  const opts = { store };
  const started = await startClientInterview({ clientId, forceNew: true }, opts);
  let turn = started;
  for (const answer of answers) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint);
  const approved = await approveBlueprint(turn.blueprint.id, opts);
  return { opts, started, turn, approved };
}

function mockApprovedBlueprintService(blueprint) {
  return {
    async getApprovedClientBlueprint() {
      return blueprint;
    },
  };
}

describe('SPEC-103 detectors', () => {
  it('classifies reasoning vs recall vs evidence vs execution conceptually', () => {
    assert.equal(
      looksLikeBusinessUnderstandingAsk(
        'Max, what do you understand about my business?'
      ),
      true
    );
    assert.equal(
      isClientContextReasoningRequest(
        'Based on what you know about my business, what should we focus on first?'
      ),
      true
    );
    assert.equal(
      isClientContextReasoningRequest(
        'What do you think our biggest opportunity is?'
      ),
      true
    );
    assert.equal(
      isEvidenceDependentClientRequest(
        'Which property managers in the GTA are showing buying signals right now?'
      ),
      true
    );
    assert.equal(
      isClientContextReasoningRequest(
        'Which property managers in the GTA are showing buying signals right now?'
      ),
      false
    );
    assert.equal(
      isClientContextExecutionRequest('Launch that campaign.'),
      true
    );
    assert.equal(
      isClientContextReasoningRequest('What campaign would you recommend?'),
      true
    );
    assert.equal(isClientContextExecutionRequest('Okay.'), false);
  });
});

describe('SPEC-103 client-context reasoning', () => {
  it('TEST A — direct reasoning from approved Blueprint', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);

    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
      clientId: AS_CLEANING_ID,
    });

    const result = await engine.ask({
      sessionId: opened.sessionId,
      question:
        'Based on what you know about my business, what should we focus on first?',
    });

    assert.equal(result.executionDomain, 'workspace');
    assert.match(
      String(result.domainDecision && result.domainDecision.reason),
      /client_intelligence/
    );
    assert.doesNotMatch(result.prose, /Switching from Workspace to General Conversation/i);
    assert.doesNotMatch(result.prose, /Unavailable in current context: detailed_answer/i);
    assert.doesNotMatch(result.prose, /I can investigate .* briefing/i);
    assert.match(result.prose, /property|facility|commercial|GTA|Toronto|recurring/i);
    assert.match(result.prose, /moderate|don'?t know yet|evidence/i);
    assert.equal(result.mission, null);
    assert.ok(result.context.clientIntelligence.approved);
    assert.ok(
      !result.recommendedActions ||
        result.recommendedActions.every(
          (a) => a.type === 'review' || a.id === 'acknowledge'
        )
    );
  });

  it('TEST B — fresh session without prior conversation', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const fresh = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: fresh.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(result.prose, /AS Cleaning|property|facility|commercial|Toronto|GTA/i);
    assert.ok(result.context.clientIntelligence.approved);
    assert.doesNotMatch(result.prose, /detailed_answer/i);
  });

  it('TEST C — biggest opportunity synthesizes rather than recites', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What do you think our biggest opportunity is?',
    });
    assert.match(result.prose, /opportunity|acquisition|motion|property|facility/i);
    assert.match(result.prose, /reasoning|evidence|approved|confidence|moderate/i);
    // Should not be a bare bullet dump of every Blueprint field only
    assert.ok(result.prose.length > 120);
  });

  it('TEST D — unresolved ICP is not invented', async () => {
    const blueprint = {
      id: 'bp-unresolved-icp',
      status: 'approved',
      clientId: AS_CLEANING_ID,
      sections: {
        identity: { summary: 'AS Cleaning Co. commercial cleaning.' },
        services: { summary: 'Commercial cleaning preferred.' },
        idealCustomers: { summary: '', unknowns: ['commercial customer segment'] },
        targetMarkets: { summary: 'Greater Toronto Area' },
        campaignGoals: { summary: 'Recurring revenue pipeline' },
        successMetrics: { summary: 'Walkthroughs and recurring clients' },
      },
    };
    const turn = await maybeHandleClientIntelligenceTurn({
      question: 'Who should we target first?',
      context: { tenantId: String(AS_CLEANING_ID) },
      cieService: mockApprovedBlueprintService(blueprint),
    });
    assert.equal(turn.handled, true);
    assert.match(turn.prose, /haven'?t chosen|unresolved|segment|not invent/i);
    assert.doesNotMatch(turn.prose, /prioritize: property managers/i);
  });

  it('TEST E — missing Market Intelligence still allows bounded reasoning', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
      marketIntelligenceService: null,
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(result.prose, /property|facility|commercial|start/i);
    assert.match(result.prose, /evidence|don'?t know yet|moderate/i);
    assert.doesNotMatch(result.prose, /Unavailable in current context: detailed_answer/i);
  });

  it('TEST F — evidence-dependent question fails closed without fabricating', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question:
        'Which property managers in the GTA are showing buying signals right now?',
    });
    assert.match(result.prose, /do not yet have|not invent|evidence/i);
    assert.doesNotMatch(result.prose, /\b(Acme|Corp|Inc\.|Ltd)\b/);
    assert.ok(
      (result.structured.metadata.unavailable || []).includes(
        'live_buying_signals'
      ) ||
        /buying signals|market/i.test(result.prose)
    );
  });

  it('TEST G — advisory vs execution distinction', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: true,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });

    const advisory = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What campaign would you recommend?',
    });
    assert.match(
      String(advisory.domainDecision && advisory.domainDecision.reason),
      /client_intelligence/
    );
    assert.equal(advisory.mission, null);
    assert.match(advisory.prose, /recommend|campaign|advisory|not authorization|learning loop/i);

    const launch = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Launch that campaign.',
    });
    // Must leave CIE advisory handler — execution/review path owns this.
    assert.notEqual(
      launch.domainDecision && launch.domainDecision.reason,
      'client_intelligence_reasoning'
    );
    assert.doesNotMatch(
      String(launch.domainDecision && launch.domainDecision.reason),
      /^client_intelligence_/
    );
  });

  it('TEST H — tenant isolation AS Cleaning vs Anchor', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    await approveClient(store, ANCHOR_ID, ANCHOR_ANSWERS);

    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const aji = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: aji.sessionId,
      question: 'Who should we target?',
    });
    assert.doesNotMatch(result.prose, /Anchor Cleaning/i);
    assert.doesNotMatch(result.prose, /Manchester/i);
    assert.doesNotMatch(result.prose, /Commercial Cleaning - Manchester/i);
    assert.doesNotMatch(result.prose, /Public Max Launch/i);
    assert.match(result.prose, /property|facility|Toronto|GTA|commercial/i);
  });

  it('TEST I — normal follow-up Why? stays coherent', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const first = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    assert.ok(first.context.lastClientIntelligenceTurn);

    const follow = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Why?',
    });
    assert.match(
      String(follow.domainDecision && follow.domainDecision.reason),
      /client_intelligence/
    );
    assert.match(follow.prose, /because|approved|ICP|commercial|property|facility/i);
    assert.doesNotMatch(follow.prose, /detailed_answer/i);
  });

  it('TEST J — recall regression still works', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What do you understand about my business?',
    });
    assert.match(result.prose, /AS Cleaning/i);
    assert.match(result.prose, /approved Business Blueprint/i);
    assert.doesNotMatch(result.prose, /No autonomous execution/i);
  });

  it('TEST K — no Blueprint does not fabricate understanding', async () => {
    const store = createMemoryStore();
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(result.prose, /do not yet have an approved Business Blueprint|will not invent/i);
    assert.doesNotMatch(result.prose, /property managers and facility managers/i);
  });

  it('TEST L — pending Blueprint is not authoritative', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startClientInterview(
      { clientId: AS_CLEANING_ID, forceNew: true },
      opts
    );
    let turn = started;
    for (const answer of AS_CLEANING_COMMERCIAL) {
      turn = await postInterviewMessage(started.interviewId, answer, opts);
    }
    assert.ok(turn.blueprint);
    assert.notEqual(String(turn.blueprint.status).toLowerCase(), 'approved');

    const loaded = await loadApprovedClientIntelligence({
      tenantId: String(AS_CLEANING_ID),
      cieOpts: { store },
    });
    assert.equal(loaded.summary, null);

    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const opened = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(result.prose, /do not yet have an approved Business Blueprint|will not invent/i);
    assert.doesNotMatch(result.prose, /I'd start by proving/i);
  });

  it('composeClientContextReasoning distinguishes inference from evidence', () => {
    const summary = {
      approved: true,
      identity: 'AS Cleaning Co.',
      services: 'Commercial cleaning',
      idealCustomers: 'Property and facility managers',
      targetMarkets: 'Greater Toronto Area',
      campaignGoals: 'Recurring revenue pipeline',
      successMetrics: 'Walkthroughs and recurring clients',
      unknowns: [],
    };
    const composed = composeClientContextReasoning(
      summary,
      'What should we focus on first?'
    );
    assert.match(composed.prose, /property and facility managers/i);
    assert.match(composed.prose, /moderate|evidence/i);
    assert.equal(composed.confidenceLabel, 'moderate');
  });
});
