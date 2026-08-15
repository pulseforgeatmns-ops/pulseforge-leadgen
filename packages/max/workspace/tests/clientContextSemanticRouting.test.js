'use strict';

/**
 * SPEC-103B — Client Conversation Semantic Routing Hardening
 *
 * Adversarial paraphrase matrix + regression. Classification must be semantic
 * (multi-feature scoring), not phrase allowlists.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  maybeHandleClientIntelligenceTurn,
  isClientContextReasoningRequest,
  isEvidenceDependentClientRequest,
  isClientContextExecutionRequest,
  isAmbiguousExecutionAdjacentRequest,
  looksLikeClientIntelligenceAsk,
  scoreClientBusinessSemantics,
  normalizeClientUtterance,
  looksLikeBusinessUnderstandingAsk,
} = require('../ClientIntelligenceContext');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');

const AS_CLEANING_ID = 11;
const ANCHOR_ID = 10;

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

async function approveClient(store, clientId, answers) {
  const opts = { store };
  const started = await startClientInterview({ clientId, forceNew: true }, opts);
  let turn = started;
  for (const answer of answers) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint);
  await approveBlueprint(turn.blueprint.id, opts);
  return { opts, started };
}

function priorSession(kind = 'reasoning', focus = 'property and facility managers') {
  return {
    context: {
      lastClientIntelligenceTurn: {
        kind,
        recommendationFocus: focus,
        reason: 'client_intelligence_reasoning',
      },
    },
  };
}

/** @type {Array<object>} */
const ADVERSARIAL_MATRIX = [
  {
    utterance: 'What are the principal uncertainties in our current strategy?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: "What don't we know yet?",
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'What don’t we know yet?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'What are we missing?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: "Anything we're overlooking?",
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'Biggest risk?',
    intent: 'risk',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'Why that?',
    intent: 'follow_up',
    route: 'client_context_follow_up',
    context: 'prior_cie_turn+blueprint',
    execution: 'no',
    epistemic: 'explain_prior_recommendation',
    session: priorSession(),
  },
  {
    utterance: 'Anything else?',
    intent: 'follow_up',
    route: 'client_context_follow_up',
    context: 'prior_cie_turn+blueprint',
    execution: 'no',
    epistemic: 'continue_prior_thread',
    session: priorSession(),
  },
  {
    utterance: 'Where would you go from here?',
    intent: 'next_actions',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: "How's this looking to you?",
    intent: 'follow_up_eval',
    route: 'client_context_follow_up',
    context: 'prior_cie_turn+blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
    session: priorSession(),
  },
  {
    utterance: 'How do you feel about it?',
    intent: 'ambiguous_referent',
    route: 'client_context_clarify',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'ask_clarification',
  },
  {
    utterance: 'If this was your company what would you be thinking about?',
    intent: 'owner_perspective',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'What arree we misssing heere?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: "What shouldn't we focus on?",
    intent: 'priority_negation',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'Why commercial instead of residential?',
    intent: 'comparative_follow_up',
    route: 'client_context_follow_up',
    context: 'prior_cie_turn+blueprint',
    execution: 'no',
    epistemic: 'explain_prior_recommendation',
    session: priorSession(),
  },
  {
    utterance: 'Who actually needs us right now?',
    intent: 'evidence_dependent',
    route: 'evidence_boundary',
    context: 'approved_blueprint_insufficient',
    execution: 'no',
    epistemic: 'fail_closed_no_invented_companies',
  },
  {
    utterance: 'Which companies need us right now?',
    intent: 'evidence_dependent',
    route: 'evidence_boundary',
    context: 'approved_blueprint_insufficient',
    execution: 'no',
    epistemic: 'fail_closed_no_invented_companies',
  },
  {
    utterance: 'Who around here needs cleaning?',
    intent: 'evidence_dependent',
    route: 'evidence_boundary',
    context: 'approved_blueprint_insufficient',
    execution: 'no',
    epistemic: 'fail_closed_no_invented_companies',
  },
  {
    utterance: "Let's go after them.",
    intent: 'execution_adjacent',
    route: 'execution_clarify',
    context: 'prior_or_blueprint',
    execution: 'no',
    epistemic: 'clarify_not_authorize',
  },
  {
    utterance: 'Alright, let’s go after them.',
    intent: 'execution_adjacent',
    route: 'execution_clarify',
    context: 'prior_or_blueprint',
    execution: 'no',
    epistemic: 'clarify_not_authorize',
  },
  {
    utterance: 'Launch the campaign.',
    intent: 'execution',
    route: 'execution_review_path',
    context: 'none_cie',
    execution: 'yes',
    epistemic: 'leave_cie_advisory',
  },
  {
    utterance: "What's 12 times 14?",
    intent: 'unrelated',
    route: 'not_client_context',
    context: 'none',
    execution: 'no',
    epistemic: 'not_forced_business',
  },
  {
    utterance: 'What time is it?',
    intent: 'unrelated',
    route: 'not_client_context',
    context: 'none',
    execution: 'no',
    epistemic: 'not_forced_business',
  },
  {
    utterance: 'Where are the gaps?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'What are you least sure about?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'What do you still need to learn about us?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'Do you see any holes?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'Where are we weakest?',
    intent: 'risk',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'What would you figure out next?',
    intent: 'next_investigation',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
  {
    utterance: 'Okay. What now?',
    intent: 'next_actions',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'Where would you start?',
    intent: 'priority',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'What would make you change your mind?',
    intent: 'assumption_challenge',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'evidence_needed',
  },
  {
    utterance: 'Anything here concern you?',
    intent: 'risk',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance: 'What would make this fail?',
    intent: 'risk',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'bounded_inference',
  },
  {
    utterance:
      'What would you want to learn before we put real money behind it?',
    intent: 'gap_unknowns',
    route: 'client_context_reasoning',
    context: 'approved_blueprint',
    execution: 'no',
    epistemic: 'known_vs_unknown_vs_evidence',
  },
];

describe('SPEC-103B semantic classifier (no phrase allowlist)', () => {
  it('normalizes curly apostrophes and typo collapse', () => {
    assert.equal(
      normalizeClientUtterance('What don’t we know yet?'),
      "what don't we know yet?"
    );
    assert.equal(
      normalizeClientUtterance('What arree we misssing heere?'),
      'what are we missing here?'
    );
  });

  it('does not implement primary routing as includes(missing|unknown|worry)', () => {
    const scored = scoreClientBusinessSemantics(
      'What are the principal uncertainties in our current strategy?'
    );
    assert.equal(scored.isClientBusiness, true);
    assert.equal(scored.mode, 'unknowns');
    assert.ok(scored.features.gap >= 1);
    assert.ok(scored.score >= 3.5);
  });

  it('approved Blueprint defaults interrogative wording into CIE without phrase hits', () => {
    const {
      shouldClaimClientIntelligenceTurn,
      isClearlyNonBusinessUtterance,
    } = require('../ClientIntelligenceContext');
    // Unseen paraphrase — no product vocabulary required.
    assert.equal(
      shouldClaimClientIntelligenceTurn(
        'What would make the beachhead wobble first?',
        null,
        { approvedBlueprint: true }
      ),
      true
    );
    assert.equal(
      isClearlyNonBusinessUtterance('What time is it?'),
      true
    );
    assert.equal(
      shouldClaimClientIntelligenceTurn('What time is it?', null, {
        approvedBlueprint: true,
      }),
      false
    );
    assert.equal(
      shouldClaimClientIntelligenceTurn('hello', null, {
        approvedBlueprint: true,
      }),
      false
    );
  });

  it('adversarial matrix classifies by meaning (≥30 utterances)', () => {
    assert.ok(ADVERSARIAL_MATRIX.length >= 30);

    for (const row of ADVERSARIAL_MATRIX) {
      const scored = scoreClientBusinessSemantics(
        row.utterance,
        row.session || null
      );
      const cie = looksLikeClientIntelligenceAsk(
        row.utterance,
        row.session || null
      );

      if (row.route === 'not_client_context') {
        assert.equal(
          scored.features.unrelated || !cie,
          true,
          `expected unrelated/non-CIE for ${row.utterance}`
        );
        assert.equal(isClientContextReasoningRequest(row.utterance), false);
        continue;
      }

      if (row.route === 'execution_review_path') {
        assert.equal(isClientContextExecutionRequest(row.utterance), true);
        assert.equal(cie, false);
        continue;
      }

      if (row.route === 'execution_clarify') {
        assert.equal(
          isAmbiguousExecutionAdjacentRequest(row.utterance),
          true
        );
        assert.equal(isClientContextExecutionRequest(row.utterance), false);
        continue;
      }

      if (row.route === 'evidence_boundary') {
        assert.equal(
          isEvidenceDependentClientRequest(row.utterance),
          true,
          row.utterance
        );
        assert.equal(isClientContextReasoningRequest(row.utterance), false);
        continue;
      }

      if (row.route === 'client_context_clarify') {
        assert.equal(scored.features.referentAmbiguous, true, row.utterance);
        assert.equal(cie, true);
        continue;
      }

      if (
        row.route === 'client_context_reasoning' ||
        row.route === 'client_context_follow_up'
      ) {
        assert.equal(cie, true, `CIE ask expected for ${row.utterance}`);
        if (row.route === 'client_context_follow_up') {
          assert.equal(scored.features.followUp, true, row.utterance);
        } else {
          assert.equal(
            isClientContextReasoningRequest(
              row.utterance,
              row.session || null
            ) ||
              scored.mode === 'unknowns' ||
              scored.mode === 'risk' ||
              scored.mode === 'approach' ||
              scored.mode === 'focus',
            true,
            `reasoning expected for ${row.utterance} mode=${scored.mode}`
          );
        }
        assert.equal(row.execution, 'no');
      }
    }
  });
});

describe('SPEC-103B end-to-end routing', () => {
  it('unseen paraphrase still routes with approved Blueprint (no phrase hit required)', async () => {
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
      question: 'What would make the beachhead wobble first?',
    });
    assert.match(
      String(result.domainDecision && result.domainDecision.reason),
      /client_intelligence/
    );
    assert.doesNotMatch(
      result.prose,
      /Switching from Workspace to General Conversation|detailed_answer|today's briefing/i
    );
    assert.match(result.prose, /property|facility|commercial|Toronto|GTA|Blueprint/i);
  });

  it('gap question routes to client-context unknowns, not briefing', async () => {
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
      question: "What don't we know yet?",
    });
    assert.match(
      String(result.domainDecision && result.domainDecision.reason),
      /client_intelligence/
    );
    assert.doesNotMatch(
      result.prose,
      /Switching from Workspace to General Conversation/i
    );
    assert.doesNotMatch(result.prose, /You're reviewing today's briefing/i);
    assert.doesNotMatch(
      result.prose,
      /Unavailable in current context: detailed_answer/i
    );
    assert.match(
      result.prose,
      /KNOWN|enough clarity|would not claim to know/i
    );
    assert.match(result.prose, /UNKNOWN|INFERENCE|EVIDENCE NEEDED/i);
    assert.match(
      result.prose,
      /property|facility|GTA|Toronto|commercial|walkthrough|acquisition/i
    );
  });

  it('curly-apostrophe gap paraphrase still routes to CIE', async () => {
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
      question: 'What don’t we know yet?',
    });
    assert.match(
      String(result.domainDecision && result.domainDecision.reason),
      /client_intelligence/
    );
    assert.match(result.prose, /UNKNOWN|EVIDENCE NEEDED|would not claim/i);
  });

  it('evidence-dependent colloquial wording stays fail-closed', async () => {
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
      question: 'Who around here needs cleaning?',
    });
    assert.match(result.prose, /do not yet have|not invent|evidence/i);
    assert.doesNotMatch(result.prose, /\b(Acme|Corp|Inc\.|Ltd)\b/);
  });

  it('execution-adjacent language clarifies and does not authorize', async () => {
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
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: "Alright, let's go after them.",
    });
    assert.match(
      String(result.domainDecision && result.domainDecision.reason),
      /execution_clarify/
    );
    assert.match(
      result.prose,
      /will not launch|reviewable|strategy agreement/i
    );
    assert.equal(result.mission, null);
  });

  it('explicit launch still leaves CIE advisory path', async () => {
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
    const launch = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Launch the campaign.',
    });
    assert.doesNotMatch(
      String(launch.domainDecision && launch.domainDecision.reason),
      /^client_intelligence_/
    );
  });

  it('unrelated math is not forced into business reasoning', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const turn = await maybeHandleClientIntelligenceTurn({
      question: "What's 12 times 14?",
      context: { tenantId: String(AS_CLEANING_ID) },
      cieOpts: { store },
    });
    assert.equal(turn.handled, false);
  });

  it('regression: focus / why / recall / evidence / tenant isolation', async () => {
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

    const focus = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(
      String(focus.domainDecision && focus.domainDecision.reason),
      /client_intelligence/
    );

    const why = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Why?',
    });
    assert.match(
      String(why.domainDecision && why.domainDecision.reason),
      /client_intelligence/
    );

    const recall = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What do you understand about my business?',
    });
    assert.match(recall.prose, /AS Cleaning/i);
    assert.equal(
      looksLikeBusinessUnderstandingAsk(
        'What do you understand about my business?'
      ),
      true
    );

    const evidence = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Which companies need us right now?',
    });
    assert.match(evidence.prose, /do not yet have|not invent|evidence/i);
    assert.doesNotMatch(evidence.prose, /Anchor Cleaning|Manchester/i);
  });

  it('no approved Blueprint does not fabricate; pending is non-authoritative', async () => {
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
    const missing = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should we do next?',
    });
    assert.match(
      missing.prose,
      /do not yet have an approved Business Blueprint|will not invent/i
    );

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

    const pendingEngine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });
    const pendingOpen = pendingEngine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const pending = await pendingEngine.ask({
      sessionId: pendingOpen.sessionId,
      question: "What don't we know yet?",
    });
    assert.match(
      pending.prose,
      /do not yet have an approved Business Blueprint|will not invent/i
    );
  });

  it('tenant isolation: AS Cleaning never surfaces Anchor', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    await approveClient(store, ANCHOR_ID, [
      'Anchor Cleaning — commercial cleaning for professional offices.',
      'Recurring commercial cleaning and weekly office cleans.',
      'Property managers, facility managers, and professional offices.',
      'Lowest-price bargain hunters.',
      'Greater Manchester including Bedford and Hooksett.',
      'Reliable crews that do the work right without chasing.',
      'Calm professional reliable voice.',
      'Grow commercial cleaning in Greater Manchester.',
      'Clearer path to commercial opportunities in 90 days.',
    ]);
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
      question: 'Where are the holes?',
    });
    assert.doesNotMatch(
      result.prose,
      /Anchor Cleaning|Manchester|Public Max Launch/i
    );
    assert.match(
      result.prose,
      /Toronto|GTA|property|facility|commercial/i
    );
  });
});
