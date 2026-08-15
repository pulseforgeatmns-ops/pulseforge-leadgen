'use strict';

/**
 * SPEC-103C — Active conversational reasoning continuity
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const {
  classifyActiveThoughtFollowUp,
  getActiveClientReasoning,
} = require('../ActiveClientReasoning');
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
  await approveBlueprint(turn.blueprint.id, opts);
  return { opts };
}

async function openEngine(store) {
  return createWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    clientIntelligenceOpts: { store },
  });
}

async function runConversation(sessionId, engine, questions) {
  const results = [];
  for (const question of questions) {
    results.push(
      await engine.ask({ sessionId, question: String(question).trim() })
    );
  }
  return results;
}

describe('SPEC-103C active reasoning continuity', () => {
  it('TEST A — recommendation then Why explains without repeating essay', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    const [rec, why] = await runConversation(opened.sessionId, engine, [
      'What should we focus on first?',
      'Why?',
    ]);
    assert.match(rec.prose, /repeatable commercial acquisition|property|facility/i);
    assert.match(why.prose, /because|approved/i);
    assert.notEqual(why.prose, rec.prose);
    assert.match(
      String(why.domainDecision && why.domainDecision.reason),
      /follow_up/
    );
  });

  it('TEST B — decompose creates ordered plan without execution', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    const steps = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    assert.match(steps.prose, /1\. Define qualification criteria/i);
    assert.match(
      String(steps.domainDecision && steps.domainDecision.reason),
      /decompose/
    );
    assert.equal(steps.mission, null);
    const session = engine._sessions.get(opened.sessionId);
    const active = getActiveClientReasoning(session);
    assert.ok(active.planSteps && active.planSteps.length >= 6);
  });

  it('TEST C/D/E — first step, advance, deepen stay inside plan', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    const [rec, , decomp, first, next, deepen] = await runConversation(
      opened.sessionId,
      engine,
      [
        'What should we focus on first?',
        'Why?',
        'Be specific. Give me the exact steps.',
        'Okay, what is the very first thing we should do?',
        'And then what?',
        'How exactly?',
      ]
    );
    assert.match(first.prose, /first thing|Step 1 of/i);
    assert.doesNotMatch(first.prose, /I'd start by proving a repeatable commercial acquisition motion/i);
    assert.notEqual(first.prose, rec.prose);
    assert.match(next.prose, /Step 2 of|After step 1/i);
    assert.doesNotMatch(next.prose, /I'd start by proving/i);
    assert.match(deepen.prose, /current step \(2\)|step \(2\)/i);
    assert.notEqual(deepen.prose, decomp.prose);
  });

  it('TEST F — plan critique does not repeat recommendation', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    const results = await runConversation(opened.sessionId, engine, [
      'What should we focus on first?',
      'Be specific. Give me the exact steps.',
      'What could go wrong with this approach?',
    ]);
    const critique = results[2];
    assert.match(
      String(critique.domainDecision && critique.domainDecision.reason),
      /plan_critique/
    );
    assert.match(critique.prose, /Critiquing the active plan|reasoning hypotheses/i);
    assert.doesNotMatch(critique.prose, /I'd start by proving a repeatable commercial acquisition motion/i);
  });

  it('TEST G — falsification stays on active recommendation', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    const falsify = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What would make you change your mind?',
    });
    assert.match(falsify.prose, /change my mind|weaken|evidence/i);
    assert.doesNotMatch(falsify.prose, /Critiquing the active plan/i);
  });

  it('TEST H/I — capability and operator mapping on active plan', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const cap = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What can Pulseforge actually do for me in those steps right now?',
    });
    const op = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What do I need to do myself?',
    });
    assert.match(String(cap.domainDecision && cap.domainDecision.reason), /plan_capability/);
    assert.match(cap.prose, /Pulseforge can help now|Not currently available/i);
    assert.doesNotMatch(cap.prose, /1\. Define qualification criteria\n\n2\./);
    assert.match(String(op.domainDecision && op.domainDecision.reason), /plan_operator/);
    assert.match(op.prose, /operator-owned/i);
    assert.equal(cap.mission, null);
  });

  it('TEST J — capability question vs execution intent', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const cap = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Can Pulseforge build the target list?',
    });
    assert.match(cap.prose, /do not yet have|not invent|evidence|Market Intelligence|Scout/i);
    assert.doesNotMatch(
      String(cap.domainDecision && cap.domainDecision.reason),
      /execution_review/
    );
    const build = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Build it.',
    });
    assert.doesNotMatch(
      String(build.domainDecision && build.domainDecision.reason),
      /^client_intelligence_plan/
    );
  });

  it('TEST K — why>? retains referent after recommendation', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    const why = await engine.ask({ sessionId: opened.sessionId, question: 'why>?' });
    assert.match(String(why.domainDecision && why.domainDecision.reason), /follow_up/);
    assert.match(why.prose, /because|approved/i);
  });

  it('TEST L — commercial vs residential comparison', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    const cmp = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Why that instead of residential?',
    });
    assert.match(cmp.prose, /commercial|residential|approved/i);
    assert.doesNotMatch(cmp.prose, /Acme|invented performance/i);
  });

  it('TEST M/N — subject change then recover plan step', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const subject = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Separate question: should we raise our residential prices?',
    });
    assert.doesNotMatch(subject.prose, /Step 2 of 8/i);
    const recover = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Go back to the commercial plan. What was step two?',
    });
    assert.match(recover.prose, /Step 2 of|Build a short account set/i);
  });

  it('TEST P — evidence fail-closed with plan context', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const companies = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Which actual companies should I call Monday?',
    });
    assert.match(companies.prose, /do not yet have|not invent|evidence/i);
    assert.match(companies.prose, /step 1|qualification criteria|active plan/i);
    assert.doesNotMatch(companies.prose, /Acme|Corp\.|Inc\./);
  });

  it('TEST Q — plan acceptance enters preparation without execution clarify', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const agree = await engine.ask({
      sessionId: opened.sessionId,
      question: "Okay, I'm happy with that plan. Let's do it.",
    });
    assert.match(
      String(agree.domainDecision && agree.domainDecision.reason),
      /plan_preparation/
    );
    assert.match(agree.prose, /Good\.|first step|qualification criteria/i);
    assert.match(agree.prose, /Nothing will be sent or launched without your approval/i);
    assert.doesNotMatch(agree.prose, /strategy agreement only|reviewable execution path/i);
    assert.doesNotMatch(agree.prose, /I hear you leaning toward/i);
    assert.equal(agree.mission, null);
  });

  it('TEST R — tenant isolation for AS Cleaning', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    await approveClient(store, ANCHOR_ID, [
      'Anchor Cleaning — commercial cleaning for professional offices.',
      'Recurring commercial cleaning.',
      'Property managers and professional offices.',
      'Bargain hunters.',
      'Greater Manchester.',
      'Reliable crews.',
      'Professional voice.',
      'Grow commercial cleaning.',
      'Commercial opportunities in 90 days.',
    ]);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'And then what?',
    });
    assert.doesNotMatch(result.prose, /Anchor Cleaning|Manchester|Public Max Launch/i);
    assert.match(result.prose, /account set|Step 2 of|property managers/i);
  });

  it('TEST S — fresh session lacks plan memory; Blueprint recall still works', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const a = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: a.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: a.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const b = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    const missing = await engine.ask({
      sessionId: b.sessionId,
      question: "What's step two?",
    });
    assert.doesNotMatch(
      String(missing.domainDecision && missing.domainDecision.reason),
      /plan_continuity/
    );
    const focus = await engine.ask({
      sessionId: b.sessionId,
      question: 'What should we focus on first?',
    });
    assert.match(focus.prose, /property|facility|commercial|Toronto|GTA/i);
  });

  it('adversarial phrasing retains semantic continuity', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Okay, what is the very first thing we should do?',
    });
    const adversarial = [
      ['and after that?', /Step 2 of|After step/i],
      ["what's the weak spot here?", /Critiquing the active plan|reasoning hypotheses/i],
      ['which part can you handle?', /Pulseforge can help now/i],
      ['what part is on me?', /operator-owned/i],
    ];
    for (const [q, pattern] of adversarial) {
      const r = await engine.ask({ sessionId: opened.sessionId, question: q });
      assert.match(r.prose, pattern, q);
      assert.doesNotMatch(r.prose, /I'd start by proving a repeatable commercial acquisition motion/i, q);
    }
  });

  it('classifier resolves advance/select against active plan semantically', () => {
    const session = {
      context: {
        activeClientReasoning: {
          planSteps: ['a', 'b', 'c'],
          conversationalFocusIndex: 0,
          recommendationFocus: 'commercial plan',
        },
      },
    };
    assert.equal(classifyActiveThoughtFollowUp('And then?', session).op, 'advance');
    assert.equal(classifyActiveThoughtFollowUp("What's first?", session).op, 'select');
    assert.equal(
      classifyActiveThoughtFollowUp('What could go wrong?', session).op,
      'critique'
    );
  });
});

describe('SPEC-103D advisory-to-preparation handoff', () => {
  it('TEST A — plan acceptance begins Step 1 preparation', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const accept = await engine.ask({
      sessionId: opened.sessionId,
      question: "I'm happy with that plan. Let's do it.",
    });
    assert.match(String(accept.domainDecision && accept.domainDecision.reason), /plan_preparation/);
    assert.match(accept.prose, /qualification criteria|first step/i);
    assert.doesNotMatch(accept.prose, /strategy agreement only/i);
    assert.equal(accept.mission, null);
  });

  it('TEST B — Sounds good lets start variant', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const accept = await engine.ask({
      sessionId: opened.sessionId,
      question: "Sounds good. Let's start.",
    });
    assert.match(String(accept.domainDecision && accept.domainDecision.reason), /plan_preparation/);
    assert.match(accept.prose, /Good\.|qualification criteria/i);
  });

  it('TEST C — Go ahead after preparation proposal drafts criteria', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    await engine.ask({
      sessionId: opened.sessionId,
      question: "Okay, I'm happy with that plan. Let's do it.",
    });
    const go = await engine.ask({ sessionId: opened.sessionId, question: 'Go ahead.' });
    assert.match(String(go.domainDecision && go.domainDecision.reason), /plan_preparation/);
    assert.match(go.prose, /qualification criteria|Geography:/i);
    assert.doesNotMatch(go.prose, /strategy agreement only/i);
    assert.equal(go.mission, null);
  });

  it('TEST D — explicit send leaves execution policy path', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const send = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Send those emails.',
    });
    assert.doesNotMatch(
      String(send.domainDecision && send.domainDecision.reason),
      /plan_preparation/
    );
  });

  it('TEST E — contextual go ahead classification', () => {
    const { classifyAdvisoryHandoffIntent } = require('../ActiveClientReasoning');
    const prepSession = {
      context: {
        activeClientReasoning: {
          planSteps: ['a', 'b'],
          planAccepted: true,
          preparationProposed: true,
        },
      },
    };
    const execSession = {
      context: {
        activeClientReasoning: {
          planSteps: ['a', 'b'],
          executionReviewPending: true,
        },
      },
    };
    assert.equal(
      classifyAdvisoryHandoffIntent('Go ahead.', prepSession).kind,
      'preparation_authorize'
    );
    assert.equal(
      classifyAdvisoryHandoffIntent('Go ahead.', execSession).kind,
      'external_execution'
    );
  });

  it('TEST F — progression after acceptance without false completion', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    await engine.ask({
      sessionId: opened.sessionId,
      question: "I'm happy with that plan. Let's do it.",
    });
    const next = await engine.ask({
      sessionId: opened.sessionId,
      question: "What's next after that?",
    });
    assert.match(next.prose, /Step 2|After step|account set/i);
    assert.doesNotMatch(next.prose, /I'd start by proving a repeatable commercial acquisition motion/i);
  });

  it('TEST G — Scout limitation stated naturally at account-set step', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    await engine.ask({
      sessionId: opened.sessionId,
      question: "I'm happy with that plan. Let's do it.",
    });
    const go = await engine.ask({ sessionId: opened.sessionId, question: 'Go ahead.' });
    assert.match(go.prose, /can't initiate Scout|Scout discovery|not callable/i);
  });

  it('TEST H — no Mission from plan acceptance alone', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const accept = await engine.ask({
      sessionId: opened.sessionId,
      question: "Okay, let's do it.",
    });
    assert.equal(accept.mission, null);
  });

  it('TEST I — tenant isolation on preparation handoff', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Be specific. Give me the exact steps.',
    });
    const accept = await engine.ask({
      sessionId: opened.sessionId,
      question: "I'm happy with that plan. Let's do it.",
    });
    assert.doesNotMatch(accept.prose, /Anchor Cleaning|Manchester/i);
    assert.match(accept.prose, /Toronto|GTA|property|facility/i);
  });

  it('TEST J/L — execution-adjacent without plan still clarifies', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, AS_CLEANING_COMMERCIAL);
    const engine = await openEngine(store);
    const opened = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    await engine.ask({ sessionId: opened.sessionId, question: 'What should we focus on first?' });
    const go = await engine.ask({
      sessionId: opened.sessionId,
      question: "Alright, let's go after them.",
    });
    assert.match(
      String(go.domainDecision && go.domainDecision.reason),
      /execution_clarify/
    );
  });
});
