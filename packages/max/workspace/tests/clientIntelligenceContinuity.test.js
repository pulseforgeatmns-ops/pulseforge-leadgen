'use strict';

/**
 * SPEC-098 — Max Client Intelligence Continuity tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  createWorkspaceEngine,
} = require('../WorkspaceEngine');
const {
  normalizeContext,
} = require('../ContextEnvelope');
const {
  maybeHandleClientIntelligenceTurn,
  attachClientIntelligenceContext,
  loadApprovedClientIntelligence,
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

const ANSWERS = [
  'AS Cleaning Co. — residential cleaning for busy households.',
  'Weekly home cleans and move-out cleans.',
  'Homeowners who want reliable recurring service.',
  'One-off bargain hunters.',
  'Manchester NH and nearby towns.',
  'Show-up quality without reminders.',
  'Warm and clear.',
  'Fill recurring routes this quarter.',
  'More booked weekly cleans in 90 days.',
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

describe('SPEC-098 ClientIntelligenceContext', () => {
  it('ContextEnvelope preserves clientIntelligence passthrough', () => {
    const normalized = normalizeContext({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
      clientIntelligence: { approved: true, identity: 'AS Cleaning Co.' },
      businessBlueprint: { id: 'bp-1', status: 'approved' },
    });
    assert.equal(normalized.clientIntelligence.identity, 'AS Cleaning Co.');
    assert.equal(normalized.businessBlueprint.id, 'bp-1');
  });

  it('loads approved Blueprint for correct client only', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, ANSWERS);
    await approveClient(store, ANCHOR_ID, ANCHOR_ANSWERS);

    const asLoaded = await loadApprovedClientIntelligence({
      tenantId: String(AS_CLEANING_ID),
      cieOpts: { store },
    });
    assert.ok(asLoaded.summary);
    assert.equal(asLoaded.summary.approved, true);
    assert.match(asLoaded.summary.identity || '', /AS Cleaning/i);
    assert.doesNotMatch(asLoaded.summary.identity || '', /Anchor Cleaning/i);

    const anchorLoaded = await loadApprovedClientIntelligence({
      tenantId: String(ANCHOR_ID),
      cieOpts: { store },
    });
    assert.match(anchorLoaded.summary.identity || '', /Anchor/i);
    assert.doesNotMatch(anchorLoaded.summary.identity || '', /AS Cleaning/i);
  });

  it('pending Blueprint is not treated as approved', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startClientInterview(
      { clientId: AS_CLEANING_ID, forceNew: true },
      opts
    );
    let turn = started;
    for (const answer of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, answer, opts);
    }
    assert.ok(turn.blueprint);
    assert.notEqual(String(turn.blueprint.status).toLowerCase(), 'approved');

    const loaded = await loadApprovedClientIntelligence({
      tenantId: String(AS_CLEANING_ID),
      cieOpts: { store },
    });
    assert.equal(loaded.summary, null);
  });

  it('missing Blueprint fails closed with onboarding guidance', async () => {
    const store = createMemoryStore();
    assert.equal(looksLikeBusinessUnderstandingAsk('What do you understand about my business?'), true);
    const turn = await maybeHandleClientIntelligenceTurn({
      question: 'What do you understand about my business?',
      context: { tenantId: String(AS_CLEANING_ID) },
      cieOpts: { store },
    });
    assert.equal(turn.handled, false);
    assert.equal(turn.skipReason, 'governed_pipeline');

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
      question: 'What do you understand about my business?',
    });
    assert.match(missing.prose, /do not yet have an approved Business Blueprint/i);
    assert.match(missing.prose, /client-intel|onboarding|will not invent/i);
  });

  it('fresh Max session answers from approved AS Cleaning context', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, ANSWERS);

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

    const understanding = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What do you understand about my business?',
    });
    assert.match(understanding.prose, /AS Cleaning/i);
    assert.doesNotMatch(understanding.prose, /Anchor Cleaning/i);
    assert.doesNotMatch(understanding.prose, /Commercial Cleaning - Manchester/i);
    assert.doesNotMatch(understanding.prose, /Public Max Launch/i);
    assert.ok(understanding.context.clientIntelligence);
    assert.equal(understanding.context.clientIntelligence.approved, true);

    // Completely fresh session — no prior Workspace messages required.
    const fresh = engine.open({
      tenantId: String(AS_CLEANING_ID),
      page: 'command-deck',
    });
    const targeting = await engine.ask({
      sessionId: fresh.sessionId,
      question: 'Who should we target first?',
    });
    assert.match(targeting.prose, /Homeowners|homeowners|recurring/i);
    assert.doesNotMatch(targeting.prose, /Anchor Cleaning/i);

    const unknowns = await engine.ask({
      sessionId: fresh.sessionId,
      question: 'What are our biggest unknowns?',
    });
    assert.ok(unknowns.prose);

    const focus = await engine.ask({
      sessionId: fresh.sessionId,
      question: 'What should we focus on this week?',
    });
    assert.match(focus.prose, /Blueprint|routes|cleans|review/i);

    const next = await engine.ask({
      sessionId: fresh.sessionId,
      question: 'What should we do next?',
    });
    assert.doesNotMatch(next.prose, /Anchor Cleaning/i);
    assert.ok(next.context.clientIntelligence.approved);
  });

  it('Aji cannot receive Anchor Blueprint; Anchor cannot receive Aji', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, ANSWERS);
    await approveClient(store, ANCHOR_ID, ANCHOR_ANSWERS);

    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: { store },
    });

    const aji = engine.open({ tenantId: String(AS_CLEANING_ID), page: 'command-deck' });
    const ajiAsk = await engine.ask({
      sessionId: aji.sessionId,
      question: 'What do you understand about my business?',
    });
    assert.match(ajiAsk.prose, /AS Cleaning/i);
    assert.doesNotMatch(ajiAsk.prose, /Anchor Cleaning/i);
    assert.doesNotMatch(ajiAsk.prose, /property managers/i);

    const anchor = engine.open({ tenantId: String(ANCHOR_ID), page: 'command-deck' });
    const anchorAsk = await engine.ask({
      sessionId: anchor.sessionId,
      question: 'What do you understand about my business?',
    });
    assert.match(anchorAsk.prose, /Anchor Cleaning/i);
    assert.doesNotMatch(anchorAsk.prose, /AS Cleaning/i);
  });

  it('context reconstruction does not trigger autonomous action', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, ANSWERS);
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
    assert.equal(result.mission, null);
    assert.ok(
      !result.recommendedActions ||
        result.recommendedActions.every((a) => a.type === 'review' || a.id === 'acknowledge')
    );
  });

  it('attaches CIE on non-CIE questions without inventing Anchor facts', async () => {
    const store = createMemoryStore();
    await approveClient(store, AS_CLEANING_ID, ANSWERS);
    const session = {
      context: { tenantId: String(AS_CLEANING_ID), page: 'command-deck' },
    };
    const attached = await attachClientIntelligenceContext({
      session,
      cieOpts: { store },
    });
    assert.ok(attached.summary);
    assert.equal(session.context.clientIntelligence.approved, true);
    assert.doesNotMatch(
      JSON.stringify(session.context.clientIntelligence),
      /Anchor Cleaning/
    );
  });
});

describe('SPEC-098 wiring markers', () => {
  it('WorkspaceEngine and ContextEnvelope include SPEC-098 seams', () => {
    const engineSrc = fs.readFileSync(
      path.join(__dirname, '..', 'WorkspaceEngine.js'),
      'utf8'
    );
    const envelopeSrc = fs.readFileSync(
      path.join(__dirname, '..', 'ContextEnvelope.js'),
      'utf8'
    );
    assert.match(engineSrc, /maybeHandleClientIntelligenceTurn/);
    assert.match(envelopeSrc, /clientIntelligence/);
    assert.match(envelopeSrc, /businessBlueprint/);
  });
});
