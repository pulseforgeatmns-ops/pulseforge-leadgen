'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  maybeHandleRetrievalBeforeDelegationTurn: rawCieHandler,
} = require('../RetrievalBeforeDelegationContext');
const {
  maybeHandleScoutAcquisitionTurn,
  shouldHandleScoutAcquisition,
} = require('../ScoutAcquisitionContext');
const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../../../../services/specialistDelegation');
const { createMemoryAcquisitionState } = require('../../../../services/scoutAcquisitionIntelligence');
const cie = require('../../../../services/clientIntelligenceInterview');

async function approvedAnchorFixture() {
  const opts = { store: cie.createMemoryStore() };
  const started = await cie.startClientInterview({ clientId: 10, forceNew: true }, opts);
  const answers = [
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
  let turn = started;
  for (const answer of answers) {
    turn = await cie.postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint);
  await cie.approveBlueprint(turn.blueprint.id, opts);
  return opts;
}

describe('SPEC-102 retrieval before delegation — workspace hook', () => {
  let session;

  beforeEach(() => {
    session = {
      id: 'sess-102',
      context: {
        tenantId: '10',
        page: 'command-deck',
        domainId: 'acquisition',
        acquisitionLoop: true,
        lastScoutEvaluation: { id: 'ev-1', materialChange: false },
        clientIntelligence: {
          approved: true,
          businessName: 'Anchor Cleaning',
          identity: 'Anchor Cleaning — commercial cleaning for professional offices.',
          geography: 'Greater Manchester including Bedford and Hooksett',
          targetMarkets: 'Greater Manchester including Bedford and Hooksett',
          idealCustomers: 'property managers and professional offices',
          unknowns: ['Which commercial segment will respond first'],
        },
      },
    };
  });

  it('answers service-area recall from durable knowledge without Scout', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you currently understand about our service area?',
      session,
      context: session.context,
      cieOpts: await approvedAnchorFixture(),
    });
    assert.ok(turn);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /Greater Manchester|Bedford|Hooksett/i);
    assert.doesNotMatch(turn.prose, /\bScout\.\.\./i);
    assert.equal(turn.structured.metadata.scoutDelegated, undefined);
    assert.equal(turn.structured.metadata.specialistDelegated, false);
  });

  it('answers Anchor recall from the Blueprint', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you know about Anchor?',
      session,
      context: session.context,
      cieOpts: await approvedAnchorFixture(),
    });
    assert.ok(turn);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /Anchor/i);
  });

  it('explains a non-elevation from the last evaluation', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: "Why didn't you elevate Acquisition?",
      session,
      context: session.context,
    });
    assert.ok(turn);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /didn'?t elevate|not material/i);
  });

  it('reflects uncertainty without delegating', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What are you uncertain about?',
      session,
      context: session.context,
    });
    assert.ok(turn);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /uncertain|unknown|don't currently know/i);
  });

  it('says it does not currently know instead of calling Scout', async () => {
    const bare = {
      id: 'sess-empty',
      context: { tenantId: '10' },
    };
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'Who is Aji?',
      session: bare,
      context: bare.context,
    });
    assert.ok(turn);
    assert.match(turn.prose, /don't currently know/i);
    assert.doesNotMatch(turn.prose, /\bScout\b/i);
  });

  it('does not claim explicit investigation questions', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'Find commercial cleaning opportunities.',
      operatingEvidenceOpts: require('./workspaceTestHarness').unavailableOperatingEvidence(),
      session,
      context: session.context,
    });
    assert.equal(turn, null);
  });
});

describe('SPEC-102 Scout workspace entry', () => {
  it('refuses retrieval questions even with acquisition-loop stickiness', () => {
    const input = {
      question: 'What do you currently understand about our service area?',
      session: {
        context: {
          tenantId: '10',
          acquisitionLoop: true,
          lastScoutEvaluation: { id: 'ev-1' },
          domainId: 'acquisition',
        },
      },
      context: { tenantId: '10', domainId: 'acquisition', acquisitionLoop: true },
    };
    assert.equal(shouldHandleScoutAcquisition(input), false);
  });

  it('still claims explicit investigation', () => {
    assert.equal(
      shouldHandleScoutAcquisition({
        question: 'Find commercial cleaning opportunities.',
        context: { tenantId: '10' },
      }),
      true
    );
    assert.equal(
      shouldHandleScoutAcquisition({
        question: 'Investigate property managers.',
        context: { tenantId: '10' },
      }),
      true
    );
    assert.equal(
      shouldHandleScoutAcquisition({
        question: 'Research competitors.',
        context: { tenantId: '10' },
      }),
      true
    );
    assert.equal(
      shouldHandleScoutAcquisition({
        question: 'Look for expansion signals.',
        context: { tenantId: '10' },
      }),
      true
    );
  });

  it('does not execute Scout for a service-area question after a prior loop', async () => {
    const store = createMemoryStore();
    const service = createSpecialistDelegationService({ store });
    const session = {
      id: 'sess-sticky',
      context: {
        tenantId: '10',
        acquisitionLoop: true,
        lastScoutEvaluation: { id: 'ev-1' },
        domainId: 'acquisition',
      },
    };
    const before = (await store.listDelegations({ tenantId: '10' })).length;
    const turn = await maybeHandleScoutAcquisitionTurn({
      question: 'What do you currently understand about our service area?',
      session,
      context: session.context,
      delegationService: service,
      delegationOpts: { store },
      aoStore: createMemoryAcquisitionState(),
      companies: [],
    });
    assert.equal(turn, null);
    const after = (await store.listDelegations({ tenantId: '10' })).length;
    assert.equal(after, before);
  });
});

describe('SPEC-102 WorkspaceEngine wiring', () => {
  it('retrieves approved service-area memory without rerunning a previous Scout investigation', async () => {
    let scoutCalls = 0;
    const engine = require('./workspaceTestHarness').createHermeticWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      clientIntelligenceOpts: await approvedAnchorFixture(),
      scoutAcquisitionOpts: { runLoop: async () => { scoutCalls += 1; throw new Error('Unexpected Scout delegation'); } },
    });
    const opened = engine.open({ tenantId: '10', page: 'command-deck' });
    const session = engine._sessions.get(opened.sessionId);
    session.context.acquisitionLoop = true;
    session.context.lastScoutEvaluation = { id: 'existing-evaluation', materialChange: false };
    const result = await engine.ask({ sessionId: opened.sessionId, question: 'What do you currently understand about our service area?' });
    assert.match(result.prose, /Greater Manchester|Bedford|Hooksett/i);
    assert.equal(result.mission, null);
    assert.equal(result.scoutLoop, undefined);
    assert.equal(scoutCalls, 0);
    assert.ok(result.clientIntelligence && result.clientIntelligence.approved);
  });
});

const { createHermeticRetrievalHandler } = require('./workspaceTestHarness');
const maybeHandleRetrievalBeforeDelegationTurn = createHermeticRetrievalHandler(input => rawCieHandler({
  operatingEvidenceOpts: require('./workspaceTestHarness').unavailableOperatingEvidence(),
  ...input,
}));
