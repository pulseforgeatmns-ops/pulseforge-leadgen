'use strict';

/**
 * SPEC-106 — operator-reported operating evidence.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { maybeHandleRetrievalBeforeDelegationTurn } = require('../RetrievalBeforeDelegationContext');
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
  shouldRetrieveOperatingEvidence,
  loadOperatingEvidence,
  composeOperatingEvidenceAnswer,
} = require('../OperatingEvidenceRetrieval');
const {
  TURN_TYPE,
  EPISTEMIC,
  SEMANTIC,
  DISPOSITION,
  isOperatorOperatingUpdate,
  extractOperatingAssertions,
  maybeHandleOperatorOperatingUpdate,
} = require('../OperatorOperatingUpdate');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const { createKnowledgeRuntime } = require('../../../knowledge');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');

const ANCHOR_ID = 10;
const OTHER_ID = 1;
const PILOT_NOW = '2026-08-17T16:00:00.000-04:00';
const PILOT_MESSAGE =
  'Quick operating update: Campaign 001 was physically mailed on August 6. I met with Mike, one of our AOs, earlier today for training and walked him through the app and workflow. Follow-up on those 20 Campaign 001 leads should begin tomorrow.';

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

function anchorLoaders() {
  return {
    loadCampaignAo: async ({ clientId }) => ({
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
      leads: [{ id: 1, client_id: clientId, campaign_name: 'Campaign 001', operational_state: 'not_started' }],
    }),
    loadProspects: async () => ({ available: true, counts: { total: 18, qualified: 7 } }),
    loadScout: async () => ({ available: true, launchedNewWork: false, intelligence: { counts: { matched: 2 } }, state: {} }),
    loadMissions: async () => ({ available: true, rows: [] }),
    loadObjectives: async () => ({ available: true, rows: [] }),
    loadActivity: async () => ({ available: true, touchpoints: [], activity: [] }),
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
  };
}

function updateOpts(knowledge, extras = {}) {
  return {
    knowledge,
    now: extras.now || PILOT_NOW,
    timeZone: 'America/New_York',
    people: extras.people || [{ id: 'ao-mike', name: 'Mike', role: 'ao', client_id: ANCHOR_ID }],
    campaignLeadCount: 20,
    rebuildOperatorContext: extras.rebuildOperatorContext || (async () => {}),
    ...extras,
  };
}

function engineFor(knowledge, extras = {}) {
  return createWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: false,
    operatingUpdateOpts: updateOpts(knowledge, extras),
    operatingEvidenceOpts: {
      ...anchorLoaders(),
      knowledge,
    },
  });
}

describe('SPEC-106 routing', () => {
  it('classifies a mailed assertion as an operating update, not CIE', () => {
    const question = 'Campaign 001 was mailed August 6.';
    assert.equal(isOperatorOperatingUpdate(question), true);
    assert.equal(isOperatingEvidenceQuestion(question), false);
    assert.equal(shouldRetrieveOperatingEvidence(question), false);
    assert.equal(
      shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
      false
    );
  });

  it('routes Was Campaign 001 mailed? to SPEC-105 retrieval, not SPEC-106 write', () => {
    const question = 'Was Campaign 001 mailed?';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(shouldRetrieveOperatingEvidence(question), true);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RETRIEVAL);
  });

  it('does not swallow recommendation questions', () => {
    const question = 'What should we do next with Campaign 001?';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.RECOMMENDATION);
  });

  it('does not swallow Scout investigation', () => {
    const question = 'Find additional property managers for Campaign 001.';
    assert.equal(isOperatorOperatingUpdate(question), false);
    assert.equal(shouldHandleScoutAcquisition({ question, context: { tenantId: '10' } }), true);
    assert.equal(classifyCognitiveMode(question).kind, COGNITIVE_MODES.INVESTIGATION);
  });

  it('WorkspaceEngine places operating-update recognition after retrieval and before CIE', () => {
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
  });
});

describe('SPEC-106 Pilot message', () => {
  let knowledge;
  let cieCalled;
  let scoutCalled;
  let externalCalled;

  beforeEach(() => {
    knowledge = createKnowledgeRuntime({ withSync: false, startIngestor: false }).knowledge;
    cieCalled = 0;
    scoutCalled = 0;
    externalCalled = 0;
  });

  it('extracts three classified candidate events from the Pilot message', () => {
    const assertions = extractOperatingAssertions(PILOT_MESSAGE, {
      operatingUpdateOpts: updateOpts(knowledge),
    });
    assert.equal(assertions.length, 3);
    const mail = assertions.find((a) => a.semanticType === SEMANTIC.CAMPAIGN_EXECUTION);
    const training = assertions.find((a) => a.semanticType === SEMANTIC.INTERNAL_OPERATIONAL_EVENT);
    const followUp = assertions.find((a) => a.semanticType === SEMANTIC.CAMPAIGN_FOLLOW_UP);
    assert.ok(mail);
    assert.equal(mail.temporalClass, 'completed');
    assert.equal(mail.epistemicState, EPISTEMIC.OPERATOR_ATTESTED);
    assert.equal(mail.occurredAt, '2026-08-06');
    assert.ok(training);
    assert.equal(training.temporalClass, 'completed');
    assert.equal(training.occurredAt, '2026-08-17');
    assert.ok(followUp);
    assert.equal(followUp.temporalClass, 'expected');
    assert.equal(followUp.expectedAt, '2026-08-18');
    assert.notEqual(followUp.temporalClass, 'completed');
    assert.notEqual(followUp.value, 'completed');
  });

  it('handles the Pilot turn as operating_update without CIE, Scout, or outreach', async () => {
    const turn = await maybeHandleOperatorOperatingUpdate({
      question: PILOT_MESSAGE,
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge, {
        rebuildOperatorContext: async () => {},
      }),
    });
    assert.ok(turn);
    assert.equal(turn.turnType, TURN_TYPE);
    assert.equal(turn.assertions.length, 3);
    assert.equal(turn.cieClaimed, false);
    assert.equal(turn.launchedScout, false);
    assert.equal(turn.externalAction, false);
    assert.equal(turn.aoMutated, false);
    assert.match(turn.prose, /operator-reported as physically mailed on 2026-08-06/i);
    assert.match(turn.prose, /Mike completed AO workflow training today/i);
    assert.match(turn.prose, /expected to begin 2026-08-18/i);
    assert.match(turn.prose, /have not treated/i);
    assert.doesNotMatch(turn.prose, /KNOWN|INFERENCE|UNKNOWN|EVIDENCE NEEDED/);

    const cie = await maybeHandleClientIntelligenceTurn({
      question: PILOT_MESSAGE,
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
    });
    if (cie && cie.handled) cieCalled += 1;
    assert.equal(cie.handled, false);
    assert.equal(cie.skipReason, 'operating_update');
    assert.equal(cieCalled, 0);

    const scout = await maybeHandleScoutAcquisitionTurn({
      question: PILOT_MESSAGE,
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
    });
    if (scout) scoutCalled += 1;
    assert.equal(scout, null);
    assert.equal(scoutCalled, 0);
    assert.equal(externalCalled, 0);
  });

  it('WorkspaceEngine Pilot path acknowledges all three events', async () => {
    const engine = engineFor(knowledge);
    const opened = engine.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: PILOT_MESSAGE,
    });
    assert.equal(result.domainDecision.reason, 'operator_operating_update');
    assert.equal(result.operatingUpdate.turnType, TURN_TYPE);
    assert.equal(result.operatingUpdate.assertions.length, 3);
    assert.equal(result.clientIntelligence, undefined);
    assert.equal(result.scoutLoop, undefined);
    assert.match(result.prose, /operator-reported as physically mailed/i);
    assert.match(result.prose, /expected to begin/i);
    assert.doesNotMatch(result.prose, /I'd recommend a focused first campaign/i);
  });
});

describe('SPEC-106 durability and correction', () => {
  let knowledge;

  beforeEach(() => {
    knowledge = createKnowledgeRuntime({ withSync: false, startIngestor: false }).knowledge;
  });

  it('recovers Event A from a fresh workspace without the prior SessionStore', async () => {
    const first = engineFor(knowledge);
    const opened = first.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    await first.ask({
      sessionId: opened.sessionId,
      question: 'Campaign 001 was physically mailed on August 6.',
    });

    const fresh = engineFor(knowledge);
    const reopened = fresh.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    assert.notEqual(reopened.sessionId, opened.sessionId);
    const result = await fresh.ask({
      sessionId: reopened.sessionId,
      question: "What's the current state of Campaign 001?",
    });
    assert.equal(result.domainDecision.reason, 'operating_evidence_retrieval');
    assert.match(result.prose, /operator-reported as physically mailed on 2026-08-06/i);
    assert.doesNotMatch(result.prose, /verified mailed August 6/i);
    assert.doesNotMatch(result.prose, /KNOWN\n/i);
  });

  it('supersedes an August 6 claim with an August 7 correction and keeps history', async () => {
    const context = { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID };
    const opts = updateOpts(knowledge);
    await maybeHandleOperatorOperatingUpdate({
      question: 'Campaign 001 was mailed August 6.',
      context,
      operatingUpdateOpts: opts,
    });
    const correction = await maybeHandleOperatorOperatingUpdate({
      question: 'Correction: Campaign 001 actually went out August 7, not August 6.',
      context,
      operatingUpdateOpts: opts,
    });
    assert.ok(correction);
    const mail = correction.results.find((r) => r.assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION);
    assert.equal(mail.assertion.occurredAt, '2026-08-07');
    assert.equal(mail.disposition, DISPOSITION.PERSISTED);

    const claims = await knowledge.findClaims({ tenantId: String(ANCHOR_ID), limit: 20 });
    const mailClaims = claims.filter((c) => c.metadata && c.metadata.predicate === 'physical_mail_execution');
    const active = mailClaims.filter((c) => c.status === 'active');
    const inactive = mailClaims.filter((c) => c.status !== 'active');
    assert.equal(active.length, 1);
    assert.equal(active[0].metadata.occurredAt, '2026-08-07');
    assert.equal(active[0].metadata.epistemicState, EPISTEMIC.OPERATOR_ATTESTED);
    assert.ok(inactive.length >= 1);
    assert.ok(inactive.some((c) => c.metadata && c.metadata.occurredAt === '2026-08-06'));

    const evidence = await knowledge.findEvidence({
      tenantId: String(ANCHOR_ID),
      sourceType: 'operator_report',
      limit: 20,
    });
    assert.ok(evidence.length >= 2);

    const fresh = engineFor(knowledge);
    const opened = fresh.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const result = await fresh.ask({
      sessionId: opened.sessionId,
      question: 'When was Campaign 001 mailed?',
    });
    assert.match(result.prose, /operator-reported as mailed 2026-08-07/i);
    assert.match(result.prose, /earlier operator report listed 2026-08-06/i);
  });

  it('does not treat an expected follow-up as completed after the date passes', async () => {
    const context = { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID };
    await maybeHandleOperatorOperatingUpdate({
      question: 'Follow-up should begin tomorrow.',
      context,
      operatingUpdateOpts: updateOpts(knowledge, { now: PILOT_NOW }),
    });

    const later = engineFor(knowledge, { now: '2026-08-20T12:00:00.000-04:00' });
    const opened = later.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const result = await later.ask({
      sessionId: opened.sessionId,
      question: 'Did follow-up begin?',
    });
    assert.match(result.prose, /expected to begin 2026-08-18/i);
    assert.match(result.prose, /don'?t have recorded execution confirming that it actually began/i);
    assert.doesNotMatch(result.prose, /follow-up (?:has )?completed/i);
    assert.doesNotMatch(result.prose, /follow-up began/i);
  });
});

describe('SPEC-106 tenant isolation and ambiguity', () => {
  let knowledge;

  beforeEach(() => {
    knowledge = createKnowledgeRuntime({ withSync: false, startIngestor: false }).knowledge;
  });

  it('scopes operator-attested mail to the authorized tenant', async () => {
    await maybeHandleOperatorOperatingUpdate({
      question: 'Campaign 001 was mailed August 6.',
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge),
    });
    const anchorClaims = await knowledge.findClaims({ tenantId: String(ANCHOR_ID), limit: 20 });
    assert.ok(anchorClaims.some((c) => c.metadata && c.metadata.clientId === ANCHOR_ID));
    const leaked = await knowledge.findClaims({ tenantId: String(OTHER_ID), limit: 20 });
    assert.equal(leaked.length, 0);

    const otherEngine = engineFor(knowledge);
    const otherOpen = otherEngine.open({
      tenantId: String(OTHER_ID),
      clientId: OTHER_ID,
      page: 'command-deck',
    });
    const otherAsk = await otherEngine.ask({
      sessionId: otherOpen.sessionId,
      question: 'Was Campaign 001 mailed?',
    });
    assert.doesNotMatch(otherAsk.prose, /operator-reported as (?:physically )?mailed on 2026-08-06/i);

    const anchorEngine = engineFor(knowledge);
    const anchorOpen = anchorEngine.open({
      tenantId: String(ANCHOR_ID),
      clientId: ANCHOR_ID,
      page: 'command-deck',
    });
    const anchorAsk = await anchorEngine.ask({
      sessionId: anchorOpen.sessionId,
      question: 'Was Campaign 001 mailed?',
    });
    assert.match(anchorAsk.prose, /operator-reported as mailed 2026-08-06/i);
  });

  it('fails closed when two people named Mike exist', async () => {
    const turn = await maybeHandleOperatorOperatingUpdate({
      question: 'Mike finished training today.',
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge, {
        people: [
          { id: 'ao-1', name: 'Mike Smith', role: 'ao' },
          { id: 'ao-2', name: 'Mike Jones', role: 'ao' },
        ],
      }),
    });
    assert.ok(turn);
    const training = turn.results.find(
      (r) => r.assertion.semanticType === SEMANTIC.INTERNAL_OPERATIONAL_EVENT
    );
    assert.ok(training);
    assert.equal(training.assertion.entityResolution, 'ambiguous');
    assert.equal(training.disposition, DISPOSITION.REJECTED);
    assert.equal(training.assertion.subject.id, null);
    assert.match(turn.prose, /could not uniquely resolve Mike/i);
  });

  it('does not invent client identity from natural-language text', async () => {
    const turn = await maybeHandleOperatorOperatingUpdate({
      question: 'For client 1, Campaign 001 was mailed August 6.',
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge),
    });
    const mail = turn.results.find((r) => r.assertion.semanticType === SEMANTIC.CAMPAIGN_EXECUTION);
    assert.equal(mail.claim.metadata.clientId, ANCHOR_ID);
    const leaked = await knowledge.findClaims({ tenantId: '1', limit: 10 });
    assert.equal(leaked.length, 0);
  });
});

describe('SPEC-106 persistence policy', () => {
  it('persists Event A, acknowledges Event B only, and requires confirmation for Event C cohort mutation', async () => {
    const knowledge = createKnowledgeRuntime({ withSync: false, startIngestor: false }).knowledge;
    const turn = await maybeHandleOperatorOperatingUpdate({
      question: PILOT_MESSAGE,
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge),
    });
    const byType = Object.fromEntries(turn.results.map((r) => [r.assertion.semanticType, r]));
    assert.equal(byType[SEMANTIC.CAMPAIGN_EXECUTION].disposition, DISPOSITION.PERSISTED);
    assert.ok(byType[SEMANTIC.CAMPAIGN_EXECUTION].claim);
    assert.equal(byType[SEMANTIC.INTERNAL_OPERATIONAL_EVENT].disposition, DISPOSITION.ACKNOWLEDGED_ONLY);
    assert.equal(byType[SEMANTIC.INTERNAL_OPERATIONAL_EVENT].claim, null);
    assert.equal(byType[SEMANTIC.CAMPAIGN_FOLLOW_UP].disposition, DISPOSITION.CONFIRMATION_REQUIRED);
    assert.equal(byType[SEMANTIC.CAMPAIGN_FOLLOW_UP].aoMutated, false);
    assert.match(turn.prose, /have not silently created or modified/i);
  });
});

describe('SPEC-106 retrieval compose', () => {
  it('keeps operator-attested mail out of the verified bucket', async () => {
    const knowledge = createKnowledgeRuntime({ withSync: false, startIngestor: false }).knowledge;
    await maybeHandleOperatorOperatingUpdate({
      question: 'Campaign 001 was physically mailed on August 6.',
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingUpdateOpts: updateOpts(knowledge),
    });
    const bundle = await loadOperatingEvidence({
      context: { tenantId: String(ANCHOR_ID), clientId: ANCHOR_ID },
      operatingEvidenceOpts: { ...anchorLoaders(), knowledge },
    });
    const composed = composeOperatingEvidenceAnswer("What's the current state of Campaign 001?", bundle);
    assert.match(composed.prose, /operator-reported as physically mailed on 2026-08-06/i);
    const mail = bundle.items.find((item) => item.debugSource === 'operator_attested_mail');
    assert.ok(mail);
    assert.equal(mail.epistemic, 'operator_attested');
  });
});
