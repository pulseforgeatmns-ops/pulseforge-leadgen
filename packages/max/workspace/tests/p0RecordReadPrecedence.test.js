'use strict';
require('./workspaceTestHarness');
const { test, describe } = require('node:test');
const assert = require('node:assert/strict');

/**
 * P0 regression after #1115 (LA Endeavors, tenant 21, 2026-11-11).
 *
 * The production prompt below was claimed by the legacy operating-evidence
 * detector (`shouldRetrieveOperatingEvidence`: "prospects" + "recorded")
 * inside `claimsOperationalRead` BEFORE the operational-read classifier ran,
 * so the turn was routed `knowledge_retrieval/knowledge_retrieval` without
 * `operationalRead` and composed by the generic Business Intelligence path
 * (19-style counts plus Campaign 001 / AO / Yelp / walkthrough absences).
 * The #1115 fixture question passed only because it contains none of the
 * legacy trigger words. These tests pin record-level precedence through the
 * real routing → retrieval → composition → presentation path.
 */

const { fixture } = require('./fixtures/investigationProspects');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { claimsOperationalRead } = require('../WorkspaceOwnershipResolver');
const { classifyOperationalRead } = require('../OperationalReadIntent');
const { isOperatorOperatingUpdate } = require('../OperatorOperatingUpdate');
const { isClaimChallenge, isOperatorClaimCorrection } = require('../RecommendationClaimChallenge');
const { shouldRetrieveOperatingEvidence } = require('../OperatingEvidenceRetrieval');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');

const PRODUCTION_PROMPT =
  'Rank the six prospects currently marked contact_now by investigation priority, using their actual recorded evidence. ' +
  'Explain what distinguishes each prospect, whether the financing request fits our documented criteria, what information is missing, ' +
  'and what should be investigated next. Keep outreach authorization separate from investigation priority. ' +
  'Do not contact anyone or modify records.';

const NAMES = ['Alex Example', 'Blair Example', 'Casey Example', 'Devon Example', 'Emery Example', 'Frankie Example'];
const UNRELATED_ABSENCES = /Business Intelligence|Campaign 001|Yelp|walkthrough|\bAO\b|physical mail|job, or payment/i;

async function openEngine({ approved = true } = {}) {
  const rows = fixture();
  const store = createMemoryStore();
  if (approved) {
    let interview = await startClientInterview({ clientId: 21, forceNew: true }, { store });
    for (const answer of ['Lending to investors.', 'Bridge loans. Financing range: $9,000–$15,000.',
      'Real-estate operators.', 'Unsupported borrowers.', 'Example County.', 'Clear terms.', 'Professional.',
      'Qualified pipeline.', 'Funded loans.']) {
      interview = await postInterviewMessage(interview.interviewId, answer, { store });
    }
    await approveBlueprint(interview.blueprint.id, { store });
  }
  const engine = createHermeticWorkspaceEngine({
    clientIntelligenceOpts: { store },
    operatingEvidenceOpts: {
      ...unavailableOperatingEvidence(),
      loadProspects: async input => {
        assert.equal(input.clientId, 21);
        return { available: true, counts: { total: rows.length },
          records: { status: 'found', complete: true, rows: [...rows, { ...fixture(22)[0], notes: 'FOREIGN_SECRET' }] } };
      },
    },
  });
  const ask = async question => {
    const opened = engine.open({ tenantId: '21', page: 'command-deck' });
    return engine.ask({ sessionId: opened.sessionId, question });
  };
  return { rows, engine, ask };
}

describe('exact failed production prompt (full engine)', () => {
  test('the prompt contains a legacy trigger word — the boundary the #1115 fixture missed', () => {
    assert.equal(shouldRetrieveOperatingEvidence(PRODUCTION_PROMPT), true);
    const read = classifyOperationalRead(PRODUCTION_PROMPT);
    assert.equal(read.isRead, true);
    assert.equal(read.reviewState, 'contact_now');
    assert.equal(read.priorityMode, 'investigation');
    assert.equal(read.wantsRanking, true);
  });

  test('routes to operational_read and returns six named records with investigation priority, sources and blocked outreach', async () => {
    const { ask, rows, engine } = await openEngine();
    const before = JSON.stringify(rows);
    const r = await ask(PRODUCTION_PROMPT);

    assert.equal(r.workspaceOwnership.owner, 'knowledge_retrieval');
    assert.equal(r.workspaceOwnership.reason, 'operational_read');
    assert.equal(r.presentation, 'prospect_records');
    assert.equal(r.structured.metadata.recordIntelligence, true);
    assert.equal(r.structured.metadata.investigationPriority.policy, 'recorded-deal-coverage-v1');
    assert.equal(r.structured.metadata.investigationPriority.ranked, true);
    assert.equal(r.structured.metadata.investigationPriority.reviewState, 'contact_now');

    const evidence = r.structured.supportingEvidence.filter(item => item.recordRef);
    assert.equal(evidence.length, 6);
    for (const name of NAMES) assert.match(r.prose, new RegExp(name));
    assert.ok(evidence.every(item => /^prospect:21:/.test(item.recordRef)));
    assert.ok(evidence.some(item => item.priority && item.priority.mode === 'investigation'));
    assert.match(r.prose, /Provisional investigation priority — analytical attention only, not outreach priority/);
    assert.match(r.prose, /showing 6/);
    assert.match(r.prose, /\$9,000–\$15,000/);

    // Source evidence: record citations, recorded source URLs and raw notes kept as recorded claims.
    for (let i = 1; i <= 6; i += 1) assert.match(r.prose, new RegExp(`https://example\\.invalid/evidence/${i}`));
    assert.ok(evidence.every(item => item.recordedClaims && item.recordedClaims.epistemic === 'recorded_claim'));
    assert.ok(evidence.every(item => item.recordedClaims.sourceUrls.length === 1));
    assert.ok(evidence.every(item => /Fit evidence:/.test(item.recordedClaims.notes)));

    // Outreach authorization stays separate and blocked.
    assert.equal((r.prose.match(/Outreach eligibility: blocked; existing gates remain in force/g) || []).length, 6);
    assert.equal(r.structured.metadata.executed, false);
    assert.equal(r.structured.metadata.specialistDelegated, false);
    assert.equal(r.retrieval.delegated, false);

    // No unrelated generic absences, no other tenant, no writes.
    assert.doesNotMatch(r.prose, UNRELATED_ABSENCES);
    assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:/);
    assert.doesNotMatch(JSON.stringify(engine.sessions.get(r.sessionId)), /FOREIGN_SECRET/);
    assert.equal(JSON.stringify(rows), before);
  });
});

describe('trigger-word routing matrix (resolver)', () => {
  const recordLevel = [
    'Rank my contact_now prospects by their recorded evidence.',
    'Rank my top five contact_now prospects using their recorded notes.',
    'Rank my prospects by outreach priority based on recorded evidence.',
    'List my existing prospects.',
    'Show me the prospects already in my CRM.',
    'Which prospects have we already contacted?',
    'Show the inventory of my prospects.',
    'List my recorded qualify_first prospects.',
    'Show my existing hold prospects.',
    'List my existing prospects after record ID 00000000-0000-0000-0000-000000000003.',
    'Rank the actual recorded contact_now prospects for investigation.',
  ];
  for (const q of recordLevel) {
    test(`record-level → operational_read: ${q}`, () => {
      assert.equal(shouldRetrieveOperatingEvidence(q), true, 'fixture must contain a legacy trigger');
      const claim = claimsOperationalRead(q);
      assert.ok(claim, q);
      assert.equal(claim.reason, 'operational_read');
    });
  }

  test('pagination cursor survives into the operational read', () => {
    const claim = claimsOperationalRead('List my existing prospects after record ID 00000000-0000-0000-0000-000000000003.');
    assert.equal(claim.operationalRead.cursor, '00000000-0000-0000-0000-000000000003');
  });

  const legacyNonRecord = [
    'What acquisition activity is already recorded?',
    "Given what we've already tried and what PulseForge can actually verify, what should I focus on next to build the repeatable commercial pipeline?",
    "Before recommending anything, tell me what we've already tried.",
    'What prospect activity is already recorded?',
  ];
  for (const q of legacyNonRecord) {
    test(`genuine non-record operating question keeps legacy route: ${q}`, () => {
      assert.equal(claimsOperationalRead(q), null, q);
    });
  }

  test('rollback switch still restores pre-#1110 routing', () => {
    const old = process.env.MAX_OPERATIONAL_READ_PRECEDENCE;
    try {
      process.env.MAX_OPERATIONAL_READ_PRECEDENCE = 'false';
      assert.equal(claimsOperationalRead(PRODUCTION_PROMPT), null);
    } finally {
      if (old === undefined) delete process.env.MAX_OPERATIONAL_READ_PRECEDENCE;
      else process.env.MAX_OPERATIONAL_READ_PRECEDENCE = old;
    }
  });
});

describe('operator update / claim challenge / claim correction keep priority', () => {
  const cases = [
    ['update', 'Operating update: list my contact_now prospects — six were reviewed today.', isOperatorOperatingUpdate],
    ['update', 'Operating update: which prospects are contact_now — six reviewed today.', isOperatorOperatingUpdate],
    ['challenge', "That isn't true — my recorded prospects list shows 6 contact_now prospects.", isClaimChallenge],
    ['challenge', "That's incorrect. Show my existing prospects again.", isClaimChallenge],
    ['challenge', "No, that's wrong — list my existing prospects.", isClaimChallenge],
    ['correction', "Email outbound isn't running, so list my existing contact_now prospects.", isOperatorClaimCorrection],
    ['correction', 'Outbound is not active — show my recorded contact_now prospects.', isOperatorClaimCorrection],
  ];
  for (const [kind, q, detector] of cases) {
    test(`${kind} is never claimed as a record read: ${q}`, () => {
      assert.equal(detector(q), true, 'fixture must be recognized by its owner');
      assert.equal(classifyOperationalRead(q).isRead, true, 'fixture must also look like a record read');
      assert.equal(claimsOperationalRead(q), null);
    });
  }
});

describe('record-modification requests are not routed to read-only record retrieval', () => {
  const modifications = [
    'Mark the six recorded prospects as hold.',
    'Move the existing contact_now prospects to hold.',
    'Change the review state of my recorded prospects to qualify_first.',
    'Update the recorded notes for my contact_now prospects.',
    'Delete the existing prospects already marked reject.',
    'Remove the duplicate prospects already in my CRM.',
    'Assign my existing contact_now prospects to Levi.',
    'Email the six contact_now prospects already recorded.',
    'Set my existing prospects to contacted.',
    'Import these prospects into my existing list.',
    'Archive the recorded prospects marked hold.',
    'Approve outreach for my existing contact_now prospects.',
    'Mark Alex Example as qualified in my existing records.',
    'Reject the prospects already marked hold.',
    'Enroll my existing contact_now prospects in the sequence.',
    'Promote my recorded qualify_first prospects to contact_now.',
  ];
  for (const q of modifications) {
    test(`not an operational read: ${q}`, () => {
      assert.equal(classifyOperationalRead(q).isRead, false, q);
      assert.equal(claimsOperationalRead(q), null, q);
    });
  }

  test('full engine: a review-state change request is never answered with a record listing', async () => {
    const { ask, rows } = await openEngine({ approved: false });
    const before = JSON.stringify(rows);
    const r = await ask('Change the review state of my recorded prospects to qualify_first.');
    assert.notEqual(r.workspaceOwnership.reason, 'operational_read');
    assert.notEqual(r.presentation, 'prospect_records');
    assert.equal(JSON.stringify(rows), before);
  });
});

describe('plain listing through the full engine', () => {
  for (const q of ['List my existing prospects.', 'Which prospects have we already contacted?', 'Show the inventory of my prospects.']) {
    test(`returns tenant records, not Business Intelligence: ${q}`, async () => {
      const { ask } = await openEngine({ approved: false });
      const r = await ask(q);
      assert.equal(r.workspaceOwnership.reason, 'operational_read');
      assert.equal(r.presentation, 'prospect_records');
      assert.equal(r.structured.supportingEvidence.filter(item => item.recordRef).length, 6);
      for (const name of NAMES) assert.match(r.prose, new RegExp(name));
      assert.doesNotMatch(r.prose, UNRELATED_ABSENCES);
      assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET/);
      assert.equal(r.structured.metadata.executed, false);
    });
  }
});
