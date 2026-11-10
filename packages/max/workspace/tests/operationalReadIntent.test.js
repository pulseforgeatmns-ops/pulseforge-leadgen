'use strict';

/**
 * P0 #1110 — Operational read intent (unit).
 * Detector, rollback switch, ranking follow-up scope, eight-word Blueprint
 * follow-up constraint, resolver claim, and retrieval status vocabulary.
 */

const { describe, it, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyOperationalRead,
  operationalReadPrecedenceEnabled,
  recordOperationalRead,
} = require('../OperationalReadIntent');
const { shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { claimsOperationalRead } = require('../WorkspaceOwnershipResolver');
const {
  loadOperatingEvidence,
  RETRIEVAL_STATUS,
  prospectRetrievalStatus,
} = require('../OperatingEvidenceRetrieval');

// Exact production utterances (tenant 21, 2026-11-10 19:36 / 19:37 UTC).
const FAILURE_A =
  "Brief me on my prospects. Identify the top five opportunities, explain why they're ranked that way, and recommend the next action for each. Use actual workspace data and cite the supporting evidence.";
const FAILURE_B =
  'List the actual prospect records currently stored in this workspace. For each, return its name, record ID, prospect type, current status, and source. Do not use the Business Blueprint as a substitute for prospect data. If no records are available, explicitly state which data source was queried and why it returned no records.';

const READS = [
  [FAILURE_A, 'prospects'],
  [FAILURE_B, 'prospects'],
  ['brief me on my prospects', 'prospects'],
  ['Show my prospect list', 'prospects'],
  ['How many prospects do we have?', 'prospects'],
  ['Which leads are qualified?', 'prospects'],
  ['show my leads', 'prospects'],
  ['what tasks are due?', 'tasks'],
  ['What follow-ups are due today?', 'tasks'],
  ['how many emails went out?', 'emails'],
  ['Who replied to our emails?', 'emails'],
  ['any replies yet?', 'emails'],
  ['pipeline status?', 'pipeline'],
  ["What's in the pipeline today?", 'pipeline'],
  ['How many contacts did we import?', 'prospects'],
];

const NOT_READS = [
  // Blueprint advisory (must keep working)
  'What campaign would you recommend?',
  'Which actual companies should I call Monday?',
  'What is our pricing?',
  'Who are our ideal customers?',
  'What should we focus on this week?',
  'What do you think our biggest opportunity is?',
  "What's our pipeline strategy?",
  'How can we grow our pipeline?',
  'How should we follow up with our leads?',
  'Tell me more',
  // Discovery (Scout / missions)
  'Find new borrower prospects',
  'Find new borrower prospects in Charleston',
  'Identify new opportunities in Nashville',
  'Find more leads like our best customers',
  // Execution / persistence / desk workflows
  'Send emails to my prospects',
  'Launch outreach to our prospect list',
  'Draft follow-ups for my leads',
  'Create a task to call Bob',
  'Remember that our best leads are lenders',
  'Use 3 prospects from the existing Campaign 001 work for a preparation-only canary package.',
];

describe('P0 #1110 classifyOperationalRead', () => {
  for (const [question, object] of READS) {
    it(`reads: ${question.slice(0, 60)}`, () => {
      const read = classifyOperationalRead(question);
      assert.equal(read.isRead, true, `${question} → ${read.reason}`);
      assert.ok(read.objects.includes(object), `${question} objects=${read.objects}`);
    });
  }

  for (const question of NOT_READS) {
    it(`not a read: ${question.slice(0, 60)}`, () => {
      assert.equal(classifyOperationalRead(question).isRead, false, question);
    });
  }

  it('flags record-list and ranking requests (PR 2 is not implemented)', () => {
    assert.equal(classifyOperationalRead(FAILURE_B).wantsRecordList, true);
    assert.equal(classifyOperationalRead(FAILURE_A).wantsRanking, true);
  });

  it('treats a ranking follow-up as a read only right after an operational read', () => {
    const q = 'Why is the top opportunity ranked first?';
    const session = { operatorTurnSeq: 1, context: {} };
    recordOperationalRead(session, classifyOperationalRead(FAILURE_A));
    session.operatorTurnSeq = 2;
    const follow = classifyOperationalRead(q, { session });
    assert.equal(follow.isRead, true);
    assert.equal(follow.reason, 'ranking_follow_up_after_operational_read');

    session.operatorTurnSeq = 4; // two turns later — no longer immediate
    assert.equal(classifyOperationalRead(q, { session }).isRead, false);
    assert.equal(classifyOperationalRead(q).isRead, false);
  });
});

describe('P0 #1110 rollback switch', () => {
  it('is on by default and off for false/0/off', () => {
    assert.equal(operationalReadPrecedenceEnabled({}), true);
    assert.equal(operationalReadPrecedenceEnabled({ MAX_OPERATIONAL_READ_PRECEDENCE: 'true' }), true);
    for (const v of ['false', '0', 'off', 'OFF', 'disabled']) {
      assert.equal(operationalReadPrecedenceEnabled({ MAX_OPERATIONAL_READ_PRECEDENCE: v }), false, v);
    }
  });
});

describe('P0 #1110 Blueprint claim gate', () => {
  const prior = (operatorTurn) => ({
    kind: 'reasoning',
    reason: 'client_intelligence_reasoning',
    question: 'What should we focus on first?',
    operatorTurn,
  });

  it('never claims an operational read, even right after a Blueprint turn', () => {
    const session = { operatorTurnSeq: 2, context: { lastClientIntelligenceTurn: prior(1) } };
    for (const q of ['show my leads', 'brief me on my prospects', 'what tasks are due?', 'pipeline status?']) {
      assert.equal(shouldClaimClientIntelligenceTurn(q, session, { approvedBlueprint: true }), false, q);
    }
  });

  it('eight-word follow-up rule applies only to the immediately previous Blueprint turn', () => {
    const q = 'Tell me more';
    const immediate = { operatorTurnSeq: 2, context: { lastClientIntelligenceTurn: prior(1) } };
    const stale = { operatorTurnSeq: 4, context: { lastClientIntelligenceTurn: prior(1) } };
    assert.equal(shouldClaimClientIntelligenceTurn(q, immediate, { approvedBlueprint: true }), true);
    assert.equal(shouldClaimClientIntelligenceTurn(q, stale, { approvedBlueprint: true }), false);
  });

  it('keeps legacy behaviour for callers without an engine turn counter', () => {
    const session = { context: { lastClientIntelligenceTurn: { kind: 'reasoning' } } };
    assert.equal(shouldClaimClientIntelligenceTurn('Tell me more', session, { approvedBlueprint: true }), true);
  });

  it('keeps legitimate Blueprint advisory claims', () => {
    const session = { operatorTurnSeq: 1, context: {} };
    assert.equal(shouldClaimClientIntelligenceTurn('What campaign would you recommend?', session, { approvedBlueprint: true }), true);
    assert.equal(shouldClaimClientIntelligenceTurn('What should we focus on first?', session, { approvedBlueprint: true }), true);
  });
});

describe('P0 #1110 resolver claim', () => {
  afterEach(() => {
    delete process.env.MAX_OPERATIONAL_READ_PRECEDENCE;
  });

  it('claims Failure A ahead of Scout as an operational read', () => {
    const claim = claimsOperationalRead(FAILURE_A, {});
    assert.ok(claim);
    assert.equal(claim.owner, 'knowledge_retrieval');
    assert.equal(claim.reason, 'operational_read');
  });

  it('does not claim discovery, Blueprint advice, or desk workflows', () => {
    assert.equal(claimsOperationalRead('Find new borrower prospects in Charleston', {}), null);
    assert.equal(claimsOperationalRead('What campaign would you recommend?', {}), null);
    assert.equal(claimsOperationalRead('Who are our ideal customers?', {}), null);
  });

  it('leaves questions the existing operating-evidence detector owns on their legacy route', () => {
    assert.equal(claimsOperationalRead('What prospects do we already have?', {}), null);
  });

  it('is disabled by the rollback switch', () => {
    process.env.MAX_OPERATIONAL_READ_PRECEDENCE = 'false';
    assert.equal(claimsOperationalRead(FAILURE_A, {}), null);
  });
});

describe('P0 #1110 prospect retrieval status', () => {
  const others = () => {
    const nr = async () => ({ available: false, reason: 'unit_fixture_not_recorded' });
    return {
      loadCampaignAo: nr, loadScout: nr, loadMissions: nr, loadObjectives: nr,
      loadActivity: nr, loadOutcomes: nr, loadOperatorAttested: nr, loadCapability: nr,
    };
  };
  const load = (loadProspects) =>
    loadOperatingEvidence({ tenantId: '21', clientId: 21, operatingEvidenceOpts: { ...others(), loadProspects } });
  const prospectItem = (bundle) => bundle.items.find((i) => i.sourceKind === 'prospects');

  it('found: counts with a named source', async () => {
    const bundle = await load(async () => ({ available: true, counts: { total: 10, qualified: 3 } }));
    assert.equal(prospectRetrievalStatus(bundle.prospects), RETRIEVAL_STATUS.FOUND);
    assert.match(prospectItem(bundle).claim, /10 existing prospects.*prospects table/);
  });

  it('empty: zero rows is reported as zero rows, not as a failure', async () => {
    const bundle = await load(async () => ({ available: true, counts: { total: 0 } }));
    assert.equal(prospectRetrievalStatus(bundle.prospects), RETRIEVAL_STATUS.EMPTY);
    assert.match(prospectItem(bundle).claim, /0 rows/);
  });

  it('failed: a thrown query is a retrieval failure, never "nothing there"', async () => {
    const bundle = await load(async () => {
      const err = new Error('relation "prospects" does not exist SELECT secret');
      err.code = '42P01';
      throw err;
    });
    assert.equal(bundle.prospects.status, RETRIEVAL_STATUS.FAILED);
    assert.equal(bundle.prospects.errorClass, '42P01');
    const item = prospectItem(bundle);
    assert.match(item.claim, /retrieval failure, not an empty result/);
    assert.doesNotMatch(JSON.stringify(bundle), /SELECT secret/);
  });

  it('unavailable: no store is distinct from failed and from empty', async () => {
    const bundle = await load(async () => ({ available: false, reason: 'prospect_store_unavailable' }));
    assert.equal(prospectRetrievalStatus(bundle.prospects), RETRIEVAL_STATUS.UNAVAILABLE);
    assert.match(prospectItem(bundle).claim, /not an empty result/);
  });
});
