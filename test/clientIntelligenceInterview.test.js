'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  SESSION_STATUSES,
  ALLOWED_TRANSITIONS,
  BLUEPRINT_SECTIONS,
  QUESTION_BANK,
  ClientIntelligenceError,
  createMemoryStore,
  scoreEvidenceConfidence,
  summarizeSection,
  computeProgress,
  buildReflection,
  hasSpecificitySignals,
  looksAmbiguous,
  assertTransition,
  startClientInterview,
  postInterviewMessage,
  getInterview,
  reviseBlueprint,
  approveBlueprint,
  answerLooksEmpty,
} = require('../services/clientIntelligenceInterview');

const {
  SECTION_PROVENANCE,
} = require('../services/clientIntelligencePlaybookHandoff');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const AJI_ANSWERS = [
  'Aji Home Services — premium residential cleaning.',
  'Recurring cleans, deep cleans, and move-out cleans.',
  'Busy homeowners and property managers.',
  'Bargain hunters and warehouses.',
  'Coastal South Carolina.',
  'Reliable crews and clear communication.',
  'Friendly professional voice.',
  'Book qualified cleaning appointments in 90 days.',
  'Appointments booked and close rate.',
];

async function completeInterview(opts) {
  const started = await startClientInterview({ clientId: 42 }, opts);
  let turn = started;
  for (const answer of AJI_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  return { started, turn };
}

describe('clientIntelligenceInterview lifecycle', () => {
  it('allows only sequential status transitions', () => {
    assert.deepEqual(ALLOWED_TRANSITIONS.NEW, ['DISCOVERY']);
    assert.deepEqual(ALLOWED_TRANSITIONS.DISCOVERY, ['CLARIFICATION']);
    assert.throws(() => assertTransition('NEW', 'APPROVED'), ClientIntelligenceError);
    assert.throws(() => assertTransition('DISCOVERY', 'APPROVED'), ClientIntelligenceError);
    assert.equal(SESSION_STATUSES.length, 7);
  });

  it('starts interactive interview with first question and goal/askedBecause', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 7 }, opts);
    assert.ok(started.interviewId);
    assert.equal(started.status, 'DISCOVERY');
    assert.equal(started.nextAction, 'ASK');
    assert.equal(started.question.id, 'identity');
    assert.ok(started.question.goal);
    assert.ok(started.question.askedBecause);
    assert.match(started.message, /tell me about the business/i);
    assert.equal(started.progress.percent, 0);
    assert.equal(started.progress.label, 'Business Understanding');
  });

  it('extracts evidence after every answer and advances stages', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 8 }, opts);
    const turn = await postInterviewMessage(
      started.interviewId,
      'Aji Home Services provides residential cleaning.',
      opts
    );
    assert.ok(turn.evidence);
    assert.equal(turn.evidence.type, 'EXPLICIT');
    assert.equal(turn.evidence.category, 'identity');
    assert.ok(turn.evidence.confidence >= 0.7);
    assert.equal(turn.question.id, 'services');

    const detail = await getInterview(started.interviewId, opts);
    assert.equal(detail.evidence.length, 1);
    assert.ok(detail.turns.some((t) => t.speaker === 'client' && t.goal));
    assert.ok(detail.turns.some((t) => t.askedBecause));
  });

  it('completes discovery through full lifecycle to CLIENT_REVIEW blueprint', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    assert.equal(turn.status, 'CLIENT_REVIEW');
    assert.ok(turn.blueprint);
    assert.equal(turn.blueprint.generatedBy, 'CIE-v1');
    assert.equal(turn.blueprint.status, 'in_review');
    for (const key of BLUEPRINT_SECTIONS) {
      assert.ok(turn.blueprint.sections[key]);
      assert.ok(Array.isArray(turn.blueprint.sections[key].evidenceIds));
      assert.ok(Array.isArray(turn.blueprint.sections[key].unknowns));
      assert.ok('confidence' in turn.blueprint.sections[key]);
      assert.ok(turn.blueprint.sections[key].summary);
      // Consultant notes — not raw transcript mirroring
      assert.notEqual(turn.blueprint.sections[key].summary, AJI_ANSWERS[BLUEPRINT_SECTIONS.indexOf(key)]);
    }
    assert.equal(QUESTION_BANK.length, 9);
    assert.equal(turn.progress.percent, 100);
  });

  it('inserts conversational reflections every 3 answers without adding questions', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 11 }, opts);
    let turn = started;
    for (let i = 0; i < 3; i += 1) {
      turn = await postInterviewMessage(started.interviewId, AJI_ANSWERS[i], opts);
    }
    assert.ok(turn.reflection);
    assert.match(turn.reflection, /so far i'm hearing|let me make sure i understand|taking away so far/i);
    assert.equal(turn.question.id, 'avoid_customers');
    assert.equal(QUESTION_BANK.length, 9);

    const detail = await getInterview(started.interviewId, opts);
    const reflections = detail.turns.filter(
      (t) => t.speaker === 'assistant' && /so far i'm hearing|let me make sure i understand|taking away so far/i.test(t.message)
    );
    assert.equal(reflections.length, 1);
    assert.equal(turn.progress.completed, 3);
  });

  it('notes mode skips Q&A and generates blueprint', async () => {
    const { opts } = withStore();
    const started = await startClientInterview(
      {
        clientId: 9,
        notes:
          'We are Aji Home Services. We sell residential cleaning. Ideal customers are homeowners. Avoid warehouses. Market is Myrtle Beach. Advantage is reliable crews. Voice is friendly. Goal is more appointments. Success metric is booked jobs.',
      },
      opts
    );
    assert.equal(started.status, 'CLIENT_REVIEW');
    assert.ok(started.blueprint);
    assert.ok(started.blueprint.sections.identity.summary);
    assert.match(started.blueprint.sections.identity.summary, /Aji Home Services/i);
    assert.equal(
      /^We are Aji Home Services\.?$/i.test(started.blueprint.sections.identity.summary),
      false
    );
  });
});

describe('consultant summaries', () => {
  it('rewrites identity into understanding notes', () => {
    const summary = summarizeSection('identity', [
      'Anchor Cleaning—we are a commercial focused cleaning company serving Greater Manchester',
    ]);
    assert.match(summary, /Anchor Cleaning is a commercial/i);
    assert.ok(summary.split(/(?<=[.!?])\s+/).length <= 4);
  });
});

describe('confidence rules', () => {
  it('does not use response length — short explicit can score high', () => {
    const short = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Property managers.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const long = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement:
        'Property managers are our preferred ICP and we love working with them across many buildings and portfolios every single week of the year.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    // Length alone must not change score; both share the property-managers specificity signal.
    assert.equal(short, long);
  });

  it('differs naturally across specificity, ambiguity, confirmation, and contradiction', () => {
    const vague = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Maybe various customers, not sure.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const specific = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Commercial property managers in Manchester.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    assert.ok(specific > vague);
    assert.equal(looksAmbiguous('Maybe various customers, not sure.'), true);
    assert.equal(hasSpecificitySignals('Commercial property managers in Manchester.'), true);

    const base = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'We serve property managers.',
      priorStatements: [],
      isConfirmation: false,
      hasCorroboration: false,
    });
    const confirmed = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'Yes, property managers are correct.',
      priorStatements: ['We serve property managers.'],
      isConfirmation: true,
      hasCorroboration: true,
    });
    assert.ok(confirmed > base);

    const contradicted = scoreEvidenceConfidence({
      type: 'EXPLICIT',
      statement: 'We never serve property managers.',
      priorStatements: ['We serve property managers.'],
      isConfirmation: false,
      hasCorroboration: true,
    });
    assert.ok(contradicted < base);
  });

  it('spreads section confidence across a completed interview', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const scores = BLUEPRINT_SECTIONS.map((k) => turn.blueprint.sections[k].confidence);
    const unique = new Set(scores.map((n) => Number(n.toFixed(3))));
    assert.ok(unique.size >= 2, 'expected confidence values to differ across sections');
  });

  it('treats empty answers as unknowns without inventing facts', () => {
    assert.equal(answerLooksEmpty('n/a'), true);
    assert.equal(answerLooksEmpty('Property managers'), false);
  });

  it('progress is driven by completed blueprint sections', () => {
    const progress = computeProgress({
      identity: { summary: 'Acme is a cleaning company.' },
      services: { summary: '' },
      idealCustomers: { summary: 'Busy homeowners.' },
    });
    assert.equal(progress.completed, 2);
    assert.equal(progress.total, BLUEPRINT_SECTIONS.length);
    assert.equal(progress.percent, Math.round((2 / BLUEPRINT_SECTIONS.length) * 100));
  });

  it('buildReflection summarizes current understanding only', () => {
    const text = buildReflection(
      {
        identity: { summary: 'Aji Home Services is a premium residential cleaning company.' },
        services: { summary: 'Today the business delivers recurring and deep cleans.' },
      },
      3
    );
    assert.match(text, /so far i'm hearing/i);
    assert.match(text, /Identity:/);
    assert.match(text, /Services:/);
  });
});

describe('blueprint revise + approve + playbook handoff', () => {
  it('revises in_review blueprint and records CLIENT_EDITED evidence', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const revised = await reviseBlueprint(
      turn.blueprint.id,
      { identity: { summary: 'Aji Home Services LLC' } },
      opts
    );
    assert.equal(revised.sections.identity.summary, 'Aji Home Services LLC');
    assert.equal(revised.version, '1.0');
    const detail = await getInterview(turn.interviewId, opts);
    assert.ok(detail.evidence.some((e) => e.type === 'CLIENT_EDITED'));
  });

  it('approves immutably and creates pending_review playbook with provenance', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    const result = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(result.blueprint.status, 'approved');
    assert.ok(result.playbook);
    assert.equal(result.playbook.status, 'pending_review');
    assert.deepEqual(result.playbook.preferredChannels, []);
    assert.deepEqual(result.playbook.offers, []);
    assert.deepEqual(result.playbook.outreachSequence, []);
    assert.ok(result.sectionProvenance);
    for (const [field, sources] of Object.entries(SECTION_PROVENANCE)) {
      if (['preferredChannels', 'outreachSequence', 'offers', 'constraints'].includes(field)) {
        continue;
      }
      assert.ok(sources.length >= 1, `${field} must trace to blueprint sections`);
    }

    const again = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(again.alreadyApproved, true);
    assert.equal(again.blueprint.status, 'approved');
  });

  it('never overwrites an approved blueprint — revise creates new version', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    await approveBlueprint(turn.blueprint.id, opts);
    const next = await reviseBlueprint(
      turn.blueprint.id,
      { services: { summary: 'Only recurring residential cleans' } },
      opts
    );
    assert.notEqual(next.version, '1.0');
    assert.equal(next.status, 'in_review');
    assert.equal(next.sections.services.summary, 'Only recurring residential cleans');
  });
});
