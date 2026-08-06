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
    assert.match(started.message, /business name/i);
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
    }
    assert.equal(QUESTION_BANK.length, 9);
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
    assert.equal(short, long);
  });

  it('increases on confirmation and consistency, decreases on contradiction', () => {
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

  it('treats empty answers as unknowns without inventing facts', () => {
    assert.equal(answerLooksEmpty('n/a'), true);
    assert.equal(answerLooksEmpty('Property managers'), false);
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
