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
  buildUnderstandingProgress,
  buildExecutiveSummary,
  buildReflection,
  hasSpecificitySignals,
  looksAmbiguous,
  assertTransition,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
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
    assert.deepEqual(ALLOWED_TRANSITIONS.CLIENT_REVIEW, ['APPROVED', 'DISCOVERY']);
    assert.throws(() => assertTransition('NEW', 'APPROVED'), ClientIntelligenceError);
    assert.throws(() => assertTransition('DISCOVERY', 'APPROVED'), ClientIntelligenceError);
    assert.equal(SESSION_STATUSES.length, 7);
  });

  it('starts interactive interview with first question and understanding progress', async () => {
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
    assert.ok(started.understanding);
    assert.equal(started.understanding.sections.length, BLUEPRINT_SECTIONS.length);
    for (const row of started.understanding.sections) {
      assert.equal('summary' in row, false);
      assert.ok(row.title);
      assert.ok(row.status);
    }
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
    assert.ok(turn.executiveSummary);
    assert.equal(turn.executiveSummary.title, 'My Understanding of Your Business');
    assert.equal(turn.executiveSummary.subtitle, 'Generated from our conversation');
    assert.equal(turn.executiveSummary.sections.length, 6);
    for (const key of BLUEPRINT_SECTIONS) {
      assert.ok(turn.blueprint.sections[key]);
      assert.ok(Array.isArray(turn.blueprint.sections[key].evidenceIds));
      assert.ok(Array.isArray(turn.blueprint.sections[key].unknowns));
      assert.ok('confidence' in turn.blueprint.sections[key]);
      assert.ok(turn.blueprint.sections[key].summary);
      // Consultant notes — not raw transcript mirroring
      assert.notEqual(turn.blueprint.sections[key].summary, AJI_ANSWERS[BLUEPRINT_SECTIONS.indexOf(key)]);
    }
    assert.ok(turn.understanding);
    for (const row of turn.understanding.sections) {
      assert.equal('summary' in row, false);
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
    assert.match(
      turn.reflection,
      /here's what i'm hearing so far|let me make sure i understand|taking away so far/i
    );
    assert.equal(turn.question.id, 'avoid_customers');
    assert.equal(QUESTION_BANK.length, 9);

    const detail = await getInterview(started.interviewId, opts);
    const reflections = detail.turns.filter(
      (t) =>
        t.speaker === 'assistant' &&
        /here's what i'm hearing so far|let me make sure i understand|taking away so far/i.test(
          t.message
        )
    );
    assert.equal(reflections.length, 1);
    assert.equal(turn.progress.completed, 3);
  });

  it('resumes CLIENT_REVIEW into discovery for refinement', async () => {
    const { opts } = withStore();
    const { turn } = await completeInterview(opts);
    assert.equal(turn.status, 'CLIENT_REVIEW');
    await assert.rejects(
      () => postInterviewMessage(turn.interviewId, 'More detail', opts),
      (err) => err instanceof ClientIntelligenceError && err.code === 'awaiting_review'
    );

    const resumed = await resumeInterview(turn.interviewId, opts);
    assert.equal(resumed.status, 'DISCOVERY');
    assert.equal(resumed.resumed, true);
    assert.match(resumed.message, /refine/i);

    const refined = await postInterviewMessage(
      turn.interviewId,
      'Our ideal customers are boutique hotel owners along the coast.',
      opts
    );
    assert.equal(refined.status, 'CLIENT_REVIEW');
    assert.ok(refined.blueprint);
    assert.ok(refined.executiveSummary);
    assert.match(
      refined.blueprint.sections.idealCustomers.summary,
      /boutique hotel|ideal customers/i
    );
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
    assert.match(text, /here's what i'm hearing so far/i);
    assert.match(text, /Identity:/);
    assert.match(text, /Services:/);
  });

  it('buildUnderstandingProgress never includes summaries', () => {
    const progress = buildUnderstandingProgress({
      identity: {
        summary: 'Secret narrative that clients must not see yet.',
        confidence: 0.9,
        evidenceIds: ['e1'],
        unknowns: [],
      },
      services: {
        summary: '',
        confidence: 0.2,
        evidenceIds: [],
        unknowns: ['Missing clear answer for services'],
      },
    });
    assert.equal(progress.sections.length, BLUEPRINT_SECTIONS.length);
    const identity = progress.sections.find((s) => s.key === 'identity');
    assert.equal(identity.status, 'ready');
    assert.equal('summary' in identity, false);
    assert.ok(!JSON.stringify(progress).includes('Secret narrative'));
  });

  it('buildExecutiveSummary maps blueprint sections into read-only narratives', () => {
    const summary = buildExecutiveSummary({
      identity: { summary: 'Aji is a cleaning company.', confidence: 0.9, unknowns: [] },
      services: { summary: 'Recurring cleans.', confidence: 0.8, unknowns: [] },
      idealCustomers: { summary: 'Homeowners.', confidence: 0.8, unknowns: [] },
      avoidCustomers: { summary: 'Bargain hunters.', confidence: 0.7, unknowns: [] },
      targetMarkets: { summary: 'Coastal SC.', confidence: 0.7, unknowns: [] },
      competitiveAdvantages: { summary: 'Reliable crews.', confidence: 0.8, unknowns: [] },
      brandVoice: { summary: 'Friendly professional.', confidence: 0.7, unknowns: [] },
      campaignGoals: { summary: 'Book appointments.', confidence: 0.8, unknowns: [] },
      successMetrics: {
        summary: 'Close rate.',
        confidence: 0.6,
        unknowns: ['Pricing philosophy'],
      },
    });
    assert.equal(summary.title, 'My Understanding of Your Business');
    assert.equal(summary.sections.length, 6);
    assert.match(summary.sections[0].body, /Aji/);
    assert.match(summary.sections[5].body, /Pricing philosophy/);
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
