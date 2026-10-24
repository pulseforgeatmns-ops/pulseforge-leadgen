'use strict';

/**
 * CIE Executive Brief semantic fidelity patch (post–SPEC-101).
 * Presentation normalization only — raw evidence/provenance unchanged.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  ingestAnswerIntoNormalizedFacts,
  emptyNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  normalizeMechanicalTypos,
  normalizePresentationProse,
  normalizeGoalOutcomePhrase,
  composeCustomerConstraintPresentation,
} = require('../services/clientIntelligenceInterview');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

function briefFromAnswers(answers) {
  let facts = emptyNormalizedFacts();
  for (const [section, text] of Object.entries(answers)) {
    facts = ingestAnswerIntoNormalizedFacts(facts, section, text);
  }
  const sections = sectionsFromNormalizedFacts(facts);
  const brief = buildExecutiveSummary(sections, {
    normalizedFacts: facts,
    businessName: facts.business_name || 'AS Cleaning Co.',
  });
  const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));
  return { facts, sections, brief, byId };
}

describe('CIE Executive Brief fidelity — TEST A goal grammar', () => {
  it('normalizes "we will establish…" without "priority is we will"', () => {
    assert.match(
      normalizeGoalOutcomePhrase(
        'we will establish a reliable pipeline turning prospects into clients'
      ),
      /^establishing a reliable pipeline that turns prospects into clients$/i
    );

    const { byId } = briefFromAnswers({
      identity: 'AS Cleaning Co., we are a cleaning company',
      campaignGoals:
        'we will establish a reliable pipeline turning prospects into clients',
    });
    assert.equal(/priority is we will/i.test(byId.whereHeaded.body), false);
    assert.match(byId.whereHeaded.body, /near-term priority is establishing/i);
    assert.match(
      byId.whereHeaded.body,
      /reliable pipeline that turns prospects into clients/i
    );
  });
});

describe('CIE Executive Brief fidelity — TEST B metric typo', () => {
  it('removes createed from Brief while preserving raw evidence', async () => {
    assert.equal(
      normalizeMechanicalTypos('recurring revenue createed'),
      'recurring revenue created'
    );
    assert.match(
      normalizePresentationProse('recurring revenue created'),
      /new recurring revenue/i
    );

    const { store, opts } = withStore();
    const started = await startClientInterview({ clientId: 11, forceNew: true }, opts);
    await postInterviewMessage(
      started.interviewId,
      'AS Cleaning Co., we are a cleaning company with commercial preference.',
      opts
    );
    // Advance through interview by answering remaining required sections lightly,
    // then land the metrics answer with the typo for provenance check.
    const session = await store.getSession(started.interviewId);
    assert.ok(session);

    const { facts, byId } = briefFromAnswers({
      identity: 'AS Cleaning Co., we are a cleaning company',
      successMetrics: 'walkthroughs completed, recurring revenue createed',
    });

    assert.equal(/createed/i.test(JSON.stringify(byId.recommendedScorecard)), false);
    assert.match(JSON.stringify(byId.recommendedScorecard), /walkthroughs completed/i);
    assert.match(JSON.stringify(byId.recommendedScorecard), /new recurring revenue|recurring revenue/i);

    // Structured list may correct the mechanical typo; raw operator text stays separate.
    assert.equal(
      (facts.success_metrics || []).some((m) => /createed/i.test(m)),
      false
    );
  });

  it('keeps the original operator metrics statement in evidence provenance', async () => {
    const { store, opts } = withStore();
    const started = await startClientInterview({ clientId: 12, forceNew: true }, opts);
    const answers = [
      'AS Cleaning Co., we are a cleaning company preferring commercial work.',
      'Standard home cleaning, move in/out cleans, deep cleans, office cleaning, recurring cleans',
      'property managers and facility managers, apartment buildings',
      'restaurants because back of house is a nightmare. would consider front of house but not preferred at this time',
      'Greater Toronto Area',
      'Excellent quality and on time service',
      'warm and neighborhood',
      'we will establish a reliable pipeline turning prospects into clients',
      'walkthroughs completed, recurring revenue createed',
    ];
    let last = null;
    for (const msg of answers) {
      last = await postInterviewMessage(started.interviewId, msg, opts);
    }
    assert.ok(last);
    const evidence = await store.listEvidence(started.interviewId);
    const metricsEvidence = evidence.find(
      (e) =>
        e.category === 'successMetrics' &&
        /createed/i.test(String(e.statement || ''))
    );
    assert.ok(metricsEvidence, 'raw createed statement must remain in evidence');
    assert.match(metricsEvidence.statement, /createed/i);

    if (last.executiveSummary) {
      const blob = JSON.stringify(last.executiveSummary);
      assert.equal(/createed/i.test(blob), false);
    }
  });
});

describe('CIE Executive Brief fidelity — TEST C conditional restaurant preference', () => {
  it('does not claim categorical restaurant exclusion', () => {
    const prose = composeCustomerConstraintPresentation(
      'AS Cleaning Co.',
      'restaurants because back of house is a nightmare. would consider front of house but not preferred at this time'
    );
    assert.equal(/deliberately avoids restaurants/i.test(prose), false);
    assert.equal(/\bwill not serve restaurants\b/i.test(prose), false);
    assert.match(prose, /does not currently prioritize restaurant/i);
    assert.match(prose, /front-of-house opportunities may be considered/i);

    const { byId } = briefFromAnswers({
      identity: 'AS Cleaning Co., we are a cleaning company',
      idealCustomers: 'property managers and facility managers, apartment buildings',
      avoidCustomers:
        'restaurants because back of house is a nightmare. would consider front of house but not preferred at this time',
    });
    assert.equal(/deliberately avoids restaurants/i.test(byId.whoYouServe.body), false);
    assert.match(byId.whoYouServe.body, /not currently prioritize restaurant/i);
    assert.match(byId.whoYouServe.body, /front-of-house/i);
  });

  it('still treats lowest-price disqualification as deliberate avoidance', () => {
    const prose = composeCustomerConstraintPresentation(
      'Anchor Cleaning',
      'customers whose main priority is the lowest price'
    );
    assert.match(prose, /deliberately avoids customers who prioritize the lowest price/i);
  });
});

describe('CIE Executive Brief fidelity — TEST D ICP prose', () => {
  it('distinguishes decision-maker roles from apartment/multifamily segment', () => {
    const { facts, byId } = briefFromAnswers({
      identity: 'AS Cleaning Co., we are a cleaning company',
      idealCustomers: 'facility managers and property managers, apartment buildings',
    });
    assert.ok(facts.ideal_customers.some((c) => /property managers/i.test(c)));
    assert.ok(facts.ideal_customers.some((c) => /facility managers/i.test(c)));
    assert.ok(facts.ideal_customers.some((c) => /apartment|multifamily/i.test(c)));

    assert.match(
      byId.whoYouServe.body,
      /property managers and facility managers/i
    );
    assert.match(
      byId.whoYouServe.body,
      /particularly those responsible for apartment and multifamily buildings/i
    );
    assert.equal(
      /including opportunities associated with/i.test(byId.whoYouServe.body),
      false
    );
  });
});

describe('CIE Executive Brief fidelity — TEST E provenance invariant', () => {
  it('does not rewrite original operator statements when normalizing Brief prose', async () => {
    const { store, opts } = withStore();
    const started = await startClientInterview({ clientId: 13, forceNew: true }, opts);
    const goal =
      'we will establish a reliable pipeline turning prospects into clients';
    const avoid =
      'restaurants because back of house is a nightmare. would consider front of house but not preferred at this time';
    const metrics = 'walkthroughs completed, recurring revenue createed';

    const sequence = [
      'AS Cleaning Co., we are a cleaning company preferring commercial.',
      'Standard home cleaning, move in/out cleans, deep cleans, office cleaning, recurring cleans',
      'facility managers and property managers, apartment buildings',
      avoid,
      'Greater Toronto Area',
      'Excellent quality and reliable crews',
      'warm and neighborhood',
      goal,
      metrics,
    ];
    let last = null;
    for (const msg of sequence) {
      last = await postInterviewMessage(started.interviewId, msg, opts);
    }
    const evidence = await store.listEvidence(started.interviewId);
    const statements = evidence.map((e) => String(e.statement || ''));
    assert.ok(statements.some((s) => s.includes(goal)));
    assert.ok(statements.some((s) => s.includes(avoid)));
    assert.ok(statements.some((s) => s.includes(metrics)));

    if (last && last.executiveSummary) {
      const blob = JSON.stringify(last.executiveSummary);
      assert.equal(/priority is we will/i.test(blob), false);
      assert.equal(/createed/i.test(blob), false);
      assert.equal(/deliberately avoids restaurants/i.test(blob), false);
    }
  });
});
