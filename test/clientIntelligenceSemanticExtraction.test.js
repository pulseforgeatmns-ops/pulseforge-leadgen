'use strict';

/**
 * SPEC-101 — CIE Semantic Extraction & Presentation Normalization
 * Preserves SPEC-096–100 behavior while fixing decision-maker/segment collapse
 * and client-facing mechanical typo presentation.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  extractCustomerSegments,
  extractServiceList,
  ingestAnswerIntoNormalizedFacts,
  emptyNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  normalizeMechanicalTypos,
  normalizePresentationProse,
  sanitizeBusinessName,
  isConversationalFiller,
} = require('../services/clientIntelligenceInterview');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const ICP_INPUT =
  'The ideal customer is a property or facilities manager, apartment buildings, etc';
const LATER_TARGET =
  "We serve the Greater Toronto Area and let's target property and facilities managers";
const SERVICES =
  'Standard home cleaning, move in/out cleans, deep cleans, office cleaning, recurring cleans';
const TYPO_DIFF =
  "Excellent quality and wee'll b ee theree on ttime";

describe('SPEC-101 decision maker + segment extraction', () => {
  it('preserves property managers, facility managers, and apartment/multifamily buildings', () => {
    const segments = extractCustomerSegments(ICP_INPUT);
    assert.ok(
      segments.some((s) => /property managers/i.test(s)),
      `missing property managers in ${JSON.stringify(segments)}`
    );
    assert.ok(
      segments.some((s) => /facilit(?:y|ies)\s+managers/i.test(s)),
      `missing facility/facilities managers in ${JSON.stringify(segments)}`
    );
    assert.ok(
      segments.some((s) => /apartment|multifamily/i.test(s)),
      `missing apartment/multifamily segment in ${JSON.stringify(segments)}`
    );
    assert.equal(
      segments.some((s) => isConversationalFiller(s) || /^etc\.?$/i.test(s)),
      false,
      `filler leaked into segments: ${JSON.stringify(segments)}`
    );
  });

  it('does not reduce the answer to apartment buildings alone', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', ICP_INPUT);
    assert.ok((facts.ideal_customers || []).length >= 3);
    assert.equal(
      facts.ideal_customers.length === 1 &&
        /apartment/i.test(facts.ideal_customers[0]),
      false
    );
  });
});

describe('SPEC-101 later clarification precedence', () => {
  it('promotes property and facility managers after targeting clarification', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', ICP_INPUT);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'targetMarkets', LATER_TARGET);

    assert.ok(facts.geography.some((g) => /Greater Toronto/i.test(g)));
    const ideal = facts.ideal_customers || [];
    assert.ok(ideal.some((s) => /property managers/i.test(s)));
    assert.ok(ideal.some((s) => /facilit(?:y|ies)\s+managers/i.test(s)));

    const dmIndex = Math.min(
      ideal.findIndex((s) => /property managers/i.test(s)),
      ideal.findIndex((s) => /facilit(?:y|ies)\s+managers/i.test(s))
    );
    const aptIndex = ideal.findIndex((s) => /apartment|multifamily/i.test(s));
    assert.ok(dmIndex >= 0);
    if (aptIndex >= 0) {
      assert.ok(
        dmIndex < aptIndex,
        `decision makers should precede apartment segment: ${JSON.stringify(ideal)}`
      );
    }

    const brief = buildExecutiveSummary(sectionsFromNormalizedFacts(facts), {
      normalizedFacts: facts,
    });
    const whoYouServe = brief.sections.find((s) => s.id === 'whoYouServe').body;
    assert.match(whoYouServe, /property managers/i);
    assert.match(whoYouServe, /facilit(?:y|ies)\s+managers/i);
    assert.match(whoYouServe, /particularly those responsible for/i);
    assert.equal(/^[^]*ideal customers include apartment/i.test(whoYouServe), false);
    assert.equal(/apartment buildings,\s*etc/i.test(whoYouServe), false);
    assert.equal(/including opportunities associated with/i.test(whoYouServe), false);
  });
});

describe('SPEC-101 conversational filler', () => {
  it('does not treat etc. as durable semantic business data', () => {
    assert.equal(isConversationalFiller('etc'), true);
    assert.equal(isConversationalFiller('etc.'), true);
    assert.equal(isConversationalFiller('and stuff'), true);
    assert.equal(isConversationalFiller('things like that'), true);
    assert.equal(isConversationalFiller('apartment buildings'), false);

    const segments = extractCustomerSegments('apartment buildings, etc.');
    assert.deepEqual(
      segments.filter((s) => /^etc\.?$/i.test(s) || isConversationalFiller(s)),
      []
    );
    assert.ok(segments.some((s) => /apartment/i.test(s)));
  });
});

describe('SPEC-101 raw evidence vs presentation normalization', () => {
  it('keeps typo-filled operator statement intact in provenance', async () => {
    const { store, opts } = withStore();
    const started = await startClientInterview({ clientId: 11, forceNew: true }, opts);

    await postInterviewMessage(
      started.interviewId,
      'AS Cleaning, we are a cleaning company with a mix of residential and commercial clients but prefer commercial.',
      opts
    );
    await postInterviewMessage(started.interviewId, SERVICES, opts);
    await postInterviewMessage(started.interviewId, ICP_INPUT, opts);
    await postInterviewMessage(
      started.interviewId,
      "No restaurants unless it's only front of house cleaning",
      opts
    );
    await postInterviewMessage(started.interviewId, LATER_TARGET, opts);
    const adv = await postInterviewMessage(started.interviewId, TYPO_DIFF, opts);

    assert.ok(adv.interviewId);
    const evidence = await store.listEvidence(started.interviewId);
    const advEvidence = evidence.find(
      (e) =>
        e.category === 'competitiveAdvantages' &&
        String(e.statement || '').includes("wee'll b ee")
    );
    assert.ok(advEvidence, 'raw typo statement must remain in evidence');
    assert.match(advEvidence.statement, /wee'll b ee theree on ttime/i);
  });

  it('removes known mechanical typos from Executive Brief without strengthening claims', () => {
    assert.equal(
      normalizeMechanicalTypos("miix moreee neiighborhood rreevvenue wee'll b ee theree on ttime"),
      "mix more neighborhood revenue we'll be there on time"
    );
    assert.match(
      normalizePresentationProse('warm and neighborhood'),
      /warm and neighborhood-oriented/i
    );

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'identity',
      'AS Cleaning, we are a cleaning company with a miix of residential and commercial clients but prefer commercial.'
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', SERVICES);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', ICP_INPUT);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'targetMarkets', LATER_TARGET);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'competitiveAdvantages', TYPO_DIFF);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'brandVoice', 'warm and neiighborhood');
    facts = ingestAnswerIntoNormalizedFacts(facts, 'campaignGoals', 'generating moreee walkthroughs');
    facts = ingestAnswerIntoNormalizedFacts(facts, 'successMetrics', 'walkthroughs and rreevvenue');

    const brief = buildExecutiveSummary(sectionsFromNormalizedFacts(facts), {
      normalizedFacts: facts,
    });
    const blob = JSON.stringify(brief);
    for (const bad of ['miix', 'moreee', 'neiighborhood', 'rreevvenue', "wee'll b ee", 'ttime']) {
      assert.equal(blob.includes(bad), false, `Brief still contains ${bad}`);
    }
    assert.equal(/industry-leading/i.test(blob), false);
    assert.match(blob, /excellent quality and reliable, on-time service/i);
    assert.match(blob, /neighborhood-oriented/i);
  });
});

describe('SPEC-101 authoritative client identity', () => {
  it('prefers AS Cleaning Co. over As Cleaning in presentation', () => {
    assert.equal(sanitizeBusinessName('As Cleaning'), 'AS Cleaning Co.');
    assert.equal(sanitizeBusinessName('AS Cleaning'), 'AS Cleaning Co.');
    assert.equal(sanitizeBusinessName('AS Cleaning Co.'), 'AS Cleaning Co.');

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'identity',
      'As Cleaning, we are a cleaning company with a mix of residential and commercial clients but prefer commercial.'
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', ICP_INPUT);
    assert.equal(facts.business_name, 'AS Cleaning Co.');

    const brief = buildExecutiveSummary(sectionsFromNormalizedFacts(facts), {
      normalizedFacts: facts,
    });
    const blob = JSON.stringify(brief);
    assert.match(blob, /AS Cleaning Co\./);
    assert.equal(/\bAs'\s+ideal customers/i.test(blob), false);
    assert.equal(/\bAs Cleaning\b/.test(blob), false);
  });
});

describe('SPEC-101 service regression', () => {
  it('preserves five supplied services from SPEC-100 fixture shape', () => {
    const services = extractServiceList(SERVICES);
    assert.ok(services.length >= 5, JSON.stringify(services));
    for (const re of [
      /standard home/i,
      /move-?in\/move-?out/i,
      /deep cleans?/i,
      /office cleaning/i,
      /recurring cleaning/i,
    ]) {
      assert.ok(services.some((s) => re.test(s)), `missing ${re}`);
    }
  });
});

describe('SPEC-101 uncertainty regression', () => {
  it('keeps I don\'t know yet as UNKNOWN and does not promote hedged maybe answers', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', "I don't know yet.");
    assert.deepEqual(facts.ideal_customers, []);

    facts = ingestAnswerIntoNormalizedFacts(
      emptyNormalizedFacts(),
      'idealCustomers',
      'Maybe property managers?'
    );
    assert.deepEqual(facts.ideal_customers, []);
  });
});

describe('SPEC-101 tenant isolation', () => {
  it('AS Cleaning normalization never consumes Anchor identity or evidence', async () => {
    const { store, opts } = withStore();
    const asStarted = await startClientInterview({ clientId: 11, forceNew: true }, opts);
    const anchorStarted = await startClientInterview({ clientId: 10, forceNew: true }, opts);

    await postInterviewMessage(
      asStarted.interviewId,
      'AS Cleaning, we are a cleaning company with a mix of residential and commercial clients but prefer commercial.',
      opts
    );
    await postInterviewMessage(
      anchorStarted.interviewId,
      'Anchor Cleaning—we are a commercial-focused cleaning company serving Greater Manchester',
      opts
    );

    const asSession = await store.getSession(asStarted.interviewId);
    const anchorSession = await store.getSession(anchorStarted.interviewId);

    assert.equal(asSession.client_id, 11);
    assert.equal(anchorSession.client_id, 10);
    assert.equal(asSession.interview_state.normalizedFacts.business_name, 'AS Cleaning Co.');
    assert.equal(anchorSession.interview_state.normalizedFacts.business_name, 'Anchor Cleaning');
    assert.equal(
      /anchor/i.test(String(asSession.interview_state.normalizedFacts.business_name || '')),
      false
    );

    const asEvidence = await store.listEvidence(asStarted.interviewId);
    const anchorEvidence = await store.listEvidence(anchorStarted.interviewId);
    assert.ok(asEvidence.every((e) => Number(e.client_id) === 11));
    assert.ok(anchorEvidence.every((e) => Number(e.client_id) === 10));
    assert.equal(
      asEvidence.some((e) => /Anchor Cleaning/i.test(String(e.statement || ''))),
      false
    );
  });
});
