'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  EPISTEMIC_STATES,
  classifyEpistemicState,
  createBusinessFact,
  preserveEpistemicState,
  extractBusinessFacts,
} = require('../services/clientIntelligenceEpistemic');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  getApprovedClientBlueprint,
  emptyNormalizedFacts,
  ingestAnswerIntoNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  composeAssessment,
} = require('../services/clientIntelligenceInterview');
const {
  assessAnswerSufficiency,
  classifyAnswerDisposition,
} = require('../services/clientIntelligenceReasoning');
const { normalizeBlueprintSummary } = require('../packages/max/workspace/ClientIntelligenceContext');

const MIXED_BUYING_REASON =
  "We don't know what actually tips the decision. There is insufficient U.S. market evidence to establish the buying reason. Our current hypothesis is that the practical transformation model may be important. That hypothesis remains unvalidated. Initial sales conversations are intended to discover buying reasons.";

describe('SPEC-221 — Durable Epistemic State for Business Understanding', () => {

  describe('1. Semantic Epistemic State Classification', () => {
    it('classifies KNOWN facts', () => {
      assert.strictEqual(
        classifyEpistemicState('Our primary market is the United States.'),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState('Our customers choose us because implementation takes one day.'),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState('We provide commercial cleaning for medical offices in Manchester.'),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We don't want to work with businesses that don't have a specific relevant pain."),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We don't want to work with solopreneurs."),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We generally avoid businesses without a specific relevant pain."),
        EPISTEMIC_STATES.KNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We're less focused on larger businesses with mature management infrastructure."),
        EPISTEMIC_STATES.KNOWN
      );
    });

    it('distinguishes uncertainty from negative business preference', () => {
      assert.strictEqual(
        classifyEpistemicState('We do not know which customers to avoid.'),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We think solopreneurs are probably a poor fit, but we haven't validated that yet."),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('We do not have employees.', { questionContext: 'employee_management' }),
        EPISTEMIC_STATES.NOT_APPLICABLE
      );
      assert.strictEqual(
        classifyEpistemicState('We do not have employees.', { questionContext: 'business_profile' }),
        EPISTEMIC_STATES.KNOWN
      );
    });

    it('classifies HYPOTHESIS statements across natural language variations', () => {
      assert.strictEqual(
        classifyEpistemicState('We think service businesses will respond best.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('Our working assumption is X.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('X seems likely, but we haven\'t validated it.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('That\'s only a theory right now.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('My guess would be property managers.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
      assert.strictEqual(
        classifyEpistemicState('We believe commercial clients are best, but don\'t have evidence yet.'),
        EPISTEMIC_STATES.HYPOTHESIS
      );
    });

    it('classifies UNKNOWN statements across natural language variations', () => {
      assert.strictEqual(
        classifyEpistemicState("We haven't defined a formal brand voice yet."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("I don't know."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We haven't defined that yet."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We've never established that."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We're still figuring that out."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("There isn't enough evidence to answer."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("I couldn't tell you yet."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We haven't tested that."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We haven't investigated why customers choose us."),
        EPISTEMIC_STATES.UNKNOWN
      );
      assert.strictEqual(
        classifyEpistemicState("We don't know why customers choose us yet."),
        EPISTEMIC_STATES.UNKNOWN
      );
    });

    it('classifies NOT_APPLICABLE statements in the correct employee-management context', () => {
      assert.strictEqual(
        classifyEpistemicState("We don't have employees and don't plan to hire any.", { questionContext: 'employee_management' }),
        EPISTEMIC_STATES.NOT_APPLICABLE
      );
      assert.strictEqual(
        classifyEpistemicState("That's not applicable to our business.", { questionContext: 'employee_management' }),
        EPISTEMIC_STATES.NOT_APPLICABLE
      );
      assert.strictEqual(
        classifyEpistemicState("We don't sell to consumers.", { questionContext: 'sales_channels' }),
        EPISTEMIC_STATES.KNOWN
      );
    });

    it('classifies empty or filler input as UNRESOLVED', () => {
      assert.strictEqual(classifyEpistemicState(''), EPISTEMIC_STATES.UNRESOLVED);
      assert.strictEqual(classifyEpistemicState(null), EPISTEMIC_STATES.UNRESOLVED);
    });
  });

  describe('2. Canonical BusinessFact Invariant & Persistence', () => {
    it('creates BusinessFact separating value from epistemic state', () => {
      const unknownFact = createBusinessFact({
        subject: 'brand_voice',
        value: "We haven't defined a formal brand voice yet.",
        epistemicState: EPISTEMIC_STATES.UNKNOWN,
        confidence: 0.98,
        evidence: "We haven't defined a formal brand voice yet.",
        provenance: 'turn_1',
      });
      assert.strictEqual(unknownFact.subject, 'brand_voice');
      assert.strictEqual(unknownFact.epistemic_state, EPISTEMIC_STATES.UNKNOWN);
      assert.strictEqual(unknownFact.value, null);
      assert.strictEqual(unknownFact.evidence, "We haven't defined a formal brand voice yet.");

      const knownFact = createBusinessFact({
        subject: 'geography',
        value: 'Greater Manchester',
        epistemicState: EPISTEMIC_STATES.KNOWN,
        confidence: 0.95,
        evidence: 'We operate in Greater Manchester',
        provenance: 'turn_2',
      });
      assert.strictEqual(knownFact.epistemic_state, EPISTEMIC_STATES.KNOWN);
      assert.strictEqual(knownFact.value, 'Greater Manchester');
    });

    it('enforces persistence invariant preventing silent promotion of UNKNOWN to KNOWN', () => {
      const initial = createBusinessFact({
        subject: 'brand_voice',
        epistemicState: EPISTEMIC_STATES.UNKNOWN,
        evidence: "We haven't defined a formal brand voice yet.",
      });

      const unconfirmedAttempt = {
        subject: 'brand_voice',
        value: "We haven't defined a formal brand voice yet.",
        epistemic_state: EPISTEMIC_STATES.KNOWN,
        explicitOperatorConfirmation: false,
      };

      const preserved = preserveEpistemicState(initial, unconfirmedAttempt);
      assert.strictEqual(preserved.epistemic_state, EPISTEMIC_STATES.UNKNOWN);
      assert.strictEqual(preserved.value, null);
    });
  });

  describe('3. Babrun Production Regression Case', () => {
    it('does not populate brand_voice with uncertainty statement in normalizedFacts', () => {
      const facts = emptyNormalizedFacts();
      const updated = ingestAnswerIntoNormalizedFacts(
        facts,
        'brandVoice',
        "We haven't defined a formal brand voice yet."
      );

      assert.strictEqual(updated.brand_voice, null);
      assert.strictEqual(updated.epistemic_states.brand_voice, EPISTEMIC_STATES.UNKNOWN);
      assert.strictEqual(
        updated.evidence_statements.brand_voice,
        "We haven't defined a formal brand voice yet."
      );
    });

    it('classifies the Babrun production exclusion answer as known business preference and advances the question', () => {
      const babrunAnswer =
        'We would generally rather not take on pre-business founders or pure solopreneurs with no employees, because they have not reached the stage where the people-management problems we address exist.';

      const classification = classifyEpistemicState(babrunAnswer);
      assert.strictEqual(classification, EPISTEMIC_STATES.KNOWN);
      assert.strictEqual(classification === EPISTEMIC_STATES.UNKNOWN, false);
      assert.strictEqual(classification === EPISTEMIC_STATES.NOT_APPLICABLE, false);

      const sufficiency = assessAnswerSufficiency(babrunAnswer, { section: 'avoidCustomers' }, { hasSpecificity: true });
      assert.strictEqual(sufficiency.sufficient, true);
      assert.strictEqual(sufficiency.reason, null);

      const disposition = classifyAnswerDisposition(babrunAnswer, { section: 'avoidCustomers' }, { hasSpecificity: true });
      assert.strictEqual(disposition.shouldAdvance, true);
      assert.strictEqual(disposition.disposition, 'ANSWER_ACCEPTED');

      const facts = emptyNormalizedFacts();
      const updated = ingestAnswerIntoNormalizedFacts(facts, 'avoidCustomers', babrunAnswer);
      assert.ok((updated.disqualified_customers || []).some((item) => /pre-business founders/i.test(item)));
      assert.ok((updated.disqualified_customers || []).some((item) => /solopreneurs/i.test(item)));
      assert.strictEqual(updated.epistemic_states.disqualified_customers, EPISTEMIC_STATES.KNOWN);
    });

    it('does not render affirmative brand voice prose in Executive Business Brief', () => {
      let facts = emptyNormalizedFacts();
      facts = ingestAnswerIntoNormalizedFacts(
        facts,
        'identity',
        'Babrun Co is a business consulting practice.'
      );
      facts = ingestAnswerIntoNormalizedFacts(
        facts,
        'brandVoice',
        "We haven't defined a formal brand voice yet."
      );

      const brief = buildExecutiveSummary(null, { normalizedFacts: facts });
      const briefString = JSON.stringify(brief);

      assert.strictEqual(
        briefString.includes("sounding We haven't defined a formal brand voice yet"),
        false
      );
      assert.strictEqual(
        briefString.includes("brand voice reinforces its positioning by sounding"),
        false
      );

      const brandVoiceSection = sectionsFromNormalizedFacts(facts).brandVoice;
      assert.strictEqual(brandVoiceSection.epistemic_state, EPISTEMIC_STATES.UNKNOWN);
      assert.strictEqual(brandVoiceSection.summary, 'Brand voice: Not yet defined.');
    });

    it('accepts the mixed Babrun differentiation answer once and advances', async () => {
      const disposition = classifyAnswerDisposition(
        MIXED_BUYING_REASON,
        { section: 'competitiveAdvantages' },
        { hasSpecificity: true }
      );
      assert.strictEqual(disposition.disposition, 'ANSWER_ACCEPTED');
      assert.strictEqual(disposition.shouldAdvance, true);

      const store = createMemoryStore();
      const opts = { store, useMemoryPlaybookStore: true };
      const started = await startClientInterview({ clientId: 22102 }, opts);
      const priorAnswers = [
        'Babrun is a consulting practice.',
        'Twelve-week one-to-one founder coaching.',
        'Owners of founder-led U.S. service businesses with 1-10 employees.',
        'Avoid pre-business founders and solopreneurs with no employees.',
        'The United States, initially across service industries.',
      ];
      for (const answer of priorAnswers) {
        await postInterviewMessage(started.interviewId, answer, opts);
      }

      const response = await postInterviewMessage(started.interviewId, MIXED_BUYING_REASON, opts);
      assert.strictEqual(response.answerDisposition, 'ANSWER_ACCEPTED');
      assert.strictEqual(response.nextAction, 'ASK');
      assert.notStrictEqual(response.question.id, 'competitive_advantages');

      const session = await store.getSession(started.interviewId);
      const differentiation = session.interview_state.normalizedFacts.business_facts.differentiation;
      assert.ok(differentiation.some((fact) => fact.epistemic_state === EPISTEMIC_STATES.UNKNOWN));
      assert.ok(differentiation.some((fact) => fact.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS));
    });
  });

  describe('4. Assessment & Confidence Invariant', () => {
    it('gives 1 star for UNKNOWN differentiation without positive evidence boost', () => {
      let facts = emptyNormalizedFacts();
      facts = ingestAnswerIntoNormalizedFacts(
        facts,
        'identity',
        'Babrun Co is a consulting firm.'
      );
      facts = ingestAnswerIntoNormalizedFacts(
        facts,
        'competitiveAdvantages',
        "We haven't investigated why customers choose us."
      );

      const sections = sectionsFromNormalizedFacts(facts);
      const assessment = composeAssessment(sections, { normalizedFacts: facts });

      const diffRating = assessment.ratings.find((r) => r.label === 'Differentiation');
      assert.ok(diffRating);
      assert.strictEqual(diffRating.stars, 1);
      assert.ok(diffExplanationIsUncertain(diffRating.explanation));
    });

    it('caps stars at 2 for HYPOTHESIS differentiation with clear explanation', () => {
      let facts = emptyNormalizedFacts();
      facts = ingestAnswerIntoNormalizedFacts(
        facts,
        'competitiveAdvantages',
        'We think customers choose us because implementation is fast.'
      );

      const sections = sectionsFromNormalizedFacts(facts);
      const assessment = composeAssessment(sections, { normalizedFacts: facts });

      const diffRating = assessment.ratings.find((r) => r.label === 'Differentiation');
      assert.ok(diffRating);
      assert.strictEqual(diffRating.stars, 2);
      assert.ok(diffRating.explanation.includes('hypothesis'));
    });
  });

  describe('5. Proposition-Level Durable Facts', () => {
    it('extracts UNKNOWN, KNOWN, and HYPOTHESIS propositions without collapsing a mixed answer', () => {
      const facts = extractBusinessFacts(MIXED_BUYING_REASON, {
        section: 'competitiveAdvantages',
        provenance: 'turn_123',
      });
      assert.ok(facts.some((fact) => fact.subject === 'customer_buying_reason' && fact.epistemic_state === EPISTEMIC_STATES.UNKNOWN));
      assert.ok(facts.some((fact) => fact.subject === 'customer_buying_reason_evidence_state' && fact.epistemic_state === EPISTEMIC_STATES.KNOWN));
      assert.ok(facts.some((fact) => fact.subject === 'candidate_customer_buying_reason' && fact.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS));
      assert.ok(facts.every((fact) => fact.provenance === 'turn_123'));
    });

    it('retains known evidence alongside a hypothesis when no unknown is present', () => {
      const facts = extractBusinessFacts(
        'There is sufficient market evidence that customers value speed. Our hypothesis is that practical transformation also matters.',
        { section: 'competitiveAdvantages' }
      );
      assert.ok(facts.some((fact) => fact.epistemic_state === EPISTEMIC_STATES.KNOWN));
      assert.ok(facts.some((fact) => fact.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS));
    });

    it('preserves multiple known facts, known constraints, and true non-applicability independently', () => {
      const known = extractBusinessFacts('We avoid price-only work and exclude businesses without a clear need.', { section: 'avoidCustomers' });
      assert.equal(known.length, 1);
      assert.equal(known[0].epistemic_state, EPISTEMIC_STATES.KNOWN);
      const notApplicable = extractBusinessFacts("We don't have employees.", { questionContext: 'employee_management' });
      assert.equal(notApplicable[0].epistemic_state, EPISTEMIC_STATES.NOT_APPLICABLE);
    });

    it('updates one field collection without mutating unrelated proposition facts', () => {
      let facts = emptyNormalizedFacts();
      facts = ingestAnswerIntoNormalizedFacts(facts, 'competitiveAdvantages', MIXED_BUYING_REASON, { provenance: 'turn_123' });
      const before = JSON.stringify(facts.business_facts.differentiation);
      facts = ingestAnswerIntoNormalizedFacts(facts, 'brandVoice', 'Professional and direct.', { provenance: 'turn_124' });
      assert.equal(JSON.stringify(facts.business_facts.differentiation), before);
      assert.equal(facts.business_facts.brand_voice[0].epistemic_state, EPISTEMIC_STATES.KNOWN);
    });

    it('adds later evidence without mutating the earlier unknown or hypothesis', () => {
      let facts = ingestAnswerIntoNormalizedFacts(emptyNormalizedFacts(), 'competitiveAdvantages', MIXED_BUYING_REASON, { provenance: 'turn_123' });
      const original = facts.business_facts.differentiation.map((fact) => ({ ...fact }));
      facts = ingestAnswerIntoNormalizedFacts(facts, 'competitiveAdvantages', 'Customers now report that practical transformation tips the decision.', { provenance: 'turn_124' });
      assert.ok(facts.business_facts.differentiation.length > original.length);
      assert.deepEqual(facts.business_facts.differentiation.slice(0, original.length), original);
    });

    it('renders a mixed differentiation state without promoting its hypothesis', () => {
      const facts = ingestAnswerIntoNormalizedFacts(emptyNormalizedFacts(), 'competitiveAdvantages', MIXED_BUYING_REASON, { provenance: 'turn_123' });
      const section = sectionsFromNormalizedFacts(facts).competitiveAdvantages;
      assert.match(section.summary, /Actual customer reason-to-choose: Not yet established/i);
      assert.match(section.summary, /Current hypothesis:.*practical transformation/i);
      const brief = buildExecutiveSummary(null, { normalizedFacts: facts });
      const whyChoose = brief.sections.find((sectionItem) => sectionItem.id === 'whyChooseYou');
      assert.match(whyChoose.body, /Actual customer reason-to-choose: Not yet established/i);
      assert.match(whyChoose.body, /Current hypothesis:.*practical transformation/i);
    });

    it('persists proposition facts with the Blueprint and exposes them to specialist context', async () => {
      const store = createMemoryStore();
      const opts = { store, useMemoryPlaybookStore: true };
      const started = await startClientInterview({ clientId: 22101 }, opts);
      const answers = [
        'Babrun is a consulting practice.', 'Transformation advisory.', 'Growing U.S. businesses.',
        'Avoid price-only work.', 'United States.', MIXED_BUYING_REASON,
        'Professional and direct.', 'Learn buying reasons.', 'Qualified conversations.',
      ];
      for (const answer of answers) await postInterviewMessage(started.interviewId, answer, opts);
      const drafts = await store.listBlueprintsForClient(22101, { status: 'in_review' });
      assert.equal(drafts.length, 1);
      const blueprint = drafts[0];
      await store.updateBlueprint(blueprint.id, blueprint.version, { status: 'approved' });
      const loaded = await getApprovedClientBlueprint(22101, opts);
      assert.ok(loaded.epistemicFacts.differentiation.length >= 3);
      assert.ok(loaded.normalizedFacts.business_facts.differentiation.length >= 3);
      assert.ok(normalizeBlueprintSummary(loaded).businessFacts.differentiation.length >= 3);
    });

    it('keeps legacy sessions readable when proposition facts are absent', () => {
      const legacy = normalizeBlueprintSummary({
        id: 'legacy', status: 'approved', sections: { competitiveAdvantages: { summary: 'Competitive edge is described as reliable service.' } },
      });
      assert.equal(legacy.competitiveAdvantages, 'reliable service');
      assert.deepEqual(legacy.businessFacts, {});
    });
  });
});

function diffExplanationIsUncertain(exp) {
  return /not yet defined|open area|not yet evidenced/i.test(exp);
}
