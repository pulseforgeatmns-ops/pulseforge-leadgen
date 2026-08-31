'use strict';

const assert = require('assert');
const {
  EPISTEMIC_STATES,
  classifyEpistemicState,
  createBusinessFact,
  preserveEpistemicState,
} = require('../services/clientIntelligenceEpistemic');
const {
  emptyNormalizedFacts,
  ingestAnswerIntoNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  composeAssessment,
} = require('../services/clientIntelligenceInterview');

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

    it('classifies NOT_APPLICABLE statements', () => {
      assert.strictEqual(
        classifyEpistemicState("We don't have employees and don't plan to hire any."),
        EPISTEMIC_STATES.NOT_APPLICABLE
      );
      assert.strictEqual(
        classifyEpistemicState("That's not applicable to our business."),
        EPISTEMIC_STATES.NOT_APPLICABLE
      );
      assert.strictEqual(
        classifyEpistemicState("We don't sell to consumers."),
        EPISTEMIC_STATES.NOT_APPLICABLE
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
});

function diffExplanationIsUncertain(exp) {
  return /not yet defined|open area|not yet evidenced/i.test(exp);
}
