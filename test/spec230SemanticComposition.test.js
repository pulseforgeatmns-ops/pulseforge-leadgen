'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
  sectionsFromNormalizedFacts,
} = require('../services/clientIntelligenceInterview');

const OLD_VALUE = 'premium positioning';
const HYPOTHESIS = 'the practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended advice because it changes how the owner actually operates';
const KNOWN = 'practical transformation-focused implementation';

function facts(differentiation = OLD_VALUE) {
  return {
    business_name: 'Babrun',
    differentiation,
    epistemic_states: { differentiation: 'KNOWN' },
    hypotheses: {},
    evidence_statements: { differentiation },
    superseded_slots: [],
  };
}

function operation(operation, value, epistemic_state = 'KNOWN') {
  return { operation, slot: 'differentiation', value, epistemic_state };
}

function replacement(value, epistemic_state) {
  return operation('CORRECT', value, epistemic_state);
}

describe('SPEC-230 coherent same-slot semantic operation composition', () => {
  it('keeps a replacement hypothesis when correction precedes stale-value retraction', () => {
    const active = projectWorkingSemanticOperations(facts(), [
      replacement(HYPOTHESIS, 'HYPOTHESIS'),
      operation('RETRACT', OLD_VALUE),
    ]);
    assert.equal(active.differentiation, HYPOTHESIS);
    assert.equal(active.epistemic_states.differentiation, 'HYPOTHESIS');
    assert.equal(active.hypotheses.differentiation, HYPOTHESIS);
  });

  it('is invariant when stale-value retraction precedes replacement correction', () => {
    const active = projectWorkingSemanticOperations(facts(), [
      operation('RETRACT', OLD_VALUE),
      replacement(HYPOTHESIS, 'HYPOTHESIS'),
    ]);
    assert.equal(active.differentiation, HYPOTHESIS);
    assert.equal(active.epistemic_states.differentiation, 'HYPOTHESIS');
    assert.equal(active.hypotheses.differentiation, HYPOTHESIS);
  });

  it('keeps a known replacement proposition', () => {
    const active = projectWorkingSemanticOperations(facts(), [
      replacement(KNOWN, 'KNOWN'),
      operation('RETRACT', OLD_VALUE),
    ]);
    assert.equal(active.differentiation, KNOWN);
    assert.equal(active.epistemic_states.differentiation, 'KNOWN');
    assert.equal(active.hypotheses.differentiation, undefined);
  });

  it('removes the entire concept only for a slot-level retraction', () => {
    const active = projectWorkingSemanticOperations(facts(), [operation('RETRACT', null)]);
    assert.equal(active.differentiation, null);
    assert.equal(active.epistemic_states.differentiation, 'UNKNOWN');
    assert.equal(active.hypotheses.differentiation, undefined);
    assert.equal(active.evidence_statements.differentiation, undefined);
  });

  it('leaves replacement metadata untouched when the retracted value is unrelated', () => {
    const active = projectWorkingSemanticOperations(facts(), [
      replacement(HYPOTHESIS, 'HYPOTHESIS'),
      operation('RETRACT', 'old unconnected proposition'),
    ]);
    assert.equal(active.differentiation, HYPOTHESIS);
    assert.equal(active.hypotheses.differentiation, HYPOTHESIS);
    assert.equal(active.evidence_statements.differentiation, HYPOTHESIS);
  });

  it('removes the active proposition when it is retracted without replacement', () => {
    const active = projectWorkingSemanticOperations(facts(), [operation('RETRACT', OLD_VALUE)]);
    assert.equal(active.differentiation, null);
    assert.equal(active.hypotheses.differentiation, undefined);
    assert.equal(active.evidence_statements.differentiation, undefined);
  });

  it('produces the same result on repeated refinement replay', () => {
    const operations = [replacement(HYPOTHESIS, 'HYPOTHESIS'), operation('RETRACT', OLD_VALUE)];
    const first = projectWorkingSemanticOperations(facts(), operations);
    const second = projectWorkingSemanticOperations(first, operations);
    assert.deepEqual(second, first);
  });

  it('projects the exact Babrun correction into a coherent differentiation hypothesis', () => {
    const correction = 'Our differentiation is still a hypothesis: ' +
      HYPOTHESIS + '. We have not validated that as a consistent buying reason yet.\n' +
      'We have not established premium positioning for Babrun.';
    const operations = reviewCorrectionOperations(correction, { normalizedFacts: facts() }, 'turn-spec-230');
    const active = projectWorkingSemanticOperations(facts(), operations);
    const sections = sectionsFromNormalizedFacts(active);

    assert.equal(active.differentiation, `${HYPOTHESIS}.`);
    assert.equal(active.epistemic_states.differentiation, 'HYPOTHESIS');
    assert.equal(active.hypotheses.differentiation, `${HYPOTHESIS}.`);
    assert.equal(/premium positioning/i.test(JSON.stringify(active)), false);
    assert.doesNotMatch(sections.competitiveAdvantages.summary, /under evaluation|premium positioning/i);
  });

  it('fails closed when a hypothesis state has no active hypothesis metadata', () => {
    assert.throws(
      () => projectWorkingSemanticOperations({
        differentiation: HYPOTHESIS,
        epistemic_states: { differentiation: 'HYPOTHESIS' },
        hypotheses: {},
      }, []),
      /incoherent.*differentiation/i
    );
  });
});