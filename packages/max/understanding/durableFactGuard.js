'use strict';

const { EPISTEMIC_CATEGORY } = require('./types');

const INFERENCE_MARKERS = /\b(probably|sounds like|seems like|might be|maybe|i guess|likely)\b/i;

function isInferenceUtterance(text) {
  return INFERENCE_MARKERS.test(String(text || ''));
}

function painPointDurable(pain, sourceText = '') {
  if (!pain) return false;
  if (pain.current === false) return false;
  if (pain.epistemic === EPISTEMIC_CATEGORY.INFERRED) return false;
  if (isInferenceUtterance(sourceText) && pain.epistemic !== EPISTEMIC_CATEGORY.CONFIRMED) return false;
  return true;
}

function decisionMakerDurable(signal) {
  if (!signal) return false;
  if (signal.epistemic === EPISTEMIC_CATEGORY.INFERRED) return false;
  if (signal.epistemic === EPISTEMIC_CATEGORY.UNCERTAIN) return false;
  if (signal.role === 'suspected_decision_maker' && signal.epistemic !== EPISTEMIC_CATEGORY.REPORTED) {
    return false;
  }
  return signal.epistemic === EPISTEMIC_CATEGORY.CONFIRMED
    || (signal.epistemic === EPISTEMIC_CATEGORY.REPORTED && signal.reportedBy);
}

function classifyDissatisfaction(text) {
  const lower = String(text || '').toLowerCase();
  if (/not unhappy|aren't unhappy|are not unhappy|not dissatisfied/i.test(lower)) {
    return { dissatisfied: false, epistemic: EPISTEMIC_CATEGORY.CONFIRMED };
  }
  if (isInferenceUtterance(lower) && /unhappy|dissatisfied|upset/i.test(lower)) {
    return { dissatisfied: null, epistemic: EPISTEMIC_CATEGORY.INFERRED, commentary: true };
  }
  return null;
}

module.exports = {
  isInferenceUtterance,
  painPointDurable,
  decisionMakerDurable,
  classifyDissatisfaction,
};
