'use strict';

/**
 * Personalization effectiveness tracking (SPEC-036).
 */

const {
  PERSONALIZATION_DIMENSIONS,
  RESPONSE_OUTCOMES,
  SUCCESS_OUTCOMES,
  buildPersonalizationFeedback,
} = require('./types');

/**
 * Aggregate personalization dimension effectiveness from outcomes.
 * @param {object[]} outcomes
 * @returns {object}
 */
function trackPersonalization(outcomes) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  /** @type {Record<string, { exposures: number, responses: number, wins: number }>} */
  const dims = {};
  for (const d of PERSONALIZATION_DIMENSIONS) {
    dims[d] = { exposures: 0, responses: 0, wins: 0 };
  }

  for (const o of list) {
    const present = dimensionsPresent(o);
    for (const d of present) {
      dims[d].exposures += 1;
      if (RESPONSE_OUTCOMES.has(o.outcomeType)) dims[d].responses += 1;
      if (SUCCESS_OUTCOMES.has(o.outcomeType) || o.successful) dims[d].wins += 1;
    }
  }

  return buildPersonalizationFeedback({ dimensions: dims });
}

/**
 * @param {object} o
 * @returns {string[]}
 */
function dimensionsPresent(o) {
  const attrs = (o && o.attributes) || {};
  const present = [];
  if (attrs.openingParagraph || attrs.opening_paragraph || attrs.opening) {
    present.push('opening_paragraph');
  }
  if (
    attrs.personalizationFacts ||
    attrs.personalization_facts ||
    attrs.facts ||
    attrs.handwritten
  ) {
    present.push('personalization_facts');
  }
  if (attrs.offer) present.push('offer');
  if (attrs.cta || attrs.callToAction) present.push('cta');
  if (attrs.insertPackage || attrs.insert_package || attrs.inserts) {
    present.push('insert_package');
  }
  // If no explicit personalization attrs, still attribute generic exposure
  // to facts when any personalization flag exists
  if (!present.length && attrs.personalization) {
    present.push('personalization_facts');
  }
  return present;
}

module.exports = {
  trackPersonalization,
  dimensionsPresent,
};
