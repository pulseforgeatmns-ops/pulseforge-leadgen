'use strict';

/**
 * Universal relationship vocabulary (SPEC-017).
 * Domain ontologies may register additional relationship types.
 */

const CORE_EDGE_TYPES = Object.freeze({
  SUPPORTS: 'SUPPORTS',
  CONTRADICTS: 'CONTRADICTS',
  ABOUT: 'ABOUT',
  GENERATED: 'GENERATED',
  RESULTED_IN: 'RESULTED_IN',
  OBSERVED_ON: 'OBSERVED_ON',
  PART_OF: 'PART_OF',
  INFLUENCED: 'INFLUENCED',
  SIMILAR_TO: 'SIMILAR_TO',
});

const CORE_EDGE_TYPE_SET = new Set(Object.values(CORE_EDGE_TYPES));

/**
 * @param {string} type
 * @returns {boolean}
 */
function isCoreEdgeType(type) {
  return CORE_EDGE_TYPE_SET.has(type);
}

module.exports = {
  CORE_EDGE_TYPES,
  CORE_EDGE_TYPE_SET,
  isCoreEdgeType,
};
