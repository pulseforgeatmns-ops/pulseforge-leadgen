'use strict';

/** @typedef {'HAS_CONTACT'|'PARTICIPATED_IN'|'GENERATED'|'SUPPORTS'|'ABOUT'|'USES'|'LOCATED_IN'|'KNOWS'|'WORKS_FOR'} EdgeType */

const EDGE_TYPES = Object.freeze({
  HAS_CONTACT: 'HAS_CONTACT',
  PARTICIPATED_IN: 'PARTICIPATED_IN',
  GENERATED: 'GENERATED',
  SUPPORTS: 'SUPPORTS',
  ABOUT: 'ABOUT',
  USES: 'USES',
  LOCATED_IN: 'LOCATED_IN',
  KNOWS: 'KNOWS',
  WORKS_FOR: 'WORKS_FOR',
});

const EDGE_TYPE_SET = new Set(Object.values(EDGE_TYPES));

/**
 * @param {string} type
 * @returns {type is EdgeType}
 */
function isEdgeType(type) {
  return EDGE_TYPE_SET.has(type);
}

module.exports = {
  EDGE_TYPES,
  EDGE_TYPE_SET,
  isEdgeType,
};
