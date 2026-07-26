'use strict';

/** @typedef {'company'|'person'|'interaction'|'evidence'|'claim'} NodeType */

const NODE_TYPES = Object.freeze({
  COMPANY: 'company',
  PERSON: 'person',
  INTERACTION: 'interaction',
  EVIDENCE: 'evidence',
  CLAIM: 'claim',
});

const NODE_TYPE_SET = new Set(Object.values(NODE_TYPES));

/**
 * @param {string} type
 * @returns {type is NodeType}
 */
function isNodeType(type) {
  return NODE_TYPE_SET.has(type);
}

module.exports = {
  NODE_TYPES,
  NODE_TYPE_SET,
  isNodeType,
};
