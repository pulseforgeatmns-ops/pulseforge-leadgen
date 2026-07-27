'use strict';

const { CORE_NODE_CATEGORIES } = require('../ontology/coreGraphInvariants');
const { CRM_ENTITY_TYPES } = require('../ontology/crm/CrmOntology');
const { getOntologyRegistry } = require('../ontology/OntologyRegistry');

/**
 * Core graph node categories + CRM entity types (backward compatible).
 * Additional domain entity types are registered via OntologyRegistry.
 *
 * @typedef {import('../ontology/coreGraphInvariants').CoreNodeCategory|'company'|'person'|'interaction'|string} NodeType
 */

const NODE_TYPES = Object.freeze({
  ...CORE_NODE_CATEGORIES,
  ...CRM_ENTITY_TYPES,
});

/**
 * @returns {Set<string>}
 */
function getNodeTypeSet() {
  return new Set(getOntologyRegistry().getNodeTypes());
}

/**
 * @param {string} type
 * @returns {type is NodeType}
 */
function isNodeType(type) {
  return getOntologyRegistry().isNodeType(type);
}

/** @deprecated use getNodeTypeSet() — kept for backward compatibility */
const NODE_TYPE_SET = getNodeTypeSet();

module.exports = {
  NODE_TYPES,
  NODE_TYPE_SET,
  CORE_NODE_CATEGORIES,
  CRM_ENTITY_TYPES,
  getNodeTypeSet,
  isNodeType,
};
