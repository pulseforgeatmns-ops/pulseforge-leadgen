'use strict';

const { CORE_EDGE_TYPES } = require('../ontology/coreEdges');
const { CRM_RELATIONSHIP_TYPES } = require('../ontology/crm/CrmOntology');
const { getOntologyRegistry } = require('../ontology/OntologyRegistry');

/**
 * Universal core edges + CRM relationship types (backward compatible).
 * Additional domain relationship types register via OntologyRegistry.
 *
 * @typedef {keyof typeof CORE_EDGE_TYPES|keyof typeof CRM_RELATIONSHIP_TYPES|string} EdgeType
 */

const EDGE_TYPES = Object.freeze({
  ...CORE_EDGE_TYPES,
  ...CRM_RELATIONSHIP_TYPES,
});

/**
 * @returns {Set<string>}
 */
function getEdgeTypeSet() {
  return new Set(getOntologyRegistry().getEdgeTypes());
}

/**
 * @param {string} type
 * @returns {type is EdgeType}
 */
function isEdgeType(type) {
  return getOntologyRegistry().isEdgeType(type);
}

/** @deprecated use getEdgeTypeSet() — kept for backward compatibility */
const EDGE_TYPE_SET = getEdgeTypeSet();

module.exports = {
  EDGE_TYPES,
  EDGE_TYPE_SET,
  CORE_EDGE_TYPES,
  CRM_RELATIONSHIP_TYPES,
  getEdgeTypeSet,
  isEdgeType,
};
