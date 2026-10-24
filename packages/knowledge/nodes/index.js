'use strict';

const { createCompany, updateCompany } = require('./Company');
const { createPerson, updatePerson } = require('./Person');
const { createInteraction, updateInteraction } = require('./Interaction');
const { createEvidenceNode, updateEvidenceNode } = require('./Evidence');
const { createClaimNode, updateClaimNode } = require('./Claim');
const { createObservationNode, updateObservationNode } = require('./Observation');
const { createOutcomeNode, updateOutcomeNode } = require('./Outcome');
const { createOntologyEntity, updateOntologyEntity } = require('./OntologyEntity');
const { NODE_TYPES } = require('../types/nodeTypes');
const { CORE_NODE_CATEGORIES } = require('../ontology/coreGraphInvariants');
const { getOntologyRegistry } = require('../ontology/OntologyRegistry');

const FACTORIES = Object.freeze({
  [NODE_TYPES.COMPANY]: createCompany,
  [NODE_TYPES.PERSON]: createPerson,
  [NODE_TYPES.INTERACTION]: createInteraction,
  [CORE_NODE_CATEGORIES.EVIDENCE]: createEvidenceNode,
  [CORE_NODE_CATEGORIES.CLAIM]: createClaimNode,
  [CORE_NODE_CATEGORIES.OBSERVATION]: createObservationNode,
  [CORE_NODE_CATEGORIES.OUTCOME]: createOutcomeNode,
});

const UPDATERS = Object.freeze({
  [NODE_TYPES.COMPANY]: updateCompany,
  [NODE_TYPES.PERSON]: updatePerson,
  [NODE_TYPES.INTERACTION]: updateInteraction,
  [CORE_NODE_CATEGORIES.EVIDENCE]: updateEvidenceNode,
  [CORE_NODE_CATEGORIES.CLAIM]: updateClaimNode,
  [CORE_NODE_CATEGORIES.OBSERVATION]: updateObservationNode,
  [CORE_NODE_CATEGORIES.OUTCOME]: updateOutcomeNode,
});

/**
 * @param {string} type
 * @param {object} input
 */
function createNodeByType(type, input) {
  const factory = FACTORIES[type];
  if (factory) {
    return factory(input);
  }
  const registry = getOntologyRegistry();
  if (registry.entityTypes.has(type)) {
    return createOntologyEntity({ ...input, type });
  }
  throw new Error(`Unknown node type: ${type}`);
}

/**
 * @param {object} node
 * @param {object} patch
 */
function updateNodeByType(node, patch) {
  const updater = UPDATERS[node.type];
  if (updater) {
    return updater(node, patch);
  }
  const registry = getOntologyRegistry();
  if (registry.entityTypes.has(node.type)) {
    return updateOntologyEntity(node, patch);
  }
  throw new Error(`Unknown node type: ${node.type}`);
}

module.exports = {
  createNodeByType,
  updateNodeByType,
  createCompany,
  createPerson,
  createInteraction,
  createEvidenceNode,
  createClaimNode,
  createObservationNode,
  createOutcomeNode,
  createOntologyEntity,
};
