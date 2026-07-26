'use strict';

const { createCompany, updateCompany } = require('./Company');
const { createPerson, updatePerson } = require('./Person');
const { createInteraction, updateInteraction } = require('./Interaction');
const { createEvidenceNode, updateEvidenceNode } = require('./Evidence');
const { createClaimNode, updateClaimNode } = require('./Claim');
const { NODE_TYPES } = require('../types/nodeTypes');

const FACTORIES = Object.freeze({
  [NODE_TYPES.COMPANY]: createCompany,
  [NODE_TYPES.PERSON]: createPerson,
  [NODE_TYPES.INTERACTION]: createInteraction,
  [NODE_TYPES.EVIDENCE]: createEvidenceNode,
  [NODE_TYPES.CLAIM]: createClaimNode,
});

const UPDATERS = Object.freeze({
  [NODE_TYPES.COMPANY]: updateCompany,
  [NODE_TYPES.PERSON]: updatePerson,
  [NODE_TYPES.INTERACTION]: updateInteraction,
  [NODE_TYPES.EVIDENCE]: updateEvidenceNode,
  [NODE_TYPES.CLAIM]: updateClaimNode,
});

/**
 * @param {string} type
 * @param {object} input
 */
function createNodeByType(type, input) {
  const factory = FACTORIES[type];
  if (!factory) {
    throw new Error(`Unknown node type: ${type}`);
  }
  return factory(input);
}

/**
 * @param {object} node
 * @param {object} patch
 */
function updateNodeByType(node, patch) {
  const updater = UPDATERS[node.type];
  if (!updater) {
    throw new Error(`Unknown node type: ${node.type}`);
  }
  return updater(node, patch);
}

module.exports = {
  createNodeByType,
  updateNodeByType,
  createCompany,
  createPerson,
  createInteraction,
  createEvidenceNode,
  createClaimNode,
};
