'use strict';

const { CORE_NODE_CATEGORIES } = require('./coreGraphInvariants');
const { CORE_EDGE_TYPES } = require('./coreEdges');
const {
  OntologyRegistry,
  getOntologyRegistry,
  resetOntologyRegistry,
  registerDomainOntology,
} = require('./OntologyRegistry');
const { assertDomainOntology, createDomainOntology } = require('./assertDomainOntology');
const { buildProvenance, validateProvenance, PROVENANCE_FIELDS } = require('./provenance');
const { deterministicId, entityId, observationId } = require('./identity');
const {
  CRM_ENTITY_TYPES,
  CRM_RELATIONSHIP_TYPES,
  CRM_OBSERVATION_TYPES,
  CRM_CLAIM_VOCABULARY,
  CRM_OUTCOME_VOCABULARY,
  createCrmOntology,
} = require('./crm/CrmOntology');

module.exports = {
  CORE_NODE_CATEGORIES,
  CORE_EDGE_TYPES,
  OntologyRegistry,
  getOntologyRegistry,
  resetOntologyRegistry,
  registerDomainOntology,
  assertDomainOntology,
  createDomainOntology,
  buildProvenance,
  validateProvenance,
  PROVENANCE_FIELDS,
  deterministicId,
  entityId,
  observationId,
  CRM_ENTITY_TYPES,
  CRM_RELATIONSHIP_TYPES,
  CRM_OBSERVATION_TYPES,
  CRM_CLAIM_VOCABULARY,
  CRM_OUTCOME_VOCABULARY,
  createCrmOntology,
};
