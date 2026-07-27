'use strict';

const { assertDomainOntology } = require('./assertDomainOntology');
const { CORE_NODE_CATEGORIES } = require('./coreGraphInvariants');
const { CORE_EDGE_TYPES } = require('./coreEdges');

/**
 * Pluggable domain ontology registry (SPEC-017).
 * Graph storage stays universal; domains contribute vocabulary.
 */
class OntologyRegistry {
  constructor() {
    /** @type {Map<string, import('./assertDomainOntology').DomainOntologyDefinition>} */
    this.domains = new Map();
    this.entityTypes = new Set();
    this.subjectTypes = new Set();
    this.relationshipTypes = new Set(Object.values(CORE_EDGE_TYPES));
    this.observationTypes = new Set();
    /** @type {Map<string, import('./assertDomainOntology').VocabularyTerm & { domain: string }>} */
    this.claimVocabulary = new Map();
    /** @type {Map<string, import('./assertDomainOntology').VocabularyTerm & { domain: string }>} */
    this.outcomeVocabulary = new Map();
  }

  /**
   * @param {import('./assertDomainOntology').DomainOntologyDefinition} ontology
   */
  register(ontology) {
    assertDomainOntology(ontology);
    if (this.domains.has(ontology.id)) {
      throw new Error(`Domain ontology already registered: ${ontology.id}`);
    }

    for (const entityType of ontology.entityTypes) {
      if (this.entityTypes.has(entityType)) {
        throw new Error(`Entity type collision: ${entityType}`);
      }
    }
    for (const relationshipType of ontology.relationshipTypes) {
      if (this.relationshipTypes.has(relationshipType)) {
        throw new Error(`Relationship type collision: ${relationshipType}`);
      }
    }
    for (const observationType of ontology.observationTypes) {
      if (this.observationTypes.has(observationType)) {
        throw new Error(`Observation type collision: ${observationType}`);
      }
    }
    for (const term of ontology.claimVocabulary) {
      if (this.claimVocabulary.has(term.id)) {
        throw new Error(`Claim vocabulary collision: ${term.id}`);
      }
    }
    for (const term of ontology.outcomeVocabulary) {
      if (this.outcomeVocabulary.has(term.id)) {
        throw new Error(`Outcome vocabulary collision: ${term.id}`);
      }
    }

    this.domains.set(ontology.id, ontology);
    for (const entityType of ontology.entityTypes) this.entityTypes.add(entityType);
    for (const subjectType of ontology.subjectTypes) this.subjectTypes.add(subjectType);
    for (const relationshipType of ontology.relationshipTypes) {
      this.relationshipTypes.add(relationshipType);
    }
    for (const observationType of ontology.observationTypes) {
      this.observationTypes.add(observationType);
    }
    for (const term of ontology.claimVocabulary) {
      this.claimVocabulary.set(term.id, { ...term, domain: ontology.id });
    }
    for (const term of ontology.outcomeVocabulary) {
      this.outcomeVocabulary.set(term.id, { ...term, domain: ontology.id });
    }

    return ontology;
  }

  /**
   * @param {string} domainId
   */
  getDomain(domainId) {
    return this.domains.get(domainId) || null;
  }

  /**
   * @returns {string[]}
   */
  listDomains() {
    return [...this.domains.keys()].sort();
  }

  /**
   * Core graph categories + registered domain entity types.
   * @returns {string[]}
   */
  getNodeTypes() {
    return [
      ...Object.values(CORE_NODE_CATEGORIES),
      ...[...this.entityTypes].sort(),
    ];
  }

  /**
   * @returns {string[]}
   */
  getEdgeTypes() {
    return [...this.relationshipTypes].sort();
  }

  /**
   * @param {string} type
   */
  isNodeType(type) {
    return this.getNodeTypes().includes(type);
  }

  /**
   * @param {string} type
   */
  isEdgeType(type) {
    return this.relationshipTypes.has(type);
  }

  /**
   * @param {string} type
   */
  isSubjectType(type) {
    return this.subjectTypes.has(type);
  }

  /**
   * @param {string} type
   */
  isObservationType(type) {
    return this.observationTypes.has(type);
  }

  /**
   * @param {string} claimId
   */
  getClaimTerm(claimId) {
    return this.claimVocabulary.get(claimId) || null;
  }

  /**
   * @param {string} outcomeId
   */
  getOutcomeTerm(outcomeId) {
    return this.outcomeVocabulary.get(outcomeId) || null;
  }
}

/** @type {OntologyRegistry|null} */
let defaultRegistry = null;

/**
 * @returns {OntologyRegistry}
 */
function getOntologyRegistry() {
  if (!defaultRegistry) {
    defaultRegistry = new OntologyRegistry();
    const { createCrmOntology } = require('./crm/CrmOntology');
    defaultRegistry.register(createCrmOntology());
  }
  return defaultRegistry;
}

/**
 * Reset registry (tests only).
 */
function resetOntologyRegistry() {
  defaultRegistry = null;
}

/**
 * @param {import('./assertDomainOntology').DomainOntologyDefinition} ontology
 */
function registerDomainOntology(ontology) {
  return getOntologyRegistry().register(ontology);
}

module.exports = {
  OntologyRegistry,
  getOntologyRegistry,
  resetOntologyRegistry,
  registerDomainOntology,
};
