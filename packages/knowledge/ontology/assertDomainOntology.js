'use strict';

/**
 * @typedef {object} VocabularyTerm
 * @property {string} id
 * @property {string} label
 * @property {string} [description]
 */

/**
 * @typedef {object} DomainOntologyDefinition
 * @property {string} id - stable domain key (e.g. crm, market)
 * @property {string} label
 * @property {string[]} entityTypes
 * @property {string[]} subjectTypes
 * @property {string[]} relationshipTypes
 * @property {string[]} observationTypes
 * @property {VocabularyTerm[]} claimVocabulary
 * @property {VocabularyTerm[]} outcomeVocabulary
 */

/**
 * @param {unknown} ontology
 * @returns {asserts ontology is DomainOntologyDefinition}
 */
function assertDomainOntology(ontology) {
  if (!ontology || typeof ontology !== 'object') {
    throw new Error('DomainOntology must be an object');
  }
  const o = /** @type {DomainOntologyDefinition} */ (ontology);
  requireNonEmptyString(o.id, 'DomainOntology.id');
  requireNonEmptyString(o.label, 'DomainOntology.label');
  requireStringArray(o.entityTypes, 'DomainOntology.entityTypes');
  requireStringArray(o.subjectTypes, 'DomainOntology.subjectTypes');
  requireStringArray(o.relationshipTypes, 'DomainOntology.relationshipTypes');
  requireStringArray(o.observationTypes, 'DomainOntology.observationTypes');
  requireVocabulary(o.claimVocabulary, 'DomainOntology.claimVocabulary');
  requireVocabulary(o.outcomeVocabulary, 'DomainOntology.outcomeVocabulary');

  for (const list of [
    o.entityTypes,
    o.subjectTypes,
    o.relationshipTypes,
    o.observationTypes,
  ]) {
    assertUnique(list);
  }
  assertUnique(o.claimVocabulary.map((t) => t.id));
  assertUnique(o.outcomeVocabulary.map((t) => t.id));
}

/**
 * @param {Partial<DomainOntologyDefinition>} input
 * @returns {DomainOntologyDefinition}
 */
function createDomainOntology(input) {
  const ontology = {
    id: String(input.id),
    label: String(input.label),
    entityTypes: [...(input.entityTypes || [])],
    subjectTypes: [...(input.subjectTypes || [])],
    relationshipTypes: [...(input.relationshipTypes || [])],
    observationTypes: [...(input.observationTypes || [])],
    claimVocabulary: [...(input.claimVocabulary || [])],
    outcomeVocabulary: [...(input.outcomeVocabulary || [])],
  };
  assertDomainOntology(ontology);
  return ontology;
}

/**
 * @param {string} name
 * @param {string} value
 */
function requireNonEmptyString(value, name) {
  if (!value || typeof value !== 'string') {
    throw new Error(`${name} must be a non-empty string`);
  }
}

/**
 * @param {unknown} value
 * @param {string} name
 */
function requireStringArray(value, name) {
  if (!Array.isArray(value)) {
    throw new Error(`${name} must be an array`);
  }
  for (const item of value) {
    if (typeof item !== 'string' || !item) {
      throw new Error(`${name} must contain non-empty strings`);
    }
  }
}

/**
 * @param {unknown} value
 * @param {string} name
 */
function requireVocabulary(value, name) {
  if (!Array.isArray(value)) {
    throw new Error(`${name} must be an array`);
  }
  for (const term of value) {
    if (!term || typeof term !== 'object') {
      throw new Error(`${name} entries must be objects`);
    }
    requireNonEmptyString(term.id, `${name}[].id`);
    requireNonEmptyString(term.label, `${name}[].label`);
  }
}

/**
 * @param {string[]} values
 */
function assertUnique(values) {
  const seen = new Set();
  for (const value of values) {
    if (seen.has(value)) {
      throw new Error(`Duplicate ontology term: ${value}`);
    }
    seen.add(value);
  }
}

module.exports = {
  assertDomainOntology,
  createDomainOntology,
};
