'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler.
 * Compile market knowledge into an approved AIM. Never execute outreach.
 */

const types = require('./types');
const ingestion = require('./Ingestion');
const extraction = require('./Extraction');
const ontology = require('./Ontology');
const review = require('./Review');
const publication = require('./Publication');
const { createMemoryAicStore } = require('./Store');
const compiler = require('./Compiler');

module.exports = {
  ...types,
  ...ingestion,
  ...extraction,
  ...ontology,
  ...review,
  ...publication,
  createMemoryAicStore,
  ...compiler,
};
