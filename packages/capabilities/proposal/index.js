'use strict';

/**
 * Proposal Generator Capability (SPEC-027B / ADR-014).
 */

const types = require('./types');
const pricing = require('./pricing');
const evidence = require('./evidence');
const personalize = require('./personalize');
const render = require('./render');
const {
  InMemoryProposalStore,
  createInMemoryProposalStore,
} = require('./ProposalStore');
const {
  PostgresProposalStore,
  createPostgresProposalStore,
} = require('./PostgresProposalStore');
const {
  createProposalGeneratorCapability,
  resolveDiscoverySummary,
  resolveProfile,
} = require('./ProposalGenerator');

module.exports = {
  ...types,
  ...pricing,
  ...evidence,
  ...personalize,
  ...render,
  InMemoryProposalStore,
  createInMemoryProposalStore,
  PostgresProposalStore,
  createPostgresProposalStore,
  createProposalGeneratorCapability,
  resolveDiscoverySummary,
  resolveProfile,
};
