'use strict';

/**
 * Client Playbook package (SPEC-028 / ADR-015).
 * Strategy asset: how a client wins customers.
 */

const types = require('./types');
const {
  ClientPlaybookStore,
  createClientPlaybookStore,
  bumpVersion,
} = require('./ClientPlaybookStore');
const {
  PostgresClientPlaybookStore,
  createPostgresClientPlaybookStore,
  ensureClientPlaybookSchema,
  ENSURE_SQL,
} = require('./PostgresClientPlaybookStore');
const {
  PlaybookSelector,
  createPlaybookSelector,
  inferPlaybookId,
  inferClientId,
} = require('./PlaybookSelector');
const { seedClientPlaybooks } = require('./seedPlaybooks');
const apply = require('./apply');

module.exports = {
  ...types,
  ClientPlaybookStore,
  createClientPlaybookStore,
  bumpVersion,
  PostgresClientPlaybookStore,
  createPostgresClientPlaybookStore,
  ensureClientPlaybookSchema,
  ENSURE_SQL,
  PlaybookSelector,
  createPlaybookSelector,
  inferPlaybookId,
  inferClientId,
  seedClientPlaybooks,
  ...apply,
};
