'use strict';

/**
 * Operator Inbox Capability (SPEC-037 / ADR-024).
 */

const types = require('./types');
const priority = require('./priority');
const dedupe = require('./dedupe');
const ingest = require('./ingest');
const validate = require('./validate');
const actions = require('./actions');
const assemble = require('./assemble');
const {
  InMemoryOperatorInboxStore,
  createInMemoryOperatorInboxStore,
} = require('./OperatorInboxStore');
const { createOperatorInboxCapability } = require('./OperatorInbox');

module.exports = {
  ...types,
  ...priority,
  ...dedupe,
  ...ingest,
  ...validate,
  ...actions,
  ...assemble,
  InMemoryOperatorInboxStore,
  createInMemoryOperatorInboxStore,
  createOperatorInboxCapability,
};
