'use strict';

/**
 * Direct Mail Execution Capability (SPEC-035 / ADR-022).
 */

const types = require('./types');
const transitions = require('./transitions');
const validate = require('./validate');
const assemble = require('./assemble');
const actions = require('./actions');
const {
  InMemoryDirectMailExecutionStore,
  createInMemoryDirectMailExecutionStore,
} = require('./DirectMailExecutionStore');
const {
  createDirectMailExecutionCapability,
} = require('./DirectMailExecution');

module.exports = {
  ...types,
  ...transitions,
  ...validate,
  ...assemble,
  ...actions,
  InMemoryDirectMailExecutionStore,
  createInMemoryDirectMailExecutionStore,
  createDirectMailExecutionCapability,
};
