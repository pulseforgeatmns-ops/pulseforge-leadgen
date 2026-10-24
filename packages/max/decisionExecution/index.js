'use strict';

const pipeline = require('./pipeline');
const types = require('./types');
const delegation = require('./delegation');
const { MemoryDecisionStore } = require('./store/memoryStore');

module.exports = {
  ...pipeline,
  ...types,
  ...delegation,
  MemoryDecisionStore,
  get PostgresDecisionStore() {
    return require('./store/postgresStore').PostgresDecisionStore;
  },
};
