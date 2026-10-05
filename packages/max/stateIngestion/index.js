'use strict';

const pipeline = require('./pipeline');
const spreadsheet = require('./spreadsheet');
const types = require('./types');
const expectations = require('./expectations');
const { MemoryStateStore } = require('./store/memoryStore');

module.exports = {
  ...pipeline,
  ...spreadsheet,
  ...types,
  ...expectations,
  MemoryStateStore,
  get PostgresStateStore() {
    return require('./store/postgresStore').PostgresStateStore;
  },
};
