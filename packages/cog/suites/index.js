'use strict';

const { listDomainIds } = require('../domains');

/** @type {import('../types').BenchmarkSuite} */
const COG_001 = Object.freeze({
  id: 'COG-001',
  version: '0.1.0',
  label: 'COG Initial Benchmark Suite',
  description: 'Foundational cognitive evaluation across all ten initial domains (COG-101 through COG-110).',
  domainIds: listDomainIds(),
  createdAt: '2026-08-24T00:00:00.000Z',
});

/** @type {Record<string, import('../types').BenchmarkSuite>} */
const SUITE_MAP = {
  'COG-001': COG_001,
};

function listSuites() {
  return Object.values(SUITE_MAP).map(s => ({ ...s }));
}

function getSuite(suiteId) {
  const found = SUITE_MAP[suiteId];
  return found ? { ...found } : null;
}

function registerSuite(suite) {
  if (!suite?.id || !suite?.version) {
    throw new Error('Suite requires id and version');
  }
  if (SUITE_MAP[suite.id]) {
    throw new Error(`Suite ${suite.id} already registered`);
  }
  SUITE_MAP[suite.id] = Object.freeze(suite);
  return { ...suite };
}

module.exports = {
  COG_001,
  SUITE_MAP,
  listSuites,
  getSuite,
  registerSuite,
};
