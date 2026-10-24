'use strict';

const { InMemorySignalStore } = require('./InMemorySignalStore');
const { PostgresSignalStore } = require('./PostgresSignalStore');
const { ensureSignalSchema } = require('./ensureSignalSchema');
const { seedFrontRunnersFixtures } = require('../fixtures/seedFixtures');
const { callStore } = require('./storeUtils');

/**
 * Production uses Postgres when pool is provided; tests use in-memory by default.
 *
 * @param {{ query: Function }|null} pool
 * @param {{ seedFixtures?: boolean }} [options]
 */
async function createSignalStore(pool, options = {}) {
  if (pool && typeof pool.query === 'function') {
    await ensureSignalSchema(pool);
    const store = new PostgresSignalStore(pool);
    if (options.seedFixtures !== false) {
      await seedPostgresFixtures(store);
    }
    return store;
  }
  const mem = new InMemorySignalStore();
  if (options.seedFixtures !== false) {
    seedFrontRunnersFixtures(mem);
  }
  return mem;
}

async function seedPostgresFixtures(store) {
  const mem = new InMemorySignalStore();
  seedFrontRunnersFixtures(mem);

  for (const cluster of mem.clusters.values()) {
    await callStore(store, 'upsertCluster', cluster);
  }
  for (const source of mem.sources.values()) {
    await callStore(store, 'upsertSource', source);
  }
  for (const [sourceId, clusterId] of mem.clusterMembers) {
    await callStore(store, 'addClusterMember', sourceId, clusterId);
  }
  for (const perf of mem.sourcePerformance.values()) {
    await callStore(store, 'upsertSourcePerformance', perf);
  }
  for (const token of mem.tokens.values()) {
    await callStore(store, 'upsertToken', token);
  }
  for (const event of mem.events) {
    await callStore(store, 'insertEvent', event);
  }
}

module.exports = {
  createSignalStore,
  seedPostgresFixtures,
};
