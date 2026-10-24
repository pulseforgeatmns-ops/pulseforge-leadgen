'use strict';

/**
 * Shared Mission Engine runtime (SPEC-022 + SPEC-024).
 * MISSION_ENGINE=0 disables product routing (rollback).
 */

const {
  createMissionEngine,
  createInMemoryMissionStore,
  createPostgresMissionStore,
  ensureMissionSchema,
  missionEnabled,
  routeIntent,
  activeMissionResolverEnabled,
} = require('../packages/mission-engine');
const { createBuiltinRegistry } = require('../packages/capabilities');
const {
  createPostgresDiscoveryProfileStore,
  ensureDiscoveryProfileSchema,
  createDiscoveryProfileStore,
} = require('../packages/capabilities/discovery');

let enginePromise = null;

/**
 * @param {object} [options]
 * @param {boolean} [options.reset]
 * @param {{ query: Function }} [options.pool]
 * @param {boolean} [options.inMemory]
 * @param {boolean} [options.useFixture]
 */
function getMissionEngine(options = {}) {
  if (options.reset) {
    enginePromise = null;
  }
  if (!enginePromise) {
    enginePromise = boot(options);
  }
  return enginePromise;
}

async function boot(options = {}) {
  let pool = options.pool || null;
  if (!pool && options.inMemory !== true) {
    try {
      pool = require('../db');
    } catch {
      pool = null;
    }
  }

  let profileStore = createDiscoveryProfileStore();
  let crmLookup = null;

  if (pool && options.inMemory !== true) {
    try {
      await ensureDiscoveryProfileSchema(pool);
      const pgProfiles = createPostgresDiscoveryProfileStore(pool);
      await pgProfiles.seedIfEmpty();
      profileStore = pgProfiles;
      crmLookup = createCrmLookup(pool);
    } catch (err) {
      console.error(
        '[missionRuntime] Discovery profile store failed — using in-memory:',
        err.message
      );
    }
  }

  const registry = createBuiltinRegistry({
    discovery: {
      profileStore,
      crmLookup,
      useFixture: options.useFixture === true,
    },
  });

  if (options.inMemory === true) {
    return createMissionEngine({
      registry,
      store: createInMemoryMissionStore(),
      planner: undefined,
    });
  }

  if (pool) {
    try {
      await ensureMissionSchema(pool);
      return createMissionEngine({
        registry,
        store: createPostgresMissionStore(pool),
      });
    } catch (err) {
      console.error(
        '[missionRuntime] Postgres store failed — using in-memory:',
        err.message
      );
    }
  }

  return createMissionEngine({
    registry,
    store: createInMemoryMissionStore(),
  });
}

/**
 * Tenant-scoped CRM duplicate lookup for Prospect Discovery.
 * @param {{ query: Function }} pool
 */
function createCrmLookup(pool) {
  return async function crmLookup(query) {
    const clientId = Number(query.clientId) || null;
    if (clientId == null) return [];
    const websites = (query.websites || []).filter(Boolean).slice(0, 100);
    const names = (query.names || []).filter(Boolean).slice(0, 100);
    if (!websites.length && !names.length) return [];

    const params = [clientId];
    const clauses = [];
    if (websites.length) {
      params.push(websites);
      clauses.push(
        `LOWER(REGEXP_REPLACE(COALESCE(website, ''), '^https?://(www\\.)?', '', 'i')) = ANY($${params.length}::text[])`
      );
    }
    if (names.length) {
      params.push(names);
      clauses.push(
        `LOWER(REGEXP_REPLACE(COALESCE(name, ''), '[^a-z0-9]+', ' ', 'g')) = ANY($${params.length}::text[])`
      );
    }

    try {
      const { rows } = await pool.query(
        `SELECT name AS "companyName", website, false AS "isCustomer"
         FROM companies
         WHERE client_id = $1 AND (${clauses.join(' OR ')})
         LIMIT 200`,
        params
      );
      return rows;
    } catch {
      return [];
    }
  };
}

module.exports = {
  getMissionEngine,
  missionEnabled,
  routeIntent,
  activeMissionResolverEnabled,
  createCrmLookup,
};
