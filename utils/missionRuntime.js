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
/** @type {{ durable: boolean, store: string, reason: string, at: string }|null} */
let persistenceStatus = null;

const MISSION_PERSISTENCE_UNAVAILABLE = 'MISSION_PERSISTENCE_UNAVAILABLE';

/**
 * P0 Max reliability — mission persistence never silently degrades.
 *
 * Ephemeral (in-memory) mission storage is only used when a caller asks for it
 * explicitly (`inMemory: true`, isolated tests) or a developer opts in with
 * MISSION_STORE_ALLOW_EPHEMERAL=1. Otherwise a missing pool or a failed
 * PostgreSQL initialization rejects with MISSION_PERSISTENCE_UNAVAILABLE so no
 * caller can report a mission as persisted when it is not.
 *
 * @param {object} [options]
 */
function ephemeralAllowed(options = {}) {
  if (options.inMemory === true) return true;
  if (options.allowEphemeral === true) return true;
  return String(process.env.MISSION_STORE_ALLOW_EPHEMERAL || '') === '1';
}

function persistenceUnavailable(reason, cause) {
  const err = new Error(`Mission persistence unavailable: ${reason}`);
  err.code = MISSION_PERSISTENCE_UNAVAILABLE;
  err.reason = reason;
  if (cause) err.cause = cause;
  return err;
}

function recordPersistence(engine, status) {
  persistenceStatus = { ...status, at: new Date().toISOString() };
  if (engine && typeof engine === 'object') {
    engine.persistence = { ...persistenceStatus };
  }
  return engine;
}

/**
 * @returns {{ durable: boolean, store: string, reason: string, at: string }|null}
 */
function getMissionPersistenceStatus() {
  return persistenceStatus ? { ...persistenceStatus } : null;
}

/**
 * @param {object} [options]
 * @param {boolean} [options.reset]
 * @param {{ query: Function }} [options.pool]
 * @param {boolean} [options.inMemory]
 * @param {boolean} [options.allowEphemeral]
 * @param {boolean} [options.useFixture]
 */
function getMissionEngine(options = {}) {
  if (options.reset) {
    enginePromise = null;
  }
  if (!enginePromise) {
    const booting = boot(options);
    enginePromise = booting;
    // A failed boot must not be cached: the next caller retries, so a
    // transient database outage recovers without a process restart.
    booting.catch(() => {
      if (enginePromise === booting) enginePromise = null;
    });
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
    return recordPersistence(
      createMissionEngine({
        registry,
        store: createInMemoryMissionStore(),
        planner: undefined,
      }),
      { durable: false, store: 'memory', reason: 'explicit_in_memory' }
    );
  }

  let failure = null;
  if (pool) {
    try {
      await ensureMissionSchema(pool);
      return recordPersistence(
        createMissionEngine({
          registry,
          store: createPostgresMissionStore(pool),
        }),
        { durable: true, store: 'postgres', reason: 'postgres_ready' }
      );
    } catch (err) {
      failure = persistenceUnavailable('postgres_initialization_failed', err);
    }
  } else {
    failure = persistenceUnavailable('no_database_pool');
  }

  if (!ephemeralAllowed(options)) {
    recordPersistence(null, { durable: false, store: 'none', reason: failure.reason });
    console.error('[missionRuntime] mission persistence unavailable — failing closed:', failure.reason,
      failure.cause && failure.cause.message ? failure.cause.message : '');
    throw failure;
  }

  console.warn('[missionRuntime] EPHEMERAL mission store (explicit opt-in):', failure.reason);
  return recordPersistence(
    createMissionEngine({
      registry,
      store: createInMemoryMissionStore(),
    }),
    { durable: false, store: 'memory', reason: `ephemeral_opt_in:${failure.reason}` }
  );
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
  MISSION_PERSISTENCE_UNAVAILABLE,
  getMissionEngine,
  getMissionPersistenceStatus,
  missionEnabled,
  routeIntent,
  activeMissionResolverEnabled,
  createCrmLookup,
};
