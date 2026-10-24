'use strict';

/**
 * SPEC-130 — Hydrate persisted Acquisition Missions before workspace runtime resolution.
 * SPEC-140 — Hydration executes against Runtime.current() only.
 * Mission visibility precedes runtime selection.
 */

const {
  resolveTenantId,
  resolveAcquisitionMissionRuntime,
} = require('./WorkspaceMissionInspection');

/** @type {Map<string, { hydratedAt: string, missionsLoaded: number }>} */
const _hydrationCache = new Map();

/** @type {Map<string, Promise<{ hydrated: boolean, missionsLoaded: number }>>} */
const _hydrationInFlight = new Map();

/** @type {object[]} */
const _auditLog = [];

function logAmoHydrationEvent(event, payload = {}) {
  const row = {
    event,
    timestamp: payload.timestamp || new Date().toISOString(),
    ...payload,
  };
  _auditLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info(`[${event}]`, JSON.stringify(row));
  }
  return row;
}

function listAmoHydrationAuditLog() {
  return _auditLog.map((row) => ({ ...row }));
}

function clearAmoHydrationAuditLog() {
  _auditLog.length = 0;
}

function clearAmoHydrationCache() {
  _hydrationCache.clear();
  _hydrationInFlight.clear();
}

function hydrationCacheKey(input = {}, tenantId) {
  const sessionId =
    input.session && input.session.id ? String(input.session.id) : 'global';
  return `${sessionId}:${String(tenantId)}`;
}

function isSessionHydrated(input = {}, tenantId) {
  const session = input.session;
  if (!session || typeof session !== 'object') return false;
  const marker = session._amoHydration;
  if (!marker || typeof marker !== 'object') return false;
  return String(marker.tenantId) === String(tenantId) && marker.hydrated === true;
}

function markSessionHydrated(input = {}, tenantId, missionsLoaded) {
  const session = input.session;
  if (!session || typeof session !== 'object') return;
  session._amoHydration = {
    tenantId: String(tenantId),
    hydrated: true,
    missionsLoaded,
    hydratedAt: new Date().toISOString(),
  };
}

function shouldHydrateFromRuntime(input = {}) {
  const tenantId = resolveTenantId(input);
  if (!tenantId) return false;
  return Boolean(resolveAcquisitionMissionRuntime(input));
}

/**
 * @param {object} input
 * @returns {Promise<{ hydrated: boolean, missionsLoaded: number, skipped?: string }>}
 */
async function ensureAmoTenantHydrated(input = {}) {
  const tenantId = resolveTenantId(input);
  if (!tenantId) {
    return { hydrated: false, missionsLoaded: 0, skipped: 'no_tenant' };
  }

  const runtime = resolveAcquisitionMissionRuntime(input);
  const persistOpts = runtime.persistOpts(input);
  const cacheKey = hydrationCacheKey(input, tenantId);

  if (isSessionHydrated(input, tenantId)) {
    const cached = _hydrationCache.get(cacheKey);
    return {
      hydrated: true,
      missionsLoaded: cached ? cached.missionsLoaded : input.session._amoHydration.missionsLoaded,
      skipped: 'session_cached',
    };
  }

  const cached = _hydrationCache.get(cacheKey);
  if (cached) {
    markSessionHydrated(input, tenantId, cached.missionsLoaded);
    return { hydrated: true, missionsLoaded: cached.missionsLoaded, skipped: 'cache_hit' };
  }

  if (persistOpts.persist === false) {
    logAmoHydrationEvent('AMO_HYDRATE_BEGIN', { tenantId, cacheKey });
    await runtime.hydrate(tenantId, { persist: false });
    const missionsLoaded = runtime.engine().list(tenantId).length;
    logAmoHydrationEvent('AMO_HYDRATE_COMPLETE', {
      tenantId,
      cacheKey,
      missionsLoaded,
      runtimeId: runtime.runtimeId,
      engineId: runtime.engineId,
      storeId: runtime.storeId,
    });
    logAmoHydrationEvent('missionsLoaded', { tenantId, cacheKey, missionsLoaded });
    markSessionHydrated(input, tenantId, missionsLoaded);
    _hydrationCache.set(cacheKey, {
      hydratedAt: new Date().toISOString(),
      missionsLoaded,
    });
    return { hydrated: true, missionsLoaded, skipped: 'in_memory_runtime' };
  }

  if (_hydrationInFlight.has(cacheKey)) {
    return _hydrationInFlight.get(cacheKey);
  }

  const hydratePromise = (async () => {
    logAmoHydrationEvent('AMO_HYDRATE_BEGIN', { tenantId, cacheKey });

    await runtime.hydrate(tenantId, persistOpts);

    const missionsLoaded = runtime.engine().list(tenantId).length;

    logAmoHydrationEvent('AMO_HYDRATE_COMPLETE', {
      tenantId,
      cacheKey,
      missionsLoaded,
      runtimeId: runtime.runtimeId,
      engineId: runtime.engineId,
      storeId: runtime.storeId,
    });
    logAmoHydrationEvent('missionsLoaded', {
      tenantId,
      cacheKey,
      missionsLoaded,
    });

    const result = { hydrated: true, missionsLoaded };
    _hydrationCache.set(cacheKey, {
      hydratedAt: new Date().toISOString(),
      missionsLoaded,
    });
    markSessionHydrated(input, tenantId, missionsLoaded);
    return result;
  })();

  _hydrationInFlight.set(cacheKey, hydratePromise);
  try {
    return await hydratePromise;
  } finally {
    _hydrationInFlight.delete(cacheKey);
  }
}

function logAmoActiveResolved(mission, tenantId) {
  if (!mission) return null;
  return logAmoHydrationEvent('AMO_ACTIVE_RESOLVED', {
    tenantId,
    missionId: mission.id,
    stage: mission.stage || null,
    pendingOperatorDecision: Boolean(mission.pendingOperatorDecision),
  });
}

module.exports = {
  ensureAmoTenantHydrated,
  logAmoActiveResolved,
  logAmoHydrationEvent,
  listAmoHydrationAuditLog,
  clearAmoHydrationAuditLog,
  clearAmoHydrationCache,
  shouldHydrateFromRuntime,
};
