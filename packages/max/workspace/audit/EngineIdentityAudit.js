'use strict';

/**
 * AUDIT-026 — Engine Identity tracing.
 * Logs engine/store/singleton/cache identity at durable mutation and resolution sites.
 */

const crypto = require('crypto');

const MODULE_INSTANCE_ID = crypto.randomUUID();
const engineIdentity = new WeakMap();
const storeIdentity = new WeakMap();
const workspaceEngineIdentity = new WeakMap();

/** @type {object|null} */
let singletonEngine = null;
/** @type {string|null} */
let singletonId = null;

const eventLog = [];
let sequence = 0;

function refId(map, obj, prefix) {
  if (!obj || (typeof obj !== 'object' && typeof obj !== 'function')) return null;
  if (!map.has(obj)) {
    map.set(obj, `${prefix}_${crypto.randomUUID().slice(0, 8).toUpperCase()}`);
  }
  return map.get(obj);
}

function engineId(engine) {
  return refId(engineIdentity, engine, 'ENG');
}

function storeId(store) {
  return refId(storeIdentity, store, 'STORE');
}

function workspaceEngineId(instance) {
  return refId(workspaceEngineIdentity, instance, 'WSE');
}

function registerSingleton(engine) {
  singletonEngine = engine || null;
  singletonId = engine ? engineId(engine) : null;
  return singletonId;
}

function singletonSnapshot() {
  return {
    singletonId,
    singletonStoreId: singletonEngine && singletonEngine.store ? storeId(singletonEngine.store) : null,
  };
}

function clearEngineIdentityLog() {
  eventLog.length = 0;
  sequence = 0;
}

function listEngineIdentityEvents() {
  return eventLog.slice();
}

/**
 * @param {string} site
 * @param {object} [ctx]
 */
function logEngineIdentity(site, ctx = {}) {
  const engine = ctx.engine || null;
  const store = ctx.store || (engine && engine.store) || null;
  sequence += 1;
  const row = {
    seq: sequence,
    event: 'ENGINE_IDENTITY',
    site,
    timestamp: new Date().toISOString(),
    pid: process.pid,
    moduleInstanceId: MODULE_INSTANCE_ID,
    engineId: engine ? engineId(engine) : ctx.engineId || null,
    storeId: store ? storeId(store) : ctx.storeId || null,
    ...singletonSnapshot(),
    engineMatchesSingleton: engine ? engine === singletonEngine : null,
    storeMatchesSingletonStore:
      store && singletonEngine && singletonEngine.store
        ? store === singletonEngine.store
        : null,
    tenantId: ctx.tenantId != null ? String(ctx.tenantId) : null,
    tenantCacheKey: ctx.tenantCacheKey || null,
    tenantCacheHit: ctx.tenantCacheHit ?? null,
    tenantCacheInFlight: ctx.tenantCacheInFlight ?? null,
    missionId: ctx.missionId || null,
    engineSource: ctx.engineSource || null,
    workspaceEngineId: ctx.workspaceEngine ? workspaceEngineId(ctx.workspaceEngine) : null,
    missionCount: ctx.missionCount ?? null,
    version: ctx.version ?? null,
  };
  eventLog.push(row);
  console.log('ENGINE_IDENTITY', JSON.stringify(row));
  return row;
}

function findEngineIdentityMismatch(events = eventLog) {
  const mismatches = [];
  for (let i = 0; i < events.length; i += 1) {
    for (let j = i + 1; j < events.length; j += 1) {
      const a = events[i];
      const b = events[j];
      if (!a.engineId || !b.engineId || a.engineId === b.engineId) continue;
      if (a.tenantId && b.tenantId && a.tenantId !== b.tenantId) continue;
      mismatches.push({
        first: a,
        second: b,
        engineDelta: `${a.engineId} → ${b.engineId}`,
        storeDelta:
          a.storeId && b.storeId && a.storeId !== b.storeId
            ? `${a.storeId} → ${b.storeId}`
            : null,
      });
    }
  }
  return mismatches;
}

function formatEngineIdentityReport(events = eventLog) {
  const lines = ['AUDIT-026 — Engine Identity', ''];
  for (const row of events) {
    lines.push(
      `#${row.seq} ${row.site}`,
      `  engineId: ${row.engineId || 'null'}`,
      `  storeId: ${row.storeId || 'null'}`,
      `  singletonId: ${row.singletonId || 'null'}`,
      `  engineMatchesSingleton: ${row.engineMatchesSingleton}`,
      `  engineSource: ${row.engineSource || 'null'}`,
      `  tenantCacheKey: ${row.tenantCacheKey || 'null'}`,
      `  missionId: ${row.missionId || 'null'}`,
      ''
    );
  }
  const mismatches = findEngineIdentityMismatch(events);
  if (mismatches.length === 0) {
    lines.push('No engine identity divergence observed across traced sites.');
  } else {
    lines.push('ENGINE IDENTITY DIVERGENCE DETECTED');
    for (const row of mismatches) {
      lines.push('');
      lines.push(`  ${row.first.site} (${row.first.engineId})`);
      lines.push(`  vs ${row.second.site} (${row.second.engineId})`);
      if (row.storeDelta) lines.push(`  store: ${row.storeDelta}`);
    }
  }
  return lines.join('\n');
}

module.exports = {
  MODULE_INSTANCE_ID,
  engineId,
  storeId,
  workspaceEngineId,
  registerSingleton,
  singletonSnapshot,
  clearEngineIdentityLog,
  listEngineIdentityEvents,
  logEngineIdentity,
  findEngineIdentityMismatch,
  formatEngineIdentityReport,
};
