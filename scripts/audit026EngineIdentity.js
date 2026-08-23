'use strict';

/**
 * AUDIT-026 — Engine Identity
 *
 * Traces engine/store/singleton identity through mission create + plan approval.
 *
 *   node scripts/audit026EngineIdentity.js
 */

const {
  clearEngineIdentityLog,
  listEngineIdentityEvents,
} = require('../packages/max/workspace/audit/EngineIdentityAudit');
const { resetAmoRuntime, resetEngine } = require('../services/acquisitionMission');
const { createWorkspaceEngine } = require('../packages/max/workspace/WorkspaceEngine');
const { clearAmoHydrationCache } = require('../packages/max/workspace/AmoWorkspaceHydration');
const amo = require('../packages/acquisition-mission');
const { maybeHandleAcquisitionOwnershipTurn } = require('../packages/max/workspace/AcquisitionOwnership');
const { maybeHandleAcquisitionMissionExecution } = require('../packages/max/workspace/AcquisitionMissionExecution');

const TENANT_ID = '10';
const OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

function createAmoMemoryPool() {
  const tables = {
    acquisition_missions: new Map(),
    acquisition_mission_events: new Map(),
    acquisition_mission_contributions: new Map(),
    acquisition_mission_observations: new Map(),
    acquisition_mission_outcomes: new Map(),
    acquisition_mission_execution_audit: new Map(),
    acquisition_mission_learning: new Map(),
  };
  let txnBackup = null;

  function cloneTables() {
    return Object.fromEntries(Object.entries(tables).map(([n, m]) => [n, new Map(m)]));
  }

  function restoreTables(backup) {
    for (const [name, map] of Object.entries(backup)) tables[name] = map;
  }

  async function query(sql, params = []) {
    const trimmed = sql.trim();
    if (/^CREATE TABLE|^CREATE INDEX/i.test(trimmed)) return { rows: [] };
    if (trimmed === 'BEGIN') { txnBackup = cloneTables(); return { rows: [] }; }
    if (trimmed === 'COMMIT') { txnBackup = null; return { rows: [] }; }
    if (trimmed === 'ROLLBACK') { if (txnBackup) restoreTables(txnBackup); txnBackup = null; return { rows: [] }; }
    if (/INSERT INTO acquisition_missions/i.test(sql)) {
      tables.acquisition_missions.set(params[0], { id: params[0], tenant_id: String(params[1]), payload: params[14] });
      return { rows: [] };
    }
    if (/INSERT INTO acquisition_mission_/i.test(sql)) return { rows: [] };
    if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      return { rows: [...tables.acquisition_missions.values()].filter((r) => String(r.tenant_id) === tenantId) };
    }
    if (/SELECT payload FROM acquisition_mission_contributions/i.test(sql)) return { rows: [] };
    if (/SELECT payload, id, mission_id/i.test(sql)) return { rows: [] };
    if (/SELECT id, mission_id, specialist/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_outcomes/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) return { rows: [] };
    if (/SELECT COUNT\(\*\)/i.test(sql)) return { rows: [{ count: 0 }] };
    if (/DELETE FROM acquisition_mission/i.test(sql)) return { rows: [] };
    if (/UPDATE session/i.test(sql)) return { rowCount: 0, rows: [] };
    throw new Error(`Unhandled SQL: ${trimmed.split('\n')[0]}`);
  }

  const client = { query, release() {} };
  return { query, connect: async () => client, tables };
}

function operationalEvents(events) {
  const lastResetIdx = events.map((row) => row.site).lastIndexOf('resetEngine()');
  const start = lastResetIdx >= 0 ? lastResetIdx : 0;
  return events.slice(start);
}

function compareOperationalSites(events) {
  const ops = operationalEvents(events);
  const pick = (site) => ops.filter((row) => row.site === site).pop();
  return {
    createMission: pick('createMission()'),
    advancePlanAfterApproval: pick('advancePlanAfterApproval()'),
    resolveAcquisitionActiveMission: pick('resolveAcquisitionActiveMission()'),
    workspaceAsk: ops.filter((row) => row.site === 'WorkspaceEngine.ask()'),
  };
}

function reportOperationalComparison(label, events) {
  const sites = compareOperationalSites(events);
  const lines = [`${label} — operational engine comparison`, ''];
  const rows = [
    ['createMission()', sites.createMission],
    ['advancePlanAfterApproval()', sites.advancePlanAfterApproval],
    ['resolveAcquisitionActiveMission()', sites.resolveAcquisitionActiveMission],
  ];
  for (const [name, row] of rows) {
    if (!row) {
      lines.push(`${name}: (not reached)`);
      continue;
    }
    lines.push(
      `${name}: engineId=${row.engineId} storeId=${row.storeId} singletonId=${row.singletonId} source=${row.engineSource}`
    );
  }
  if (sites.workspaceAsk.length) {
    lines.push('');
    sites.workspaceAsk.forEach((row, idx) => {
      lines.push(
        `WorkspaceEngine.ask()[${idx}]: engineId=${row.engineId} storeId=${row.storeId} source=${row.engineSource} missionId=${row.missionId || 'null'}`
      );
    });
  }
  const ids = [
    ...rows.map(([, row]) => row && row.engineId),
    ...sites.workspaceAsk.map((row) => row.engineId),
  ].filter(Boolean);
  const unique = [...new Set(ids)];
  if (unique.length <= 1) {
    lines.push('', 'Result: single engine identity across operational sites.');
  } else {
    lines.push('', `Result: ENGINE IDENTITY SPLIT — ${unique.join(' vs ')}`);
  }
  return lines.join('\n');
}

async function runInjectedEnginePath() {
  clearEngineIdentityLog();
  resetEngine();
  clearAmoHydrationCache();

  const injectedEngine = amo.createAcquisitionMissionEngine({ engineSource: 'injected_workspace' });
  const service = require('../services/acquisitionMission');
  const workspace = createWorkspaceEngine({
    acquisitionMissionEngine: injectedEngine,
    acquisitionMissionService: service,
    missionsEnabled: true,
    disableLlm: true,
  });

  const opened = workspace.open({ tenantId: TENANT_ID, page: 'command-deck' });
  await workspace.ask({
    sessionId: opened.sessionId,
    question: OBJECTIVE,
    context: { tenantId: TENANT_ID },
  });
  await workspace.ask({
    sessionId: opened.sessionId,
    question: 'approved',
    context: { tenantId: TENANT_ID },
  });

  return {
    events: listEngineIdentityEvents(),
    injectedMissionCount: injectedEngine.list(TENANT_ID).length,
    singletonMissionCount: service.getEngine().list(TENANT_ID).length,
  };
}

async function runServicePath(pool) {
  clearEngineIdentityLog();
  resetEngine();
  clearAmoHydrationCache();
  await resetAmoRuntime({ tenantId: TENANT_ID, pool, clearSessions: false });

  const service = require('../services/acquisitionMission');
  const session = { context: { tenantId: TENANT_ID } };

  const createTurn = await maybeHandleAcquisitionOwnershipTurn({
    question: OBJECTIVE,
    context: { tenantId: TENANT_ID },
    session,
    acquisitionMissionService: service,
    persist: true,
    pool,
    cieService: { getApprovedClientBlueprint: async () => null },
  });

  const missionId = createTurn.mission.id;

  await maybeHandleAcquisitionMissionExecution({
    question: 'approved',
    context: { tenantId: TENANT_ID, missionId },
    acquisitionMissionService: service,
    persist: true,
    pool,
    allowFixtureFallback: true,
  });

  return listEngineIdentityEvents();
}

async function runWorkspacePath(pool) {
  clearEngineIdentityLog();
  resetEngine();
  clearAmoHydrationCache();
  await resetAmoRuntime({ tenantId: TENANT_ID, pool, clearSessions: false });

  const service = require('../services/acquisitionMission');
  const workspace = createWorkspaceEngine({
    acquisitionMissionService: service,
    missionsEnabled: true,
    disableLlm: true,
    operatorContextOpts: { pool },
  });

  const opened = workspace.open({ tenantId: TENANT_ID, page: 'command-deck' });

  await workspace.ask({
    sessionId: opened.sessionId,
    question: OBJECTIVE,
    context: { tenantId: TENANT_ID },
    pool,
    persist: true,
  });

  await workspace.ask({
    sessionId: opened.sessionId,
    question: 'approved',
    context: { tenantId: TENANT_ID },
    pool,
    persist: true,
  });

  return listEngineIdentityEvents();
}

async function main() {
  const pool = createAmoMemoryPool();

  console.log('=== PATH A: service facade (createMission → advancePlanAfterApproval) ===\n');
  const serviceEvents = await runServicePath(pool);
  console.log('\n' + reportOperationalComparison('PATH A', serviceEvents));

  console.log('\n\n=== PATH B: WorkspaceEngine.ask (service singleton only) ===\n');
  const workspaceEvents = await runWorkspacePath(pool);
  console.log('\n' + reportOperationalComparison('PATH B', workspaceEvents));

  console.log('\n\n=== PATH C: WorkspaceEngine with injected acquisitionMissionEngine ===\n');
  const injected = await runInjectedEnginePath();
  console.log('\n' + reportOperationalComparison('PATH C', injected.events));
  console.log(`\ninjected engine mission count: ${injected.injectedMissionCount}`);
  console.log(`singleton mission count: ${injected.singletonMissionCount}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
