'use strict';

/**
 * AUDIT-025 — Last Durable Writer (Stop Report)
 *
 * Traces every persistMission() write through plan approval and stops at the
 * first stale overwrite of acquisition_missions state.
 *
 *   node scripts/audit025LastDurableWriter.js
 */

const amo = require('../packages/acquisition-mission');
const {
  clearWriteEventLog,
  listWriteEvents,
  findFirstStaleOverwrite,
} = require('../services/acquisitionMissionPersistence');
const {
  resetAmoRuntime,
  resetEngine,
  getEngine,
} = require('../services/acquisitionMission');
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
    session: new Map(),
  };
  let txnBackup = null;

  function cloneTables() {
    return Object.fromEntries(
      Object.entries(tables).map(([name, map]) => [name, new Map(map)])
    );
  }

  function restoreTables(backup) {
    for (const [name, map] of Object.entries(backup)) {
      tables[name] = map;
    }
  }

  async function query(sql, params = []) {
    const trimmed = sql.trim();
    if (/^CREATE TABLE|^CREATE INDEX/i.test(trimmed)) {
      return { rows: [] };
    }
    if (trimmed === 'BEGIN') {
      txnBackup = cloneTables();
      return { rows: [] };
    }
    if (trimmed === 'COMMIT') {
      txnBackup = null;
      return { rows: [] };
    }
    if (trimmed === 'ROLLBACK') {
      if (txnBackup) restoreTables(txnBackup);
      txnBackup = null;
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_missions/i.test(sql)) {
      const mission = params[14];
      const id = params[0];
      tables.acquisition_missions.set(id, {
        id,
        tenant_id: String(params[1]),
        client_id: params[2],
        stage: params[3],
        status: params[4],
        objective: params[5],
        target_segment: params[6],
        campaign: params[7],
        title: params[8],
        priority: params[9],
        confidence: params[10],
        owner: params[11],
        created_by: params[12],
        orchestration_mission_id: params[13],
        payload: mission,
        created_at: params[15],
        updated_at: params[16],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_events/i.test(sql)) {
      tables.acquisition_mission_events.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: params[6],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_contributions/i.test(sql)) {
      tables.acquisition_mission_contributions.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: params[5],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_observations/i.test(sql)) {
      tables.acquisition_mission_observations.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_outcomes/i.test(sql)) {
      tables.acquisition_mission_outcomes.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: params[7],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_execution_audit/i.test(sql)) {
      tables.acquisition_mission_execution_audit.set(params[0], {
        id: params[0],
        transaction_id: params[1],
        mission_id: params[2],
        tenant_id: params[3] != null ? String(params[3]) : null,
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_learning/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_missions.values()].filter(
        (row) => String(row.tenant_id) === tenantId
      );
      return { rows };
    }

    if (/SELECT payload FROM acquisition_mission_contributions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_contributions.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({ payload: row.payload }));
      return { rows };
    }

    if (/SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_events.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({
          id: row.id,
          mission_id: row.mission_id,
          kind: row.kind,
          specialist: row.specialist,
          label: row.label,
          at: row.at,
          payload: row.payload,
        }));
      return { rows };
    }

    if (/SELECT id, mission_id, specialist, observation, at FROM acquisition_mission_observations/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_observations.values()].filter(
        (row) => String(row.tenant_id) === tenantId
      );
      return { rows };
    }

    if (/SELECT payload FROM acquisition_mission_outcomes WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_outcomes.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({ payload: row.payload }));
      return { rows };
    }

    if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT COUNT\(\*\)/i.test(sql)) {
      const tableMatch = trimmed.match(/FROM (\w+)/i);
      const table = tableMatch ? tableMatch[1] : null;
      const map = table && tables[table] ? tables[table] : new Map();
      let count = map.size;
      if (/WHERE tenant_id/i.test(trimmed) && params[0]) {
        const tenantId = String(params[0]);
        count = [...map.values()].filter((row) => String(row.tenant_id || row.payload?.tenantId) === tenantId).length;
      }
      return { rows: [{ count }] };
    }

    if (/DELETE FROM acquisition_mission/i.test(sql) || /DELETE FROM acquisition_missions/i.test(sql)) {
      const tableMatch = trimmed.match(/FROM (\w+)/i);
      const table = tableMatch ? tableMatch[1] : null;
      if (table && tables[table]) {
        if (/WHERE tenant_id/i.test(trimmed) && params[0]) {
          const tenantId = String(params[0]);
          for (const [key, row] of tables[table]) {
            if (String(row.tenant_id || row.payload?.tenantId) === tenantId) {
              tables[table].delete(key);
            }
          }
        } else {
          tables[table].clear();
        }
      }
      return { rows: [] };
    }

    if (/UPDATE session/i.test(sql)) {
      return { rowCount: 0, rows: [] };
    }

    throw new Error(`Unhandled SQL in amo memory pool: ${trimmed.split('\n')[0]}`);
  }

  const client = {
    query,
    release() {},
  };

  return {
    query,
    connect: async () => client,
    tables,
  };
}

function formatWriteLine(event) {
  return [
    `WRITE #${event.writeNumber}`,
    '',
    'caller',
    event.caller,
    '',
    'version',
    String(event.version),
    '',
    'approved',
    String(event.structuredMissionApproved),
    '',
    'pending',
    event.pendingOperatorDecisionKind || 'null',
    '',
    'updatedAt',
    event.updatedAt || 'null',
  ].join('\n');
}

function formatReport(events, stale) {
  const lines = ['AUDIT-025 — Last Durable Writer (Stop Report)', ''];

  for (const event of events) {
    lines.push(formatWriteLine(event));
    lines.push('');
  }

  if (!stale) {
    lines.push('Every durable write occurred in lifecycle order. No stale overwrite was observed.');
    return lines.join('\n');
  }

  const { previous, current, reason } = stale;
  lines.push('STOP — first stale overwrite detected');
  lines.push('');
  lines.push(`Reason: ${[
    reason.versionRegression ? 'version regression' : null,
    reason.updatedAtRegression ? 'updatedAt regression' : null,
    reason.lifecycleRegression ? 'lifecycle regression' : null,
  ].filter(Boolean).join(', ')}`);
  lines.push('');
  lines.push('Previous write');
  lines.push(`  caller: ${previous.caller}`);
  lines.push(`  version: ${previous.version}`);
  lines.push(`  structuredMissionApproved: ${previous.structuredMissionApproved}`);
  lines.push(`  pendingOperatorDecision.kind: ${previous.pendingOperatorDecisionKind || 'null'}`);
  lines.push('');
  lines.push('Overwriting write');
  lines.push(`  caller: ${current.caller}`);
  lines.push(`  version: ${current.version}`);
  lines.push(`  structuredMissionApproved: ${current.structuredMissionApproved}`);
  lines.push(`  pendingOperatorDecision.kind: ${current.pendingOperatorDecisionKind || 'null'}`);
  lines.push(`  timestamp: ${current.timestamp}`);
  lines.push(`  transactionId: ${current.transactionId || 'null'}`);

  return lines.join('\n');
}

async function runAudit() {
  const pool = createAmoMemoryPool();
  clearWriteEventLog();
  resetEngine();

  await resetAmoRuntime({
    tenantId: TENANT_ID,
    pool,
    clearSessions: false,
  });

  const service = require('../services/acquisitionMission');
  const session = { context: { tenantId: TENANT_ID } };

  const createTurn = await maybeHandleAcquisitionOwnershipTurn({
    question: OBJECTIVE,
    context: { tenantId: TENANT_ID },
    session,
    acquisitionMissionService: service,
    persist: true,
    pool,
    cieService: {
      getApprovedClientBlueprint: async () => null,
    },
  });

  if (!createTurn || !createTurn.mission) {
    throw new Error('Mission creation turn failed.');
  }

  const missionId = createTurn.mission.id;
  const engine = getEngine({ pool });
  const afterCreate = engine.inspect(missionId, { tenantId: TENANT_ID });
  const pending = afterCreate.mission.pendingOperatorDecision;
  if (!pending || pending.kind !== amo.OPERATOR_DECISION_KINDS.PLAN_APPROVAL) {
    throw new Error(`Expected plan_approval pending state, got ${pending && pending.kind}`);
  }

  const approvalTurn = await maybeHandleAcquisitionMissionExecution({
    question: 'approved',
    context: { tenantId: TENANT_ID, missionId },
    acquisitionMissionEngine: engine,
    persist: true,
    pool,
    allowFixtureFallback: true,
  });

  if (!approvalTurn) {
    throw new Error('Plan approval execution turn failed.');
  }

  const afterApproval = engine.inspect(missionId, { tenantId: TENANT_ID });
  if (!afterApproval.mission.structuredMissionApproved) {
    throw new Error('Plan approval did not lock structured mission.');
  }

  const events = listWriteEvents(missionId);
  const stale = findFirstStaleOverwrite(events);
  const report = formatReport(events, stale);
  console.log('\n' + report);
  return { events, stale, report, afterApproval };
}

runAudit().catch((err) => {
  console.error(err);
  process.exit(1);
});
