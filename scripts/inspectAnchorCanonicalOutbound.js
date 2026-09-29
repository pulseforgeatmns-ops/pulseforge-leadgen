#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — read-only canonical AMO inspect.
 *
 * Lists every acquisition mission, identifies the STR operator objective,
 * and reports Scout / Max / Paige / Emmett / sendable-queue state.
 *
 * Never sends mail. Never mutates missions. Never enables autosend.
 *
 *   node scripts/inspectAnchorCanonicalOutbound.js --confirm-production
 */

require('dotenv').config();

const pool = require('../db');
const { inspectMission, listMissions } = require('../services/acquisitionMission');
const {
  TENANT_ID,
  CLIENT_ID,
  STR_OBJECTIVE,
  summarizeMissionSnapshot,
  pickCanonicalStrMission,
} = require('./lib/anchorCanonicalOutbound');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => arg !== '--confirm-production' && arg !== '--help' && arg !== '-h'
  );
  if (unknown.length) {
    throw Object.assign(
      new Error(`Unknown argument(s): ${unknown.join(', ')}.`),
      { code: 'unknown_args' }
    );
  }
  return { confirmProduction, help };
}

function printUsage() {
  console.log(`Anchor canonical AMO inspect (tenant ${TENANT_ID})

Usage:
  node scripts/inspectAnchorCanonicalOutbound.js --confirm-production

Safety:
  Read-only. Never APPROVE_EXECUTION, EXECUTE_OUTBOUND, or enable autosend.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }
}

async function loadClient(db) {
  const { rows } = await db.query(
    `SELECT id, name, sender_email, sending_domain, enabled_agents, autosend_enabled, active
       FROM clients
      WHERE id = $1`,
    [CLIENT_ID]
  );
  return rows[0] || null;
}

async function loadOutboundExecutions(db, missionId) {
  try {
    const { rows } = await db.query(
      `SELECT id, status, attempted_at, provider, to_email
         FROM acquisition_mission_outbound_executions
        WHERE mission_id = $1
        ORDER BY attempted_at DESC
        LIMIT 10`,
      [missionId]
    );
    return rows;
  } catch (_) {
    return [];
  }
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }
  assertRuntimeEnv();

  const db = options.pool || pool;
  const client = await loadClient(db);
  if (client?.autosend_enabled === true) {
    const err = new Error('Anchor autosend_enabled is true. Recovery refuses to proceed.');
    err.code = 'autosend_enabled';
    throw err;
  }

  const missions = await listMissions(TENANT_ID, { pool: db, production: true });
  const summaries = [];
  const inspectErrors = [];
  for (const mission of missions) {
    try {
      const snapshot = await inspectMission(mission.id, { tenantId: TENANT_ID, pool: db });
      const executions = await loadOutboundExecutions(db, mission.id);
      summaries.push(summarizeMissionSnapshot(snapshot, {
        autosendEnabled: client?.autosend_enabled === true,
        outboundExecutions: executions.map((row) => ({
          id: row.id,
          status: row.status,
          at: row.attempted_at,
          provider: row.provider || null,
          toEmail: row.to_email || null,
        })),
      }));
    } catch (err) {
      inspectErrors.push({
        id: mission.id,
        objective: mission.objective || null,
        code: err.code || null,
        message: err.message,
      });
    }
  }

  const canonical = pickCanonicalStrMission(summaries);
  const report = {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    inspectedAt: new Date().toISOString(),
    strObjective: STR_OBJECTIVE,
    client: client
      ? {
        id: client.id,
        name: client.name,
        active: client.active,
        autosendEnabled: client.autosend_enabled === true,
        enabledAgents: client.enabled_agents || [],
        senderEmail: client.sender_email || null,
      }
      : null,
    missionCount: summaries.length,
    inspectErrors,
    missions: summaries.map((row) => ({
      id: row.id,
      objective: row.objective,
      stage: row.stage,
      status: row.status,
      isStrObjective: row.isStrObjective,
      isLawFirmObjective: row.isLawFirmObjective,
      scoutCandidateCount: row.scoutCandidateCount,
      sendableCount: row.sendableCount,
      pendingIntent: row.pendingIntent,
      waitingReason: row.waitingReason,
    })),
    canonical: canonical || null,
    lawFirmMissions: summaries.filter((row) => row.isLawFirmObjective).map((row) => row.id),
    readOnly: true,
    sent: false,
  };

  return report;
}

module.exports = {
  run,
  parseArgs,
  printUsage,
  TENANT_ID,
  STR_OBJECTIVE,
};

if (require.main === module) {
  run(parseArgs())
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      if (report.help) return;
    })
    .catch((err) => {
      console.error(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
