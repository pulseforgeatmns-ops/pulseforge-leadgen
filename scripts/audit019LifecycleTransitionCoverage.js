'use strict';

const { classifyMission, timestampContext } = require('../packages/acquisition-mission/audit/LifecycleTransitionAudit');

const EVENT_LIMIT = 1000;
const USAGE = `AUDIT-019: limited lifecycle pattern observations, not an origin diagnosis.
No database access occurs by default or merely because DATABASE_URL is present.

Manual read-only mode (requires separate authorization for the target database):
  node scripts/audit019LifecycleTransitionCoverage.js --database --tenant-id <id> [--mission-id <id>] [--limit <1..1000>]
Set AUDIT019_DATABASE_URL through your approved secret mechanism, preferably for
a SELECT-only role. Ambient DATABASE_URL and .env files are not used.

The tenant scope is mandatory. Mission limit defaults to 100. Results explicitly
report truncation. Creation/update times and recorded transition events cannot
prove mutation origin, deployment timing, or whether Engine.progress executed.
No known pattern matches does not certify full mission consistency.
This diagnostic never repairs data or imports the application's shared DB pool.`;

function parseArgs(argv) {
  const out = { database: false, help: false, tenantId: null, missionId: null, limit: 100 };
  const seen = new Set();
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (seen.has(flag)) throw new Error('Duplicate option. Use --help for usage.');
    seen.add(flag);
    if (flag === '--database') out.database = true;
    else if (flag === '--help') out.help = true;
    else if (['--tenant-id', '--mission-id', '--limit'].includes(flag)) {
      const value = argv[++i];
      if (!value || value.startsWith('--') || !value.trim()) {
        throw new Error('An option value is missing. Use --help for usage.');
      }
      if (flag === '--tenant-id') out.tenantId = value;
      if (flag === '--mission-id') out.missionId = value;
      if (flag === '--limit') {
        if (!/^[1-9]\d*$/.test(value) || Number(value) > 1000) {
          throw new Error('--limit must be an integer from 1 to 1000.');
        }
        out.limit = Number(value);
      }
    } else throw new Error('Unknown option. Use --help for usage.');
  }
  if (!out.help) {
    if (out.database && !out.tenantId) throw new Error('--database requires --tenant-id.');
    if (!out.database && (out.tenantId || out.missionId || seen.has('--limit'))) {
      throw new Error('Database scope options require explicit --database mode.');
    }
  }
  return out;
}

function missionFromRow(row) {
  const payload = row.payload && typeof row.payload === 'object' && !Array.isArray(row.payload)
    ? row.payload : {};
  return {
    ...payload,
    id: row.id,
    tenantId: row.tenant_id,
    stage: row.stage,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

// This function owns a dedicated client, never the application's shared pool.
// The injected factory keeps all offline tests independent of pg and the DB.
async function auditFromDatabase(opts, createClient) {
  if (!opts.database || typeof opts.tenantId !== 'string' || !opts.tenantId.trim()
      || !Number.isInteger(opts.limit) || opts.limit < 1 || opts.limit > 1000
      || (opts.missionId != null && (typeof opts.missionId !== 'string' || !opts.missionId.trim()))) {
    throw new Error('Explicit database mode, tenant scope and a bounded limit are required.');
  }
  const client = createClient();
  let transactionOpen = false;
  let failure;
  let report;
  try {
    await client.connect();
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    transactionOpen = true;
    await client.query("SET LOCAL statement_timeout = '10s'");
    await client.query("SET LOCAL idle_in_transaction_session_timeout = '10s'");
    const values = [opts.tenantId];
    const missionClause = opts.missionId ? ' AND id = $2' : '';
    if (opts.missionId) values.push(opts.missionId);
    values.push(opts.limit + 1);
    const { rows } = await client.query(
      `SELECT id, tenant_id, stage, status, created_at, updated_at, payload
       FROM acquisition_missions WHERE tenant_id = $1${missionClause}
       ORDER BY created_at ASC, id ASC LIMIT $${values.length}`,
      values
    );
    report = {
      audit: 'AUDIT-019',
      readOnly: true,
      scope: { tenantId: opts.tenantId, missionId: opts.missionId || null, limit: opts.limit },
      missionsTruncated: rows.length > opts.limit,
      limitations: [
        'Only four historical patterns are checked; no matches is not full consistency validation.',
        'Origin remains unknown. Creation/update timestamps are not mutation provenance.',
        'The historical merge reference is not verified deployment evidence.',
        'Recorded transition rows, or their absence, do not prove Engine.progress execution or causality.',
      ],
      missions: [],
    };
    for (const row of rows.slice(0, opts.limit)) {
      if (row.tenant_id !== opts.tenantId || (opts.missionId && row.id !== opts.missionId)) {
        throw new Error('Database returned a mission outside the requested scope.');
      }
      const mission = missionFromRow(row);
      const classification = classifyMission(mission);
      const observation = { id: mission.id, stage: mission.stage, classification, transitions: null };
      if (classification.hits.length) {
        const events = await client.query(
          `SELECT id, at FROM acquisition_mission_events
           WHERE tenant_id = $1 AND mission_id = $2 AND kind = 'stage_transition'
           ORDER BY at ASC, id ASC LIMIT $3`,
          [opts.tenantId, mission.id, EVENT_LIMIT + 1]
        );
        observation.transitions = {
          observedCount: Math.min(events.rows.length, EVENT_LIMIT),
          truncated: events.rows.length > EVENT_LIMIT,
          limit: EVENT_LIMIT,
          recorded: events.rows.slice(0, EVENT_LIMIT).map((event) => ({
            id: event.id, at: timestampContext(event.at),
          })),
        };
      }
      report.missions.push(observation);
    }
  } catch (error) {
    failure = error;
  } finally {
    if (transactionOpen) {
      try { await client.query('ROLLBACK'); } catch (error) { failure ||= error; }
    }
    try { await client.end(); } catch (error) { failure ||= error; }
  }
  if (failure) throw failure;
  return report;
}

async function main(argv = process.argv.slice(2), {
  env = process.env,
  log = console.log,
  createClient = (config) => new (require('pg').Client)(config),
} = {}) {
  const opts = parseArgs(argv);
  if (opts.help || !opts.database) {
    log(USAGE);
    return;
  }
  const connectionString = env.AUDIT019_DATABASE_URL;
  if (!connectionString) throw new Error('Explicit AUDIT019_DATABASE_URL is required for database mode.');
  const report = await auditFromDatabase(opts, () => createClient({
    connectionString,
    application_name: 'audit019-read-only',
    connectionTimeoutMillis: 5000,
    options: '-c default_transaction_read_only=on',
  }));
  log(JSON.stringify(report, null, 2));
  return report;
}

if (require.main === module) {
  main().catch(() => {
    // Driver errors can contain connection strings or SQL details. Do not print
    // them from a diagnostic likely to be copied into an incident report.
    console.error('AUDIT-019 failed. Check --help, scope, read-only credentials and database availability. No complete report was produced.');
    process.exitCode = 1;
  });
}

module.exports = { EVENT_LIMIT, USAGE, parseArgs, missionFromRow, auditFromDatabase, main };
