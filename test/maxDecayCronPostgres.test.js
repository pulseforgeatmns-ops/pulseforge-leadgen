'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { execFileSync } = require('node:child_process');
const test = require('node:test');
const { Pool } = require('pg');
const { run } = require('../maxDecayAgent');
const { createMaxDecayCronHandler } = require('../utils/maxDecayCron');

function response() {
  return {
    statusCode: 200,
    payload: null,
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.payload = payload; return this; },
  };
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

async function startPostgres(t) {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    t.skip('set MAX_SMOKE_DISPOSABLE_PG=true for the disposable PostgreSQL integration test');
    return null;
  }
  for (const binary of ['/usr/local/bin/initdb', '/usr/local/bin/pg_ctl']) {
    if (!fs.existsSync(binary)) {
      t.skip(`PostgreSQL binary unavailable: ${binary}`);
      return null;
    }
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'max-decay-cron-pg-'));
  const port = await freePort();
  const logFile = path.join(directory, 'postgres.log');
  const options = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync('/usr/local/bin/initdb', ['-A', 'trust', '-U', 'postgres', '-D', directory], options);
  try {
    // Pin unix_socket_directories to the disposable data dir. Default socket
    // paths are not writable in GitHub Actions runners for PostgreSQL 18.
    execFileSync('/usr/local/bin/pg_ctl', [
      '-D', directory,
      '-l', logFile,
      '-o', `-p ${port} -h 127.0.0.1 -k ${directory}`,
      '-w',
      'start',
    ], options);
  } catch (error) {
    const diagnostics = fs.existsSync(logFile)
      ? fs.readFileSync(logFile, 'utf8')
      : 'PostgreSQL did not create a server log.';
    throw new Error(`Temporary PostgreSQL failed to start:\n${diagnostics}`, { cause: error });
  }
  const pool = new Pool({ connectionString: `postgresql://postgres@127.0.0.1:${port}/postgres` });
  t.after(async () => {
    await pool.end();
    execFileSync('/usr/local/bin/pg_ctl', ['-D', directory, '-m', 'fast', '-w', 'stop'], options);
    fs.rmSync(directory, { recursive: true, force: true });
  });
  return pool;
}

test('authenticated decay endpoint completes one production-shaped PostgreSQL run and skips overlap', { timeout: 30000 }, async t => {
  const db = await startPostgres(t);
  if (!db) return;
  await db.query(`
    CREATE TABLE clients (id INTEGER PRIMARY KEY);
    CREATE TABLE prospects (
      id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id), warmth_score INTEGER,
      do_not_contact BOOLEAN DEFAULT FALSE, downgrade_candidate_since TIMESTAMPTZ,
      warmth_score_updated_at TIMESTAMPTZ, last_meaningful_signal_at TIMESTAMPTZ
    );
    CREATE TABLE prospect_signal_events (
      id TEXT PRIMARY KEY, client_id INTEGER, prospect_id UUID, event_type TEXT, event_timestamp TIMESTAMPTZ
    );
    CREATE TABLE max_decay_run_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID NOT NULL, job_type TEXT DEFAULT 'max_decay',
      mode TEXT NOT NULL, status TEXT NOT NULL, recorded_at TIMESTAMPTZ DEFAULT NOW(),
      started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ, lock_acquired BOOLEAN DEFAULT FALSE,
      client_scope INTEGER, batch_limit INTEGER, start_cursor UUID, end_cursor UUID,
      candidates_found INTEGER DEFAULT 0, prospects_evaluated INTEGER DEFAULT 0, scores_changed INTEGER DEFAULT 0,
      downgrade_candidates INTEGER DEFAULT 0, recommendations_created INTEGER DEFAULT 0,
      decisions_created INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, error_stage TEXT, error_code TEXT,
      error_summary TEXT, retryable BOOLEAN, operational_effects JSONB DEFAULT '{}'::jsonb,
      deployment_commit TEXT, details JSONB DEFAULT '{}'::jsonb
    );
    CREATE TABLE max_decisions (id UUID PRIMARY KEY, client_id INTEGER, prospect_id UUID, is_shadow BOOLEAN);
    CREATE TABLE prospect_state_transitions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), decision_id UUID, client_id INTEGER,
      prospect_id UUID, is_shadow BOOLEAN, applied BOOLEAN
    );
    CREATE TABLE max_actions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), decision_id UUID, client_id INTEGER,
      prospect_id UUID, action_status TEXT, error_code TEXT
    );
    INSERT INTO clients VALUES (10);
    INSERT INTO prospects (id,client_id,warmth_score,downgrade_candidate_since)
    VALUES ('1000c166-c9c3-4bab-adef-d4cbdf14ab18',10,50,NOW()-INTERVAL '4 days');
  `);

  let entered;
  let release;
  const enteredPromise = new Promise(resolve => { entered = resolve; });
  const blocker = new Promise(resolve => { release = resolve; });
  const runDecayFn = (params, database) => run(params, database, {
    loadClientOrchestrationConfig: async () => ({}),
    evaluateProspectShadow: async ({ db: transactionDb, prospectId, clientId }) => {
      const decisionId = crypto.randomUUID();
      await transactionDb.query('INSERT INTO max_decisions VALUES ($1,$2,$3,TRUE)', [decisionId, clientId, prospectId]);
      await transactionDb.query('INSERT INTO prospect_state_transitions (decision_id,client_id,prospect_id,is_shadow,applied) VALUES ($1,$2,$3,TRUE,FALSE)', [decisionId, clientId, prospectId]);
      await transactionDb.query("INSERT INTO max_actions (decision_id,client_id,prospect_id,action_status,error_code) VALUES ($1,$2,$3,'skipped','SHADOW_MODE')", [decisionId, clientId, prospectId]);
      entered();
      await blocker;
      return {
        duplicate: false,
        score: { score: 50 },
        decision: { id: decisionId, transition_recommended: true, actions: ['operator_review'], reason_codes: [] },
      };
    },
  });
  const secret = 'b'.repeat(48);
  const env = {
    MAX_DECAY_CRON_SECRET: secret,
    MAX_ORCHESTRATION_ENABLED: 'true', MAX_SCORING_ENABLED: 'true', MAX_SHADOW_MODE: 'true',
    MAX_STATE_TRANSITIONS_ENABLED: 'false', MAX_ENRICHMENT_ACTIONS_ENABLED: 'false',
    MAX_WARM_SEQUENCE_ENABLED: 'false', MAX_CALL_TASKS_ENABLED: 'false', MAX_HOT_ESCALATIONS_ENABLED: 'false',
    MAX_RECYCLE_ACTIONS_ENABLED: 'false', MAX_SEQUENCE_ACTIONS_ENABLED: 'false',
    MAX_OPERATOR_TASKS_ENABLED: 'false', MAX_ENRICHMENT_RETRY_ENABLED: 'false', MAX_PROSPECT_ACTIONS_ENABLED: 'false',
  };
  const handler = createMaxDecayCronHandler({ db, env, runDecayFn });
  const req = { query: {}, body: {}, get: name => name === 'authorization' ? `Bearer ${secret}` : undefined };
  const firstResponse = response();
  const first = handler(req, firstResponse);
  await enteredPromise;
  const overlapResponse = response();
  await handler(req, overlapResponse);
  release();
  await first;

  assert.equal(firstResponse.statusCode, 200);
  assert.equal(firstResponse.payload.status, 'completed');
  assert.equal(firstResponse.payload.operational_effects, 0);
  assert.equal(overlapResponse.payload.status, 'skipped_overlap');
  const audit = await db.query(`
    SELECT
      (SELECT COUNT(*)::int FROM max_decisions WHERE is_shadow=TRUE) decisions,
      (SELECT COUNT(*)::int FROM prospect_state_transitions WHERE is_shadow=TRUE AND applied=FALSE) transitions,
      (SELECT COUNT(*)::int FROM max_actions WHERE action_status='skipped' AND error_code='SHADOW_MODE') actions,
      (SELECT COUNT(*)::int FROM max_decay_run_events WHERE status='skipped_overlap') overlap_runs
  `);
  assert.deepEqual(audit.rows[0], { decisions: 1, transitions: 1, actions: 1, overlap_runs: 1 });
});
