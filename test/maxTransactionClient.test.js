'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { Pool } = require('pg');
const { evaluateProspectShadow } = require('../utils/maxOrchestration');
const { persistNormalizedSignal, safeIngestBrevoSignal } = require('../utils/maxSignalIngestion');

const PROSPECT_ID = '5128ba03-dc0b-44fe-aeb1-f9419142d3e3';
const SIGNAL_ID = 'maxsig_transaction_client_integration';

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}

const shadowEnv = {
  MAX_ORCHESTRATION_ENABLED: 'true',
  MAX_SCORING_ENABLED: 'true',
  MAX_SHADOW_MODE: 'true',
  MAX_STATE_TRANSITIONS_ENABLED: 'false',
  MAX_ENRICHMENT_ACTIONS_ENABLED: 'false',
  MAX_WARM_SEQUENCE_ENABLED: 'false',
  MAX_CALL_TASKS_ENABLED: 'false',
  MAX_HOT_ESCALATIONS_ENABLED: 'false',
  MAX_RECYCLE_ACTIONS_ENABLED: 'false',
  MAX_SEQUENCE_ACTIONS_ENABLED: 'false',
  MAX_OPERATOR_TASKS_ENABLED: 'false',
  MAX_ENRICHMENT_RETRY_ENABLED: 'false',
  MAX_PROSPECT_ACTIONS_ENABLED: 'false',
};

test('complete shadow decision flow uses one caller-owned transaction and rolls back', { timeout: 30000 }, async t => {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    return t.skip('set MAX_SMOKE_DISPOSABLE_PG=true for the disposable PostgreSQL integration test');
  }
  for (const binary of ['/usr/local/bin/initdb', '/usr/local/bin/pg_ctl']) {
    if (!fs.existsSync(binary)) return t.skip(`PostgreSQL binary unavailable: ${binary}`);
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'max-transaction-client-pg-'));
  const port = await freePort();
  const logFile = path.join(directory, 'postgres.log');
  const commandOptions = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync('/usr/local/bin/initdb', ['-A', 'trust', '-U', 'postgres', '-D', directory], commandOptions);
  try {
    // Pin unix_socket_directories to the disposable data dir. Default socket
    // paths are not writable in GitHub Actions runners for PostgreSQL 18.
    execFileSync('/usr/local/bin/pg_ctl', [
      '-D', directory,
      '-l', logFile,
      '-o', `-p ${port} -h 127.0.0.1 -k ${directory}`,
      '-w',
      'start',
    ], commandOptions);
  } catch (error) {
    const diagnostics = fs.existsSync(logFile)
      ? fs.readFileSync(logFile, 'utf8')
      : 'PostgreSQL did not create a server log.';
    throw new Error(`Temporary PostgreSQL failed to start:\n${diagnostics}`, { cause: error });
  }
  const db = new Pool({ connectionString: `postgres://postgres@127.0.0.1:${port}/postgres`, ssl: false });
  t.after(async () => {
    await db.end().catch(() => {});
    execFileSync('/usr/local/bin/pg_ctl', ['-D', directory, '-m', 'fast', '-w', 'stop'], commandOptions);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  await db.query(`
    CREATE TABLE clients (
      id integer primary key, vertical_tiers jsonb default '{}'::jsonb,
      max_orchestration_config jsonb default '{}'::jsonb
    );
    CREATE TABLE companies (id uuid primary key, client_id integer, name text, industry text);
    CREATE TABLE prospects (
      id uuid primary key, client_id integer not null, company_id uuid, status text,
      do_not_contact boolean default false, lifecycle_state text, vertical text, icp_score integer,
      email text, email_verified boolean, phone text, decision_maker boolean,
      warmth_score integer, warmth_score_updated_at timestamptz, warmth_score_version text,
      last_meaningful_signal_at timestamptz, last_human_open_at timestamptz,
      last_reply_at timestamptz, last_positive_reply_at timestamptz,
      downgrade_candidate_since timestamptz, updated_at timestamptz default now()
    );
    CREATE TABLE email_events (event_id text, open_source text);
    CREATE TABLE prospect_signal_events (
      id text primary key, client_id integer not null, prospect_id uuid not null, company_id uuid,
      event_type text not null, event_timestamp timestamptz not null, source text not null,
      source_record_id text not null, metadata jsonb default '{}'::jsonb, created_at timestamptz default now(),
      UNIQUE (source, source_record_id, event_type, prospect_id)
    );
    CREATE TABLE max_decisions (
      id uuid primary key, client_id integer, prospect_id uuid, company_id uuid,
      trigger_event_type text, trigger_event_id text, idempotency_key text, decision_version text,
      score_version text, current_state text, recommended_state text, warmth_score integer,
      score_components jsonb, reason_codes jsonb, reason_summary text, next_best_action text,
      actions jsonb, operator_required boolean, operator_priority text, is_shadow boolean,
      config_snapshot jsonb, processing_duration_ms integer, created_at timestamptz,
      UNIQUE (client_id, idempotency_key)
    );
    CREATE TABLE prospect_state_transitions (
      id bigserial primary key, client_id integer, prospect_id uuid, decision_id uuid,
      from_state text, to_state text, warmth_score integer, reason_codes jsonb,
      reason_summary text, trigger_event_type text, trigger_event_id text,
      decision_source text, action_selected text, operator_required boolean,
      is_shadow boolean, applied boolean, created_at timestamptz
    );
    CREATE TABLE max_actions (
      id uuid primary key, client_id integer, prospect_id uuid, decision_id uuid,
      action_type text, action_status text, autonomy_level text, idempotency_key text,
      input_payload jsonb, output_payload jsonb, error_code text, error_message text,
      completed_at timestamptz, created_at timestamptz
    );
    CREATE TABLE max_orchestration_metrics (
      id bigserial primary key, client_id integer, metric_name text, metric_value numeric,
      prospect_id uuid, signal_event_id text, decision_id uuid, dimensions jsonb
    );
    CREATE TABLE agent_actions (id uuid primary key, payload jsonb, client_id integer);
    CREATE TABLE cal_queue (id bigserial primary key, prospect_id uuid, client_id integer);
    CREATE TABLE touchpoints (id uuid primary key, prospect_id uuid, client_id integer, action_type text);
  `);
  await db.query("INSERT INTO clients (id) VALUES (10)");
  await db.query("INSERT INTO prospects (id,client_id,status,icp_score) VALUES ($1,10,'cold',85)", [PROSPECT_ID]);

  const client = await db.connect();
  const calls = [];
  const originalQuery = client.query.bind(client);
  client.query = async (...args) => {
    calls.push(String(args[0]));
    return originalQuery(...args);
  };
  try {
    await client.query('BEGIN');
    const transactionContext = { client, transactionManagedByCaller: true };
    const brevoResult = {
      prospect_id: PROSPECT_ID, client_id: 10, event_id: 'brevo-epoch-unsubscribe',
      event_type: 'unsubscribed', open_source: 'unknown', has_corresponding_send: true,
    };
    const firstResult = await safeIngestBrevoSignal(brevoResult, { ts: '1784131374' }, {
      db: client, env: shadowEnv, transactionContext,
    });
    const first = firstResult.primary;
    assert.equal(first.failed, undefined);
    assert.equal(first.duplicate, false);
    assert.ok(Number.isFinite(Number(first.score.score)));
    assert.equal(first.decision.recommended_state, 'disqualified');
    const duplicateResult = await safeIngestBrevoSignal(brevoResult, { ts: 1784131374000 }, {
      db: client, env: shadowEnv, transactionContext,
    });
    assert.equal(duplicateResult.primary.duplicate, true);
    const storedSignal = await client.query('SELECT event_timestamp,metadata FROM prospect_signal_events WHERE id=$1', [first.signal_id]);
    assert.equal(storedSignal.rows[0].event_timestamp.toISOString(), '2026-07-15T16:02:54.000Z');
    assert.equal(storedSignal.rows[0].metadata.raw_source_timestamp, '1784131374');

    const signal = {
      id: SIGNAL_ID,
      client_id: 10,
      prospect_id: PROSPECT_ID,
      event_type: 'email_positive_reply',
      event_timestamp: new Date(),
      source: 'max_shadow_smoke',
      source_record_id: 'transaction-client-integration',
      metadata: { synthetic: true },
    };
    const args = {
      db: client,
      prospectId: PROSPECT_ID,
      clientId: 10,
      triggerEvent: {
        id: SIGNAL_ID,
        event_type: signal.event_type,
        event_timestamp: signal.event_timestamp,
        source: signal.source,
        source_record_id: signal.source_record_id,
        metadata: signal.metadata,
      },
      clientConfig: {},
      env: shadowEnv,
      now: new Date(),
      transactionContext,
    };

    const audit = await client.query(`
      SELECT COUNT(DISTINCT d.id)::int decisions,
             COUNT(DISTINCT t.id)::int transitions,
             COUNT(DISTINCT a.id)::int actions,
             COALESCE(BOOL_AND(a.action_status='skipped' AND a.error_code='SHADOW_MODE'),FALSE) all_skipped
      FROM max_decisions d
      LEFT JOIN prospect_state_transitions t ON t.decision_id=d.id
      LEFT JOIN max_actions a ON a.decision_id=d.id
      WHERE d.prospect_id=$1
    `, [PROSPECT_ID]);
    assert.deepEqual(audit.rows[0], { decisions: 1, transitions: 1, actions: 1, all_skipped: true });
    assert.equal(calls.filter(sql => /^\s*BEGIN\s*$/i.test(sql)).length, 1, 'only the caller begins');
    assert.equal(calls.some(sql => /^\s*COMMIT\s*$/i.test(sql)), false);
    assert.equal(calls.some(sql => /pg_advisory_xact_lock/.test(sql)), true);
    const operational = await client.query(`
      SELECT (SELECT COUNT(*) FROM agent_actions)::int agent_actions,
             (SELECT COUNT(*) FROM cal_queue)::int cal_queue,
             (SELECT COUNT(*) FROM touchpoints)::int touchpoints
    `);
    assert.deepEqual(operational.rows[0], { agent_actions: 0, cal_queue: 0, touchpoints: 0 });
    await client.query('ROLLBACK');

    await client.query(`
      ALTER TABLE max_actions
      ADD CONSTRAINT force_action_persistence_failure
      CHECK (action_type <> 'stop_automated_sequences')
    `);
    calls.length = 0;
    await client.query('BEGIN');
    const failingSignal = {
      ...signal,
      id: `${SIGNAL_ID}_failure`,
      source_record_id: 'transaction-client-integration-failure',
    };
    await persistNormalizedSignal(failingSignal, client);
    await assert.rejects(evaluateProspectShadow({
      ...args,
      triggerEvent: {
        ...args.triggerEvent,
        id: failingSignal.id,
        source_record_id: failingSignal.source_record_id,
      },
    }), /force_action_persistence_failure/);
    assert.equal(calls.filter(sql => /^\s*ROLLBACK\s*$/i.test(sql)).length, 0, 'nested failure does not roll back caller transaction');
    await client.query('ROLLBACK');
  } finally {
    client.release();
  }

  const residuals = await db.query(`
    SELECT (SELECT COUNT(*) FROM prospect_signal_events)::int signals,
           (SELECT COUNT(*) FROM max_decisions)::int decisions,
           (SELECT COUNT(*) FROM prospect_state_transitions)::int transitions,
           (SELECT COUNT(*) FROM max_actions)::int actions,
           (SELECT COUNT(*) FROM max_orchestration_metrics)::int metrics
  `);
  assert.deepEqual(residuals.rows[0], { signals: 0, decisions: 0, transitions: 0, actions: 0, metrics: 0 });
  const prospect = await db.query('SELECT status,warmth_score FROM prospects WHERE id=$1', [PROSPECT_ID]);
  assert.deepEqual(prospect.rows[0], { status: 'cold', warmth_score: null });
});
