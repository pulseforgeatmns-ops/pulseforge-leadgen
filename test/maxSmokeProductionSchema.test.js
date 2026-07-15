'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { Pool } = require('pg');
const {
  OPERATIONAL_FIELDS,
  captureInvariantCounts,
  run,
} = require('../scripts/smokeMaxShadow');

const PROSPECT_ID = '5128ba03-dc0b-44fe-aeb1-f9419142d3e3';

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

function prospectColumnType(name) {
  if (['do_not_contact', 'setter_visible', 'operator_required'].includes(name)) return 'boolean';
  if (name === 'warmth_score') return 'integer';
  if (name === 'state_reason_codes') return "jsonb default '[]'::jsonb";
  if (name.endsWith('_at')) return 'timestamptz';
  return 'text';
}

function dependencies() {
  return {
    validateSchema: async () => ({ valid: true }),
    loadClientOrchestrationConfig: async () => ({ max_orchestration_config: {} }),
    loadMaxOrchestrationConfig: () => ({
      enabled: true,
      flags: {
        max_scoring_enabled: true,
        max_shadow_mode: true,
        max_state_transitions_enabled: false,
        max_enrichment_actions_enabled: false,
        max_warm_sequence_enabled: false,
        max_call_tasks_enabled: false,
        max_hot_escalations_enabled: false,
        max_recycle_actions_enabled: false,
        max_sequence_actions_enabled: false,
        max_operator_tasks_enabled: false,
        max_enrichment_retry_enabled: false,
        max_prospect_actions_enabled: false,
      },
    }),
    randomUUID: () => '00000000-0000-4000-8000-000000000001',
    ingestNormalizedSignal: async (signal, options) => {
      await options.db.query(`INSERT INTO prospect_signal_events
        (id,client_id,prospect_id,source,source_record_id,metadata)
        VALUES ('signal-1',$1,$2,$3,$4,$5::jsonb)`, [
        signal.client_id, signal.prospect_id, signal.source,
        signal.source_record_id, JSON.stringify(signal.metadata),
      ]);
      await options.db.query(`INSERT INTO max_decisions
        (id,client_id,prospect_id,trigger_event_id,is_shadow,warmth_score)
        VALUES ('decision-1',$1,$2,'signal-1',TRUE,82)`, [signal.client_id, signal.prospect_id]);
      await options.db.query(`INSERT INTO prospect_state_transitions
        (client_id,prospect_id,decision_id,is_shadow,applied)
        VALUES ($1,$2,'decision-1',TRUE,FALSE)`, [signal.client_id, signal.prospect_id]);
      await options.db.query(`INSERT INTO max_actions
        (id,client_id,prospect_id,decision_id,action_status,error_code)
        VALUES ('action-1',$1,$2,'decision-1','skipped','SHADOW_MODE'),
               ('action-2',$1,$2,'decision-1','skipped','SHADOW_MODE')`, [signal.client_id, signal.prospect_id]);
      await options.db.query('UPDATE prospects SET warmth_score=82 WHERE id=$1 AND client_id=$2', [signal.prospect_id, signal.client_id]);
      return { signal_id: 'signal-1', score: { score: 82 }, decision: { id: 'decision-1' } };
    },
  };
}

test('smoke test runs against a disposable production-shaped agent_actions schema', { timeout: 30000 }, async t => {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    return t.skip('set MAX_SMOKE_DISPOSABLE_PG=true for the disposable PostgreSQL integration test');
  }
  for (const binary of ['/usr/local/bin/initdb', '/usr/local/bin/pg_ctl']) {
    if (!fs.existsSync(binary)) return t.skip(`PostgreSQL binary unavailable: ${binary}`);
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'max-smoke-pg-'));
  const port = await freePort();
  const commandOptions = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync('/usr/local/bin/initdb', ['-A', 'trust', '-U', 'postgres', '-D', directory], commandOptions);
  execFileSync('/usr/local/bin/pg_ctl', ['-D', directory, '-o', `-p ${port} -h 127.0.0.1`, '-w', 'start'], commandOptions);
  const db = new Pool({ connectionString: `postgres://postgres@127.0.0.1:${port}/postgres`, ssl: false });
  t.after(async () => {
    await db.end().catch(() => {});
    execFileSync('/usr/local/bin/pg_ctl', ['-D', directory, '-m', 'fast', '-w', 'stop'], commandOptions);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  const prospectColumns = OPERATIONAL_FIELDS
    .map(name => `${name} ${prospectColumnType(name)}`)
    .join(',\n');
  await db.query(`
    CREATE TABLE prospects (id uuid primary key, client_id integer not null, ${prospectColumns});
    CREATE TABLE agent_actions (
      id uuid primary key, created_by text, action_type text, title text, description text,
      payload jsonb, status text, executed_at timestamptz, result text,
      created_at timestamptz default now(), client_id integer
    );
    CREATE TABLE cal_queue (id serial primary key, prospect_id uuid, client_id integer);
    CREATE TABLE touchpoints (id uuid primary key, prospect_id uuid, client_id integer, channel text, action_type text);
    CREATE TABLE email_events (id bigserial primary key, prospect_id uuid, client_id integer, event_type text);
    CREATE TABLE agent_log (id uuid primary key, prospect_id uuid, client_id integer, payload jsonb, action text);
    CREATE TABLE prospect_signal_events (
      id text primary key, client_id integer, prospect_id uuid, source text,
      source_record_id text, metadata jsonb default '{}'::jsonb
    );
    CREATE TABLE max_decisions (
      id text primary key, client_id integer, prospect_id uuid, trigger_event_id text,
      is_shadow boolean, warmth_score integer
    );
    CREATE TABLE prospect_state_transitions (
      id bigserial primary key, client_id integer, prospect_id uuid, decision_id text,
      is_shadow boolean, applied boolean
    );
    CREATE TABLE max_actions (
      id text primary key, client_id integer, prospect_id uuid, decision_id text,
      action_status text, error_code text
    );
  `);
  await db.query('INSERT INTO prospects (id,client_id,status) VALUES ($1,10,\'cold\')', [PROSPECT_ID]);
  await db.query(`
    INSERT INTO agent_actions (id,action_type,payload,client_id) VALUES
      ('00000000-0000-4000-8000-000000000010','top_level',jsonb_build_object('prospect_id',$1::text),10),
      ('00000000-0000-4000-8000-000000000011','nested',jsonb_build_object('prospect',jsonb_build_object('id',$1::text)),10),
      ('00000000-0000-4000-8000-000000000012','no_context','{}'::jsonb,10),
      ('00000000-0000-4000-8000-000000000013','null_payload',NULL,10),
      ('00000000-0000-4000-8000-000000000014','scalar_payload','"malformed-shape"'::jsonb,10)
  `, [PROSPECT_ID]);

  const before = await captureInvariantCounts(db, PROSPECT_ID, 10);
  assert.equal(before.agent_actions, 1, 'only confirmed top-level prospect_id is attributable');
  const report = await run({ clientId: 10, prospectId: PROSPECT_ID }, db, {}, dependencies());
  assert.equal(report.valid, true);
  assert.equal(report.synthetic_records_remaining, 0);
  assert.equal(report.post_rollback.counts.agent_actions, 1);
  assert.equal(report.post_rollback.prospect.status, 'cold');
  assert.equal(report.post_rollback.prospect.warmth_score, null);
});
