const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const migration = fs.readFileSync(path.join(root, 'migrations/2026-07-18-anchor-closed-loop-revenue-phase1.sql'), 'utf8');
const service = fs.readFileSync(path.join(root, 'services/revenueService.js'), 'utf8');
const routes = fs.readFileSync(path.join(root, 'routes/revenue.js'), 'utf8');
const phase15 = fs.readFileSync(path.join(root, 'migrations/2026-07-18-anchor-closed-loop-revenue-phase15.sql'), 'utf8');
const operations = fs.readFileSync(path.join(root, 'services/revenueOperations.js'), 'utf8');
const workflow = fs.readFileSync(path.join(root, '.github/workflows/revenue-postgres.yml'), 'utf8');
const phase16 = fs.readFileSync(path.join(root, 'utils/revenuePhase16.js'), 'utf8');

test('every Phase 1 revenue entity is tenant-scoped and composite-linked', () => {
  for (const table of ['customers', 'opportunities', 'revenue_jobs', 'revenue_payments', 'revenue_events', 'revenue_outcomes', 'revenue_follow_up_recommendations']) {
    const block = migration.slice(migration.indexOf(`CREATE TABLE ${table}`));
    assert.match(block.slice(0, block.indexOf('\n);') + 4), /client_id INTEGER NOT NULL/);
  }
  assert.match(migration, /FOREIGN KEY \(client_id, customer_id\) REFERENCES customers\(client_id, id\)/);
  assert.match(migration, /FOREIGN KEY \(client_id, job_id\) REFERENCES revenue_jobs\(client_id, id\)/);
  assert.match(service, /WHERE client_id = \$1 AND id = \$2/);
});

test('ledger is append-only and write idempotency is tenant/source scoped', () => {
  assert.match(migration, /UNIQUE \(client_id, source_system, idempotency_key\)/);
  assert.match(migration, /revenue_events_no_update_delete/);
  assert.match(migration, /RAISE EXCEPTION 'revenue_events is append-only'/);
  assert.match(service, /pg_advisory_xact_lock/);
  assert.match(service, /idempotentReplay: true/);
});

test('manual revenue routes require actor, tenant, idempotency, and correlation context', () => {
  assert.match(routes, /requireRevenueActor/);
  assert.match(routes, /Idempotency-Key/);
  assert.match(routes, /X-Correlation-ID/);
  assert.match(routes, /session\?\.clients\?\.\[clientId\]/);
  assert.match(routes, /String\(user\.client_id \|\| ''\) === clientId/);
  assert.match(routes, /status\(404\)\.json\(\{ error: 'Not found' \}\)/);
});

test('revenue workflow has no autonomous communication side effects', () => {
  for (const forbidden of ['sendMail(', 'publishTo', 'twilio', 'axios.', 'fetch(']) {
    assert.equal(service.includes(forbidden), false, `unexpected external side effect: ${forbidden}`);
  }
  assert.match(migration, /status TEXT NOT NULL DEFAULT 'recommended'/);
});

test('Max receives only the read model and explicit read-only permissions', () => {
  assert.match(service, /permissions: \{ read_only: true, financial_mutations: false, autonomous_follow_up: false \}/);
  assert.match(service, /async function getMaxRevenueContext/);
});

test('Phase 1.5 flags are independent, database-backed, and fail closed', () => {
  for (const flag of ['revenue_schema_enabled','revenue_operator_reads_enabled','revenue_operator_writes_enabled','revenue_max_reads_enabled','revenue_followup_recommendations_enabled']) {
    assert.match(phase15, new RegExp(`${flag} BOOLEAN NOT NULL DEFAULT FALSE`));
  }
  assert.match(routes, /requireFlag\('revenue_operator_writes_enabled'\)/);
  assert.match(routes, /requireFlag\('revenue_operator_reads_enabled'\)/);
  assert.match(routes, /requireFlag\('revenue_max_reads_enabled'\)/);
});

test('operator health and audit surfaces are read-only and omit customer financial detail', () => {
  assert.match(routes, /router\.get\('\/:clientId\/revenue-health'/);
  assert.match(routes, /router\.get\('\/:clientId\/revenue-audit'/);
  assert.doesNotMatch(routes, /router\.(post|patch|delete)\('\/:clientId\/revenue-health'/);
  assert.match(operations, /duplicate_rejection_count/);
  assert.match(operations, /last_successful_rebuild/);
});

test('required revenue CI check runs the real disposable PostgreSQL harness', () => {
  assert.match(workflow, /revenue-postgresql-required/);
  assert.match(workflow, /npm run test:revenue:postgres/);
  assert.match(phase15, /REVOKE UPDATE, DELETE, TRUNCATE ON revenue_events FROM PUBLIC/);
});

test('Phase 1.6 certification preparation is authorization-bound and records reconciliation separately from projection writes', () => {
  assert.match(phase16, /maximum_canary_outcomes must equal 1/);
  assert.match(phase16, /authorization window is not currently active/);
  assert.match(operations, /options\.record/);
  assert.match(operations, /revenue_reconciliation/);
});
