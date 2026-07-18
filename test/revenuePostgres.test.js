'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const test = require('node:test');
const { Pool } = require('pg');

const root = path.join(__dirname, '..');

function binary(name) {
  try { return execFileSync('sh', ['-c', `command -v ${name}`], { encoding: 'utf8' }).trim(); } catch {
    try {
      const bindir = execFileSync('pg_config', ['--bindir'], { encoding: 'utf8' }).trim();
      const candidate = path.join(bindir, name);
      return fs.existsSync(candidate) ? candidate : null;
    } catch { return null; }
  }
}
function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => { const port = server.address().port; server.close(() => resolve(port)); });
  });
}
async function fingerprint(db) {
  const { rows } = await db.query(`SELECT md5(COALESCE(jsonb_agg(to_jsonb(x) ORDER BY x.client_id,x.job_id)::text,'[]')) AS hash FROM (SELECT * FROM revenue_outcomes) x`);
  return rows[0].hash;
}

test('revenue lifecycle, replay, rollback, isolation, protection, rebuild, and reconciliation pass on disposable PostgreSQL', { timeout: 90000 }, async t => {
  if (process.env.REVENUE_TEST_POSTGRES !== 'true') {
    t.skip('set REVENUE_TEST_POSTGRES=true to run the required disposable PostgreSQL test');
    return;
  }
  const initdb = binary('initdb');
  const pgCtl = binary('pg_ctl');
  if (!initdb || !pgCtl) assert.fail('PostgreSQL initdb and pg_ctl are required for the revenue integration check');

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'revenue-phase15-pg-'));
  const port = await freePort();
  const logFile = path.join(directory, 'postgres.log');
  const commandOptions = { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } };
  execFileSync(initdb, ['-A', 'trust', '-U', 'postgres', '-D', directory], commandOptions);
  try {
    execFileSync(pgCtl, [
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
  const connectionString = `postgresql://postgres@127.0.0.1:${port}/postgres`;
  const admin = new Pool({ connectionString });
  let servicePool;
  t.after(async () => {
    if (servicePool) await servicePool.end();
    await admin.end();
    execFileSync(pgCtl, ['-D', directory, '-m', 'fast', '-w', 'stop'], commandOptions);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  await admin.query(fs.readFileSync(path.join(__dirname, 'fixtures/revenueBaseSchema.sql'), 'utf8'));
  const revenueMigrations = fs.readdirSync(path.join(root, 'migrations'))
    .filter(name => /anchor-closed-loop-revenue-phase\d+\.sql$/.test(name)).sort();
  assert.deepEqual(revenueMigrations, [
    '2026-07-18-anchor-closed-loop-revenue-phase1.sql',
    '2026-07-18-anchor-closed-loop-revenue-phase15.sql',
  ]);
  for (const migration of revenueMigrations) await admin.query(fs.readFileSync(path.join(root, 'migrations', migration), 'utf8'));

  await admin.query(`
    INSERT INTO clients(id,name) VALUES (10,'Anchor Cleaning'),(11,'Other Tenant');
    INSERT INTO prospects(id,client_id,first_name,last_name,email) VALUES
      ('10000000-0000-4000-8000-000000000010',10,'Anchor','Customer','anchor@example.test'),
      ('10000000-0000-4000-8000-000000000011',11,'Other','Customer','other@example.test');
    INSERT INTO revenue_feature_flags
      (client_id,revenue_schema_enabled,revenue_operator_reads_enabled,revenue_operator_writes_enabled,revenue_max_reads_enabled,revenue_followup_recommendations_enabled,updated_by)
    VALUES (10,TRUE,TRUE,TRUE,TRUE,FALSE,'integration-test'),(11,TRUE,TRUE,TRUE,TRUE,FALSE,'integration-test');
  `);
  Object.assign(process.env, {
    DATABASE_URL: connectionString, DATABASE_SSL: 'false',
    REVENUE_SCHEMA_ENABLED: 'true', REVENUE_OPERATOR_READS_ENABLED: 'true',
    REVENUE_OPERATOR_WRITES_ENABLED: 'true', REVENUE_MAX_READS_ENABLED: 'true',
    REVENUE_FOLLOWUP_RECOMMENDATIONS_ENABLED: 'true',
  });
  delete require.cache[require.resolve('../db')];
  servicePool = require('../db');
  const revenue = require('../services/revenueService');
  const operations = require('../services/revenueOperations');
  const ctx = key => ({ idempotencyKey: key, sourceSystem: 'phase15_test', actorType: 'operator', actorId: 'test-operator' });

  const concurrent = await Promise.all([
    revenue.createCustomer(10, { prospectId: '10000000-0000-4000-8000-000000000010', customerType: 'commercial' }, ctx('customer-10')),
    revenue.createCustomer(10, { prospectId: '10000000-0000-4000-8000-000000000010', customerType: 'commercial' }, ctx('customer-10')),
  ]);
  assert.equal(concurrent.filter(result => result.idempotentReplay).length, 1);
  const customer = concurrent[0].customer;
  assert.equal((await admin.query('SELECT COUNT(*)::int n FROM customers WHERE client_id=10')).rows[0].n, 1);

  await assert.rejects(
    revenue.createOpportunity(11, { customerId: customer.id, serviceType: 'commercial cleaning', estimatedValueCents: 30000, source: 'manual' }, ctx('cross-tenant')),
    error => error.code === 'NOT_FOUND'
  );
  assert.equal((await admin.query('SELECT COUNT(*)::int n FROM opportunities WHERE client_id=11')).rows[0].n, 0);

  let opportunity = (await revenue.createOpportunity(10, {
    customerId: customer.id, prospectId: '10000000-0000-4000-8000-000000000010',
    serviceType: 'commercial cleaning', estimatedValueCents: 30000, estimatedCostCents: 10000, source: 'manual',
  }, ctx('opportunity-10'))).opportunity;
  for (const stage of ['contacted', 'qualified', 'quoted']) {
    opportunity = (await revenue.updateOpportunity(10, opportunity.id, { stage }, ctx(`opportunity-${stage}`))).opportunity;
  }
  const job = (await revenue.createJob(10, {
    opportunityId: opportunity.id, customerId: customer.id, serviceType: 'commercial cleaning',
    scheduledStart: '2026-07-18T14:00:00Z', quotedAmountCents: 30000, estimatedDirectCostCents: 10000,
  }, ctx('job-10'))).job;
  await revenue.completeJob(10, job.id, {
    completionConfirmed: true, completionDate: '2026-07-18T16:00:00Z', fullyCompleted: true,
    finalAmountCents: 30000, actualDirectCostCents: 9000,
  }, ctx('complete-10'));
  const payment = (await revenue.recordPayment(10, {
    jobId: job.id, amountCents: 30000, status: 'succeeded', paymentMethod: 'card', receivedAt: '2026-07-18T16:05:00Z',
  }, ctx('payment-10'))).payment;
  await revenue.recordRefund(10, { paymentId: payment.id, amountCents: 5000, refundedAt: '2026-07-18T17:00:00Z' }, ctx('refund-10'));

  await admin.query(`
    CREATE FUNCTION fail_selected_revenue_audit() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN IF NEW.idempotency_key='rollback-proof' THEN RAISE EXCEPTION 'rollback proof'; END IF; RETURN NEW; END $$;
    CREATE TRIGGER revenue_audit_rollback_proof BEFORE INSERT ON revenue_operator_audit FOR EACH ROW EXECUTE FUNCTION fail_selected_revenue_audit();
  `);
  await assert.rejects(revenue.createCustomer(10, { displayName: 'Must Roll Back' }, ctx('rollback-proof')), /rollback proof/);
  assert.equal((await admin.query("SELECT COUNT(*)::int n FROM customers WHERE display_name='Must Roll Back'")).rows[0].n, 0);
  assert.equal((await admin.query("SELECT COUNT(*)::int n FROM revenue_events WHERE idempotency_key='rollback-proof'")).rows[0].n, 0);
  await admin.query('DROP TRIGGER revenue_audit_rollback_proof ON revenue_operator_audit; DROP FUNCTION fail_selected_revenue_audit()');

  const report = await operations.rebuildProjections(servicePool, { clientId: 10, compareOnly: true });
  assert.equal(report.status, 'passed');
  assert.equal(report.mismatched_records, 0);
  assert.equal(report.unexplained_events, 0);
  assert.equal(report.tenants[0].source_totals.net_collected_revenue_cents, 25000);
  assert.equal(report.tenants[0].source_totals.refunded_revenue_cents, 5000);
  const recorded = await operations.rebuildProjections(servicePool, { clientId: 10, compareOnly: true, record: true });
  assert.equal(recorded.status, 'passed');
  assert.equal(recorded.recorded, true);
  assert.equal((await admin.query("SELECT COUNT(*)::int n FROM revenue_reconciliation_runs WHERE client_id=10 AND status='passed'")).rows[0].n, 1);

  await admin.query('UPDATE revenue_outcomes SET collected_revenue_cents=1 WHERE client_id=10 AND job_id=$1', [job.id]);
  const corrupted = await fingerprint(admin);
  await assert.rejects(operations.rebuildProjections(servicePool, { clientId: 10, apply: true, forceRollback: true }), error => error.code === 'FORCED_ROLLBACK_PROOF');
  assert.equal(await fingerprint(admin), corrupted, 'forced failure must roll projection repair back atomically');
  const repaired = await operations.rebuildProjections(servicePool, { clientId: 10, apply: true });
  assert.equal(repaired.status, 'passed');
  assert.equal(repaired.mismatched_records, 0);
  assert.equal(repaired.applied, true);
  const beforeFailedApply = await fingerprint(admin);
  await admin.query('UPDATE revenue_jobs SET quoted_amount_cents=31000 WHERE client_id=10 AND id=$1', [job.id]);
  const failedApply = await operations.rebuildProjections(servicePool, { clientId: 10, apply: true });
  assert.equal(failedApply.status, 'failed');
  assert.equal(failedApply.rolled_back, true);
  assert.equal(await fingerprint(admin), beforeFailedApply, 'a reconciliation mismatch must not commit projection changes');
  await admin.query('UPDATE revenue_jobs SET quoted_amount_cents=30000 WHERE client_id=10 AND id=$1', [job.id]);
  await admin.query('DELETE FROM revenue_outcomes WHERE client_id=10 AND job_id=$1', [job.id]);
  const rebuiltFromEmpty = await operations.rebuildProjections(servicePool, { clientId: 10, apply: true });
  assert.equal(rebuiltFromEmpty.status, 'passed');
  assert.equal(rebuiltFromEmpty.projected_outcome_count, 1);
  const rebuiltFingerprint = await fingerprint(admin);
  const repeatedRebuild = await operations.rebuildProjections(servicePool, { clientId: 10, apply: true });
  assert.equal(repeatedRebuild.status, 'passed');
  assert.equal(await fingerprint(admin), rebuiltFingerprint, 'repeated rebuild must reproduce the exact projection');
  const ranged = await operations.rebuildProjections(servicePool, {
    clientId: 10, compareOnly: true, from: '2026-07-18T13:00:00Z', to: '2026-07-19T00:00:00Z',
  });
  assert.equal(ranged.status, 'passed');
  assert.equal(ranged.tenants[0].source_totals.job_count, 1);

  await admin.query(`CREATE ROLE revenue_application LOGIN; GRANT USAGE ON SCHEMA public TO revenue_application; GRANT SELECT,INSERT ON revenue_events TO revenue_application`);
  const ordinary = new Pool({ connectionString: `postgresql://revenue_application@127.0.0.1:${port}/postgres` });
  await assert.rejects(ordinary.query('UPDATE revenue_events SET event_type=event_type'), error => error.code === '42501');
  await assert.rejects(ordinary.query('DELETE FROM revenue_events'), error => error.code === '42501');
  await ordinary.end();
  await assert.rejects(admin.query('UPDATE revenue_events SET event_type=event_type'), /append-only/);

  const health = await operations.getRevenueHealth(servicePool, 10);
  assert.equal(health.reconciliation_status, 'passed');
  assert.equal(Number(health.unprojected_event_count), 0);
  assert.ok(health.last_successful_rebuild);
  const audit = await operations.listOperatorAudit(servicePool, 10);
  assert.ok(audit.some(row => row.action === 'refund_issued' && row.financial_delta.refunded_revenue_cents === 5000));
  assert.equal((await admin.query('SELECT COUNT(*)::int n FROM revenue_follow_up_recommendations')).rows[0].n, 0);
  if (process.env.REVENUE_TEST_EVIDENCE === 'true') {
    console.log(`# REVENUE_PHASE15_EVIDENCE ${JSON.stringify({
      status: repaired.status, correlation_id: repaired.execution_correlation_id,
      ledger_event_count: repaired.ledger_event_count, projected_outcome_count: repaired.projected_outcome_count,
      mismatched_records: repaired.mismatched_records, unexplained_events: repaired.unexplained_events,
      totals: repaired.tenants[0].source_totals,
    })}`);
  }
});
