'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createShadowEventSink } = require('../packages/decision-service/ShadowEventSink');
const { insertShadowEvent, listShadowEvents } = require('../packages/decision-service/ShadowEventRepository');
const { queryShadowReview } = require('../packages/decision-service/shadowReview');
const { DecisionService } = require('../packages/decision-service/DecisionService');
const fixture = require('./fixtures/decisionShadowEvent.json');
const execFileAsync = promisify(execFile);
const root = path.join(__dirname, '..');

test('real PostgreSQL: migration, full round trip, duplicate safety, review filters, failures and read-only CLI',
  { timeout: 60000 }, async t => {
    // Always creates its own database; never uses DATABASE_URL from the shell.
    const instance = await startDisposablePostgres('jev002-');
    const db = new Pool({ connectionString: instance.connectionString });
    const env = { DECISION_SHADOW_ENABLED: 'true', DATABASE_URL: instance.connectionString,
      DATABASE_SSL: 'false', DECISION_SHADOW_DB_TIMEOUT_MS: '500' };
    const warnings = [];
    const sink = createShadowEventSink({ env, warn: row => warnings.push(row) });
    t.after(async () => { await sink.close(); await db.end(); await instance.stop(); });

    // Missing migrations are a best-effort write failure, never a request error.
    sink.write(fixture);
    await sink.drain();
    assert.equal(sink.stats().failed, 1);
    assert.equal(warnings[0].reason, 'write_failed');
    const migration = fs.readFileSync(path.join(root, 'migrations/2026-09-21-decision-shadow-review.sql'), 'utf8');
    await db.query(migration);
    await db.query(migration);
    // A failed write did not poison the pool; no automatic retries occur.
    sink.write(fixture);
    await sink.drain();
    let rows = await listShadowEvents(db);
    assert.deepEqual(JSON.parse(JSON.stringify(rows)), [fixture]);
    sink.write({ ...fixture, confidence: 0.01 });
    await sink.drain();
    assert.equal((await listShadowEvents(db))[0].confidence, 0.99, 'duplicate decision cannot overwrite original evidence');

    const rawRow = { ...fixture, decision_id: randomUUID(), tenant_id: null, session_id: null,
      mission_id: null, message_index: null, raw_redacted_response: { usage: { input_tokens: 12 } } };
    await insertShadowEvent(db, rawRow);
    const roundtrip = (await listShadowEvents(db)).find(row => row.decision_id === rawRow.decision_id);
    assert.deepEqual(JSON.parse(JSON.stringify(roundtrip)), rawRow);

    // Exercise the normal process.env/default singleton path, not only injection.
    // With JEV_ENABLED=false the real provider is noop; this makes no API call.
    const child = await execFileAsync(process.execPath, ['-e', `
      const { DecisionService } = require('./packages/decision-service/DecisionService');
      const a = new DecisionService({ audit() {} });
      const b = new DecisionService({ audit() {} });
      if (a._persistence !== b._persistence) throw Error('default sink must be shared');
      a.begin({ question: 'Status?', context: { tenantId: 'default-path-test' } }).complete({ route: 'mission' });
      a.drain().then(() => console.log('drained')).catch(() => { process.exitCode = 1; });
    `], { cwd: root, env: { ...process.env, ...env, DECISION_PROVIDER: 'noop', JEV_ENABLED: 'false',
      DECISION_SHADOW_PERSIST_ENABLED: 'true' } });
    assert.match(child.stdout, /drained/);
    const defaultRows = await listShadowEvents(db, { tenantId: 'default-path-test' });
    assert.equal(defaultRows.length, 1);
    assert.equal(defaultRows[0].status, 'fallback');

    const service = new DecisionService({ env, persistence: sink, audit() {}, provider: {
      name: 'jev', evaluate() { throw Object.assign(Error('private'), { code: 'http_error', http_status: 401 }); },
    } });
    service.begin({ question: 'Status?', context: { tenantId: '10' } }).complete({ route: 'intelligence' });
    await service.drain();
    const errors = await queryShadowReview(db, { tenantId: '10', filter: 'errors' });
    assert.equal(errors.summary.errors, 1);
    assert.deepEqual(errors.errors[0].errors, [{ code: 'http_error', http_status: 401 }]);
    assert.equal(errors.errors[0].recommended_route, null);

    // Newer matches and another tenant cannot bury the mismatch-specific query.
    for (let i = 0; i < 55; i += 1) await insertShadowEvent(db, { ...fixture,
      decision_id: randomUUID(), timestamp: new Date(Date.UTC(2026, 9, 1, 0, 0, i)).toISOString(),
      comparison: 'match', route_matches: true, recommended_route: 'conversation' });
    await insertShadowEvent(db, { ...fixture, decision_id: randomUUID(), tenant_id: '11',
      timestamp: '2026-11-01T00:00:00.000Z' });
    const latest = await queryShadowReview(db, { tenantId: '10' });
    assert.equal(latest.evaluations.length, 50);
    assert.equal(latest.summary.mismatches, 0);
    assert.equal(latest.evaluations[0].timestamp.toISOString(), '2026-10-01T00:00:54.000Z');
    const mismatches = await queryShadowReview(db, { tenantId: '10', filter: 'mismatches' });
    assert.equal(mismatches.summary.mismatches, 1);
    assert.equal(mismatches.summary.likely_mission_inspections, 1);
    assert.equal(mismatches.mismatches[0].decision_id, fixture.decision_id);
    const warningsReport = await queryShadowReview(db, { tenantId: '10', filter: 'warnings' });
    assert.equal(warningsReport.summary.likely_mission_inspections, 1);
    assert.deepEqual(warningsReport.evaluations, [fixture]);
    assert.deepEqual(warningsReport.operator_warnings, [fixture]);
    assert.equal((await queryShadowReview(db, { tenantId: "10' OR true --" })).summary.total, 0);

    // A database with only SELECT privileges is sufficient for the command.
    await db.query('CREATE ROLE shadow_reader LOGIN');
    await db.query('GRANT SELECT ON decision_shadow_events TO shadow_reader');
    const readUrl = instance.connectionString.replace('postgres@', 'shadow_reader@');
    const cli = await execFileAsync(process.execPath,
      ['scripts/reviewDecisionShadow.js', '--tenant', '10', '--mismatches', '--json'], {
        cwd: root, env: { ...process.env, DATABASE_URL: readUrl, DATABASE_SSL: 'false' },
      });
    const report = JSON.parse(cli.stdout);
    assert.equal(report.summary.likely_mission_inspections, 1);
    assert.deepEqual(report.mismatches, [fixture]);
    const warningCli = await execFileAsync(process.execPath,
      ['scripts/reviewDecisionShadow.js', '--tenant', '10', '--warnings', '--json'], {
        cwd: root, env: { ...process.env, DATABASE_URL: readUrl, DATABASE_SSL: 'false' },
      });
    const warningReport = JSON.parse(warningCli.stdout);
    assert.deepEqual(warningReport.operator_warnings, [fixture]);
    const textCli = await execFileAsync(process.execPath, ['scripts/reviewDecisionShadow.js', '--errors'], {
      cwd: root, env: { ...process.env, DATABASE_URL: readUrl, DATABASE_SSL: 'false' },
    });
    assert.match(textCli.stdout, /http_error/);

    // A real table lock exercises server-side statement timeout and pool recovery.
    const lock = await db.connect();
    try {
      await lock.query('BEGIN');
      await lock.query('LOCK TABLE decision_shadow_events IN ACCESS EXCLUSIVE MODE');
      sink.write({ ...fixture, decision_id: randomUUID() });
      await sink.drain();
      assert.equal(sink.stats().failed, 2);
    } finally { await lock.query('ROLLBACK'); lock.release(); }
    sink.write({ ...fixture, decision_id: randomUUID() });
    await sink.drain();
    assert.equal(sink.stats().pending, 0);
    assert.equal(sink.stats().failed, 2);
  });
