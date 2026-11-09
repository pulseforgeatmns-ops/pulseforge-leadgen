'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const http = require('node:http');
const { getMaxOperationalReadiness } = require('../services/maxOperationalReadiness');
const { createMaxOperationalReadinessRouter } = require('../routes/maxOperationalReadiness');
const { recordObserverHealth, resetObserverHealth } = require('../packages/max/observability/ObserverHealth');

function database() {
  return { query: async sql => {
    if (sql.includes("to_regclass('public.decision_shadow_events')")) return { rows: [{
      decision_shadow_events: true, decision_shadow_evidence: true,
      max_interpretation_shadow: true, max_conversation_context: true,
      anchor_email_recovery_control: true, acquisition_outbound_programs: true,
    }] };
    if (sql.includes('information_schema.columns')) {
      const { TABLES } = require('../packages/decision-service/shadowSchema');
      return { rows: Object.entries(TABLES).flatMap(([table, fields]) => fields.map(column => ({ table_name: table, column_name: column, data_type: 'text' }))) };
    }
    if (sql.includes('FROM decision_shadow_events')) return { rows: [{ n: 4, last_at: '2026-11-09T03:00:00.000Z' }] };
    if (sql.includes('FROM decision_shadow_evidence')) return { rows: [{ n: 2, last_at: '2026-11-09T03:01:00.000Z' }] };
    if (sql.includes('FROM max_interpretation_shadow')) return { rows: [{ n: 3, last_at: '2026-11-09T03:02:00.000Z' }] };
    if (sql.includes('FROM max_conversation_context')) return { rows: [{ n: 1, last_at: '2026-11-09T05:00:00.000Z' }] };
    if (sql.includes("to_regclass('public.anchor_email_recovery_control')")) return { rows: [{ control_table: 'anchor_email_recovery_control' }] };
    if (sql.includes('SELECT maintenance_armed FROM anchor_email_recovery_control')) return { rows: [{ maintenance_armed: true }] };
    if (sql.includes('FROM anchor_email_recovery_control')) return { rows: [{ singleton: true, maintenance_armed: true, manifest: null }] };
    if (sql.includes('FROM acquisition_outbound_programs')) return { rows: [{ mode: 'paused', authorized_at: '2026-11-01T00:00:00.000Z', expires_at: '2026-11-02T00:00:00.000Z' }] };
    throw new Error(`unexpected query: ${sql}`);
  } };
}

test('readiness exposes booleans, bounded health and aggregate persistence only', async () => {
  resetObserverHealth();
  recordObserverHealth('semantic_shadow', 'failure', { reason: 'observer_failed' });
  recordObserverHealth('semantic_shadow', 'dropped', { pending: 16 });
  const result = await getMaxOperationalReadiness({
    db: database(),
    env: {
      DECISION_SHADOW_ENABLED: 'true', DECISION_PROVIDER: 'jev', JEV_ENABLED: 'true',
      MAX_INTERPRETATION_SHADOW: 'true', MAX_CONVERSATION_CONTEXT: 'true',
      ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true', DATABASE_URL: 'must-not-appear',
    },
  });
  assert.equal(result.readOnly, true);
  assert.equal(result.jev.schema.ready, true);
  assert.equal(result.semanticShadow.activeRouting, false);
  assert.deepEqual(result.conversationContext.authenticatedRecoveryCapability, {
    implementedInDeployedCode: false, scope: 'deployed_code', configurationIndependent: true,
  });
  assert.equal(result.observerHealth.scope, 'process');
  assert.equal(result.observerHealth.durableHistory, false);
  assert.ok(result.observerHealth.processStartedAt);
  assert.equal(result.observerHealth.observers.semantic_shadow.failed, 1);
  assert.equal(result.observerHealth.observers.semantic_shadow.dropped, 1);
  assert.equal(result.outbound.verified, true);
  assert.equal(result.outbound.status, 'verified');
  assert.equal(result.outbound.recoveryKillSwitchEffective, true);
  assert.equal(result.outbound.persistedMaintenanceArmed, true);
  assert.deepEqual(result.outbound.authorization, { mode: 'paused', authorizedAt: '2026-11-01T00:00:00.000Z', expiresAt: '2026-11-02T00:00:00.000Z' });
  assert.doesNotMatch(JSON.stringify(result), /must-not-appear|customer@example/i);
});

test('failed outbound gate reads are explicitly unknown and never look safely verified', async () => {
  const db = database();
  const query = db.query;
  db.query = async sql => {
    if (sql.includes('FROM anchor_email_recovery_control')) throw new Error('unavailable');
    return query(sql);
  };
  const result = await getMaxOperationalReadiness({ db, env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } });
  assert.equal(result.outbound.verified, false);
  assert.equal(result.outbound.status, 'unknown');
  assert.equal(result.outbound.mode, null);
  assert.equal(result.outbound.reason, 'gate_state_unverified');
  assert.equal(result.outbound.recoveryKillSwitchEffective, null);
  assert.equal(result.outbound.persistedMaintenanceArmed, null);
});

function appFor(user, readiness) {
  const app = express();
  app.use((req, _res, next) => { if (user) req.session = { user }; next(); });
  app.use(createMaxOperationalReadinessRouter({ db: database(), readiness }));
  return app;
}

async function get(app) {
  const server = await new Promise(resolve => {
    const listening = app.listen(0, '127.0.0.1', () => resolve(listening));
  });
  try {
    return await new Promise((resolve, reject) => {
      const req = http.get({ hostname: '127.0.0.1', port: server.address().port, path: '/api/v1/max/operational-readiness' }, res => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(body) }));
      });
      req.on('error', reject);
    });
  } finally { await new Promise(resolve => server.close(resolve)); }
}

test('readiness route is authenticated, role restricted, no-store and fail closed', async () => {
  assert.equal((await get(appFor(null, async () => ({})))).status, 401);
  assert.equal((await get(appFor({ role: 'viewer' }, async () => ({})))).status, 403);
  const ok = await get(appFor({ role: 'admin' }, async () => ({ readOnly: true })));
  assert.equal(ok.status, 200);
  assert.equal(ok.headers['cache-control'], 'no-store');
  assert.equal(ok.body.readOnly, true);
  const failed = await get(appFor({ role: 'manager' }, async () => { throw Error('secret'); }));
  assert.equal(failed.status, 503);
  assert.deepEqual(failed.body, { error: 'max_operational_readiness_unavailable', readOnly: true });
});
