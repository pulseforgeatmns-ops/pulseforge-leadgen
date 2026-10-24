'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const express = require('express');
const { Pool } = require('pg');
const { createAssessmentRouter } = require('../routes/substralAssessment');
const { captureAssessmentRequest } = require('../lib/substralAssessmentIntake');
const url = process.env.SUBSTRAL_TEST_DATABASE_URL;

test('real PostgreSQL intake, durable retries, operator routing and fail states', { skip: !url }, async (t) => {
  const schema = `substral_qa_${Date.now()}`;
  const admin = new Pool({ connectionString: url });
  await admin.query(`CREATE SCHEMA ${schema}`);
  const db = new Pool({ connectionString: url, options: `-c search_path=${schema},public` });
  const previousTenant = process.env.STUDIO_SUBSTRAL_CLIENT_ID;
  process.env.STUDIO_SUBSTRAL_CLIENT_ID = '1';
  let server;
  try {
    await db.query(`CREATE TABLE clients(id integer primary key); INSERT INTO clients VALUES(1),(7);
      CREATE TABLE agent_actions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(), created_by text,
      action_type text NOT NULL, title text NOT NULL, description text NOT NULL, payload jsonb,
      status text DEFAULT 'pending', client_id integer REFERENCES clients(id), created_at timestamptz DEFAULT now());`);
    const migration = fs.readFileSync(path.join(__dirname, '../migrations/2026-09-29-substral-assessment-idempotency.sql'), 'utf8');
    await db.query(migration); await db.query(migration);
    const app = express();
    app.use(createAssessmentRouter({ db }));
    server = app.listen(0, '127.0.0.1');
    await new Promise(resolve => server.once('listening', resolve));
    const endpoint = `http://127.0.0.1:${server.address().port}/api/public/website-assessment`;
    const post = (body, origin = 'https://studiosubstral.com', extra = {}) => fetch(endpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin, ...extra }, body: JSON.stringify(body),
    });
    const payload = { domain: 'https://www.example.com/about', email: 'QA@example.com', context: 'Human review smoke test', request_key: randomUUID() };
    let reference;
    await t.test('preflight permits only the canonical site and www', async () => {
      for (const origin of ['https://studiosubstral.com', 'https://www.studiosubstral.com']) {
        const res = await fetch(endpoint, { method: 'OPTIONS', headers: { Origin: origin, 'Access-Control-Request-Method': 'POST' } });
        assert.equal(res.status, 204); assert.equal(res.headers.get('access-control-allow-origin'), origin);
      }
      assert.equal((await post(payload, 'https://unrelated.example')).status, 403);
    });
    await t.test('invalid payloads and honeypot never create requests', async () => {
      for (const body of [null, [], {}, { domain: 'localhost', email: 'bad' }, { ...payload, company_website: 'bot' }, { ...payload, request_key: 'bad' }]) {
        assert.equal((await post(body)).status, 400);
      }
      assert.equal((await db.query('SELECT count(*)::int n FROM agent_actions')).rows[0].n, 0);
      assert.equal((await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{' })).status, 400);
      assert.equal((await post({ ...payload, context: 'x'.repeat(9000) })).status, 413);
      assert.equal((await fetch(endpoint, { method: 'POST', body: 'plain text' })).status, 415);
    });
    await t.test('confirmation follows a committed queue notification in tenant 1', async () => {
      const res = await post(payload); const body = await res.json();
      assert.equal(res.status, 201); assert.equal(body.review_mode, 'human');
      assert.equal(res.headers.get('cache-control'), 'no-store');
      assert.match(body.message, /A person will review/); reference = body.request_id;
      const rows = await db.query("SELECT * FROM agent_actions WHERE id = $1 AND client_id = 1 AND status IN ('pending','in_progress')", [reference]);
      assert.equal(rows.rows.length, 1);
      assert.equal(rows.rows[0].payload.reply_to, 'qa@example.com');
      assert.equal(rows.rows[0].payload.stated_context, payload.context);
      assert.equal(rows.rows[0].payload.stage, 'requested');
      assert.equal(rows.rows[0].payload.review_mode, 'human');
      assert.equal((await db.query('SELECT id FROM agent_actions WHERE id=$1 AND client_id=7', [reference])).rowCount, 0);
    });
    await t.test('lost-response retry and router restart return the same request', async () => {
      const retry = await post(payload); assert.equal(retry.status, 200);
      assert.equal((await retry.json()).request_id, reference);
      // Persistence is independent of the router or its in-memory limiter.
      const stored = await captureAssessmentRequest(db, { domain: 'example.com', email: 'qa@example.com', context: payload.context, request_key: payload.request_key });
      assert.equal(stored.id, reference); assert.equal(stored.duplicate, true);
    });
    await t.test('concurrent retries produce exactly one row', async () => {
      const values = { domain: 'concurrent.example', email: 'owner@concurrent.example', context: null, request_key: randomUUID() };
      const result = await Promise.all(Array.from({ length: 8 }, () => captureAssessmentRequest(db, values)));
      assert.equal(new Set(result.map(x => x.id)).size, 1);
      assert.equal(result.filter(x => !x.duplicate).length, 1);
    });
    await t.test('a reused key cannot confirm changed data', async () => {
      assert.equal((await post({ ...payload, email: 'different@example.com' })).status, 409);
    });
    await t.test('native form POST is accessible, escaped and does not expose details in the URL', async () => {
      const res = await fetch(endpoint, { method: 'POST', headers: { Origin: 'https://studiosubstral.com', Accept: 'text/html' }, body: new URLSearchParams({ domain: 'native.example', email: 'native@example.com' }) });
      assert.equal(res.status, 201); assert.match(res.headers.get('content-type'), /text\/html/);
      const html = await res.text(); assert.match(html, /Request received/); assert.match(html, /human review/);
      assert.equal(res.url, endpoint); assert.match(res.headers.get('x-robots-tag'), /noindex/);
    });
    await t.test('rate limit is not bypassed with spoofed forwarding headers', async () => {
      let res;
      for (let i = 0; i < 7; i++) res = await post({ ...payload, email: 'rate@example.com', request_key: randomUUID() }, 'https://studiosubstral.com', { 'X-Forwarded-For': `192.0.2.${i}` });
      assert.equal(res.status, 429); assert.equal(res.headers.get('retry-after'), '3600');
    });
    await t.test('missing database migration fails without a false confirmation', async () => {
      await db.query('DROP INDEX substral_assessment_request_key');
      const res = await post({ ...payload, email: 'unavailable@example.com', request_key: randomUUID() });
      assert.equal(res.status, 503); assert.equal((await res.json()).ok, undefined);
    });
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    await db.end(); await admin.query(`DROP SCHEMA ${schema} CASCADE`); await admin.end();
    if (previousTenant === undefined) delete process.env.STUDIO_SUBSTRAL_CLIENT_ID;
    else process.env.STUDIO_SUBSTRAL_CLIENT_ID = previousTenant;
  }
});
