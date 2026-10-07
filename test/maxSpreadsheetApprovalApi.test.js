'use strict';

// Isolated HTTP integration: real router, authorization, session cookies, parser,
// planner and PostgreSQL store. Never load server.js or the production db module.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const express = require('express');
const session = require('express-session');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const fixture = fs.readFileSync(path.join(__dirname, 'fixtures/anchor-cleaning-actual.xlsx'));
const sourceHash = 'b1cddfa475f244e27d8c81a381976b30911b34f53ea6c449f868a54208ba6a75';
const schema = fs.readFileSync(path.join(__dirname, 'fixtures/maxSpreadsheetBaseSchema.sql'), 'utf8');

test('authenticated spreadsheet API safety gate with actual workbook and disposable PostgreSQL', { timeout: 120000 }, async t => {
  assert.equal(createHash('sha256').update(fixture).digest('hex'), sourceHash);
  const instance = await startDisposablePostgres('spreadsheet-api-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const db = new Pool({ connectionString: instance.connectionString });
  let server;
  const originalApprover = process.env.MAX_SPREADSHEET_APPROVER_USER_ID;
  process.env.MAX_SPREADSHEET_APPROVER_USER_ID = '11';
  const restoredModules = [];
  function substitute(relative, exports) {
    const filename = require.resolve(relative);
    restoredModules.push([filename, require.cache[filename]]);
    require.cache[filename] = { id: filename, filename, loaded: true, exports };
  }
  t.after(async () => {
    if (server) await new Promise(resolve => server.close(resolve));
    await db.end();
    await instance.stop();
    for (const [filename, original] of restoredModules.reverse()) {
      if (original) require.cache[filename] = original; else delete require.cache[filename];
    }
    if (originalApprover === undefined) delete process.env.MAX_SPREADSHEET_APPROVER_USER_ID;
    else process.env.MAX_SPREADSHEET_APPROVER_USER_ID = originalApprover;
  });
  await db.query(schema);
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-07-max-spreadsheet-reliability.sql'), 'utf8'));
  await db.query(`INSERT INTO clients VALUES(1),(2);
    INSERT INTO users(id,client_id,name,role) VALUES
    (10,1,'Test AO','ao'),(11,1,'Test Jake','admin'),(12,1,'Other AO','ao'),
    (13,1,'Other admin','admin'),(20,2,'Other tenant','ao');`);
  const { extract } = require('../packages/max/composer/adapters/spreadsheet');
  const parsed = await extract({ filename: 'Anchor Cleaning Prospect List.xlsx' }, { buffer: fixture });
  for (const row of parsed.structuredData.sheets[0].rows) {
    const company = await db.query('INSERT INTO companies(client_id,name) VALUES(1,$1) RETURNING id', [row.values.company]);
    await db.query(`INSERT INTO prospects(client_id,company_id,assigned_ao_id,phone,ao_last_touch_at)
      VALUES(1,$1,10,$2,'2026-09-01Z')`, [company.rows[0].id, row.values.phone]);
  }

  substitute('../db', db);
  const forbidden = () => { throw new Error('Unexpected generic ingestion or external execution'); };
  substitute('../services/maxComposerService', { submitMaxComposerTurn: forbidden });
  substitute('../services/maxStateIngestionService', { ingestOperationalEvidence: forbidden, ingestSpreadsheetEvidence: forbidden, listOverdueExpectationPrompts: forbidden });
  substitute('../services/maxDecisionExecutionService', { afterIngestionDecisions: forbidden });
  substitute('../services/aoImpersonationService', { logImpersonationAction: forbidden });
  substitute('../services/maxVoiceService', { uploadAndTranscribeVoice: forbidden, retryTranscription: forbidden, createVoiceTranscriptionAdapter: forbidden });
  const app = express();
  app.use(express.json({ limit: '1mb' }));
  app.use(session({ secret: randomUUID(), resave: false, saveUninitialized: false, cookie: { httpOnly: true } }));
  // Test-only login uses server-owned fixture principals, not supplied role/tenant.
  app.post('/test/session/:id', async (req, res) => {
    const user = (await db.query('SELECT * FROM users WHERE id=$1', [Number(req.params.id)])).rows[0];
    if (!user) return res.sendStatus(401);
    req.session.user = user;
    req.session.active_client_id = user.client_id;
    res.json({ ok: true });
  });
  app.get('/test/review', (req, res) => res.type('html').send('<!doctype html><html><head><link rel="icon" href="data:,"></head><body><div id="scope"></div><div id="proposal"></div><div id="messages"></div><script src="/test/review.js"></script></body></html>'));
  app.get('/test/review.js', (req, res) => res.type('js').send(fs.readFileSync(path.join(__dirname, '../public/shared/spreadsheetReview.js'))));
  app.use(require('../routes/maxStateIngestion'));
  server = await new Promise(resolve => { const listener = app.listen(0, '127.0.0.1', () => resolve(listener)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  async function request(url, { cookie, body, method = 'POST' } = {}) {
    const response = await fetch(base + url, { method, headers: { 'content-type': 'application/json', ...(cookie ? { cookie } : {}) }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    return { status: response.status, body: await response.json(), cookie: response.headers.get('set-cookie')?.split(';')[0] };
  }
  const jake = (await request('/test/session/11')).cookie;
  const ao = (await request('/test/session/10')).cookie;
  const otherAo = (await request('/test/session/12')).cookie;
  const otherAdmin = (await request('/test/session/13')).cookie;
  const otherTenant = (await request('/test/session/20')).cookie;
  const body = () => ({ client_id: 1, ao_id: 10, conversation_id: randomUUID(), text: 'Compare only. Do not save.', confirm: true,
    attachments: [{ id: randomUUID(), type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx', content_base64: fixture.toString('base64') }] });
  const business = async () => {
    const tables = ['companies', 'prospects', 'ao_prospect_activity', 'max_spreadsheet_contacts', 'ao_prospect_tasks', 'max_ao_follow_up_tasks', 'touchpoints', 'tenant_outreach_suppressions', 'max_spreadsheet_suppressions', 'max_spreadsheet_relationships', 'max_spreadsheet_effects', 'prospect_notes', 'prospect_lifecycle_events', 'ao_leads', 'ao_contacts', 'ao_follow_up_tasks'];
    return Promise.all(tables.map(async table => ({ table, rows: (await db.query(`SELECT * FROM ${table} ORDER BY 1`)).rows })));
  };
  let proposal, approval;
  const commit = overrides => request(`/api/v1/max/spreadsheet/proposals/${proposal.id}/commit`, { cookie: jake, body: { ...approval, ...overrides } });

  await t.test('unauthenticated, spoofed tenant and AO identities are rejected', async () => {
    assert.equal((await request('/api/v1/max/composer', { body: body() })).status, 401);
    for (const [cookie, overrides, expected] of [[ao, { client_id: 2 }, 'tenant_scope_mismatch'], [ao, { ao_id: 12 }, 'ao_scope_mismatch'], [jake, { ao_id: 20 }, 'ao_scope_not_authorized'], [otherTenant, {}, 'tenant_scope_mismatch']]) {
      const result = await request('/api/v1/max/composer', { cookie, body: { ...body(), ...overrides, actor: { userId: 11, role: 'admin' } } });
      assert.equal(result.status, 403); assert.equal(result.body.error, expected);
    }
  });
  await t.test('actual source preview reads CRM with zero business writes despite confirm=true', async () => {
    const before = await business();
    const result = await request('/api/v1/max/composer', { cookie: jake, body: body() });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    assert.equal(result.body.committed, false); assert.equal(result.body.preview_only, true);
    proposal = result.body.spreadsheet_proposal;
    assert.equal(proposal.sourceHash, sourceHash); assert.equal(proposal.plan.rows.length, 12);
    assert.ok(proposal.plan.rows.some(row => row.accountResolution.status === 'matched'));
    assert.deepEqual(await business(), before);
    const selected = proposal.plan.operations.find(operation => operation.type === 'ADD_NOTE' && !operation.blocked);
    assert.ok(selected, 'The real fixture must produce at least one safe historical note proposal');
    approval = { text: 'Approve selected operations', client_id: 1, ao_id: 10, proposal_digest: proposal.digest, source_hash: proposal.sourceHash, conversation_id: proposal.conversationId, operation_ids: [selected.id], idempotency_key: randomUUID() };
  });
  await t.test('approval fails closed for missing config, wrong authenticated principal and revoked role', async () => {
    const before = await business();
    delete process.env.MAX_SPREADSHEET_APPROVER_USER_ID;
    assert.equal((await commit()).body.error, 'jake_approval_required');
    process.env.MAX_SPREADSHEET_APPROVER_USER_ID = '10';
    const configuredAo = await request(`/api/v1/max/spreadsheet/proposals/${proposal.id}/commit`, { cookie: ao, body: approval });
    assert.equal(configuredAo.status, 403); assert.equal(configuredAo.body.error, 'jake_approval_required');
    process.env.MAX_SPREADSHEET_APPROVER_USER_ID = '11';
    for (const cookie of [ao, otherAdmin]) {
      const result = await request(`/api/v1/max/spreadsheet/proposals/${proposal.id}/commit`, { cookie, body: { ...approval, approved_by: 11, can_approve: true, actor: { id: 11, role: 'admin' } } });
      assert.equal(result.status, 403); assert.equal(result.body.error, 'jake_approval_required');
    }
    await db.query('UPDATE users SET active=false WHERE id=11');
    assert.equal((await commit()).body.error, 'actor_access_revoked');
    await db.query('UPDATE users SET active=true WHERE id=11');
    assert.deepEqual(await business(), before);
  });
  await t.test('negative text, missing bindings, client plans and invalid selections cannot persist', async () => {
    const before = await business();
    for (const text of ['Do not save those updates yet', 'Preview before you save', 'Do not save', 'Hold', 'Never apply']) {
      const result = await commit({ text, confirm: true });
      assert.equal(result.status, 409); assert.equal(result.body.error, 'explicit_approval_required');
    }
    for (const [overrides, error] of [[{ plan: { operations: [] } }, 'client_proposal_not_accepted'], [{ operation_ids: [] }, 'exact_operations_required'], [{ operation_ids: [approval.operation_ids[0], approval.operation_ids[0]] }, 'exact_operations_required'], [{ source_hash: null }, 'approval_binding_required']]) {
      assert.equal((await commit(overrides)).body.error, error);
    }
    for (const [overrides, error] of [[{ proposal_digest: 'tampered' }, 'PROPOSAL_DIGEST_MISMATCH'], [{ source_hash: 'a'.repeat(64) }, 'PROPOSAL_DIGEST_MISMATCH'], [{ conversation_id: 'other' }, 'conversation_scope_mismatch'], [{ operation_ids: ['invented'] }, 'UNAPPROVABLE_OPERATION']]) {
      const result = await commit(overrides); assert.notEqual(result.status, 200); assert.equal(result.body.error, error);
    }
    assert.deepEqual(await business(), before);
  });
  await t.test('legacy and browser-owned pending plans cannot reach generic ingestion', async () => {
    const before = await business();
    assert.equal((await request('/api/v1/max/ingest/spreadsheet', { cookie: jake, body: { client_id: 1, confirm: true } })).status, 410);
    const result = await request('/api/v1/max/composer', { cookie: jake, body: { client_id: 1, text: 'save', confirm: true, conversation_memory: { pendingSpreadsheetWorkbook: { reconciliation_plan: proposal.plan } } } });
    assert.equal(result.status, 409); assert.equal(result.body.error, 'server_proposal_required');
    assert.deepEqual(await business(), before);
  });
  await t.test('text-only exact selected approval saves once and returns verified replay receipt', async () => {
    const prospects = (await db.query('SELECT * FROM prospects ORDER BY id')).rows;
    const result = await commit();
    assert.equal(result.status, 200, JSON.stringify(result.body));
    assert.equal(result.body.committed, true); assert.equal(result.body.spreadsheet_commit.results.length, 1);
    assert.equal(result.body.spreadsheet_commit.results[0].status, 'verified');
    assert.equal((await db.query('SELECT count(*) FROM ao_prospect_activity')).rows[0].count, '1');
    assert.deepEqual((await db.query('SELECT * FROM prospects ORDER BY id')).rows, prospects);
    const after = await business();
    const replay = await commit(); assert.equal(replay.status, 200); assert.equal(replay.body.spreadsheet_commit.replayed, true);
    assert.deepEqual(await business(), after);
  });
  await t.test('CRM changes invalidate a previously reviewed exact approval without partial effects', async () => {
    const result = await request('/api/v1/max/composer', { cookie: jake, body: body() });
    assert.equal(result.status, 200);
    const fresh = result.body.spreadsheet_proposal;
    const selected = fresh.plan.operations.find(operation => operation.type === 'ADD_NOTE' && !operation.blocked);
    assert.ok(selected);
    const account = (await db.query('SELECT id, phone FROM prospects ORDER BY id LIMIT 1')).rows[0];
    await db.query("UPDATE prospects SET phone='603-999-9999' WHERE id=$1", [account.id]);
    const before = await business();
    const rejected = await request(`/api/v1/max/spreadsheet/proposals/${fresh.id}/commit`, { cookie: jake, body: {
      ...approval, proposal_digest: fresh.digest, conversation_id: fresh.conversationId,
      operation_ids: [selected.id], idempotency_key: randomUUID(),
    } });
    assert.notEqual(rejected.status, 200); assert.equal(rejected.body.error, 'STALE_PROPOSAL');
    assert.deepEqual(await business(), before);
    assert.equal((await db.query('SELECT status FROM max_spreadsheet_proposals WHERE id=$1', [fresh.id])).rows[0].status, 'pending');
    await db.query('UPDATE prospects SET phone=$1 WHERE id=$2', [account.phone, account.id]);
  });
  await t.test('missing bytes and snapshot failure do not create partial proposals or business writes', async () => {
    const before = await business();
    const input = body(); delete input.attachments[0].content_base64;
    const missing = await request('/api/v1/max/composer', { cookie: jake, body: input });
    assert.equal(missing.status, 422); assert.equal(missing.body.error, 'spreadsheet_bytes_required');
    const count = (await db.query('SELECT count(*) FROM max_spreadsheet_proposals')).rows[0].count;
    await db.query('ALTER TABLE max_spreadsheet_contacts RENAME TO unavailable_contacts');
    try {
      const failed = await request('/api/v1/max/composer', { cookie: jake, body: body() });
      assert.equal(failed.status, 503);
      assert.equal((await db.query('SELECT count(*) FROM max_spreadsheet_proposals')).rows[0].count, count);
    } finally { await db.query('ALTER TABLE unavailable_contacts RENAME TO max_spreadsheet_contacts'); }
    assert.deepEqual(await business(), before);
  });
  await t.test('Tony proposal can be recovered and approved by designated Jake, never another AO/admin', async () => {
    const created = await request('/api/v1/max/composer', { cookie: ao, body: body() });
    assert.equal(created.status, 200); assert.equal(created.body.can_approve, false);
    const owned = created.body.spreadsheet_proposal;
    assert.equal(owned.actorId, 10);
    const query = `?client_id=1&ao_id=10&conversation_id=${owned.conversationId}`;
    for (const cookie of [ao, jake]) {
      const recovered = await request(`/api/v1/max/spreadsheet/proposals/${owned.id}${query}`, { cookie, method: 'GET' });
      assert.equal(recovered.status, 200); assert.deepEqual(recovered.body.spreadsheet_proposal.plan, owned.plan);
      assert.equal(recovered.body.can_approve, cookie === jake);
      const listed = await request('/api/v1/max/spreadsheet/proposals?client_id=1&ao_id=10', { cookie, method: 'GET' });
      assert.equal(listed.status, 200); assert.ok(listed.body.proposals.some(item => item.id === owned.id));
    }
    for (const cookie of [otherAo, otherAdmin, otherTenant]) {
      const denied = await request(`/api/v1/max/spreadsheet/proposals/${owned.id}${query}`, { cookie, method: 'GET' });
      assert.ok([403, 404].includes(denied.status));
    }
    const selected = owned.plan.operations.find(operation => operation.type === 'ADD_NOTE' && !operation.blocked);
    assert.ok(selected);
    const payload = { ...approval, proposal_digest: owned.digest, conversation_id: owned.conversationId,
      operation_ids: [selected.id], idempotency_key: randomUUID() };
    assert.equal((await request(`/api/v1/max/spreadsheet/proposals/${owned.id}/commit`, { cookie: ao, body: payload })).status, 403);
    const saved = await request(`/api/v1/max/spreadsheet/proposals/${owned.id}/commit`, { cookie: jake, body: payload });
    assert.equal(saved.status, 200, JSON.stringify(saved.body));
    assert.equal(saved.body.spreadsheet_commit.approvedBy, 11);
    assert.equal(saved.body.spreadsheet_proposal.actorId, 10);
    const persisted = (await db.query('SELECT actor_id,approved_by FROM max_spreadsheet_proposals WHERE id=$1', [owned.id])).rows[0];
    assert.deepEqual(persisted, { actor_id: 10, approved_by: 11 });
  });
  await t.test('source-bound identity resolution supersedes immutable plan without writes; approval orders new account/contact dependencies', async () => {
    // Deliberately absent account in this disposable baseline exercises the real
    // workbook's new-account path; never remove or change any external CRM data.
    const removed = await db.query("DELETE FROM prospects WHERE company_id IN (SELECT id FROM companies WHERE name='Grappone Ford') RETURNING company_id");
    await db.query('DELETE FROM companies WHERE id=$1', [removed.rows[0].company_id]);
    const created = await request('/api/v1/max/composer', { cookie: ao, body: body() });
    assert.equal(created.status, 200);
    const prior = created.body.spreadsheet_proposal;
    const resolveBody = { client_id: 1, ao_id: 10, conversation_id: prior.conversationId,
      proposal_digest: prior.digest, source_hash: prior.sourceHash,
      resolutions: [{ sourceHash, sheet: 'Sheet1', rowNumber: 14, createAccount: true,
        contacts: [{ sourceName: 'Mike', create: true }], identityEvidence: 'Test-only verified identity: Grappone Ford at source address and the separately confirmed Mike contact.' }] };
    const before = await business();
    const endpoint = `/api/v1/max/spreadsheet/proposals/${prior.id}/resolve`;
    assert.equal((await request(endpoint, { cookie: ao, body: resolveBody })).status, 403);
    assert.equal((await request(endpoint, { cookie: jake, body: { ...resolveBody, resolutions: [{ ...resolveBody.resolutions[0], sourceHash: '0'.repeat(64) }] } })).status, 400);
    const result = await request(endpoint, { cookie: jake, body: resolveBody });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    assert.equal(result.body.committed, false); assert.deepEqual(await business(), before);
    const resolved = result.body.spreadsheet_proposal;
    assert.notEqual(resolved.id, prior.id); assert.notEqual(resolved.digest, prior.digest); assert.equal(resolved.actorId, 10);
    const old = (await db.query('SELECT plan,status FROM max_spreadsheet_proposals WHERE id=$1', [prior.id])).rows[0];
    assert.deepEqual(old.plan, prior.plan); assert.equal(old.status, 'superseded');
    const operations = resolved.plan.rows.find(row => row.rowNumber === 14).operations;
    const create = operations.find(operation => operation.type === 'CREATE_ACCOUNT');
    const contact = operations.find(operation => operation.type === 'ADD_CONTACT');
    assert.ok(create && contact); assert.equal(create.blocked, false); assert.equal(contact.blocked, false);
    assert.ok(contact.dependsOn.includes(create.id));
    const payload = { ...approval, source_hash: resolved.sourceHash, proposal_digest: resolved.digest,
      conversation_id: resolved.conversationId, operation_ids: [contact.id, create.id], idempotency_key: randomUUID() };
    const missingDependency = await request(`/api/v1/max/spreadsheet/proposals/${resolved.id}/commit`, { cookie: jake, body: { ...payload, operation_ids: [contact.id] } });
    assert.equal(missingDependency.status, 422); assert.equal(missingDependency.body.error, 'MISSING_OPERATION_DEPENDENCY');
    assert.deepEqual(await business(), before);
    const saved = await request(`/api/v1/max/spreadsheet/proposals/${resolved.id}/commit`, { cookie: jake, body: payload });
    assert.equal(saved.status, 200, JSON.stringify(saved.body));
    assert.deepEqual(saved.body.spreadsheet_commit.results.map(item => item.operationId), [create.id, contact.id]);
    assert.equal((await db.query('SELECT count(*) FROM prospects WHERE id=$1 AND client_id=1 AND assigned_ao_id=10', [create.target.accountId])).rows[0].count, '1');
    assert.equal((await db.query('SELECT data FROM max_spreadsheet_contacts WHERE prospect_id=$1', [create.target.accountId])).rows[0].data.name, 'Mike');
    const after = await business();
    const stale = await request(`/api/v1/max/spreadsheet/proposals/${prior.id}/commit`, { cookie: jake, body: { ...payload, proposal_digest: prior.digest, operation_ids: [prior.plan.operations.find(operation => !operation.blocked).id], idempotency_key: randomUUID() } });
    assert.notEqual(stale.status, 200); assert.equal(stale.body.error, 'PROPOSAL_NOT_PENDING');
    assert.deepEqual(await business(), after);
  });
  await t.test('lost COMMIT response reports unknown outcome and exact retry recovers durable receipt', async () => {
    const created = await request('/api/v1/max/composer', { cookie: ao, body: body() });
    assert.equal(created.status, 200);
    const pending = created.body.spreadsheet_proposal;
    const operation = pending.plan.operations.find(item => item.type === 'ADD_NOTE' && !item.blocked && !item.dependsOn?.length);
    assert.ok(operation);
    const payload = { ...approval, proposal_digest: pending.digest, source_hash: pending.sourceHash,
      conversation_id: pending.conversationId, operation_ids: [operation.id], idempotency_key: randomUUID() };
    const { PostgresSpreadsheetProposalStore } = require('../packages/max/stateIngestion/spreadsheetProposalStore');
    const original = PostgresSpreadsheetProposalStore.prototype.commitProposal;
    let injected = false;
    PostgresSpreadsheetProposalStore.prototype.commitProposal = async function(args) {
      const realDb = this.db;
      this.db = { async connect() {
        const client = await realDb.connect();
        return { release: client.release.bind(client), async query(sql, values) {
          const result = await client.query(sql, values);
          if ((typeof sql === 'string' ? sql : sql.text) === 'COMMIT' && !injected) {
            injected = true; throw Object.assign(new Error('Simulated lost commit response'), { code: 'ECONNRESET' });
          }
          return result;
        } };
      } };
      try { return await original.call(this, args); } finally { this.db = realDb; }
    };
    let uncertain;
    try { uncertain = await request(`/api/v1/max/spreadsheet/proposals/${pending.id}/commit`, { cookie: jake, body: payload }); }
    finally { PostgresSpreadsheetProposalStore.prototype.commitProposal = original; }
    assert.equal(injected, true); assert.equal(uncertain.status, 503);
    assert.equal(uncertain.body.outcome, 'unknown'); assert.equal(uncertain.body.retry_same_request, true);
    const persisted = await business();
    const retry = await request(`/api/v1/max/spreadsheet/proposals/${pending.id}/commit`, { cookie: jake, body: payload });
    assert.equal(retry.status, 200); assert.equal(retry.body.spreadsheet_commit.replayed, true);
    assert.deepEqual(retry.body.spreadsheet_commit.selectedOperationIds, [operation.id]);
    assert.deepEqual(await business(), persisted);
  });
  await t.test('Chromium uses actual authenticated API and PostgreSQL: resume, select, negative-save, approve, readback and reload', {
    skip: process.env.MAX_SPREADSHEET_BROWSER_TEST !== '1', timeout: 30000,
  }, async browserTest => {
    const created = await request('/api/v1/max/composer', { cookie: ao, body: body() });
    assert.equal(created.status, 200);
    const target = created.body.spreadsheet_proposal;
    const selected = target.plan.operations.find(operation => operation.type === 'ADD_NOTE' && !operation.blocked && !operation.dependsOn?.length);
    assert.ok(selected, 'Fixture has a remaining selectable source note');
    const before = await business();
    const puppeteer = require('puppeteer');
    assert.ok(fs.existsSync(puppeteer.executablePath()), 'The pinned Chromium executable is required for this gate');
    const browser = await puppeteer.launch({ headless: true, args: ['--disable-background-networking'] });
    browserTest.after(() => browser.close());
    const page = await browser.newPage();
    const unexpected = [], commits = [];
    await page.setRequestInterception(true);
    page.on('request', browserRequest => {
      if (!browserRequest.url().startsWith(base + '/')) {
        unexpected.push(browserRequest.url()); browserRequest.abort(); return;
      }
      if (browserRequest.method() === 'POST' && browserRequest.url().endsWith('/commit')) commits.push(JSON.parse(browserRequest.postData()));
      browserRequest.continue();
    });
    await page.goto(`${base}/test/review`);
    // Real session middleware creates the signed HttpOnly cookie in Chromium.
    await page.evaluate(async () => {
      const login = await fetch('/test/session/11', { method: 'POST', headers: { 'content-type': 'application/json' } });
      if (!login.ok) throw new Error('Test principal login failed');
    });
    async function openServerProposal() {
      await page.evaluate(async () => {
        window.review = PulseforgeSpreadsheetReview.create({ fetch: (...args) => fetch(...args),
          host: document.getElementById('proposal'), scopeHost: document.getElementById('scope'),
          onMessage: message => { document.getElementById('messages').textContent = message; } });
        await review.loadScope();
      });
      await page.select('[data-spreadsheet-ao]', '10');
      await page.click('[data-spreadsheet-refresh]');
      await page.waitForSelector('[data-resume-choice]');
      const choice = await page.$eval('[data-resume-choice]', (element, id) => [...element.options].find(option => option.textContent.includes(id))?.value, target.id);
      assert.notEqual(choice, undefined);
      await page.select('[data-resume-choice]', choice);
      await page.click('[data-resume-load]');
      await page.waitForFunction(id => document.getElementById('proposal').textContent.includes(id), {}, target.id);
    }
    await openServerProposal();
    assert.deepEqual(await business(), before, 'Opening the server-owned proposal makes no business changes');
    await page.evaluate(operationId => {
      const remove = [...document.querySelectorAll('[data-spreadsheet-operation]:checked')]
        .map(input => input.getAttribute('data-spreadsheet-operation')).filter(id => id !== operationId);
      for (const id of remove) {
        const input = document.querySelector(`[data-spreadsheet-operation="${id}"]`);
        input.checked = false; input.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }, selected.id);
    assert.deepEqual(await page.$$eval('[data-spreadsheet-operation]:checked', inputs => inputs.map(input => input.getAttribute('data-spreadsheet-operation'))), [selected.id]);
    await page.evaluate(() => review.handleText('Do not save those updates yet'));
    assert.equal(commits.length, 0); assert.deepEqual(await business(), before);
    await page.click('[data-spreadsheet-save]');
    await page.waitForFunction(() => document.getElementById('messages').textContent.includes('committed and verified'));
    assert.equal(commits.length, 1); assert.deepEqual(commits[0].operation_ids, [selected.id]);
    assert.equal(commits[0].proposal_digest, target.digest); assert.equal(commits[0].plan, undefined);
    const persisted = (await db.query('SELECT actor_id,approved_by,status,receipt FROM max_spreadsheet_proposals WHERE id=$1', [target.id])).rows[0];
    assert.equal(persisted.actor_id, 10); assert.equal(persisted.approved_by, 11); assert.equal(persisted.status, 'committed');
    assert.deepEqual(persisted.receipt.selectedOperationIds, [selected.id]);
    assert.equal(persisted.receipt.results[0].status, 'verified');
    assert.equal((await db.query('SELECT notes FROM ao_prospect_activity WHERE id=$1', [persisted.receipt.results[0].observed.id])).rows[0].notes, selected.after.text);
    const after = await business();
    for (const entry of before.filter(entry => !['ao_prospect_activity', 'max_spreadsheet_effects'].includes(entry.table))) {
      assert.deepEqual(after.find(item => item.table === entry.table), entry, `${entry.table} must remain unchanged`);
    }
    assert.equal(await page.$('[data-spreadsheet-save]'), null);
    await page.reload();
    await openServerProposal();
    await page.waitForFunction(() => document.getElementById('proposal').textContent.includes('Saved result recovered'));
    assert.equal(await page.$('[data-spreadsheet-save]'), null);
    assert.equal(commits.length, 1); assert.deepEqual(await business(), after);
    assert.deepEqual(unexpected, []);
  });
});
