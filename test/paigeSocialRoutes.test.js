'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const { createPaigeSocialRouter } = require('../routes/paigeSocial');
const { fixture } = require('./helpers/paigeSocialFixture');

test('HTTP review requires session role, rejects tenant/account changes, binds displayed content, and keeps publish separate', async () => {
  const f = await fixture(); const app = express(); app.use(express.json());
  app.use((req, _res, next) => { if (req.headers['x-test-role']) req.session = { user: { id: 7, role: req.headers['x-test-role'], client_id: 10 } }; next(); });
  app.use(createPaigeSocialRouter({ store: f.store, approval: f.approval, accounts: () => [f.account], delegate: {}, publish: async i => {
    const r = await f.publish(i); return { success: r.status === 'completed', published: r.outputs?.published, error: r.errors?.[0]?.message };
  } }));
  const server = await new Promise((resolve, reject) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); s.on('error', reject); });
  const base = `http://127.0.0.1:${server.address().port}/api/paige/social`;
  const request = (suffix, body, role='manager') => fetch(base + suffix, { headers: { 'Content-Type': 'application/json', ...(role ? { 'x-test-role': role } : {}) }, ...(body ? { method: 'POST', body: JSON.stringify(body) } : {}) });
  try {
    assert.equal((await request('?client_id=10', null, null)).status, 401);
    assert.equal((await request('?client_id=10', null, 'viewer')).status, 403);
    assert.equal((await request('?client_id=11')).status, 403);
    assert.equal((await request('?client_id=garbage')).status, 400);
    const prefix = `/${f.artifact.id}`;
    assert.equal((await request(`${prefix}/publish?client_id=10`, {})).status, 409);
    const preview = await (await request(`${prefix}/preview?client_id=10`)).json();
    assert.equal(preview.artifact.body, f.artifact.body);
    assert.equal((await request(`${prefix}/decision?client_id=10`, { decision: 'approve', expectedApprovalHash: 'wrong', accountId: f.account.id })).status, 409);
    const approved = await request(`${prefix}/decision?client_id=10`, { decision: 'approve', expectedApprovalHash: preview.approvalHash, accountId: f.account.id, approvedBy: 'spoofed' });
    assert.equal(approved.status, 200); assert.equal((await f.current()).approvalBinding.approvedBy, 'operator:7');
    assert.equal(f.counts().sends, 0);
    assert.equal((await request(`${prefix}/publish?client_id=10`, {})).status, 200);
    assert.equal(f.counts().sends, 1);
  } finally { await new Promise(resolve => server.close(resolve)); }
});

test('existing outcome and learning routes reject cross-client manager overrides', async () => {
  const app = express(); app.use(express.json());
  app.use((req, _res, next) => { req.session = { user: { id: 7, role: 'manager', client_id: 10 } }; next(); });
  app.use(require('../routes/contentOutcomeIntelligence')); app.use(require('../routes/contentLearning'));
  const server = await new Promise((resolve, reject) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); s.on('error', reject); });
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const url of ['/api/content-publications/x?client_id=11', '/api/content-learnings?client_id=11']) {
      assert.equal((await fetch(base + url)).status, 403);
    }
    for (const url of ['/api/content-publications', '/api/content-learning/evaluate/x']) {
      assert.equal((await fetch(base + url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clientId: 11 }) })).status, 403);
    }
  } finally { await new Promise(r => server.close(r)); }
});
