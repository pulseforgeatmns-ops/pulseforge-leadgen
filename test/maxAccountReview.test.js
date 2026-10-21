'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createAccountReviewRouter } = require('../routes/accountReview');
const { publicError, errorStatus } = require('../services/maxSpreadsheetService');
const { create } = require('../public/shared/spreadsheetReview');

function request(router, session, { aoId = 24, tenantId, id } = {}) {
  return new Promise((resolve, reject) => {
    const url = `/api/v1/max/account-review${id ? `/${id}` : ''}`;
    const req = { method: 'GET', url, originalUrl: url, path: url, query: { ao_id: aoId, ...(tenantId ? { client_id: tenantId } : {}) }, session, get: () => 'application/json' };
    const res = { statusCode: 200, status(code) { this.statusCode = code; return this; }, set() { return this; }, json(body) { resolve({ status: this.statusCode, body }); } };
    router.handle(req, res, error => error ? reject(error) : reject(new Error('unhandled route')));
  });
}
function fixture() {
  const calls = [];
  const users = [{ id: 1, role: 'admin', client_id: 10, active: true }, { id: 24, role: 'ao', client_id: 10, active: true }, { id: 20, role: 'ao', client_id: 10, active: true }];
  const db = { query: async (sql, params) => {
    calls.push({ sql, params });
    assert.doesNotMatch(sql, /max_spreadsheet|INSERT|UPDATE|DELETE|CREATE|ALTER/);
    if (sql.includes('ANY')) return { rows: users.filter(u => params[0].includes(u.id)) };
    return { rows: users.filter(u => u.id === params[0] && u.client_id === params[1] && u.role === 'ao') };
  } };
  const accounts = [{ prospect_id: 'account-1', company_name: 'A property manager', assigned_ao_id: '24', assigned_ao_name: 'Tony', next_action: 'call' }];
  const router = createAccountReviewRouter({ db,
    listAccounts: async input => { calls.push(input); assert.deepEqual(input.filters, { ao_owner_id: 24 }); assert.equal(input.clientId, 10); return { accounts, summary: { total: 1 }, date: '2026-10-19' }; },
    getAccount: async input => { calls.push(input); assert.equal(input.aoUserId, 24); assert.equal(input.clientId, 10); return { summary: { company_name: 'A property manager', assigned_ao_id: 24 }, contact: { name: 'A contact' }, history: [] }; },
  });
  return { router, calls, accounts };
}
const admin = { user: { id: 1, role: 'admin', client_id: 10 }, active_client_id: 10 };

test('assigned account review works without spreadsheet proposal tables or save approval configuration', async () => {
  const { router, calls } = fixture();
  const result = await request(router, structuredClone(admin));
  assert.equal(result.status, 200); assert.equal(result.body.ao_id, 24); assert.equal(result.body.tenant_id, 10);
  assert.equal(result.body.ao_name, 'Tony'); assert.equal(result.body.accounts.length, 1);
  assert.ok(calls.length > 1);
  const detail = await request(router, structuredClone(admin), { id: 'account-1' });
  assert.equal(detail.status, 200); assert.equal(detail.body.account.contact.name, 'A contact');
});

test('roles, tenant changes, AO ownership and revoked access remain enforced', async () => {
  const { router } = fixture();
  assert.equal((await request(router, {})).status, 401);
  assert.equal((await request(router, { user: { id: 1, role: 'viewer' }, active_client_id: 10 })).status, 403);
  assert.equal((await request(router, structuredClone(admin), { tenantId: 11 })).status, 403);
  assert.equal((await request(router, structuredClone(admin), { aoId: 999 })).status, 403);
  assert.equal((await request(router, { user: { id: 20, role: 'ao', client_id: 10 }, active_client_id: 10 })).status, 403);
  assert.equal((await request(router, { user: { id: 999, role: 'admin' }, active_client_id: 10 })).status, 403);
});

test('wrong-owner details or rows cannot leak into a selected AO review', async () => {
  const { accounts, router } = fixture(); accounts[0].assigned_ao_id = '20';
  const mismatch = await request(router, structuredClone(admin));
  assert.equal(mismatch.status, 500); assert.equal(mismatch.body.accounts, undefined);
  const detailRouter = createAccountReviewRouter({ db: {}, resolveScope: async () => ({ clientId: 10, aoId: 24 }), getAccount: async () => ({ summary: { assigned_ao_id: 20 }, contact: { name: 'Must not leak' } }) });
  const result = await request(detailRouter, structuredClone(admin), { id: 'wrong-owner' });
  assert.equal(result.status, 404); assert.doesNotMatch(JSON.stringify(result.body), /Must not leak/);
});

test('database diagnostics are sanitized while unknown-save recovery remains explicit', () => {
  for (const code of ['42P01', '42703', '55P03', '57014', 'SPREADSHEET_DB_TIMEOUT']) {
    const error = Object.assign(new Error('relation private_table missing SELECT private_column'), { code });
    const body = publicError(error);
    assert.equal(errorStatus(error), 503); assert.match(body.message, /still review assigned accounts/);
    assert.doesNotMatch(JSON.stringify(body), /42P01|private_table|private_column|SELECT/);
  }
  assert.deepEqual(publicError({ commitOutcomeUnknown: true }), { ok: false, error: 'spreadsheet_save_unconfirmed', message: 'The save result has not been confirmed. Retry the same approval to recover its result.', outcome: 'unknown', retry_same_request: true });
});

test('account review compatibility entry point delegates text without frontend owner parsing', async t => {
  const requests = [], directOpens = [];
  const previous = globalThis.PulseforgeReviewNavigation;
  globalThis.PulseforgeReviewNavigation = { open: async (intent, question) => { requests.push({ intent, question }); return true; } };
  t.after(() => { globalThis.PulseforgeReviewNavigation = previous; });
  const review = create({ host: null, scopeHost: null, onReviewAccounts: owner => directOpens.push(owner), fetch: async () => { throw new Error('Frontend must not resolve a name from a roster'); } });
  for (const text of ["Review Tony's accounts", 'Show me Tony’s accounts', 'Open accounts assigned to Tony']) {
    assert.equal(await review.handleAccountReview(text), true);
  }
  assert.equal(requests.length, 3); assert.ok(requests.every(item => item.intent === null));
  assert.equal(directOpens.length, 0);
});
