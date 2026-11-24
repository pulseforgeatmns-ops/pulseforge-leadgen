'use strict';
// P0 safety invariant (R8): a prospect with ao_outreach_review_required = true
// never becomes setter-visible through recomputation, handoff, scout, manual or
// engagement paths, for any tenant. Legitimate visibility for prospects without
// the review requirement is unchanged, and no setter path clears the flag.
// Disposable PostgreSQL only; all other network is denied.
let allowedPort = null;
const deny = name => function () { throw new Error(`Unexpected network: ${name}`); };
for (const protocol of ['http', 'https']) { const t = require(`node:${protocol}`); t.request = deny(`${protocol}.request`); t.get = deny(`${protocol}.get`); }
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('fetch');
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || (['127.0.0.1', 'localhost', '::1', undefined].includes(o.host) && Number(o.port) === allowedPort);
  if (!local) throw new Error('Unexpected network: net.connect');
  return originalConnect.apply(this, args);
};

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  computeSetterVisible, recomputeSetterVisibility, setSetterVisibility, outreachReviewHeld,
} = require('../utils/setterVisibility');
const { importReviewOnlyContacts } = require('../services/reviewOnlyProspectImportService');

const TENANTS = Object.freeze([21, 10, 1]);

test('pure gate: the review hold excludes every tenant and every reason', () => {
  const base = { status: 'cold', icp_score: 95, service_area_match: 'Somewhere', do_not_contact: false };
  for (const client_id of [...TENANTS, 5, 2]) {
    for (const reason of ['scout', 'handoff', 'recompute', 'manual', 'engagement']) {
      assert.equal(computeSetterVisible({ ...base, client_id, ao_outreach_review_required: true }, { reason }), false, `${client_id}/${reason}`);
    }
    assert.equal(computeSetterVisible({ ...base, client_id, ao_outreach_review_required: true, setter_visible: true }, { reason: 'stage_change', stageStatus: 'booked' }), false);
  }
  // Without the hold, behaviour is unchanged.
  assert.equal(computeSetterVisible({ ...base, client_id: 21, ao_outreach_review_required: false }, { reason: 'handoff' }), true);
  assert.equal(computeSetterVisible({ ...base, client_id: 1 }, { reason: 'scout' }), true, 'missing flag on a non-Anchor tenant is not a hold');
  assert.equal(computeSetterVisible({ ...base, client_id: 1, icp_score: 69 }, { reason: 'scout' }), false);
  assert.equal(computeSetterVisible({ ...base, client_id: 1, icp_score: 10 }, { reason: 'manual' }), true, 'manual override still works without a hold');
  // Tenant 10 keeps its stricter rule: unknown means held.
  assert.equal(outreachReviewHeld({ client_id: 10 }), true);
  assert.equal(outreachReviewHeld({ client_id: 10, ao_outreach_review_required: false }), false);
  assert.equal(outreachReviewHeld({ client_id: 21 }), false);
});

const SCHEMA = `
  CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, name TEXT, location TEXT);
  CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, company_id UUID REFERENCES companies(id),
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT, linkedin_url TEXT, source TEXT, icp_score INTEGER,
    status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT, setter_visible BOOLEAN DEFAULT false,
    setter_updated_at TIMESTAMPTZ, setter_visibility_reason TEXT, do_not_contact BOOLEAN DEFAULT false, ao_outreach_review_required BOOLEAN DEFAULT false,
    discovery_method TEXT, acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW());`;

async function setup(t) {
  const pg = await startDisposablePostgres('setter-review-hold-');
  allowedPort = Number(pg.port);
  const db = new Pool({ connectionString: pg.connectionString, max: 6 });
  db.on('error', () => {});
  t.after(async () => { await db.end().catch(() => {}); await pg.stop(); });
  await db.query(SCHEMA);
  return db;
}

async function insert(db, { clientId, label, hold, icp = 85, area = 'Service Area', visible = false, reason = null, status = 'cold', source = 'scout' }) {
  const company = (await db.query('INSERT INTO companies(client_id, name) VALUES ($1, $2) RETURNING id', [clientId, `${label} Co`])).rows[0].id;
  const { rows } = await db.query(`INSERT INTO prospects(client_id, company_id, first_name, source, icp_score, status, service_area_match,
      setter_visible, ao_outreach_review_required) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
  [clientId, company, label, source, icp, status, area, visible, hold]);
  if (reason) await db.query('UPDATE prospects SET setter_visibility_reason = $2 WHERE id = $1', [rows[0].id, reason]);
  return rows[0].id;
}

const read = async (db, id) => (await db.query(
  'SELECT setter_visible, setter_visibility_reason, ao_outreach_review_required FROM prospects WHERE id = $1', [id])).rows[0];

test('recompute/handoff: held prospects stay hidden in tenants 21, 10 and 1; unheld prospects keep current behaviour', async t => {
  const db = await setup(t);
  const ids = {};
  for (const clientId of TENANTS) {
    ids[clientId] = {
      held: await insert(db, { clientId, label: `held-${clientId}`, hold: true, icp: 95 }),
      heldVisibleManual: await insert(db, { clientId, label: `held-manual-${clientId}`, hold: true, visible: true, reason: 'manual' }),
      heldVisibleEngaged: await insert(db, { clientId, label: `held-engaged-${clientId}`, hold: true, visible: true, reason: 'engagement' }),
      clear: await insert(db, { clientId, label: `clear-${clientId}`, hold: false, icp: 85 }),
      clearLow: await insert(db, { clientId, label: `low-${clientId}`, hold: false, icp: 65 }),
      clearNoArea: await insert(db, { clientId, label: `noarea-${clientId}`, hold: false, icp: 90, area: ' ' }),
      clearManual: await insert(db, { clientId, label: `manual-${clientId}`, hold: false, icp: 20, visible: true, reason: 'manual' }),
    };
  }
  for (const clientId of TENANTS) {
    await recomputeSetterVisibility(db, { clientId, reason: 'handoff', preserveOverrides: true });
  }
  // A global recompute (clientId null) must not expose anything either.
  await recomputeSetterVisibility(db, { clientId: null, reason: 'recompute', preserveOverrides: true });

  for (const clientId of TENANTS) {
    const r = ids[clientId];
    for (const key of ['held', 'heldVisibleManual', 'heldVisibleEngaged']) {
      const row = await read(db, r[key]);
      assert.equal(row.setter_visible, false, `${clientId}/${key} hidden`);
      assert.equal(row.setter_visibility_reason, 'hard_exclusion', `${clientId}/${key} reason`);
      assert.equal(row.ao_outreach_review_required, true, `${clientId}/${key} hold never cleared by a setter path`);
    }
    assert.equal((await read(db, r.clear)).setter_visible, true, `${clientId} unheld qualified row stays visible`);
    assert.equal((await read(db, r.clearLow)).setter_visible, false, `${clientId} below threshold stays hidden`);
    assert.equal((await read(db, r.clearNoArea)).setter_visible, false, `${clientId} blank area stays hidden`);
    const manual = await read(db, r.clearManual);
    assert.equal(manual.setter_visible, true, `${clientId} manual override without a hold is preserved`);
    assert.equal(manual.setter_visibility_reason, 'manual');
  }
});

test('tenant 10 keeps its stricter rules (unknown flag and held sibling)', async t => {
  const db = await setup(t);
  const unknown = await insert(db, { clientId: 10, label: 'unknown-10', hold: null });
  const sibling = await insert(db, { clientId: 10, label: 'sibling-10', hold: false });
  const company = (await db.query('SELECT company_id FROM prospects WHERE id = $1', [sibling])).rows[0].company_id;
  await db.query(`INSERT INTO prospects(client_id, company_id, first_name, source, icp_score, status, service_area_match, ao_outreach_review_required)
    VALUES (10, $1, 'held-peer', 'scout', 90, 'cold', 'Area', true)`, [company]);
  const unknownOther = await insert(db, { clientId: 1, label: 'unknown-1', hold: null });
  await recomputeSetterVisibility(db, { clientId: null, reason: 'handoff' });
  assert.equal((await read(db, unknown)).setter_visible, false, 'tenant 10 unknown is held');
  assert.equal((await read(db, sibling)).setter_visible, false, 'tenant 10 held sibling holds the company');
  assert.equal((await read(db, unknownOther)).setter_visible, true, 'outside tenant 10 an unknown flag is not a hold (unchanged)');
});

test('per-row paths (scout, manual, engagement, stage_change) cannot expose a held prospect', async t => {
  const db = await setup(t);
  for (const clientId of TENANTS) {
    for (const reason of ['scout', 'handoff', 'manual', 'engagement']) {
      const id = await insert(db, { clientId, label: `${reason}-${clientId}`, hold: true, icp: 99 });
      const row = await setSetterVisibility(db, id, { reason, clientId });
      assert.equal(row.setter_visible, false, `${clientId}/${reason}`);
      assert.equal(row.setter_visibility_reason, 'hard_exclusion');
      assert.equal(row.ao_outreach_review_required, true, 'flag untouched');
    }
    const staged = await insert(db, { clientId, label: `stage-${clientId}`, hold: true, visible: true, reason: 'manual' });
    const row = await setSetterVisibility(db, staged, { reason: 'stage_change', stageStatus: 'booked', clientId });
    assert.equal(row.setter_visible, false, `${clientId} stage change hides a held visible row`);
    // Unheld rows: manual still works, scout still promotes on score.
    const ok = await insert(db, { clientId, label: `ok-${clientId}`, hold: false, icp: 20 });
    assert.equal((await setSetterVisibility(db, ok, { reason: 'manual', clientId })).setter_visible, true);
    const scout = await insert(db, { clientId, label: `scout-ok-${clientId}`, hold: false, icp: 80 });
    assert.equal((await setSetterVisibility(db, scout, { reason: 'scout', clientId })).setter_visible, true);
  }
});

test('review-only contact_now imports (icp 80) stay hidden for tenants 21 and 1 after handoff', async t => {
  const db = await setup(t);
  for (const clientId of [21, 1]) {
    await importReviewOnlyContacts([{ name: `Review ${clientId} Person`, company: `Review ${clientId} Co`, reviewState: 'contact_now',
      market: 'Example County', sourceUrl: 'https://example.invalid/r' }], { clientId, db, actorId: 'test' });
    await recomputeSetterVisibility(db, { clientId, reason: 'handoff' });
    const row = (await db.query('SELECT icp_score, setter_visible, ao_outreach_review_required FROM prospects WHERE client_id = $1', [clientId])).rows[0];
    assert.equal(row.icp_score, 80);
    assert.equal(row.ao_outreach_review_required, true);
    assert.equal(row.setter_visible, false);
  }
});
