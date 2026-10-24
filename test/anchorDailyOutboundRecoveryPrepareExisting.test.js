'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const recovery = require('../services/anchorEmailRecovery');
const prepare = require('../services/anchorEmailRecoveryPrepareExisting');
const { clone } = require('./helpers/anchorRecoveryManifestFixture');

const { prepareExistingFixture: fixture } = require('./helpers/anchorRecoveryPrepareExistingFixture');

const options = f => ({ now: f.now, env: f.env });
const review = f => prepare.reviewExistingPreparation(f.pool, f.config, options(f));
const apply = f => prepare.prepareExistingApproved(f.pool, f.config, options(f));
function rebindArtifacts(f) {
  f.config.artifacts = Object.fromEntries(f.artifactRows.map(row => [row.specialist, { id: row.id, hash: hash(row.payload) }]));
  f.config.contributionSetHash = prepare.contributionSetHash(f.artifactRows);
  f.config.preparedRevision = prepare.preparedRevision(f.missionRow.id, f.artifactRows.map(row => row.payload));
}

test('prepare-existing import never loads DB, provider, runtime or schema bootstrap', () => {
  const run = spawnSync(process.execPath, ['-e', `
    const Module=require('node:module'), original=Module._load;
    Module._load=function(id,parent,...args) {
      if (['pg','axios','dotenv'].includes(id) || /(?:^|\\/)db$|governedOutboundAdapters|acquisitionMissionPersistence|acquisitionMissionRuntime|OutboundExecution/.test(id)) throw new Error('Forbidden dependency: '+id);
      return original.call(this,id,parent,...args);
    }; require('./services/anchorEmailRecoveryPrepareExisting');
  `], { cwd: path.resolve(__dirname, '..'), encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr); assert.equal(run.stdout, '');
  const source = fs.readFileSync(path.join(__dirname, '../services/anchorEmailRecoveryPrepareExisting.js'), 'utf8');
  assert.doesNotMatch(source, /\.prepare\(|\.route\(|\.freeze\(|runScout|enrichProspect|routeExecutionRequest|ensureAcquisition/);
});

test('read-only review derives one exact non-activatable proposal without provider authority', async () => {
  const f = fixture(); const result = await review(f);
  assert.equal(result.ready, true, JSON.stringify(result));
  assert.equal(result.frozen, false); assert.equal(result.providerAuthorized, false);
  assert.equal(result.proposedManifest.admissions.length, 1);
  assert.equal(result.proposedManifest.admissions[0].recipientEmail, f.prospect.email);
  assert.equal(result.proposedManifest.id, null); assert.equal(result.proposedManifest.approvalReference, null);
  assert.equal(result.proposedManifest.admissions[0].envelopeApprovalId, null);
  assert.throws(() => recovery.validateManifest(result.proposedManifest, options(f)));
  assert.equal(f.inserts.length, 0);
  assert.match(f.calls[0].sql, /^BEGIN.*READ ONLY/);
  assert.ok(f.calls.every(call => !/FOR SHARE|FOR UPDATE|advisory|INSERT|UPDATE |DELETE|CREATE |ALTER /.test(call.sql)));
  assert.deepEqual(f.releases, [false]);
});

test('explicit default-disabled grant freezes one envelope, item and audit while control remains closed', async () => {
  const f = fixture(); const original = clone(f.control);
  const result = await apply(f);
  assert.equal(result.frozen, true); assert.equal(result.providerAuthorized, false);
  assert.deepEqual(result.stillRequired, ['envelope_execution_approval', 'fresh_send_manifest_approval']);
  assert.equal(f.committed.length, 3); assert.deepEqual(f.control, original);
  assert.match(f.committed[0].sql, /'frozen'/);
  assert.doesNotMatch(f.committed.map(row => row.sql).join('\n'), /UPDATE|DELETE|acquisition_mission_contributions/);
  const freezeSnapshot = JSON.parse(f.committed[1].values[7]);
  assert.equal(freezeSnapshot.prospectId, f.prospect.id);
  assert.equal(freezeSnapshot.message.body, f.config.candidate.body);
  const event = JSON.parse(f.committed[2].values[5]);
  assert.equal(event.executionApprovalCreated, false); assert.equal(event.providerAuthorized, false);
  assert.equal(event.approvalReference, f.config.approvalReference);
  const locks = f.calls.filter(call => /advisory/.test(call.sql));
  assert.deepEqual(locks[0].values, [261018, 10]); assert.deepEqual(locks[1].values, [701252, 0]);
  assert.deepEqual(locks[2].values, [recovery.RECOVERY_LOCK_KEY]);
  assert.match(f.calls.find(call => /FROM anchor_email_recovery_control/.test(call.sql)).sql, /FOR UPDATE/);
  assert.ok(f.calls.every(call => call.timeout === 6000));
});

test('prepare-only config cannot enable send, use another tenant, expand cap or omit reviewed inputs', async () => {
  for (const field of Object.keys(fixture().config)) {
    const f = fixture(); delete f.config[field]; await assert.rejects(apply(f)); assert.equal(f.calls.length, 0);
  }
  for (const patch of [{ action: 'SEND' }, { tenantId: '13' }, { maxItems: 2 }, { approvalReference: '' },
    { localDay: '2026-10-16' }, { allowResearch: true }, { expiresAt: '2026-10-19T16:00:00.000Z' }]) {
    const f = fixture(); Object.assign(f.config, patch); await assert.rejects(apply(f)); assert.equal(f.calls.length, 0);
  }
  for (const mutate of [f => { f.env = {}; }, f => { f.env.ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID = 'other'; },
    f => { f.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true'; }, f => { f.env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID = 'send-grant'; },
    f => { f.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false'; }]) {
    const f = fixture(); mutate(f); await assert.rejects(apply(f), { code: 'anchor_prepare_existing_not_activated' }); assert.equal(f.calls.length, 0);
  }
});

test('missing or Oct16 artifacts are an explicit dependency, never regenerated or refreshed', async () => {
  for (const mutate of [f => { f.missionRow = null; }, f => { f.missionRow.created_at = '2026-10-16T14:00:00.000Z'; },
    f => { f.missionRow.updated_at = '2026-10-16T14:00:00.000Z'; }, f => { f.artifactRows = []; },
    f => { f.artifactRows[0].at = '2026-10-16T14:00:00.000Z'; f.artifactRows[0].payload.at = f.artifactRows[0].at; rebindArtifacts(f); },
    f => { f.artifactRows[2].payload.payload.superseded = true; rebindArtifacts(f); }]) {
    const f = fixture(); mutate(f); const result = await review(f);
    assert.equal(result.ready, false); assert.match(result.dependency, /Separate bounded research or preparation approval/);
    assert.equal(f.inserts.length, 0); assert.equal(f.committed.length, 0);
  }
});

test('copy, queue, governor, canonical revision and exact source hashes cannot change', async () => {
  for (const mutate of [f => { f.artifactRows[2].payload.payload.variants[0].body += 'changed'; },
    f => { f.config.preparedRevision = 'changed'; },
    f => { f.artifactRows[3].payload.payload.governor.outcome = 'halt'; rebindArtifacts(f); },
    f => { f.artifactRows[3].payload.payload.queue.items[0].paige.candidateId = 'different'; rebindArtifacts(f); },
    f => { f.config.candidate.body += 'unapproved'; }, f => { f.config.candidate.senderName = 'unapproved'; },
    f => { f.config.contributionSetHash = 'f'.repeat(64); }]) {
    const f = fixture(); mutate(f); const result = await review(f);
    assert.equal(result.ready, false); assert.equal(f.inserts.length, 0);
  }
});

test('current spreadsheet holds, DNC, unverified contacts, relinks and suppression remain blocking', async () => {
  for (const mutate of [f => { f.prospect.ao_outreach_review_required = true; },
    f => { f.prospect.ao_outreach_review_required = null; }, f => { delete f.prospect.ao_outreach_review_required; },
    f => { f.prospect.do_not_contact = true; }, f => { f.prospect.is_synthetic = true; },
    f => { f.prospect.email_verified = false; }, f => { f.prospect.company_id = 'relinked'; },
    f => { f.prospect.assigned_ao_id = 'operator'; }, f => { f.suppression = 'prior'; },
    f => { f.suppression = 'lifecycle'; }]) {
    const f = fixture(); mutate(f); const result = await review(f);
    assert.equal(result.ready, false); assert.equal(f.inserts.length, 0);
  }
});

test('maintenance manifest, unquarantined unknown, duplicate envelope and consumed grant block freeze', async () => {
  for (const mutate of [f => { f.control.manifest = f.manifest; }, f => { f.control.maintenance_armed = false; },
    f => { f.unresolved.push({ ...f.unresolved[0], item_id: 'another-unknown', attempt_token: null }); },
    f => { f.existingEnvelope = true; }, f => { f.existingGrant = true; }, f => { f.overlap = true; }]) {
    const f = fixture(); mutate(f); await assert.rejects(apply(f)); assert.equal(f.inserts.length, 0); assert.equal(f.committed.length, 0);
  }
});

test('prepare re-reads under locks; stale review, contention, conflict and partial write roll back', async () => {
  const f = fixture(); assert.equal((await review(f)).ready, true);
  f.prospect.ao_outreach_review_required = true;
  await assert.rejects(apply(f)); assert.equal(f.committed.length, 0);
  for (const conflict of [/INSERT INTO acquisition_outbound_envelopes/, /INSERT INTO acquisition_outbound_events/]) {
    const g = fixture(); g.conflict = conflict; await assert.rejects(apply(g));
    assert.equal(g.committed.length, 0); assert.equal(g.calls.at(-1).sql, 'ROLLBACK'); assert.deepEqual(g.releases, [true]);
  }
  const g = fixture(); g.fail = /INSERT INTO acquisition_outbound_items/;
  await assert.rejects(apply(g)); assert.equal(g.committed.length, 0); assert.equal(g.calls.at(-1).sql, 'ROLLBACK');
});

test('expiry/deactivation observed after artifact reads prevents any freeze write', async () => {
  for (const mutate of [f => { f.env.ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID = ''; }, f => { f.now.setTime(Date.parse(f.config.expiresAt)); }]) {
    const f = fixture(); f.onQuery = sql => { if (/i.id AS item_id/.test(sql)) mutate(f); };
    await assert.rejects(apply(f)); assert.equal(f.inserts.length, 0);
  }
});

test('freeze records only an explicit active tenant operator with matching durable program authority', async () => {
  for (const mutate of [f => { f.config.operatorRole = 'client'; }, f => { f.config.testMode = true; },
    f => { f.operator = null; }, f => { f.operator.active = false; }, f => { f.operator.client_id = 13; },
    f => { f.operator.role = 'manager'; }, f => { f.program.authorized_by = 'different'; }]) {
    const f = fixture(); mutate(f); await assert.rejects(apply(f)); assert.equal(f.committed.length, 0);
  }
});
