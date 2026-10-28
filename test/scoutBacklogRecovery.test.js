'use strict';
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const { spawnSync } = require('node:child_process');
const backlog = require('../services/scoutBacklogRecovery');
const { createCandidateTicketStore, TICKET_VERSION } = require('../services/scoutCandidateTickets');
const { PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
const { invocation } = require('../scripts/scout-backlog-recovery/run.cjs');

const FINGERPRINT = '22d50dc07f997a3a811368a7e8b1de5a3211df5bd555260c3d42c4e780a960b7';
const key = i => i.toString(16).padStart(64, '0');
function manifest(overrides = {}) {
  const now = Date.now();
  return { version:1, kind:'scout_backlog_recovery', approvalRef:'synthetic-backlog-1', tenantId:'10',
    programId:'program-1', missionId:'mission-1', programHash:'a'.repeat(64), sourceHash:'b'.repeat(64), fingerprint:FINGERPRINT,
    planVersion:PLAN_VERSION, ticketVersion:TICKET_VERSION, maxCandidates:2,
    candidates:[{ candidateKey:key(1), membershipRevision:'1', attempts:0 }, { candidateKey:key(2), membershipRevision:'1', attempts:0 }],
    authority:{ expectedRevision:'c'.repeat(40), quarantineDecisionHash:'d'.repeat(64), recoveryControlHash:'e'.repeat(64),
      discovery:false, places:false, enrichment:false, verification:false, preparation:false, email:false },
    window:{ notBefore:new Date(now - 1000).toISOString(), expiresAt:new Date(now + 1800000).toISOString() }, ...overrides };
}
const untouchable = { query() { throw Error('database must not be reached'); }, connect() { throw Error('database must not be reached'); } };

test('the production recovery is bound to Tenant 10 and the single verified Scout fingerprint', () => {
  assert.equal(backlog.APPROVED_FINGERPRINT, FINGERPRINT);
  assert.equal(backlog.MAX_CANDIDATES, 20);
  const valid = backlog.validateManifest(manifest());
  assert.match(valid.manifestHash, /^[a-f0-9]{64}$/);
  assert.equal(backlog.validateManifest(valid).manifestHash, valid.manifestHash, 'normalized manifest is idempotent');
  for (const [label, override, code] of [
    ['tenant 1', { tenantId:'1' }, /tenant_not_authorized/], ['numeric tenant', { tenantId:10 }, /tenant_not_authorized/],
    ['other fingerprint', { fingerprint:'f'.repeat(64) }, /fingerprint_not_authorized/],
    ['zero candidates', { maxCandidates:0, candidates:[] }, /candidate_limit/],
    ['over cap', { maxCandidates:21, candidates:Array.from({ length:21 }, (_, i) => ({ candidateKey:key(i + 1), membershipRevision:'1', attempts:0 })) }, /candidate_limit/],
    ['unsorted', { candidates:[{ candidateKey:key(2), membershipRevision:'1', attempts:0 }, { candidateKey:key(1), membershipRevision:'1', attempts:0 }] }, /candidates_invalid/],
    ['bad revision', { candidates:[{ candidateKey:key(1), membershipRevision:'0', attempts:0 }, { candidateKey:key(2), membershipRevision:'1', attempts:0 }] }, /candidates_invalid/],
    ['kind', { kind:'bounded_places_research' }, /manifest_invalid/],
    ['enrichment right', { authority:{ ...manifest().authority, enrichment:true } }, /authority_invalid/],
    ['verification right', { authority:{ ...manifest().authority, verification:true } }, /authority_invalid/],
    ['preparation right', { authority:{ ...manifest().authority, preparation:true } }, /authority_invalid/],
    ['non-UTC window', { window:{ notBefore:'2026-10-27T10:00:00+01:00', expiresAt:'2026-10-27T10:30:00+01:00' } }, /window_invalid/],
    ['inverted window', { window:{ notBefore:'2026-10-27T10:30:00Z', expiresAt:'2026-10-27T10:00:00Z' } }, /window_invalid/],
  ]) assert.throws(() => backlog.validateManifest(manifest(override)), code, label);
});

test('validation and confirmation failures never reach the database', async () => {
  await assert.rejects(backlog.previewBacklogRecovery(untouchable, manifest({ tenantId:'2' })), /tenant_not_authorized/);
  await assert.rejects(backlog.installBacklogRecovery(untouchable, manifest(), '0'.repeat(64)), /exact_confirmation/);
  await assert.rejects(backlog.revokeBacklogRecovery(untouchable, manifest(), '0'.repeat(64)), /exact_confirmation/);
  await assert.rejects(backlog.executeBacklogRecovery({ pool:untouchable, manifest:manifest(), confirmationHash:'0'.repeat(64) }), /exact_confirmation/);
  const approved = manifest(), hash = backlog.validateManifest(approved).manifestHash;
  await assert.rejects(backlog.executeBacklogRecovery({ pool:{ ...untouchable, options:{ max:1 } }, manifest:approved, confirmationHash:hash }), /dependencies_required/);
});

test('claimDue exact-key restriction is validated before any database work', async () => {
  const pool = { query() { throw Error('database must not be reached'); }, connect() { throw Error('database must not be reached'); } };
  const store = createCandidateTicketStore({ pool, tenantId:'10', fingerprint:FINGERPRINT });
  for (const candidateKeys of [[], ['not-hex'], [key(1), key(1)], 'abc', Array.from({ length:3 }, (_, i) => key(i + 1))]) {
    await assert.rejects(store.claimDue({ dependencyKey:'dep', limit:2, candidateKeys }), /candidate key restriction/);
  }
  await assert.rejects(store.claimDue({ dependencyKey:'dep', limit:1, candidateKeys:[key(1)], db:{} }), /claim transaction/);
});

test('CLI requires an explicit mode and exact confirmation only for mutations', () => {
  assert.deepEqual(invocation(['--preview', 'm.json']), { mode:'--preview', file:'m.json', confirmHash:undefined });
  assert.equal(invocation(['--execute-approved', '-', '--confirm', 'a'.repeat(64)]).confirmHash, 'a'.repeat(64));
  for (const args of [[], ['--execute-approved', 'm.json'], ['--install-approved', 'm.json'], ['--preview', 'm.json', '--confirm', 'a'.repeat(64)],
    ['--execute-approved', 'm.json', '--confirm', 'short'], ['--run', 'm.json'], ['--preview', '--receipt']]) {
    assert.throws(() => invocation(args), /explicit_invocation_required/, JSON.stringify(args));
  }
});

test('CLI stops with a fixed reason before connecting when the manifest is outside the approved scope', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-backlog-cli-')); t.after(() => fs.rmSync(dir, { recursive:true, force:true }));
  const file = path.join(dir, 'manifest.json');
  fs.writeFileSync(file, JSON.stringify(manifest({ fingerprint:'f'.repeat(64) })));
  const cli = path.join(__dirname, '../scripts/scout-backlog-recovery/run.cjs');
  const env = { ...process.env, DATABASE_URL:'postgresql://127.0.0.1:1/never-connect', DATABASE_SSL:'false' };
  const run = args => spawnSync(process.execPath, ['--require', path.join(__dirname, 'helpers/denyExternalProviders.cjs'), cli, ...args],
    { env, encoding:'utf8', timeout:30000 });
  const scoped = run(['--preview', file]);
  assert.equal(scoped.status, 1); assert.equal(scoped.stdout, '');
  assert.match(scoped.stderr, /Scout backlog recovery stopped\..*Reason: backlog_recovery_fingerprint_not_authorized\./);
  const unconfirmed = run(['--execute-approved', file]);
  assert.equal(unconfirmed.status, 1); assert.match(unconfirmed.stderr, /Reason: backlog_recovery_explicit_invocation_required\./);
});
