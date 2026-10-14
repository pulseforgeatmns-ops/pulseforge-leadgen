'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { applyScoutEnrichment, babrunScoutContactEvidence } = require('../utils/babrunScoutContactEvidence');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { runScoutWaterfall, loadScoutSave, recordingDb, verificationReceipt, leadFixture, ownerFixture } = require('./helpers/babrunScoutIntake');
const policy = { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] };

async function savedRow(lead, verification = verificationReceipt, clientId = 13) {
  const db = recordingDb();
  const intake = loadScoutSave({ db, clientId, verify: async () => verification });
  assert.equal((await intake.save([lead])).saved, 1);
  return db.rows[0];
}

test('actual standard Scout waterfall, projection and INSERT preserve the selected owner receipt', async () => {
  const { lead, calls } = await runScoutWaterfall(leadFixture());
  assert.deepEqual(calls, ['prospeo', 'hunter']);
  const row = await savedRow(lead);
  assert.equal(row.first_name, 'Ada'); assert.equal(row.last_name, 'Lovelace'); assert.equal(row.job_title, 'Owner');
  const r = row.acquisition_metadata.contactResolution;
  assert.equal(r.bestEmail, row.email); assert.equal(r.finalState, 'VERIFIED_FOUNDER_EMAIL');
  assert.equal(r.discoveryMethod, 'hunter'); assert.equal(r.discoverySource, ownerFixture().sourceUrl);
  assert.equal(row.enrichment_provenance.email.source, 'hunter');
  assert.equal(row.enrichment_provenance.email.source_url, ownerFixture().sourceUrl);
  assert.equal(row.enrichment_provenance.email.email, row.email);
  assert.deepEqual(r.verification.raw, row.verifier_response);
  assert.equal(r.verification.verifiedAt, row.verified_at);
  assert.equal(r.verification.checkedAt, row.verifier_checked_at);
  assert.equal(governedContactReason(row, policy), null);
});

test('mixed discovery sources cannot lend the selected person a different owner identity', async t => {
  for (const selected of [
    { email: 'grace@fixturepainting.example', contact: 'Grace Hopper', title: 'Office Manager' },
    { email: 'info@fixturepainting.example', contact: '', title: '' },
    { email: 'ada@fixturepainting.example' },
    { email: 'grace@fixturepainting.example', contact: 'Ada Lovelace', title: 'Owner' },
  ]) await t.test(selected.email + ':' + (selected.title || 'unnamed'), async () => {
    const lead = { ...leadFixture(), ...ownerFixture(), source: ['google_places', 'hunter'] };
    const result = await runScoutWaterfall(lead, { prospeo: selected });
    const row = await savedRow(result.lead);
    assert.notEqual(governedContactReason(row, policy), null);
    assert.equal(row.enrichment_provenance.email.source, 'prospeo');
    assert.equal(row.enrichment_provenance.email.source_url, null, 'never inherit a previous candidate URL');
    assert.notEqual(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
    if (!selected.contact) { assert.equal(row.first_name, null); assert.equal(row.job_title, null); }
  });
});

test('unsupported, ambiguous, pattern, changed-address and changed-company evidence stays held', async t => {
  const variants = [
    ['missing receipt', lead => { delete lead.scoutContactEvidence; }],
    ['unsupported source with first-party URL', lead => { lead.scoutContactEvidence.source = 'directory'; }],
    ['mixed source string', lead => { lead.scoutContactEvidence.source = 'hunter+pattern_candidate'; }],
    ['pattern only', lead => { lead.scoutContactEvidence.source = 'pattern_candidate'; }],
    ['changed selected address', lead => { lead.email = 'grace@fixturepainting.example'; }],
    ['changed company', lead => { lead.scoutContactEvidence.companyDomain = 'other.example'; }],
    ['unnamed', lead => { lead.scoutContactEvidence.contactName = null; }],
    ['generic contact', lead => { lead.scoutContactEvidence.contactName = 'Owner'; }],
    ['missing title', lead => { lead.scoutContactEvidence.contactTitle = null; }],
    ['vice president', lead => { lead.scoutContactEvidence.contactTitle = 'Vice President'; }],
    ['assistant to owner', lead => { lead.scoutContactEvidence.contactTitle = 'Assistant to Owner'; }],
    ['past founder', lead => { lead.scoutContactEvidence.contactTitle = 'Former Founder'; }],
    ['unattributed scraped name', lead => { lead.scoutContactEvidence.source = 'scraped'; lead.scoutContactEvidence.contactTitle = ''; }],
    ['foreign scrape URL', lead => { lead.scoutContactEvidence.source = 'scraped'; lead.scoutContactEvidence.sourceUrl = 'https://directory.example/about'; }],
  ];
  for (const [name, modify] of variants) await t.test(name, async () => {
    const { lead } = await runScoutWaterfall(leadFixture()); modify(lead);
    const row = await savedRow(lead);
    assert.notEqual(governedContactReason(row, policy), null);
    assert.notEqual(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
  });
});

test('verification cannot supply identity or override non-valid status', async t => {
  for (const status of ['unknown', 'risky', 'catchall', 'invalid']) {
    for (const emailVerified of [false, true]) await t.test(`${status}/${emailVerified}`, async () => {
      const { lead } = await runScoutWaterfall(leadFixture());
      const row = await savedRow(lead, { ...verificationReceipt, emailStatus: status, emailVerified });
      assert.notEqual(governedContactReason(row, policy), null);
      assert.notEqual(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
    });
  }
  const { lead } = await runScoutWaterfall(leadFixture());
  const row = await savedRow(lead, { ...verificationReceipt, doNotContact: true });
  assert.equal(governedContactReason(row, policy), 'do_not_contact');
  row.do_not_contact = false; row.email = 'grace@fixturepainting.example';
  assert.equal(governedContactReason(row, policy), 'contact_classification_email_changed');
  row.client_id = 10;
  assert.equal(governedContactReason(row, policy), 'contact_tenant_mismatch');
});

test('explicit verification rejection wins over contradictory verified/valid flags', async () => {
  const { lead } = await runScoutWaterfall(leadFixture());
  const row = await savedRow(lead, { ...verificationReceipt, reject: true, rejectReason: 'rejected_fixture' });
  const receipt = row.acquisition_metadata.contactResolution.verification;
  assert.equal(receipt.rejected, true); assert.equal(receipt.verified, false);
  assert.equal(receipt.reason, 'rejected_fixture');
  assert.notEqual(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
  assert.notEqual(governedContactReason(row, policy), null);
});

test('duplicate and INSERT conflict paths never overwrite existing contact evidence', async () => {
  for (const mode of [{ duplicate: true }, { conflict: true }]) {
    const db = recordingDb(mode); let verifications = 0;
    const intake = loadScoutSave({ db, verify: async () => { verifications++; return verificationReceipt; } });
    const { lead } = await runScoutWaterfall(leadFixture());
    const result = await intake.save([lead]);
    assert.equal(result.saved, 0); assert.equal(result.skipped, 1);
    assert.equal(intake.writes.length, 0);
    assert.equal(db.calls.filter(c => /UPDATE prospects/.test(c.sql)).length, 0);
    assert.equal(verifications, mode.duplicate ? 0 : 1);
  }
});

test('other tenant standard projections and INSERT columns retain their exact prior shape', async () => {
  for (const clientId of [1, 10, 17]) {
    const { lead } = await runScoutWaterfall(leadFixture(), { clientId });
    assert.equal(Object.hasOwn(lead, 'scoutContactEvidence'), false);
    assert.equal(Object.hasOwn(lead, 'sourceUrl'), false);
    const row = await savedRow(lead, verificationReceipt, clientId);
    assert.equal(Object.keys(row).length, 29);
    for (const field of ['job_title', 'acquisition_metadata', 'enrichment_provenance']) assert.equal(Object.hasOwn(row, field), false);
    assert.equal(row.first_name, 'Ada'); assert.equal(row.last_name, 'Lovelace');
  }
});

test('capture helper preserves other tenant mutation semantics and never uses lead identity for Babrun', () => {
  for (const clientId of [1, 10, 17, 13]) {
    const original = { ...leadFixture(), ...ownerFixture(), source: ['google', 'hunter'] };
    const selected = { email: 'other@fixturepainting.example', source: ['provider_observed'] };
    const expected = { ...original, ...selected, source: ['provider_observed', 'prospeo'] };
    const actual = applyScoutEnrichment(original, selected, 'prospeo', clientId, 'fixturepainting.example');
    if (clientId === 13) {
      assert.equal(actual.scoutContactEvidence.contactName, null);
      assert.equal(actual.scoutContactEvidence.contactTitle, null);
      assert.equal(actual.scoutContactEvidence.sourceUrl, null);
      delete actual.scoutContactEvidence;
    }
    assert.deepEqual(actual, expected);
  }
});

test('new intake module is side-effect-free and promotion uses the identical shared classifier', () => {
  const script = `const Module=require('node:module'),old=Module._load; Module._load=function(id,...rest){
    if (/dotenv|axios|db$|emailVerifier|tieredEnrichment|leadgen|promoteUnenriched/.test(id)) throw new Error('Forbidden import: '+id);
    return old.call(this,id,...rest); }; require('./utils/babrunScoutContactEvidence');`;
  const result = spawnSync(process.execPath, ['-e', script], { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const shared = require('../utils/babrunContactClassification');
  const legacy = require('../scripts/lib/babrunContactResolution');
  for (const key of ['classifyCandidate', 'mapVerificationResult', 'candidateRecord', 'CONTACT_FINAL_STATE']) assert.equal(legacy[key], shared[key]);
  const source = fs.readFileSync(path.join(__dirname, '../scripts/promoteUnenriched.js'), 'utf8');
  assert.ok(source.includes("require('../utils/babrunContactClassification')"));
});
