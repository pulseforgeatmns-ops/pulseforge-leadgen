'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
// Execute the production pure projection without importing leadgen, DB or providers.
const file = path.join(__dirname, '../scripts/promoteUnenriched.js');
const source = fs.readFileSync(file, 'utf8');
const body = source.slice(source.indexOf('function babrunPromotionContactResolution('), source.indexOf('\nasync function promoteRecord('));
const localRequire = createRequire(file);
const resolve = new Function('require', body + '; return babrunPromotionContactResolution;')(module => {
  assert.ok(['../utils/babrunContactClassification', '../utils/canonicalEmailEligibility', '../utils/babrunScoutContactEvidence'].includes(module));
  return localRequire(module);
});
const verification = { emailVerified: true, emailStatus: 'valid', emailVerificationMethod: 'fixture', verifiedAt: '2026-10-17T00:00:00.000Z' };
const owner = { email: 'maya@sampleco.example', contact: 'Maya Chen', title: 'Owner', source: ['hunter'] };
function state(enriched, receipt = verification) { return resolve({ enriched, verification: receipt, officialDomain: 'sampleco.example' }).contactResolution.finalState; }

test('first-party employee is not a founder', () => {
  assert.equal(state({ ...owner, title: 'Receptionist', source: ['scraped'], sourceUrl: 'https://sampleco.example/team' }), 'REVIEW_REQUIRED');
});
for (const title of ['Assistant to Founder', 'Vice President', 'Owner Relations Manager']) test(`${title} is not ownership evidence`, () => {
  assert.equal(state({ ...owner, title }), 'REVIEW_REQUIRED');
});
for (const status of ['risky', 'catchall', 'unknown', 'invalid']) test(`inconsistent verified flag cannot override ${status}`, () => {
  assert.notEqual(state(owner, { ...verification, emailStatus: status }), 'VERIFIED_FOUNDER_EMAIL');
});
test('explicit reject cannot become verified founder', () => assert.notEqual(state(owner, { ...verification, reject: true }), 'VERIFIED_FOUNDER_EMAIL'));
test('supported named owner remains eligible', () => {
  for (const source of ['hunter', 'prospeo', 'scraped', 'website_email']) {
    assert.equal(state({ ...owner, source: [source], sourceUrl: 'https://sampleco.example/about' }), 'VERIFIED_FOUNDER_EMAIL');
  }
});
test('role inbox remains role, even for a named owner', () => assert.equal(state({ ...owner, email: 'info@sampleco.example' }), 'VERIFIED_ROLE_EMAIL'));
test('third-party or ambiguous provenance never gains first-party ownership', () => {
  assert.equal(state({ ...owner, source: ['third_party'], sourceUrl: 'https://sampleco.example/about' }), 'REVIEW_REQUIRED');
  assert.equal(state({ ...owner, source: ['hunter', 'third_party'] }), 'REVIEW_REQUIRED');
});
test('generic placeholder names cannot gain founder evidence', () => assert.equal(state({ ...owner, email: 'owner@sampleco.example', contact: 'Owner' }), 'REVIEW_REQUIRED'));
test('receipt retains exact email binding, source and verification timestamps', () => {
  const result = resolve({ enriched: owner, verification, officialDomain: 'sampleco.example' });
  assert.equal(result.contactResolution.bestEmail, owner.email);
  assert.equal(result.contactResolution.contactName, owner.contact);
  assert.equal(result.contactResolution.verification.verifiedAt, verification.verifiedAt);
  assert.equal(result.sendableFounder, true);
});

test('stale selected evidence cannot supply missing current name/title', () => {
  assert.equal(state({ ...owner, contact: '', title: '', scoutContactEvidence: {
    email: owner.email, contactName: 'Maya Chen', contactTitle: 'Founder', source: 'hunter', companyDomain: 'sampleco.example',
  } }), 'REVIEW_REQUIRED');
});
test('changed selected email cannot borrow owner attribution from a previous address', () => {
  assert.equal(state({ ...owner, email: 'someoneelse@sampleco.example', scoutContactEvidence: {
    email: owner.email, contactName: 'Maya Chen', contactTitle: 'Founder', source: 'hunter', companyDomain: 'sampleco.example',
  } }), 'REVIEW_REQUIRED');
});
test('first-party source for another company is not attribution for the selected company', () => {
  assert.equal(state({ ...owner, source: ['scraped'], sourceUrl: 'https://another.example/team' }), 'REVIEW_REQUIRED');
});

for (const tenantId of [10, 13, 17]) test(`actual promotion persists classification only for Babrun (tenant ${tenantId})`, async () => {
  const vm = require('node:vm');
  const promoteBody = source.slice(source.indexOf('async function promoteRecord('), source.indexOf('\nasync function run()'));
  const context = { babrunPromotionContactResolution: resolve, configuredServiceAreas: () => [],
    promotionServiceAreaMatch: () => null, normalizeVertical: value => value,
    findOrCreateCompanyForClient: async () => 'company-fixture',
    console: { log() {} },
    require(module) {
      if (module === '../services/governedOutboundStore') return { GovernedOutboundStore: class {
        async candidateOwnership() { return null; } async suppression() { return null; }
      } };
      assert.ok(['../utils/canonicalEmailEligibility', '../utils/governedContactEligibility'].includes(module));
      return localRequire(module);
    },
  };
  vm.runInNewContext(promoteBody + '; this.promote = promoteRecord;', context);
  let metadata;
  const result = await context.promote({ id: 1, client_id: tenantId, domain: 'sampleco.example',
    website_url: 'https://sampleco.example', company: 'Sample', vertical: 'cleaning' }, {
    db: { query: async (sql, params) => {
      if (/INSERT INTO prospects/.test(sql)) { metadata = JSON.parse(params[16]); return { rows: [{ id: 'prospect-fixture' }] }; }
      assert.match(sql, /DELETE FROM scout_unenriched/); return { rows: [] };
    } }, enrich: async () => owner, verify: async () => verification, loadClientConfig: async () => ({}),
  });
  assert.equal(result.promoted, true);
  if (tenantId === 13) assert.equal(metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
  else assert.deepEqual(metadata, {});
});
