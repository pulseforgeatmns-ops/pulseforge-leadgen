'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
// Real helper + eligibility/classification code; no real DB, providers, or startup I/O.
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  query: async () => { throw new Error('unexpected default database access'); },
} };
const { persistProviderChainEmail, enrichProspectRow } = require('../scripts/lib/anchorMissionBoundEnrichment');
const { governedContactReason } = require('../utils/governedContactEligibility');
const policy = { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] };
const row = () => ({ prospect_id: 'synthetic-p', company_id: 'synthetic-c', client_id: 13, domain: 'fixturepainting.example',
  company_name: 'Fixture Painting', do_not_contact: false, acquisition_metadata: { unrelated: 'keep' } });
const selected = () => ({ email: 'ada@fixturepainting.example', contact: 'Ada Lovelace', title: 'Owner',
  source: ['hunter'], sourceUrl: 'https://fixturepainting.example/about' });
const verified = () => ({ emailVerified: true, emailStatus: 'valid', emailVerificationMethod: 'fixture_verifier',
  doNotContact: false, verifiedAt: '2026-10-15T12:00:00Z', verifierCheckedAt: '2026-10-15T12:00:00Z',
  verifierResponse: { status: 'deliverable', synthetic: true } });
function recordingDb(original, { noUpdate = false, current = original } = {}) {
  const calls = []; let saved;
  return { calls, get saved() { return saved; }, query: async (sql, p) => {
    calls.push({sql, p});
    if (sql.includes('UPDATE prospects')) {
      assert.match(sql, /AND client_id = \$11/);
      assert.match(sql, /FOR SHARE/);
      assert.match(sql, /company_id::text = \$13::text/);
      assert.match(sql, /do_not_contact IS DISTINCT FROM TRUE/);
      const unchanged = !p[11] || (current.company_id === p[12]
        && (current.domain ?? null) === p[13] && (current.website ?? null) === p[14]
        && (current.website_url ?? null) === p[15] && (current.company_name ?? null) === p[16]
        && (current.email ?? null) === p[17]);
      if (noUpdate || !unchanged) return {rows: [], rowCount: 0};
      saved = { ...original, email: p[0], email_verified: p[3], email_status: p[5],
        enrichment_provenance: JSON.parse(p[8]), acquisition_metadata: {
          ...original.acquisition_metadata, ...(p[11] ? JSON.parse(p[11]) : {}) } };
      return {rows: [], rowCount: 1};
    }
    assert.match(sql, /INSERT INTO agent_log/);
    return {rows: [], rowCount: 1};
  } };
}
async function save({ original = row(), enriched = selected(), receipt = verified(), noUpdate = false, current = original } = {}) {
  const db = recordingDb(original, {noUpdate, current});
  const result = await persistProviderChainEmail(db, original, enriched, receipt, false);
  return { db, result };
}
test('governed provider-chain write atomically retains selected founder evidence with verified email', async () => {
  const {db,result} = await save();
  assert.equal(result.persisted, true);
  const evidence = db.saved.acquisition_metadata.contactResolution;
  assert.equal(evidence.finalState, 'VERIFIED_FOUNDER_EMAIL');
  assert.equal(evidence.bestEmail, db.saved.email);
  assert.equal(evidence.contactName, 'Ada Lovelace');
  assert.equal(evidence.contactTitle, 'Owner');
  assert.deepEqual(evidence.verification.raw, verified().verifierResponse);
  assert.equal(evidence.verification.verifiedAt, verified().verifiedAt);
  assert.equal(db.saved.acquisition_metadata.unrelated, 'keep');
  assert.equal(db.calls.filter(c => c.sql.includes('UPDATE prospects')).length, 1);
  assert.equal(governedContactReason(db.saved,policy), null);
});
test('missing, ambiguous, mixed, stale, unrelated and generic identity cannot become a founder', async t => {
  const variants = [
    ['unnamed', {contact: ''}], ['no title', {title: ''}], ['generic name', {contact:'Owner'}],
    ['office manager', {title:'Office Manager'}], ['assistant', {title:'Assistant to Owner'}],
    ['vice president', {title:'Vice President'}], ['former founder', {title:'Former Founder'}],
    ['mixed source', {source:['hunter','pattern_candidate']}], ['unsupported', {source:['directory']}],
    ['pattern', {source:['pattern_candidate']}], ['changed address', {email:'grace@fixturepainting.example'}],
    ['generic address', {email:'info@fixturepainting.example'}],
    ['foreign scrape', {source:['scraped'],sourceUrl:'https://directory.example/about'}],
    ['missing source', {source:[]}],
  ];
  for (const [name,patch] of variants) await t.test(name, async () => {
    const original = { ...row(), first_name:'Ada', last_name:'Lovelace',job_title:'Owner',
      acquisition_metadata:{contactResolution:{finalState:'VERIFIED_FOUNDER_EMAIL',bestEmail:'old@fixturepainting.example'}} };
    const {db,result} = await save({original,enriched:{...selected(),...patch}});
    if (result.persisted) {
      assert.notEqual(db.saved.acquisition_metadata.contactResolution.finalState,'VERIFIED_FOUNDER_EMAIL');
      assert.notEqual(governedContactReason(db.saved,policy),null);
    } else assert.equal(db.saved,undefined);
  });
});
test('wrong-company address, DNC, rejection, risky and contradictory flags cannot promote', async t => {
  for (const patch of [
    {enriched:{...selected(),email:'ada@other.example'}},
    {original:{...row(),do_not_contact:true}},
    {receipt:{...verified(),doNotContact:true}},
    {receipt:{...verified(),reject:true}},
    ...['unknown','risky','catchall','invalid'].map(emailStatus=>({receipt:{...verified(),emailStatus}})),
    {receipt:{...verified(),emailVerified:false}},
  ]) await t.test(JSON.stringify(patch),async()=>{
    const {db,result}=await save(patch);
    assert.equal(result.persisted,false); assert.equal(db.saved,undefined);
  });
});
test('tenant 10 does not receive Babrun classification or change existing metadata',async()=>{
  const original={...row(),client_id:10};
  const {db,result}=await save({original});
  assert.equal(result.persisted,true);
  assert.equal(db.calls[0].p[11],null);
  assert.deepEqual(db.saved.acquisition_metadata,original.acquisition_metadata);
  assert.equal(governedContactReason({...db.saved,client_id:10},policy),'contact_tenant_mismatch');
});
test('concurrent DNC/no matching tenant row cannot produce success or an audit success event',async()=>{
  const {db,result}=await save({noUpdate:true});
  assert.equal(result.persisted,false); assert.equal(result.reason,'contact_update_not_applied');
  assert.equal(db.calls.length,1);
});
test('already verified rows without founder evidence remain held without provider calls or invented promotion',async()=>{
  const original={...row(),email:selected().email,email_verified:true,email_status:'valid',
    enrichment_provenance:{email:{source:'hunter'}}};
  const before=JSON.stringify(original);
  const result=await enrichProspectRow(original,{db:{query:async()=>{throw Error('unexpected write');}},dryRun:false,
    processProspect:async()=>{throw Error('unexpected provider');},runEnrichmentChain:async()=>{throw Error('unexpected provider');}});
  assert.equal(result.path,'existing_crm');
  assert.equal(JSON.stringify(original),before);
  assert.equal(governedContactReason(original,policy),'contact_classification_not_sendable');
});

test('changed company identity or website inputs fail closed without overwriting new identity', async t => {
  for (const patch of [{company_id:'different-company'}, {domain:'different.example'},
    {website:'https://different.example'}, {website_url:'https://different.example'},
    {company_name:'Different Company'}, {email:'new@fixturepainting.example'}]) {
    await t.test(JSON.stringify(patch), async () => {
      const original=row(); const current={...original,...patch};
      const {db,result}=await save({original,current});
      assert.equal(result.persisted,false); assert.equal(result.reason,'contact_update_not_applied');
      assert.equal(db.saved,undefined); assert.equal(db.calls.length,1);
    });
  }
});
