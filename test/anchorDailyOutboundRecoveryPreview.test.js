'use strict';

// Synthetic snapshots only. No outbound adapter or provider is used.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { prepareOnlyPreview, validatePreviewConfig } = require('../services/anchorEmailRecoveryPreview');
const { validateManifest } = require('../services/anchorEmailRecovery');
const { recoveryFixture } = require('./helpers/anchorRecoveryManifestFixture');
function fixture() {
  const f = recoveryFixture();
  f.control.manifest = null;
  Object.assign(f.item, { status: 'pending', attempted_at: null, attempt_token: null });
  f.unresolved = f.unresolved.slice(0,1); f.reservations = [];
  f.config = { mode:'PREPARE_ONLY',tenantId:'10',programId:f.program.id,policyHash:f.program.policy_hash,
    approvalReference:'local-synthetic-preview-only-approval',validFrom:f.manifest.validFrom,expiresAt:f.manifest.expiresAt,
    maxPreviewItems:1,quarantineDecisionHash:f.quarantine.decision_hash };
  const query = f.db.query.bind(f.db);
  f.db.query = (sql, values) => {
    assert.match(sql, /^\s*SELECT\b/i);
    assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|TRUNCATE|FOR SHARE|FOR UPDATE|pg_advisory|set_config)\b/i);
    return query(sql, values);
  };
  return f;
}
async function preview(f) { return prepareOnlyPreview(f.db, f.config, { now:f.now }); }
function assertBlocked(result) {
  assert.equal(result.status,'blocked'); assert.equal(result.readOnly,true); assert.equal(result.providerCalls,0);
  assert.equal(result.dispatchAllowed,false); assert.equal(result.approvalRequired,true); assert.equal(result.proposedManifest,null); assert.deepEqual(result.preview,[]);
}

test('PREPARE_ONLY previews one current approved snapshot without granting activation or changing records', async () => {
  const f = fixture(); const before = JSON.stringify([f.program,f.envelope,f.item,f.prospect,f.quarantine,f.control,f.reservations]);
  const result = await preview(f);
  assert.equal(result.status,'preview', result.reason); assert.equal(result.preview.length,1);
  assert.equal(result.readOnly,true); assert.equal(result.providerCalls,0); assert.equal(result.dispatchAllowed,false);
  assert.equal(result.approvalRequired,true); assert.equal(result.rawUnknownCount,1); assert.equal(result.quarantinedUnknownCount,1);
  assert.equal(result.outcome,'UNKNOWN'); assert.equal(result.retryAllowed,false); assert.equal(result.blockingUnknownCount,0);
  assert.equal(result.preview[0].recipientEmail,f.item.email); assert.equal(result.preview[0].body,f.item.snapshot.message.body);
  assert.equal(result.proposedManifest.id,null); assert.equal(result.proposedManifest.approvalReference,null);
  assert.equal(result.proposedManifest.maxFreshSends,1); assert.equal(result.proposedManifest.admissions.length,1);
  assert.notEqual(result.proposedManifest.approvalReference,f.config.approvalReference);
  assert.throws(() => validateManifest(result.proposedManifest,{now:f.now}), error => error.code === 'anchor_recovery_invalid_manifest');
  assert.equal(JSON.stringify([f.program,f.envelope,f.item,f.prospect,f.quarantine,f.control,f.reservations]), before);
});

test('PREPARE_ONLY missing/broadened/expired config blocks before the first database read', async () => {
  const original = fixture().config;
  for (const key of Object.keys(original)) {
    const f = fixture(); delete f.config[key]; assertBlocked(await preview(f)); assert.equal(f.calls.length,0);
  }
  for (const change of [{mode:'ACTIVE'},{tenantId:'13'},{tenantId:10},{maxPreviewItems:2},{maxPreviewItems:0},
    {approvalReference:''},{policyHash:'unknown'},{quarantineDecisionHash:'unknown'}, {sendApproved:true},
    {validFrom:'2026-10-20T00:00:00.000Z',expiresAt:'2026-10-20T01:00:00.000Z'},
    {expiresAt:'2026-10-21T00:00:00.000Z'}, {expiresAt:'2026-10-19T15:59:59.999Z'}]) {
    const f = fixture(); Object.assign(f.config,change); assertBlocked(await preview(f)); assert.equal(f.calls.length,0,JSON.stringify(change));
  }
  assert.throws(() => validatePreviewConfig(undefined));
});

test('PREPARE_ONLY requires durable closed maintenance and independently applied exact quarantine', async () => {
  for (const change of [f => {f.control=null;}, f=>{f.control.maintenance_armed=false;}, f=>{f.control.manifest=f.manifest;},
    f=>{f.quarantine=null;}, f=>{f.quarantine.decision_hash='0'.repeat(64);}, f=>{f.fail=/anchor_email_recovery_control/;}]) {
    const f=fixture();change(f);assertBlocked(await preview(f));
  }
});

test('PREPARE_ONLY absent current prepared work reports bounded preparation approval needed', async () => {
  for (const target of ['envelope','item']) {
    const f=fixture();f[target]=null;const result=await preview(f);
    assertBlocked(result);assert.equal(result.reason,'current_prepared_item_required');
    assert.match(result.nextStep,/separately approved bounded research\/preparation/);
    assert.equal(result.rawUnknownCount,1);
  }
  for (const change of [f=>{f.envelope.local_day='2026-10-16';},f=>{f.envelope.status='expired';},f=>{f.item.status='expired';},
    f=>{f.item.attempted_at='2026-10-19T15:00:00Z';},f=>{f.item.attempt_token='2026-10-19 15:00:00+00';},f=>{f.item.provider_message_id='receipt';}]) {
    const f=fixture();change(f);assertBlocked(await preview(f));
  }
});

test('PREPARE_ONLY unknown rows, snapshot/approval changes and current CRM holds stay blocked', async () => {
  for (const change of [
    f=>{f.unresolved.push({...f.unresolved[0],item_id:'another-unknown',attempt_token:null});},
    f=>{f.program.policy.dailyCap+=1;}, f=>{f.envelope.manifest_hash='0'.repeat(64);},
    f=>{f.item.snapshot.message.body='unapproved replacement';}, f=>{f.approval.payload.payload.approved=false;},
    f=>{f.prospect.ao_outreach_review_required=true;f.prospect.source='spreadsheet_approved';},
    f=>{f.prospect.ao_outreach_review_required=null;},f=>{delete f.prospect.ao_outreach_review_required;},
    f=>{f.prospect.ao_outreach_review_required='false';},f=>{f.fail=/FROM prospects p/;},
    f=>{f.prospect.do_not_contact=true;},f=>{f.prospect.is_synthetic=true;},
    f=>{f.prospect.email='different@synthetic-recovery.test';},f=>{f.prospect.enrichment_provenance={};},
    f=>{f.companies[0].domain=f.oldDecision.companyDomain;f.companies[0].website=null;},
    f=>{f.client.sender_email='changed@synthetic-recovery.test';}, f=>{f.integration.status='inactive';},
    f=>{f.reservations=[{item_id:f.item.id}];},
  ]) { const f=fixture();change(f);assertBlocked(await preview(f)); }
  const f=fixture();f.unresolved.push({...f.unresolved[0],item_id:'another-unknown',attempt_token:null});
  const result=await preview(f);assert.equal(result.rawUnknownCount,2);assert.equal(result.blockingUnknownCount,1);
});

test('PREPARE_ONLY remains SELECT-only under native PostgreSQL READ ONLY and cannot fabricate fresh work', {timeout:60000}, async t => {
  const instance=await startDisposablePostgres('anchor-preview-');const pool=new Pool({connectionString:instance.connectionString,max:3});
  t.after(async()=>{await pool.end();await instance.stop();});
  const f=fixture();
  await pool.query(`
    CREATE TABLE anchor_email_recovery_control(singleton boolean,maintenance_armed boolean,manifest jsonb);
    CREATE TABLE anchor_email_quarantines(id text,tenant_id text,item_id text,envelope_id text,program_id text,mission_id text,canonical_id text,candidate_id text,expected_revision text,attempt_token text,recipient_email text,prospect_id text,company_id text,company_name_key text,company_domain text,expected_status text,expected_reason text,approval_ref text,decision_hash text,created_at timestamptz);
    CREATE TABLE anchor_email_recovery_reservations(item_id text);
    CREATE TABLE acquisition_outbound_programs(id text,tenant_id text,mode text,source_mission_id text,policy jsonb,policy_hash text,authorized_by text);
    CREATE TABLE acquisition_outbound_envelopes(id text,program_id text,tenant_id text,local_day date,mission_id text,revision text,manifest jsonb,manifest_hash text,approval_id text,status text);
    CREATE TABLE acquisition_outbound_items(id text,tenant_id text,envelope_id text,candidate_id text,prospect_id text,company_id text,email text,snapshot jsonb,status text,attempted_at timestamptz,provider_message_id text,reason text);
    CREATE TABLE acquisition_mission_outbound_executions(id text,tenant_id text,mission_id text,prospect_id text);
    CREATE TABLE acquisition_mission_contributions(id text,tenant_id text,mission_id text,specialist text,kind text,payload jsonb);
    CREATE TABLE prospects(id text,client_id integer,company_id text,email text,do_not_contact boolean,is_synthetic boolean,ao_outreach_review_required boolean,email_verified boolean,email_status text,enrichment_provenance jsonb,acquisition_metadata jsonb,contact_classification text);
    CREATE TABLE companies(id text,client_id integer,name text,domain text,website text);
    CREATE TABLE clients(id integer,active boolean,autosend_enabled boolean,sender_email text,sender_name text,sending_domain text);
    CREATE TABLE tenant_mailbox_integrations(id text,tenant_id text,status text,mailbox_address text);
  `);
  async function insert(table,row) {
    // Table names are fixed fixture constants, never user input.
    await pool.query(`INSERT INTO ${table} SELECT * FROM jsonb_populate_record(NULL::${table},$1::jsonb)`,[JSON.stringify(row)]);
  }
  await insert('anchor_email_recovery_control',f.control);await insert('anchor_email_quarantines',f.quarantine);
  await insert('acquisition_outbound_programs',f.program);await insert('acquisition_outbound_envelopes',f.envelope);
  await insert('acquisition_outbound_items',f.item);await insert('acquisition_mission_contributions',f.approval);
  await insert('prospects',f.prospect);for(const company of f.companies) await insert('companies',company);
  await insert('clients',f.client);await insert('tenant_mailbox_integrations',f.integration);
  const q=f.quarantine;
  await insert('acquisition_outbound_envelopes',{id:q.envelope_id,program_id:q.program_id,tenant_id:q.tenant_id,local_day:'2026-10-16',mission_id:q.mission_id,revision:q.expected_revision,status:'expired'});
  await insert('acquisition_outbound_items',{id:q.item_id,tenant_id:q.tenant_id,envelope_id:q.envelope_id,candidate_id:q.candidate_id,prospect_id:q.prospect_id,company_id:q.company_id,email:q.recipient_email,status:q.expected_status,reason:q.expected_reason,attempted_at:q.attempt_token});
  await insert('acquisition_mission_outbound_executions',{id:q.canonical_id,tenant_id:q.tenant_id,mission_id:q.mission_id,prospect_id:q.candidate_id});
  const db=await pool.connect();
  try {
    await db.query('BEGIN READ ONLY');
    const result=await prepareOnlyPreview(db,f.config,{now:f.now});assert.equal(result.status,'preview',JSON.stringify(result));
    assert.equal(result.providerCalls,0);assert.equal(result.dispatchAllowed,false);assert.equal(result.rawUnknownCount,1);
    assert.equal((await db.query('SELECT txid_current_if_assigned() AS id')).rows[0].id,null);
    await db.query('ROLLBACK');
    await pool.query("UPDATE acquisition_outbound_envelopes SET local_day='2026-10-16',status='expired'");
    await db.query('BEGIN READ ONLY');
    const missing=await prepareOnlyPreview(db,f.config,{now:f.now});assertBlocked(missing);assert.equal(missing.reason,'current_prepared_item_required');
    assert.equal((await db.query('SELECT count(*)::int n FROM acquisition_outbound_items')).rows[0].n,2);
    assert.equal((await db.query('SELECT count(*)::int n FROM anchor_email_recovery_reservations')).rows[0].n,0);
  } finally {await db.query('ROLLBACK');db.release();}
});
