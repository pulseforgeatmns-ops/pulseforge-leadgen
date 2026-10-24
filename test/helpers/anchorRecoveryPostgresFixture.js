'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { recoveryFixture } = require('./anchorRecoveryManifestFixture');

async function insert(db, table, row) {
  const keys = Object.keys(row);
  await db.query(`INSERT INTO ${table}(${keys.join(',')}) VALUES(${keys.map((_,i)=>`$${i+1}`).join(',')})`,
    keys.map(key => typeof row[key] === 'object' && row[key] !== null && !(row[key] instanceof Date) ? JSON.stringify(row[key]) : row[key]));
}
async function seedRecovery(db, { pending = false, heldStatus } = {}) {
  const f = recoveryFixture();
  if (heldStatus) {
    f.oldDecision.expectedStatus=heldStatus;
    const hash=require('../../services/anchorEmailQuarantine').decisionHash(f.oldDecision);
    Object.assign(f.quarantine,{expected_status:heldStatus,decision_hash:hash,id:`quarantine_${hash}`});
    f.manifest.quarantineDecisionHash=hash;
  }
  await db.query(`DROP TABLE IF EXISTS anchor_email_recovery_reservations,anchor_email_quarantines,anchor_email_recovery_control,
    acquisition_outbound_events,acquisition_outbound_lifecycle,acquisition_mission_contributions,acquisition_mission_outbound_executions,
    acquisition_outbound_items,acquisition_outbound_envelopes,acquisition_outbound_programs,prospects,companies,clients,tenant_mailbox_integrations CASCADE;
    CREATE TABLE acquisition_outbound_programs(id text PRIMARY KEY,tenant_id text,mode text,source_mission_id text,policy jsonb,policy_hash text,authorized_by text,authorized_at timestamptz);
    CREATE TABLE acquisition_outbound_envelopes(id text PRIMARY KEY,program_id text,tenant_id text,local_day date,mission_id text,revision text,manifest jsonb,manifest_hash text,approval_id text,status text);
    CREATE TABLE acquisition_outbound_items(id text PRIMARY KEY,tenant_id text,envelope_id text,candidate_id text,prospect_id text,company_id text,email text,snapshot jsonb,status text,attempted_at timestamptz,provider_message_id text,reason text);
    CREATE TABLE acquisition_mission_outbound_executions(id text PRIMARY KEY,tenant_id text,mission_id text,prospect_id text,status text,prepared_artifact_revision text,payload jsonb,provider_message_id text,sent_at timestamptz,attempted_at timestamptz);
    CREATE TABLE acquisition_mission_contributions(id text PRIMARY KEY,tenant_id text,mission_id text,specialist text,kind text,payload jsonb);
    CREATE TABLE companies(id text PRIMARY KEY,client_id int,name text,domain text,website text);
    CREATE TABLE prospects(id text PRIMARY KEY,client_id int,company_id text,email text,do_not_contact boolean,is_synthetic boolean,ao_outreach_review_required boolean,email_verified boolean,email_status text,enrichment_provenance jsonb,acquisition_metadata jsonb,contact_classification text);
    CREATE TABLE clients(id int PRIMARY KEY,active boolean,autosend_enabled boolean,sender_email text,sender_name text,sending_domain text);
    CREATE TABLE tenant_mailbox_integrations(id text PRIMARY KEY,tenant_id text,status text,mailbox_address text);
    CREATE TABLE acquisition_outbound_lifecycle(tenant_id text,suppressed boolean,email text,company_id text);
    CREATE TABLE acquisition_outbound_events(id text PRIMARY KEY,tenant_id text,program_id text,envelope_id text,item_id text,event_type text,payload jsonb,created_at timestamptz DEFAULT now());
  `);
  const schema = fs.readFileSync(path.join(__dirname,'../../docs/sql/anchor-email-recovery-schema-proposal.sql'),'utf8');
  const client = db.connect ? await db.connect() : db;
  try {
    await client.query("SELECT set_config('anchor_email_recovery.maintenance_armed','true',false),set_config('anchor_email_recovery.provider_gate_verified','closed',false),set_config('anchor_email_recovery.verified_source_revision',repeat('a',40),false)");
    await client.query(schema);
  } finally { if (db.connect) client.release(); }
  await insert(db,'acquisition_outbound_programs',f.program);
  await insert(db,'acquisition_outbound_envelopes',f.envelope);
  await insert(db,'acquisition_mission_contributions',f.approval);
  const { attempt_token, ...freshItem } = f.item;
  if (pending) { freshItem.status='pending'; freshItem.attempted_at=null; }
  await insert(db,'acquisition_outbound_items',freshItem);
  await insert(db,'companies',{id:f.prospect.company_id,client_id:10,name:'Fresh Synthetic Enterprise',domain:f.prospect.company_domain,website:f.prospect.company_website});
  const {company_domain,company_website,...prospect}=f.prospect;
  await insert(db,'prospects',prospect);
  await insert(db,'clients',f.client);
  await insert(db,'tenant_mailbox_integrations',f.integration);
  await insert(db,'anchor_email_quarantines',f.quarantine);
  if (!pending) await insert(db,'anchor_email_recovery_reservations',f.reservations[0]);
  const q=f.quarantine;
  await insert(db,'companies',{id:q.company_id,client_id:10,name:'Held Synthetic Enterprise',domain:q.company_domain || 'synthetic-held.test',website:`https://${q.company_domain || 'synthetic-held.test'}`});
  await insert(db,'prospects',{id:q.prospect_id,client_id:10,company_id:q.company_id,email:q.recipient_email,do_not_contact:false,is_synthetic:false,ao_outreach_review_required:true});
  await insert(db,'acquisition_outbound_envelopes',{id:q.envelope_id,program_id:q.program_id,tenant_id:'10',local_day:'2026-10-16',mission_id:q.mission_id,revision:q.expected_revision,status:'authorized'});
  await insert(db,'acquisition_outbound_items',{id:q.item_id,tenant_id:'10',envelope_id:q.envelope_id,candidate_id:q.candidate_id,prospect_id:q.prospect_id,company_id:q.company_id,email:q.recipient_email,status:q.expected_status,reason:q.expected_reason,attempted_at:q.attempt_token,provider_message_id:null});
  await insert(db,'acquisition_mission_outbound_executions',{id:q.canonical_id,tenant_id:'10',mission_id:q.mission_id,prospect_id:q.candidate_id,status:'attempted',prepared_artifact_revision:q.expected_revision,payload:{email:q.recipient_email},provider_message_id:null,sent_at:null,attempted_at:q.attempt_token});
  if (!pending) await insert(db,'acquisition_mission_outbound_executions',{id:'local-fresh-canonical',tenant_id:'10',mission_id:f.envelope.mission_id,prospect_id:f.item.candidate_id,status:'attempted',prepared_artifact_revision:f.envelope.revision,payload:{email:f.item.email},provider_message_id:null,sent_at:null,attempted_at:f.item.attempt_token});
  await db.query('UPDATE anchor_email_recovery_control SET manifest=$1 WHERE singleton=true',[f.manifest]);
  return f;
}
module.exports={seedRecovery,insert};
