'use strict';
// Synthetic fixture for the disposable loopback PostgreSQL harness only.
const { seedRecovery, insert } = require('./anchorRecoveryPostgresFixture');
const { prepareExistingFixture } = require('./anchorRecoveryPrepareExistingFixture');
async function seedPrepareExisting(db) {
  await db.query('DROP TABLE IF EXISTS acquisition_missions,users,touchpoints,ao_leads,ao_contacts,ao_prospect_tasks CASCADE');
  const base = await seedRecovery(db, { pending: true });
  const f = prepareExistingFixture();
  await db.query(`ALTER TABLE acquisition_outbound_programs ADD COLUMN scope_hash text;
    ALTER TABLE acquisition_mission_contributions ADD COLUMN at timestamptz;
    ALTER TABLE acquisition_outbound_items ALTER COLUMN status SET DEFAULT 'pending';
    ALTER TABLE acquisition_outbound_envelopes ADD CONSTRAINT recovery_day_unique UNIQUE(tenant_id,local_day);
    ALTER TABLE acquisition_outbound_lifecycle ADD COLUMN state text;
    ALTER TABLE prospects ADD COLUMN assigned_ao_id text,ADD COLUMN last_contacted_at timestamptz,
      ADD COLUMN closer_id text,ADD COLUMN last_reply_at timestamptz;
    CREATE TABLE acquisition_missions(id text PRIMARY KEY,tenant_id text,client_id integer,stage text,status text,
      objective text,target_segment text,orchestration_mission_id text,payload jsonb,created_at timestamptz,updated_at timestamptz);
    CREATE TABLE users(id text PRIMARY KEY,client_id integer,role text,active boolean);
    CREATE TABLE touchpoints(client_id integer,prospect_id text,action_type text);
    CREATE TABLE ao_prospect_tasks(client_id integer,prospect_id text);
    CREATE TABLE ao_leads(id text PRIMARY KEY,client_id integer,crm_prospect_id text,business_name text);
    CREATE TABLE ao_contacts(lead_id text,email text);
  `);
  await db.query('DELETE FROM acquisition_outbound_items WHERE id=$1',[base.item.id]);
  await db.query('DELETE FROM acquisition_outbound_envelopes WHERE id=$1',[base.envelope.id]);
  await db.query('DELETE FROM acquisition_mission_contributions');
  await db.query('UPDATE anchor_email_recovery_control SET manifest=NULL');
  await db.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2',[f.program.scope_hash,f.program.id]);
  await insert(db,'users',f.operator);
  await insert(db,'acquisition_missions',f.missionRow);
  for (const row of f.artifactRows) await insert(db,'acquisition_mission_contributions',row);
  return f;
}
module.exports = { seedPrepareExisting };
