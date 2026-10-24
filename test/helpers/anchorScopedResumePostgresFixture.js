'use strict';
// Disposable synthetic schema only; never imports application DB/bootstrap.
const fs = require('node:fs');
const path = require('node:path');
const { seedRecovery, insert } = require('./anchorRecoveryPostgresFixture');
const { scopedResumeFixture } = require('./anchorScopedResumeFixture');
const schema = fs.readFileSync(path.join(__dirname, '../../docs/sql/anchor-scoped-resume-schema-proposal.sql'), 'utf8');
async function applyScopedSchema(pool, { addReviewColumn = false } = {}) {
  const db = await pool.connect();
  try {
    await db.query("SELECT set_config('anchor_scoped_resume.tenant_10_gate_verified','closed',false),set_config('anchor_scoped_resume.verified_source_revision',repeat('b',40),false),set_config('anchor_scoped_resume.schema_approval_ref','local-synthetic-shared-schema-approval',false)");
    if (addReviewColumn) await db.query("SELECT set_config('anchor_scoped_resume.add_review_column','approved_nullable_no_default',false)");
    await db.query(schema);
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}
async function seedScopedResume(pool, { schemaInstalled = true, preparation = false } = {}) {
  await pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
  await seedRecovery(pool, { pending: true });
  const f = scopedResumeFixture();
  f.actor = { id: f.manifest.operatorId, role: f.manifest.operatorRole };
  await pool.query(`UPDATE anchor_email_recovery_control SET manifest=NULL;
    ALTER TABLE acquisition_outbound_programs ADD COLUMN scope_hash text;
    ALTER TABLE acquisition_outbound_items ALTER COLUMN status SET DEFAULT 'pending';
    ALTER TABLE prospects ADD COLUMN source text,ADD COLUMN source_type text,ADD COLUMN source_ref text,ADD COLUMN import_metadata jsonb;
    ALTER TABLE prospects ADD COLUMN assigned_ao_id integer,ADD COLUMN closer_id integer,
      ADD COLUMN last_contacted_at timestamptz,ADD COLUMN last_reply_at timestamptz,
      ADD COLUMN operational_status text,ADD COLUMN setter_status text,ADD COLUMN closer_status text;
    ALTER TABLE acquisition_outbound_lifecycle ADD COLUMN state text;
    CREATE TABLE touchpoints(id text PRIMARY KEY,client_id integer,prospect_id text,action_type text);
    CREATE TABLE ao_prospect_tasks(id text PRIMARY KEY,client_id integer,prospect_id text);
    CREATE TABLE ao_leads(id text PRIMARY KEY,client_id integer,crm_prospect_id text,business_name text);
    CREATE TABLE ao_contacts(id text PRIMARY KEY,lead_id text,email text);
    CREATE TABLE acquisition_missions(id text PRIMARY KEY,tenant_id text,payload jsonb);
    CREATE TABLE users(id text PRIMARY KEY,client_id integer,role text,active boolean);`);
  await pool.query('UPDATE acquisition_outbound_programs SET scope_hash=$1 WHERE id=$2', [f.program.scope_hash, f.program.id]);
  await pool.query('UPDATE prospects SET source=$1 WHERE id=$2', [f.prospect.source, f.prospect.id]);
  await insert(pool, 'acquisition_missions', { id: f.sourceMission.id, tenant_id: '10', payload: f.sourceMission });
  await insert(pool, 'users', { id: f.actor.id, client_id: 10, role: f.actor.role, active: true });
  if (preparation) f.manifest.scopes.preparation = { enabled: true, approvalReference: 'local-synthetic-preparation-approval',
    dailyCap: 2, totalCap: 3, dailyItemCap: 2, totalItemCap: 3, operations: ['reuse_existing_artifacts', 'freeze', 'refill'] };
  if (schemaInstalled) await applyScopedSchema(pool);
  return f;
}
module.exports = { schema, applyScopedSchema, seedScopedResume };
