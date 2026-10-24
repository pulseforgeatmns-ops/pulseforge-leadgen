'use strict';
// Synthetic local manual-DDL helper; never imported by application runtime.
const fs = require('node:fs'), path = require('node:path');
const schema = fs.readFileSync(path.join(__dirname, '../../docs/sql/anchor-crm-preparation-schema-proposal.sql'), 'utf8');
async function applyCrmPreparationSchema(pool) {
  const db = await pool.connect();
  try {
    await db.query("SELECT set_config('anchor_crm_preparation.tenant_10_gate_verified','closed',false),set_config('anchor_crm_preparation.verified_source_revision',repeat('c',40),false),set_config('anchor_crm_preparation.schema_approval_ref','local-synthetic-crm-schema-approval',false)");
    await db.query(schema);
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}
module.exports = { schema, applyCrmPreparationSchema };
