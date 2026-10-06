'use strict';

/** Columns the AO prospect routing service reads on `prospects` (keep in sync with production migrations). */
const REQUIRED_PROSPECT_ROUTING_COLUMNS = Object.freeze([
  'disposition_status',
  'next_action_status',
  'next_action_due_at',
  'prospect_motion',
  'assigned_ao_id',
  'do_not_contact',
  'icp_score',
]);

const REQUIRED_USER_ROUTING_COLUMNS = Object.freeze([
  'ao_operational_status',
]);

async function assertAoProspectRoutingSchemaContract(db) {
  const { rows } = await db.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'prospects'
      AND column_name = ANY($1::text[])
  `, [REQUIRED_PROSPECT_ROUTING_COLUMNS]);
  const found = new Set(rows.map(r => r.column_name));
  const missing = REQUIRED_PROSPECT_ROUTING_COLUMNS.filter(name => !found.has(name));
  if (missing.length) {
    throw new Error(`AO routing integration schema missing prospects columns: ${missing.join(', ')}`);
  }

  const userCols = (await db.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'users'
      AND column_name = ANY($1::text[])
  `, [REQUIRED_USER_ROUTING_COLUMNS])).rows;
  const userFound = new Set(userCols.map(r => r.column_name));
  const userMissing = REQUIRED_USER_ROUTING_COLUMNS.filter(name => !userFound.has(name));
  if (userMissing.length) {
    throw new Error(`AO routing integration schema missing users columns: ${userMissing.join(', ')}`);
  }
}

module.exports = {
  REQUIRED_PROSPECT_ROUTING_COLUMNS,
  REQUIRED_USER_ROUTING_COLUMNS,
  assertAoProspectRoutingSchemaContract,
};
