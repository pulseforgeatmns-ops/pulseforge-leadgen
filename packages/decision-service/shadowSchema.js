'use strict';

const { FIELDS: eventFields } = require('./ShadowEventRepository');
const { FIELDS: evidenceFields } = require('./ShadowEvidenceRepository');
const TABLES = Object.freeze({ decision_shadow_events: eventFields, decision_shadow_evidence: evidenceFields });

// Read-only: reports the exact missing projection without interpreting a driver
// message or falling back to a smaller INSERT that silently loses audit data.
async function inspectShadowSchema(db) {
  const { rows } = await db.query(`SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = ANY($1::text[])`, [Object.keys(TABLES)]);
  const missing = {};
  for (const [table, fields] of Object.entries(TABLES)) {
    const present = new Set(rows.filter(r => r.table_name === table).map(r => r.column_name));
    missing[table] = fields.filter(field => !present.has(field));
  }
  return { ready: Object.values(missing).every(fields => fields.length === 0), missing };
}
module.exports = { TABLES, inspectShadowSchema };
