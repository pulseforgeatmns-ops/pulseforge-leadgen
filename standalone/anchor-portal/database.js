'use strict';
const { Pool } = require('pg');
const { MARKER } = require('./config');
function poolFor(config) {
  return new Pool({ connectionString: config.databaseUrl, max: 5, ssl: config.mode === 'hosted' ? { rejectUnauthorized: true } : false });
}
async function verifyDatabase(pool) {
  const { rows: [schema] } = await pool.query('SELECT current_schema() AS name, current_database() AS database');
  if (schema.name !== 'public' || schema.database !== 'anchor_portal_synthetic') throw new Error('Unexpected database schema');
  const { rows: markers } = await pool.query('SELECT marker FROM portal_demo_marker');
  if (markers.length !== 1 || markers[0].marker !== MARKER) throw new Error('Synthetic database marker missing');
  const { rows } = await pool.query("SELECT schemaname,tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')");
  const allowed = new Set(['portal_demo_marker','portal_sessions','users', ...['locations','scope_sections','scope_items','user_locations','visits','visit_items','issues','evidence'].map(n => 'anchor_portal_' + n)]);
  if (rows.length !== allowed.size || rows.some(r => r.schemaname !== 'public' || !allowed.has(r.tablename))) throw new Error('Unexpected database tables');
  await require('../../utils/anchorPortalSchema').createPortalSchema(pool).verifyAnchorPortalUpgrade(pool);
  const { rows: bad } = await pool.query("SELECT 1 FROM users WHERE email NOT LIKE '%@exeter-fixture.invalid' OR role NOT IN ('manager','cleaner','facility_client') OR client_id NOT IN (10,11) OR password_hash !~ '^\\$2[aby]\\$12\\$[./A-Za-z0-9]{53}$' UNION ALL SELECT 1 FROM anchor_portal_locations WHERE is_demo IS NOT TRUE LIMIT 1");
  if (bad.length) throw new Error('Non-synthetic data or invalid credentials');
}
module.exports = { poolFor, verifyDatabase };
