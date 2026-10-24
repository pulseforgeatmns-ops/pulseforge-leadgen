'use strict';
const { Pool } = require('pg');
const { X509Certificate } = require('node:crypto');
const { checkServerIdentity } = require('node:tls');
const { MARKER } = require('./config');
function tlsOptions(config) {
 if(config.mode!=='hosted') { if(config.databaseCa)throw new Error('CA is hosted-only');return false; }
 const options={rejectUnauthorized:true,checkServerIdentity};
 if(config.databaseCa){
  if(!/^-----BEGIN CERTIFICATE-----[\s\S]+-----END CERTIFICATE-----\s*$/.test(config.databaseCa)||config.databaseCa.includes('PRIVATE KEY'))throw new Error('Public CA certificate required');
  const cert=new X509Certificate(config.databaseCa);
  if(!cert.ca||Date.parse(cert.validFrom)>Date.now()||Date.parse(cert.validTo)<=Date.now())throw new Error('Valid CA certificate required');
  options.ca=config.databaseCa;
 }
 return options;
}
function poolFor(config) {
 return new Pool({connectionString:config.databaseUrl,max:5,ssl:tlsOptions(config)});
}
async function verifyDatabase(pool, { requireAccounts = true } = {}) {
  const { rows: [schema] } = await pool.query('SELECT current_schema() AS name, current_database() AS database');
  if (schema.name !== 'public' || schema.database !== 'anchor_portal_synthetic') throw new Error('Unexpected database schema');
  const { rows: markers } = await pool.query('SELECT marker,expires_at FROM portal_demo_marker');
  if (markers.length !== 1 || markers[0].marker !== MARKER) throw new Error('Synthetic database marker missing');
  const { rows } = await pool.query("SELECT schemaname,tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')");
  const allowed = new Set(['portal_demo_marker','portal_sessions','users', ...['locations','scope_sections','scope_items','user_locations','visits','visit_items','issues','evidence'].map(n => 'anchor_portal_' + n)]);
  // The explicitly migrated request table is optional for pre-migration private setups.
  if (rows.some(r => r.schemaname === 'public' && r.tablename === 'anchor_portal_service_requests')) allowed.add('anchor_portal_service_requests');
  if (rows.length !== allowed.size || rows.some(r => r.schemaname !== 'public' || !allowed.has(r.tablename))) throw new Error('Unexpected database tables');
  await require('../../utils/anchorPortalSchema').createPortalSchema(pool).verifyAnchorPortalUpgrade(pool);
  const { rows: bad } = await pool.query("SELECT 1 FROM users WHERE email NOT LIKE '%@exeter-fixture.invalid' OR role NOT IN ('manager','cleaner','facility_client') OR client_id NOT IN (10,11) OR password_hash !~ '^\\$2[aby]\\$12\\$[./A-Za-z0-9]{53}$' UNION ALL SELECT 1 FROM anchor_portal_locations WHERE is_demo IS NOT TRUE LIMIT 1");
  if (bad.length) throw new Error('Non-synthetic data or invalid credentials');
  if(requireAccounts){
   const {rows:[accounts]}=await pool.query("SELECT count(*)::int AS count FROM users WHERE active=TRUE AND valid_until>NOW() AND email IN ('client@exeter-fixture.invalid','operator@exeter-fixture.invalid')");
   if(accounts.count!==2||!markers[0].expires_at||new Date(markers[0].expires_at)<=new Date())throw Object.assign(new Error('Approved unexpired demo accounts required'),{code:'PORTAL_SETUP_PENDING'});
  }
}
module.exports = { poolFor, verifyDatabase, tlsOptions };
