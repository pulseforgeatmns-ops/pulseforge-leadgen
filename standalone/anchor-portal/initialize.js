'use strict';
const { config, MARKER } = require('./config');
const { poolFor } = require('./database');
async function initialize(pool, marker, hashes) {
  if (marker !== MARKER) throw new Error('Exact synthetic marker required');
  const keys = ['client','operator','cleaner','unassigned','foreignOperator','foreignClient'];
  if (!hashes || Object.keys(hashes).length !== keys.length || keys.some(k => !/^\$2[aby]\$12\$[./A-Za-z0-9]{53}$/.test(hashes[k] || ''))) throw new Error('Six explicitly provisioned bcrypt cost-12 hashes required');
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const { rows: [identity] } = await db.query('SELECT current_database() AS name, current_schema() AS schema');
    if (identity.name !== 'anchor_portal_synthetic' || identity.schema !== 'public') throw new Error('Dedicated public synthetic database required');
    await db.query("SELECT pg_advisory_xact_lock(91720261014)");
    const { rows } = await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')");
    if (rows.length) throw new Error('Initializer requires an entirely empty dedicated database');
    await db.query("CREATE TABLE portal_demo_marker(marker TEXT PRIMARY KEY); INSERT INTO portal_demo_marker VALUES ('anchor-portal-synthetic-only-v1')");
    await db.query("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('manager','cleaner','facility_client')), client_id INTEGER NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE)");
    await db.query('CREATE TABLE portal_sessions (sid VARCHAR NOT NULL PRIMARY KEY, sess JSON NOT NULL, expire TIMESTAMP(6) NOT NULL); CREATE INDEX portal_sessions_expire ON portal_sessions(expire)');
    await require('../../utils/anchorPortalSchema').createPortalSchema(db).ensureAnchorPortalTables(db);
    const fixtures = await require('./fixtures').seed(db, hashes);
    await db.query('COMMIT');
    return fixtures;
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}
if (require.main === module) {
  (async () => {
    const cfg = config(process.env);
    if (process.argv[2] !== '--initialize-synthetic-only') throw new Error('Explicit initialization flag required');
    let input = ''; for await (const chunk of process.stdin) { input += chunk; if(input.length > 4096) throw new Error('Oversized provisioning input'); }
    const pool = poolFor(cfg);
    try { await initialize(pool, cfg.marker, JSON.parse(input)); console.log('Synthetic database initialized. No credentials printed.'); }
    finally { await pool.end(); }
  })().catch(() => { console.error('Synthetic initialization failed; inspect configuration and empty dedicated database.'); process.exitCode = 1; });
}
module.exports = { initialize };
