'use strict';
// Isolated migration probe. No application routes, schedulers, Stripe handlers, or schema writes.
const http = require('node:http');
const { Pool } = require('pg');
const {validateMigrationDatabaseUrl,EXPECTED_STAGING_DB}=require('./scripts/migration/validateMigrationDatabaseUrl');
const port = Number(process.env.PORT || 10000);
const server = http.createServer(async (req, res) => {
  if (req.url !== '/healthz') { res.writeHead(404); return res.end('Not found'); }
  try { validateMigrationDatabaseUrl(process.env.DATABASE_URL); }
  catch (error) {
    console.error('[migration-probe] unexpected database target:',error.message);
    res.writeHead(503); return res.end('Isolated Render database not configured');
  }
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 1,
    connectionTimeoutMillis: 4000, query_timeout: 4000,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false } });
  try {
    const identity=await pool.query('SELECT current_database() AS name');
    if(identity.rows[0]?.name!==EXPECTED_STAGING_DB)throw Error('unexpected_database_identity');
    res.writeHead(200, { 'content-type': 'text/plain' }); res.end('staging database reachable');
  } catch (error) {
    console.error('[migration-probe] database connection failed:', error.code || error.name);
    res.writeHead(503); res.end('Database unavailable');
  } finally { await pool.end().catch(() => {}); }
});
try {
  validateMigrationDatabaseUrl(process.env.DATABASE_URL);
  console.log('[migration-probe] isolated destination URL matches approved database name');
} catch (error) {
  console.error('[migration-probe] unsafe destination URL:',error.message);
}

async function auditStagingDatabaseReadOnly() {
  let pool;
  try {
    validateMigrationDatabaseUrl(process.env.DATABASE_URL);
    pool = new Pool({connectionString:process.env.DATABASE_URL,max:1,
      connectionTimeoutMillis:5000,query_timeout:12000,
      ssl:String(process.env.DATABASE_SSL||'').toLowerCase()==='false'?false:{rejectUnauthorized:false}});
    const sql = `SELECT current_database() AS db_name,
      current_setting('server_version_num')::int AS version_num,
      pg_database_size(current_database())::text AS size_bytes,
      (SELECT count(*)::int FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
       WHERE n.nspname NOT IN ('pg_catalog','information_schema')
         AND n.nspname NOT LIKE 'pg_toast%'
         AND c.relkind IN ('r','p','v','m','S')) AS non_system_objects`;
    const result=await pool.query(sql);
    const row=result.rows[0];
    if(row.db_name!==EXPECTED_STAGING_DB || Number(row.version_num)<180000 || Number(row.version_num)>=190000)
      throw Error('unexpected_connected_database_identity_or_version');
    console.log('[migration-probe] read-only identity confirmed', JSON.stringify({
      database:row.db_name,postgresVersion:Number(row.version_num),
      nonSystemObjects:Number(row.non_system_objects),sizeBytes:row.size_bytes
    }));
  } catch(error) {
    console.error('[migration-probe] read-only identity check failed:',error.code||error.message||error.name);
  } finally {
    if(pool)await pool.end().catch(()=>{});
  }
}
void auditStagingDatabaseReadOnly();

server.listen(port, '0.0.0.0', () => console.log('[migration-probe] isolated read-only health service listening'));
