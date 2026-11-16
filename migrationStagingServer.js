'use strict';
// Isolated migration probe. No application routes, schedulers, Stripe handlers, or schema writes.
const http = require('node:http');
const { Pool } = require('pg');
const port = Number(process.env.PORT || 10000);
const server = http.createServer(async (req, res) => {
  if (req.url !== '/healthz') { res.writeHead(404); return res.end('Not found'); }
  if (!process.env.DATABASE_URL) { res.writeHead(503); return res.end('Database not configured'); }
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 1,
    connectionTimeoutMillis: 4000, query_timeout: 4000,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false } });
  try {
    await pool.query('SELECT 1');
    res.writeHead(200, { 'content-type': 'text/plain' }); res.end('staging database reachable');
  } catch (error) {
    console.error('[migration-probe] database connection failed:', error.code || error.name);
    res.writeHead(503); res.end('Database unavailable');
  } finally { await pool.end().catch(() => {}); }
});
server.listen(port, '0.0.0.0', () => console.log('[migration-probe] isolated read-only health service listening'));
