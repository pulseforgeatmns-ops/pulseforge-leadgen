require('dotenv').config();

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Railway requires TLS on its public connection path. Disposable/local
  // PostgreSQL validation can opt out explicitly; production remains TLS-on.
  ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false'
    ? false
    : { rejectUnauthorized: false }
});

module.exports = pool;
