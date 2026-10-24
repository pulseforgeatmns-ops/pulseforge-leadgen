const pool = require('../db');

async function ensureScoutUnenrichedTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_unenriched (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL,
      company TEXT,
      website_url TEXT,
      domain TEXT,
      vertical TEXT,
      location TEXT,
      source TEXT,
      enrichment_attempts INTEGER DEFAULT 1,
      last_attempt_at TIMESTAMPTZ DEFAULT NOW(),
      notes TEXT
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_scout_unenriched_client ON scout_unenriched(client_id)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_scout_unenriched_domain ON scout_unenriched(domain)
  `);
}

module.exports = { ensureScoutUnenrichedTable };
