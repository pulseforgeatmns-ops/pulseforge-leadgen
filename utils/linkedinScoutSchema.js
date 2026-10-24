const pool = require('../db');

async function ensureLinkedInScoutSchema() {
  await pool.query(`
    ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS linkedin_headline TEXT,
      ADD COLUMN IF NOT EXISTS linkedin_location TEXT,
      ADD COLUMN IF NOT EXISTS linkedin_source_query JSONB;

    CREATE UNIQUE INDEX IF NOT EXISTS prospects_linkedin_url_client_uniq
      ON prospects (client_id, linkedin_url)
      WHERE linkedin_url IS NOT NULL;
  `);
}

module.exports = { ensureLinkedInScoutSchema };
