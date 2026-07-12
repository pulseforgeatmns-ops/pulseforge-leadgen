const pool = require('../db');

const SCOUT_SKIP_REASONS = Object.freeze({
  DUPLICATE: 'duplicate', OUT_OF_AREA: 'out_of_area', INSERT_CONFLICT: 'insert_conflict',
  DB_ERROR: 'db_error', NO_EMAIL: 'no_email', LOW_SCORE: 'low_score',
  MISSING_REQUIRED_FIELD: 'missing_required_field', INVALID_PROSPECT: 'invalid_prospect',
  PRE_ENRICHMENT_REJECT: 'pre_enrichment_reject',
  B2C_CLASSIFICATION: 'b2c_classification',
  LOW_CONFIDENCE_B2B: 'low_confidence_b2b',
});

async function ensureScoutSkipLogTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_skip_log (
      id BIGSERIAL PRIMARY KEY, run_id TEXT, client_id INTEGER, vertical TEXT,
      location TEXT, search_query TEXT, discovery_method TEXT, skip_reason TEXT NOT NULL,
      candidate_identifier TEXT, detail JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_scout_skip_log_client_reason_created ON scout_skip_log (client_id, skip_reason, created_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_scout_skip_log_run ON scout_skip_log (run_id)`);
  await pool.query(`
    CREATE OR REPLACE VIEW scout_skip_summary_7d AS
    SELECT DATE(created_at) AS day, client_id, vertical, location, skip_reason, COUNT(*) AS skip_count
    FROM scout_skip_log WHERE created_at > NOW() - INTERVAL '7 days'
    GROUP BY DATE(created_at), client_id, vertical, location, skip_reason
    ORDER BY day DESC, client_id, skip_count DESC
  `);
}

async function logScoutSkip({ runId, clientId, vertical, location, searchQuery, discoveryMethod, skipReason, candidateIdentifier, detail }) {
  try {
    await pool.query(`
      INSERT INTO scout_skip_log (
        run_id, client_id, vertical, location, search_query, discovery_method,
        skip_reason, candidate_identifier, detail
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
    `, [runId == null ? null : String(runId), clientId || null, vertical || null,
      location || null, searchQuery || null, discoveryMethod || null, skipReason,
      candidateIdentifier || null, JSON.stringify(detail || {})]);
    return true;
  } catch (err) {
    console.error('[Scout] scout_skip_log write failed:', err.message);
    return false;
  }
}

module.exports = { SCOUT_SKIP_REASONS, ensureScoutSkipLogTable, logScoutSkip };
