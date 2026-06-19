const pool = require('../db');

async function ensureEmailVerificationColumns() {
  await pool.query(`
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT false;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_verification_method TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_status TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS verifier_response JSONB;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS verifier_checked_at TIMESTAMPTZ;

    CREATE TABLE IF NOT EXISTS verifier_call_log (
      id BIGSERIAL PRIMARY KEY,
      vendor TEXT NOT NULL,
      email TEXT NOT NULL,
      response_payload JSONB,
      duration_ms INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

module.exports = { ensureEmailVerificationColumns };
