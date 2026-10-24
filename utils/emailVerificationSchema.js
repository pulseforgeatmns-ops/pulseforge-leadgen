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
      email TEXT NOT NULL,
      prospect_id UUID,
      vendor TEXT NOT NULL,
      status TEXT NOT NULL,
      cost_credits NUMERIC,
      response_payload JSONB,
      duration_ms INTEGER,
      called_at TIMESTAMPTZ DEFAULT NOW()
    );

    ALTER TABLE verifier_call_log ADD COLUMN IF NOT EXISTS prospect_id UUID;
    ALTER TABLE verifier_call_log ADD COLUMN IF NOT EXISTS status TEXT;
    ALTER TABLE verifier_call_log ADD COLUMN IF NOT EXISTS cost_credits NUMERIC;
    ALTER TABLE verifier_call_log ADD COLUMN IF NOT EXISTS called_at TIMESTAMPTZ DEFAULT NOW();
    UPDATE verifier_call_log SET status = 'unknown' WHERE status IS NULL;
    ALTER TABLE verifier_call_log ALTER COLUMN status SET NOT NULL;
  `);
}

module.exports = { ensureEmailVerificationColumns };
