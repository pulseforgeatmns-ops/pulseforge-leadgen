const pool = require('../db');

async function ensureEmailVerificationColumns() {
  await pool.query(`
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT false;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_verification_method TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
  `);
}

module.exports = { ensureEmailVerificationColumns };
