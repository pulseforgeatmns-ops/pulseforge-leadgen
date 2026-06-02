const pool = require('../db');
const { ensureClientArchitecture } = require('./clientContext');

async function ensureCloserSchema() {
  await ensureClientArchitecture();
  await pool.query('SELECT pg_advisory_lock(91720260517)');
  try {
    // Drop every CHECK constraint on users that references `role`, then add
    // the canonical one that includes dashboard roles beyond admin/manager. The old version of this code
    // hardcoded the constraint name and excluded 'sales', which threw
    // "check constraint ... is violated by some row" once any user had
    // role='sales' (Levi).
    const { rows: existing } = await pool.query(`
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class cls ON cls.oid = con.conrelid
      WHERE cls.relname = 'users'
        AND con.contype = 'c'
        AND pg_get_constraintdef(con.oid) ILIKE '%role%'
    `);
    for (const row of existing) {
      await pool.query(`ALTER TABLE users DROP CONSTRAINT IF EXISTS "${row.conname}"`);
    }
    await pool.query(`
      ALTER TABLE users ADD CONSTRAINT users_role_check
      CHECK (role IN ('admin', 'manager', 'setter', 'closer', 'sales', 'viewer', 'client'))
    `);
  } finally {
    await pool.query('SELECT pg_advisory_unlock(91720260517)');
  }
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS closer_id INTEGER REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS booked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS mrr_value NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS close_notes TEXT,
    ADD COLUMN IF NOT EXISTS closer_status TEXT DEFAULT 'booked'
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS commissions (
      id SERIAL PRIMARY KEY,
      closer_id INTEGER REFERENCES users(id),
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER REFERENCES clients(id),
      mrr_amount NUMERIC(10,2),
      commission_rate NUMERIC(5,4) DEFAULT 0.15,
      commission_amt NUMERIC(10,2) GENERATED ALWAYS AS (mrr_amount * commission_rate) STORED,
      status TEXT DEFAULT 'pending' CHECK (status IN ('pending','paid','void')),
      closed_at TIMESTAMPTZ,
      paid_at TIMESTAMPTZ,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

module.exports = { ensureCloserSchema };
