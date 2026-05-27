require('dotenv').config();
const pool = require('./db');
const fs = require('fs');

// One-time backfill: promote client_id=1 prospects from 'cold' to 'contacted'
// when they already have an 'email_sent' agent_log row (i.e. Emmett has
// reached out). Matches on the agent_log.prospect_id FK — not a payload
// substring — so only genuinely contacted prospects are updated.
async function run() {
  const result = await pool.query(`
    UPDATE prospects p
    SET status = 'contacted', updated_at = NOW()
    WHERE p.client_id = 1
      AND p.status = 'cold'
      AND EXISTS (
        SELECT 1 FROM agent_log a
        WHERE a.action = 'email_sent'
          AND a.prospect_id = p.id
      )
  `);

  console.log(`Backfill complete — ${result.rowCount} prospect(s) promoted cold -> contacted.`);

  await pool.end();

  // Self-delete: this is a one-time script.
  fs.unlinkSync(__filename);
  console.log(`Removed one-time script: ${__filename}`);
}

run().catch(err => { console.error(err); process.exit(1); });
