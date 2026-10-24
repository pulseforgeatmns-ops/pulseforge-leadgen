const pool = require('../db');

const RETIRED_MIRA_CLIENT_IDS = Object.freeze([2, 5]);

async function enforceMiraClientState() {
  await pool.query(`
    UPDATE clients
    SET active = false,
        enabled_agents = ARRAY[]::text[]
    WHERE id = ANY($1::int[])
  `, [RETIRED_MIRA_CLIENT_IDS]);
}

module.exports = { RETIRED_MIRA_CLIENT_IDS, enforceMiraClientState };
