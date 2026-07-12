#!/usr/bin/env node
require('dotenv').config({ quiet: true });

const pool = require('../db');
const { getClientConfig } = require('../utils/clientContext');
const { previewRecalculateICP } = require('../utils/icpScoring');

async function main() {
  const client = await getClientConfig(1);
  const { rows } = await pool.query(`
    SELECT id, icp_score
    FROM prospects
    WHERE client_id = 1
      AND vertical = 'property_management'
      AND icp_score BETWEEN 60 AND 79
    ORDER BY icp_score DESC, updated_at DESC
    LIMIT 5
  `);
  const breakdowns = [];
  for (const row of rows) {
    breakdowns.push(await previewRecalculateICP(row.id, { clientId: 1, clientConfig: client }));
  }
  process.stdout.write(`${JSON.stringify({ requested: 5, found: breakdowns.length, breakdowns }, null, 2)}\n`);
}

main().catch(err => {
  console.error(err.stack || err.message);
  process.exitCode = 1;
}).finally(() => pool.end());
