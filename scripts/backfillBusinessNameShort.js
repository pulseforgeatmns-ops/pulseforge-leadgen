require('dotenv').config();

const pool = require('../db');
const { deriveBusinessNameShort, ensureBusinessNameShortColumns } = require('../utils/businessNameShort');

function parseClientId() {
  const arg = process.argv.slice(2).find(item => item.startsWith('--client_id='));
  const parsed = parseInt(arg ? arg.split('=')[1] : '10', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 10;
}

async function run() {
  const clientId = parseClientId();
  await ensureBusinessNameShortColumns(pool);

  const { rows } = await pool.query(`
    SELECT id, name, business_name_short AS previous_business_name_short
    FROM companies
    WHERE client_id = $1
    ORDER BY name ASC, id ASC
  `, [clientId]);

  const report = [];
  for (const row of rows) {
    const derived = deriveBusinessNameShort(row.name);
    await pool.query(`
      UPDATE companies
      SET business_name_short = $1,
          business_name_short_confidence = $2,
          business_name_short_flags = $3::text[],
          updated_at = NOW()
      WHERE id = $4
        AND client_id = $5
    `, [
      derived.business_name_short,
      derived.confidence,
      derived.flags,
      row.id,
      clientId,
    ]);

    report.push({
      id: row.id,
      name: row.name,
      business_name_short: derived.business_name_short,
      confidence: derived.confidence,
      flags: derived.flags,
      stripped: derived.stripped,
      changed: row.previous_business_name_short !== derived.business_name_short,
    });
  }

  console.log(JSON.stringify({
    client_id: clientId,
    company_count: report.length,
    low_confidence: report.filter(item => item.confidence !== 'high'),
    companies: report,
  }, null, 2));
}

run()
  .catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
