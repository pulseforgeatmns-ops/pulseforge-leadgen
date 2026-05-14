'use strict';

require('dotenv').config();

const pool = require('./db');
const { appendQualifiedScoutLead, hasSetterSheetAuth } = require('./utils/setterSheet');

const AGENT_NAME = 'setter_handoff';

function leadFromProspect(row) {
  const [businessName, website = ''] = String(row.notes || '').split(' — ');
  const contact = `${row.first_name || ''} ${row.last_name || ''}`.trim();
  return {
    company: businessName || contact || row.email || 'Unknown business',
    contact: contact && contact !== 'there' ? contact : '',
    title: row.job_title || '',
    email: row.email || '',
    phone: row.phone || '',
    url: website || '',
    score: row.icp_score || 0,
  };
}

async function run(params = {}) {
  const lookbackDays = Number(params.lookbackDays || process.env.SETTER_HANDOFF_LOOKBACK_DAYS || 7);
  console.log('\nSetter Handoff Agent');
  console.log('─────────────────────────────────');
  console.log(`Lookback: ${lookbackDays} day(s)\n`);

  if (!hasSetterSheetAuth()) {
    console.warn('[setter_handoff] Missing Google Sheets auth; no setter rows can be written in this environment.');
    return;
  }

  const { rows } = await pool.query(`
    SELECT id, first_name, last_name, email, phone, job_title, notes, vertical, icp_score, created_at
    FROM prospects
    WHERE source = 'scout'
      AND COALESCE(icp_score, 0) >= 40
      AND COALESCE(do_not_contact, false) = false
      AND created_at >= NOW() - ($1::int * INTERVAL '1 day')
    ORDER BY icp_score DESC NULLS LAST, created_at DESC
  `, [lookbackDays]);

  let appended = 0;
  let skipped = 0;
  let failed = 0;

  for (const prospect of rows) {
    const lead = leadFromProspect(prospect);
    try {
      const handoff = await appendQualifiedScoutLead(lead, prospect.vertical);
      if (handoff.appended) {
        appended++;
        await pool.query(`
          INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
          VALUES ($1, 'queue_setter_lead', $2, $3, 'success', NOW())
        `, [
          AGENT_NAME,
          prospect.id,
          JSON.stringify({ business_name: lead.company, vertical: prospect.vertical, score: lead.score }),
        ]);
      } else {
        skipped++;
      }
    } catch (err) {
      failed++;
      console.error(`[setter_handoff] Failed ${lead.company}: ${err.message}`);
    }
  }

  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
    VALUES ($1, 'backfill_setter_queue', $2, $3, NOW())
  `, [
    AGENT_NAME,
    JSON.stringify({ candidates: rows.length, appended, skipped, failed, lookbackDays }),
    failed ? 'error' : 'success',
  ]).catch(() => {});

  console.log(`Candidates: ${rows.length}`);
  console.log(`Appended:   ${appended}`);
  console.log(`Skipped:    ${skipped}`);
  console.log(`Failed:     ${failed}`);
  console.log('─────────────────────────────────\n');
}

module.exports = { run };

if (require.main === module) {
  run().catch(err => {
    console.error('[setter_handoff] Fatal error:', err.message);
    process.exit(1);
  });
}
