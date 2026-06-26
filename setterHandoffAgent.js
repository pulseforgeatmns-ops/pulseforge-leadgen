'use strict';

require('dotenv').config();

const pool = require('./db');
const { appendQualifiedScoutLead, hasSetterSheetAuth } = require('./utils/setterSheet');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const AGENT_NAME = 'handoff_utility';
const SETTER_ICP_THRESHOLD = 70;
const TERMINAL_PROSPECT_STATUSES = ['dead', 'disqualified', 'bounced', 'do_not_email'];

async function ensureSetterQueueColumns() {
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_visible BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
}

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
  const clientId = getRuntimeClientId(params);
  const clientConfig = await getClientConfig(clientId);
  if (!clientConfig) throw new Error(`Active client not found: ${clientId}`);

  console.log('\nSetter Handoff Agent');
  console.log('─────────────────────────────────');
  console.log(`Lookback: ${lookbackDays} day(s)\n`);
  console.log(`Client: ${clientId}\n`);

  if (clientId !== 1) {
    console.log('Setter handoff is enabled only for Pulseforge client_id=1.');
    return;
  }

  await ensureSetterQueueColumns();

  const cleanup = await pool.query(`
    UPDATE prospects
    SET setter_visible = false,
        setter_updated_at = NOW()
    WHERE client_id = $1
      AND COALESCE(setter_visible, false) = true
      AND (
        COALESCE(icp_score, 0) < $2
        OR COALESCE(do_not_contact, false) = true
        OR COALESCE(status, '') = ANY($3::text[])
        OR NULLIF(BTRIM(COALESCE(service_area_match, '')), '') IS NULL
      )
    RETURNING id
  `, [clientId, SETTER_ICP_THRESHOLD, TERMINAL_PROSPECT_STATUSES]);

  const { rows } = await pool.query(`
    SELECT id, first_name, last_name, email, phone, job_title, notes, vertical, icp_score, created_at
    FROM prospects
    WHERE source = 'scout'
      AND COALESCE(icp_score, 0) >= $3
      AND COALESCE(do_not_contact, false) = false
      AND COALESCE(status, '') <> ALL($4::text[])
      AND NULLIF(BTRIM(COALESCE(service_area_match, '')), '') IS NOT NULL
      AND client_id = $2
      AND created_at >= NOW() - ($1::int * INTERVAL '1 day')
    ORDER BY icp_score DESC NULLS LAST, created_at DESC
  `, [lookbackDays, clientId, SETTER_ICP_THRESHOLD, TERMINAL_PROSPECT_STATUSES]);

  if (rows.length) {
    await pool.query(`
      UPDATE prospects
      SET setter_status = COALESCE(setter_status, 'new'),
          setter_visible = true,
          setter_updated_at = NOW()
      WHERE id = ANY($1::uuid[]) AND client_id = $2
        AND COALESCE(icp_score, 0) >= $3
        AND COALESCE(do_not_contact, false) = false
        AND COALESCE(status, '') <> ALL($4::text[])
        AND NULLIF(BTRIM(COALESCE(service_area_match, '')), '') IS NOT NULL
    `, [rows.map(row => row.id), clientId, SETTER_ICP_THRESHOLD, TERMINAL_PROSPECT_STATUSES]);
  }

  let appended = 0;
  let skipped = 0;
  let failed = 0;

  const canWriteSheet = hasSetterSheetAuth();
  if (!canWriteSheet) {
    skipped = rows.length;
    console.warn('[setter_handoff] Missing Google Sheets auth; DB setter visibility was updated but no sheet rows were written.');
  }

  for (let i = 0; i < rows.length; i++) {
    const stillActive = await getClientConfig(clientId);
    if (!stillActive) {
      throw new Error(`[setterHandoff] Client ${clientId} deactivated mid-run — aborting at item ${i + 1}/${rows.length} after ${appended} handoffs processed`);
    }

    if (!canWriteSheet) break;
    const prospect = rows[i];
    const lead = leadFromProspect(prospect);
    try {
      const handoff = await appendQualifiedScoutLead(lead, prospect.vertical);
      if (handoff.appended) {
        appended++;
        await pool.query(`
          INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
          VALUES ($1, 'queue_setter_lead', $2, $3, 'success', NOW(), $4)
        `, [
          AGENT_NAME,
          prospect.id,
          JSON.stringify({ business_name: lead.company, vertical: prospect.vertical, score: lead.score }),
          clientId,
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
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, 'backfill_setter_queue', $2, $3, NOW(), $4)
  `, [
    AGENT_NAME,
    JSON.stringify({ candidates: rows.length, hidden_below_threshold: cleanup.rowCount, appended, skipped, failed, lookbackDays, client_id: clientId, icp_threshold: SETTER_ICP_THRESHOLD }),
    failed ? 'failed' : 'success',
    clientId,
  ]).catch(() => {});

  console.log(`Hidden <${SETTER_ICP_THRESHOLD}: ${cleanup.rowCount}`);
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
