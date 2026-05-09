require('dotenv').config();
const pool = require('./db');
const fs = require('fs');

// Known opens from Brevo CSV export — prospects only (no jacob@gopulseforge.com)
const OPENS = [
  { email: 'richard.parillo@buildingonellc.com', subject: 'still thinking about Buildingonellc', ts: '2026-05-04T20:41:33Z' },
  { email: 'richard.parillo@buildingonellc.com', subject: 'still thinking about Buildingonellc', ts: '2026-05-04T20:41:53Z' },
  { email: 'richard.parillo@buildingonellc.com', subject: 'still thinking about Buildingonellc', ts: '2026-05-04T20:53:23Z' },
  { email: 'richard.parillo@buildingonellc.com', subject: 'still thinking about Buildingonellc', ts: '2026-05-05T14:23:13Z' },
  { email: '102salonnorth@gmail.com',            subject: '102salonnorth — honest question',      ts: '2026-05-04T18:57:35Z' },
  { email: '102salonnorth@gmail.com',            subject: '102salonnorth — honest question',      ts: '2026-05-04T20:46:10Z' },
  { email: '102salonnorth@gmail.com',            subject: '102salonnorth — honest question',      ts: '2026-05-05T12:26:37Z' },
  { email: '102salonnorth@gmail.com',            subject: '102salonnorth — honest question',      ts: '2026-05-05T19:44:47Z' },
  { email: 'stylist@towerofcurls.com',           subject: 'Towerofcurls — honest question',       ts: '2026-05-04T18:29:04Z' },
  { email: 'stylist@towerofcurls.com',           subject: 'Towerofcurls — honest question',       ts: '2026-05-05T14:53:29Z' },
  { email: 'stylist@towerofcurls.com',           subject: 'Towerofcurls — honest question',       ts: '2026-05-05T14:53:52Z' },
  { email: 'wynwoodhairstudio@gmail.com',        subject: 'Wynwoodhairstudionh — honest question', ts: '2026-05-04T18:29:06Z' },
  { email: 'wynwoodhairstudio@gmail.com',        subject: 'Wynwoodhairstudionh — honest question', ts: '2026-05-05T05:27:38Z' },
  { email: 'info@salonelavina.com',              subject: 'Salonelavina — honest question',        ts: '2026-05-04T18:29:17Z' },
  { email: 'info@salonelavina.com',              subject: 'Salonelavina — honest question',        ts: '2026-05-04T19:35:23Z' },
  { email: 'info@humblewarriorpoweryoga.com',    subject: "what's actually working in Manchester right now", ts: '2026-05-04T15:39:53Z' },
  { email: 'info@humblewarriorpoweryoga.com',    subject: "what's actually working in Manchester right now", ts: '2026-05-04T15:42:12Z' },
  { email: 'info@humblewarriorpoweryoga.com',    subject: "what's actually working in Manchester right now", ts: '2026-05-04T15:44:49Z' },
  { email: 'tthairsalon245@gmail.com',           subject: 'Tthairsalon — honest question',         ts: '2026-05-04T10:06:00Z' },
  { email: 'tthairsalon245@gmail.com',           subject: 'Tthairsalon — honest question',         ts: '2026-05-04T18:29:01Z' },
  { email: 'oleamanchester@gmail.com',           subject: 'Oleamanchester — honest question',       ts: '2026-05-04T18:29:16Z' },
  { email: 'amanda@dizscafe.com',                subject: 'still thinking about Dizscafe',          ts: '2026-05-05T11:03:51Z' },
  { email: 'william@oxifresh.com',               subject: 'Carpet Cleaning in Manchester, NH — honest question', ts: '2026-05-08T13:14:20Z' },
  { email: 'office@barrelifenh.com',             subject: "what's actually working in Manchester right now", ts: '2026-05-05T08:56:57Z' },
];

const DRY_RUN = process.argv.includes('--dry-run');

async function run() {
  console.log(DRY_RUN ? '--- DRY RUN ---\n' : '--- LIVE ---\n');
  let inserted = 0, skipped = 0, notFound = 0;

  for (const open of OPENS) {
    const email = open.email.toLowerCase().trim();

    const prospectRes = await pool.query(
      `SELECT id, status FROM prospects WHERE LOWER(email) = $1 LIMIT 1`, [email]
    );

    if (!prospectRes.rows.length) {
      console.log(`NOT FOUND: ${email}`);
      notFound++;
      continue;
    }

    const prospect = prospectRes.rows[0];

    // Check if this exact touchpoint already exists (avoid double-insert)
    const exists = await pool.query(`
      SELECT 1 FROM touchpoints
      WHERE prospect_id = $1
        AND action_type = 'email_opened'
        AND content_summary = $2
        AND created_at BETWEEN $3::timestamptz - INTERVAL '5 minutes'
                           AND $3::timestamptz + INTERVAL '5 minutes'
    `, [prospect.id, open.subject, open.ts]);

    if (exists.rows.length) {
      console.log(`SKIP (exists): ${email} — ${open.subject.slice(0, 40)}`);
      skipped++;
      continue;
    }

    if (DRY_RUN) {
      console.log(`WOULD INSERT: ${email} | ${open.subject.slice(0, 40)} | ${open.ts}`);
    } else {
      await pool.query(`
        INSERT INTO touchpoints
          (prospect_id, channel, action_type, content_summary, outcome, sentiment, created_at)
        VALUES ($1, 'email', 'email_opened', $2, '{"source":"brevo_csv_backfill"}', 'neutral', $3)
      `, [prospect.id, open.subject, open.ts]);
      console.log(`INSERTED: ${email} | ${open.subject.slice(0, 40)}`);
      inserted++;
    }
  }

  console.log(`\nDone — ${inserted} inserted, ${skipped} skipped, ${notFound} not found in DB`);
  await pool.end();
}

run().catch(err => { console.error(err); process.exit(1); });
