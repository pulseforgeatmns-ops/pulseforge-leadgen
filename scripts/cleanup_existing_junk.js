require('dotenv').config();
const pool = require('../db');
const { shouldExcludeProspect, extractEmailDomain } = require('../utils/prospectFilter');

const TRACK1_MARKER = '[excluded by track1:';

async function logExcludedProspect({ email, source, exclusion, prospectId }) {
  await pool.query(`
    INSERT INTO excluded_prospect_log (email, domain, source, exclusion_reason, exclusion_detail)
    VALUES ($1, $2, $3, $4, $5::jsonb)
  `, [
    email || null,
    extractEmailDomain(email),
    source,
    exclusion.reason,
    JSON.stringify({
      prospect_id: prospectId,
      ...(exclusion.detail || {}),
    }),
  ]);
}

function increment(map, key) {
  map.set(key, (map.get(key) || 0) + 1);
}

async function run() {
  const { rows } = await pool.query(`
    SELECT id, email, website_url, source, status, do_not_contact, notes
    FROM prospects
    WHERE email IS NOT NULL
      AND TRIM(email) <> ''
    ORDER BY id ASC
  `);

  const byReason = new Map();
  let scanned = 0;
  let excluded = 0;
  let alreadyMarked = 0;
  let notActionable = 0;

  for (const prospect of rows) {
    scanned++;

    if (prospect.do_not_contact === true && String(prospect.notes || '').includes(TRACK1_MARKER)) {
      alreadyMarked++;
      if (scanned % 200 === 0) {
        console.log(`Progress: ${scanned}/${rows.length} scanned, ${excluded} excluded`);
      }
      continue;
    }

    const exclusion = await shouldExcludeProspect({
      email: prospect.email,
      websiteUrl: prospect.website_url,
      source: prospect.source || null,
    });

    if (!exclusion.excluded) {
      if (scanned % 200 === 0) {
        console.log(`Progress: ${scanned}/${rows.length} scanned, ${excluded} excluded`);
      }
      continue;
    }

    if (prospect.status === 'closed' || prospect.do_not_contact === true) {
      notActionable++;
      if (scanned % 200 === 0) {
        console.log(`Progress: ${scanned}/${rows.length} scanned, ${excluded} excluded`);
      }
      continue;
    }

    const marker = ` [excluded by track1: ${exclusion.reason}]`;
    await pool.query(`
      UPDATE prospects
      SET do_not_contact = true,
          status = 'dead',
          notes = CASE
            WHEN COALESCE(notes, '') LIKE $1 THEN notes
            ELSE COALESCE(notes, '') || $2
          END,
          updated_at = NOW()
      WHERE id = $3
        AND COALESCE(status, '') <> 'closed'
        AND COALESCE(do_not_contact, false) = false
    `, [`%${TRACK1_MARKER}%`, marker, prospect.id]);

    await logExcludedProspect({
      email: prospect.email,
      source: 'cleanup_pass',
      exclusion,
      prospectId: prospect.id,
    });

    excluded++;
    increment(byReason, exclusion.reason);

    if (scanned % 200 === 0) {
      console.log(`Progress: ${scanned}/${rows.length} scanned, ${excluded} excluded`);
    }
  }

  console.log(`Total scanned: ${scanned}`);
  console.log(`Excluded count: ${excluded}`);
  console.log(`Already marked: ${alreadyMarked}`);
  console.log(`Skipped closed or DNC: ${notActionable}`);
  console.log('Excluded by reason:');
  for (const [reason, count] of [...byReason.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    console.log(`${reason}: ${count}`);
  }
}

if (require.main === module) {
  run()
    .catch(err => {
      console.error('[cleanup_existing_junk] Fatal:', err.message);
      process.exitCode = 1;
    })
    .finally(async () => {
      await pool.end();
    });
}

module.exports = { run };
