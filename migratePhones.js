require('dotenv').config();
const pool = require('./db');

const DRY_RUN = process.argv.includes('--dry-run');
const PHONE_RE = /\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}/g;

function extractPhone(notes) {
  const matches = notes.match(PHONE_RE);
  if (!matches) return null;
  const digits = matches[0].replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits[0] === '1') return `+${digits}`;
  return null;
}

async function run() {
  if (DRY_RUN) console.log('--- DRY RUN — no writes will happen ---\n');

  const { rows } = await pool.query(`
    SELECT id, notes FROM prospects
    WHERE phone IS NULL AND notes IS NOT NULL AND notes != ''
  `);

  console.log(`Found ${rows.length} prospects with no phone and non-empty notes.\n`);

  let updated = 0, skipped = 0;

  for (const row of rows) {
    const phone = extractPhone(row.notes);
    if (!phone) {
      console.log(`Skipped  ${row.id}: no phone found in "${row.notes}"`);
      skipped++;
      continue;
    }
    if (DRY_RUN) {
      console.log(`Would update  ${row.id}: ${phone}  ←  "${row.notes}"`);
    } else {
      await pool.query(`UPDATE prospects SET phone = $1 WHERE id = $2`, [phone, row.id]);
      console.log(`Updated  ${row.id}: extracted ${phone} from "${row.notes}"`);
    }
    updated++;
  }

  const verb = DRY_RUN ? 'would be updated' : 'updated';
  console.log(`\nDone — ${updated} ${verb}, ${skipped} skipped (no phone found)`);
  await pool.end();
}

run().catch(err => { console.error(err); process.exit(1); });
