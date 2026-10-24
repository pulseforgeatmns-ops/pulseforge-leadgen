/**
 * Backfill MX + role-pattern validation for existing cold prospects.
 * Usage: node scripts/backfillMxValidation.js [--client_id=1]
 */
require('dotenv').config();
const pool = require('../db');
const { validateEmail, clearMxCache, extractDomain } = require('../utils/emailValidation');
const { ensureEmailVerificationColumns } = require('../utils/emailVerificationSchema');

function parseClientId(argv) {
  const flag = argv.find(a => a.startsWith('--client_id='));
  if (flag) return Number.parseInt(flag.split('=')[1], 10) || null;
  return null;
}

async function run() {
  const clientId = parseClientId(process.argv.slice(2));
  await ensureEmailVerificationColumns();

  const params = [];
  let clientFilter = '';
  if (clientId) {
    params.push(clientId);
    clientFilter = 'AND client_id = $1';
  }

  const { rows } = await pool.query(`
    SELECT id, email, discovery_method
    FROM prospects
    WHERE email IS NOT NULL
      AND TRIM(email) <> ''
      AND status = 'cold'
      AND verified_at IS NULL
      ${clientFilter}
    ORDER BY id ASC
  `, params);

  console.log(`[backfillMxValidation] Processing ${rows.length} prospect(s)${clientId ? ` for client_id=${clientId}` : ''}`);

  clearMxCache();
  const domainBatch = new Map();
  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const domain = extractDomain(row.email);
    if (domain && domainBatch.has(domain)) {
      const cached = domainBatch.get(domain);
      await pool.query(`
        UPDATE prospects
        SET email_verified = $1,
            email_verification_method = $2,
            verified_at = NOW(),
            do_not_contact = CASE WHEN $3 THEN true ELSE do_not_contact END
        WHERE id = $4
      `, [cached.email_verified, cached.email_verification_method, cached.do_not_contact, row.id]);
      updated++;
    } else {
      const result = await validateEmail(row.email);
      let emailVerified = false;
      let method = 'mx_lookup_failed';
      let doNotContact = false;

      if (!result.valid) {
        method = result.reason === 'no_mx_record' ? 'no_mx_record' : 'invalid_format';
        doNotContact = result.reason === 'no_mx_record';
      } else if (result.isRole) {
        method = 'mx_lookup_role';
        emailVerified = false;
      } else {
        emailVerified = true;
        method = 'mx_lookup';
      }

      const record = { email_verified: emailVerified, email_verification_method: method, do_not_contact: doNotContact };
      if (domain) domainBatch.set(domain, record);

      await pool.query(`
        UPDATE prospects
        SET email_verified = $1,
            email_verification_method = $2,
            verified_at = NOW(),
            do_not_contact = CASE WHEN $3 THEN true ELSE do_not_contact END
        WHERE id = $4
      `, [emailVerified, method, doNotContact, row.id]);
      updated++;
    }

    if ((i + 1) % 100 === 0) {
      console.log(`[backfillMxValidation] Progress: ${i + 1}/${rows.length} (${updated} updated, ${skipped} skipped)`);
    }
  }

  console.log(`[backfillMxValidation] Done — ${updated} updated, ${skipped} skipped, ${domainBatch.size} unique domains cached`);
}

run()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('[backfillMxValidation] Fatal:', err.message);
    process.exit(1);
  });
