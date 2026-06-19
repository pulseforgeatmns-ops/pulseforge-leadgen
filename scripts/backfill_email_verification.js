/**
 * Backfill Bouncer email verification for legacy prospects.
 * Usage: node scripts/backfill_email_verification.js
 *   [--client_id=1] [--statuses=cold,contacted] [--max=600]
 */
require('dotenv').config();
const pool = require('../db');
const { verifyEmail } = require('../utils/emailVerifier');

const MAX_CALLS = 2000;
const DEFAULT_COST_PER_CHECK = 0.008;

function parseClientId(argv) {
  const flag = argv.find(a => a.startsWith('--client_id='));
  if (flag) return Number.parseInt(flag.split('=')[1], 10) || null;
  return null;
}

function parseStatuses(argv) {
  const flag = argv.find(a => a.startsWith('--statuses='));
  if (!flag) return [];
  return flag.split('=')[1].split(',').map(value => value.trim()).filter(Boolean);
}

function parseMaxCalls(argv) {
  const flag = argv.find(a => a.startsWith('--max='));
  if (!flag) return MAX_CALLS;
  const requested = Number.parseInt(flag.split('=')[1], 10);
  if (!Number.isInteger(requested) || requested <= 0) {
    throw new Error('--max must be a positive integer');
  }
  return Math.min(requested, MAX_CALLS);
}

function shouldMarkDnc(status) {
  return ['invalid', 'catchall', 'risky'].includes(status);
}

function buildVerifierNote(result) {
  return `Email verifier marked ${result.status}${result.reason ? ` (${result.reason})` : ''}; outbound disabled.`;
}

async function updateProspect(row, result) {
  const markDnc = shouldMarkDnc(result.status);
  const note = markDnc ? buildVerifierNote(result) : null;

  await pool.query(`
    UPDATE prospects
    SET email_verified = $1,
        email_verification_method = $2,
        verified_at = NOW(),
        email_status = $3,
        verifier_response = $4::jsonb,
        verifier_checked_at = NOW(),
        do_not_contact = CASE WHEN $5::boolean THEN true ELSE do_not_contact END,
        notes = CASE
          WHEN $5::boolean THEN CONCAT_WS(E'\n', NULLIF(notes, ''), $6)
          ELSE notes
        END
    WHERE id = $7
  `, [
    result.valid,
    result.method,
    result.status,
    JSON.stringify(result.raw || null),
    markDnc,
    note,
    row.id,
  ]);

  return markDnc;
}

async function run() {
  const argv = process.argv.slice(2);
  const clientId = parseClientId(argv);
  const statuses = parseStatuses(argv);
  const maxCalls = parseMaxCalls(argv);
  const params = [];
  let clientFilter = '';
  let statusFilter = '';
  if (clientId) {
    params.push(clientId);
    clientFilter = `AND client_id = $${params.length}`;
  }
  if (statuses.length) {
    params.push(statuses);
    statusFilter = `AND status = ANY($${params.length}::text[])`;
  }
  params.push(maxCalls);
  const limitPlaceholder = `$${params.length}`;

  const { rows } = await pool.query(`
    SELECT id, email
    FROM prospects
    WHERE email IS NOT NULL
      AND TRIM(email) <> ''
      AND (email_status IS NULL OR email_status = 'unverified_legacy')
      AND (verifier_checked_at IS NULL OR verifier_checked_at <= NOW() - INTERVAL '30 days')
      ${clientFilter}
      ${statusFilter}
    ORDER BY id ASC
    LIMIT ${limitPlaceholder}
  `, params);

  console.log(`[backfill_email_verification] Processing ${rows.length} prospect(s)${clientId ? ` for client_id=${clientId}` : ''}`);

  const breakdown = {};
  let verified = 0;
  let markedDnc = 0;
  let calls = 0;

  for (let i = 0; i < rows.length; i++) {
    if (calls >= maxCalls) {
      console.error(`[backfill_email_verification] Stopping because call ceiling reached at ${maxCalls}`);
      break;
    }

    const row = rows[i];
    calls++;
    const result = await verifyEmail(row.email);
    const didMarkDnc = await updateProspect(row, result);

    verified++;
    if (didMarkDnc) markedDnc++;
    breakdown[result.status] = (breakdown[result.status] || 0) + 1;

    if (verified % 50 === 0) {
      console.log(`[backfill_email_verification] Progress: ${verified}/${rows.length} verified, ${markedDnc} marked DNC`);
    }
  }

  const costPerCheck = Number.parseFloat(process.env.BOUNCER_ESTIMATED_COST_PER_CHECK || String(DEFAULT_COST_PER_CHECK));
  const estimatedCost = Number.isFinite(costPerCheck) ? calls * costPerCheck : 0;

  console.log('[backfill_email_verification] Final summary');
  console.log(`Total verified: ${verified}`);
  console.log(`Breakdown by status: ${JSON.stringify(breakdown)}`);
  console.log(`Total marked DNC: ${markedDnc}`);
  console.log(`Estimated cost: $${estimatedCost.toFixed(2)} at $${costPerCheck.toFixed(4)} per check`);
}

run()
  .then(() => process.exit(0))
  .catch(err => {
    console.error(`[backfill_email_verification] Fatal: ${err.message}`);
    process.exit(1);
  });
