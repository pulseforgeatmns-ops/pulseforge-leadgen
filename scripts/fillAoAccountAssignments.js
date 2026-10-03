'use strict';

/**
 * Backfill AO CRM account assignments for Anchor (client_id=10) per SPEC-250 segment affinity.
 *
 * Review: node scripts/fillAoAccountAssignments.js
 * Apply:  node scripts/fillAoAccountAssignments.js --apply --confirm=anchor-ao-account-fill
 */

require('dotenv').config();

const pool = require('../db');
const {
  fillActiveAoAccounts,
  deprioritizeAccountForContactResearch,
  assertMinimumAoAccountCounts,
  DEFAULT_MIN_ACCOUNTS_PER_AO,
} = require('../utils/aoAccountFill');

const CLIENT_ID = 10;
const APPLY_CONFIRMATION = 'anchor-ao-account-fill';

function parseArgs(argv) {
  const apply = argv.includes('--apply');
  const confirm = argv.find(a => a.startsWith('--confirm='))?.split('=')[1] || null;
  const min = Number(argv.find(a => a.startsWith('--min='))?.split('=')[1] || DEFAULT_MIN_ACCOUNTS_PER_AO);
  const target = Number(argv.find(a => a.startsWith('--target='))?.split('=')[1] || 15);
  const deprioritizeGiant = !argv.includes('--skip-giant-deprioritize');
  return { apply, confirm, min, target, deprioritizeGiant };
}

async function findGiantProspectId(db) {
  const { rows } = await db.query(`
    SELECT p.id
    FROM prospects p
    JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1 AND c.name ILIKE '%Giant Property Management%'
    LIMIT 1
  `, [CLIENT_ID]);
  return rows[0]?.id || null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && args.confirm !== APPLY_CONFIRMATION) {
    console.error(`Refusing apply without --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }

  const before = await assertMinimumAoAccountCounts({
    clientId: CLIENT_ID,
    db: pool,
    minPerAo: args.min,
  });
  console.log('[fillAoAccountAssignments] counts before:', JSON.stringify(before.counts, null, 2));

  const fillResult = await fillActiveAoAccounts({
    clientId: CLIENT_ID,
    db: pool,
    dryRun: !args.apply,
    minPerAo: args.min,
    targetPerAo: args.target,
  });
  console.log('[fillAoAccountAssignments] fill result:', JSON.stringify({
    dryRun: fillResult.dryRun,
    assigned_count: fillResult.assigned.length,
    skipped: fillResult.skipped,
    gaps: fillResult.gaps?.map(g => ({
      ao: g.ao.name,
      current: g.current,
      need: g.need,
    })),
    sample: fillResult.assigned.slice(0, 8),
  }, null, 2));

  if (args.apply && args.deprioritizeGiant) {
    const giantId = await findGiantProspectId(pool);
    if (giantId) {
      await deprioritizeAccountForContactResearch({
        clientId: CLIENT_ID,
        prospectId: giantId,
        reason: 'bad_contact_info',
        db: pool,
      });
      console.log('[fillAoAccountAssignments] deprioritized Giant Property Management', giantId);
    }
  }

  const after = await assertMinimumAoAccountCounts({
    clientId: CLIENT_ID,
    db: pool,
    minPerAo: args.min,
  });
  console.log('[fillAoAccountAssignments] counts after:', JSON.stringify(after.counts, null, 2));

  if (!after.ok && args.apply) {
    console.error('[fillAoAccountAssignments] minimum account check FAILED:', after.failures);
    process.exit(2);
  }

  if (!args.apply) {
    console.log('[fillAoAccountAssignments] dry run complete — re-run with --apply to persist.');
  }
}

main()
  .catch(err => {
    console.error('[fillAoAccountAssignments] fatal:', err.message);
    process.exit(1);
  })
  .finally(() => pool.end().catch(() => {}));
