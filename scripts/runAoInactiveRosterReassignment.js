'use strict';

/**
 * SPEC-AO-ROSTER-REASSIGN-001 — pause Zach and transfer assigned accounts to Jake.
 *
 * Preview: node scripts/runAoInactiveRosterReassignment.js
 * Apply:    node scripts/runAoInactiveRosterReassignment.js --apply --confirm=ao-inactive-zach-to-jake
 */

require('dotenv').config();

const { runZachToJakeTransfer } = require('../services/aoRosterReassignmentService');

const APPLY_CONFIRMATION = 'ao-inactive-zach-to-jake';

async function main() {
  const apply = process.argv.includes('--apply');
  const confirm = process.argv.find(a => a.startsWith('--confirm='))?.split('=')[1] || null;
  if (apply && confirm !== APPLY_CONFIRMATION) {
    console.error(`Refusing apply without --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }

  const result = await runZachToJakeTransfer({ clientId: 10, dryRun: !apply });
  console.log(JSON.stringify(result, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
