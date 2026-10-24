'use strict';

/**
 * Bulk-add Anchor Campaign 001 direct mail targets to Mike's AO queue as warm
 * in-person follow-ups (client_id=10).
 *
 * Review: node scripts/seedAnchorDirectMailAoQueue.js
 * Apply:   node scripts/seedAnchorDirectMailAoQueue.js --apply --confirm=client_10-direct-mail-ao-2026-08-16
 */

require('dotenv').config();

const pool = require('../db');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const aoField = require('../services/aoFieldService');
const {
  CLIENT_ID,
  CAMPAIGN_NAME,
  DEFAULT_NOTE,
  MIKE_NAME_PATTERN,
  DIRECT_MAIL_TARGETS,
} = require('./data/anchorDirectMailTargets');

const APPLY_CONFIRMATION = 'client_10-direct-mail-ao-2026-08-16';

async function resolveMike() {
  const mike = await aoField.resolveAoOwnerByName(MIKE_NAME_PATTERN, CLIENT_ID);
  if (!mike) {
    throw new Error(
      'Mike AO user not found — create an active AO user named Mike with client_id=10 in /admin/users',
    );
  }
  return mike;
}

async function run({ apply = false } = {}) {
  await ensureAoFieldSchema();
  const mike = await resolveMike();
  const dueDate = aoField.endOfBusinessWeekISO();
  const results = [];

  for (const target of DIRECT_MAIL_TARGETS) {
    if (!apply) {
      const existing = await aoField.findDirectMailLead(CLIENT_ID, target.business_name);
      results.push({
        business_name: target.business_name,
        business_type: target.business_type,
        ao_owner: mike.name,
        due_date: dueDate,
        action: existing ? 'would_skip_existing' : 'would_insert',
      });
      continue;
    }

    const outcome = await aoField.createDirectMailFollowUpLead({
      clientId: CLIENT_ID,
      aoOwnerId: mike.id,
      aoName: mike.name.split(' ')[0] || 'Mike',
      businessName: target.business_name,
      businessType: target.business_type,
      campaignName: CAMPAIGN_NAME,
      note: DEFAULT_NOTE,
      dueDate,
    });

    results.push({
      business_name: target.business_name,
      business_type: target.business_type,
      ao_owner: mike.name,
      due_date: dueDate,
      action: outcome.skipped ? 'skipped_existing' : 'inserted',
      lead_id: outcome.lead?.id || null,
      task_id: outcome.task?.id || null,
    });
  }

  const inserted = results.filter(r => r.action === 'inserted').length;
  const skipped = results.filter(r => r.action === 'skipped_existing' || r.action === 'would_skip_existing').length;

  return {
    mode: apply ? 'APPLY' : 'REVIEW_ONLY',
    client_id: CLIENT_ID,
    campaign: CAMPAIGN_NAME,
    ao_owner: { id: mike.id, name: mike.name, email: mike.email },
    due_date: dueDate,
    expected_total: DIRECT_MAIL_TARGETS.length,
    inserted,
    skipped,
    ok: apply ? inserted + skipped === DIRECT_MAIL_TARGETS.length : true,
    results,
  };
}

if (require.main === module) {
  const apply = process.argv.includes('--apply');
  const confirmation = process.argv.find(arg => arg.startsWith('--confirm='))?.slice('--confirm='.length);
  if (apply && confirmation !== APPLY_CONFIRMATION) {
    console.error(`Refusing writes. Use --apply --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }
  run({ apply })
    .then(output => {
      console.log(JSON.stringify(output, null, 2));
      process.exit(output.ok ? 0 : 1);
    })
    .catch(err => {
      console.error(err.message);
      process.exit(1);
    });
}

module.exports = {
  APPLY_CONFIRMATION,
  CLIENT_ID,
  CAMPAIGN_NAME,
  DIRECT_MAIL_TARGETS,
  run,
};
