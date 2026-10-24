'use strict';

/**
 * Idempotent Jake AO prospect book loader (client_id=10).
 *
 * Dry-run (default):
 *   node scripts/seedJakeAoProspectBook.js
 *
 * Apply:
 *   node scripts/seedJakeAoProspectBook.js --confirm-production
 *
 * Optional — grant AO Field Mode login redirect (role=ao, client_id=10):
 *   node scripts/seedJakeAoProspectBook.js --confirm-production --enable-ao-field-mode
 */

require('dotenv').config();

const pool = require('../db');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const {
  distributeDueDates,
  isJakeAssignmentBatchNote,
  laneInitialNextAction,
  todayISOInZone,
} = require('../utils/aoAssignment');
const aoField = require('../services/aoFieldService');
const {
  CLIENT_ID,
  BATCH_SLUG,
  PROSPECTS,
} = require('./data/jakeAoProspectBook');

const APPLY_CONFIRMATION = 'client_10-jake-ao-dogfood-2026-09-16';

function aoFieldModeReady(user) {
  if (!user) return { ready: false, reason: 'user_not_found' };
  if (user.role === 'ao' && Number(user.client_id) === CLIENT_ID) {
    return { ready: true, mode: 'ao_role' };
  }
  if (['admin', 'manager'].includes(user.role)) {
    return {
      ready: true,
      mode: 'admin_ao_access',
      note: 'Admin/manager can use /ao; leads scoped by ao_owner_id = user.id',
    };
  }
  return { ready: false, reason: `role_${user.role}_not_ao_capable` };
}

async function evaluateProspect(prospect, ctx) {
  const {
    jake,
    otherAos,
    dueDate,
    apply,
  } = ctx;

  const initialNextAction = laneInitialNextAction(prospect.lane);
  const base = {
    business_name: prospect.business_name,
    lane: prospect.lane,
    segment: prospect.segment,
    due_date: dueDate,
    initial_next_action: initialNextAction,
  };

  const existingLead = await aoField.findAoLeadByBusinessName(CLIENT_ID, prospect.business_name);
  if (existingLead) {
    const batchOwned = isJakeAssignmentBatchNote(existingLead.original_visit_note, BATCH_SLUG);

    if (batchOwned && existingLead.ao_owner_id === jake.id) {
      return { ...base, action: apply ? 'skipped_existing_jake_batch' : 'would_skip_existing_jake_batch', lead_id: existingLead.id };
    }

    if (existingLead.ao_owner_id && existingLead.ao_owner_id !== jake.id) {
      const ownerName = existingLead.ao_owner_name || `user:${existingLead.ao_owner_id}`;
      const isOtherAo = otherAos.some(o => o.id === existingLead.ao_owner_id);
      return {
        ...base,
        action: apply ? 'skipped_ao_owner_conflict' : 'would_skip_ao_owner_conflict',
        conflict_owner: ownerName,
        conflict_owner_id: existingLead.ao_owner_id,
        is_canonical_ao: isOtherAo,
        lead_id: existingLead.id,
      };
    }

    if (existingLead.ao_owner_id === jake.id) {
      return {
        ...base,
        action: apply ? 'skipped_existing_jake_owned' : 'would_skip_existing_jake_owned',
        lead_id: existingLead.id,
        note: 'Jake already owns this AO lead outside batch — flag for review',
        flag_for_review: true,
      };
    }
  }

  const crm = await aoField.findCrmLinkForBusiness(CLIENT_ID, prospect.business_name);
  const crmLink = crm ? {
    crm_company_id: crm.crm_company_id,
    crm_prospect_id: crm.crm_prospect_id,
    prospect_status: crm.prospect_status,
  } : null;

  if (crm?.prospect_status && ['warm', 'hot'].includes(String(crm.prospect_status).toLowerCase())) {
    return {
      ...base,
      action: apply ? 'skipped_warm_crm_review' : 'would_flag_warm_crm_review',
      crm: crmLink,
      flag_for_review: true,
      note: 'Warm CRM relationship — excluded unless explicitly transferred (SPEC-250)',
    };
  }

  if (!apply) {
    return {
      ...base,
      action: crmLink ? 'would_insert_reuse_crm' : 'would_insert',
      crm: crmLink,
    };
  }

  try {
    const outcome = await aoField.createAoAssignmentLead({
      clientId: CLIENT_ID,
      aoOwnerId: jake.id,
      aoOwnerName: jake.name.split(' ')[0] || 'Jake',
      businessName: prospect.business_name,
      address: prospect.address,
      businessType: prospect.business_type,
      lane: prospect.lane,
      priority: prospect.priority,
      initialNextAction,
      dueDate,
      batchSlug: BATCH_SLUG,
      crmProspectId: crm?.crm_prospect_id || null,
      crmCompanyId: crm?.crm_company_id || null,
    });

    if (outcome.skipped) {
      return {
        ...base,
        action: 'skipped_existing_jake_batch',
        lead_id: outcome.lead?.id || null,
        crm: crmLink,
      };
    }

    return {
      ...base,
      action: 'inserted',
      lead_id: outcome.lead?.id || null,
      task_id: outcome.task?.id || null,
      crm: crmLink,
      crm_reused: Boolean(crmLink),
    };
  } catch (err) {
    return { ...base, action: 'failed', error: err.message, crm: crmLink };
  }
}

async function maybeEnableAoFieldMode(jake, apply, enableFlag) {
  if (!apply || !enableFlag) {
    return { changed: false, skipped: !enableFlag };
  }
  if (jake.role === 'ao' && Number(jake.client_id) === CLIENT_ID) {
    return { changed: false, reason: 'already_ao_role' };
  }
  if (!['admin', 'manager'].includes(jake.role)) {
    return { changed: false, reason: `refused_role_${jake.role}` };
  }

  await pool.query(`
    UPDATE users
    SET role = 'ao', client_id = $2, updated_at = NOW()
    WHERE id = $1
  `, [jake.id, CLIENT_ID]);

  return {
    changed: true,
    previous_role: jake.role,
    previous_client_id: jake.client_id,
    note: 'Login now redirects to /ao Field Mode. Admin dashboard requires role change back.',
  };
}

async function run({ apply = false, enableAoFieldMode = false } = {}) {
  await ensureAoFieldSchema();

  const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
  if (!jake) {
    throw new Error(
      'Jake AO identity not found — verify an active client_id=10 AO user exists '
      + '(jzmaynard7@gmail.com or JAKE_EMAIL override)',
    );
  }

  const fieldMode = aoFieldModeReady(jake);
  const otherAos = await aoField.listActiveAoOwners(CLIENT_ID, { excludeUserId: jake.id });
  const otherAoIds = otherAos.map(o => o.id);
  const beforeCounts = await aoField.countAoLeadsByOwnerIds(
    [...otherAoIds, jake.id],
    CLIENT_ID,
  );

  const today = todayISOInZone();
  const dueDates = distributeDueDates(PROSPECTS.length, { today });
  const results = [];

  for (let i = 0; i < PROSPECTS.length; i += 1) {
    const outcome = await evaluateProspect(PROSPECTS[i], {
      jake,
      otherAos,
      dueDate: dueDates[i],
      apply,
    });
    results.push(outcome);
  }

  const aoFieldModeUpdate = await maybeEnableAoFieldMode(jake, apply, enableAoFieldMode);
  const afterCounts = apply
    ? await aoField.countAoLeadsByOwnerIds([...otherAoIds, jake.id], CLIENT_ID)
    : beforeCounts;

  const summary = {
    inserted: results.filter(r => r.action === 'inserted').length,
    skipped_existing: results.filter(r => r.action.includes('skipped_existing')).length,
    skipped_ao_conflict: results.filter(r => r.action.includes('ao_owner_conflict')).length,
    flagged_warm_crm: results.filter(r => r.action.includes('warm_crm')).length,
    flagged_review: results.filter(r => r.flag_for_review).length,
    failed: results.filter(r => r.action === 'failed').length,
    would_insert: results.filter(r => r.action.startsWith('would_insert')).length,
    crm_reused: results.filter(r => r.crm && r.action !== 'would_flag_warm_crm_review').length,
  };

  return {
    mode: apply ? 'APPLY' : 'DRY_RUN',
    batch_slug: BATCH_SLUG,
    client_id: CLIENT_ID,
    jake_identity: {
      id: jake.id,
      name: jake.name,
      email: jake.email,
      role: jake.role,
      client_id: jake.client_id,
      ao_field_mode: fieldMode,
      ao_field_mode_update: aoFieldModeUpdate,
    },
    other_ao_owners: otherAos.map(o => ({ id: o.id, name: o.name, email: o.email })),
    ownership_before: beforeCounts,
    ownership_after: afterCounts,
    ownership_unchanged_for_other_aos: otherAoIds.every(
      id => (beforeCounts[id] || 0) === (afterCounts[id] || 0),
    ),
    candidate_count: PROSPECTS.length,
    due_date_distribution: {
      today,
      buckets: {
        due_today: dueDates.filter(d => d === today).length,
        due_tomorrow: dueDates.filter(d => d === dueDates[5]).length,
        due_later: dueDates.filter(d => d !== today && d !== dueDates[5]).length,
      },
      dates: dueDates,
    },
    summary,
    ok: apply
      ? summary.failed === 0
      : summary.would_insert + summary.skipped_existing + summary.skipped_ao_conflict
        + summary.flagged_warm_crm + summary.flagged_review <= PROSPECTS.length,
    results,
  };
}

if (require.main === module) {
  const apply = process.argv.includes('--confirm-production');
  const enableAoFieldMode = process.argv.includes('--enable-ao-field-mode');
  const confirmation = process.argv.find(arg => arg.startsWith('--confirm='))?.slice('--confirm='.length);

  if (apply && confirmation !== APPLY_CONFIRMATION) {
    console.error(`Refusing writes. Use --confirm-production --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }

  run({ apply, enableAoFieldMode })
    .then(output => {
      console.log(JSON.stringify(output, null, 2));
      process.exit(output.ok ? 0 : 1);
    })
    .catch(err => {
      console.error(err.message || err.cause?.message || String(err));
      process.exit(1);
    });
}

module.exports = {
  APPLY_CONFIRMATION,
  BATCH_SLUG,
  CLIENT_ID,
  PROSPECTS,
  run,
};
