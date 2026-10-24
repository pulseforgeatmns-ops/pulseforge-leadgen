'use strict';

/**
 * Smoke test SPEC-AO-FOLLOWUP-001 against DATABASE_URL.
 * Creates a temporary prospect, exercises draft/save/list, then cleans up.
 */

const pool = require('../db');
const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
const aoFollowup = require('../services/aoFollowupService');
const { composeAoFollowUp } = require('../utils/aoFollowupComposer');

const PROSPECT_ID = '30000000-0000-4000-8000-00000000f001';
const COMPANY_ID = '30000000-0000-4000-8000-00000000c001';
const CLIENT_ID = 10;

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error('DATABASE_URL required');
    process.exit(1);
  }

  await ensureAoCrmSchema();

  const aoRow = (await pool.query(`
    SELECT id, name, email FROM users
    WHERE client_id = $1 AND role = 'ao' AND active = true
    ORDER BY id LIMIT 1
  `, [CLIENT_ID])).rows[0];

  if (!aoRow) {
    console.error('No active AO user for client_id=10');
    process.exit(1);
  }

  await pool.query('BEGIN');
  try {
    await pool.query(`
      INSERT INTO companies(id, client_id, name, location)
      VALUES ($1::uuid, $2, 'Smoke Follow-Up Co', 'Manchester NH')
      ON CONFLICT (id) DO NOTHING
    `, [COMPANY_ID, CLIENT_ID]);

    await pool.query(`
      INSERT INTO prospects(
        id, client_id, company_id, first_name, email, assigned_ao_id,
        ao_next_action, help_requested, do_not_contact
      ) VALUES ($1::uuid, $2, $3::uuid, 'Lori', 'lori-smoke@example.com', $4, 'follow_up', false, false)
      ON CONFLICT (id) DO UPDATE SET
        assigned_ao_id = EXCLUDED.assigned_ao_id,
        ao_next_action = EXCLUDED.ao_next_action,
        help_requested = false,
        do_not_contact = false
    `, [PROSPECT_ID, CLIENT_ID, COMPANY_ID, aoRow.id]);

    const gen = await aoFollowup.generateFollowUpDraft({
      clientId: CLIENT_ID,
      aoUserId: aoRow.id,
      prospectId: PROSPECT_ID,
      body: { aoNotes: 'Front desk receptionist cleans the office.' },
      profile: aoRow,
    });
    if (!gen.ok) throw new Error(JSON.stringify(gen));
    if (!new RegExp(aoRow.name.split(/\s+/)[0], 'i').test(gen.draft.emailDraft || '')) {
      throw new Error('Draft not signed by assigned AO');
    }

    const saved = await aoFollowup.saveFollowUpDraft({
      clientId: CLIENT_ID,
      aoUserId: aoRow.id,
      prospectId: PROSPECT_ID,
      draft: gen.draft,
      inputSnapshot: gen.input_snapshot,
      flagJakeReview: false,
    });
    const helpAfterSave = (await pool.query('SELECT help_requested FROM prospects WHERE id = $1::uuid', [PROSPECT_ID])).rows[0];
    if (helpAfterSave.help_requested) throw new Error('help_requested set without intentional Jake flag');

    const draftRow = (await pool.query(
      'SELECT account_id::text FROM ao_followup_drafts WHERE id = $1',
      [saved.draft.id]
    )).rows[0];
    if (draftRow.account_id !== PROSPECT_ID) throw new Error('Draft account_id UUID mismatch');

    const activity = (await pool.query(`
      SELECT prospect_id::text, activity_type FROM ao_prospect_activity
      WHERE prospect_id = $1::uuid AND activity_type = 'followup_draft_created'
      ORDER BY created_at DESC LIMIT 1
    `, [PROSPECT_ID])).rows[0];
    if (!activity) throw new Error('Missing followup_draft_created activity');
    if (activity.prospect_id !== PROSPECT_ID) throw new Error('Activity prospect_id mismatch');

    const listed = await aoFollowup.listFollowUpDrafts({
      clientId: CLIENT_ID,
      prospectId: PROSPECT_ID,
      aoUserId: aoRow.id,
    });
    if (!listed.drafts.some(d => d.id === saved.draft.id)) throw new Error('List drafts missing saved row');

    const dnc = composeAoFollowUp({
      tenantId: CLIENT_ID,
      accountId: 1,
      accountName: 'Manchester Family Dentistry',
      assignedAoId: aoRow.id,
      assignedAoName: aoRow.name,
      aoNotes: 'Asked to be removed from the call list.',
    });
    if (dnc.emailDraft) throw new Error('DNC should not produce email draft');

    const ambiguous = composeAoFollowUp({
      tenantId: CLIENT_ID,
      accountId: 1,
      accountName: 'Southern New Hampshire University',
      assignedAoId: aoRow.id,
      assignedAoName: aoRow.name,
      contactEmail: 'Robert.Oelschlager@unh.edu',
      aoNotes: 'UNH facilities link in notes.',
    });
    if (ambiguous.status !== 'needs_clarification' || ambiguous.emailDraft) {
      throw new Error('SNHU/UNH should need clarification without draft');
    }

    console.log('smokeAoFollowupComposer: OK', {
      ao: aoRow.name,
      prospect_id: PROSPECT_ID,
      draft_id: saved.draft.id,
      activity_type: activity.activity_type,
    });
    await pool.query('ROLLBACK');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error('smokeAoFollowupComposer: FAIL', err.message);
  process.exit(1);
});
