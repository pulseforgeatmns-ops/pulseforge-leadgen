'use strict';
// Synthetic disposable-database fixtures only. Production confirmation is the
// existing password endpoint; tests seed its server-side session marker.
async function confirmCycleRelease(pool, actor, purpose = 'scout_research_cycle_release') {
  if (!actor.sessionId.startsWith('synthetic-')) throw Error('synthetic_session_required');
  const marker = require('../../utils/freshConfirmation').marker(actor.id, purpose);
  await pool.query(`UPDATE "session" SET sess=jsonb_set(sess::jsonb,'{freshConfirmation}',$2::jsonb)::json WHERE sid=$1`,
    [actor.sessionId, JSON.stringify(marker)]);
}
async function releaseNextResearchCycle(pool, actor, grantId, reference) {
  const grants = require('../../services/scoutResearchOnlyGrant');
  const preview = await grants.previewResearchCycleRelease(pool, { grantId, approvalReference: reference,
    statement: 'Synthetic administrator review approves exactly the next research cycle.' }, actor);
  await confirmCycleRelease(pool, actor);
  return grants.releaseResearchCycle(pool, { release: preview.release, releaseHash: preview.releaseHash }, actor);
}
module.exports = { confirmCycleRelease, releaseNextResearchCycle };
