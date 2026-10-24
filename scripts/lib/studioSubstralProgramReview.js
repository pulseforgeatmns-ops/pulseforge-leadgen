'use strict';

const { policy, hash, missionScope, fail } = require('../../packages/acquisition-mission/DailyOutboundPolicy');
const { TENANT_ID, STUDIO_SUBSTRAL_MAILBOX } = require('./studioSubstralCanonicalOutbound');

// No bootstrap, schema setup, capacity production, or other writes in this review.
// The caller must choose an already approved mission and explicit bounded authority.
async function reviewStudioSubstralProgram(db, input = {}, now = new Date()) {
  for (const field of ['sourceMissionId', 'startsAt', 'expiresAt', 'dailyCap', 'totalCap',
    'spacingMinutes', 'startHour', 'endHour', 'allowedContactClassifications']) {
    if (input[field] == null || input[field] === '') fail(`program_review_${field}_required`);
  }
  const identity = {
    tenantId: TENANT_ID,
    senderEmail: STUDIO_SUBSTRAL_MAILBOX.senderEmail,
    inboxIntegrationId: STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId,
    sendingIdentityId: STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId,
  };
  for (const [field, value] of Object.entries(identity)) {
    if (input[field] != null && String(input[field]) !== value) fail('substral_program_identity_mismatch');
  }
  const reviewedPolicy = policy({ ...input, ...identity }, now);
  const { rows } = await db.query(
    `SELECT id, tenant_id, stage, status, objective, target_segment, payload
       FROM acquisition_missions WHERE id=$1 AND tenant_id=$2`,
    [reviewedPolicy.sourceMissionId, TENANT_ID]
  );
  const row = rows[0];
  if (!row || String(row.tenant_id) !== TENANT_ID) fail('approved_source_mission_required');
  // Mirror the canonical column precedence used by acquisitionMissionPersistence.
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  const columnOrPayload = (column, value) => column === undefined ? value : column;
  const mission = { ...payload, id: row.id, tenantId: row.tenant_id,
    stage: columnOrPayload(row.stage, payload.stage), status: columnOrPayload(row.status, payload.status),
    objective: columnOrPayload(row.objective, payload.objective),
    targetSegment: columnOrPayload(row.target_segment, payload.targetSegment) };
  if (mission.structuredMission?.immutable !== true || mission.planCancelled
    || /cancel/i.test(String(mission.status || ''))) fail('approved_source_mission_required');
  if (mission.stage !== 'ready') fail('ready_source_mission_required');
  const sourceMissionScope = missionScope(mission);
  const scopeHash = hash(sourceMissionScope);
  const reviewHash = hash({ policy: reviewedPolicy, scopeHash });
  return { reviewRequired: true, tenantId: TENANT_ID, mode: 'shadow',
    policy: reviewedPolicy, sourceMissionScope, scopeHash, reviewHash,
    note: 'Review the exact offer, geography, sender, contact classes, dates and caps. This review does not create a program or authorize sending.' };
}

module.exports = { reviewStudioSubstralProgram };
