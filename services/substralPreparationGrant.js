'use strict';

const { hash, missionScope, windowReason, policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedOutboundPreparationEnabledForTenant, governedOutboundEnabledForTenant } = require('./governedOutboundTenant');

function isSubstralPreparationGrant(program) {
  return String(program?.tenant_id) === '17' && program?.policy?.tenantId === '17'
    && program.policy.preparationOnly === true && program.mode === 'shadow';
}

// Checked before discovery or preparation; this grant never permits dispatch.
function substralPreparationGrantReason(program, mission, now = new Date()) {
  if (!isSubstralPreparationGrant(program)) return 'preparation_shadow_grant_required';
  if (governedOutboundEnabledForTenant('17') || !governedOutboundPreparationEnabledForTenant('17')) return 'preparation_requires_disabled_sending';
  if (!program.authorized_by || hash(program.policy) !== program.policy_hash) return 'policy_changed';
  const timeReason = windowReason(program.policy, now, false);
  if (timeReason) return timeReason;
  try {
    if (hash(policy(program.policy, now)) !== program.policy_hash) return 'policy_changed';
  } catch (error) { return error.code || 'invalid_preparation_bounds'; }
  if (!mission || String(mission.tenantId) !== '17' || mission.id !== program.source_mission_id
    || program.policy.sourceMissionId !== program.source_mission_id || !mission.structuredMission?.immutable
    || mission.stage !== 'ready' || mission.planCancelled || /cancel/i.test(String(mission.status || ''))
    || hash(missionScope(mission)) !== program.scope_hash) return 'source_scope_changed';
  return null;
}

module.exports = { isSubstralPreparationGrant, substralPreparationGrantReason };
