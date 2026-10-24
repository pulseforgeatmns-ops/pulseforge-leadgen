'use strict';

const { hash } = require('../../packages/acquisition-mission/DailyOutboundPolicy');

function validationScope(resolved) {
  return { objective: resolved.objective, geography: resolved.geography,
    market: resolved.market, segmentLabel: resolved.segmentLabel };
}

async function ensureValidationMission(input, deps) {
  const { tenantId, createdBy, resolvedObjective } = input;
  if (!resolvedObjective.ready || resolvedObjective.ambiguities?.length) {
    throw Object.assign(new Error('Canonical objective must resolve before creating a validation mission.'),
      { code: 'canonical_plan_ambiguous' });
  }
  const missions = await deps.listMissions(tenantId);
  const active = missions.filter(m => m.createdBy === createdBy && !m.planCancelled);
  const scopeHash = hash(validationScope(resolvedObjective));
  const matching = active.filter(m => hash(validationScope(m.resolvedObjective || {})) === scopeHash);
  if (matching.length > 1) throw Object.assign(new Error('Multiple current validation missions require reconciliation.'), { code: 'duplicate_validation_missions' });
  for (const mission of active.filter(m => !matching.includes(m))) {
    const snapshot = await deps.inspectMission(mission.id);
    if (mission.structuredMission?.immutable || snapshot.contributions?.some(c => c.specialist !== 'operator')) {
      throw Object.assign(new Error('An existing validation mission has committed work under a different scope.'), { code: 'validation_scope_changed' });
    }
    await deps.cancelMission(mission.id);
    if (!(await deps.inspectMission(mission.id)).mission?.planCancelled) {
      throw Object.assign(new Error('Canonical cancellation did not persist; refusing another active mission.'),
        { code: 'validation_cancellation_not_persisted' });
    }
  }
  if (matching.length) return matching[0];
  const id = `mission_validation_${hash([tenantId, createdBy, scopeHash]).slice(0, 24)}`;
  if (missions.some(m => m.id === id)) throw Object.assign(new Error('Validation mission was explicitly cancelled.'), { code: 'validation_mission_cancelled' });
  return deps.createMission({ ...input, id });
}

module.exports = { validationScope, ensureValidationMission };
