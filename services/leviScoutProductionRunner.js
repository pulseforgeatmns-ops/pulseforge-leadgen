'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const execution = require('./leviScoutResearchOnlyExecution');
const { assertResearchEnabled, productionLimits, productionPermissions, AGENT } = require('./leviScoutEnablement');
const { loadApprovedCriteria } = require('./leviScoutApprovedCriteria');
const { inspectApprovedBlueprint, loadTenantUnderstanding } = require('./leviScoutBlueprintReadiness');
const { createProductionDeps } = require('./leviScoutProductionDeps');
const { citiesOf } = require('./leviScoutBorrowerDiscovery');
const { deliverToMax } = require('./leviScoutMaxDelivery');
const { cycleIdFor } = require('./leviScoutSchedule');

function authorityFor({ cycleId, criteria, approvalRef = 'P0-SCOUT-LEVI-001' }) {
  return {
    version: 1,
    kind: 'levi_scout_research_only',
    tenantId: String(TENANT_ID),
    cycleId,
    approvalRef,
    criteria: criteria || { approved: false, source: 'approved_blueprint' },
    limits: productionLimits(),
    permissions: productionPermissions(),
  };
}

async function runLeviScoutProductionCycle({
  env = process.env,
  db,
  fetch: fetchImpl,
  now = new Date(),
  cycleId,
  loadUnderstanding,
  understanding,
  sink,
  deps,
} = {}) {
  assertResearchEnabled(env);
  const id = cycleId || cycleIdFor(now);
  const resolvedUnderstanding = understanding !== undefined
    ? understanding
    : (typeof loadUnderstanding === 'function'
      ? await loadUnderstanding({ tenantId: TENANT_ID })
      : (db ? await loadTenantUnderstanding({ db, tenantId: TENANT_ID }) : null));
  const loaded = await loadApprovedCriteria({ tenantId: TENANT_ID, understanding: resolvedUnderstanding });
  const criteria = loaded || { approved: false, source: 'approved_blueprint' };
  const blueprint = inspectApprovedBlueprint(resolvedUnderstanding, TENANT_ID);
  const resolvedDeps = deps || createProductionDeps({ db, fetch: fetchImpl, env, now });
  const result = await execution.runResearchOnlyCycle({
    authority: authorityFor({ cycleId: id, criteria }),
    env,
    deps: resolvedDeps,
  });
  const cities = (resolvedDeps.state && resolvedDeps.state.lastDiscover && resolvedDeps.state.lastDiscover.cities)
    || citiesOf((resolvedDeps.state && resolvedDeps.state.lastDiscover && resolvedDeps.state.lastDiscover.rows) || []);
  const snapshot = {
    ...result,
    agent: AGENT,
    cities,
    outreachAttempted: false,
    paidProvidersUsed: false,
    blueprintQuestions: blueprint.questionsForLevi,
    missingBlueprintDecisions: blueprint.missing,
  };
  await deliverToMax(snapshot, { db, sink });
  return snapshot;
}

module.exports = { authorityFor, runLeviScoutProductionCycle };
