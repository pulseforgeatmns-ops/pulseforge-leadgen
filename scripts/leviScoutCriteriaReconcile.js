#!/usr/bin/env node
'use strict';

// Read-only comparison of tenant 21's approved Blueprint against the documented
// nationwide rehab / $5k–$20k research profile. Never writes and never promotes
// research assumptions into approved terms.

const { loadTenantUnderstanding } = require('../services/leviScoutBlueprintReadiness');
const { reconcileResearchProfile } = require('../services/leviScoutCriteriaReconcile');
const { TENANT_ID } = require('../services/leviScoutProductionContract');

async function reconcile({ db, tenantId = TENANT_ID } = {}) {
  const understanding = await loadTenantUnderstanding({ db, tenantId });
  const report = reconcileResearchProfile(understanding, tenantId);
  return {
    tenantId: String(tenantId),
    readOnly: true,
    loadError: understanding.loadError || null,
    ...report,
  };
}

module.exports = { reconcile };

if (require.main === module) {
  Promise.resolve()
    .then(async () => {
      const db = require('../db');
      const report = await reconcile({ db });
      console.log(JSON.stringify(report, null, 2));
    })
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(`[levi-scout-criteria-reconcile] ${error.code || error.message}`);
      process.exit(2);
    });
}
