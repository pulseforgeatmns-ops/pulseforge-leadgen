#!/usr/bin/env node
'use strict';

const { discover, citiesOf, DISCOVERY_CAP } = require('../services/leviScoutBorrowerDiscovery');
const { enrichmentCounts } = require('../services/leviScoutEnrichment');
const { inspectApprovedBlueprint } = require('../services/leviScoutBlueprintReadiness');

async function smoke({ env = process.env, fetch: fetchImpl = globalThis.fetch, now = new Date(), understanding } = {}) {
  const rows = await discover({
    maxCandidates: DISCOVERY_CAP,
    maxProviderRequests: 0,
    fetch: fetchImpl,
    now,
  });
  const counts = enrichmentCounts(rows);
  const blueprint = inspectApprovedBlueprint(understanding);
  return {
    discovered: rows.length,
    cities: citiesOf(rows),
    qualified: 0,
    researchCandidates: counts.researchCandidates,
    verifiableBusinessContacts: counts.verifiableBusinessContacts,
    missingBlueprintDecisions: blueprint.missing.length,
    outbound: false,
    persist: 'dry-run',
    status: 'dry_run',
    enabled: env.SCOUT_LEVI_RESEARCH_ONLY_ENABLED === 'true',
  };
}

module.exports = { smoke };

if (require.main === module) {
  Promise.resolve()
    .then(() => smoke({}))
    .then(summary => {
      console.log(JSON.stringify(summary));
      process.exit(summary.discovered >= 1 && summary.qualified === 0 && summary.outbound === false ? 0 : 2);
    })
    .catch(error => {
      console.error(`[levi-scout-production-smoke] ${error.code || error.message}`);
      process.exit(2);
    });
}
