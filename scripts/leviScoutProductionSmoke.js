#!/usr/bin/env node
'use strict';

const { runLeviScoutProductionCycle } = require('../services/leviScoutProductionRunner');
const { researchEnabled } = require('../services/leviScoutEnablement');

async function smoke({ env = process.env, db, fetch: fetchImpl, now = new Date(), understanding, loadUnderstanding } = {}) {
  if (!researchEnabled(env)) {
    return { discovered: 0, cities: [], qualified: 0, outbound: false, status: 'disabled' };
  }
  const result = await runLeviScoutProductionCycle({ env, db, fetch: fetchImpl, now, understanding, loadUnderstanding, sink: async () => {} });
  return {
    discovered: Number(result.discovered || 0),
    cities: [...(result.cities || [])],
    qualified: Number(result.qualified || 0),
    outbound: false,
    status: result.status,
    persisted: Number(result.persisted || 0),
  };
}

module.exports = { smoke };

if (require.main === module) {
  Promise.resolve()
    .then(() => smoke({ db: require('../db') }))
    .then(summary => {
      console.log(JSON.stringify(summary));
      process.exit(0);
    })
    .catch(error => {
      console.error(`[levi-scout-production-smoke] ${error.code || error.message}`);
      process.exit(2);
    });
}
