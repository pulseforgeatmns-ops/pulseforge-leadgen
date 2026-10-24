#!/usr/bin/env node
'use strict';

/**
 * SPEC-STUDIO-SCOUT-001 — First batch: 25 prospects across category mix
 * (Greater Manchester NH + southern NH). Outreach remains disabled.
 *
 * Usage:
 *   node scripts/studioSubstralScoutBatch001.js [--dry-run]
 *
 * Requires STUDIO_SUBSTRAL_CLIENT_ID or provisioned studio-substral tenant.
 */

require('dotenv').config();

const {
  resolveStudioSubstralClientId,
  ensureStudioSubstralServiceArea,
} = require('../utils/studioSubstralTenant');
const { STUDIO_SUBSTRAL_SCOUT_PLAN } = require('../services/studioSubstralScoutIntelligence');
const { run } = require('../leadgen');

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  await ensureStudioSubstralServiceArea();
  const clientId = await resolveStudioSubstralClientId();
  const mix = STUDIO_SUBSTRAL_SCOUT_PLAN.batch_mix;
  const runs = [];

  for (const [category, count] of Object.entries(mix)) {
    const queries = STUDIO_SUBSTRAL_SCOUT_PLAN.verticals[category] || [];
    for (let i = 0; i < queries.length; i += 1) {
      const queryTemplate = queries[i];
      const city = STUDIO_SUBSTRAL_SCOUT_PLAN.cities[i % STUDIO_SUBSTRAL_SCOUT_PLAN.cities.length];
      const location = `${city} ${STUDIO_SUBSTRAL_SCOUT_PLAN.state}`;
      const industry = queryTemplate.replace('{city}', city).replace('{state}', STUDIO_SUBSTRAL_SCOUT_PLAN.state);
      const perQueryMax = Math.max(2, Math.ceil(count / queries.length));
      runs.push({
        category,
        client_id: clientId,
        industry,
        location,
        max: perQueryMax,
        vertical: category,
      });
    }
  }

  console.log(`[StudioSubstralScoutBatch001] ${runs.length} scout passes (target mix total ${Object.values(mix).reduce((a, b) => a + b, 0)}) for client ${clientId}`);
  if (dryRun) {
    console.log(JSON.stringify(runs, null, 2));
    return;
  }

  for (const pass of runs) {
    console.log(`\n[Batch] ${pass.category}: ${pass.industry} @ ${pass.location} (max ${pass.max})`);
    // eslint-disable-next-line no-await-in-loop
    await run({
      client_id: pass.client_id,
      industry: pass.industry,
      location: pass.location,
      maxResults: pass.max,
      vertical: pass.vertical,
    });
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
