#!/usr/bin/env node
'use strict';

const { discover, citiesOf, DISCOVERY_CAP } = require('../services/leviScoutBorrowerDiscovery');
const { evaluateProspect } = require('../services/leviScoutProductionContract');
const { inspectApprovedBlueprint, loadTenantUnderstanding } = require('../services/leviScoutBlueprintReadiness');
const { enrichmentCounts } = require('../services/leviScoutEnrichment');
const { composeReviewBundle } = require('../services/leviScoutMaxReview');

async function validate({
  env = process.env,
  fetch: fetchImpl = globalThis.fetch,
  now = new Date(),
  db,
  understanding,
} = {}) {
  const loadedUnderstanding = understanding !== undefined
    ? understanding
    : (db ? await loadTenantUnderstanding({ db }) : null);
  const blueprint = inspectApprovedBlueprint(loadedUnderstanding);
  const rows = await discover({
    maxCandidates: DISCOVERY_CAP,
    maxProviderRequests: 0,
    fetch: fetchImpl,
    now,
  });
  const counts = enrichmentCounts(rows);
  const qualified = rows.filter(row => evaluateProspect(row).qualified).length;
  const bundle = composeReviewBundle({
    records: rows,
    understanding: loadedUnderstanding,
  });
  return {
    status: 'dry_run',
    persist: 'dry-run',
    outbound: false,
    paidProvidersUsed: false,
    campaignEnrollment: false,
    inventedCriteria: false,
    enabled: env.SCOUT_LEVI_RESEARCH_ONLY_ENABLED === 'true',
    cities: citiesOf(rows),
    missingBlueprintDecisions: blueprint.missing,
    questionsForLevi: blueprint.questionsForLevi,
    newEvidenceSources: [
      'NYC DOB NOW permits (w9ak-ipjd) — property activity / optional initial_cost',
      'Chicago building permits (ydr8-5enu) — property activity / optional reported_cost and owner contact fields',
      'Chicago Business Owners (ezma-pppn) — filed owner title and name, exact legal/DBA match only',
      'Chicago Business Licenses (r5kz-chrr) — registered business address and public phone/email when filed',
    ],
    candidatesEnriched: counts.enriched,
    verifiableBusinessContacts: counts.verifiableBusinessContacts,
    meetingApprovedQualification: qualified,
    researchCandidates: bundle.counts.researchCandidates,
    qualifiedProspects: bundle.counts.qualifiedProspects,
    remainingBlockers: [
      ...(blueprint.questionsForLevi.length ? ['missing_blueprint_lending_criteria'] : []),
      'permit_does_not_establish_borrower_intent',
      ...(counts.verifiableBusinessContacts < counts.enriched ? ['public_business_contact_coverage'] : []),
      'no_deploy_until_review',
    ],
  };
}

module.exports = { validate };

if (require.main === module) {
  Promise.resolve()
    .then(() => validate({}))
    .then((summary) => {
      console.log(JSON.stringify(summary, null, 2));
      const ok = summary.outbound === false
        && summary.paidProvidersUsed === false
        && summary.meetingApprovedQualification === 0
        && summary.inventedCriteria === false;
      process.exit(ok ? 0 : 2);
    })
    .catch((error) => {
      console.error(`[levi-scout-signal-validation] ${error.code || error.message}`);
      process.exit(2);
    });
}
