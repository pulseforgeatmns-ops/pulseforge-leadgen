'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');

const AGENT = 'scout_levi';
const ENABLE_FLAG = 'SCOUT_LEVI_RESEARCH_ONLY_ENABLED';
const PAID_FLAG = 'SCOUT_LEVI_PAID_PROVIDERS_APPROVED';
const MAX_CANDIDATES = 20;

function fail(code) { throw Object.assign(new Error(code), { code }); }

function researchEnabled(env = process.env) {
  return env[ENABLE_FLAG] === 'true';
}

function paidProvidersApproved(env = process.env) {
  return env[PAID_FLAG] === 'true';
}

function assertResearchEnabled(env = process.env) {
  if (!researchEnabled(env)) fail('levi_research_disabled');
}

function productionLimits() {
  return Object.freeze({ maxCandidates: MAX_CANDIDATES, maxProviderRequests: 0 });
}

function productionPermissions() {
  return Object.freeze({
    discover: true,
    persistReviewOnly: true,
    outreach: false,
    campaignEnrollment: false,
    modifySuppression: false,
  });
}

module.exports = {
  TENANT_ID, AGENT, ENABLE_FLAG, PAID_FLAG, MAX_CANDIDATES,
  researchEnabled, paidProvidersApproved, assertResearchEnabled,
  productionLimits, productionPermissions,
};
