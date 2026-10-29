'use strict';

const { AsyncLocalStorage } = require('node:async_hooks');
const authority = new AsyncLocalStorage();
const CONTROL_CODES = new Set(['anchor_research_provider_paused', 'research_provider_tenant_required', 'research_provider_tenant_mismatch',
  'research_only_google_spend_denied', 'research_only_scope_required']);

function armed() { return process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED === 'true'; }
function normalize(value) {
  if (!['string', 'number'].includes(typeof value) || !/^[1-9][0-9]*$/.test(String(value))) return null;
  const number = Number(value);
  return Number.isSafeInteger(number) ? String(number) : null;
}
function reject(code) { const error = new Error(code); error.code = code; throw error; }
function hasResearchProviderScope() { return Boolean(authority.getStore()); }
function assertResearchProviderAllowed(provider, explicitTenant) {
  if (!armed()) return;
  const values = [...(authority.getStore()?.tenants || []), ...(explicitTenant == null ? [] : [explicitTenant])];
  const tenants = values.map(normalize);
  if (!tenants.length || tenants.some(value => value === null)) reject('research_provider_tenant_required');
  if (new Set(tenants).size !== 1) reject('research_provider_tenant_mismatch');
  if (tenants[0] === '10'
    && !require('../services/scoutBoundedResearchAuthority').allowsBoundedPlacesResearch(provider, tenants[0])
    && !require('../services/scoutResearchOnlyGrant').allowsResearchOnlyProvider(provider, tenants[0])) {
    reject('anchor_research_provider_paused');
  }
}
function withResearchProviderScope(tenantId, fn) {
  const parent = authority.getStore();
  const tenants = Object.freeze([...(parent?.tenants || []),
    ...((tenantId == null && parent) ? [] : [tenantId])]);
  return authority.run(Object.freeze({ tenants }), () => {
    assertResearchProviderAllowed('research');
    return fn();
  });
}
function isResearchProviderControlError(error) { return CONTROL_CODES.has(error?.code); }
function rethrowResearchProviderControl(error) { if (isResearchProviderControlError(error)) throw error; }

module.exports = { armed, hasResearchProviderScope, assertResearchProviderAllowed, withResearchProviderScope,
  isResearchProviderControlError, rethrowResearchProviderControl };
