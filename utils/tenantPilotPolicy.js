'use strict';

// Server-enforced tenant pilot policy (P0-SCOUT-LEVI-001).
//
// A pilot tenant is prospecting-only: its people may view prospects, research
// evidence and pipeline state, and the operator-scheduled FREE research path
// (Levi Scout research-only over public open-data SODA endpoints) keeps
// running. Paid research providers, outbound contact of any channel, agent
// dispatch beyond Max, and specialist-dispatching acquisition-mission
// execution are denied for that tenant.
//
// The pilot list is a code constant on purpose. It does not read the database,
// the AIM, enabled_agents, request parameters, session state or environment,
// so publishing an AIM, editing enabled_agents, passing a different client_id
// or flipping an env flag cannot lift it. Lifting it is a reviewed code change.
// Environment can ADD pilot tenants (TENANT_PILOT_PROSPECTING_ONLY_ADD=7,9) but
// can never remove a code-listed one.

const POLICY_NAME = 'prospecting_only_pilot';
const CODE = 'tenant_pilot_policy_denied';

const CODE_PILOT_TENANTS = Object.freeze(['21']);

// Agents a pilot tenant may dispatch through the generic agent boundaries
// (/api/run/:agent, /cron/:agent). Max only reads and briefs. scout_levi has its
// own operator-only cron handler and weekly clock and is not dispatched here.
const ALLOWED_AGENTS = Object.freeze(['max']);

// Mission intents that never dispatch a specialist, provider or sender.
const ALLOWED_MISSION_INTENTS = Object.freeze([
  'CLARIFY_PLAN', 'CANCEL_PLAN', 'EDIT_PLAN', 'APPLY_PLAN_EDITS', 'REVIEW_CLARITY_RECOMMENDATION',
]);

const CAPABILITY = Object.freeze({
  VIEW: 'view',                       // prospects, research evidence, pipeline
  FREE_RESEARCH: 'free_research',     // public open-data research (no paid provider)
  PAID_RESEARCH: 'paid_research',     // Places, SerpAPI, Prospeo, Hunter, Apollo, phone enrichment, ...
  OUTBOUND: 'outbound',               // email, SMS, calls, scheduled sends
  AGENT_DISPATCH: 'agent_dispatch',   // generic agent run boundaries
  MISSION_EXECUTION: 'mission_execution',
});

function normalizeTenant(value) {
  if (value == null) return null;
  if (typeof value === 'object') {
    return normalizeTenant(value.tenantId ?? value.tenant_id ?? value.clientId ?? value.client_id ?? value.id);
  }
  const text = String(value).trim();
  if (!/^[1-9][0-9]*$/.test(text)) return null;
  return String(Number(text));
}

function pilotTenants(env = process.env) {
  const added = String(env.TENANT_PILOT_PROSPECTING_ONLY_ADD || '')
    .split(',').map(normalizeTenant).filter(Boolean);
  return new Set([...CODE_PILOT_TENANTS, ...added]);
}

function isPilotTenant(tenantId, env = process.env) {
  const tenant = normalizeTenant(tenantId);
  return Boolean(tenant && pilotTenants(env).has(tenant));
}

function decide(tenantId, capability, detail = {}, env = process.env) {
  const tenant = normalizeTenant(tenantId);
  if (!tenant || !pilotTenants(env).has(tenant)) {
    return { allowed: true, pilot: false, tenantId: tenant, capability };
  }
  let allowed;
  switch (capability) {
    case CAPABILITY.VIEW:
    case CAPABILITY.FREE_RESEARCH:
      allowed = true; break;
    case CAPABILITY.AGENT_DISPATCH:
      allowed = ALLOWED_AGENTS.includes(String(detail.agent || '').toLowerCase()); break;
    case CAPABILITY.MISSION_EXECUTION:
      allowed = ALLOWED_MISSION_INTENTS.includes(String(detail.intent || '')); break;
    case CAPABILITY.PAID_RESEARCH:
    case CAPABILITY.OUTBOUND:
    default:
      allowed = false; // unknown capabilities fail closed for pilot tenants
  }
  return { allowed, pilot: true, tenantId: tenant, capability, policy: POLICY_NAME };
}

function denialError(decision, detail = {}) {
  return Object.assign(
    new Error(`Tenant ${decision.tenantId} is a ${POLICY_NAME}: ${decision.capability} is not permitted`),
    {
      code: CODE,
      status: 403,
      statusCode: 403,
      tenantId: decision.tenantId,
      capability: decision.capability,
      policy: POLICY_NAME,
      detail: Object.freeze({ ...detail }),
      providerBoundaryCrossed: false,
    }
  );
}

function assertTenantAllows(tenantId, capability, detail = {}, env = process.env) {
  const decision = decide(tenantId, capability, detail, env);
  if (!decision.allowed) throw denialError(decision, detail);
  return decision;
}

// True when ANY of the given tenants is a pilot tenant that denies capability.
function anyTenantDenies(tenantIds, capability, detail = {}, env = process.env) {
  for (const tenant of tenantIds || []) {
    const decision = decide(tenant, capability, detail, env);
    if (!decision.allowed) return decision;
  }
  return null;
}

function isPolicyDenial(error) {
  return Boolean(error && error.code === CODE);
}

module.exports = {
  POLICY_NAME,
  CODE,
  CAPABILITY,
  CODE_PILOT_TENANTS,
  ALLOWED_AGENTS,
  ALLOWED_MISSION_INTENTS,
  normalizeTenant,
  pilotTenants,
  isPilotTenant,
  decide,
  denialError,
  assertTenantAllows,
  anyTenantDenies,
  isPolicyDenial,
};
