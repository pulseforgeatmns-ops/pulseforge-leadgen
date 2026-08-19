'use strict';

/**
 * SPEC-114 — Max tenant context resolution.
 *
 * Active Tenant → Business Blueprint → Published AIM → Knowledge → Mission → Reasoning
 * No tenant → fail closed: "No active client selected."
 */

const NO_ACTIVE_CLIENT = 'No active client selected.';

function asPositiveId(value) {
  if (value == null || value === '') return null;
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

/**
 * Resolve the active tenant from a request without defaulting to client 1.
 * Client-role users are locked to users.client_id.
 */
function resolveActiveTenantId(req) {
  const user = req?.user || req?.session?.user || null;
  const role = user?.role || null;
  if (role === 'client') {
    return asPositiveId(user.client_id);
  }
  const sessionId = asPositiveId(req?.session?.active_client_id);
  if (sessionId != null) return sessionId;
  return asPositiveId(user?.client_id);
}

function failClosed(extra = {}) {
  return {
    ok: false,
    error: 'no_active_client',
    message: NO_ACTIVE_CLIENT,
    tenant: null,
    tenantId: null,
    blueprint: null,
    publishedAim: null,
    knowledge: null,
    mission: null,
    reasoning: null,
    ...extra,
  };
}

/**
 * Build the Max prompt context for an activated tenant.
 * Missing tenant fails closed. Missing intelligence stays empty — never borrowed.
 */
function resolveMaxPromptContext(input = {}) {
  const tenant = input.tenant || null;
  const tenantId = tenant?.id ?? input.tenantId ?? input.activeTenantId ?? null;
  if (tenantId == null || tenantId === '') {
    return failClosed();
  }

  const blueprint = input.blueprint || null;
  const publishedAim = input.publishedAim || null;
  const knowledge = Array.isArray(input.knowledge) ? input.knowledge : (input.knowledge || []);
  const mission = input.mission || null;
  const workspace = input.workspace || null;

  return {
    ok: true,
    error: null,
    message: null,
    tenantId: String(tenantId),
    tenant,
    blueprint,
    publishedAim,
    knowledge,
    mission,
    workspace,
    reasoning: {
      hasTenant: true,
      hasBlueprint: Boolean(blueprint),
      hasPublishedAim: Boolean(publishedAim),
      knowledgeCount: Array.isArray(knowledge) ? knowledge.length : 0,
      hasMission: Boolean(mission),
      readyForOnboarding: !blueprint && !publishedAim,
    },
  };
}

function buildTenantGreeting(tenantName) {
  const name = String(tenantName || '').trim() || 'there';
  const greeting = `Welcome, ${name}.`;
  const body = [
    "Let's begin by understanding your business.",
    'The first step is completing Client Intelligence so I can understand your company.',
    "After that we'll build your Acquisition Intelligence Model and begin prospect discovery.",
  ];
  const prompt = 'Shall we start Client Intelligence?';
  return {
    greeting,
    body,
    prompt,
    fullText: [greeting, '', ...body, '', prompt].join('\n'),
  };
}

module.exports = {
  NO_ACTIVE_CLIENT,
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  failClosed,
};
