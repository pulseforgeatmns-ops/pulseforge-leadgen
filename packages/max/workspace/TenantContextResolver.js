'use strict';

/**
 * SPEC-114 — Max tenant context resolution.
 *
 * Active Tenant → Business Blueprint → Published AIM → Knowledge → Mission → Reasoning
 * No tenant → fail closed: "No active client selected."
 */

const {
  buildOnboardingGreeting,
  BEGIN_CLIENT_INTELLIGENCE,
} = require('../../../services/pilotOnboarding');

const NO_ACTIVE_CLIENT = 'No active client selected.';
const NO_WORKSPACE = 'No workspace provisioned.';
const REGISTRATION_GREETING = [
  'Welcome to PulseForge.',
  'Before I can help you grow your business, I need to understand it.',
  'Everything I recommend will be grounded in what you teach me.',
  "Let's begin with Client Intelligence.",
];

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
    error: extra.error || 'no_active_client',
    message: extra.message || NO_ACTIVE_CLIENT,
    tenant: null,
    tenantId: null,
    workspace: null,
    blueprint: null,
    publishedAim: null,
    knowledge: null,
    mission: null,
    reasoning: null,
    ...extra,
  };
}

function failClosedNoWorkspace(extra = {}) {
  return failClosed({
    error: 'no_workspace',
    message: NO_WORKSPACE,
    ...extra,
  });
}

/**
 * Build the Max prompt context for an activated tenant.
 * Missing tenant fails closed. Missing intelligence stays empty — never borrowed.
 */
function resolveMaxPromptContext(input = {}) {
  const user = input.user || null;
  const tenant = input.tenant || null;
  const workspace = input.workspace || null;
  const tenantId = tenant?.id ?? input.tenantId ?? input.activeTenantId ?? workspace?.client_id ?? null;

  if (user?.role === 'client' && (tenantId == null || tenantId === '' || !workspace)) {
    return failClosedNoWorkspace();
  }
  if (tenantId == null || tenantId === '') {
    return failClosed();
  }
  if (input.requireWorkspace && !workspace) {
    return failClosedNoWorkspace();
  }

  const blueprint = input.blueprint || null;
  const publishedAim = input.publishedAim || null;
  const knowledge = Array.isArray(input.knowledge) ? input.knowledge : (input.knowledge || []);
  const mission = input.mission || null;

  return {
    ok: true,
    error: null,
    message: null,
    user: user
      ? { id: user.id, role: user.role, client_id: user.client_id || null }
      : null,
    tenantId: String(tenantId),
    tenant,
    workspace,
    blueprint,
    publishedAim,
    knowledge,
    mission,
    reasoning: {
      hasUser: Boolean(user),
      hasWorkspace: Boolean(workspace),
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
  return buildOnboardingGreeting(tenantName);
}

function buildRegistrationGreeting() {
  const [greeting, ...body] = REGISTRATION_GREETING;
  return {
    greeting,
    body,
    prompt: BEGIN_CLIENT_INTELLIGENCE,
    cta: BEGIN_CLIENT_INTELLIGENCE,
    fullText: REGISTRATION_GREETING.join('\n'),
  };
}

function greetingForWorkspace(workspace, tenantName) {
  if (workspace && workspace.origin === 'self_service') {
    return buildRegistrationGreeting();
  }
  return buildTenantGreeting(tenantName);
}

module.exports = {
  NO_ACTIVE_CLIENT,
  NO_WORKSPACE,
  BEGIN_CLIENT_INTELLIGENCE,
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  buildRegistrationGreeting,
  greetingForWorkspace,
  failClosed,
  failClosedNoWorkspace,
};
