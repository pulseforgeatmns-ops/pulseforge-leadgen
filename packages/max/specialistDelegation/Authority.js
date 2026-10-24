'use strict';

/**
 * SPEC-098 — authority validation and policy supremacy.
 *
 * Effective authority =
 *   Delegation authority ∩ tenant policy ∩ capability policy ∩ platform safety
 *
 * execute is never inferred. Fail closed. Do not silently downgrade.
 */

const {
  AUTHORITY_LEVELS,
  AUTHORITY_RANK,
  SpecialistDelegationError,
  asText,
  isPlainObject,
  normalizeConstraints,
} = require('./Types');

const DEFAULT_TENANT_DELEGATION_POLICY = Object.freeze({
  maxDelegationAuthority: 'execute_after_approval',
  blockedSpecialists: Object.freeze([]),
  blockedCapabilities: Object.freeze([]),
  blockAutonomousOutreach: true,
  blockedChannels: Object.freeze([]),
  approvalRequiredChannels: Object.freeze(['email', 'linkedin', 'sms']),
});

const EXECUTION_AUTHORITIES = Object.freeze(['execute', 'execute_after_approval']);

function policyEvent(kind, message, extras = {}) {
  return {
    kind,
    message,
    ...extras,
  };
}

function rankOf(authority) {
  return AUTHORITY_RANK[authority] != null ? AUTHORITY_RANK[authority] : -1;
}

function normalizeTenantPolicy(policy) {
  const src = isPlainObject(policy) ? policy : {};
  const maxAuth = AUTHORITY_LEVELS.includes(src.maxDelegationAuthority)
    ? src.maxDelegationAuthority
    : DEFAULT_TENANT_DELEGATION_POLICY.maxDelegationAuthority;
  return {
    maxDelegationAuthority: maxAuth,
    blockedSpecialists: Array.isArray(src.blockedSpecialists)
      ? src.blockedSpecialists.map(asText).filter(Boolean)
      : [],
    blockedCapabilities: Array.isArray(src.blockedCapabilities)
      ? src.blockedCapabilities.map(asText).filter(Boolean)
      : [],
    blockAutonomousOutreach:
      src.blockAutonomousOutreach !== undefined
        ? src.blockAutonomousOutreach === true
        : DEFAULT_TENANT_DELEGATION_POLICY.blockAutonomousOutreach,
    blockedChannels: Array.isArray(src.blockedChannels)
      ? src.blockedChannels.map(asText).filter(Boolean)
      : [],
    approvalRequiredChannels: Array.isArray(src.approvalRequiredChannels)
      ? src.approvalRequiredChannels.map(asText).filter(Boolean)
      : DEFAULT_TENANT_DELEGATION_POLICY.approvalRequiredChannels.slice(),
  };
}

function constraintIsDeterminate(value) {
  if (value == null) return false;
  if (typeof value === 'string') return value.trim() !== '' && value.trim() !== 'unknown';
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  if (typeof value === 'number') return Number.isFinite(value);
  return true;
}

/**
 * Fail closed when a required constraint cannot be determined.
 * Specialists must not silently broaden scope.
 *
 * @param {object} constraints
 * @returns {object|null} policy event or null
 */
function validateConstraints(constraints) {
  const c = normalizeConstraints(constraints);
  const required = Array.isArray(c.requiredDeterminate) ? c.requiredDeterminate : [];
  for (const key of required) {
    if (!constraintIsDeterminate(c[key])) {
      return policyEvent(
        'constraint_indeterminate',
        `Required constraint "${key}" cannot be determined — fail closed.`,
        { constraint: key }
      );
    }
  }
  return null;
}

/**
 * Validate a delegation against registry + tenant + platform policy.
 * Returns { ok, events, effectiveAuthority } or throws on structural errors.
 *
 * @param {object} input
 * @param {object} input.delegation
 * @param {object|null} input.capability
 * @param {object} [input.tenantPolicy]
 * @returns {{ ok: boolean, events: object[], effectiveAuthority: string|null }}
 */
function validateDelegationAuthority(input = {}) {
  const delegation = input.delegation || {};
  const capability = input.capability || null;
  const tenantPolicy = normalizeTenantPolicy(input.tenantPolicy);
  const events = [];

  const authority = asText(delegation.authority);
  if (!authority) {
    events.push(
      policyEvent(
        'missing_authority',
        'Delegation authority is required and cannot be inferred.'
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }
  if (!AUTHORITY_LEVELS.includes(authority)) {
    events.push(
      policyEvent(
        'missing_authority',
        `Unknown authority "${authority}". Canonical levels: ${AUTHORITY_LEVELS.join(', ')}.`
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (!capability) {
    events.push(
      policyEvent(
        'unknown_capability',
        `Unknown capability ${delegation.specialist}/${delegation.capability}.`
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (!capability.authoritySupported.includes(authority)) {
    events.push(
      policyEvent(
        'unsupported_authority',
        `Capability ${capability.specialist}/${capability.capability} does not support authority "${authority}".`,
        { supported: capability.authoritySupported.slice() }
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (tenantPolicy.blockedSpecialists.includes(capability.specialist)) {
    events.push(
      policyEvent(
        'tenant_policy_conflict',
        `Tenant policy blocks specialist "${capability.specialist}".`
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (tenantPolicy.blockedCapabilities.includes(capability.capability)) {
    events.push(
      policyEvent(
        'tenant_policy_conflict',
        `Tenant policy blocks capability "${capability.capability}".`
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (rankOf(authority) > rankOf(tenantPolicy.maxDelegationAuthority)) {
    events.push(
      policyEvent(
        'tenant_policy_conflict',
        `Requested authority "${authority}" exceeds tenant max "${tenantPolicy.maxDelegationAuthority}".`,
        { requested: authority, tenantMax: tenantPolicy.maxDelegationAuthority }
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (
    tenantPolicy.blockAutonomousOutreach &&
    authority === 'execute' &&
    ['emmett', 'sam', 'scout', 'cal'].includes(capability.specialist)
  ) {
    events.push(
      policyEvent(
        'tenant_policy_conflict',
        'Tenant policy blocks autonomous outreach execution.'
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  const constraints = normalizeConstraints(delegation.constraints);
  const allowed = Array.isArray(constraints.allowedChannels)
    ? constraints.allowedChannels.map(asText).filter(Boolean)
    : [];
  const blockedHit = allowed.find((ch) => tenantPolicy.blockedChannels.includes(ch));
  if (blockedHit) {
    events.push(
      policyEvent(
        'tenant_policy_conflict',
        `Tenant policy blocks channel "${blockedHit}".`
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  if (authority === 'execute' && allowed.length) {
    const needsApproval = allowed.find((ch) =>
      tenantPolicy.approvalRequiredChannels.includes(ch)
    );
    if (needsApproval) {
      events.push(
        policyEvent(
          'platform_safety_conflict',
          `Channel "${needsApproval}" requires execute_after_approval — execute cannot be granted.`,
          { channel: needsApproval }
        )
      );
      return { ok: false, events, effectiveAuthority: null };
    }
  }

  if (authority === 'execute' && !capability.callable) {
    events.push(
      policyEvent(
        'platform_safety_conflict',
        'execute cannot be granted to a capability without a callable adapter.'
      )
    );
    return { ok: false, events, effectiveAuthority: null };
  }

  const constraintEvent = validateConstraints(constraints);
  if (constraintEvent) {
    events.push(constraintEvent);
    return { ok: false, events, effectiveAuthority: null };
  }

  return { ok: true, events, effectiveAuthority: authority };
}

function assertAuthorizedTenant(authorizedTenantId, claimedTenantId) {
  const authorized = asText(authorizedTenantId);
  const claimed = asText(claimedTenantId);
  if (!authorized) {
    throw new SpecialistDelegationError(
      'tenant_required',
      'authorizedTenantId is required — never trust a client-supplied tenant id.',
      403
    );
  }
  if (claimed && claimed !== authorized) {
    throw new SpecialistDelegationError(
      'tenant_mismatch',
      'Claimed tenantId does not match the authorized tenant.',
      403,
      { authorizedTenantId: authorized }
    );
  }
  return authorized;
}

module.exports = {
  DEFAULT_TENANT_DELEGATION_POLICY,
  EXECUTION_AUTHORITIES,
  normalizeTenantPolicy,
  validateDelegationAuthority,
  validateConstraints,
  assertAuthorizedTenant,
  policyEvent,
  rankOf,
};
