'use strict';

/**
 * Canonical single-sender identity for a tenant.
 *
 * Authority is clients.sender_email / sender_name / sending_domain only.
 * Environment sender values are not tenant identity and must not override
 * this contract for canonical Acquisition Mission Orchestration (AMO) sends.
 *
 * CIE sender_identity is onboarding context, not execution authority,
 * until it is persisted into clients.sender_*.
 */

const BLOCK_CODES = Object.freeze({
  TENANT_REQUIRED: 'canonical_sender_tenant_required',
  CLIENT_NOT_FOUND: 'canonical_sender_client_not_found',
  INCOMPLETE: 'canonical_sender_incomplete',
  DOMAIN_MISMATCH: 'canonical_sender_domain_mismatch',
  REQUIRED: 'canonical_sender_required',
  CAPACITY_MISSING: 'capacity_sender_identity_missing',
  CAPACITY_STALE: 'capacity_sender_identity_stale',
  NOT_READY: 'canonical_sender_not_ready',
});

const SENDER_IDENTITY_STATUS = Object.freeze({
  MATCH: 'match',
  MISMATCH: 'mismatch',
  UNKNOWN: 'unknown',
});

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalized(value) {
  return clean(value).toLowerCase();
}

function emailDomain(email) {
  const at = normalized(email).split('@');
  return at.length === 2 && at[1] ? at[1] : '';
}

function normalizeSendingDomain(value) {
  let host = normalized(value);
  if (!host) return '';
  host = host.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split(':')[0];
  host = host.replace(/[<>"'()]/g, '');
  return host;
}

function tenantKey(input = {}) {
  const clientId = input.clientId != null && input.clientId !== ''
    ? input.clientId
    : input.tenantId;
  const tenantId = input.tenantId != null && input.tenantId !== ''
    ? String(input.tenantId)
    : (clientId != null && clientId !== '' ? String(clientId) : '');
  return { tenantId, clientId };
}

function blocked(code, blockReason, extras = {}) {
  return {
    ok: false,
    identity: null,
    code,
    blockReason,
    ...extras,
  };
}

function identityFromClient(client, input = {}) {
  const { tenantId, clientId } = tenantKey({
    tenantId: input.tenantId != null ? input.tenantId : client?.id,
    clientId: input.clientId != null ? input.clientId : client?.id,
  });
  return {
    tenantId,
    clientId: clientId != null && clientId !== '' ? clientId : (client?.id ?? null),
    senderEmail: clean(client?.sender_email || client?.senderEmail),
    senderName: clean(client?.sender_name || client?.senderName),
    sendingDomain: normalizeSendingDomain(client?.sending_domain || client?.sendingDomain),
  };
}

function validateCanonicalSenderConfig(identity = {}) {
  const senderEmail = clean(identity.senderEmail);
  const senderName = clean(identity.senderName);
  const sendingDomain = normalizeSendingDomain(identity.sendingDomain);
  if (!senderEmail || !senderName || !sendingDomain) {
    return blocked(
      BLOCK_CODES.INCOMPLETE,
      'Canonical sender identity is incomplete. clients.sender_email, sender_name, and sending_domain are required.',
      {
        details: {
          senderEmail: senderEmail || null,
          senderName: senderName || null,
          sendingDomain: sendingDomain || null,
        },
      }
    );
  }
  const domain = emailDomain(senderEmail);
  if (!domain || domain !== sendingDomain) {
    return blocked(
      BLOCK_CODES.DOMAIN_MISMATCH,
      'Canonical sender email domain does not match clients.sending_domain.',
      {
        details: {
          senderEmail,
          senderDomain: domain || null,
          sendingDomain,
        },
      }
    );
  }
  return {
    ok: true,
    identity: {
      tenantId: identity.tenantId != null ? String(identity.tenantId) : '',
      clientId: identity.clientId,
      senderEmail,
      senderName,
      sendingDomain,
    },
    code: null,
    blockReason: null,
  };
}

/**
 * Resolve the single canonical sender for a tenant from clients.*.
 * Env vars are never consulted.
 *
 * @param {object} input
 * @param {string|number} [input.tenantId]
 * @param {string|number} [input.clientId]
 * @param {object} [input.client] — already-loaded clients row
 * @param {object} [input.pool]
 * @param {Function} [input.loadClient]
 */
async function resolveCanonicalSenderIdentity(input = {}) {
  const { tenantId, clientId } = tenantKey(input);
  if (tenantId === '' && (clientId == null || clientId === '')) {
    return blocked(BLOCK_CODES.TENANT_REQUIRED, 'Tenant id is required to resolve canonical sender identity.');
  }

  let client = input.client && typeof input.client === 'object' ? input.client : null;
  if (!client) {
    const load = input.loadClient;
    if (typeof load === 'function') {
      client = await load(clientId != null ? clientId : tenantId, input);
    } else if (input.pool) {
      const id = clientId != null && clientId !== '' ? clientId : tenantId;
      const result = await input.pool.query(
        `SELECT id, sender_email, sender_name, sending_domain, active
         FROM clients WHERE id = $1`,
        [id]
      );
      client = result.rows[0] || null;
    }
  }

  if (!client) {
    return blocked(
      BLOCK_CODES.CLIENT_NOT_FOUND,
      'Canonical sender identity could not be loaded from tenant configuration.',
      { tenantId, clientId }
    );
  }

  return validateCanonicalSenderConfig(identityFromClient(client, { tenantId, clientId }));
}

function normalizeCanonicalSender(value) {
  if (!value) {
    return blocked(BLOCK_CODES.REQUIRED, 'Canonical sender identity is required for tenant acquisition sends.');
  }
  if (typeof value === 'string') {
    return blocked(
      BLOCK_CODES.REQUIRED,
      'Canonical sender identity must be an explicit sender object, not an email string or environment fallback.'
    );
  }
  return validateCanonicalSenderConfig({
    tenantId: value.tenantId,
    clientId: value.clientId,
    senderEmail: value.senderEmail || value.email || value.senderIdentity,
    senderName: value.senderName || value.name,
    sendingDomain: value.sendingDomain || value.domain,
  });
}

function extractCapacitySenderIdentity(payload = {}) {
  const explicit = payload.senderIdentity && typeof payload.senderIdentity === 'object'
    ? payload.senderIdentity
    : {};
  const inboxId = clean(explicit.inboxId || explicit.senderEmail || payload.inboxId);
  const senderEmail = clean(explicit.senderEmail || inboxId);
  const sendingDomain = normalizeSendingDomain(
    explicit.sendingDomain || explicit.domain || payload.sendingDomain || payload.domain
  );
  return {
    inboxId: inboxId || null,
    senderEmail: senderEmail || null,
    sendingDomain: sendingDomain || null,
    domain: sendingDomain || null,
  };
}

function identitiesMatch(left = {}, right = {}) {
  const leftEmail = normalized(left.senderEmail || left.inboxId || left.email);
  const rightEmail = normalized(right.senderEmail || right.inboxId || right.email);
  const leftDomain = normalizeSendingDomain(left.sendingDomain || left.domain);
  const rightDomain = normalizeSendingDomain(right.sendingDomain || right.domain);
  return Boolean(leftEmail) && Boolean(rightEmail) && leftEmail === rightEmail
    && Boolean(leftDomain) && Boolean(rightDomain) && leftDomain === rightDomain;
}

function assertCapacityMatchesCanonical(capacityIdentity, canonical) {
  const capacity = extractCapacitySenderIdentity(
    capacityIdentity && capacityIdentity.senderIdentity
      ? capacityIdentity
      : { senderIdentity: capacityIdentity }
  );
  if (!capacity.senderEmail || !capacity.sendingDomain) {
    return blocked(
      BLOCK_CODES.CAPACITY_MISSING,
      'CAPACITY contribution is missing canonical sender identity. Re-prepare before sending.',
      { capacityIdentity: capacity }
    );
  }
  if (!identitiesMatch(capacity, canonical)) {
    return blocked(
      BLOCK_CODES.CAPACITY_STALE,
      'Execution approval is stale: sender identity changed since CAPACITY preparation.',
      {
        capacityIdentity: capacity,
        canonicalSender: {
          senderEmail: canonical.senderEmail,
          sendingDomain: canonical.sendingDomain,
        },
      }
    );
  }
  return { ok: true, code: null, blockReason: null, capacityIdentity: capacity };
}

function clientRowFromIdentity(identity = {}) {
  return {
    id: identity.clientId,
    sender_email: identity.senderEmail,
    sender_name: identity.senderName,
    sending_domain: identity.sendingDomain,
  };
}

async function evaluateCanonicalSenderReadiness(input = {}) {
  const { evaluateSenderIdentityReadiness } = require('./sendingReadiness');
  const resolved = input.identity
    ? validateCanonicalSenderConfig(input.identity)
    : await resolveCanonicalSenderIdentity(input);
  if (!resolved.ok) {
    return {
      ready: false,
      sendable: false,
      code: resolved.code,
      blockReason: resolved.blockReason,
      identity: null,
      checks: [],
      failures: [{ code: resolved.code, message: resolved.blockReason, details: resolved.details || {} }],
    };
  }
  const readiness = await evaluateSenderIdentityReadiness({
    client: input.client || clientRowFromIdentity(resolved.identity),
    brevoState: input.brevoState,
    brevoApiKey: input.brevoApiKey,
    http: input.http,
  });
  const failures = readiness.failures || [];
  const blockReason = failures.length
    ? failures.map((row) => row.message).join(' ')
    : null;
  return {
    ready: readiness.sendable === true,
    sendable: readiness.sendable === true,
    code: readiness.sendable ? null : BLOCK_CODES.NOT_READY,
    blockReason,
    identity: resolved.identity,
    checks: readiness.checks,
    failures,
  };
}

function classifyEventSenderIdentity({ eventDomain, tenantDomain } = {}) {
  const event = normalizeSendingDomain(eventDomain);
  const tenant = normalizeSendingDomain(tenantDomain);
  if (!event) {
    return {
      status: SENDER_IDENTITY_STATUS.UNKNOWN,
      reason: 'missing_sender_domain',
      eventDomain: null,
      tenantDomain: tenant || null,
      matchesTenant: false,
      contaminatesReputation: false,
    };
  }
  if (!tenant) {
    return {
      status: SENDER_IDENTITY_STATUS.UNKNOWN,
      reason: 'tenant_domain_unconfigured',
      eventDomain: event,
      tenantDomain: null,
      matchesTenant: false,
      contaminatesReputation: false,
    };
  }
  if (event === tenant) {
    return {
      status: SENDER_IDENTITY_STATUS.MATCH,
      reason: null,
      eventDomain: event,
      tenantDomain: tenant,
      matchesTenant: true,
      contaminatesReputation: false,
    };
  }
  return {
    status: SENDER_IDENTITY_STATUS.MISMATCH,
    reason: 'event_domain_does_not_match_tenant_sending_domain',
    eventDomain: event,
    tenantDomain: tenant,
    matchesTenant: false,
    contaminatesReputation: true,
  };
}

function shouldIngestCanonicalReputation(classification = {}) {
  return classification.status !== SENDER_IDENTITY_STATUS.MISMATCH;
}

module.exports = {
  BLOCK_CODES,
  SENDER_IDENTITY_STATUS,
  clean,
  normalized,
  emailDomain,
  normalizeSendingDomain,
  identityFromClient,
  validateCanonicalSenderConfig,
  resolveCanonicalSenderIdentity,
  normalizeCanonicalSender,
  extractCapacitySenderIdentity,
  identitiesMatch,
  assertCapacityMatchesCanonical,
  evaluateCanonicalSenderReadiness,
  classifyEventSenderIdentity,
  shouldIngestCanonicalReputation,
  clientRowFromIdentity,
};
