'use strict';

/**
 * Canonical single-sender identity for a tenant.
 *
 * Authority: clients.sender_email / clients.sender_name / clients.sending_domain
 * Environment sender values are NOT tenant identity authority for acquisition sends.
 *
 * CIE sender_identity is not execution authority until persisted into clients.sender_*.
 * If CIE and client configuration disagree, clients sender config wins for execution.
 */

const { getBrevoState } = require('./sendingReadiness');

const SENDER_CONFIG_CODES = Object.freeze({
  MISSING_EMAIL: 'canonical_sender_email_missing',
  MISSING_NAME: 'canonical_sender_name_missing',
  MISSING_DOMAIN: 'canonical_sending_domain_missing',
  DOMAIN_MISMATCH: 'canonical_sender_domain_mismatch',
  IDENTITY_DRIFT: 'canonical_sender_identity_drift',
  CAPACITY_IDENTITY_MISSING: 'capacity_sender_identity_missing',
  READINESS_FAILED: 'canonical_sender_readiness_failed',
});

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalized(value) {
  return clean(value).toLowerCase();
}

function emailLocalDomain(email) {
  const parts = normalized(email).split('@');
  return parts.length === 2 && parts[1] ? parts[1] : '';
}

function normalizeSendingDomain(value) {
  let host = normalized(value);
  if (!host) return '';
  host = host.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split(':')[0];
  host = host.replace(/[<>"'()]/g, '');
  return host;
}

function condition(code, passed, message, details = {}) {
  return { code, passed: Boolean(passed), message, details };
}

/**
 * Resolve canonical sender identity from a clients row (or equivalent).
 * Does not read FROM_EMAIL / BREVO_SENDER_* as authority.
 *
 * @param {{ tenantId?: string|number, clientId?: string|number, client?: object }} input
 * @returns {{ tenantId: string, clientId: number|string, senderEmail: string, senderName: string, sendingDomain: string }}
 */
function resolveCanonicalSenderIdentity(input = {}) {
  const client = input.client && typeof input.client === 'object' ? input.client : null;
  const clientId = client?.id != null
    ? client.id
    : (input.clientId != null ? input.clientId : input.tenantId);
  const tenantId = input.tenantId != null ? input.tenantId : clientId;

  return {
    tenantId: tenantId != null ? String(tenantId) : '',
    clientId: clientId != null && clientId !== '' ? (Number(clientId) || clientId) : null,
    senderEmail: clean(client?.sender_email || client?.senderEmail),
    senderName: clean(client?.sender_name || client?.senderName),
    sendingDomain: normalizeSendingDomain(client?.sending_domain || client?.sendingDomain),
  };
}

/**
 * Validate that canonical sender configuration is complete and internally consistent.
 * @returns {{ ok: boolean, identity: object, failures: object[], reason: string|null }}
 */
function validateCanonicalSenderConfiguration(identity = {}, opts = {}) {
  const requireName = opts.requireName !== false;
  const senderEmail = clean(identity.senderEmail);
  const senderName = clean(identity.senderName);
  const sendingDomain = normalizeSendingDomain(identity.sendingDomain);
  const senderDomain = emailLocalDomain(senderEmail);

  const failures = [];
  failures.push(condition(
    SENDER_CONFIG_CODES.MISSING_EMAIL,
    Boolean(senderEmail),
    'Canonical sender email must be configured on the tenant (clients.sender_email).',
    { senderEmail: senderEmail || null }
  ));
  if (requireName) {
    failures.push(condition(
      SENDER_CONFIG_CODES.MISSING_NAME,
      Boolean(senderName),
      'Canonical sender name must be configured on the tenant (clients.sender_name).',
      { senderName: senderName || null }
    ));
  }
  failures.push(condition(
    SENDER_CONFIG_CODES.MISSING_DOMAIN,
    Boolean(sendingDomain),
    'Canonical sending domain must be configured on the tenant (clients.sending_domain).',
    { sendingDomain: sendingDomain || null }
  ));
  failures.push(condition(
    SENDER_CONFIG_CODES.DOMAIN_MISMATCH,
    Boolean(senderDomain) && senderDomain === sendingDomain,
    'Canonical sender email domain must match clients.sending_domain.',
    { senderDomain: senderDomain || null, sendingDomain: sendingDomain || null }
  ));

  const blocked = failures.filter((row) => !row.passed);
  return {
    ok: blocked.length === 0,
    identity: {
      tenantId: identity.tenantId != null ? String(identity.tenantId) : '',
      clientId: identity.clientId,
      senderEmail,
      senderName,
      sendingDomain,
    },
    failures: blocked,
    reason: blocked.length ? blocked.map((row) => row.message).join(' ') : null,
  };
}

/**
 * Load canonical sender identity from clients via pool.
 */
async function loadCanonicalSenderIdentity({ tenantId, clientId, pool } = {}) {
  if (!pool) {
    return {
      ok: false,
      identity: null,
      reason: 'Database pool is required to resolve canonical tenant sender identity.',
      failures: [condition('canonical_sender_pool_missing', false, 'Database pool is required.')],
    };
  }
  const id = Number(clientId != null ? clientId : tenantId);
  if (!Number.isFinite(id)) {
    return {
      ok: false,
      identity: null,
      reason: 'tenantId/clientId is required to resolve canonical sender identity.',
      failures: [condition('canonical_sender_tenant_missing', false, 'tenantId/clientId is required.')],
    };
  }

  let row;
  try {
    const result = await pool.query(
      `SELECT id, sender_email, sender_name, sending_domain
         FROM clients
        WHERE id = $1
        LIMIT 1`,
      [id]
    );
    row = result.rows[0] || null;
  } catch (err) {
    return {
      ok: false,
      identity: null,
      reason: `Failed to load tenant sender configuration: ${err.message}`,
      failures: [condition('canonical_sender_load_failed', false, err.message)],
    };
  }

  if (!row) {
    return {
      ok: false,
      identity: null,
      reason: `No client found for tenant ${id}.`,
      failures: [condition('canonical_sender_client_missing', false, `No client found for tenant ${id}.`)],
    };
  }

  const identity = resolveCanonicalSenderIdentity({ tenantId: id, clientId: row.id, client: row });
  const validated = validateCanonicalSenderConfiguration(identity);
  return {
    ok: validated.ok,
    identity: validated.identity,
    reason: validated.reason,
    failures: validated.failures,
  };
}

/**
 * Sender infrastructure readiness for canonical AMO sends.
 * Reuses Brevo domain/sender checks from evaluateSendingReadiness authority
 * without prospect-sequence gates.
 */
async function evaluateCanonicalSenderReadiness({
  client,
  identity,
  brevoState,
  brevoApiKey,
  http,
  requireName = true,
} = {}) {
  const resolvedIdentity = identity || resolveCanonicalSenderIdentity({
    client,
    clientId: client?.id,
    tenantId: client?.id,
  });
  const config = validateCanonicalSenderConfiguration(resolvedIdentity, { requireName });
  const checks = [];

  checks.push(condition(
    'client_sender_configured',
    Boolean(config.identity.senderEmail)
      && (!requireName || Boolean(config.identity.senderName))
      && Boolean(config.identity.sendingDomain),
    'Client sender_email, sender_name, and sending_domain must all be configured.',
    {
      sender_email: config.identity.senderEmail || null,
      sender_name: config.identity.senderName || null,
      sending_domain: config.identity.sendingDomain || null,
    }
  ));
  checks.push(condition(
    'client_sender_domain_matches',
    Boolean(emailLocalDomain(config.identity.senderEmail))
      && emailLocalDomain(config.identity.senderEmail) === config.identity.sendingDomain,
    'Client sender_email must use the configured sending_domain.',
    {
      sender_domain: emailLocalDomain(config.identity.senderEmail) || null,
      sending_domain: config.identity.sendingDomain || null,
    }
  ));

  const clientForBrevo = client || {
    id: resolvedIdentity.clientId,
    sender_email: config.identity.senderEmail,
    sender_name: config.identity.senderName,
    sending_domain: config.identity.sendingDomain,
  };

  const resolvedBrevo = brevoState || await getBrevoState(clientForBrevo, { brevoApiKey, http });
  const domainAuthenticated = Boolean(
    resolvedBrevo.domain?.authenticated === true && resolvedBrevo.domain?.verified === true
  );
  checks.push(condition(
    'brevo_domain_authenticated',
    domainAuthenticated,
    'Sending domain must be verified and authenticated in Brevo.',
    {
      sending_domain: config.identity.sendingDomain || null,
      verified: resolvedBrevo.domain?.verified ?? false,
      authenticated: resolvedBrevo.domain?.authenticated ?? false,
      errors: resolvedBrevo.errors || [],
    }
  ));

  const senderActive = Boolean(
    resolvedBrevo.sender && resolvedBrevo.sender.active === true
  );
  checks.push(condition(
    'brevo_sender_active',
    senderActive,
    'Sender email must be registered and active in Brevo.',
    {
      sender_email: config.identity.senderEmail || null,
      registered: Boolean(resolvedBrevo.sender),
      active: resolvedBrevo.sender?.active ?? false,
      errors: resolvedBrevo.errors || [],
    }
  ));

  const failures = checks.filter((row) => !row.passed);
  return {
    sendable: failures.length === 0,
    identity: config.identity,
    checks,
    failures,
    reason: failures.length ? failures.map((row) => row.message).join(' ') : null,
  };
}

/**
 * Extract CAPACITY-bound sender identity fields from Emmett contribution payload.
 */
function capacitySenderIdentity(emmettPayload = {}) {
  const bound = emmettPayload.senderIdentity || emmettPayload.sender_identity || null;
  if (!bound || typeof bound !== 'object') {
    return { inboxId: null, senderEmail: null, sendingDomain: null };
  }
  const senderEmail = clean(bound.senderEmail || bound.inboxId || bound.email);
  const sendingDomain = normalizeSendingDomain(bound.sendingDomain || bound.domain);
  return {
    inboxId: clean(bound.inboxId || senderEmail) || null,
    senderEmail: senderEmail || null,
    sendingDomain: sendingDomain || null,
  };
}

/**
 * Build the CAPACITY senderIdentity block from an infrastructure snapshot.
 */
function senderIdentityFromInfrastructureSnapshot(snapshot = {}) {
  const senderEmail = clean(snapshot.inboxId || snapshot.senderEmail || snapshot.sender_email);
  const sendingDomain = normalizeSendingDomain(snapshot.domain || snapshot.sendingDomain || snapshot.sending_domain);
  return {
    inboxId: senderEmail || null,
    senderEmail: senderEmail || null,
    sendingDomain: sendingDomain || null,
  };
}

/**
 * Verify CAPACITY-prepared identity still matches canonical tenant sender.
 * Drift after CAPACITY preparation blocks execution.
 */
function assertCapacitySenderBinding({ capacityPayload, canonicalSender } = {}) {
  const capacity = capacitySenderIdentity(capacityPayload);
  if (!capacity.senderEmail || !capacity.sendingDomain) {
    return {
      ok: false,
      code: SENDER_CONFIG_CODES.CAPACITY_IDENTITY_MISSING,
      reason: 'Emmett CAPACITY contribution is missing bound sender identity. Re-run PREPARE.',
      capacity,
      canonicalSender: null,
    };
  }

  const config = validateCanonicalSenderConfiguration(canonicalSender);
  if (!config.ok) {
    return {
      ok: false,
      code: config.failures[0]?.code || SENDER_CONFIG_CODES.READINESS_FAILED,
      reason: config.reason,
      capacity,
      canonicalSender: config.identity,
    };
  }

  const canonicalEmail = normalized(config.identity.senderEmail);
  const capacityEmail = normalized(capacity.senderEmail);
  const canonicalDomain = normalizeSendingDomain(config.identity.sendingDomain);
  const capacityDomain = normalizeSendingDomain(capacity.sendingDomain);

  if (canonicalEmail !== capacityEmail || canonicalDomain !== capacityDomain) {
    return {
      ok: false,
      code: SENDER_CONFIG_CODES.IDENTITY_DRIFT,
      reason: 'CAPACITY was prepared for a different sender identity. Re-approve execution after regenerating capacity.',
      capacity,
      canonicalSender: config.identity,
    };
  }

  return {
    ok: true,
    code: null,
    reason: null,
    capacity,
    canonicalSender: config.identity,
  };
}

/**
 * Compare provider-event domain to canonical tenant domain.
 * Distinguishes known mismatch from identity unknown.
 */
function classifyProviderEventSenderIdentity({
  eventSendingDomain,
  eventSenderEmail,
  tenantSendingDomain,
} = {}) {
  const eventDomain = normalizeSendingDomain(
    eventSendingDomain || emailLocalDomain(eventSenderEmail)
  );
  const tenantDomain = normalizeSendingDomain(tenantSendingDomain);

  if (!eventDomain) {
    return {
      status: 'unknown',
      match: null,
      reputationExcluded: false,
      eventDomain: null,
      tenantDomain: tenantDomain || null,
      reason: 'Provider event lacks sender-domain evidence.',
    };
  }

  if (!tenantDomain) {
    return {
      status: 'unknown',
      match: null,
      reputationExcluded: false,
      eventDomain,
      tenantDomain: null,
      reason: 'Tenant canonical sending domain is not configured.',
    };
  }

  if (eventDomain === tenantDomain) {
    return {
      status: 'matched',
      match: true,
      reputationExcluded: false,
      eventDomain,
      tenantDomain,
      reason: null,
    };
  }

  return {
    status: 'mismatch',
    match: false,
    reputationExcluded: true,
    eventDomain,
    tenantDomain,
    reason: `Provider event domain ${eventDomain} does not match tenant sending domain ${tenantDomain}.`,
  };
}

module.exports = {
  SENDER_CONFIG_CODES,
  clean,
  normalized,
  emailLocalDomain,
  normalizeSendingDomain,
  resolveCanonicalSenderIdentity,
  validateCanonicalSenderConfiguration,
  loadCanonicalSenderIdentity,
  evaluateCanonicalSenderReadiness,
  capacitySenderIdentity,
  senderIdentityFromInfrastructureSnapshot,
  assertCapacitySenderBinding,
  classifyProviderEventSenderIdentity,
};
