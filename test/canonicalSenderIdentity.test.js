'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  BLOCK_CODES,
  resolveCanonicalSenderIdentity,
  validateCanonicalSenderConfig,
  normalizeCanonicalSender,
  extractCapacitySenderIdentity,
  assertCapacityMatchesCanonical,
  classifyEventSenderIdentity,
  shouldIngestCanonicalReputation,
  SENDER_IDENTITY_STATUS,
  evaluateCanonicalSenderReadiness,
} = require('../utils/canonicalSenderIdentity');
const { evaluateSenderIdentityReadiness } = require('../utils/sendingReadiness');

const ANCHOR_CLIENT = {
  id: 10,
  sender_email: 'anchor@domain.com',
  sender_name: 'Anchor Cleaning',
  sending_domain: 'domain.com',
};

describe('canonical sender resolution', () => {
  const priorFrom = process.env.FROM_EMAIL;
  const priorBrevo = process.env.BREVO_SENDER_EMAIL;

  beforeEach(() => {
    process.env.FROM_EMAIL = 'hello@gopulseforge.com';
    process.env.BREVO_SENDER_EMAIL = 'ops@gopulseforge.com';
  });

  afterEach(() => {
    if (priorFrom == null) delete process.env.FROM_EMAIL;
    else process.env.FROM_EMAIL = priorFrom;
    if (priorBrevo == null) delete process.env.BREVO_SENDER_EMAIL;
    else process.env.BREVO_SENDER_EMAIL = priorBrevo;
  });

  it('resolves tenant 10 from clients.sender_* and ignores FROM_EMAIL', async () => {
    const resolved = await resolveCanonicalSenderIdentity({
      tenantId: '10',
      clientId: 10,
      client: ANCHOR_CLIENT,
    });
    assert.equal(resolved.ok, true);
    assert.equal(resolved.identity.senderEmail, 'anchor@domain.com');
    assert.equal(resolved.identity.senderName, 'Anchor Cleaning');
    assert.equal(resolved.identity.sendingDomain, 'domain.com');
    assert.notEqual(resolved.identity.senderEmail, process.env.FROM_EMAIL);
  });

  it('loads clients from the tenant row, not environment defaults', async () => {
    const pool = {
      async query(sql, params) {
        assert.match(sql, /FROM clients/);
        assert.equal(String(params[0]), '10');
        return { rows: [ANCHOR_CLIENT] };
      },
    };
    const resolved = await resolveCanonicalSenderIdentity({
      tenantId: 10,
      pool,
    });
    assert.equal(resolved.ok, true);
    assert.equal(resolved.identity.senderEmail, 'anchor@domain.com');
  });

  it('blocks missing tenant sender configuration instead of falling back', async () => {
    const resolved = await resolveCanonicalSenderIdentity({
      tenantId: '10',
      clientId: 10,
      client: { id: 10, sender_email: null, sender_name: null, sending_domain: null },
    });
    assert.equal(resolved.ok, false);
    assert.equal(resolved.code, BLOCK_CODES.INCOMPLETE);
    assert.match(resolved.blockReason, /incomplete/i);
  });

  it('blocks when sender email domain does not match sending_domain', async () => {
    const resolved = validateCanonicalSenderConfig({
      tenantId: '10',
      clientId: 10,
      senderEmail: 'anchor@other.com',
      senderName: 'Anchor Cleaning',
      sendingDomain: 'domain.com',
    });
    assert.equal(resolved.ok, false);
    assert.equal(resolved.code, BLOCK_CODES.DOMAIN_MISMATCH);
  });

  it('rejects a bare email string so env fallbacks cannot sneak in', () => {
    const resolved = normalizeCanonicalSender(process.env.FROM_EMAIL);
    assert.equal(resolved.ok, false);
    assert.equal(resolved.code, BLOCK_CODES.REQUIRED);
  });
});

describe('CAPACITY identity binding', () => {
  it('matches CAPACITY inbox/domain to the live canonical sender', () => {
    const capacity = {
      senderIdentity: {
        inboxId: 'anchor@domain.com',
        senderEmail: 'anchor@domain.com',
        domain: 'domain.com',
        sendingDomain: 'domain.com',
      },
    };
    const bind = assertCapacityMatchesCanonical(capacity, {
      senderEmail: 'anchor@domain.com',
      sendingDomain: 'domain.com',
    });
    assert.equal(bind.ok, true);
  });

  it('treats a post-CAPACITY sender change as a stale approval', () => {
    const capacity = extractCapacitySenderIdentity({
      senderIdentity: {
        inboxId: 'a@domain-a.com',
        senderEmail: 'a@domain-a.com',
        sendingDomain: 'domain-a.com',
      },
    });
    const bind = assertCapacityMatchesCanonical(
      { senderIdentity: capacity },
      { senderEmail: 'b@domain-b.com', sendingDomain: 'domain-b.com' }
    );
    assert.equal(bind.ok, false);
    assert.equal(bind.code, BLOCK_CODES.CAPACITY_STALE);
    assert.match(bind.blockReason, /stale/i);
  });
});

describe('provider readiness reuse', () => {
  it('blocks inactive sender and unauthenticated domain through evaluateSendingReadiness authority', async () => {
    const readiness = await evaluateSenderIdentityReadiness({
      client: ANCHOR_CLIENT,
      brevoState: {
        domain: { verified: false, authenticated: false },
        sender: { email: ANCHOR_CLIENT.sender_email, active: false },
        errors: [],
      },
    });
    assert.equal(readiness.sendable, false);
    const codes = readiness.failures.map((row) => row.code);
    assert.ok(codes.includes('brevo_domain_authenticated'));
    assert.ok(codes.includes('brevo_sender_active'));

    const canonical = await evaluateCanonicalSenderReadiness({
      client: ANCHOR_CLIENT,
      tenantId: 10,
      brevoState: {
        domain: { verified: true, authenticated: true },
        sender: { email: ANCHOR_CLIENT.sender_email, active: true },
        errors: [],
      },
    });
    assert.equal(canonical.ready, true);
  });
});

describe('provider event identity classification', () => {
  it('matches the tenant sending domain', () => {
    const classification = classifyEventSenderIdentity({
      eventDomain: 'domain-a.com',
      tenantDomain: 'domain-a.com',
    });
    assert.equal(classification.status, SENDER_IDENTITY_STATUS.MATCH);
    assert.equal(shouldIngestCanonicalReputation(classification), true);
  });

  it('marks a known foreign domain as mismatch and excludes it from reputation', () => {
    const classification = classifyEventSenderIdentity({
      eventDomain: 'domain-b.com',
      tenantDomain: 'domain-a.com',
    });
    assert.equal(classification.status, SENDER_IDENTITY_STATUS.MISMATCH);
    assert.equal(classification.contaminatesReputation, true);
    assert.equal(shouldIngestCanonicalReputation(classification), false);
    assert.equal(classification.eventDomain, 'domain-b.com');
    assert.equal(classification.tenantDomain, 'domain-a.com');
  });

  it('treats missing sender-domain evidence as unknown, not mismatch', () => {
    const classification = classifyEventSenderIdentity({
      eventDomain: null,
      tenantDomain: 'domain-a.com',
    });
    assert.equal(classification.status, SENDER_IDENTITY_STATUS.UNKNOWN);
    assert.equal(classification.reason, 'missing_sender_domain');
    assert.equal(classification.contaminatesReputation, false);
    assert.equal(shouldIngestCanonicalReputation(classification), true);
  });
});
