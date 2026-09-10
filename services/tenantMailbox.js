'use strict';

/**
 * SPEC-248 — tenant-owned mailbox and sending identity infrastructure.
 *
 * This module is intentionally transport-neutral at the public boundary. SMTP,
 * IMAP, and secret backends are resolved only inside execution-time functions.
 */

const crypto = require('crypto');
const dns = require('node:dns').promises;
const defaultPool = require('../db');

const PROVIDER_TYPES = Object.freeze({
  GENERIC_SMTP_IMAP: 'GENERIC_SMTP_IMAP',
  GOOGLE_WORKSPACE: 'GOOGLE_WORKSPACE',
  MICROSOFT_365: 'MICROSOFT_365',
});

const MAILBOX_STATUS = Object.freeze({
  ACTIVE: 'active',
  DISABLED: 'disabled',
  REVOKED: 'revoked',
  UNVERIFIED: 'unverified',
});

const IDENTITY_STATUS = Object.freeze({
  ACTIVE: 'active',
  INACTIVE: 'inactive',
  DISABLED: 'disabled',
  REVOKED: 'revoked',
  UNVERIFIED: 'unverified',
});

const MESSAGE_DIRECTION = Object.freeze({
  OUTBOUND: 'OUTBOUND',
  INBOUND: 'INBOUND',
});

const MESSAGE_STATUS = Object.freeze({
  PENDING: 'pending',
  SENT: 'sent',
  FAILED: 'failed',
  RECEIVED: 'received',
});

const THREAD_STATUS = Object.freeze({
  OPEN: 'open',
  REPLIED: 'replied',
  PAUSED: 'paused',
  COMPLETED: 'completed',
  FAILED: 'failed',
  STOPPED: 'manually_stopped',
});

const SEQUENCE_STATE = Object.freeze({
  NOT_YET_SENT: 'not_yet_sent',
  SENT: 'sent',
  REPLY_RECEIVED: 'reply_received',
  PAUSED: 'paused',
  COMPLETED: 'completed',
  FAILED: 'failed',
  MANUALLY_STOPPED: 'manually_stopped',
  QUALIFIED_HANDOFF: 'qualified_handoff',
  BOUNCED: 'bounced',
});

const EVENT_TYPES = Object.freeze({
  REPLY_RECEIVED: 'TENANT_OUTREACH_REPLY_RECEIVED',
});

const BABRUN_MAILBOX_CONFIG = Object.freeze({
  providerType: PROVIDER_TYPES.GENERIC_SMTP_IMAP,
  mailboxAddress: 'hello@babrun.com',
  displayName: 'Fedir | Babrun',
  senderEmail: 'hello@babrun.com',
  senderDisplayName: 'Fedir | Babrun',
  replyToAddress: 'hello@babrun.com',
  smtpHost: 'mail.adm.tools',
  smtpPort: 465,
  smtpTlsMode: 'SSL_TLS',
  imapHost: 'mail.adm.tools',
  imapPort: 993,
  imapTlsMode: 'SSL_TLS',
  smtpSecretRef: 'BABRUN_MAILBOX_SMTP_PASSWORD',
  imapSecretRef: 'BABRUN_MAILBOX_IMAP_PASSWORD',
});

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function tenantKey(value) {
  if (value == null || value === '') return '';
  return String(value);
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso(opts = {}) {
  if (typeof opts.now === 'function') return new Date(opts.now()).toISOString();
  return opts.now ? new Date(opts.now).toISOString() : new Date().toISOString();
}

function stableHash(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function prefixedId(prefix, value) {
  return `${prefix}_${stableHash(value || `${Date.now()}_${Math.random()}`).slice(0, 24)}`;
}

function generateRfcMessageId({ tenantId, sendingIdentityId, messageId } = {}) {
  const host = lower(String(sendingIdentityId || '').split('@').pop()) || `tenant-${tenantKey(tenantId)}.pulseforge.local`;
  return `<${messageId || prefixedId('pfmsg', `${tenantId}:${Date.now()}`)}@${host}>`;
}

function asJson(value, fallback) {
  if (value == null) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_err) {
    return fallback;
  }
}

function sanitizeErrorMessage(err) {
  const raw = String(err?.message || err || 'unknown_error');
  return raw
    .replace(/password=([^&\s]+)/ig, 'password=[redacted]')
    .replace(/pass=([^&\s]+)/ig, 'pass=[redacted]')
    .replace(/auth[^,\n]+/ig, 'auth=[redacted]')
    .slice(0, 500);
}

function mailboxError(code, message, extras = {}) {
  const err = new Error(message || code);
  err.code = code;
  Object.assign(err, extras);
  return err;
}

function publicIntegration(integration) {
  if (!integration) return null;
  const {
    smtpSecretRef: _smtpSecretRef,
    imapSecretRef: _imapSecretRef,
    sharedSecretRef: _sharedSecretRef,
    smtp_secret_ref: _smtp_secret_ref,
    imap_secret_ref: _imap_secret_ref,
    shared_secret_ref: _shared_secret_ref,
    ...safe
  } = integration;
  return {
    ...safe,
    hasSmtpSecretRef: Boolean(integration.smtpSecretRef || integration.smtp_secret_ref || integration.sharedSecretRef || integration.shared_secret_ref),
    hasImapSecretRef: Boolean(integration.imapSecretRef || integration.imap_secret_ref || integration.sharedSecretRef || integration.shared_secret_ref),
  };
}

function publicIdentity(identity) {
  if (!identity) return null;
  return { ...identity };
}

function resolveSecretRef(secretRef, opts = {}) {
  const ref = clean(secretRef);
  if (!ref) {
    throw mailboxError('mailbox_secret_ref_missing', 'Mailbox credential secret reference is missing.');
  }
  const resolver = opts.secretResolver;
  if (typeof resolver === 'function') {
    const value = resolver(ref);
    if (!value) throw mailboxError('mailbox_secret_missing', `Mailbox credential secret is not configured for ${ref}.`, { secretRef: ref });
    return value;
  }
  const env = opts.env || process.env;
  const value = env[ref];
  if (!value) throw mailboxError('mailbox_secret_missing', `Mailbox credential secret is not configured for ${ref}.`, { secretRef: ref });
  return value;
}

function outboundIdempotencyKey(input = {}) {
  return clean(input.idempotencyKey || input.metadata?.idempotencyKey)
    || stableHash([
      input.tenantId,
      input.sendingIdentityId,
      input.missionId,
      input.prospectId,
      input.outreachAssetId,
      input.threadId,
      lower(input.to),
      input.subject,
      input.body,
    ].join('|'));
}

function normalizeRecipients(to) {
  const list = Array.isArray(to) ? to : [to];
  return list.map((value) => clean(value)).filter(Boolean);
}

function normalizedSubject(subject) {
  return clean(subject).replace(/^re:\s*/i, '').trim().toLowerCase();
}

function parseReferences(value) {
  const text = Array.isArray(value) ? value.join(' ') : clean(value);
  return text.match(/<[^>]+>/g) || [];
}

function looksLikeStopSignal(message = {}) {
  const body = lower(message.body || message.text || '');
  const subject = lower(message.subject || '');
  return /\b(unsubscribe|do not contact|don't contact|stop emailing|remove me|opt out|opt-out)\b/.test(`${subject} ${body}`);
}

function normalizeIntegration(row = null) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: tenantKey(row.tenantId ?? row.tenant_id),
    providerType: row.providerType || row.provider_type || PROVIDER_TYPES.GENERIC_SMTP_IMAP,
    mailboxAddress: row.mailboxAddress || row.mailbox_address,
    displayName: row.displayName || row.display_name || null,
    smtpHost: row.smtpHost || row.smtp_host || null,
    smtpPort: row.smtpPort ?? row.smtp_port ?? null,
    smtpTlsMode: row.smtpTlsMode || row.smtp_tls_mode || null,
    imapHost: row.imapHost || row.imap_host || null,
    imapPort: row.imapPort ?? row.imap_port ?? null,
    imapTlsMode: row.imapTlsMode || row.imap_tls_mode || null,
    smtpSecretRef: row.smtpSecretRef || row.smtp_secret_ref || null,
    imapSecretRef: row.imapSecretRef || row.imap_secret_ref || null,
    sharedSecretRef: row.sharedSecretRef || row.shared_secret_ref || null,
    status: row.status || MAILBOX_STATUS.UNVERIFIED,
    verificationState: asJson(row.verificationState || row.verification_state, {}),
    createdAt: row.createdAt || row.created_at || null,
    updatedAt: row.updatedAt || row.updated_at || null,
    disabledAt: row.disabledAt || row.disabled_at || null,
    revokedAt: row.revokedAt || row.revoked_at || null,
  };
}

function normalizeIdentity(row = null) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: tenantKey(row.tenantId ?? row.tenant_id),
    mailboxIntegrationId: row.mailboxIntegrationId || row.mailbox_integration_id,
    senderEmail: row.senderEmail || row.sender_email,
    senderDisplayName: row.senderDisplayName || row.sender_display_name || null,
    replyToAddress: row.replyToAddress || row.reply_to_address || null,
    status: row.status || IDENTITY_STATUS.UNVERIFIED,
    verificationState: asJson(row.verificationState || row.verification_state, {}),
    createdAt: row.createdAt || row.created_at || null,
    updatedAt: row.updatedAt || row.updated_at || null,
    disabledAt: row.disabledAt || row.disabled_at || null,
    revokedAt: row.revokedAt || row.revoked_at || null,
  };
}

function normalizeThread(row = null) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: tenantKey(row.tenantId ?? row.tenant_id),
    missionId: row.missionId || row.mission_id || null,
    prospectId: row.prospectId || row.prospect_id || null,
    contactRef: row.contactRef || row.contact_ref || null,
    participants: asJson(row.participants, []),
    currentStatus: row.currentStatus || row.current_status || THREAD_STATUS.OPEN,
    latestInboundMessageId: row.latestInboundMessageId || row.latest_inbound_message_id || null,
    latestOutboundMessageId: row.latestOutboundMessageId || row.latest_outbound_message_id || null,
    lastActivityAt: row.lastActivityAt || row.last_activity_at || null,
    replyState: row.replyState || row.reply_state || 'none',
    sequenceState: row.sequenceState || row.sequence_state || SEQUENCE_STATE.NOT_YET_SENT,
    createdAt: row.createdAt || row.created_at || null,
    updatedAt: row.updatedAt || row.updated_at || null,
  };
}

function normalizeMessage(row = null) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: tenantKey(row.tenantId ?? row.tenant_id),
    missionId: row.missionId || row.mission_id || null,
    prospectId: row.prospectId || row.prospect_id || null,
    contactRef: row.contactRef || row.contact_ref || null,
    sendingIdentityId: row.sendingIdentityId || row.sending_identity_id || null,
    threadId: row.threadId || row.thread_id || null,
    direction: row.direction,
    subject: row.subject || '',
    body: row.body || null,
    contentRef: row.contentRef || row.content_ref || null,
    sender: asJson(row.sender, {}),
    recipients: asJson(row.recipients, []),
    sentAt: row.sentAt || row.sent_at || null,
    receivedAt: row.receivedAt || row.received_at || null,
    providerMessageId: row.providerMessageId || row.provider_message_id || null,
    rfcMessageId: row.rfcMessageId || row.rfc_message_id || null,
    inReplyTo: row.inReplyTo || row.in_reply_to || null,
    referencesHeader: row.referencesHeader || row.references_header || null,
    status: row.status,
    outreachAssetId: row.outreachAssetId || row.outreach_asset_id || null,
    sequenceStepRef: row.sequenceStepRef || row.sequence_step_ref || null,
    metadata: asJson(row.metadata, {}),
    failureCode: row.failureCode || row.failure_code || null,
    failureMessage: row.failureMessage || row.failure_message || null,
    createdAt: row.createdAt || row.created_at || null,
    updatedAt: row.updatedAt || row.updated_at || null,
  };
}

class MemoryTenantMailboxStore {
  constructor(seed = {}) {
    this.integrations = new Map((seed.integrations || []).map((row) => [row.id, normalizeIntegration(row)]));
    this.identities = new Map((seed.identities || []).map((row) => [row.id, normalizeIdentity(row)]));
    this.threads = new Map((seed.threads || []).map((row) => [row.id, normalizeThread(row)]));
    this.messages = new Map((seed.messages || []).map((row) => [row.id, normalizeMessage(row)]));
    this.events = new Map((seed.events || []).map((row) => [row.id, row]));
    this.suppressions = new Map();
    for (const row of seed.suppressions || []) this.suppressions.set(`${tenantKey(row.tenantId)}:${lower(row.email)}`, row);
    this.pollState = new Map();
  }

  async ensureSchema() {}

  async saveIntegration(input) {
    const row = normalizeIntegration({
      ...input,
      id: input.id || prefixedId('tmi', `${input.tenantId}:${input.mailboxAddress}`),
      createdAt: input.createdAt || nowIso(),
      updatedAt: nowIso(),
    });
    this.integrations.set(row.id, row);
    return row;
  }

  async getIntegration(tenantId, integrationId) {
    const row = this.integrations.get(integrationId);
    return row && row.tenantId === tenantKey(tenantId) ? row : null;
  }

  async listIntegrations(tenantId) {
    return [...this.integrations.values()].filter((row) => row.tenantId === tenantKey(tenantId));
  }

  async saveIdentity(input) {
    const row = normalizeIdentity({
      ...input,
      id: input.id || prefixedId('tsi', `${input.tenantId}:${input.senderEmail}:${input.mailboxIntegrationId}`),
      createdAt: input.createdAt || nowIso(),
      updatedAt: nowIso(),
    });
    this.identities.set(row.id, row);
    return row;
  }

  async getIdentity(tenantId, identityId) {
    const row = this.identities.get(identityId);
    return row && row.tenantId === tenantKey(tenantId) ? row : null;
  }

  async findSentByIdempotencyKey(tenantId, key) {
    return [...this.messages.values()].find((msg) => (
      msg.tenantId === tenantKey(tenantId)
      && msg.direction === MESSAGE_DIRECTION.OUTBOUND
      && msg.status === MESSAGE_STATUS.SENT
      && msg.metadata?.idempotencyKey === key
    )) || null;
  }

  async saveThread(input) {
    const existing = input.id ? this.threads.get(input.id) : null;
    const row = normalizeThread({
      ...existing,
      ...input,
      id: input.id || prefixedId('tot', `${input.tenantId}:${input.missionId}:${input.prospectId}:${Date.now()}`),
      createdAt: existing?.createdAt || input.createdAt || nowIso(),
      updatedAt: nowIso(),
    });
    this.threads.set(row.id, row);
    return row;
  }

  async getThread(tenantId, threadId) {
    const row = this.threads.get(threadId);
    return row && row.tenantId === tenantKey(tenantId) ? row : null;
  }

  async findThreadForOutbound(input = {}) {
    return [...this.threads.values()].find((row) => (
      row.tenantId === tenantKey(input.tenantId)
      && (!input.missionId || row.missionId === input.missionId)
      && (!input.prospectId || row.prospectId === input.prospectId)
      && (!input.contactRef || row.contactRef === input.contactRef)
    )) || null;
  }

  async findThreadByReply(input = {}) {
    const tenantId = tenantKey(input.tenantId);
    const referenceIds = new Set([
      input.inReplyTo,
      ...parseReferences(input.referencesHeader),
    ].filter(Boolean));
    if (referenceIds.size) {
      for (const msg of this.messages.values()) {
        if (
          msg.tenantId === tenantId
          && msg.direction === MESSAGE_DIRECTION.OUTBOUND
          && (referenceIds.has(msg.rfcMessageId) || referenceIds.has(msg.providerMessageId))
        ) {
          return this.threads.get(msg.threadId) || null;
        }
      }
    }
    const subject = normalizedSubject(input.subject);
    const from = lower(input.from);
    const to = normalizeRecipients(input.to).map(lower);
    return [...this.threads.values()].find((thread) => (
      thread.tenantId === tenantId
      && subject
      && [...this.messages.values()].some((msg) => (
        msg.threadId === thread.id
        && normalizedSubject(msg.subject) === subject
        && (lower(msg.sender?.email) === from || to.includes(lower(msg.sender?.email)))
      ))
    )) || null;
  }

  async saveMessage(input) {
    const existing = input.id ? this.messages.get(input.id) : null;
    const row = normalizeMessage({
      ...existing,
      ...input,
      id: input.id || prefixedId('tom', `${input.tenantId}:${input.threadId}:${input.rfcMessageId || input.providerMessageId || Date.now()}`),
      createdAt: existing?.createdAt || input.createdAt || nowIso(),
      updatedAt: nowIso(),
    });
    this.messages.set(row.id, row);
    return row;
  }

  async getMessage(tenantId, messageId) {
    const row = this.messages.get(messageId);
    return row && row.tenantId === tenantKey(tenantId) ? row : null;
  }

  async findMessageByProviderOrRfc(tenantId, { providerMessageId, rfcMessageId }) {
    return [...this.messages.values()].find((msg) => (
      msg.tenantId === tenantKey(tenantId)
      && ((providerMessageId && msg.providerMessageId === providerMessageId)
        || (rfcMessageId && msg.rfcMessageId === rfcMessageId))
    )) || null;
  }

  async saveEvent(input) {
    const row = {
      ...input,
      id: input.id || prefixedId('toe', `${input.tenantId}:${input.eventType}:${input.messageId}`),
      createdAt: input.createdAt || nowIso(),
    };
    if (!this.events.has(row.id)) this.events.set(row.id, row);
    return this.events.get(row.id);
  }

  async suppress(input) {
    const row = {
      ...input,
      tenantId: tenantKey(input.tenantId),
      email: lower(input.email),
      id: input.id || prefixedId('tos', `${input.tenantId}:${lower(input.email)}:${input.reason}`),
      createdAt: input.createdAt || nowIso(),
      revokedAt: input.revokedAt || null,
    };
    this.suppressions.set(`${row.tenantId}:${row.email}`, row);
    return row;
  }

  async findSuppression(tenantId, email) {
    return this.suppressions.get(`${tenantKey(tenantId)}:${lower(email)}`) || null;
  }

  async getPollState(tenantId, integrationId) {
    return this.pollState.get(`${tenantKey(tenantId)}:${integrationId}`) || { tenantId: tenantKey(tenantId), integrationId, lastUid: 0 };
  }

  async savePollState(input) {
    const row = { ...input, tenantId: tenantKey(input.tenantId), updatedAt: nowIso() };
    this.pollState.set(`${row.tenantId}:${row.integrationId}`, row);
    return row;
  }
}

class PostgresTenantMailboxStore {
  constructor(pool = defaultPool) {
    this.pool = pool;
  }

  async ensureSchema() {
    await ensureTenantMailboxSchema(this.pool);
  }

  async saveIntegration(input) {
    await this.ensureSchema();
    const row = normalizeIntegration({ ...input, id: input.id || prefixedId('tmi', `${input.tenantId}:${input.mailboxAddress}`) });
    const res = await this.pool.query(
      `INSERT INTO tenant_mailbox_integrations (
         id, tenant_id, provider_type, mailbox_address, display_name, smtp_host, smtp_port,
         smtp_tls_mode, imap_host, imap_port, imap_tls_mode, smtp_secret_ref, imap_secret_ref,
         shared_secret_ref, status, verification_state, disabled_at, revoked_at, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
       ON CONFLICT (id) DO UPDATE SET
         provider_type = EXCLUDED.provider_type,
         mailbox_address = EXCLUDED.mailbox_address,
         display_name = EXCLUDED.display_name,
         smtp_host = EXCLUDED.smtp_host,
         smtp_port = EXCLUDED.smtp_port,
         smtp_tls_mode = EXCLUDED.smtp_tls_mode,
         imap_host = EXCLUDED.imap_host,
         imap_port = EXCLUDED.imap_port,
         imap_tls_mode = EXCLUDED.imap_tls_mode,
         smtp_secret_ref = EXCLUDED.smtp_secret_ref,
         imap_secret_ref = EXCLUDED.imap_secret_ref,
         shared_secret_ref = EXCLUDED.shared_secret_ref,
         status = EXCLUDED.status,
         verification_state = EXCLUDED.verification_state,
         disabled_at = EXCLUDED.disabled_at,
         revoked_at = EXCLUDED.revoked_at,
         updated_at = NOW()
       RETURNING *`,
      [
        row.id, row.tenantId, row.providerType, row.mailboxAddress, row.displayName,
        row.smtpHost, row.smtpPort, row.smtpTlsMode, row.imapHost, row.imapPort,
        row.imapTlsMode, row.smtpSecretRef, row.imapSecretRef, row.sharedSecretRef,
        row.status, JSON.stringify(row.verificationState || {}), row.disabledAt, row.revokedAt,
      ]
    );
    return normalizeIntegration(res.rows[0]);
  }

  async getIntegration(tenantId, integrationId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_mailbox_integrations WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
      [tenantKey(tenantId), integrationId]
    );
    return normalizeIntegration(res.rows[0]);
  }

  async listIntegrations(tenantId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_mailbox_integrations WHERE tenant_id = $1 ORDER BY created_at`,
      [tenantKey(tenantId)]
    );
    return res.rows.map(normalizeIntegration);
  }

  async saveIdentity(input) {
    await this.ensureSchema();
    const row = normalizeIdentity({ ...input, id: input.id || prefixedId('tsi', `${input.tenantId}:${input.senderEmail}:${input.mailboxIntegrationId}`) });
    const res = await this.pool.query(
      `INSERT INTO tenant_sending_identities (
         id, tenant_id, mailbox_integration_id, sender_email, sender_display_name,
         reply_to_address, status, verification_state, disabled_at, revoked_at, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
       ON CONFLICT (id) DO UPDATE SET
         mailbox_integration_id = EXCLUDED.mailbox_integration_id,
         sender_email = EXCLUDED.sender_email,
         sender_display_name = EXCLUDED.sender_display_name,
         reply_to_address = EXCLUDED.reply_to_address,
         status = EXCLUDED.status,
         verification_state = EXCLUDED.verification_state,
         disabled_at = EXCLUDED.disabled_at,
         revoked_at = EXCLUDED.revoked_at,
         updated_at = NOW()
       RETURNING *`,
      [
        row.id, row.tenantId, row.mailboxIntegrationId, row.senderEmail, row.senderDisplayName,
        row.replyToAddress, row.status, JSON.stringify(row.verificationState || {}), row.disabledAt, row.revokedAt,
      ]
    );
    return normalizeIdentity(res.rows[0]);
  }

  async getIdentity(tenantId, identityId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_sending_identities WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
      [tenantKey(tenantId), identityId]
    );
    return normalizeIdentity(res.rows[0]);
  }

  async findSentByIdempotencyKey(tenantId, key) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_messages
       WHERE tenant_id = $1 AND direction = 'OUTBOUND' AND status = 'sent'
         AND metadata->>'idempotencyKey' = $2
       ORDER BY sent_at DESC LIMIT 1`,
      [tenantKey(tenantId), key]
    );
    return normalizeMessage(res.rows[0]);
  }

  async saveThread(input) {
    await this.ensureSchema();
    const row = normalizeThread({ ...input, id: input.id || prefixedId('tot', `${input.tenantId}:${input.missionId}:${input.prospectId}:${Date.now()}`) });
    const res = await this.pool.query(
      `INSERT INTO tenant_outreach_threads (
         id, tenant_id, mission_id, prospect_id, contact_ref, participants, current_status,
         latest_inbound_message_id, latest_outbound_message_id, last_activity_at,
         reply_state, sequence_state, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
       ON CONFLICT (id) DO UPDATE SET
         participants = EXCLUDED.participants,
         current_status = EXCLUDED.current_status,
         latest_inbound_message_id = EXCLUDED.latest_inbound_message_id,
         latest_outbound_message_id = EXCLUDED.latest_outbound_message_id,
         last_activity_at = EXCLUDED.last_activity_at,
         reply_state = EXCLUDED.reply_state,
         sequence_state = EXCLUDED.sequence_state,
         updated_at = NOW()
       RETURNING *`,
      [
        row.id, row.tenantId, row.missionId, row.prospectId, row.contactRef,
        JSON.stringify(row.participants || []), row.currentStatus, row.latestInboundMessageId,
        row.latestOutboundMessageId, row.lastActivityAt, row.replyState, row.sequenceState,
      ]
    );
    return normalizeThread(res.rows[0]);
  }

  async getThread(tenantId, threadId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_threads WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
      [tenantKey(tenantId), threadId]
    );
    return normalizeThread(res.rows[0]);
  }

  async findThreadForOutbound(input = {}) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_threads
       WHERE tenant_id = $1
         AND ($2::text IS NULL OR mission_id = $2)
         AND ($3::text IS NULL OR prospect_id = $3)
         AND ($4::text IS NULL OR contact_ref = $4)
       ORDER BY updated_at DESC LIMIT 1`,
      [tenantKey(input.tenantId), input.missionId || null, input.prospectId || null, input.contactRef || null]
    );
    return normalizeThread(res.rows[0]);
  }

  async findThreadByReply(input = {}) {
    await this.ensureSchema();
    const refs = [input.inReplyTo, ...parseReferences(input.referencesHeader)].filter(Boolean);
    if (refs.length) {
      const res = await this.pool.query(
        `SELECT t.* FROM tenant_outreach_messages m
         JOIN tenant_outreach_threads t ON t.id = m.thread_id AND t.tenant_id = m.tenant_id
         WHERE m.tenant_id = $1 AND m.direction = 'OUTBOUND'
           AND (m.rfc_message_id = ANY($2::text[]) OR m.provider_message_id = ANY($2::text[]))
         ORDER BY m.sent_at DESC NULLS LAST LIMIT 1`,
        [tenantKey(input.tenantId), refs]
      );
      const match = normalizeThread(res.rows[0]);
      if (match) return match;
    }
    const res = await this.pool.query(
      `SELECT t.* FROM tenant_outreach_threads t
       JOIN tenant_outreach_messages m ON m.thread_id = t.id AND m.tenant_id = t.tenant_id
       WHERE t.tenant_id = $1
         AND lower(regexp_replace(m.subject, '^re:\\s*', '', 'i')) = $2
       ORDER BY t.updated_at DESC LIMIT 1`,
      [tenantKey(input.tenantId), normalizedSubject(input.subject)]
    );
    return normalizeThread(res.rows[0]);
  }

  async saveMessage(input) {
    await this.ensureSchema();
    const row = normalizeMessage({ ...input, id: input.id || prefixedId('tom', `${input.tenantId}:${input.threadId}:${input.rfcMessageId || input.providerMessageId || Date.now()}`) });
    const res = await this.pool.query(
      `INSERT INTO tenant_outreach_messages (
         id, tenant_id, mission_id, prospect_id, contact_ref, sending_identity_id,
         thread_id, direction, subject, body, content_ref, sender, recipients,
         sent_at, received_at, provider_message_id, rfc_message_id, in_reply_to,
         references_header, status, outreach_asset_id, sequence_step_ref,
         metadata, failure_code, failure_message, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,NOW())
       ON CONFLICT (id) DO UPDATE SET
         status = EXCLUDED.status,
         sent_at = COALESCE(EXCLUDED.sent_at, tenant_outreach_messages.sent_at),
         received_at = COALESCE(EXCLUDED.received_at, tenant_outreach_messages.received_at),
         provider_message_id = COALESCE(EXCLUDED.provider_message_id, tenant_outreach_messages.provider_message_id),
         rfc_message_id = COALESCE(EXCLUDED.rfc_message_id, tenant_outreach_messages.rfc_message_id),
         metadata = EXCLUDED.metadata,
         failure_code = EXCLUDED.failure_code,
         failure_message = EXCLUDED.failure_message,
         updated_at = NOW()
       RETURNING *`,
      [
        row.id, row.tenantId, row.missionId, row.prospectId, row.contactRef,
        row.sendingIdentityId, row.threadId, row.direction, row.subject, row.body,
        row.contentRef, JSON.stringify(row.sender || {}), JSON.stringify(row.recipients || []),
        row.sentAt, row.receivedAt, row.providerMessageId, row.rfcMessageId,
        row.inReplyTo, row.referencesHeader, row.status, row.outreachAssetId,
        row.sequenceStepRef, JSON.stringify(row.metadata || {}), row.failureCode, row.failureMessage,
      ]
    );
    return normalizeMessage(res.rows[0]);
  }

  async getMessage(tenantId, messageId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_messages WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
      [tenantKey(tenantId), messageId]
    );
    return normalizeMessage(res.rows[0]);
  }

  async findMessageByProviderOrRfc(tenantId, { providerMessageId, rfcMessageId }) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_messages
       WHERE tenant_id = $1
         AND (($2::text IS NOT NULL AND provider_message_id = $2)
           OR ($3::text IS NOT NULL AND rfc_message_id = $3))
       LIMIT 1`,
      [tenantKey(tenantId), providerMessageId || null, rfcMessageId || null]
    );
    return normalizeMessage(res.rows[0]);
  }

  async saveEvent(input) {
    await this.ensureSchema();
    const row = {
      ...input,
      id: input.id || prefixedId('toe', `${input.tenantId}:${input.eventType}:${input.messageId}`),
    };
    const res = await this.pool.query(
      `INSERT INTO tenant_outreach_events (
         id, tenant_id, mission_id, prospect_id, thread_id, message_id, event_type, payload, created_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (id) DO NOTHING
       RETURNING *`,
      [
        row.id, tenantKey(row.tenantId), row.missionId || null, row.prospectId || null,
        row.threadId || null, row.messageId || null, row.eventType,
        JSON.stringify(row.payload || {}), row.createdAt || nowIso(),
      ]
    );
    return res.rows[0] || row;
  }

  async suppress(input) {
    await this.ensureSchema();
    const row = {
      ...input,
      id: input.id || prefixedId('tos', `${input.tenantId}:${lower(input.email)}:${input.reason}`),
      tenantId: tenantKey(input.tenantId),
      email: lower(input.email),
    };
    const res = await this.pool.query(
      `INSERT INTO tenant_outreach_suppressions (
         id, tenant_id, contact_ref, email, reason, source, status, metadata, created_at, revoked_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT (tenant_id, email) WHERE revoked_at IS NULL DO UPDATE SET
         reason = EXCLUDED.reason,
         source = EXCLUDED.source,
         status = EXCLUDED.status,
         metadata = EXCLUDED.metadata
       RETURNING *`,
      [
        row.id, row.tenantId, row.contactRef || null, row.email, row.reason || 'stop',
        row.source || 'operator', row.status || 'active', JSON.stringify(row.metadata || {}),
        row.createdAt || nowIso(), row.revokedAt || null,
      ]
    );
    return res.rows[0];
  }

  async findSuppression(tenantId, email) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_suppressions
       WHERE tenant_id = $1 AND email = $2 AND revoked_at IS NULL
       LIMIT 1`,
      [tenantKey(tenantId), lower(email)]
    );
    return res.rows[0] || null;
  }

  async getPollState(tenantId, integrationId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_mailbox_poll_state WHERE tenant_id = $1 AND integration_id = $2 LIMIT 1`,
      [tenantKey(tenantId), integrationId]
    );
    return res.rows[0] || { tenantId: tenantKey(tenantId), integrationId, lastUid: 0 };
  }

  async savePollState(input) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `INSERT INTO tenant_mailbox_poll_state (
         integration_id, tenant_id, last_uid_validity, last_uid, last_seen_at, updated_at
       ) VALUES ($1,$2,$3,$4,$5,NOW())
       ON CONFLICT (integration_id) DO UPDATE SET
         last_uid_validity = EXCLUDED.last_uid_validity,
         last_uid = GREATEST(tenant_mailbox_poll_state.last_uid, EXCLUDED.last_uid),
         last_seen_at = EXCLUDED.last_seen_at,
         updated_at = NOW()
       RETURNING *`,
      [
        input.integrationId, tenantKey(input.tenantId), input.lastUidValidity || null,
        input.lastUid || 0, input.lastSeenAt || nowIso(),
      ]
    );
    return res.rows[0];
  }
}

async function ensureTenantMailboxSchema(pool = defaultPool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_mailbox_integrations (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      provider_type TEXT NOT NULL,
      mailbox_address TEXT NOT NULL,
      display_name TEXT,
      smtp_host TEXT,
      smtp_port INTEGER,
      smtp_tls_mode TEXT,
      imap_host TEXT,
      imap_port INTEGER,
      imap_tls_mode TEXT,
      smtp_secret_ref TEXT,
      imap_secret_ref TEXT,
      shared_secret_ref TEXT,
      status TEXT NOT NULL DEFAULT 'unverified',
      verification_state JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      disabled_at TIMESTAMPTZ,
      revoked_at TIMESTAMPTZ
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_mailbox_integrations_mailbox_idx
      ON tenant_mailbox_integrations (tenant_id, lower(mailbox_address))
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_sending_identities (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mailbox_integration_id TEXT NOT NULL REFERENCES tenant_mailbox_integrations(id),
      sender_email TEXT NOT NULL,
      sender_display_name TEXT,
      reply_to_address TEXT,
      status TEXT NOT NULL DEFAULT 'unverified',
      verification_state JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      disabled_at TIMESTAMPTZ,
      revoked_at TIMESTAMPTZ
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_sending_identities_sender_idx
      ON tenant_sending_identities (tenant_id, lower(sender_email), mailbox_integration_id)
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_outreach_threads (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      prospect_id TEXT,
      contact_ref TEXT,
      participants JSONB NOT NULL DEFAULT '[]'::jsonb,
      current_status TEXT NOT NULL DEFAULT 'open',
      latest_inbound_message_id TEXT,
      latest_outbound_message_id TEXT,
      last_activity_at TIMESTAMPTZ,
      reply_state TEXT NOT NULL DEFAULT 'none',
      sequence_state TEXT NOT NULL DEFAULT 'not_yet_sent',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS tenant_outreach_threads_binding_idx
      ON tenant_outreach_threads (tenant_id, mission_id, prospect_id, updated_at DESC)
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_outreach_messages (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      prospect_id TEXT,
      contact_ref TEXT,
      sending_identity_id TEXT,
      thread_id TEXT NOT NULL REFERENCES tenant_outreach_threads(id),
      direction TEXT NOT NULL,
      subject TEXT,
      body TEXT,
      content_ref TEXT,
      sender JSONB NOT NULL DEFAULT '{}'::jsonb,
      recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
      sent_at TIMESTAMPTZ,
      received_at TIMESTAMPTZ,
      provider_message_id TEXT,
      rfc_message_id TEXT,
      in_reply_to TEXT,
      references_header TEXT,
      status TEXT NOT NULL,
      outreach_asset_id TEXT,
      sequence_step_ref TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      failure_code TEXT,
      failure_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_messages_provider_idx
      ON tenant_outreach_messages (tenant_id, provider_message_id)
      WHERE provider_message_id IS NOT NULL
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_messages_rfc_idx
      ON tenant_outreach_messages (tenant_id, rfc_message_id)
      WHERE rfc_message_id IS NOT NULL
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS tenant_outreach_messages_thread_idx
      ON tenant_outreach_messages (tenant_id, thread_id, created_at DESC)
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_outreach_events (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      prospect_id TEXT,
      thread_id TEXT,
      message_id TEXT,
      event_type TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS tenant_outreach_events_thread_idx
      ON tenant_outreach_events (tenant_id, thread_id, created_at DESC)
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_outreach_suppressions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      contact_ref TEXT,
      email TEXT NOT NULL,
      reason TEXT NOT NULL,
      source TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      revoked_at TIMESTAMPTZ
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_suppressions_active_email_idx
      ON tenant_outreach_suppressions (tenant_id, email)
      WHERE revoked_at IS NULL
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_mailbox_poll_state (
      integration_id TEXT PRIMARY KEY REFERENCES tenant_mailbox_integrations(id),
      tenant_id TEXT NOT NULL,
      last_uid_validity TEXT,
      last_uid BIGINT NOT NULL DEFAULT 0,
      last_seen_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function createSmtpTransport(integration, secret, opts = {}) {
  if (opts.transport) return opts.transport;
  const nodemailer = opts.nodemailer || require('nodemailer');
  const secure = clean(integration.smtpTlsMode).toUpperCase() === 'SSL_TLS' || Number(integration.smtpPort) === 465;
  return nodemailer.createTransport({
    host: integration.smtpHost,
    port: Number(integration.smtpPort || 587),
    secure,
    auth: {
      user: integration.mailboxAddress,
      pass: secret,
    },
  });
}

async function loadImapMessages(integration, secret, state, opts = {}) {
  if (opts.imapAdapter && typeof opts.imapAdapter.fetchNewMessages === 'function') {
    return opts.imapAdapter.fetchNewMessages({ integration, secret, state });
  }
  let ImapFlow;
  try {
    ({ ImapFlow } = require('imapflow'));
  } catch (_err) {
    throw mailboxError(
      'imap_adapter_unavailable',
      'IMAP polling requires an injected imapAdapter or the optional imapflow runtime dependency.'
    );
  }
  const client = new ImapFlow({
    host: integration.imapHost,
    port: Number(integration.imapPort || 993),
    secure: clean(integration.imapTlsMode).toUpperCase() === 'SSL_TLS' || Number(integration.imapPort) === 993,
    auth: {
      user: integration.mailboxAddress,
      pass: secret,
    },
  });
  const messages = [];
  await client.connect();
  try {
    const lock = await client.getMailboxLock('INBOX');
    try {
      const sinceUid = Number(state?.lastUid || 0) + 1;
      for await (const msg of client.fetch(`${sinceUid}:*`, {
        uid: true,
        envelope: true,
        source: true,
        headers: true,
      }, { uid: true })) {
        const headers = msg.headers;
        const getHeader = (name) => {
          const value = headers?.get(name.toLowerCase()) || headers?.get(name);
          return Array.isArray(value) ? value.join(' ') : value || null;
        };
        messages.push({
          uid: msg.uid,
          providerMessageId: String(msg.uid),
          rfcMessageId: getHeader('message-id'),
          inReplyTo: getHeader('in-reply-to'),
          referencesHeader: getHeader('references'),
          subject: msg.envelope?.subject || getHeader('subject') || '',
          from: msg.envelope?.from?.[0]?.address || '',
          to: (msg.envelope?.to || []).map((row) => row.address),
          receivedAt: msg.envelope?.date ? new Date(msg.envelope.date).toISOString() : nowIso(opts),
          body: msg.source ? msg.source.toString('utf8') : '',
        });
      }
    } finally {
      lock.release();
    }
  } finally {
    await client.logout().catch(() => {});
  }
  return messages;
}

async function assertSendableMailbox(store, tenantId, sendingIdentityId) {
  const identity = await store.getIdentity(tenantId, sendingIdentityId);
  if (!identity) throw mailboxError('mailbox_identity_tenant_mismatch', 'Sending identity does not belong to this tenant.');
  if (identity.status !== IDENTITY_STATUS.ACTIVE) {
    throw mailboxError('mailbox_identity_inactive', 'Sending identity is not active.');
  }
  const integration = await store.getIntegration(tenantId, identity.mailboxIntegrationId);
  if (!integration) throw mailboxError('mailbox_integration_tenant_mismatch', 'Mailbox integration does not belong to this tenant.');
  if (integration.status !== MAILBOX_STATUS.ACTIVE) {
    throw mailboxError('mailbox_integration_disabled', 'Mailbox integration is not active.');
  }
  return { identity, integration };
}

async function assertNotSuppressed(store, tenantId, recipients) {
  for (const email of recipients) {
    const suppression = await store.findSuppression(tenantId, email);
    if (suppression) {
      throw mailboxError('tenant_outreach_suppressed', 'Recipient is suppressed for this tenant.', {
        email: lower(email),
        reason: suppression.reason,
      });
    }
  }
}

async function sendTenantEmail(input = {}, opts = {}) {
  const store = opts.store || new PostgresTenantMailboxStore(opts.pool || defaultPool);
  const tenantId = tenantKey(input.tenantId);
  if (!tenantId) throw mailboxError('tenant_required', 'tenantId is required.');
  if (!input.sendingIdentityId) throw mailboxError('sending_identity_required', 'sendingIdentityId is required.');
  const recipients = normalizeRecipients(input.to);
  if (!recipients.length) throw mailboxError('recipient_required', 'At least one recipient is required.');

  const key = outboundIdempotencyKey(input);
  const alreadySent = await store.findSentByIdempotencyKey(tenantId, key);
  if (alreadySent) return { message: alreadySent, duplicate: true, sent: false };

  const { identity, integration } = await assertSendableMailbox(store, tenantId, input.sendingIdentityId);
  await assertNotSuppressed(store, tenantId, recipients);

  let thread = input.threadId ? await store.getThread(tenantId, input.threadId) : null;
  if (input.threadId && !thread) throw mailboxError('thread_tenant_mismatch', 'Thread does not belong to this tenant.');
  if (!thread) {
    thread = await store.findThreadForOutbound({
      tenantId,
      missionId: input.missionId || null,
      prospectId: input.prospectId || null,
      contactRef: input.contactRef || null,
    });
  }
  if (!thread) {
    thread = await store.saveThread({
      tenantId,
      missionId: input.missionId || null,
      prospectId: input.prospectId || null,
      contactRef: input.contactRef || null,
      participants: [
        { role: 'sender', email: identity.senderEmail, displayName: identity.senderDisplayName },
        ...recipients.map((email) => ({ role: 'recipient', email })),
      ],
      currentStatus: THREAD_STATUS.OPEN,
      replyState: 'none',
      sequenceState: SEQUENCE_STATE.NOT_YET_SENT,
      lastActivityAt: nowIso(opts),
    });
  }

  const messageId = prefixedId('tom', `${tenantId}:${key}`);
  const rfcMessageId = input.rfcMessageId || generateRfcMessageId({ tenantId, sendingIdentityId: identity.senderEmail, messageId });
  let message = await store.saveMessage({
    id: messageId,
    tenantId,
    missionId: input.missionId || thread.missionId || null,
    prospectId: input.prospectId || thread.prospectId || null,
    contactRef: input.contactRef || thread.contactRef || null,
    sendingIdentityId: identity.id,
    threadId: thread.id,
    direction: MESSAGE_DIRECTION.OUTBOUND,
    subject: clean(input.subject),
    body: input.body || null,
    contentRef: input.contentRef || null,
    sender: {
      email: identity.senderEmail,
      displayName: identity.senderDisplayName || integration.displayName || null,
      replyTo: identity.replyToAddress || identity.senderEmail,
    },
    recipients,
    rfcMessageId,
    inReplyTo: input.inReplyTo || null,
    referencesHeader: input.referencesHeader || null,
    status: MESSAGE_STATUS.PENDING,
    outreachAssetId: input.outreachAssetId || null,
    sequenceStepRef: input.sequenceStepRef || null,
    metadata: { ...(input.metadata || {}), idempotencyKey: key },
  });

  let smtpSecret;
  try {
    smtpSecret = resolveSecretRef(integration.smtpSecretRef || integration.sharedSecretRef, opts);
    const transport = createSmtpTransport(integration, smtpSecret, opts);
    const from = identity.senderDisplayName
      ? `"${identity.senderDisplayName.replace(/"/g, '\\"')}" <${identity.senderEmail}>`
      : identity.senderEmail;
    const result = await transport.sendMail({
      from,
      replyTo: identity.replyToAddress || identity.senderEmail,
      to: recipients,
      subject: input.subject,
      text: input.text || input.body,
      html: input.html || undefined,
      headers: {
        'Message-ID': rfcMessageId,
        ...(input.inReplyTo ? { 'In-Reply-To': input.inReplyTo } : {}),
        ...(input.referencesHeader ? { References: input.referencesHeader } : {}),
      },
    });
    const providerMessageId = result?.messageId || result?.response || rfcMessageId;
    const sentAt = nowIso(opts);
    message = await store.saveMessage({
      ...message,
      status: MESSAGE_STATUS.SENT,
      providerMessageId,
      rfcMessageId,
      sentAt,
      metadata: {
        ...message.metadata,
        providerAccepted: true,
      },
    });
    await store.saveThread({
      ...thread,
      latestOutboundMessageId: message.id,
      lastActivityAt: sentAt,
      currentStatus: THREAD_STATUS.OPEN,
      sequenceState: SEQUENCE_STATE.SENT,
    });
    return { message, thread: await store.getThread(tenantId, thread.id), providerResult: { messageId: providerMessageId }, duplicate: false, sent: true };
  } catch (err) {
    message = await store.saveMessage({
      ...message,
      status: MESSAGE_STATUS.FAILED,
      failureCode: err.code || 'smtp_send_failed',
      failureMessage: sanitizeErrorMessage(err),
      metadata: {
        ...message.metadata,
        providerAccepted: false,
      },
    });
    await store.saveThread({
      ...thread,
      latestOutboundMessageId: message.id,
      lastActivityAt: nowIso(opts),
      currentStatus: THREAD_STATUS.FAILED,
      sequenceState: SEQUENCE_STATE.FAILED,
    });
    throw mailboxError(err.code || 'smtp_send_failed', sanitizeErrorMessage(err), { outboundMessage: message });
  } finally {
    smtpSecret = null;
  }
}

async function ingestInboundMessage(store, tenantId, integration, raw, opts = {}) {
  const rfcMessageId = raw.rfcMessageId || raw.messageId || null;
  const providerMessageId = raw.providerMessageId || (raw.uid != null ? String(raw.uid) : null);
  const duplicate = await store.findMessageByProviderOrRfc(tenantId, { providerMessageId, rfcMessageId });
  if (duplicate) return { message: duplicate, duplicate: true, inserted: false };

  const thread = await store.findThreadByReply({
    tenantId,
    inReplyTo: raw.inReplyTo || null,
    referencesHeader: raw.referencesHeader || raw.references || null,
    subject: raw.subject || '',
    from: raw.from || raw.sender,
    to: raw.to || [],
  });
  if (!thread) return { message: null, duplicate: false, inserted: false, unmatched: true };

  const receivedAt = raw.receivedAt || nowIso(opts);
  const message = await store.saveMessage({
    tenantId,
    missionId: thread.missionId,
    prospectId: thread.prospectId,
    contactRef: thread.contactRef,
    sendingIdentityId: raw.sendingIdentityId || null,
    threadId: thread.id,
    direction: MESSAGE_DIRECTION.INBOUND,
    subject: raw.subject || '',
    body: raw.body || raw.text || null,
    sender: { email: clean(raw.from || raw.sender) },
    recipients: normalizeRecipients(raw.to || integration.mailboxAddress),
    receivedAt,
    providerMessageId,
    rfcMessageId,
    inReplyTo: raw.inReplyTo || null,
    referencesHeader: raw.referencesHeader || raw.references || null,
    status: MESSAGE_STATUS.RECEIVED,
    metadata: { source: 'imap', uid: raw.uid || null },
  });
  const updatedThread = await store.saveThread({
    ...thread,
    latestInboundMessageId: message.id,
    lastActivityAt: receivedAt,
    currentStatus: THREAD_STATUS.REPLIED,
    replyState: 'reply_received',
    sequenceState: SEQUENCE_STATE.PAUSED,
  });
  if (looksLikeStopSignal(raw)) {
    await store.suppress({
      tenantId,
      contactRef: thread.contactRef,
      email: raw.from || raw.sender,
      reason: 'unsubscribe',
      source: 'inbound_reply',
      metadata: { messageId: message.id },
    });
  }
  const event = await store.saveEvent({
    tenantId,
    missionId: thread.missionId,
    prospectId: thread.prospectId,
    threadId: thread.id,
    messageId: message.id,
    eventType: EVENT_TYPES.REPLY_RECEIVED,
    payload: {
      tenantId,
      missionId: thread.missionId,
      prospectId: thread.prospectId,
      threadId: thread.id,
      inboundMessageId: message.id,
      sendingIdentityId: message.sendingIdentityId,
      timestamp: receivedAt,
    },
    createdAt: receivedAt,
  });
  return { message, thread: updatedThread, event, duplicate: false, inserted: true };
}

async function pollTenantMailbox(input = {}, opts = {}) {
  const store = opts.store || new PostgresTenantMailboxStore(opts.pool || defaultPool);
  const tenantId = tenantKey(input.tenantId);
  if (!tenantId) throw mailboxError('tenant_required', 'tenantId is required.');
  const integrationId = input.integrationId || input.mailboxIntegrationId;
  if (!integrationId) throw mailboxError('mailbox_integration_required', 'integrationId is required.');
  const integration = await store.getIntegration(tenantId, integrationId);
  if (!integration) throw mailboxError('mailbox_integration_tenant_mismatch', 'Mailbox integration does not belong to this tenant.');
  if ([MAILBOX_STATUS.DISABLED, MAILBOX_STATUS.REVOKED].includes(integration.status)) {
    throw mailboxError('mailbox_integration_disabled', 'Mailbox integration is disabled or revoked.');
  }

  const state = await store.getPollState(tenantId, integration.id);
  const imapSecret = resolveSecretRef(integration.imapSecretRef || integration.sharedSecretRef, opts);
  const rawMessages = await loadImapMessages(integration, imapSecret, state, opts);
  const results = [];
  let maxUid = Number(state.lastUid || state.last_uid || 0);
  for (const raw of rawMessages || []) {
    const result = await ingestInboundMessage(store, tenantId, integration, raw, opts);
    results.push(result);
    if (raw.uid != null) maxUid = Math.max(maxUid, Number(raw.uid));
  }
  if (maxUid > Number(state.lastUid || state.last_uid || 0)) {
    await store.savePollState({
      tenantId,
      integrationId: integration.id,
      lastUidValidity: input.lastUidValidity || state.lastUidValidity || state.last_uid_validity || null,
      lastUid: maxUid,
      lastSeenAt: nowIso(opts),
    });
  }
  return {
    integration: publicIntegration(integration),
    fetched: rawMessages.length,
    inserted: results.filter((row) => row.inserted).length,
    duplicates: results.filter((row) => row.duplicate).length,
    unmatched: results.filter((row) => row.unmatched).length,
    results,
  };
}

async function markTenantSuppression(input = {}, opts = {}) {
  const store = opts.store || new PostgresTenantMailboxStore(opts.pool || defaultPool);
  if (!input.tenantId || !input.email) throw mailboxError('suppression_input_required', 'tenantId and email are required.');
  return store.suppress({
    tenantId: input.tenantId,
    contactRef: input.contactRef || null,
    email: input.email,
    reason: input.reason || 'do_not_contact',
    source: input.source || 'operator',
    metadata: input.metadata || {},
  });
}

async function hasTxtRecord(name, matcher, opts = {}) {
  try {
    const records = opts.resolveTxt
      ? await opts.resolveTxt(name)
      : await dns.resolveTxt(name);
    const flat = records.flat().join(' ');
    return matcher(flat);
  } catch (_err) {
    return false;
  }
}

async function verifyTenantMailbox(input = {}, opts = {}) {
  const store = opts.store || new PostgresTenantMailboxStore(opts.pool || defaultPool);
  const tenantId = tenantKey(input.tenantId);
  const integrationId = input.integrationId || input.mailboxIntegrationId;
  const integration = await store.getIntegration(tenantId, integrationId);
  if (!integration) throw mailboxError('mailbox_integration_tenant_mismatch', 'Mailbox integration does not belong to this tenant.');
  const domain = lower(integration.mailboxAddress).split('@')[1] || '';
  const state = {
    smtp: { status: 'not_checked' },
    imap: { status: 'not_checked' },
    spf: { status: 'not_checked' },
    dkim: { status: 'not_checked' },
    dmarc: { status: 'not_checked' },
  };

  try {
    const smtpSecret = resolveSecretRef(integration.smtpSecretRef || integration.sharedSecretRef, opts);
    if (opts.smtpVerifier) {
      await opts.smtpVerifier({ integration, secret: smtpSecret });
    } else {
      const transport = createSmtpTransport(integration, smtpSecret, opts);
      if (typeof transport.verify === 'function') await transport.verify();
    }
    state.smtp = { status: 'verified' };
  } catch (err) {
    state.smtp = { status: 'failed', code: err.code || 'smtp_verification_failed', message: sanitizeErrorMessage(err) };
  }

  try {
    const imapSecret = resolveSecretRef(integration.imapSecretRef || integration.sharedSecretRef, opts);
    if (opts.imapVerifier) await opts.imapVerifier({ integration, secret: imapSecret });
    else if (!opts.imapAdapter) throw mailboxError('imap_adapter_unavailable', 'No IMAP verifier is configured.');
    state.imap = { status: 'verified' };
  } catch (err) {
    state.imap = { status: 'failed', code: err.code || 'imap_verification_failed', message: sanitizeErrorMessage(err) };
  }

  if (domain) {
    state.spf = { status: (await hasTxtRecord(domain, (txt) => /v=spf1/i.test(txt), opts)) ? 'present' : 'missing' };
    state.dmarc = { status: (await hasTxtRecord(`_dmarc.${domain}`, (txt) => /v=dmarc1/i.test(txt), opts)) ? 'present' : 'missing' };
    const selector = clean(input.dkimSelector || opts.dkimSelector);
    state.dkim = selector
      ? { status: (await hasTxtRecord(`${selector}._domainkey.${domain}`, (txt) => /v=dkim1|p=/i.test(txt), opts)) ? 'present' : 'missing' }
      : { status: 'not_checked', reason: 'dkim_selector_required' };
  }

  const updated = await store.saveIntegration({
    ...integration,
    verificationState: state,
    status: state.smtp.status === 'verified' && state.imap.status === 'verified'
      ? MAILBOX_STATUS.ACTIVE
      : integration.status,
  });
  return { integration: publicIntegration(updated), verificationState: state };
}

function babrunMailboxConfig(tenantId) {
  return {
    integration: {
      id: tenantId ? `tmi_${tenantKey(tenantId)}_babrun_hello` : null,
      tenantId: tenantId ? tenantKey(tenantId) : null,
      providerType: BABRUN_MAILBOX_CONFIG.providerType,
      mailboxAddress: BABRUN_MAILBOX_CONFIG.mailboxAddress,
      displayName: BABRUN_MAILBOX_CONFIG.displayName,
      smtpHost: BABRUN_MAILBOX_CONFIG.smtpHost,
      smtpPort: BABRUN_MAILBOX_CONFIG.smtpPort,
      smtpTlsMode: BABRUN_MAILBOX_CONFIG.smtpTlsMode,
      imapHost: BABRUN_MAILBOX_CONFIG.imapHost,
      imapPort: BABRUN_MAILBOX_CONFIG.imapPort,
      imapTlsMode: BABRUN_MAILBOX_CONFIG.imapTlsMode,
      smtpSecretRef: BABRUN_MAILBOX_CONFIG.smtpSecretRef,
      imapSecretRef: BABRUN_MAILBOX_CONFIG.imapSecretRef,
      status: MAILBOX_STATUS.UNVERIFIED,
      verificationState: {},
    },
    identity: {
      id: tenantId ? `tsi_${tenantKey(tenantId)}_babrun_fedir` : null,
      tenantId: tenantId ? tenantKey(tenantId) : null,
      mailboxIntegrationId: tenantId ? `tmi_${tenantKey(tenantId)}_babrun_hello` : null,
      senderEmail: BABRUN_MAILBOX_CONFIG.senderEmail,
      senderDisplayName: BABRUN_MAILBOX_CONFIG.senderDisplayName,
      replyToAddress: BABRUN_MAILBOX_CONFIG.replyToAddress,
      status: IDENTITY_STATUS.UNVERIFIED,
      verificationState: {},
    },
  };
}

module.exports = {
  PROVIDER_TYPES,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  MESSAGE_DIRECTION,
  MESSAGE_STATUS,
  THREAD_STATUS,
  SEQUENCE_STATE,
  EVENT_TYPES,
  BABRUN_MAILBOX_CONFIG,
  MemoryTenantMailboxStore,
  PostgresTenantMailboxStore,
  ensureTenantMailboxSchema,
  publicIntegration,
  publicIdentity,
  resolveSecretRef,
  sendTenantEmail,
  pollTenantMailbox,
  markTenantSuppression,
  verifyTenantMailbox,
  babrunMailboxConfig,
  normalizeIntegration,
  normalizeIdentity,
  normalizeThread,
  normalizeMessage,
};
