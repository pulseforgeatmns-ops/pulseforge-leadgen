'use strict';

/**
 * SPEC-AO-MAILBOX-001 — canonical AO communication identity + sender resolution.
 * Credentials remain in tenant_mailbox_integrations (referenced by mailbox_integration_id only).
 */

const { normalizeSendingDomain, emailDomain } = require('./canonicalSenderIdentity');

const AO_ROLE_LABEL = 'Acquisition Operator';
const MAILBOX_STATUSES = Object.freeze([
  'not_configured',
  'configured',
  'verification_pending',
  'ready',
  'auth_failed',
  'disabled',
]);

const SECRET_FIELD_PATTERN = /secret|password|token|oauth|credential|smtp_|imap_/i;

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function formatPhoneDisplay(raw) {
  const digits = String(raw || '').replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) {
    const area = digits.slice(1, 4);
    const mid = digits.slice(4, 7);
    const last = digits.slice(7, 11);
    return `(${area}) ${mid}-${last}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6, 10)}`;
  }
  return clean(raw) || null;
}

function canAoOriginateSend(identity) {
  if (!identity) return false;
  return identity.mailboxStatus === 'ready' && identity.senderEnabled === true;
}

function mapRowToIdentity(row, user = null) {
  if (!row) return null;
  const displayName = clean(user?.name) || null;
  return {
    aoId: row.ao_id,
    tenantId: row.tenant_id,
    displayName,
    emailAddress: clean(row.email_address),
    phoneNumber: row.phone_number || null,
    phoneDisplay: formatPhoneDisplay(row.phone_number),
    replyToAddress: clean(row.reply_to_address),
    senderEnabled: row.sender_enabled === true,
    mailboxStatus: row.mailbox_status,
    provider: row.provider || null,
    mailboxIntegrationId: row.mailbox_integration_id || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toPublicIdentity(identity) {
  if (!identity) return null;
  return {
    aoId: identity.aoId,
    tenantId: identity.tenantId,
    displayName: identity.displayName,
    email: identity.emailAddress,
    phone: identity.phoneDisplay,
    replyTo: identity.replyToAddress,
    mailboxStatus: identity.mailboxStatus,
    mailboxStatusLabel: formatMailboxStatusLabel(identity.mailboxStatus),
    senderEnabled: identity.senderEnabled,
    sendingEnabled: canAoOriginateSend(identity),
    role: AO_ROLE_LABEL,
    provider: identity.provider,
  };
}

function formatMailboxStatusLabel(status) {
  const labels = {
    not_configured: 'Not configured',
    configured: 'Configured',
    verification_pending: 'Verification pending',
    ready: 'Ready',
    auth_failed: 'Auth failed',
    disabled: 'Disabled',
  };
  return labels[status] || 'Not configured';
}

function buildAssignedAoContext(identity, user = null) {
  if (!identity && !user) return null;
  const name = clean(user?.name) || identity?.displayName || null;
  return {
    id: identity?.aoId ?? user?.id ?? null,
    name,
    role: AO_ROLE_LABEL,
    email: identity?.emailAddress || user?.email || null,
    phone: identity?.phoneDisplay || formatPhoneDisplay(user?.phone),
    mailboxStatus: identity?.mailboxStatus || 'not_configured',
    senderEnabled: identity?.senderEnabled === true,
  };
}

function buildAoSignature({
  userName,
  organizationName = 'Anchor Cleaning',
  website = 'goanchorcleaning.com',
  identity,
}) {
  const name = clean(userName) || clean(identity?.displayName) || 'Anchor Cleaning';
  const phone = identity?.phoneDisplay || formatPhoneDisplay(identity?.phoneNumber);
  const email = clean(identity?.emailAddress);
  const lines = [
    name,
    AO_ROLE_LABEL,
    organizationName,
  ];
  if (phone) lines.push(phone);
  if (email) lines.push(email);
  if (website) lines.push(website.replace(/^https?:\/\//, ''));
  return lines.join('\n');
}

function aoSenderDisplayFirstName(userName, fallback = 'Anchor Cleaning') {
  const n = clean(userName);
  if (!n) return fallback;
  return n.split(/\s+/)[0];
}

function identityToCanonicalSenderShape(identity, fallbackIdentity) {
  const email = clean(identity.emailAddress);
  const domain = normalizeSendingDomain(emailDomain(email));
  const senderName = clean(identity.displayName) || clean(fallbackIdentity?.senderName);
  return {
    tenantId: String(identity.tenantId),
    clientId: identity.tenantId,
    senderEmail: email,
    senderName,
    sendingDomain: domain || fallbackIdentity?.sendingDomain,
    replyToAddress: clean(identity.replyToAddress) || email,
    source: 'ao_communication_identity',
    aoId: identity.aoId,
  };
}

/**
 * Resolve outbound sender for a prospect assigned to an AO.
 * Falls back to tenant canonical (Jake/Anchor) unless AO mailbox is ready AND sending is enabled.
 */
function resolveOutboundSenderForAssignment({ identity, fallbackSender }) {
  if (!fallbackSender || typeof fallbackSender !== 'object') {
    return { sender: null, usedAoSender: false, reason: 'missing_fallback' };
  }
  if (!identity) {
    return { sender: fallbackSender, usedAoSender: false, reason: 'no_ao_identity' };
  }
  if (!canAoOriginateSend(identity)) {
    return {
      sender: fallbackSender,
      usedAoSender: false,
      reason: identity.mailboxStatus !== 'ready'
        ? 'ao_mailbox_not_ready'
        : 'ao_sender_disabled',
    };
  }
  return {
    sender: identityToCanonicalSenderShape(identity, fallbackSender),
    usedAoSender: true,
    reason: null,
  };
}

function stripSecretsFromRecord(row) {
  if (!row || typeof row !== 'object') return row;
  const out = {};
  for (const [key, val] of Object.entries(row)) {
    if (SECRET_FIELD_PATTERN.test(key)) continue;
    out[key] = val;
  }
  return out;
}

const IDENTITY_SELECT = `
  SELECT
    i.ao_id,
    i.tenant_id,
    i.email_address,
    i.phone_number,
    i.reply_to_address,
    i.sender_enabled,
    i.mailbox_status,
    i.provider,
    i.mailbox_integration_id,
    i.created_at,
    i.updated_at,
    u.name AS user_name,
    u.email AS user_email,
    u.phone AS user_phone,
    u.role AS user_role
  FROM ao_communication_identities i
  JOIN users u ON u.id = i.ao_id
`;

async function loadAoCommunicationIdentity(db, { aoId, tenantId = null }) {
  if (!db || aoId == null) return null;
  const params = [aoId];
  let tenantClause = '';
  if (tenantId != null) {
    params.push(Number(tenantId));
    tenantClause = ` AND i.tenant_id = $${params.length}`;
  }
  const { rows } = await db.query(
    `${IDENTITY_SELECT} WHERE i.ao_id = $1${tenantClause} LIMIT 1`,
    params
  );
  const row = rows[0];
  if (!row) return null;
  const user = { id: row.ao_id, name: row.user_name, email: row.user_email, phone: row.user_phone, role: row.user_role };
  return mapRowToIdentity(stripSecretsFromRecord(row), user);
}

async function loadIdentityForAssignedAo(db, { assignedAoId, tenantId, fallbackClient = null }) {
  if (!assignedAoId) return { identity: null, assignedAo: null };
  const identity = await loadAoCommunicationIdentity(db, { aoId: assignedAoId, tenantId });
  const { rows } = await db.query(
    'SELECT id, name, email, phone, role FROM users WHERE id = $1 LIMIT 1',
    [assignedAoId]
  );
  const user = rows[0] || null;
  const assignedAo = buildAssignedAoContext(identity, user);
  return { identity, assignedAo, user };
}

async function resolveOutboundSenderForProspect(db, {
  assignedAoId,
  tenantId,
  fallbackSender,
}) {
  const { identity, user } = await loadIdentityForAssignedAo(db, { assignedAoId, tenantId });
  if (identity && !identity.displayName && user?.name) {
    identity.displayName = user.name;
  }
  return resolveOutboundSenderForAssignment({ identity, fallbackSender });
}

async function listAoCommunicationIdentities(db, tenantId) {
  const { rows } = await db.query(
    `${IDENTITY_SELECT} WHERE i.tenant_id = $1 ORDER BY u.name ASC`,
    [Number(tenantId)]
  );
  return rows.map(row => {
    const user = { id: row.ao_id, name: row.user_name, email: row.user_email, phone: row.user_phone };
    return toPublicIdentity(mapRowToIdentity(stripSecretsFromRecord(row), user));
  });
}

/**
 * For Paige: use assigned AO's display first name when prospect has an AO owner (copy/signature prose).
 */
async function resolvePaigeSenderNameForProspect(db, { tenantId, assignedAoId, fallbackName = 'Jacob Maynard' }) {
  if (!assignedAoId || !db) return fallbackName;
  const { identity, user } = await loadIdentityForAssignedAo(db, { assignedAoId, tenantId });
  const fullName = clean(user?.name) || clean(identity?.displayName);
  if (!fullName) return fallbackName;
  return fullName;
}

async function buildAoSenderNameByProspectId(db, crmByProspectId, tenantId, fallbackName = 'Jacob Maynard') {
  const map = new Map();
  if (!db || !crmByProspectId || typeof crmByProspectId !== 'object') return map;
  const aoIds = new Set();
  for (const row of Object.values(crmByProspectId)) {
    if (row?.assigned_ao_id) aoIds.add(Number(row.assigned_ao_id));
  }
  const aoNameById = new Map();
  for (const aoId of aoIds) {
    const { user, identity } = await loadIdentityForAssignedAo(db, { assignedAoId: aoId, tenantId });
    const name = clean(user?.name) || clean(identity?.displayName);
    if (name) aoNameById.set(aoId, name);
  }
  for (const [key, row] of Object.entries(crmByProspectId)) {
    const prospectId = String(row?.prospect_id || row?.id || key);
    const aoId = row?.assigned_ao_id != null ? Number(row.assigned_ao_id) : null;
    if (aoId && aoNameById.has(aoId)) {
      map.set(prospectId, aoNameById.get(aoId));
    } else {
      map.set(prospectId, fallbackName);
    }
  }
  return map;
}

module.exports = {
  AO_ROLE_LABEL,
  MAILBOX_STATUSES,
  formatPhoneDisplay,
  formatMailboxStatusLabel,
  canAoOriginateSend,
  mapRowToIdentity,
  toPublicIdentity,
  buildAssignedAoContext,
  buildAoSignature,
  aoSenderDisplayFirstName,
  identityToCanonicalSenderShape,
  resolveOutboundSenderForAssignment,
  resolveOutboundSenderForProspect,
  loadAoCommunicationIdentity,
  loadIdentityForAssignedAo,
  listAoCommunicationIdentities,
  resolvePaigeSenderNameForProspect,
  buildAoSenderNameByProspectId,
  stripSecretsFromRecord,
};
