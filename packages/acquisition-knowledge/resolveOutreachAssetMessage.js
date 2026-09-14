'use strict';

/**
 * SPEC-247 / SPEC-252 — Resolve executable outreach copy from acquisition knowledge.
 *
 * Outreach assets store copy in content JSONB. Babrun imports and Paige variants
 * use several shapes; this helper is the single canonical reader.
 */

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function knowledgeError(code, message, extras = {}) {
  const err = new Error(message || code);
  err.code = code;
  Object.assign(err, extras);
  return err;
}

const SUBJECT_KEYS = Object.freeze([
  'subject',
  'emailSubject',
  'email_subject',
  'subjectLine',
  'subject_line',
]);

const BODY_KEYS = Object.freeze([
  'body',
  'emailBody',
  'email_body',
  'text',
  'html',
  'message',
  'statement',
  'messageBody',
  'message_body',
]);

function pickText(source, keys) {
  if (!source || typeof source !== 'object') return null;
  for (const key of keys) {
    const value = asText(source[key]);
    if (value) return value;
  }
  return null;
}

function isObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}

function collectCopySources(content = {}) {
  const sources = [content];
  for (const key of ['messaging', 'copy', 'email', 'message', 'payload', 'approved', 'approvedCopy', 'executable']) {
    if (isObject(content[key])) sources.push(content[key]);
  }
  if (isObject(content.content)) sources.push(content.content);
  if (isObject(content.messaging?.email)) sources.push(content.messaging.email);
  if (isObject(content.copy?.email)) sources.push(content.copy.email);
  return sources;
}

function resolveVariantCopy(content = {}) {
  const variantLists = [
    content.variants,
    content.messaging?.variants,
    content.copy?.variants,
  ].filter(Array.isArray);
  for (const variants of variantLists) {
    if (!variants.length) continue;
    const primary = variants.find((row) => asText(row?.label).toLowerCase() === 'primary') || variants[0];
    const subject = pickText(primary, SUBJECT_KEYS);
    const body = pickText(primary, BODY_KEYS);
    if (subject && body) {
      return { subject, body, source: 'variants' };
    }
  }
  return null;
}

function resolveDirectCopy(content = {}) {
  let subject = null;
  let body = null;
  for (const source of collectCopySources(content)) {
    subject = subject || pickText(source, SUBJECT_KEYS);
    body = body || pickText(source, BODY_KEYS);
    if (subject && body) break;
  }
  if (!subject || !body) return null;
  return { subject, body, source: 'content' };
}

function assetRevision(asset = {}) {
  const updatedAt = asset.updatedAt || asset.updated_at;
  if (updatedAt) {
    return new Date(updatedAt).toISOString();
  }
  if (asset.version != null) return String(asset.version);
  return null;
}

function isStakeholderValidated(asset = {}) {
  const validation = asText(asset.validationState || asset.validation_state).toUpperCase();
  const lifecycle = asText(asset.lifecycleState || asset.state || asset.lifecycle_state).toUpperCase();
  const status = asText(asset.status).toLowerCase();
  return validation === 'STAKEHOLDER_VALIDATED'
    || lifecycle === 'STAKEHOLDER_VALIDATED'
    || ['approved', 'canonical'].includes(status);
}

function normalizeAssetRow(row = {}) {
  if (!row || typeof row !== 'object') return row;
  return {
    id: row.id,
    objectType: row.objectType || row.object_type || null,
    title: row.title || null,
    channel: row.channel || null,
    content: row.content || {},
    lifecycleState: row.lifecycleState || row.lifecycle_state || null,
    validationState: row.validationState || row.validation_state || null,
    status: row.status || null,
    version: row.version != null ? Number(row.version) : null,
    updatedAt: row.updatedAt || row.updated_at || null,
    relationships: row.relationships || [],
    provenance: row.provenance || {},
  };
}

function resolveOutreachAssetMessage(asset = {}, opts = {}) {
  const normalized = normalizeAssetRow(asset);
  const assetId = asText(normalized.id);
  if (!assetId) {
    throw knowledgeError('outreach_asset_id_required', 'Outreach asset id is required.');
  }

  if (opts.requireStakeholderValidated && !isStakeholderValidated(normalized)) {
    throw knowledgeError('outreach_asset_not_validated', 'Outreach asset is not stakeholder-validated.', {
      assetId,
      lifecycleState: normalized.lifecycleState,
      validationState: normalized.validationState,
      status: normalized.status,
    });
  }

  const content = isObject(normalized.content) ? normalized.content : {};
  const resolved = resolveVariantCopy(content) || resolveDirectCopy(content);
  if (!resolved?.subject || !resolved?.body) {
    throw knowledgeError('outreach_asset_copy_missing', `Outreach asset ${assetId} missing executable subject/body.`, {
      assetId,
      contentKeys: Object.keys(content),
    });
  }

  const channel = asText(normalized.channel || content.channel || content.messaging?.channel) || 'email';
  const revision = assetRevision(normalized);

  return {
    subject: resolved.subject,
    body: resolved.body,
    channel,
    assetId,
    revision,
    version: normalized.version,
    source: resolved.source,
  };
}

module.exports = {
  SUBJECT_KEYS,
  BODY_KEYS,
  pickText,
  resolveOutreachAssetMessage,
  normalizeAssetRow,
  isStakeholderValidated,
};
