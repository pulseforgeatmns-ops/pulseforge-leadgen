'use strict';
const { createHash } = require('crypto');
const {
  validateAnchorSocialCopy,
  buildAnchorCopyDoctrineViolationError,
} = require('../../../utils/anchorCopyDoctrine');

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k, stable(value[k])]));
  return value;
}
function hash(value) { return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex'); }
function contentSnapshot(a) {
  return { id: a.id, tenantId: a.tenantId, clientId: a.clientId, platform: a.platform,
    body: a.body, label: a.label, mediaRefs: a.mediaRefs, meta: a.meta,
    missionId: a.missionId, contentObjective: a.contentObjective, businessId: a.businessId,
    companyName: a.companyName, contentType: a.contentType, provenance: a.provenance };
}
function contentHash(a) { return hash(contentSnapshot(a)); }
function approvalHash(a, account) { return hash({ version: 1, contentHash: contentHash(a), account }); }
function assertApproval(a, account) {
  if (a.approvalState !== 'APPROVED') throw new Error('publication_requires_approved_artifact');
  if (!a.approvalBinding) throw new Error('approval_binding_required');
  if (a.approvalBinding.hash !== approvalHash(a, account)) throw new Error('approved_artifact_or_account_changed');
}
function assertSocialCopy(a) {
  if (!a.body || typeof a.body !== 'string') throw new Error('content_required');
  // Text-only adapters must never silently discard approved media or auxiliary content.
  if ((a.mediaRefs && (!Array.isArray(a.mediaRefs) || a.mediaRefs.length)) || a.meta?.firstComment || a.meta?.first_comment) throw new Error('unsupported_social_media_or_first_comment');
  if (Number(a.clientId) !== 10) return;
  const check = validateAnchorSocialCopy(a.body);
  if (!check.ok) throw buildAnchorCopyDoctrineViolationError(check.violations);
}
module.exports = { hash, contentSnapshot, contentHash, approvalHash, assertApproval, assertSocialCopy };
