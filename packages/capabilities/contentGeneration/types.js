'use strict';

/**
 * SPEC-256 — Canonical Paige social content generation types.
 */

const CAPABILITY_ID = 'social_content';
const CAPABILITY_FAMILY = 'CONTENT_GENERATION';
const CAPABILITY_VERSION = '1.0.0';
const ARTIFACT_TYPE = 'social_content_draft';

const APPROVAL_STATES = Object.freeze({
  DRAFT: 'DRAFT',
  PENDING_APPROVAL: 'PENDING_APPROVAL',
  APPROVED: 'APPROVED',
  REJECTED: 'REJECTED',
  /** @deprecated Use publishState PUBLISHED — kept for legacy row normalization only */
  PUBLISHED: 'PUBLISHED',
});

const PUBLISH_STATES = Object.freeze({
  NOT_PUBLISHED: 'NOT_PUBLISHED',
  PUBLISHING: 'PUBLISHING',
  PUBLISHED: 'PUBLISHED',
  FAILED: 'FAILED',
  VERIFYING: 'VERIFYING',
  UNKNOWN: 'UNKNOWN',
});

const SOURCE_TYPES = Object.freeze({
  MISSION: 'mission',
  JOB: 'job',
  MANUAL: 'manual',
  PHOTO_EVIDENCE: 'photo_evidence',
  UNKNOWN: 'unknown',
});

const FORBIDDEN_GENERATION_ACTIONS = Object.freeze([
  'publish',
  'publishNow',
  'publish_now',
  'send',
  'sendNow',
  'send_now',
  'bufferCall',
  'buffer_call',
  'executePublish',
  'execute_publish',
  'autonomousPublish',
  'autonomous_publish',
]);

function assertNoPublishAuthority(payload = {}) {
  for (const key of FORBIDDEN_GENERATION_ACTIONS) {
    if (payload[key] === true) {
      throw new Error(`generation_forbids_publish_authority:${key}`);
    }
  }
}

function buildProvenance(partial = {}) {
  return {
    capabilityFamily: CAPABILITY_FAMILY,
    capabilityId: CAPABILITY_ID,
    capabilityVersion: CAPABILITY_VERSION,
    specialist: 'paige',
    tenantId: partial.tenantId != null ? String(partial.tenantId) : null,
    platform: partial.platform || partial.channel || null,
    contentObjective: partial.contentObjective || partial.objective || null,
    missionId: partial.missionId || null,
    sourceContext: partial.sourceContext || partial.workspaceContext || null,
    evidence: Array.isArray(partial.evidence) ? partial.evidence : [],
    cadenceContext: partial.cadenceContext || null,
    invokedAt: partial.invokedAt || new Date().toISOString(),
    invocationSource: partial.invocationSource || partial.source || null,
  };
}

function inferSourceType(partial = {}) {
  if (partial.sourceType) return partial.sourceType;
  if (partial.missionId || partial.provenance?.missionId) return SOURCE_TYPES.MISSION;
  if (partial.provenance?.invocationSource === 'cron' || partial.provenance?.invocationSource === 'dashboard') {
    return SOURCE_TYPES.JOB;
  }
  if (partial.provenance?.invocationSource === 'manual') return SOURCE_TYPES.MANUAL;
  return SOURCE_TYPES.UNKNOWN;
}

function normalizeLegacyApprovalPublishState(partial = {}) {
  let approvalState = partial.approvalState || APPROVAL_STATES.PENDING_APPROVAL;
  let publishState = partial.publishState || PUBLISH_STATES.NOT_PUBLISHED;
  if (approvalState === APPROVAL_STATES.PUBLISHED) {
    approvalState = APPROVAL_STATES.APPROVED;
    publishState = PUBLISH_STATES.PUBLISHED;
  }
  return { approvalState, publishState };
}

function buildSocialContentArtifact(partial = {}) {
  assertNoPublishAuthority(partial);
  const { approvalState, publishState } = normalizeLegacyApprovalPublishState(partial);
  if (publishState === PUBLISH_STATES.PUBLISHED && !partial.id) {
    throw new Error('generation_cannot_create_published_artifact');
  }
  return {
    id: partial.id,
    tenantId: String(partial.tenantId),
    clientId: Number(partial.clientId),
    sourceType: inferSourceType(partial),
    businessId: partial.businessId || partial.business_id || null,
    platform: partial.platform || partial.channel,
    contentObjective: partial.contentObjective || null,
    missionId: partial.missionId || null,
    companyName: partial.companyName || null,
    contentType: partial.contentType || null,
    label: partial.label,
    body: partial.body,
    mediaRefs: partial.mediaRefs || partial.media_refs || null,
    meta: partial.meta && typeof partial.meta === 'object' ? partial.meta : {},
    provenance: buildProvenance(partial.provenance || partial),
    approvalState,
    publishState,
    approvalBinding: partial.approvalBinding || null,
    publication: partial.publication || {},
    rejectionReason: partial.rejectionReason || partial.rejection_reason || null,
    publishError: partial.publishError || partial.publish_error || null,
    publishedUrl: partial.publishedUrl || partial.published_url || null,
    pendingCommentId: partial.pendingCommentId || null,
    createdAt: partial.createdAt || new Date().toISOString(),
    updatedAt: partial.updatedAt || new Date().toISOString(),
    approvedAt: partial.approvedAt || partial.approved_at || null,
    rejectedAt: partial.rejectedAt || partial.rejected_at || null,
    publishedAt: partial.publishedAt || partial.published_at || null,
  };
}

module.exports = {
  CAPABILITY_ID,
  CAPABILITY_FAMILY,
  CAPABILITY_VERSION,
  ARTIFACT_TYPE,
  APPROVAL_STATES,
  PUBLISH_STATES,
  SOURCE_TYPES,
  FORBIDDEN_GENERATION_ACTIONS,
  assertNoPublishAuthority,
  buildProvenance,
  buildSocialContentArtifact,
  inferSourceType,
  normalizeLegacyApprovalPublishState,
};
