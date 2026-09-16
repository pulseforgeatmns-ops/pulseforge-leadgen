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
  PUBLISHED: 'PUBLISHED',
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

function buildSocialContentArtifact(partial = {}) {
  assertNoPublishAuthority(partial);
  const approvalState = partial.approvalState || APPROVAL_STATES.PENDING_APPROVAL;
  if (approvalState === APPROVAL_STATES.PUBLISHED) {
    throw new Error('generation_cannot_create_published_artifact');
  }
  return {
    id: partial.id,
    tenantId: String(partial.tenantId),
    clientId: Number(partial.clientId),
    platform: partial.platform || partial.channel,
    contentObjective: partial.contentObjective || null,
    missionId: partial.missionId || null,
    companyName: partial.companyName || null,
    contentType: partial.contentType || null,
    label: partial.label,
    body: partial.body,
    meta: partial.meta && typeof partial.meta === 'object' ? partial.meta : {},
    provenance: buildProvenance(partial.provenance || partial),
    approvalState,
    pendingCommentId: partial.pendingCommentId || null,
    createdAt: partial.createdAt || new Date().toISOString(),
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

module.exports = {
  CAPABILITY_ID,
  CAPABILITY_FAMILY,
  CAPABILITY_VERSION,
  ARTIFACT_TYPE,
  APPROVAL_STATES,
  FORBIDDEN_GENERATION_ACTIONS,
  assertNoPublishAuthority,
  buildProvenance,
  buildSocialContentArtifact,
};
