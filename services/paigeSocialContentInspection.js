'use strict';

/**
 * SPEC-256 — Database-backed Paige social content status for operator/Max inspection.
 */

const pool = require('../db');
const {
  createPostgresSocialContentStore,
  APPROVAL_STATES,
  PUBLISH_STATES,
} = require('../packages/capabilities/contentGeneration');

const STATUS_KINDS = Object.freeze({
  NO_DRAFTS: 'no_drafts',
  PENDING_APPROVAL: 'pending_approval',
  APPROVED_NOT_PUBLISHED: 'approved_not_published',
  PUBLISHED: 'published',
  FAILED: 'failed',
  MIXED: 'mixed',
});

let _store = null;

function getStore() {
  if (!_store) {
    _store = createPostgresSocialContentStore(pool);
  }
  return _store;
}

function resetPaigeSocialContentInspectionForTests() {
  _store = null;
}

function assertTenantInput(input = {}) {
  const clientId = Number(input.client_id ?? input.clientId ?? input.tenantId);
  const tenantId = String(input.tenantId ?? input.tenant_id ?? clientId ?? '').trim();
  if (!tenantId || !Number.isInteger(clientId) || clientId < 1) {
    throw new Error('tenant_scope_required');
  }
  if (tenantId !== String(clientId)) {
    throw new Error('tenant_client_mismatch');
  }
  return { tenantId, clientId };
}

function summarizeCounts(artifacts) {
  return {
    total: artifacts.length,
    pendingApproval: artifacts.filter((a) => a.approvalState === APPROVAL_STATES.PENDING_APPROVAL).length,
    approved: artifacts.filter((a) => a.approvalState === APPROVAL_STATES.APPROVED).length,
    rejected: artifacts.filter((a) => a.approvalState === APPROVAL_STATES.REJECTED).length,
    notPublished: artifacts.filter((a) => a.publishState === PUBLISH_STATES.NOT_PUBLISHED).length,
    publishing: artifacts.filter((a) => a.publishState === PUBLISH_STATES.PUBLISHING).length,
    published: artifacts.filter((a) => a.publishState === PUBLISH_STATES.PUBLISHED).length,
    verifying: artifacts.filter(a => a.publishState === PUBLISH_STATES.VERIFYING).length,
    unknown: artifacts.filter(a => a.publishState === PUBLISH_STATES.UNKNOWN).length,
    failed: artifacts.filter((a) => a.publishState === PUBLISH_STATES.FAILED).length,
  };
}

function deriveStatusKind(counts) {
  if (counts.unknown > 0 || counts.verifying > 0 || counts.publishing > 0) return STATUS_KINDS.MIXED;
  if (counts.total === 0) return STATUS_KINDS.NO_DRAFTS;
  if (counts.failed > 0 && counts.pendingApproval === 0 && counts.approved === 0) {
    return STATUS_KINDS.FAILED;
  }
  const activeKinds = [
    counts.pendingApproval > 0 ? STATUS_KINDS.PENDING_APPROVAL : null,
    counts.approved > 0 && counts.notPublished > 0 ? STATUS_KINDS.APPROVED_NOT_PUBLISHED : null,
    counts.published > 0 ? STATUS_KINDS.PUBLISHED : null,
    counts.failed > 0 ? STATUS_KINDS.FAILED : null,
  ].filter(Boolean);
  if (activeKinds.length === 1) return activeKinds[0];
  if (activeKinds.length === 0 && counts.rejected === counts.total) return STATUS_KINDS.NO_DRAFTS;
  return STATUS_KINDS.MIXED;
}

function buildNarrative(statusKind, counts, artifacts) {
  switch (statusKind) {
    case STATUS_KINDS.NO_DRAFTS:
      return {
        summary: 'Paige has not generated any social content yet.',
        nextAction: 'generate draft content.',
        ready: false,
        waitingOnOperator: false,
      };
    case STATUS_KINDS.PENDING_APPROVAL:
      return {
        summary: 'Paige is waiting on operator approval.',
        nextAction: 'approve, reject, or request revisions.',
        ready: true,
        waitingOnOperator: true,
        pendingCount: counts.pendingApproval,
      };
    case STATUS_KINDS.APPROVED_NOT_PUBLISHED:
      return {
        summary: 'Paige has approved content ready to publish.',
        nextAction: 'publish approved artifact.',
        ready: true,
        waitingOnOperator: false,
        approvedCount: counts.approved,
      };
    case STATUS_KINDS.PUBLISHED: {
      const latest = artifacts.find((a) => a.publishState === PUBLISH_STATES.PUBLISHED) || artifacts[0];
      return {
        summary: 'Paige has published content.',
        nextAction: 'generate the next draft or review published performance.',
        ready: true,
        waitingOnOperator: false,
        latestPublished: latest
          ? {
            id: latest.id,
            platform: latest.platform,
            publishedAt: latest.publishedAt,
            publishedUrl: latest.publishedUrl,
          }
          : null,
      };
    }
    case STATUS_KINDS.FAILED: {
      const failed = artifacts.find((a) => a.publishState === PUBLISH_STATES.FAILED);
      return {
        summary: 'Paige attempted to publish but failed.',
        nextAction: 'retry or revise configuration.',
        ready: false,
        waitingOnOperator: true,
        publishError: failed?.publishError || null,
      };
    }
    default:
      return {
        summary: 'Paige has multiple social content states in flight.',
        nextAction: counts.unknown > 0 ? 'reconcile uncertain sends with the provider; do not create another post.' : counts.verifying > 0 ? 'read back accepted posts until publication is verified.' : 'review pending approvals, approved drafts, and failed publishes.',
        ready: true,
        waitingOnOperator: counts.pendingApproval > 0 || counts.failed > 0 || counts.unknown > 0 || counts.verifying > 0 || counts.publishing > 0,
      };
  }
}

/**
 * Inspect canonical Paige social artifact state for a tenant.
 * @param {object} input
 */
async function inspectPaigeSocialContentStatus(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const missionId = input.missionId || input.mission_id || null;
  const store = input.store || getStore();

  if (typeof store.ensureSchema === 'function') {
    await store.ensureSchema();
  }

  const artifacts = await store.listByTenant(tenantId, clientId, { missionId });
  const counts = summarizeCounts(artifacts);
  const statusKind = deriveStatusKind(counts);
  const narrative = buildNarrative(statusKind, counts, artifacts);

  const pendingArtifacts = artifacts
    .filter((a) => a.approvalState === APPROVAL_STATES.PENDING_APPROVAL)
    .map((a) => ({
      id: a.id,
      platform: a.platform,
      label: a.label,
      createdAt: a.createdAt,
    }));

  const publishedArtifacts = artifacts
    .filter((a) => a.publishState === PUBLISH_STATES.PUBLISHED)
    .map((a) => ({
      id: a.id,
      platform: a.platform,
      publishedAt: a.publishedAt,
      publishedUrl: a.publishedUrl,
    }));

  return {
    spec: 'SPEC-256',
    tenantId,
    clientId,
    missionId,
    statusKind,
    counts,
    narrative,
    pendingArtifacts,
    publishedArtifacts,
    publicationAttempts: artifacts.filter(a => Object.keys(a.publication || {}).length).map(a => ({ artifactId: a.id, publishState: a.publishState, ...a.publication })),
    failedArtifacts: artifacts
      .filter((a) => a.publishState === PUBLISH_STATES.FAILED)
      .map((a) => ({ id: a.id, platform: a.platform, publishError: a.publishError })),
    approvedNotPublished: artifacts
      .filter(
        (a) =>
          a.approvalState === APPROVAL_STATES.APPROVED
          && a.publishState !== PUBLISH_STATES.PUBLISHED
      )
      .map((a) => ({ id: a.id, platform: a.platform, publishState: a.publishState })),
  };
}

module.exports = {
  STATUS_KINDS,
  inspectPaigeSocialContentStatus,
  resetPaigeSocialContentInspectionForTests,
  summarizeCounts,
  deriveStatusKind,
};
