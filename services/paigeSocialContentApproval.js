'use strict';

/**
 * SPEC-256 — Canonical execution router for Paige social content approval.
 * Records operator decisions on tenant-scoped artifacts; publication is separate.
 */

const pool = require('../db');
const {
  createSocialContentApprovalService,
  isPaigeSocialPublishChannel,
  normalizeDecision,
} = require('../packages/capabilities/contentGeneration');
const { routePaigeSocialContentPublication } = require('./paigeSocialContentPublication');

let _approvalService = null;

function getApprovalService() {
  if (!_approvalService) {
    _approvalService = createSocialContentApprovalService({ pool });
  }
  return _approvalService;
}

function resetPaigeSocialContentApprovalForTests() {
  _approvalService = null;
  require('./paigeSocialContentPublication').resetPaigeSocialContentPublicationForTests();
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

/**
 * Resolve whether a pending comment is backed by a canonical Paige artifact.
 * @param {object} input
 */
async function resolveCanonicalSocialContentArtifact(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const pendingCommentId = input.pendingCommentId || input.pending_comment_id || input.id;
  if (!pendingCommentId) return null;
  if (input.channel && !isPaigeSocialPublishChannel(input.channel)) return null;

  try {
    const { artifact } = await getApprovalService().resolveArtifact({
      tenantId,
      clientId,
      pendingCommentId,
    });
    return artifact;
  } catch (err) {
    if (err.message === 'artifact_not_found') return null;
    throw err;
  }
}

/**
 * @param {object} input
 */
async function routePaigeSocialContentApproval(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const decision = normalizeDecision(input.decision || input.action);
  const publishOnApprove = input.publishOnApprove === true;

  const approval = await getApprovalService().recordDecision({
    tenantId,
    clientId,
    artifactId: input.artifactId || input.artifact_id,
    pendingCommentId: input.pendingCommentId || input.pending_comment_id || input.id,
    decision,
    expectedApprovalHash: input.expectedApprovalHash, accountId: input.accountId, approvedBy: input.approvedBy,
    rejectionReason: input.rejectionReason,
  });

  let publication = null;
  if (decision === 'approve' && publishOnApprove) {
    publication = await routePaigeSocialContentPublication({
      tenantId,
      clientId,
      artifactId: approval.artifact.id,
      invocationSource: input.invocationSource || input.source || 'approval_router',
      dryRun: Boolean(input.dryRun ?? input.dry_run),
    });
  }

  return {
    spec: 'SPEC-256',
    success: approval.ok && (decision === 'reject' || !publication || publication.success),
    client_id: clientId,
    tenant_id: tenantId,
    decision,
    idempotent: approval.idempotent === true,
    artifact: approval.artifact,
    pending_comment: approval.pendingComment,
    publication,
    error: publication?.error || null,
  };
}

/**
 * Canonical approval action — records operator decision on artifact state only.
 * Publication is optional and routed separately when publishOnApprove is true.
 */
async function applyPaigeSocialArtifactApprovalAction(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const action = input.action === 'APPROVE' || input.action === 'REJECT'
    ? input.action.toLowerCase()
    : (input.action || input.decision);
  return routePaigeSocialContentApproval({
    ...input,
    tenantId,
    clientId,
    action,
    publishOnApprove: input.publishOnApprove ?? false,
  });
}

module.exports = {
  getApprovalService,
  resolveCanonicalSocialContentArtifact,
  routePaigeSocialContentApproval,
  applyPaigeSocialArtifactApprovalAction,
  resetPaigeSocialContentApprovalForTests,
  isPaigeSocialPublishChannel,
};
