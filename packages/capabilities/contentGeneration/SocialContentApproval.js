'use strict';

/**
 * SPEC-256 — Canonical Paige social content approval.
 * Records operator decisions against tenant-scoped artifacts only.
 */

const { approvalHash, contentHash, assertSocialCopy } = require('../contentPublication/approvalBinding');
const { resolveSocialAccount } = require('../contentPublication/socialAccounts');
const { APPROVAL_STATES, PUBLISH_STATES } = require('./types');
const {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
} = require('./SocialContentStore');

const DECISIONS = Object.freeze({
  APPROVE: 'approve',
  REJECT: 'reject',
});

function normalizeDecision(value) {
  const decision = String(value || '').trim().toLowerCase();
  if (decision === 'approved') return DECISIONS.APPROVE;
  if (decision === 'rejected') return DECISIONS.REJECT;
  if (decision === DECISIONS.APPROVE || decision === DECISIONS.REJECT) return decision;
  throw new Error('invalid_approval_decision');
}

function targetStateForDecision(decision) {
  return decision === DECISIONS.APPROVE
    ? APPROVAL_STATES.APPROVED
    : APPROVAL_STATES.REJECTED;
}

function mirrorPendingCommentStatus(decision) {
  return decision === DECISIONS.APPROVE ? 'approved' : 'rejected';
}

function isPublishedArtifact(artifact) {
  return artifact.publishState === PUBLISH_STATES.PUBLISHED
    || artifact.approvalState === APPROVAL_STATES.PUBLISHED;
}

/**
 * @param {object} [deps]
 */
function createSocialContentApprovalService(deps = {}) {
  const pool = deps.pool || null;
  const store =
    deps.socialContentStore ||
    (pool ? createPostgresSocialContentStore(pool) : createInMemorySocialContentStore());
  const mirrorPendingComment =
    deps.mirrorPendingComment ||
    (async (pendingCommentId, status, clientId) => {
      if (!pool || !pendingCommentId) return null;
      const result = await pool.query(
        `UPDATE pending_comments
         SET status = $1
         WHERE id = $2 AND client_id = $3
         RETURNING id, status`,
        [status, pendingCommentId, clientId]
      );
      return result.rows[0] || null;
    });

  async function resolveArtifact(input = {}) {
    const tenantId = String(input.tenantId ?? input.clientId ?? '').trim();
    const clientId = Number(input.clientId);
    if (!tenantId || !Number.isFinite(clientId)) {
      throw new Error('tenant_scope_required');
    }
    if (tenantId !== String(clientId)) {
      throw new Error('tenant_client_mismatch');
    }

    if (input.artifactId) {
      const artifact = await store.getById(input.artifactId, tenantId, clientId);
      if (!artifact) throw new Error('artifact_not_found');
      return { tenantId, clientId, artifact };
    }

    const pendingCommentId = input.pendingCommentId || input.pending_comment_id;
    if (!pendingCommentId) throw new Error('artifact_reference_required');

    const artifact = await store.getByPendingCommentId(pendingCommentId, tenantId, clientId);
    if (!artifact) throw new Error('artifact_not_found');
    return { tenantId, clientId, artifact };
  }

  const accountResolver = deps.accountResolver || resolveSocialAccount;

  async function preview(input = {}) {
    const { tenantId, clientId, artifact } = await resolveArtifact(input);
    const { account } = await accountResolver({ tenantId, clientId, platform: artifact.platform, accountId: input.accountId || artifact.approvalBinding?.account.id });
    assertSocialCopy(artifact);
    return { artifact, account, contentHash: contentHash(artifact), approvalHash: approvalHash(artifact, account) };
  }

  async function recordDecision(input = {}) {
    const decision = normalizeDecision(input.decision || input.action);
    const { tenantId, clientId, artifact } = await resolveArtifact(input);
    const saved = await store.withLockedArtifact(artifact.id, tenantId, clientId, async (current, tx) => {
      if (['PUBLISHING', 'VERIFYING', 'UNKNOWN', 'PUBLISHED'].includes(current.publishState)) {
        if (decision === 'approve' && current.approvalState === 'APPROVED') return { artifact: current, idempotent: true };
        throw new Error('cannot_reject_published_artifact');
      }
      if (decision === 'approve' && current.approvalState === 'REJECTED') throw new Error('rejected_artifact_requires_restore');
      const patch = {};
      if (decision === 'approve') {
        if (!input.approvedBy || !input.expectedApprovalHash) throw new Error('explicit_artifact_approval_required');
        const { account } = await accountResolver({ tenantId, clientId, platform: current.platform, accountId: input.accountId });
        assertSocialCopy(current);
        const digest = approvalHash(current, account);
        if (input.expectedApprovalHash !== digest) throw new Error('approval_preview_changed');
        if (current.approvalBinding && current.approvalBinding.hash !== digest) throw new Error('approved_artifact_or_account_changed');
        patch.approvalBinding = current.approvalBinding || { version: 1, hash: digest, contentHash: contentHash(current), account,
          approvedBy: String(input.approvedBy), approvedAt: new Date().toISOString() };
        patch.approvedAt = patch.approvalBinding.approvedAt;
        patch.rejectionReason = null;
      } else {
        patch.rejectedAt = new Date().toISOString();
        patch.rejectionReason = input.rejectionReason || null;
      }
      patch.approvalState = targetStateForDecision(decision);
      return { artifact: await tx.updateArtifactMetadata(current.id, tenantId, clientId, patch), idempotent: current.approvalState === patch.approvalState };
    });
    const mirrored = saved.artifact.pendingCommentId
      ? await mirrorPendingComment(saved.artifact.pendingCommentId, saved.artifact.publishState === 'PUBLISHED' ? 'posted' : mirrorPendingCommentStatus(decision), clientId) : null;
    return { ok: true, ...saved, decision, pendingComment: mirrored };
  }

  return {
    DECISIONS,
    normalizeDecision,
    recordDecision,
    preview,
    resolveArtifact,
    store,
  };
}

module.exports = {
  DECISIONS,
  normalizeDecision,
  targetStateForDecision,
  mirrorPendingCommentStatus,
  isPublishedArtifact,
  createSocialContentApprovalService,
};
