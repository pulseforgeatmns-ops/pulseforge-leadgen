'use strict';

/**
 * SPEC-256 — Canonical Paige social content approval.
 * Records operator decisions against tenant-scoped artifacts only.
 */

const { APPROVAL_STATES } = require('./types');
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

  async function recordDecision(input = {}) {
    const decision = normalizeDecision(input.decision || input.action);
    const { tenantId, clientId, artifact } = await resolveArtifact(input);
    const toState = targetStateForDecision(decision);

    const transition = await store.transitionApprovalState(
      artifact.id,
      tenantId,
      clientId,
      toState,
      { allowedFrom: [APPROVAL_STATES.PENDING_APPROVAL] }
    );

    if (!transition.ok) {
      if (
        transition.reason === 'invalid_transition' &&
        artifact.approvalState === toState
      ) {
        const mirrored = artifact.pendingCommentId
          ? await mirrorPendingComment(
            artifact.pendingCommentId,
            mirrorPendingCommentStatus(decision),
            clientId
          )
          : null;
        return {
          ok: true,
          idempotent: true,
          decision,
          artifact: { ...artifact, approvalState: toState },
          pendingComment: mirrored,
        };
      }
      const err = new Error(transition.reason || 'approval_transition_failed');
      err.code = transition.reason || 'approval_transition_failed';
      err.from = transition.from;
      err.to = toState;
      throw err;
    }

    const mirrored = transition.artifact.pendingCommentId
      ? await mirrorPendingComment(
        transition.artifact.pendingCommentId,
        mirrorPendingCommentStatus(decision),
        clientId
      )
      : null;

    return {
      ok: true,
      idempotent: false,
      decision,
      artifact: transition.artifact,
      pendingComment: mirrored,
    };
  }

  return {
    DECISIONS,
    normalizeDecision,
    recordDecision,
    resolveArtifact,
    store,
  };
}

module.exports = {
  DECISIONS,
  normalizeDecision,
  targetStateForDecision,
  mirrorPendingCommentStatus,
  createSocialContentApprovalService,
};
