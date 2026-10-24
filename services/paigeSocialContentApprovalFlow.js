'use strict';

/**
 * Shared approval orchestration for dashboard, client portal, and legacy handlers.
 */

const pool = require('../db');
const { publishBlogPost } = require('../utils/blogPublisher');
const {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishToLinkedInPersonal,
  publishLinkComment,
} = require('../utils/publishPipeline');
const {
  resolveCanonicalSocialContentArtifact,
  routePaigeSocialContentApproval,
  isPaigeSocialPublishChannel,
} = require('./paigeSocialContentApproval');

const LEGACY_PUBLISHERS = Object.freeze({
  blog: publishBlogPost,
  google_business: publishToGoogleBusiness,
  facebook_page: publishToFacebookPage,
  facebook: publishFayeComment,
  linkedin_page: publishToLinkedInPage,
  linkedin_personal: publishToLinkedInPersonal,
  linkedin: publishLinkComment,
});

async function loadPendingComment(clientId, pendingCommentId) {
  const result = await pool.query(
    `SELECT * FROM pending_comments
     WHERE id = $1 AND client_id = $2
     LIMIT 1`,
    [pendingCommentId, clientId]
  );
  return result.rows[0] || null;
}

/**
 * Apply an approval action to a pending comment, preferring canonical artifact authority.
 * @param {object} input
 */
async function applyPendingCommentApprovalAction(input = {}) {
  const clientId = Number(input.clientId);
  const pendingCommentId = input.pendingCommentId || input.id;
  const action = String(input.action || '').trim().toLowerCase();
  if (!Number.isFinite(clientId) || !pendingCommentId) {
    throw new Error('approval_context_required');
  }
  if (!['approved', 'rejected'].includes(action)) {
    throw new Error('invalid_action');
  }

  const item = input.pendingComment || await loadPendingComment(clientId, pendingCommentId);
  if (!item) {
    return { ok: false, statusCode: 404, error: 'Approval not found' };
  }
  if (item.status !== 'pending') {
    return {
      ok: true,
      mode: 'already_decided',
      action: item.status,
      id: item.id,
    };
  }

  const artifact = await resolveCanonicalSocialContentArtifact({
    clientId,
    tenantId: String(clientId),
    pendingCommentId: item.id,
    channel: item.channel,
  });

  if (artifact && action === 'approved' && !input.expectedApprovalHash) {
    return { ok: false, statusCode: 409, mode: 'canonical_review_required', error: 'explicit_artifact_approval_required',
      message: 'Review the exact draft and destination account in Paige social review before approval.',
      reviewUrl: `/paige-social?client_id=${clientId}` };
  }
  if (artifact) {
    const canonical = await routePaigeSocialContentApproval({
      clientId,
      tenantId: String(clientId),
      pendingCommentId: item.id,
      action, expectedApprovalHash: input.expectedApprovalHash, accountId: input.accountId, approvedBy: input.approvedBy, publishOnApprove: false,
      invocationSource: input.invocationSource || input.source || 'approval_flow',
    });
    return {
      ok: canonical.success,
      mode: 'canonical',
      action,
      id: item.id,
      artifact: canonical.artifact,
      publication: canonical.publication || null,
      error: canonical.error || null,
    };
  }

  if (isPaigeSocialPublishChannel(item.channel)) {
    return {
      ok: false,
      statusCode: 409,
      mode: 'canonical_required',
      error: 'canonical_artifact_required',
      message: 'Paige social content requires a canonical artifact before approval can publish.',
    };
  }

  await pool.query(
    'UPDATE pending_comments SET status = $1 WHERE id = $2 AND client_id = $3',
    [action, item.id, clientId]
  );

  if (action === 'approved') {
    const publish = LEGACY_PUBLISHERS[item.channel];
    if (publish) {
      publish(item).catch((err) =>
        console.error(`[LegacyPublisher:${item.channel}]`, err.message)
      );
    }
  }

  return {
    ok: true,
    mode: 'legacy',
    action,
    id: item.id,
  };
}

module.exports = {
  applyPendingCommentApprovalAction,
  isPaigeSocialPublishChannel,
  LEGACY_PUBLISHERS,
};
