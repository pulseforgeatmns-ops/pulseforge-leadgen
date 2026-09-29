'use strict';

/**
 * Internal mirror shape for legacy publisher side effects (pending_comments UI).
 * Adapters may use this; the capability never passes mirrors to callers.
 */

function buildPendingCommentMirror(artifact = {}) {
  const meta = artifact.meta && typeof artifact.meta === 'object' ? artifact.meta : {};
  const company = meta.company && typeof meta.company === 'object' ? meta.company : {};
  return {
    id: artifact.pendingCommentId,
    client_id: artifact.clientId,
    channel: artifact.platform,
    author_name: artifact.companyName || company.name || null,
    author_title: company.industry || 'Local Business',
    post_content: artifact.label,
    comment: artifact.body,
    post_url: null,
    status: 'approved',
  };
}

module.exports = {
  buildPendingCommentMirror,
};
