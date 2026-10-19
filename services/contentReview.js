'use strict';

const { createPostgresSocialContentStore } = require('../packages/capabilities/contentGeneration/SocialContentStore');
const { isPaigeSocialPublishChannel } = require('../services/paigeSocialContentApproval');

function safeMediaUrl(value) {
  const url = String(value || '').trim();
  if (/^\/(?!\/)/.test(url)) return url;
  try { return ['http:', 'https:'].includes(new URL(url).protocol) ? url : null; } catch (_) { return null; }
}

function mediaFor(item) {
  let refs = item.mediaRefs || item.media_refs || item.meta?.mediaRefs || item.meta?.media_refs || item.meta?.images || [];
  if (!Array.isArray(refs)) refs = [refs];
  refs = [...refs, item.image_url, item.meta?.imageUrl, item.meta?.image_url].filter(Boolean);
  const seen = new Set();
  return refs.map(ref => {
    const url = safeMediaUrl(typeof ref === 'string' ? ref : ref.url || ref.src || ref.image_url || ref.previewUrl);
    if (!url || seen.has(url)) return null;
    seen.add(url);
    return { url, alt: typeof ref === 'object' ? String(ref.alt || ref.caption || 'Draft attachment') : 'Draft attachment',
      type: /\.(mp4|webm)(?:[?#]|$)/i.test(url) || ref?.type === 'video' ? 'video' : 'image' };
  }).filter(Boolean);
}

function canonicalItem(artifact) {
  return { id: `social:${artifact.id}`, artifactId: artifact.id, source: 'canonical',
    channel: artifact.platform, title: artifact.label || 'Social draft', body: artifact.body || '',
    objective: artifact.contentObjective || null, media: mediaFor(artifact),
    status: artifact.approvalState, publishState: artifact.publishState,
    createdAt: artifact.createdAt, pendingCommentId: artifact.pendingCommentId || null,
    canReview: artifact.approvalState === 'PENDING_APPROVAL' && ['NOT_PUBLISHED', 'FAILED'].includes(artifact.publishState),
    decisionMode: 'exact_preview', blocker: null };
}

async function readContentReview({ clientId, pool, socialStore, now = new Date() }) {
  const id = Number(clientId);
  if (!Number.isSafeInteger(id) || id <= 0) throw new Error('tenant_scope_required');
  const store = socialStore || createPostgresSocialContentStore(pool);
  const [legacyRead, socialRead] = await Promise.allSettled([
    pool.query("SELECT * FROM pending_comments WHERE client_id = $1 AND status = 'pending' ORDER BY created_at DESC", [id]),
    store.listByTenant(String(id), id),
  ]);
  const warnings = [];
  if (legacyRead.status !== 'fulfilled') warnings.push('Some queued content could not be loaded. Refresh to try again.');
  if (socialRead.status !== 'fulfilled') warnings.push('Social draft verification is temporarily unavailable. Existing drafts remain visible.');
  if (warnings.length === 2) throw new Error('content_review_unavailable');
  const artifacts = socialRead.status === 'fulfilled' ? socialRead.value : [];
  const mirrored = new Set(artifacts.map(a => String(a.pendingCommentId || '')).filter(Boolean));
  const items = artifacts.filter(a => a.approvalState === 'PENDING_APPROVAL' && !['PUBLISHING', 'VERIFYING', 'UNKNOWN', 'PUBLISHED'].includes(a.publishState)).map(canonicalItem);
  for (const row of legacyRead.status === 'fulfilled' ? legacyRead.value.rows : []) {
    if (mirrored.has(String(row.id))) continue;
    const requiresBinding = isPaigeSocialPublishChannel(row.channel);
    items.push({ id: `pending:${row.id}`, pendingCommentId: row.id, source: 'legacy', channel: row.channel,
      title: row.author_name || 'Content draft', body: row.comment || '',
      originalPost: row.post_content || null, postUrl: safeMediaUrl(row.post_url),
      media: mediaFor(row), status: 'PENDING_APPROVAL', createdAt: row.created_at,
      canReview: !requiresBinding, decisionMode: requiresBinding ? 'binding_required' : 'legacy',
      blocker: requiresBinding ? 'This older draft needs a verified post version and destination before it can be approved.' : null });
  }
  items.sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
  return { clientId: id, generatedAt: now.toISOString(), items, pendingCount: items.length, warnings };
}

module.exports = { readContentReview, safeMediaUrl, mediaFor };
