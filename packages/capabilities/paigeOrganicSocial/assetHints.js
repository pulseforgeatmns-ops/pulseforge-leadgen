'use strict';

/**
 * Filename-only hints. Never treated as verified facts about jobs or customers.
 */

function inferMediaKind(mimeType = '', filename = '') {
  const mime = String(mimeType).toLowerCase();
  const name = String(filename).toLowerCase();
  if (mime.startsWith('video/') || /\.(mp4|mov|webm|m4v)$/.test(name)) return 'video';
  if (mime.startsWith('image/') || /\.(jpe?g|png|gif|webp|heic)$/.test(name)) return 'image';
  return 'other';
}

function extractJobGroupKey(filename = '') {
  const base = String(filename).replace(/\.[^.]+$/, '');
  const dateMatch = base.match(/(20\d{2}[-_.]?\d{2}[-_.]?\d{2})/);
  if (dateMatch) {
    const tail = base.replace(dateMatch[1], '').replace(/^[-_.\s]+/, '').split(/[-_.\s]+/)[0];
    if (tail && tail.length >= 3) return `${dateMatch[1]}:${tail.slice(0, 24).toLowerCase()}`;
    return dateMatch[1];
  }
  const prefix = base.split(/[-_.\s]+/).slice(0, 2).join('-').toLowerCase();
  return prefix || null;
}

function extractVisualHints(filename = '') {
  const lower = String(filename).toLowerCase();
  const hints = {};
  if (/(?:^|[_\-\s.])before(?:[_\-\s.]|$)/.test(lower)) hints.possibleBeforeAfterRole = 'before_candidate';
  if (/(?:^|[_\-\s.])after(?:[_\-\s.]|$)/.test(lower)) hints.possibleBeforeAfterRole = 'after_candidate';
  if (/\b(ba|b[-_]a)\b/.test(lower)) hints.possibleBeforeAfterSet = true;
  if (/\bcarousel\b|\bseries\b/.test(lower)) hints.possibleMultiPhotoSet = true;
  hints.source = 'filename_only';
  hints.confidence = 'low';
  return hints;
}

function findBeforeAfterPairs(assets = []) {
  const groups = new Map();
  for (const asset of assets) {
    if (!asset.jobGroupKey) continue;
    if (!groups.has(asset.jobGroupKey)) groups.set(asset.jobGroupKey, []);
    groups.get(asset.jobGroupKey).push(asset);
  }
  const pairs = [];
  for (const [key, rows] of groups) {
    const before = rows.find((r) => r.visualHints?.possibleBeforeAfterRole === 'before_candidate');
    const after = rows.find((r) => r.visualHints?.possibleBeforeAfterRole === 'after_candidate');
    if (before && after) {
      pairs.push({ jobGroupKey: key, beforeId: before.id, afterId: after.id, evidence: 'filename_only' });
    }
  }
  return pairs;
}

module.exports = {
  inferMediaKind,
  extractJobGroupKey,
  extractVisualHints,
  findBeforeAfterPairs,
};
