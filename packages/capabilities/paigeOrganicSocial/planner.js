'use strict';

const crypto = require('crypto');
const {
  CONTENT_CATEGORIES,
  CONTENT_FORMATS,
  BACKLOG_APPROVAL_STATES,
  DEFAULT_BACKLOG_MIN,
  EXPLORATION_RATE,
  PLATFORM_CHANNELS,
} = require('./types');
const { findBeforeAfterPairs } = require('./assetHints');
const { recommendPublishWindow } = require('./scheduler');

const STORY_BANK = Object.freeze({
  work_and_results: [
    'Show how a written scope keeps recurring office cleaning predictable without inventing a specific client.',
    'Explain what changes when access instructions and off-limits rooms are documented up front.',
  ],
  useful_expertise: [
    'Call out one overlooked commercial office area that quietly drives rework when it is skipped.',
    'Explain why turnover weeks need a different checklist than steady-state maintenance.',
  ],
  operator_perspective: [
    'Share one lesson from building Anchor about owning follow-through instead of handing jobs off.',
    'Describe how Jacob thinks about saying no to work Anchor cannot document and deliver consistently.',
  ],
  social_proof: [
    'Only surface verified review language supplied by operators — never invent a customer reaction.',
    'Frame repeat commercial relationships as a systems outcome, not a named customer story.',
  ],
  anchor_progress: [
    'Note a real capability or process improvement Anchor has made recently without inventing metrics.',
    'Explain how Anchor is expanding media-backed storytelling while keeping claims conservative.',
  ],
  local_relevance: [
    'Connect Manchester-area office rhythm to why predictable cleaning windows matter for staff focus.',
    'Observe how seasonal weather shifts facility traffic patterns without naming a property.',
  ],
});

function hashPick(seed, list) {
  const idx = crypto.createHash('sha256').update(seed).digest()[0] % list.length;
  return list[idx];
}

function hashRatio(seed) {
  const digest = crypto.createHash('sha256').update(String(seed)).digest();
  return digest.readUInt32BE(0) / 0x100000000;
}

function shouldExplore(seed, rate = EXPLORATION_RATE) {
  return hashRatio(`${seed}:exploration`) < rate;
}

function computeExplorationBounds(backlogSize) {
  if (backlogSize <= 0) return { min: 0, max: 0 };
  const max = Math.max(1, Math.ceil(backlogSize * EXPLORATION_RATE));
  const min = backlogSize >= DEFAULT_BACKLOG_MIN ? 1 : 0;
  return { min: Math.min(min, max), max };
}

function recentCounts(backlog = []) {
  const formats = {};
  const categories = {};
  for (const item of backlog) {
    formats[item.proposedFormat] = (formats[item.proposedFormat] || 0) + 1;
    categories[item.contentCategory] = (categories[item.contentCategory] || 0) + 1;
  }
  return { formats, categories };
}

function chooseCategory(counts, seed) {
  const sorted = [...CONTENT_CATEGORIES].sort(
    (a, b) => (counts.categories[a] || 0) - (counts.categories[b] || 0)
  );
  return hashPick(`${seed}:category`, sorted);
}

function chooseFormat({ category, assets, pairs, counts, seed, forceTextOnly = false }) {
  if (forceTextOnly) return 'text_only';
  const underused = [...CONTENT_FORMATS].sort(
    (a, b) => (counts.formats[a] || 0) - (counts.formats[b] || 0)
  );
  if (category === 'operator_perspective' || category === 'useful_expertise') {
    if ((counts.formats.text_only || 0) <= (counts.formats.single_photo || 0)) return 'text_only';
  }
  if (pairs.length && (counts.formats.before_after || 0) <= 1) return 'before_after';
  const images = assets.filter((a) => a.mediaKind === 'image');
  if (images.length >= 3 && (counts.formats.carousel || 0) <= 1) return 'carousel';
  if (images.length >= 2 && (counts.formats.multi_photo || 0) <= 1) return 'multi_photo';
  const videos = assets.filter((a) => a.mediaKind === 'video');
  if (videos.length && (counts.formats.short_video || 0) <= 1) return 'short_video';
  if (images.length && (counts.formats.single_photo || 0) <= 2) return 'single_photo';
  return hashPick(`${seed}:format`, underused);
}

function selectAssets(format, assets, pairs, seed) {
  if (format === 'text_only') return [];
  const unused = assets.filter((a) => a.usageCount === 0);
  const pool = unused.length ? unused : assets;
  if (format === 'before_after' && pairs.length) {
    const pair = hashPick(`${seed}:pair`, pairs);
    return [pair.beforeId, pair.afterId];
  }
  if (format === 'multi_photo' || format === 'carousel') {
    const group = pool.filter((a) => a.jobGroupKey).reduce((map, a) => {
      if (!map.has(a.jobGroupKey)) map.set(a.jobGroupKey, []);
      map.get(a.jobGroupKey).push(a);
      return map;
    }, new Map());
    const best = [...group.values()].sort((a, b) => b.length - a.length)[0];
    if (best && best.length >= 2) return best.slice(0, format === 'carousel' ? 5 : 3).map((a) => a.id);
    return pool.slice(0, 3).map((a) => a.id);
  }
  if (format === 'short_video') {
    const video = pool.find((a) => a.mediaKind === 'video');
    return video ? [video.id] : [];
  }
  const image = pool.find((a) => a.mediaKind === 'image');
  return image ? [image.id] : [];
}

function buildStoryConcept(category, format, assetIds, assets) {
  const base = hashPick(`${category}:${format}`, STORY_BANK[category] || STORY_BANK.useful_expertise);
  if (format === 'text_only') {
    return `${base} Keep this text-only — do not attach a photo just to fill the slot.`;
  }
  if (!assetIds.length) {
    return `${base} No verified media context is available; stay descriptive and do not invent job details.`;
  }
  const names = assetIds
    .map((id) => assets.find((a) => a.id === id)?.filename)
    .filter(Boolean)
    .slice(0, 3);
  return `${base} Media filenames (${names.join(', ')}) may inform composition only — do not infer customer, property, service, or results from imagery.`;
}

function buildPlanningRationale({ category, format, assetIds, exploration }) {
  const parts = [
    `Story-first ${category.replace(/_/g, ' ')} idea for Anchor.`,
    `Format: ${format.replace(/_/g, ' ')}.`,
  ];
  if (format === 'text_only') parts.push('Deliberately no image — the idea stands on its own.');
  else if (assetIds.length) parts.push(`Selected ${assetIds.length} library asset(s) with conservative filename-only hints.`);
  else parts.push('No asset selected because none improved the story safely.');
  if (exploration) parts.push('Marked as controlled experimentation.');
  return parts.join(' ');
}

async function planBacklogCandidate(input = {}, deps = {}) {
  const clientId = Number(input.clientId);
  const store = deps.store;
  const assets = input.assets || await store.listMediaAssets(clientId);
  const pairs = findBeforeAfterPairs(assets);
  const backlog = input.backlog || await store.listBacklog(clientId);
  const counts = recentCounts(backlog);
  const seed = input.seed || `${clientId}:${backlog.length}:${assets.length}`;
  const exploration = input.exploration ?? shouldExplore(seed);
  const category = input.contentCategory || chooseCategory(counts, seed);
  const forceTextOnly = input.forceTextOnly || category === 'operator_perspective';
  const format = input.proposedFormat || chooseFormat({ category, assets, pairs, counts, seed, forceTextOnly });
  const assetIds = selectAssets(format, assets, pairs, seed);
  const storyConcept = buildStoryConcept(category, format, assetIds, assets);
  const platform = input.platform || hashPick(`${seed}:platform`, Object.values(PLATFORM_CHANNELS));
  const signals = deps.signals || await store.listPlatformSignals(clientId, platform);
  const schedule = recommendPublishWindow({
    platform,
    contentCategory: category,
    format,
    exploration,
    explorationSeed: `${seed}:schedule`,
  }, { signals });
  return {
    clientId,
    storyConcept,
    contentCategory: category,
    targetPlatform: platform,
    proposedFormat: format,
    assetIds,
    proposedPublishAt: schedule.proposedPublishAt,
    schedulingRationale: schedule.schedulingRationale,
    planningRationale: buildPlanningRationale({ category, format, assetIds, exploration }),
    approvalState: BACKLOG_APPROVAL_STATES.DRAFT,
    exploration,
    meta: {
      schedule: schedule.meta,
      assetTruthfulness: 'filename_hints_only',
    },
  };
}

async function applyExplorationPatch(store, clientId, item, exploration, deps = {}) {
  if (Boolean(item.exploration) === exploration) return item;
  const signals = deps.signals || await store.listPlatformSignals(clientId, item.targetPlatform);
  const schedule = recommendPublishWindow({
    platform: item.targetPlatform,
    contentCategory: item.contentCategory,
    format: item.proposedFormat,
    exploration,
    explorationSeed: `${item.id}:schedule`,
  }, { signals });
  const planningRationale = buildPlanningRationale({
    category: item.contentCategory,
    format: item.proposedFormat,
    assetIds: item.assetIds || [],
    exploration,
  });
  return store.updateBacklogItem(item.id, clientId, {
    exploration,
    planningRationale,
    schedulingRationale: schedule.schedulingRationale,
    meta: { ...(item.meta || {}), schedule: schedule.meta },
  });
}

async function rebalanceControlledExploration(items, clientId, deps = {}) {
  const store = deps.store;
  if (!store || !items.length) return items;
  const { min, max } = computeExplorationBounds(items.length);
  let explorers = items.filter((item) => item.exploration);
  if (explorers.length < min) {
    const need = min - explorers.length;
    const candidates = items
      .filter((item) => !item.exploration)
      .sort((a, b) => hashRatio(`promote:${clientId}:${a.id}`) - hashRatio(`promote:${clientId}:${b.id}`));
    for (const item of candidates.slice(0, need)) {
      await applyExplorationPatch(store, clientId, item, true, deps);
    }
  } else if (explorers.length > max) {
    const excess = explorers.length - max;
    explorers = [...explorers].sort(
      (a, b) => hashRatio(`demote:${clientId}:${a.id}`) - hashRatio(`demote:${clientId}:${b.id}`)
    );
    for (const item of explorers.slice(0, excess)) {
      await applyExplorationPatch(store, clientId, item, false, deps);
    }
  }
  return store.listBacklog(clientId);
}

async function maintainAnchorContentBacklog(input = {}, deps = {}) {
  const clientId = Number(input.clientId);
  const store = deps.store;
  const minSize = input.minSize || DEFAULT_BACKLOG_MIN;
  const existing = await store.listBacklog(clientId, { approvalState: BACKLOG_APPROVAL_STATES.DRAFT });
  const pending = await store.listBacklog(clientId, { approvalState: BACKLOG_APPROVAL_STATES.PENDING_APPROVAL });
  const active = [...existing, ...pending];
  const created = [];
  let i = 0;
  while (active.length + created.length < minSize) {
    const slot = active.length + i;
    const candidate = await planBacklogCandidate({ clientId, seed: `${clientId}:backlog-slot:${slot}` }, deps);
    const row = await store.insertBacklogItem(candidate);
    created.push(row);
    i += 1;
  }
  const combined = [...active, ...created];
  const hasTextOnly = combined.some((item) => item.proposedFormat === 'text_only');
  if (!hasTextOnly) {
    const textCandidate = await planBacklogCandidate({
      clientId,
      forceTextOnly: true,
      contentCategory: 'operator_perspective',
      proposedFormat: 'text_only',
      seed: `${clientId}:text-only`,
    }, deps);
    const row = await store.insertBacklogItem(textCandidate);
    created.push(row);
  }
  const draftItems = await store.listBacklog(clientId, { approvalState: BACKLOG_APPROVAL_STATES.DRAFT });
  const pendingItems = await store.listBacklog(clientId, { approvalState: BACKLOG_APPROVAL_STATES.PENDING_APPROVAL });
  await rebalanceControlledExploration([...draftItems, ...pendingItems], clientId, deps);
  return {
    backlogSize: active.length + created.length,
    createdCount: created.length,
    items: created,
  };
}

module.exports = {
  maintainAnchorContentBacklog,
  planBacklogCandidate,
  computeExplorationBounds,
  rebalanceControlledExploration,
  shouldExplore,
  STORY_BANK,
};
