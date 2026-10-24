'use strict';

const { ANCHOR_CLIENT_ID, BACKLOG_APPROVAL_STATES, categoryToLegacyContentType } = require('./types');
const { isAnchorOrganicSocialEnabled } = require('./config');
const { syncAnchorMediaLibrary } = require('./mediaSync');
const { maintainAnchorContentBacklog, planBacklogCandidate } = require('./planner');

async function runAnchorOrganicSocialCycle(input = {}, deps = {}) {
  const clientId = Number(input.clientId || ANCHOR_CLIENT_ID);
  if (clientId !== ANCHOR_CLIENT_ID) throw new Error('anchor_client_required');
  if (!isAnchorOrganicSocialEnabled(clientId, input.clientConfig || {})) {
    return { skipped: true, reason: 'anchor_organic_disabled', clientId };
  }
  const store = deps.store;
  if (!store) throw new Error('store_required');
  await store.ensureSchema();

  const media = input.skipMediaSync
    ? { discoveredCount: 0, assets: await store.listMediaAssets(clientId) }
    : await syncAnchorMediaLibrary({ clientId, clientConfig: input.clientConfig, folderId: input.folderId }, deps);

  const backlog = await maintainAnchorContentBacklog({ clientId, minSize: input.minBacklog || undefined }, deps);
  return {
    spec: 'SPEC-PAIGE-ANCHOR-SOCIAL-002',
    clientId,
    mediaDiscovered: media.discoveredCount,
    assetInventorySize: (media.assets || []).length,
    backlogSize: backlog.backlogSize,
    backlogCreated: backlog.createdCount,
    backlogItems: backlog.items,
  };
}

async function selectNextBacklogItemForGeneration(input = {}, deps = {}) {
  const clientId = Number(input.clientId || ANCHOR_CLIENT_ID);
  const store = deps.store;
  const platform = input.platform || input.channel;
  const assets = deps.assets || await store.listMediaAssets(clientId);
  const items = await store.listBacklog(clientId, { approvalState: BACKLOG_APPROVAL_STATES.DRAFT });
  const filtered = platform ? items.filter((i) => i.targetPlatform === platform) : items;
  const pick = filtered.sort((a, b) => String(a.proposedPublishAt).localeCompare(String(b.proposedPublishAt)))[0]
    || items[0]
    || null;
  if (!pick) return null;
  return {
    backlogId: pick.id,
    organicPlan: buildOrganicPlanFromBacklog(pick, { assets }),
    backlogItem: pick,
  };
}

function buildOrganicPlanFromBacklog(item, deps = {}) {
  const assets = deps.assets || [];
  const selectedAssets = (item.assetIds || [])
    .map((id) => assets.find((a) => a.id === id))
    .filter(Boolean);
  return {
    storyConcept: item.storyConcept,
    contentCategory: item.contentCategory,
    legacyContentType: categoryToLegacyContentType(item.contentCategory),
    targetPlatform: item.targetPlatform,
    proposedFormat: item.proposedFormat,
    assetIds: item.assetIds || [],
    assets: selectedAssets.map((a) => ({
      id: a.id,
      driveFileId: a.driveFileId,
      filename: a.filename,
      webViewLink: a.webViewLink,
      visualHints: a.visualHints,
    })),
    proposedPublishAt: item.proposedPublishAt,
    schedulingRationale: item.schedulingRationale,
    planningRationale: item.planningRationale,
    exploration: item.exploration,
    assetTruthfulness: 'Do not infer customer, property, location, service performed, results, or before/after relationships from imagery.',
  };
}

async function materializeBacklogIntoGeneration(input = {}, deps = {}) {
  let selection = await selectNextBacklogItemForGeneration(input, deps);
  if (!selection) {
    const planned = await planBacklogCandidate({ clientId: input.clientId, platform: input.platform }, deps);
    const row = await deps.store.insertBacklogItem(planned);
    const assets = deps.assets || await deps.store.listMediaAssets(input.clientId);
    selection = {
      backlogId: row.id,
      organicPlan: buildOrganicPlanFromBacklog(row, { assets }),
      backlogItem: row,
    };
  }
  return selection;
}

module.exports = {
  runAnchorOrganicSocialCycle,
  selectNextBacklogItemForGeneration,
  buildOrganicPlanFromBacklog,
  materializeBacklogIntoGeneration,
};
