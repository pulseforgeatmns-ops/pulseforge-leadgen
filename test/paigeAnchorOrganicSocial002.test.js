'use strict';

const { describe, test } = require('node:test');
const assert = require('node:assert/strict');
const {
  ANCHOR_CLIENT_ID,
  CONTENT_FORMATS,
  createMemoryOrganicSocialStore,
  syncAnchorMediaLibrary,
  runAnchorOrganicSocialCycle,
  maintainAnchorContentBacklog,
  recommendPublishWindow,
  ingestPublicationPerformance,
  findBeforeAfterPairs,
} = require('../packages/capabilities/paigeOrganicSocial');

function mockDrive(files) {
  let page = 0;
  return async () => {
    if (page > 0) return { files: [], nextPageToken: null };
    page += 1;
    return { files, nextPageToken: null };
  };
}

describe('SPEC-PAIGE-ANCHOR-SOCIAL-002 acceptance', () => {
  test('full adaptive organic cycle with media, backlog, scheduling, learning, and approval boundary', async () => {
    const store = createMemoryOrganicSocialStore();
    const listDriveFiles = mockDrive([
      {
        id: 'drive-before-1',
        name: '2026-07-01_manchester-office_before.jpg',
        mimeType: 'image/jpeg',
        size: '120000',
        createdTime: '2026-07-01T10:00:00.000Z',
        modifiedTime: '2026-07-01T10:00:00.000Z',
      },
      {
        id: 'drive-after-1',
        name: '2026-07-01_manchester-office_after.jpg',
        mimeType: 'image/jpeg',
        size: '130000',
        createdTime: '2026-07-01T10:05:00.000Z',
        modifiedTime: '2026-07-01T10:05:00.000Z',
      },
      {
        id: 'drive-new-2',
        name: '2026-10-07_operator_note.jpg',
        mimeType: 'image/jpeg',
        size: '90000',
        createdTime: '2026-10-07T09:00:00.000Z',
        modifiedTime: '2026-10-07T09:00:00.000Z',
      },
    ]);

    const firstSync = await syncAnchorMediaLibrary({ clientId: ANCHOR_CLIENT_ID }, { store, listDriveFiles });
    assert.equal(firstSync.discoveredCount, 3);
    assert.ok(firstSync.assets.length >= 3);

    const secondSync = await syncAnchorMediaLibrary({ clientId: ANCHOR_CLIENT_ID }, {
      store,
      listDriveFiles: mockDrive([
        {
          id: 'drive-new-3',
          name: '2026-10-07_new_dump.jpg',
          mimeType: 'image/jpeg',
          size: '80000',
          createdTime: '2026-10-07T12:00:00.000Z',
          modifiedTime: '2026-10-07T12:00:00.000Z',
        },
      ]),
    });
    assert.equal(secondSync.discoveredCount, 1);
    const inventory = await store.listMediaAssets(ANCHOR_CLIENT_ID);
    assert.equal(inventory.length, 4);

    const pairs = findBeforeAfterPairs(inventory);
    assert.equal(pairs.length, 1);

    const cycle = await runAnchorOrganicSocialCycle({
      clientId: ANCHOR_CLIENT_ID,
      skipMediaSync: true,
      clientConfig: { metadata: { paige: { organicSocial: { enabled: true } } } },
    }, { store });
    assert.equal(cycle.spec, 'SPEC-PAIGE-ANCHOR-SOCIAL-002');
    assert.ok(cycle.backlogSize >= 6);

    const backlog = await store.listBacklog(ANCHOR_CLIENT_ID);
    const formats = new Set(backlog.map((b) => b.proposedFormat));
    assert.ok(formats.size >= 2, 'backlog should vary formats');
    assert.ok([...formats].every((f) => CONTENT_FORMATS.includes(f)));

    const textOnly = backlog.find((b) => b.proposedFormat === 'text_only');
    assert.ok(textOnly, 'expected at least one intentional text-only backlog item');
    assert.equal(textOnly.assetIds.length, 0);
    assert.match(textOnly.storyConcept, /text-only/i);

    const imagePost = backlog.find((b) => b.assetIds.length > 0);
    assert.ok(imagePost, 'expected image-backed backlog item when media exists');
    assert.match(imagePost.storyConcept, /do not infer/i);

    const platforms = new Set(backlog.map((b) => b.targetPlatform));
    assert.ok(platforms.size >= 2, 'platform-specific planning');

    for (const item of backlog.slice(0, 5)) {
      assert.ok(item.schedulingRationale);
      assert.doesNotMatch(item.schedulingRationale, /9:00 AM every day/i);
      assert.doesNotMatch(item.proposedPublishAt, /T09:00:00\.000Z$/);
    }

    await store.upsertPlatformSignal({
      clientId: ANCHOR_CLIENT_ID,
      platform: 'linkedin_page',
      contentCategory: 'operator_perspective',
      format: 'text_only',
      dow: 2,
      hourLocal: 8,
      sampleCount: 3,
      impressionsSum: 3000,
      engagementSum: 360,
      engagementRateAvg: 0.12,
      lastObservedAt: '2026-09-30T12:15:00.000Z',
    });

    const signals = await store.listPlatformSignals(ANCHOR_CLIENT_ID, 'linkedin_page');
    assert.ok(signals.length >= 1);

    const informed = recommendPublishWindow({
      platform: 'linkedin_page',
      contentCategory: 'operator_perspective',
      format: 'text_only',
      exploration: false,
      now: new Date('2026-10-01T12:00:00.000Z'),
    }, { signals });
    assert.match(informed.schedulingRationale, /Anchor's recent|performed better/i);

    const explore = recommendPublishWindow({
      platform: 'facebook_page',
      contentCategory: 'useful_expertise',
      format: 'single_photo',
      exploration: true,
      now: new Date('2026-10-01T12:00:00.000Z'),
    }, { signals: [] });
    assert.match(explore.schedulingRationale, /experiment|prior|recommending/i);

    const explorationItems = backlog.filter((b) => b.exploration);
    assert.ok(explorationItems.length >= 1, 'maintains controlled experimentation');

    assert.ok(backlog.every((b) => ['draft', 'pending_approval'].includes(b.approvalState) || b.approvalState === 'draft'));

    const refreshed = await maintainAnchorContentBacklog({ clientId: ANCHOR_CLIENT_ID, minSize: cycle.backlogSize + 1 }, { store });
    assert.ok(refreshed.createdCount >= 1, 'new media expands future backlog automatically');
  });
});
