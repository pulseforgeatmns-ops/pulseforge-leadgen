'use strict';

const assert = require('node:assert/strict');
const { test, describe, beforeEach } = require('node:test');

const {
  createSocialContentCapability,
  createSocialContentApprovalService,
  createSocialContentPublishCapability,
  createInMemorySocialContentStore,
  APPROVAL_STATES,
  PUBLISH_STATES,
  buildSocialContentArtifact,
} = require('../packages/capabilities/contentGeneration');
const {
  resetPaigeSocialContentApprovalForTests,
} = require('../services/paigeSocialContentApproval');
const {
  resetPaigeSocialContentPublicationForTests,
} = require('../services/paigeSocialContentPublication');
const {
  inspectPaigeSocialContentStatus,
  STATUS_KINDS,
  resetPaigeSocialContentInspectionForTests,
} = require('../services/paigeSocialContentInspection');

const ANCHOR_BODY = `We work with professional offices across Greater Manchester — Manchester, Bedford, and the surrounding towns.

When a practice manager asks for a facilities assessment, we put scope, access instructions, and follow-through in writing before any work starts. Anchor Cleaning is owner-operated, and one accountable team handles the account from first visit through consistent follow-through.`;

const BANNED_PHRASES = [
  "That's how I think about",
  'That loop is becoming more interesting',
  'walkthrough',
];

function assertAnchorBrandConstraints(body) {
  for (const phrase of BANNED_PHRASES) {
    assert.doesNotMatch(body, new RegExp(phrase, 'i'), `body must not contain "${phrase}"`);
  }
  assert.match(body, /Manchester|Bedford|Greater Manchester/i);
  assert.match(body, /facilities assessment/i);
}

describe('Paige canonical social content lifecycle', () => {
  beforeEach(() => {
    resetPaigeSocialContentApprovalForTests();
    resetPaigeSocialContentPublicationForTests();
    resetPaigeSocialContentInspectionForTests();
  });

  test('pending → approved sets approvedAt', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-pending',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Dialogue',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-pending',
    })]);

    const result = await approval.recordDecision({
      tenantId: '10',
      clientId: 10,
      artifactId: 'art-pending',
      decision: 'approve',
    });

    assert.equal(result.artifact.approvalState, APPROVAL_STATES.APPROVED);
    assert.ok(result.artifact.approvedAt);
    assert.equal(result.artifact.publishState, PUBLISH_STATES.NOT_PUBLISHED);
  });

  test('pending → rejected stores rejection reason', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-reject',
      tenantId: '10',
      clientId: 10,
      platform: 'facebook_page',
      label: 'Facebook Page · Educational',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-reject',
    })]);

    const result = await approval.recordDecision({
      tenantId: '10',
      clientId: 10,
      artifactId: 'art-reject',
      decision: 'reject',
      rejectionReason: 'Tone too generic',
    });

    assert.equal(result.artifact.approvalState, APPROVAL_STATES.REJECTED);
    assert.equal(result.artifact.rejectionReason, 'Tone too generic');
    assert.ok(result.artifact.rejectedAt);
  });

  test('approved → approved is idempotent', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-idem',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-idem',
      approvalState: APPROVAL_STATES.APPROVED,
      approvedAt: '2026-09-18T00:00:00.000Z',
    })]);

    const result = await approval.recordDecision({
      tenantId: '10',
      clientId: 10,
      artifactId: 'art-idem',
      decision: 'approve',
    });
    assert.equal(result.idempotent, true);
    assert.equal(result.artifact.approvalState, APPROVAL_STATES.APPROVED);
  });

  test('rejected → rejected is idempotent', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-rej-idem',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-rej-idem',
      approvalState: APPROVAL_STATES.REJECTED,
      rejectedAt: '2026-09-18T00:00:00.000Z',
    })]);

    const result = await approval.recordDecision({
      tenantId: '10',
      clientId: 10,
      artifactId: 'art-rej-idem',
      decision: 'reject',
    });
    assert.equal(result.idempotent, true);
  });

  test('rejected → approve is blocked without restore', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-blocked',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-blocked',
      approvalState: APPROVAL_STATES.REJECTED,
    })]);

    await assert.rejects(
      () => approval.recordDecision({
        tenantId: '10',
        clientId: 10,
        artifactId: 'art-blocked',
        decision: 'approve',
      }),
      (err) => err.code === 'rejected_artifact_requires_restore'
    );
  });

  test('published → reject is blocked', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-pub',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-pub',
      approvalState: APPROVAL_STATES.APPROVED,
      publishState: PUBLISH_STATES.PUBLISHED,
      publishedAt: '2026-09-18T00:00:00.000Z',
    })]);

    await assert.rejects(
      () => approval.recordDecision({
        tenantId: '10',
        clientId: 10,
        artifactId: 'art-pub',
        decision: 'reject',
      }),
      (err) => err.code === 'cannot_reject_published_artifact'
    );
  });

  test('PENDING_APPROVAL cannot publish', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publicationService: {
        publishApprovedArtifact: async () => ({ success: true, externalPlatform: 'linkedin_page' }),
      },
    });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-unapproved',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-unapproved',
    })]);

    const result = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: 'art-unapproved' },
    });
    assert.equal(result.status, 'failed');
    assert.equal(result.errors[0].message, 'publication_requires_approved_artifact');
  });

  test('REJECTED cannot publish', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentPublishCapability({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-rejected-pub',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-rejected-pub',
      approvalState: APPROVAL_STATES.REJECTED,
    })]);

    const result = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: 'art-rejected-pub' },
    });
    assert.equal(result.errors[0].message, 'publication_requires_approved_artifact');
  });

  test('approved publish sets publishState PUBLISHED without duplicate posts', async () => {
    const store = createInMemorySocialContentStore();
    let publishCalls = 0;
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publicationService: {
        publishApprovedArtifact: async () => {
          publishCalls += 1;
          return {
            success: true,
            externalPlatform: 'linkedin_page',
            externalPostId: 'buf-999',
            externalUrl: 'https://linkedin.com/feed/update/999',
          };
        },
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-publish',
      tenantId: '10',
      clientId: 10,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Dialogue',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-publish',
      approvalState: APPROVAL_STATES.APPROVED,
    })]);

    const first = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: 'art-publish' },
    });
    assert.equal(first.status, 'completed');
    assert.equal(publishCalls, 1);

    const row = await store.getById('art-publish', '10', 10);
    assert.equal(row.publishState, PUBLISH_STATES.PUBLISHED);
    assert.equal(row.approvalState, APPROVAL_STATES.APPROVED);
    assert.ok(row.publishedAt);
    assert.equal(row.publishedUrl, 'https://linkedin.com/feed/update/999');

    const second = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: 'art-publish' },
    });
    assert.equal(second.outputs.idempotent, true);
    assert.equal(publishCalls, 1);
  });

  test('publish failure records FAILED publishState and publishError', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publicationService: {
        publishApprovedArtifact: async () => ({
          success: false,
          errorCode: 'publish_failed',
          errorMessage: 'buffer_down',
        }),
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-fail',
      tenantId: '10',
      clientId: 10,
      platform: 'facebook_page',
      label: 'Facebook Page · Promo',
      body: ANCHOR_BODY,
      pendingCommentId: 'pc-fail',
      approvalState: APPROVAL_STATES.APPROVED,
    })]);

    const result = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: 'art-fail' },
    });
    assert.equal(result.status, 'failed');
    const row = await store.getById('art-fail', '10', 10);
    assert.equal(row.publishState, PUBLISH_STATES.FAILED);
    assert.equal(row.publishError, 'buffer_down');
    assert.equal(row.approvalState, APPROVAL_STATES.APPROVED);
  });

  test('Anchor Cleaning end-to-end lifecycle with brand constraints', async () => {
    const store = createInMemorySocialContentStore();
    const published = [];

    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        drafts: [{
          company: { name: 'Anchor Cleaning' },
          content: ANCHOR_BODY,
          contentType: 'educational',
          channel: 'linkedin_page',
          meta: { format: 'dialogue' },
        }],
      }),
      mirrorPendingComment: async () => 'pc-anchor',
    });

    const approval = createSocialContentApprovalService({
      socialContentStore: store,
      mirrorPendingComment: async () => ({ id: 'pc-anchor', status: 'approved' }),
    });

    const publishCap = createSocialContentPublishCapability({
      socialContentStore: store,
      publicationService: {
        publishApprovedArtifact: async ({ artifact }) => {
          published.push(artifact);
          return {
            success: true,
            externalPlatform: 'linkedin_page',
            externalPostId: 'anchor-post-1',
            externalUrl: 'https://linkedin.com/feed/update/anchor-post-1',
          };
        },
      },
    });

    const gen = await cap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { dryRun: false, invocationSource: 'test_anchor_lifecycle' },
    });
    assert.equal(gen.status, 'completed');
    const artifact = gen.outputs.artifacts[0];
    assert.equal(artifact.approvalState, APPROVAL_STATES.PENDING_APPROVAL);
    assert.equal(artifact.publishState, PUBLISH_STATES.NOT_PUBLISHED);
    assertAnchorBrandConstraints(artifact.body);

    const approved = await approval.recordDecision({
      tenantId: '10',
      clientId: 10,
      artifactId: artifact.id,
      decision: 'approve',
    });
    assert.equal(approved.artifact.approvalState, APPROVAL_STATES.APPROVED);

    const pub = await publishCap.execute({
      tenantId: '10',
      clientId: 10,
      inputs: { artifactId: artifact.id },
    });
    assert.equal(pub.status, 'completed');
    assert.equal(published.length, 1);

    const inspection = await inspectPaigeSocialContentStatus({
      tenantId: '10',
      clientId: 10,
      store,
    });
    assert.equal(inspection.statusKind, STATUS_KINDS.PUBLISHED);
    assert.match(inspection.narrative.summary, /published content/i);
    assert.equal(inspection.counts.published, 1);
  });

  test('inspection reports pending approval state', async () => {
    const store = createInMemorySocialContentStore();
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-wait',
      tenantId: '1',
      clientId: 1,
      platform: 'google_business',
      label: 'Google Business · Educational',
      body: 'Draft waiting',
      pendingCommentId: 'pc-wait',
    })]);

    const inspection = await inspectPaigeSocialContentStatus({
      tenantId: '1',
      clientId: 1,
      store,
    });
    assert.equal(inspection.statusKind, STATUS_KINDS.PENDING_APPROVAL);
    assert.equal(inspection.narrative.waitingOnOperator, true);
    assert.equal(inspection.pendingArtifacts.length, 1);
  });

  test('inspection reports no drafts when empty', async () => {
    const store = createInMemorySocialContentStore();
    const inspection = await inspectPaigeSocialContentStatus({
      tenantId: '5',
      clientId: 5,
      store,
    });
    assert.equal(inspection.statusKind, STATUS_KINDS.NO_DRAFTS);
    assert.match(inspection.narrative.summary, /not generated any social content/i);
  });

  test('applyPaigeSocialArtifactApprovalAction defaults publishOnApprove to false', () => {
    const fs = require('node:fs');
    const path = require('node:path');
    const source = fs.readFileSync(
      path.join(__dirname, '../services/paigeSocialContentApproval.js'),
      'utf8'
    );
    assert.match(source, /publishOnApprove:\s*input\.publishOnApprove\s*\?\?\s*false/);
  });
});
