'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test, describe, beforeEach } = require('node:test');

const {
  createSocialContentCapability,
  createSocialContentApprovalService,
  createSocialContentPublishCapability,
  createInMemorySocialContentStore,
  APPROVAL_STATES,
  buildSocialContentArtifact,
} = require('../packages/capabilities/contentGeneration');
const {
  routePaigeSocialContentApproval,
  resetPaigeSocialContentApprovalForTests,
} = require('../services/paigeSocialContentApproval');
const {
  routePaigeSocialContentPublication,
  resetPaigeSocialContentPublicationForTests,
} = require('../services/paigeSocialContentPublication');
const { applyPendingCommentApprovalAction } = require('../services/paigeSocialContentApprovalFlow');
const { BUILTIN_IDS } = require('../packages/capabilities/types');

describe('SPEC-256 — Canonical Paige Social Content Approval', () => {
  beforeEach(() => {
    resetPaigeSocialContentApprovalForTests();
    resetPaigeSocialContentPublicationForTests();
  });

  test('approval handlers route through canonical approval flow module', () => {
    const apiSource = fs.readFileSync(path.join(__dirname, '../routes/api.js'), 'utf8');
    const clientSource = fs.readFileSync(path.join(__dirname, '../routes/client.js'), 'utf8');
    const approvalsSource = fs.readFileSync(path.join(__dirname, '../routes/approvals.js'), 'utf8');

    assert.match(apiSource, /applyPendingCommentApprovalAction/);
    assert.match(clientSource, /applyPendingCommentApprovalAction/);
    assert.match(approvalsSource, /applyPendingCommentApprovalAction/);
    assert.doesNotMatch(apiSource, /publishToLinkedInPage\(item\)/);
    assert.doesNotMatch(clientSource, /publishToFacebookPage\(item\)/);
  });

  test('approve records APPROVED on canonical artifact and mirrors pending_comments', async () => {
    const store = createInMemorySocialContentStore();
    const mirrored = [];
    const approval = createSocialContentApprovalService({
      socialContentStore: store,
      mirrorPendingComment: async (pendingCommentId, status) => {
        mirrored.push({ pendingCommentId, status });
        return { id: pendingCommentId, status };
      },
    });

    const artifact = buildSocialContentArtifact({
      id: 'art-1',
      tenantId: '1',
      clientId: 1,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Dialogue',
      body: 'Draft body',
      pendingCommentId: 'pc-1',
      approvalState: APPROVAL_STATES.PENDING_APPROVAL,
    });
    await store.insertBatch([artifact]);

    const result = await approval.recordDecision({
      tenantId: '1',
      clientId: 1,
      artifactId: 'art-1',
      decision: 'approve',
    });

    assert.equal(result.ok, true);
    assert.equal(result.artifact.approvalState, APPROVAL_STATES.APPROVED);
    assert.deepEqual(mirrored, [{ pendingCommentId: 'pc-1', status: 'approved' }]);
  });

  test('reject records REJECTED without publication eligibility', async () => {
    const store = createInMemorySocialContentStore();
    const approval = createSocialContentApprovalService({ socialContentStore: store });
    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-2',
      tenantId: '2',
      clientId: 2,
      platform: 'facebook_page',
      label: 'Facebook Page · Promotional',
      body: 'Body',
      pendingCommentId: 'pc-2',
    })]);

    const result = await approval.recordDecision({
      tenantId: '2',
      clientId: 2,
      artifactId: 'art-2',
      decision: 'reject',
    });
    assert.equal(result.artifact.approvalState, APPROVAL_STATES.REJECTED);
  });

  test('publication fails closed when artifact is still PENDING_APPROVAL', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publishers: {
        publishToLinkedInPage: async () => {},
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-3',
      tenantId: '3',
      clientId: 3,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: 'Body',
      pendingCommentId: 'pc-3',
    })]);

    const result = await cap.execute({
      tenantId: '3',
      clientId: 3,
      inputs: { artifactId: 'art-3' },
    });

    assert.equal(result.status, 'failed');
    assert.equal(result.errors[0].message, 'publication_requires_approved_artifact');
    const row = await store.getById('art-3', '3', 3);
    assert.equal(row.approvalState, APPROVAL_STATES.PENDING_APPROVAL);
  });

  test('publication requires APPROVED artifact and marks PUBLISHED on success', async () => {
    const store = createInMemorySocialContentStore();
    const published = [];
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publishers: {
        publishToLinkedInPage: async (item) => {
          published.push(item);
        },
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-4',
      tenantId: '4',
      clientId: 4,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Numbers',
      body: 'POST: Hello world',
      pendingCommentId: 'pc-4',
      approvalState: APPROVAL_STATES.APPROVED,
    })]);

    const result = await cap.execute({
      tenantId: '4',
      clientId: 4,
      inputs: { artifactId: 'art-4' },
    });

    assert.equal(result.status, 'completed');
    assert.equal(published.length, 1);
    assert.equal(published[0].id, 'pc-4');
    const row = await store.getById('art-4', '4', 4);
    assert.equal(row.approvalState, APPROVAL_STATES.PUBLISHED);
  });

  test('approval router chains canonical publication after approve', async () => {
    const store = createInMemorySocialContentStore();
    const published = [];

    const approvalService = createSocialContentApprovalService({
      socialContentStore: store,
      mirrorPendingComment: async () => ({ id: 'pc-5', status: 'approved' }),
    });
    const publishCap = createSocialContentPublishCapability({
      socialContentStore: store,
      publishers: {
        publishToGoogleBusiness: async (item) => published.push(item),
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-5',
      tenantId: '5',
      clientId: 5,
      platform: 'google_business',
      label: 'Google Business · Educational',
      body: 'GBP post',
      pendingCommentId: 'pc-5',
    })]);

    const { createCapabilityRunner, createCapabilityRegistry } = require('../packages/capabilities');
    const registry = createCapabilityRegistry();
    registry.register(publishCap);
    const runner = createCapabilityRunner({ registry });

    const approval = await approvalService.recordDecision({
      tenantId: '5',
      clientId: 5,
      artifactId: 'art-5',
      decision: 'approve',
    });
    assert.equal(approval.artifact.approvalState, APPROVAL_STATES.APPROVED);

    const pub = await runner.run({
      capabilityId: BUILTIN_IDS.SOCIAL_CONTENT_PUBLISH,
      context: {
        tenantId: '5',
        clientId: 5,
        inputs: { artifactId: 'art-5' },
      },
    });
    assert.equal(pub.result.status, 'completed');
    assert.equal(published.length, 1);
    assert.equal((await store.getById('art-5', '5', 5)).approvalState, APPROVAL_STATES.PUBLISHED);
  });

  test('Paige social channel without canonical artifact is rejected fail-closed', async () => {
    const originalQuery = require('../db').query;
    require('../db').query = async (sql, params) => {
      if (sql.includes('FROM pending_comments')) {
        return {
          rows: [{
            id: params[0],
            client_id: params[1],
            status: 'pending',
            channel: 'linkedin_page',
            comment: 'Body',
          }],
        };
      }
      if (sql.includes('paige_social_content_artifacts')) {
        return { rows: [] };
      }
      return originalQuery(sql, params);
    };

    try {
      const result = await applyPendingCommentApprovalAction({
        clientId: 1,
        pendingCommentId: 'missing-artifact',
        action: 'approved',
      });
      assert.equal(result.ok, false);
      assert.equal(result.mode, 'canonical_required');
      assert.equal(result.statusCode, 409);
    } finally {
      require('../db').query = originalQuery;
    }
  });

  test('generation capability remains draft-only after approval boundary', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        drafts: [{
          company: { name: 'Acme' },
          content: 'Draft',
          contentType: 'educational',
          channel: 'linkedin_page',
          meta: { format: 'dialogue' },
        }],
      }),
      mirrorPendingComment: async () => 'pc-gen',
    });

    const result = await cap.execute({
      tenantId: '7',
      clientId: 7,
      inputs: { dryRun: false },
    });
    assert.equal(result.status, 'completed');
    const artifacts = await store.listByTenant('7', 7);
    assert.equal(artifacts[0].approvalState, APPROVAL_STATES.PENDING_APPROVAL);
  });
});
