'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test, describe } = require('node:test');

const {
  PlatformAdapterRegistry,
  createPublicationService,
  createLegacyPlatformAdapter,
  createDefaultAdapterRegistry,
  buildPublishSuccess,
} = require('../packages/capabilities/contentPublication');
const {
  createSocialContentPublishCapability,
  createInMemorySocialContentStore,
  APPROVAL_STATES,
  PUBLISH_STATES,
  buildSocialContentArtifact,
} = require('../packages/capabilities/contentGeneration');

describe('SPEC-259A — Publication adapter boundary', () => {
  test('SocialContentPublish does not import publishPipeline directly', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '../packages/capabilities/contentGeneration/SocialContentPublish.js'),
      'utf8'
    );
    assert.doesNotMatch(source, /require\(['"].*publishPipeline/);
    assert.doesNotMatch(source, /require\(['"].*blogPublisher/);
    assert.match(source, /createPublicationService/);
    assert.match(source, /createDefaultAdapterRegistry/);
  });

  test('registry resolves adapter by artifact platform', () => {
    const registry = new PlatformAdapterRegistry();
    const calls = [];
    registry.register(createLegacyPlatformAdapter('linkedin_page', async () => {
      calls.push('linkedin');
      return buildPublishSuccess({ externalPlatform: 'linkedin_page', externalPostId: 'x' });
    }));

    const adapter = registry.resolve('linkedin_page');
    assert.equal(adapter.platform, 'linkedin_page');
    assert.throws(() => registry.resolve('unknown'), /unsupported_platform/);
  });

  test('duplicate platform registration throws', () => {
    const registry = new PlatformAdapterRegistry();
    registry.register(createLegacyPlatformAdapter('blog', async () => buildPublishSuccess({ externalPlatform: 'blog' })));
    assert.throws(
      () => registry.register(createLegacyPlatformAdapter('blog', async () => buildPublishSuccess({ externalPlatform: 'blog' }))),
      /adapter_already_registered/
    );
  });

  test('default registry registers all Paige publish platforms', () => {
    const registry = createDefaultAdapterRegistry({
      publishers: {
        publishToGoogleBusiness: async () => ({ success: true, externalPlatform: 'google_business' }),
        publishToFacebookPage: async () => ({ success: true, externalPlatform: 'facebook_page' }),
        publishToLinkedInPage: async () => ({ success: true, externalPlatform: 'linkedin_page' }),
        publishToLinkedInPersonal: async () => ({ success: true, externalPlatform: 'linkedin_personal' }),
      },
      publishBlogPost: async () => ({ success: true, externalPlatform: 'blog' }),
    });
    const platforms = registry.listPlatforms();
    assert.deepEqual(platforms, [
      'blog',
      'facebook_page',
      'google_business',
      'linkedin_page',
      'linkedin_personal',
    ]);
  });

  test('adapter receives canonical artifact not pending mirror at service boundary', async () => {
    const store = createInMemorySocialContentStore();
    const received = [];
    const registry = new PlatformAdapterRegistry();
    registry.register({
      platform: 'linkedin_page',
      version: '1.0.0',
      validateCredentials: () => ({ ok: true }),
      async publish({ artifact }) {
        received.push(artifact);
        return buildPublishSuccess({
          externalPlatform: 'linkedin_page',
          externalPostId: 'post-1',
        });
      },
    });

    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      adapterRegistry: registry,
      credentialResolver: () => ({ ok: true, credentials: { platform: 'linkedin_page' } }),
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-a',
      tenantId: '1',
      clientId: 1,
      platform: 'linkedin_page',
      label: 'LinkedIn Page · Punch',
      body: 'Hello',
      pendingCommentId: 'pc-a',
      approvalState: APPROVAL_STATES.APPROVED,
    })]);

    const result = await cap.execute({
      tenantId: '1',
      clientId: 1,
      inputs: { artifactId: 'art-a' },
    });

    assert.equal(result.status, 'completed');
    assert.equal(received.length, 1);
    assert.equal(received[0].id, 'art-a');
    assert.equal(received[0].body, 'Hello');
    assert.equal(received[0].platform, 'linkedin_page');
    assert.equal(result.outputs.externalPostId, 'post-1');
  });

  test('publication service fails closed on unsupported platform', async () => {
    const registry = new PlatformAdapterRegistry();
    const service = createPublicationService({ adapterRegistry: registry });
    const result = await service.publishApprovedArtifact({
      tenantId: '1',
      clientId: 1,
      artifact: buildSocialContentArtifact({
        id: 'art-b',
        tenantId: '1',
        clientId: 1,
        platform: 'linkedin_page',
        label: 'Test',
        body: 'Body',
        pendingCommentId: 'pc-b',
      }),
    });
    assert.equal(result.success, false);
    assert.match(result.errorMessage, /unsupported_platform/);
  });

  test('adapter publish failure prevents PUBLISHED transition', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentPublishCapability({
      socialContentStore: store,
      publicationService: {
        publishApprovedArtifact: async () => ({
          success: false,
          errorCode: 'publish_failed',
          errorMessage: 'buffer_down',
          retryable: true,
        }),
      },
    });

    await store.insertBatch([buildSocialContentArtifact({
      id: 'art-c',
      tenantId: '2',
      clientId: 2,
      platform: 'facebook_page',
      label: 'Facebook · Promo',
      body: 'Body',
      pendingCommentId: 'pc-c',
      approvalState: APPROVAL_STATES.APPROVED,
    })]);

    const result = await cap.execute({
      tenantId: '2',
      clientId: 2,
      inputs: { artifactId: 'art-c' },
    });

    assert.equal(result.status, 'failed');
    assert.equal(result.errors[0].message, 'buffer_down');
    const row = await store.getById('art-c', '2', 2);
    assert.equal(row.approvalState, APPROVAL_STATES.APPROVED);
    assert.equal(row.publishState, PUBLISH_STATES.FAILED);
    assert.equal(row.publishError, 'buffer_down');
  });
});
