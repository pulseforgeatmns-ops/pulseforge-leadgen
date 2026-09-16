'use strict';

/**
 * SPEC-256 — Canonical Paige social content publication.
 * Requires an APPROVED tenant-scoped artifact; never bypasses approval authority.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
} = require('../types');
const { APPROVAL_STATES, ARTIFACT_TYPE } = require('./types');
const {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
} = require('./SocialContentStore');
const { isPaigeSocialPublishChannel } = require('./channels');

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

/**
 * @param {object} [deps]
 */
function createSocialContentPublishCapability(deps = {}) {
  const pool = deps.pool || null;
  const store =
    deps.socialContentStore ||
    (pool ? createPostgresSocialContentStore(pool) : createInMemorySocialContentStore());
  const publishers =
    deps.publishers ||
    require('../../../utils/publishPipeline');
  const publishBlogPost = deps.publishBlogPost || require('../../../utils/blogPublisher').publishBlogPost;

  const publisherByChannel = {
    blog: publishBlogPost,
    google_business: publishers.publishToGoogleBusiness,
    facebook_page: publishers.publishToFacebookPage,
    linkedin_page: publishers.publishToLinkedInPage,
    linkedin_personal: publishers.publishToLinkedInPersonal,
  };

  return {
    id: BUILTIN_IDS.SOCIAL_CONTENT_PUBLISH,
    name: 'Social Content Publication',
    description:
      'Publish APPROVED Paige social content artifacts through canonical execution only',
    category: CAPABILITY_CATEGORIES.EXECUTION,
    outcomeTags: ['social_content_published'],
    version: 1,
    retryable: true,
    timeoutMs: 180_000,
    supportsRollback: false,
    idempotent: true,

    canRun(context) {
      try {
        const tenantId = String(context.tenantId || '').trim();
        const clientId = Number(context.clientId);
        return Boolean(tenantId && Number.isFinite(clientId) && tenantId === String(clientId));
      } catch (_) {
        return false;
      }
    },

    estimate() {
      return buildCapabilityEstimate({ durationMs: 20_000, confidence: 0.8 });
    },

    async execute(context) {
      const started = Date.now();
      const inputs = (context && context.inputs) || {};
      const tenantId = String(context.tenantId || inputs.tenantId || '').trim();
      const clientId = Number(context.clientId ?? inputs.clientId);
      const artifactId = inputs.artifactId || inputs.artifact_id;
      const dryRun = Boolean(inputs.dryRun ?? inputs.dry_run);

      if (!tenantId || !Number.isFinite(clientId) || tenantId !== String(clientId)) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'tenant_scope_required' }],
          duration: Date.now() - started,
        });
      }
      if (!artifactId) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'artifact_id_required' }],
          duration: Date.now() - started,
        });
      }

      if (pool && typeof pool.query === 'function') {
        await ensureSocialContentArtifactsTable(pool);
      }

      const artifact = await store.getById(artifactId, tenantId, clientId);
      if (!artifact) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'artifact_not_found' }],
          duration: Date.now() - started,
        });
      }

      if (artifact.approvalState === APPROVAL_STATES.PUBLISHED) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: { artifact, idempotent: true, published: true },
          artifacts: [{ type: ARTIFACT_TYPE, id: artifact.id, approvalState: artifact.approvalState }],
          duration: Date.now() - started,
        });
      }

      if (artifact.approvalState !== APPROVAL_STATES.APPROVED) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{
            message: 'publication_requires_approved_artifact',
            approvalState: artifact.approvalState,
          }],
          duration: Date.now() - started,
        });
      }

      if (!isPaigeSocialPublishChannel(artifact.platform)) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'unsupported_publish_channel', channel: artifact.platform }],
          duration: Date.now() - started,
        });
      }

      const publish = publisherByChannel[artifact.platform];
      if (!publish) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'publisher_not_configured', channel: artifact.platform }],
          duration: Date.now() - started,
        });
      }

      const pendingMirror = buildPendingCommentMirror(artifact);
      if (!pendingMirror.id) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'pending_comment_mirror_missing' }],
          duration: Date.now() - started,
        });
      }

      if (dryRun) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            artifact,
            dryRun: true,
            pendingMirror,
            channel: artifact.platform,
          },
          duration: Date.now() - started,
        });
      }

      try {
        await publish(pendingMirror);
      } catch (err) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: { artifact, channel: artifact.platform },
          errors: [{ message: err.message || 'publish_failed' }],
          duration: Date.now() - started,
        });
      }

      const published = await store.transitionApprovalState(
        artifact.id,
        tenantId,
        clientId,
        APPROVAL_STATES.PUBLISHED,
        { allowedFrom: [APPROVAL_STATES.APPROVED] }
      );

      if (!published.ok) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: { artifact, channel: artifact.platform },
          errors: [{ message: published.reason || 'publish_state_transition_failed' }],
          duration: Date.now() - started,
        });
      }

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          artifact: published.artifact,
          published: true,
          channel: artifact.platform,
          pendingCommentId: artifact.pendingCommentId,
        },
        artifacts: [{
          type: ARTIFACT_TYPE,
          id: published.artifact.id,
          approvalState: published.artifact.approvalState,
        }],
        duration: Date.now() - started,
      });
    },
  };
}

module.exports = {
  buildPendingCommentMirror,
  createSocialContentPublishCapability,
};
