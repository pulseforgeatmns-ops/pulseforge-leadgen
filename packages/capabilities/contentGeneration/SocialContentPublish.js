'use strict';

/**
 * SPEC-256 / SPEC-259A — Canonical Paige social content publication.
 * Requires an APPROVED tenant-scoped artifact; delegates to PublicationService + adapters.
 */

const crypto = require('crypto');
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
const {
  buildPendingCommentMirror,
  createPublicationService,
  createDefaultAdapterRegistry,
} = require('../contentPublication');

/**
 * @param {object} [deps]
 */
function createSocialContentPublishCapability(deps = {}) {
  const pool = deps.pool || null;
  const store =
    deps.socialContentStore ||
    (pool ? createPostgresSocialContentStore(pool) : createInMemorySocialContentStore());
  const adapterRegistry =
    deps.adapterRegistry || createDefaultAdapterRegistry(deps);
  const publicationService =
    deps.publicationService ||
    createPublicationService({
      adapterRegistry,
      credentialResolver: deps.credentialResolver,
    });

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

      if (!artifact.pendingCommentId) {
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
            channel: artifact.platform,
            adapter: artifact.platform,
          },
          duration: Date.now() - started,
        });
      }

      const publishResult = await publicationService.publishApprovedArtifact({
        artifact,
        tenantId,
        clientId,
        correlationId: crypto.randomUUID(),
      });

      if (!publishResult.success) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: {
            artifact,
            channel: artifact.platform,
            publishResult,
          },
          errors: [{
            message: publishResult.errorMessage || publishResult.errorCode || 'publish_failed',
            code: publishResult.errorCode || 'publish_failed',
          }],
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
          outputs: { artifact, channel: artifact.platform, publishResult },
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
          externalPostId: publishResult.externalPostId || null,
          externalUrl: publishResult.externalUrl || null,
          externalAccountId: publishResult.externalAccountId || null,
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
