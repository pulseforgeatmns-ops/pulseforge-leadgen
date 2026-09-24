'use strict';

/**
 * SPEC-256 / SPEC-259A — Canonical Paige social content publication.
 * Requires an APPROVED tenant-scoped artifact; delegates to PublicationService + adapters.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
} = require('../types');
const {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
} = require('./SocialContentStore');
const {
  buildPendingCommentMirror,
  createPublicationService,
} = require('../contentPublication');

/**
 * @param {object} [deps]
 */
function createSocialContentPublishCapability(deps = {}) {
  const pool = deps.pool || null;
  const store =
    deps.socialContentStore ||
    (pool ? createPostgresSocialContentStore(pool) : createInMemorySocialContentStore());
  const { createSocialAdapterRegistry } = require('../contentPublication/adapters/SocialPlatformAdapters');
  const { createGovernedSocialPublication } = require('../contentPublication/GovernedSocialPublication');
  const publicationService = deps.publicationService || createPublicationService({
    adapterRegistry: deps.adapterRegistry || createSocialAdapterRegistry(deps), accountResolver: deps.accountResolver,
  });
  const governed = createGovernedSocialPublication({ store, publicationService, liveEnabled: deps.liveEnabled,
    syncOutcome: deps.syncOutcome || (pool ? a => require('../../../services/paigeSocialOutcome').syncPaigeSocialOutcome(pool, a) : null) });

  return {
    id: BUILTIN_IDS.SOCIAL_CONTENT_PUBLISH,
    name: 'Social Content Publication',
    description:
      'Publish APPROVED Paige social content artifacts through canonical execution only',
    category: CAPABILITY_CATEGORIES.EXECUTION,
    outcomeTags: ['social_content_published'],
    version: 2,
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

      try {
        if (pool) await ensureSocialContentArtifactsTable(pool);
        const outputs = await governed.publish({ ...inputs, artifactId, tenantId, clientId, dryRun });
        return buildCapabilityResult({
          status: outputs.published || dryRun ? CAPABILITY_RESULT_STATUS.COMPLETED : CAPABILITY_RESULT_STATUS.FAILED,
          outputs: { ...outputs, channel: outputs.artifact?.platform, externalPostId: outputs.artifact?.publication?.providerPostId || outputs.artifact?.publication?.receipt?.externalPostId || null, externalUrl: outputs.artifact?.publishedUrl || null, externalAccountId: outputs.artifact?.approvalBinding?.account?.externalAccountId || null },
          errors: outputs.error ? [{ message: outputs.error }] : [], duration: Date.now() - started,
        });
      } catch (err) {
        return buildCapabilityResult({ status: CAPABILITY_RESULT_STATUS.FAILED, errors: [{ message: err.message }], duration: Date.now() - started });
      }
    },
  };
}
module.exports = { buildPendingCommentMirror, createSocialContentPublishCapability };
