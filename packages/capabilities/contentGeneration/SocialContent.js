'use strict';

/**
 * SPEC-256 — Canonical Paige social content generation capability.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
} = require('../types');
const {
  CAPABILITY_VERSION,
  ARTIFACT_TYPE,
  APPROVAL_STATES,
  buildProvenance,
  buildSocialContentArtifact,
} = require('./types');
const {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
} = require('./SocialContentStore');

function asClientId(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.trunc(n);
}

function assertTenantScope(context) {
  const tenantId = String(context.tenantId || '').trim();
  const clientId = asClientId(context.clientId);
  if (!tenantId || clientId == null) {
    throw new Error('tenant_scope_required');
  }
  if (tenantId !== String(clientId)) {
    throw new Error('tenant_client_mismatch');
  }
  return { tenantId, clientId };
}

function loadPaigeAgent(clientId) {
  const paigePath = require.resolve('../../../paigeAgent');
  delete require.cache[paigePath];
  process.env.ACTIVE_CLIENT_ID = String(clientId);
  return require('../../../paigeAgent');
}

function buildDraftRows(generationResult, context, inputs) {
  const { tenantId, clientId } = assertTenantScope(context);
  const outputs = Array.isArray(generationResult.outputs) ? generationResult.outputs : [];
  const drafts = Array.isArray(generationResult.drafts) ? generationResult.drafts : [];

  const sourceRows = drafts.length ? drafts : outputs.map((output) => ({
    company: { name: output.company },
    content: output.content,
    contentType: output.content_type,
    channel: output.channel,
    meta: output.meta || null,
    quality: output.quality || null,
  }));

  return sourceRows.map((row) => {
    const platform = row.channel || inputs.platform || inputs.channel;
    const provenance = buildProvenance({
      tenantId,
      platform,
      contentObjective: inputs.contentObjective || context.objective || null,
      missionId: inputs.missionId || inputs.missionContext?.missionId || context.missionId || null,
      workspaceContext: inputs.workspaceContext || null,
      sourceContext: inputs.workspaceContext || inputs.missionContext || null,
      evidence: inputs.evidence || inputs.missionContext?.evidence || [],
      cadenceContext: inputs.cadenceContext || null,
      invocationSource: inputs.invocationSource || inputs.source || null,
    });
    const channelLabel = {
      facebook_page: 'Facebook Page',
      google_business: 'Google Business',
      blog: 'Blog',
      linkedin_page: 'LinkedIn Page',
      linkedin_personal: 'LinkedIn Personal',
    }[platform] || platform;
    const typeLabel = row.meta?.format || row.contentType;
    const label = `${channelLabel} · ${String(typeLabel || 'Post').charAt(0).toUpperCase()}${String(typeLabel || 'Post').slice(1)}`;

    return buildSocialContentArtifact({
      tenantId,
      clientId,
      platform,
      contentObjective: inputs.contentObjective || context.objective || null,
      missionId: provenance.missionId,
      companyName: row.company?.name || row.companyName || null,
      contentType: row.contentType || row.content_type || null,
      label,
      body: row.content,
      meta: {
        ...(row.meta || {}),
        quality: row.quality || null,
        company: row.company || null,
      },
      provenance,
      approvalState: APPROVAL_STATES.PENDING_APPROVAL,
    });
  });
}

/**
 * @param {object} [deps]
 */
function createSocialContentCapability(deps = {}) {
  const store =
    deps.socialContentStore ||
    (deps.pool
      ? createPostgresSocialContentStore(deps.pool)
      : createInMemorySocialContentStore());
  const runGeneration =
    deps.runGeneration ||
    ((options) => loadPaigeAgent(options.client_id).generateSocialContent(options));
  const mirrorPendingComment =
    deps.mirrorPendingComment ||
    ((draft, clientId) =>
      loadPaigeAgent(clientId).mirrorSocialContentToPendingComments(draft));

  return {
    id: BUILTIN_IDS.SOCIAL_CONTENT,
    name: 'Social Content Generation',
    description:
      'Tenant-scoped Paige social content generation — drafts only, never publish authority',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['social_content_draft', 'pending_approval'],
    version: 1,
    retryable: false,
    timeoutMs: 120_000,
    supportsRollback: false,
    idempotent: false,

    canRun(context) {
      try {
        assertTenantScope(context);
        return true;
      } catch (_) {
        return false;
      }
    },

    estimate() {
      return buildCapabilityEstimate({ durationMs: 15_000, confidence: 0.85 });
    },

    async execute(context) {
      const started = Date.now();
      const inputs = (context && context.inputs) || {};
      const { tenantId, clientId } = assertTenantScope(context);
      const dryRun = Boolean(inputs.dryRun ?? inputs.dry_run);

      context.emitProgress?.({
        stage: 'generating',
        message: 'Running Paige social content generation',
      });

      const generationResult = await runGeneration({
        client_id: clientId,
        dryRun,
        channel: inputs.platform || inputs.channel || null,
        format: inputs.format || null,
        count: inputs.count || 1,
        simulateMiraUnavailable: inputs.simulateMiraUnavailable,
        contentObjective: inputs.contentObjective || context.objective || null,
        workspaceContext: inputs.workspaceContext || null,
        missionContext: inputs.missionContext || null,
        evidence: inputs.evidence || [],
        cadenceContext: inputs.cadenceContext || null,
        invocationSource: inputs.invocationSource || inputs.source || 'capability',
        skipCanonicalPersist: true,
      });

      if (generationResult?.skipped) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            generation: generationResult,
            artifacts: [],
          },
          duration: Date.now() - started,
        });
      }

      if (!generationResult?.success && !(generationResult?.outputs || []).length && !(generationResult?.drafts || []).length) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: {
            generation: generationResult,
            artifacts: [],
          },
          errors: [{ message: generationResult?.error || 'generation_failed' }],
          duration: Date.now() - started,
        });
      }

      const draftRows = buildDraftRows(generationResult, context, inputs);
      if (!draftRows.length) {
        return buildCapabilityResult({
          status: generationResult?.success ? CAPABILITY_RESULT_STATUS.COMPLETED : CAPABILITY_RESULT_STATUS.FAILED,
          outputs: {
            generation: generationResult,
            artifacts: [],
          },
          warnings: generationResult?.channels_failed?.length
            ? [`channels_failed:${generationResult.channels_failed.join(',')}`]
            : [],
          duration: Date.now() - started,
        });
      }

      if (dryRun) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            generation: generationResult,
            artifacts: draftRows,
          },
          artifacts: draftRows.map((row) => ({
            type: ARTIFACT_TYPE,
            id: row.id,
            approvalState: row.approvalState,
          })),
          evidence: draftRows.map((row) => ({
            kind: ARTIFACT_TYPE,
            summary: `${row.platform}:${row.label}`,
            provenance: row.provenance,
          })),
          duration: Date.now() - started,
        });
      }

      const pool = deps.pool || null;
      let committed = [];
      if (pool && typeof pool.connect === 'function') {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await ensureStoreSchema(store, client);
          committed = await store.insertBatch(draftRows, { client });
          for (let i = 0; i < committed.length; i++) {
            const draft = draftRows[i];
            const pendingCommentId = await mirrorPendingComment(draft, clientId);
            if (pendingCommentId) {
              committed[i] = await store.attachPendingCommentId(
                committed[i].id,
                tenantId,
                clientId,
                pendingCommentId
              );
            }
          }
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        } finally {
          client.release();
        }
      } else {
        await ensureStoreSchema(store);
        committed = await store.insertBatch(draftRows);
        for (let i = 0; i < committed.length; i++) {
          const draft = draftRows[i];
          const pendingCommentId = await mirrorPendingComment(draft, clientId);
          if (pendingCommentId) {
            committed[i] = await store.attachPendingCommentId(
              committed[i].id,
              tenantId,
              clientId,
              pendingCommentId
            );
          }
        }
      }

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          generation: generationResult,
          artifacts: committed,
        },
        artifacts: committed.map((row) => ({
          type: ARTIFACT_TYPE,
          id: row.id,
          approvalState: row.approvalState,
        })),
        evidence: committed.map((row) => ({
          kind: ARTIFACT_TYPE,
          summary: `${row.platform}:${row.label}`,
          provenance: row.provenance,
        })),
        warnings: generationResult?.channels_failed?.length
          ? [`channels_failed:${generationResult.channels_failed.join(',')}`]
          : [],
        duration: Date.now() - started,
      });
    },
  };
}

async function ensureStoreSchema(store, client) {
  if (typeof store.ensureSchema === 'function') {
    await store.ensureSchema(client);
  }
}

module.exports = {
  createSocialContentCapability,
  assertTenantScope,
  buildDraftRows,
};
