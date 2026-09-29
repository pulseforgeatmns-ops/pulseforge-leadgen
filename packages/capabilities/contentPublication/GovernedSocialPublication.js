'use strict';
const { randomUUID } = require('crypto');
const { assertApproval, contentHash } = require('./approvalBinding');
const now = () => new Date().toISOString();

function createGovernedSocialPublication({ store, publicationService, syncOutcome, liveEnabled = () => process.env.PAIGE_SOCIAL_PUBLISH_ENABLED === 'true' }) {
  async function update(id, tenantId, clientId, fn) {
    return store.withLockedArtifact(id, tenantId, clientId, async (artifact, tx) => {
      const patch = await fn(artifact);
      return tx.updateArtifactMetadata(id, tenantId, clientId, patch);
    });
  }
  async function finishOutcome(artifact) {
    if (syncOutcome && !artifact.publication.outcomeId) {
      try {
        const outcomeId = await syncOutcome(artifact);
        artifact = await update(artifact.id, artifact.tenantId, artifact.clientId, a => ({ publication: { ...a.publication, outcomeId, outcomeError: null } }));
      } catch (_) {
        artifact = await update(artifact.id, artifact.tenantId, artifact.clientId, a => ({ publication: { ...a.publication, outcomeError: 'outcome_sync_failed' } }));
      }
    }
    return artifact;
  }
  async function publish(input) {
    const { artifactId, tenantId, clientId, dryRun } = input;
    let artifact = await store.getById(artifactId, tenantId, clientId);
    if (!artifact) throw new Error('artifact_not_found');
    if (artifact.publishState === 'PUBLISHED') {
      assertApproval(artifact, artifact.approvalBinding?.account);
      return { artifact: dryRun ? artifact : await finishOutcome(artifact), published: true, idempotent: true };
    }
    const connected = await publicationService.prepare({ artifact, tenantId, clientId });
    if (dryRun) return { artifact, dryRun: true, publishReady: true, account: connected.account };
    if (!liveEnabled()) throw new Error('social_publication_disabled');

    let send = false;
    artifact = await update(artifactId, tenantId, clientId, a => {
      assertApproval(a, connected.account);
      const pub = a.publication || {};
      if (a.publishState === 'PUBLISHED' || pub.receipt?.externalPostId) return {};
      if (input.reconcilePostId) {
        if (!input.reconciledBy || !input.reconciliationReason || !['PUBLISHING', 'UNKNOWN'].includes(a.publishState)) throw new Error('reconciliation_context_required');
        if (a.publishState === 'PUBLISHING' && Date.now() - Date.parse(pub.startedAt) < 300000) throw new Error('publish_in_progress');
        return { publishState: 'VERIFYING', publication: { ...pub, receipt: { externalPostId: String(input.reconcilePostId), externalAccountId: connected.account.externalAccountId },
          reconciledBy: String(input.reconciledBy), reconciliationReason: String(input.reconciliationReason), reconciledAt: now() } };
      }
      if (a.publishState === 'PUBLISHING') throw new Error('publish_in_progress');
      if (a.publishState === 'UNKNOWN') throw new Error('publish_requires_reconciliation');
      if (a.publishState === 'FAILED' && pub.safeToRetry !== true) throw new Error('publish_not_retryable');
      const attempt = { id: randomUUID(), startedAt: now(), status: 'PUBLISHING' };
      send = true;
      return { publishState: 'PUBLISHING', publishError: null, publication: { ...pub,
        platform: a.platform, account: connected.account, artifactId: a.id, contentHash: contentHash(a), approvalHash: a.approvalBinding.hash,
        missionId: a.missionId, campaignId: a.meta?.campaignId || null,
        attemptId: attempt.id, startedAt: attempt.startedAt, safeToRetry: false,
        attempts: [...(pub.attempts || []), attempt] } };
    });
    if (artifact.publishState === 'PUBLISHED') return { artifact: await finishOutcome(artifact), published: true, idempotent: true };
    if (send) {
      let result;
      try { result = await publicationService.publishApprovedArtifact({ artifact, tenantId, clientId, idempotencyKey: artifact.approvalBinding.hash }); }
      catch (_) { result = { success: false, errorCode: 'provider_response_unknown' }; }
      const accepted = result?.success === true && Boolean(result.externalPostId);
      const definite = result?.definitelyNotPublished === true;
      // Commit the provider identifier before attempting read-back. A commit failure leaves
      // PUBLISHING (never a fresh-send retry), because the provider may already have acted.
      artifact = await update(artifactId, tenantId, clientId, a => {
        const status = accepted ? 'VERIFYING' : definite ? 'FAILED' : 'UNKNOWN';
        const error = accepted ? null : result?.errorCode || 'provider_response_unknown';
        const receipt = accepted ? { externalPostId: String(result.externalPostId), externalAccountId: String(result.externalAccountId || connected.account.externalAccountId), acceptedAt: now() } : null;
        return { publishState: status, publishError: error, publication: { ...a.publication, receipt,
          safeToRetry: !accepted && definite && result.retryable === true,
          attempts: a.publication.attempts.map(x => x.id === a.publication.attemptId ? { ...x, status, completedAt: now(), error, receipt } : x) } };
      });
      if (!accepted) return { artifact, published: false, error: artifact.publishError };
    }
    let verification;
    try { verification = await publicationService.readBack({ artifact, tenantId, clientId, receipt: artifact.publication.receipt }); }
    catch (_) { verification = { status: 'read_back_failed', published: false }; }
    const receipt = artifact.publication.receipt;
    const matches = verification.externalPostId === receipt.externalPostId &&
      String(verification.externalAccountId) === connected.account.externalAccountId &&
      verification.platformMatches === true && verification.body === artifact.body;
    const verified = matches && verification.published === true && Boolean(verification.publishedAt) && Number.isFinite(Date.parse(verification.publishedAt));
    const error = verified ? null : matches ? 'provider_not_yet_published' : 'provider_read_back_unverified';
    artifact = await update(artifactId, tenantId, clientId, a => {
      // Another read-back may have completed while this one was in flight.
      if (a.publishState === 'PUBLISHED') return {};
      assertApproval(a, connected.account);
      return { publishState: verified ? 'PUBLISHED' : 'VERIFYING', publishError: error,
        publishedAt: verified ? new Date(verification.publishedAt).toISOString() : null,
        publishedUrl: verified ? verification.externalUrl || null : null,
        publication: { ...a.publication, providerStatus: verification.status || 'unknown',
          verifiedAt: verified ? now() : null, lastCheckedAt: now(), externalUrl: verified ? verification.externalUrl || null : null,
          providerPostId: receipt.externalPostId, verificationContentHash: matches ? contentHash(a) : null,
          attempts: (a.publication.attempts || []).map(x => x.id === a.publication.attemptId ? { ...x, status: verified ? 'PUBLISHED' : 'VERIFYING', verificationError: error } : x) } };
    });
    if (artifact.publishState === 'PUBLISHED') artifact = await finishOutcome(artifact);
    return { artifact, published: artifact.publishState === 'PUBLISHED', error: artifact.publishError, verificationOnly: !send };
  }
  return { publish };
}
module.exports = { createGovernedSocialPublication };
