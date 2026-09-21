'use strict';

const { resolveEnvCredentials } = require('./credentialResolver');

/**
 * Orchestrates adapter dispatch for APPROVED canonical artifacts.
 * Does not mutate artifact approval_state — capability owns transitions.
 */

function createPublicationService(deps = {}) {
  const adapterRegistry = deps.adapterRegistry;
  if (!adapterRegistry) throw new Error('adapter_registry_required');

  const credentialResolver = deps.credentialResolver || resolveEnvCredentials;

  return {
    async publishApprovedArtifact(input = {}) {
      const artifact = input.artifact;
      const tenantId = String(input.tenantId || artifact?.tenantId || '').trim();
      const clientId = Number(input.clientId ?? artifact?.clientId);
      const correlationId = input.correlationId || null;
      const platform = artifact?.platform;

      if (!artifact || !platform) {
        return {
          success: false,
          errorCode: 'artifact_required',
          errorMessage: 'artifact_required',
          retryable: false,
        };
      }
      if (!tenantId || !Number.isFinite(clientId)) {
        return {
          success: false,
          errorCode: 'tenant_scope_required',
          errorMessage: 'tenant_scope_required',
          retryable: false,
        };
      }

      let adapter;
      try {
        adapter = adapterRegistry.resolve(platform);
      } catch (err) {
        return {
          success: false,
          errorCode: 'unsupported_platform',
          errorMessage: err.message,
          retryable: false,
        };
      }

      const creds = credentialResolver({ tenantId, clientId, platform });
      if (!creds.ok) {
        return {
          success: false,
          errorCode: creds.reason || 'credentials_missing',
          errorMessage: creds.reason || 'credentials_missing',
          retryable: false,
          missing: creds.missing || [],
        };
      }

      const validation = adapter.validateCredentials
        ? adapter.validateCredentials(creds.credentials)
        : { ok: true };
      if (!validation.ok) {
        return {
          success: false,
          errorCode: validation.reason || 'credentials_invalid',
          errorMessage: validation.reason || 'credentials_invalid',
          retryable: false,
        };
      }

      if (!artifact.pendingCommentId) {
        return {
          success: false,
          errorCode: 'pending_comment_mirror_missing',
          errorMessage: 'pending_comment_mirror_missing',
          retryable: false,
        };
      }

      return adapter.publish({
        artifact,
        credentials: creds.credentials,
        correlationId,
      });
    },
  };
}

module.exports = {
  createPublicationService,
};
