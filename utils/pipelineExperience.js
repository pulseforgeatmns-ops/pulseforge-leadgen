'use strict';

/**
 * Canonical Pipeline experience resolution for the human-setter pilot flag.
 * Shared by API responses and dashboard hydration so the UI never flashes
 * pilot_v2 and then reverts to legacy while tenant feature state is unresolved.
 */

const PIPELINE_EXPERIENCE = Object.freeze({
  PENDING: 'pending',
  PILOT_V2: 'pilot_v2',
  LEGACY: 'legacy',
});

function normalizeEnabled(value) {
  return value === true;
}

function featuresFromFlag(clientId, enabled) {
  const on = normalizeEnabled(enabled);
  return {
    client_id: clientId == null ? null : Number(clientId),
    setter_pipeline_v2_enabled: on,
    pipeline_experience: on ? PIPELINE_EXPERIENCE.PILOT_V2 : PIPELINE_EXPERIENCE.LEGACY,
  };
}

/**
 * Resolve a features payload into a stable experience decision.
 * Stale or mismatched responses must be ignored by the caller.
 */
function resolvePipelineExperience(features, {
  requestId = null,
  activeRequestId = null,
  expectedClientId = null,
} = {}) {
  if (activeRequestId != null && requestId !== activeRequestId) {
    return { stale: true, reason: 'stale_request' };
  }
  if (!features || typeof features !== 'object') {
    return {
      stale: false,
      experience: PIPELINE_EXPERIENCE.LEGACY,
      enabled: false,
      client_id: expectedClientId == null ? null : Number(expectedClientId),
      reason: 'missing_features',
    };
  }

  if (
    expectedClientId != null
    && features.client_id != null
    && Number(features.client_id) !== Number(expectedClientId)
  ) {
    return { stale: true, reason: 'client_mismatch' };
  }

  const enabled = normalizeEnabled(features.setter_pipeline_v2_enabled);
  return {
    stale: false,
    experience: enabled ? PIPELINE_EXPERIENCE.PILOT_V2 : PIPELINE_EXPERIENCE.LEGACY,
    enabled,
    client_id: features.client_id != null
      ? Number(features.client_id)
      : (expectedClientId == null ? null : Number(expectedClientId)),
    reason: 'ok',
  };
}

/** Pipeline tab is visible only when explicitly shown as the active panel. */
function isPipelineTabVisible({ inlineDisplay = '', activeTab = null } = {}) {
  if (activeTab != null) return activeTab === 'pipeline';
  return inlineDisplay === 'block';
}

module.exports = {
  PIPELINE_EXPERIENCE,
  featuresFromFlag,
  resolvePipelineExperience,
  isPipelineTabVisible,
  normalizeEnabled,
};
