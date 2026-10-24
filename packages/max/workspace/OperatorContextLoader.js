'use strict';

/**
 * SPEC-104 — Startup loading pipeline for Operator Context.
 *
 * Sequence:
 *   Load Blueprint → Load Playbook → Load Operator Context →
 *   Load recent Outcomes → Load active Missions → Generate Brief
 *
 * Called before workspace open; attaches to session envelope.
 */

const {
  loadOperatorContext,
  generateSessionBrief,
  buildOperatorContextAttachment,
} = require('../../../services/operatorContext');

/**
 * Load operator context and generate a fresh session brief.
 *
 * @param {object} input
 * @param {string|number} input.tenantId
 * @param {string|number} [input.clientId]
 * @param {{ hour?: number }} [input.options]
 * @param {object} [input.operatorContextOpts] — store/pool/missionEngine overrides
 * @returns {Promise<object>}
 */
async function loadOperatorContextForSession(input = {}) {
  const tenantId = String(
    input.tenantId || input.clientId || ''
  ).trim();
  const clientId =
    input.clientId != null && input.clientId !== ''
      ? Number(input.clientId)
      : Number(tenantId);

  if (!tenantId || !Number.isFinite(clientId)) {
    return buildOperatorContextAttachment(null, null);
  }

  const opts = {
    tenantId,
    clientId,
    rebuildIfMissing: input.rebuildIfMissing !== false,
    rebuildIfStale: input.rebuildIfStale === true,
    maxAgeMs: input.maxAgeMs,
    ...(input.operatorContextOpts || {}),
  };

  let row = null;
  try {
    row = await loadOperatorContext(opts);
  } catch (err) {
    if (input.propagateErrors) throw err;
    row = null;
  }

  const sessionBrief = row
    ? generateSessionBrief(row, { hour: input.options && input.options.hour })
    : null;

  return buildOperatorContextAttachment(row, sessionBrief);
}

module.exports = {
  loadOperatorContextForSession,
};
