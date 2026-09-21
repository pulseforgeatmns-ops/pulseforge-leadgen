'use strict';

/**
 * SPEC-255 — Read-only first-party paid attribution evidence retrieval.
 *
 * Authoritative source: agent_actions where action_type = walkthrough_request
 * and payload.attribution is present. Never reads prospect mirrors.
 */

const { SOURCE_KIND } = require('../../lib/walkthroughAttribution');
const { AVAILABILITY } = require('./types');
const { observationWindowFromDays } = require('./adapters/googleAds');

const SPEC = 'SPEC-255';
const DEFAULT_WINDOW_DAYS = 7;
const DEFAULT_LIMIT = 500;
const ACTION_TYPE = 'walkthrough_request';

const FIRST_PARTY_UNAVAILABLE_REASON = Object.freeze({
  INVALID_CLIENT_ID: 'INVALID_CLIENT_ID',
  POOL_UNAVAILABLE: 'POOL_UNAVAILABLE',
  QUERY_FAILED: 'QUERY_FAILED',
  SCHEMA_UNSUPPORTED: 'SCHEMA_UNSUPPORTED',
});

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function resolveObservationWindow(window, windowDays = DEFAULT_WINDOW_DAYS) {
  if (window && window.start && window.end) {
    return {
      start: asText(window.start),
      end: asText(window.end),
      days: window.days || windowDays,
      label: window.label || `LAST_${window.days || windowDays}_DAYS`,
    };
  }
  return observationWindowFromDays(windowDays);
}

function windowBounds(window, windowDays = DEFAULT_WINDOW_DAYS) {
  const resolved = resolveObservationWindow(window, windowDays);
  const startAt = new Date(`${resolved.start}T00:00:00.000Z`);
  const endExclusive = new Date(`${resolved.end}T23:59:59.999Z`);
  endExclusive.setUTCDate(endExclusive.getUTCDate() + 1);
  return { resolved, startAt, endAt: endExclusive };
}

function leadSourceLabel(leadSource) {
  switch (asText(leadSource).toLowerCase()) {
    case 'chatgpt_ads':
      return 'ChatGPT Ads';
    case 'google_ads':
      return 'Google Ads';
    case 'yelp':
      return 'Yelp';
    default:
      return leadSource || 'paid channel';
  }
}

function evidenceConfidence(attributionStatus) {
  const status = asText(attributionStatus).toLowerCase();
  if (status === 'deterministic') return 0.78;
  if (status === 'inferred') return 0.55;
  return 0.4;
}

function mapAgentActionToEvidence(row) {
  const payload = row.payload || {};
  const attribution = payload.attribution || {};
  const raw = attribution.raw || {};
  const normalized = attribution.normalized || {};
  const provenance = {
    ...(attribution.provenance || {}),
    sourceKind: SOURCE_KIND,
  };

  const leadSource = asText(normalized.lead_source) || 'unknown';
  const campaignId = asText(raw.campaign_id) || null;
  const channelLabel = leadSourceLabel(leadSource);
  const campaignSuffix = campaignId ? ` campaign ${campaignId}` : '';

  return {
    spec: SPEC,
    kind: 'first_party_attributed_lead',
    sourceKind: SOURCE_KIND,
    label: `Website walkthrough lead attributed to ${channelLabel}${campaignSuffix}`,
    source: 'first_party_walkthrough',
    confidence: evidenceConfidence(normalized.attribution_status),
    evidenceId: row.id,
    observedAt: row.created_at instanceof Date
      ? row.created_at.toISOString()
      : asText(row.created_at) || new Date().toISOString(),
    leadSource,
    attributionStatus: asText(normalized.attribution_status) || 'unattributed',
    campaignId,
    adGroupId: asText(raw.ad_group_id) || null,
    adId: asText(raw.ad_id) || null,
    opref: asText(raw.opref) || null,
    oppref: asText(raw.oppref) || null,
    attribution: {
      raw: { ...raw },
      normalized: { ...normalized },
      provenance,
    },
    provenance: {
      sourceKind: SOURCE_KIND,
      retrievalSource: 'agent_actions',
      readOnly: true,
      observedAt: row.created_at instanceof Date
        ? row.created_at.toISOString()
        : asText(row.created_at) || null,
      evidenceId: row.id,
    },
  };
}

function unavailableFirstPartyAttributionEvidence(result = {}) {
  return {
    spec: SPEC,
    kind: 'first_party_attribution_retrieval_unavailable',
    sourceKind: SOURCE_KIND,
    availability: result.availability || AVAILABILITY.UNAVAILABLE,
    reason: result.reason || FIRST_PARTY_UNAVAILABLE_REASON.QUERY_FAILED,
    label: `First-party attribution retrieval unavailable (${asText(result.reason) || 'unknown'})`,
    source: 'first_party_walkthrough_retrieval',
    confidence: 0,
    provenance: {
      sourceKind: SOURCE_KIND,
      retrievalSource: 'agent_actions',
      readOnly: true,
      observedAt: new Date().toISOString(),
    },
    error: result.error || null,
    observationWindow: result.observationWindow || null,
  };
}

function evidenceIdentity(row) {
  if (row == null) return null;
  if (row.evidenceId != null) return String(row.evidenceId);
  if (row.id != null) return String(row.id);
  return null;
}

/**
 * Merge DB-observed and caller-supplied acquisition evidence.
 * DB rows win on duplicate evidenceId; caller rows remain distinguishable.
 *
 * @param {object[]} operatorSupplied
 * @param {object[]} observed
 * @param {object} [opts]
 * @returns {object[]}
 */
function mergeAcquisitionEvidence(operatorSupplied = [], observed = [], opts = {}) {
  const byId = new Map();
  const withoutId = [];

  for (const row of observed || []) {
    if (row?.kind === 'first_party_attribution_retrieval_unavailable') {
      if (opts.includeUnavailable) withoutId.push(row);
      continue;
    }
    const id = evidenceIdentity(row);
    if (id) byId.set(id, row);
    else withoutId.push(row);
  }

  for (const row of operatorSupplied || []) {
    const id = evidenceIdentity(row);
    const operatorRow = {
      ...row,
      provenance: {
        sourceKind: 'OPERATOR_SUPPLIED',
        readOnly: true,
        ...(row.provenance || {}),
      },
    };
    if (id && byId.has(id)) continue;
    if (id) byId.set(`operator:${id}`, operatorRow);
    else withoutId.push(operatorRow);
  }

  return [...byId.values(), ...withoutId];
}

/**
 * Load durable first-party attribution evidence from agent_actions.
 *
 * @param {object} input
 * @param {number|string} input.clientId
 * @param {import('pg').Pool} [input.pool]
 * @param {object} [input.window]
 * @param {number} [input.windowDays]
 * @param {number} [input.limit]
 * @param {Function} [input.queryRows]
 * @returns {Promise<object>}
 */
async function loadFirstPartyAttributionEvidence(input = {}) {
  const clientId = Number(input.clientId);
  const { resolved, startAt, endAt } = windowBounds(input.window || null, input.windowDays);
  const limit = Number.isInteger(input.limit) && input.limit > 0
    ? input.limit
    : DEFAULT_LIMIT;

  if (!Number.isInteger(clientId) || clientId <= 0) {
    return {
      spec: SPEC,
      availability: AVAILABILITY.UNAVAILABLE,
      reason: FIRST_PARTY_UNAVAILABLE_REASON.INVALID_CLIENT_ID,
      observationWindow: resolved,
      evidence: [],
      observedCount: 0,
      error: 'Valid clientId is required for tenant-scoped first-party attribution retrieval.',
    };
  }

  const baseResult = {
    spec: SPEC,
    observationWindow: resolved,
    evidence: [],
    observedCount: 0,
  };

  let rows = [];
  try {
    if (typeof input.queryRows === 'function') {
      rows = await input.queryRows({
        clientId,
        startAt,
        endAt,
        limit,
        actionType: ACTION_TYPE,
      });
    } else {
      const pool = input.pool;
      if (!pool || typeof pool.query !== 'function') {
        return {
          ...baseResult,
          availability: AVAILABILITY.UNAVAILABLE,
          reason: FIRST_PARTY_UNAVAILABLE_REASON.POOL_UNAVAILABLE,
          error: 'Database pool is unavailable for first-party attribution retrieval.',
        };
      }

      const res = await pool.query(
        `SELECT id, created_at, payload
           FROM agent_actions
          WHERE client_id = $1
            AND action_type = $2
            AND payload->'attribution' IS NOT NULL
            AND created_at >= $3
            AND created_at < $4
          ORDER BY created_at ASC
          LIMIT $5`,
        [clientId, ACTION_TYPE, startAt, endAt, limit]
      );
      rows = res.rows || [];
    }
  } catch (err) {
    const message = asText(err.message);
    const reason = /json|payload|column|relation|agent_actions/i.test(message)
      ? FIRST_PARTY_UNAVAILABLE_REASON.SCHEMA_UNSUPPORTED
      : FIRST_PARTY_UNAVAILABLE_REASON.QUERY_FAILED;
    return {
      ...baseResult,
      availability: AVAILABILITY.ERROR,
      reason,
      error: message || 'First-party attribution query failed.',
    };
  }

  const evidence = rows
    .filter((row) => row?.payload?.attribution != null)
    .map(mapAgentActionToEvidence);

  return {
    ...baseResult,
    availability: AVAILABILITY.AVAILABLE,
    evidence,
    observedCount: evidence.length,
    provenance: {
      sourceKind: SOURCE_KIND,
      retrievalSource: 'agent_actions',
      readOnly: true,
      observedAt: new Date().toISOString(),
    },
  };
}

module.exports = {
  SPEC,
  ACTION_TYPE,
  DEFAULT_WINDOW_DAYS,
  DEFAULT_LIMIT,
  FIRST_PARTY_UNAVAILABLE_REASON,
  resolveObservationWindow,
  mapAgentActionToEvidence,
  unavailableFirstPartyAttributionEvidence,
  mergeAcquisitionEvidence,
  loadFirstPartyAttributionEvidence,
};
