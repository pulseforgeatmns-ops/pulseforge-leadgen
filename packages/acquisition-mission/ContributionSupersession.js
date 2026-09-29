'use strict';

/**
 * Canonical contribution supersession for durable AMO rows.
 * Production CAPACITY JSONB may store specialist payload at payload.payload;
 * superseded must be readable and writable at both levels.
 */

function contributionPayloadBody(row) {
  if (!row || typeof row !== 'object') return {};
  if (row.payload != null && typeof row.payload === 'object') return row.payload;
  return row;
}

function hasNestedSpecialistPayload(payload = {}) {
  if (!payload || typeof payload !== 'object') return false;
  if (!payload.payload || typeof payload.payload !== 'object') return false;
  return Boolean(
    payload.specialist
    || payload.kind
    || payload.missionId
    || payload.queue
    || payload.capacity
    || payload.variants
  );
}

/** Unwrap production-shaped specialist payload (payload.payload queue/variants). */
function unwrapSpecialistPayload(contributionRow) {
  const body = contributionPayloadBody(contributionRow);
  if (hasNestedSpecialistPayload(body)) {
    return body.payload;
  }
  return body;
}

function isSupersededContribution(row) {
  const body = contributionPayloadBody(row);
  if (body.superseded === true) return true;
  if (body.payload && typeof body.payload === 'object' && body.payload.superseded === true) {
    return true;
  }
  return false;
}

function markContributionSuperseded(row, supersededBy) {
  if (!row || typeof row !== 'object') return row;
  const payload = row.payload && typeof row.payload === 'object'
    ? { ...row.payload }
    : {};
  const marked = {
    ...payload,
    superseded: true,
    supersededBy,
  };
  if (hasNestedSpecialistPayload(payload)) {
    marked.payload = {
      ...(payload.payload || {}),
      superseded: true,
      supersededBy,
    };
  }
  return {
    ...row,
    payload: marked,
  };
}

/** All non-superseded rows for a specialist/kind pair (insertion order preserved). */
function listActiveContributions(contributions = [], specialist, kind) {
  return contributions.filter(
    (row) =>
      row.specialist === specialist
      && row.kind === kind
      && !isSupersededContribution(row)
  );
}

/**
 * Durably mark every active predecessor superseded. Used when revision replaces
 * prepared artifacts so orphan rows from failed prior revisions cannot remain active.
 */
function supersedeAllActiveContributions(store, contributions, specialist, kind, supersededBy) {
  if (!store || typeof store.updateContribution !== 'function') {
    throw new Error('supersedeAllActiveContributions requires a contribution store.');
  }
  const active = listActiveContributions(contributions, specialist, kind);
  for (const row of active) {
    store.updateContribution(row.id, (existing) => markContributionSuperseded(existing, supersededBy));
  }
  return active.map((row) => row.id);
}

module.exports = {
  contributionPayloadBody,
  hasNestedSpecialistPayload,
  unwrapSpecialistPayload,
  isSupersededContribution,
  markContributionSuperseded,
  listActiveContributions,
  supersedeAllActiveContributions,
};
