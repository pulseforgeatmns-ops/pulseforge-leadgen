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

module.exports = {
  contributionPayloadBody,
  hasNestedSpecialistPayload,
  unwrapSpecialistPayload,
  isSupersededContribution,
  markContributionSuperseded,
};
