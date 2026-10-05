'use strict';

/**
 * Legitimate public/authenticated JSON feed collector (operator-provided URL).
 * Set SIGNAL_CALLER_FEED_URL to a JSON array of RawCallerObservation objects.
 */
function createOperatorJsonFeedCollector(options = {}) {
  const id = 'operator-json-feed';
  const feedUrl = options.feedUrl || process.env.SIGNAL_CALLER_FEED_URL || null;
  const fetchFn = options.fetchFn || global.fetch;

  async function health() {
    if (!feedUrl) {
      return { available: false, reason: 'SIGNAL_CALLER_FEED_URL not configured' };
    }
    return { available: true };
  }

  async function poll() {
    const h = await health();
    if (!h.available) return [];

    const res = await fetchFn(feedUrl, {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(options.timeoutMs ?? 15000),
    });
    if (!res.ok) {
      throw new Error(`Caller feed HTTP ${res.status}`);
    }
    const json = await res.json();
    const rows = Array.isArray(json) ? json : json.observations || [];
    return rows.map(normalizeRow).filter(Boolean);
  }

  return { id, poll, health };
}

function normalizeRow(row) {
  if (!row || !row.sourceId || !row.externalMessageId) return null;
  return {
    sourceId: String(row.sourceId),
    communityId: row.communityId ? String(row.communityId) : null,
    externalMessageId: String(row.externalMessageId),
    messageTimestamp: row.messageTimestamp || row.occurredAt || new Date().toISOString(),
    rawText: row.rawText || row.text || null,
    rawReferenceUrl: row.rawReferenceUrl || row.url || null,
    tokenCa: row.tokenCa || row.tokenAddress || null,
    providerTimestamp: row.providerTimestamp || row.messageTimestamp || null,
    provenance: {
      ...(row.provenance || {}),
      dataClass: 'EMPIRICAL',
      collectorId: 'operator-json-feed',
    },
    forwarding: row.forwarding || null,
  };
}

module.exports = {
  createOperatorJsonFeedCollector,
};
