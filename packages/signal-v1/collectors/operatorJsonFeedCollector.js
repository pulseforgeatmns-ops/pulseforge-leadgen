'use strict';

/**
 * Legitimate public/authenticated JSON feed collector (operator-provided URL).
 * Set SIGNAL_CALLER_FEED_URL to a JSON array of RawCallerObservation objects.
 */
function createOperatorJsonFeedCollector(options = {}) {
  const id = 'operator-json-feed';
  const feedUrl = options.feedUrl || process.env.SIGNAL_CALLER_FEED_URL || null;
  const feedToken = options.feedToken || process.env.SIGNAL_OPERATOR_FEED_TOKEN || null;
  const fetchFn = options.fetchFn || global.fetch;
  let lastRemoteHealth = null;

  function authHeaders() {
    const headers = { Accept: 'application/json' };
    if (typeof feedToken === 'string' && feedToken.length >= 32) {
      headers.authorization = `Bearer ${feedToken}`;
    }
    return headers;
  }

  function healthUrl() {
    if (!feedUrl) return null;
    try {
      const u = new URL(feedUrl);
      if (u.pathname.endsWith('/feed')) {
        u.pathname = `${u.pathname.slice(0, -5)}/health`;
      } else if (!u.pathname.endsWith('/health')) {
        u.pathname = u.pathname.replace(/\/?$/, '/health');
      }
      return u.toString();
    } catch {
      return null;
    }
  }

  async function health() {
    if (!feedUrl) {
      return { available: false, reason: 'SIGNAL_CALLER_FEED_URL not configured' };
    }
    const remoteUrl = healthUrl();
    if (!remoteUrl) {
      return { available: true, connected: false, reason: 'invalid_feed_url' };
    }
    try {
      const res = await fetchFn(remoteUrl, {
        headers: authHeaders(),
        signal: AbortSignal.timeout(options.timeoutMs ?? 15000),
      });
      if (!res.ok) {
        return { available: false, connected: false, reason: `Caller feed health HTTP ${res.status}` };
      }
      const json = await res.json();
      lastRemoteHealth = json;
      const connected = Boolean(json.connected && json.credentialsConfigured !== false);
      return {
        available: true,
        connected,
        feedHealth: json,
        reason: connected ? undefined : 'caller_feed_not_connected',
      };
    } catch (err) {
      return { available: false, connected: false, reason: String(err.message || err) };
    }
  }

  async function poll() {
    const h = await health();
    if (!h.available) return [];

    const res = await fetchFn(feedUrl, {
      headers: authHeaders(),
      signal: AbortSignal.timeout(options.timeoutMs ?? 15000),
    });
    if (!res.ok) {
      throw new Error(`Caller feed HTTP ${res.status}`);
    }
    const json = await res.json();
    const rows = Array.isArray(json)
      ? json
      : json.calls || json.observations || [];
    return rows.map(normalizeRow).filter(Boolean);
  }

  function getLastRemoteHealth() {
    return lastRemoteHealth;
  }

  return { id, poll, health, getLastRemoteHealth };
}

function normalizeRow(row) {
  if (!row || !row.sourceId) return null;
  const externalMessageId = row.externalMessageId || row.externalId;
  if (!externalMessageId) return null;
  if (row.provenance?.dataClass === 'PROCEDURAL' || row.provenance?.testOnly) return null;
  const messageTimestamp = row.messageTimestamp || row.occurredAt;
  if (!messageTimestamp) return null;
  return {
    sourceId: String(row.sourceId),
    communityId: row.communityId ? String(row.communityId) : null,
    externalMessageId: String(externalMessageId),
    messageTimestamp,
    ingestedAt: row.ingestedAt || null,
    rawText: row.rawText || row.text || null,
    rawReferenceUrl: row.rawReferenceUrl || row.url || null,
    tokenCa: row.tokenCa || row.tokenAddress || null,
    providerTimestamp: row.providerTimestamp || messageTimestamp || null,
    provenance: {
      ...(row.provenance || {}),
      dataClass: row.provenance?.dataClass || 'EMPIRICAL',
      collectorId: row.provenance?.collectorId || 'operator-json-feed',
      ingestedAt: row.ingestedAt || null,
    },
    forwarding: row.forwarding || null,
  };
}

module.exports = {
  createOperatorJsonFeedCollector,
};
