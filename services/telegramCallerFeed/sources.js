'use strict';

const { CLUSTER_RELATIONSHIP } = require('../../packages/signal-v1/prospective/constants');

/**
 * Persistent account access requires an explicit, operator-approved source list.
 * Candidate names are not verified Telegram identities. An empty configuration
 * must remain disconnected instead of silently polling guessed usernames.
 */
const DEFAULT_SOURCES = Object.freeze([]);

function loadConfiguredSources() {
  const raw = process.env.TELEGRAM_CALLER_SOURCES_JSON;
  if (!raw) return DEFAULT_SOURCES.map(s => ({ ...s }));
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) throw new Error('TELEGRAM_CALLER_SOURCES_JSON must be a JSON array');
    return parsed.map(row => ({
      ...row,
      platform: row.platform || 'telegram',
      sourceRole: row.sourceRole || 'CALLER',
      clusterRelationshipStatus: row.clusterRelationshipStatus || CLUSTER_RELATIONSHIP.UNKNOWN,
      collector: row.collector || 'telegram-caller-feed',
    }));
  } catch (err) {
    throw new Error(`Invalid TELEGRAM_CALLER_SOURCES_JSON: ${err.message}`);
  }
}

module.exports = {
  DEFAULT_SOURCES,
  loadConfiguredSources,
};
