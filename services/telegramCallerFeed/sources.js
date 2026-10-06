'use strict';

const { CLUSTER_RELATIONSHIP, SOURCE_ROLES } = require('../../packages/signal-v1/prospective/constants');

/**
 * Initial production Telegram caller channels (best-effort join).
 * Usernames may be overridden per channel via TELEGRAM_CALLER_SOURCES_JSON.
 */
const DEFAULT_SOURCES = Object.freeze([
  {
    sourceId: 'telegram-front-runners',
    displayName: 'Front Runners',
    username: 'front_runners_sol',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
  {
    sourceId: 'telegram-solana-fomo-calls',
    displayName: 'SOLANA FOMO Calls',
    username: 'solanafomocalls',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
  {
    sourceId: 'telegram-jeffs-lounge',
    displayName: "Jeff's Lounge / Solana Calls",
    username: 'jeffs_lounge',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
  {
    sourceId: 'telegram-eddy-calls',
    displayName: 'Eddy Calls',
    username: 'eddycalls',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
  {
    sourceId: 'telegram-solana-memecoins-calls',
    displayName: 'SOLANA MEMECOINS CALLS',
    username: 'solanamemecoinscalls',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
  {
    sourceId: 'telegram-serena-alpha-calls',
    displayName: 'Serena Alpha Calls',
    username: 'serenaalphacalls',
    platform: 'telegram',
    sourceRole: SOURCE_ROLES.CALLER,
    clusterRelationshipStatus: CLUSTER_RELATIONSHIP.UNKNOWN,
    collector: 'telegram-caller-feed',
  },
]);

function loadConfiguredSources() {
  const raw = process.env.TELEGRAM_CALLER_SOURCES_JSON;
  if (!raw) return DEFAULT_SOURCES.map(s => ({ ...s }));
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) throw new Error('TELEGRAM_CALLER_SOURCES_JSON must be a JSON array');
    return parsed.map(row => ({
      ...row,
      platform: row.platform || 'telegram',
      sourceRole: row.sourceRole || SOURCE_ROLES.CALLER,
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
