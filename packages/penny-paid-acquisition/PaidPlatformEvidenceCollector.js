'use strict';

/**
 * SPEC-252 — Canonical read-only paid platform evidence collector.
 */

const {
  PLATFORM,
  PLATFORM_BY_CHANNEL,
  CHANNEL_BY_PLATFORM,
  AVAILABILITY,
  UNAVAILABLE_REASON,
  unavailableEvidence,
} = require('./types');
const { resolveAdAccountsForClient, accountUnavailableReason } = require('./accountResolution');
const { readGoogleAdsEvidence } = require('./adapters/googleAds');
const { readMetaAdsEvidence } = require('./adapters/metaAds');
const { readChatGptAdsEvidence, readYelpAdsEvidence } = require('./adapters/stubPlatform');

const DEFAULT_CHANNELS = Object.freeze([
  'Google Search',
  'ChatGPT Ads',
  'Yelp',
  'Meta',
]);

const PLATFORM_DB_ALIASES = Object.freeze({
  google_ads: PLATFORM.GOOGLE_ADS,
  google: PLATFORM.GOOGLE_ADS,
  meta_ads: PLATFORM.META_ADS,
  meta: PLATFORM.META_ADS,
  facebook: PLATFORM.META_ADS,
});

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeChannelList(channels) {
  const list = Array.isArray(channels) && channels.length ? channels : DEFAULT_CHANNELS.slice();
  return list.map((row) => {
    if (typeof row === 'string') return row;
    return asText(row.name || row.channel || row.label || row.id);
  }).filter(Boolean);
}

function platformForChannel(channel) {
  const text = asText(channel);
  if (PLATFORM_BY_CHANNEL[text]) return PLATFORM_BY_CHANNEL[text];
  const lower = text.toLowerCase();
  if (/google|search/.test(lower)) return PLATFORM.GOOGLE_ADS;
  if (/chatgpt|openai/.test(lower)) return PLATFORM.CHATGPT_ADS;
  if (/yelp/.test(lower)) return PLATFORM.YELP;
  if (/meta|facebook|instagram/.test(lower)) return PLATFORM.META_ADS;
  return null;
}

function dbPlatformForCanonical(platform) {
  if (platform === PLATFORM.GOOGLE_ADS) return 'google_ads';
  if (platform === PLATFORM.META_ADS) return 'meta_ads';
  return platform;
}

function stripSecrets(value) {
  if (value == null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(stripSecrets);
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (/token|secret|password|credential|refresh_token|access_token/i.test(key)) continue;
    out[key] = stripSecrets(val);
  }
  return out;
}

async function readPlatformEvidence(platform, account, opts = {}) {
  switch (platform) {
    case PLATFORM.GOOGLE_ADS:
      return readGoogleAdsEvidence({ account, window: opts.window, windowDays: opts.windowDays, http: opts.http });
    case PLATFORM.META_ADS:
      return readMetaAdsEvidence({ account, window: opts.window, windowDays: opts.windowDays, http: opts.http });
    case PLATFORM.CHATGPT_ADS:
      return readChatGptAdsEvidence();
    case PLATFORM.YELP:
      return readYelpAdsEvidence();
    default:
      return unavailableEvidence(platform, UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED);
  }
}

/**
 * Collect read-only platform evidence for requested channels.
 *
 * @param {object} input
 * @param {string|number} input.tenantId
 * @param {number|string} input.clientId
 * @param {string[]} [input.channels]
 * @param {object} [input.window]
 * @param {import('pg').Pool} [input.pool]
 * @param {Function} [input.resolveAccounts]
 * @param {object} [input.http]
 * @returns {Promise<object[]>}
 */
async function collectPaidPlatformEvidence(input = {}) {
  const clientId = Number(input.clientId != null ? input.clientId : input.tenantId);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    throw new Error('collectPaidPlatformEvidence requires a valid clientId/tenantId');
  }

  const channels = normalizeChannelList(input.channels);
  const platforms = [...new Set(channels.map(platformForChannel).filter(Boolean))];

  const accounts = await resolveAdAccountsForClient({
    clientId,
    pool: input.pool,
    queryAccounts: input.resolveAccounts,
  });

  const accountsByPlatform = new Map();
  for (const account of accounts) {
    const canonical = PLATFORM_DB_ALIASES[asText(account.platform).toLowerCase()] || account.platform;
    if (!accountsByPlatform.has(canonical)) accountsByPlatform.set(canonical, account);
  }

  const results = [];
  for (const platform of platforms) {
    const account = accountsByPlatform.get(platform);
    if ([PLATFORM.CHATGPT_ADS, PLATFORM.YELP].includes(platform)) {
      results.push(stripSecrets(await readPlatformEvidence(platform, null, input)));
      continue;
    }

    if (!account) {
      results.push(stripSecrets(unavailableEvidence(platform, UNAVAILABLE_REASON.NO_LINKED_ACCOUNT, {
        clientId,
      })));
      continue;
    }

    const missingReason = accountUnavailableReason(account, dbPlatformForCanonical(platform));
    if (missingReason) {
      results.push(stripSecrets(unavailableEvidence(platform, missingReason, { clientId })));
      continue;
    }

    const evidence = await readPlatformEvidence(platform, account, input);
    results.push(stripSecrets(evidence));
  }

  return results;
}

/**
 * Merge operator-supplied and observed platform evidence.
 * Observed API evidence wins for platform metrics; operator rows remain supplemental.
 *
 * @param {object[]} operatorSupplied
 * @param {object[]} observed
 * @returns {object[]}
 */
function mergePlatformEvidence(operatorSupplied = [], observed = []) {
  const byChannel = new Map();

  for (const row of observed) {
    const channel = asText(row.channel || CHANNEL_BY_PLATFORM[row.platform] || row.platform);
    if (!channel) continue;
    byChannel.set(channel, {
      ...row,
      provenance: row.provenance || { sourceKind: 'PLATFORM_API', readOnly: true },
    });
  }

  for (const row of operatorSupplied || []) {
    const channel = asText(row.channel || row.name || row.platform);
    if (!channel) continue;
    const canonicalChannel = Object.values(CHANNEL_BY_PLATFORM).includes(channel)
      ? channel
      : (CHANNEL_BY_PLATFORM[row.platform] || channel);
    const existing = byChannel.get(canonicalChannel);

    const operatorRow = {
      ...row,
      provenance: {
        sourceKind: 'OPERATOR_SUPPLIED',
        readOnly: true,
        ...(row.provenance || {}),
      },
    };

    if (existing && existing.availability === AVAILABILITY.AVAILABLE) {
      existing.supplementalEvidence = [...(existing.supplementalEvidence || []), operatorRow];
      continue;
    }
    if (!existing) {
      byChannel.set(canonicalChannel, operatorRow);
    }
  }

  return [...byChannel.values()];
}

module.exports = {
  DEFAULT_CHANNELS,
  collectPaidPlatformEvidence,
  mergePlatformEvidence,
  platformForChannel,
  stripSecrets,
};
