'use strict';

const { mapPlatformToOutcomeChannel } = require('./types');

function localParts(iso, timezone = 'America/New_York') {
  const date = new Date(iso);
  if (!Number.isFinite(date.getTime())) return { dow: 0, hour: 9 };
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    weekday: 'short',
    hour: 'numeric',
    hour12: false,
  }).formatToParts(date);
  const weekday = fmt.find((p) => p.type === 'weekday')?.value || 'Sun';
  const hour = Number(fmt.find((p) => p.type === 'hour')?.value || 9);
  const map = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return { dow: map[weekday] ?? 0, hourLocal: hour };
}

async function ingestPublicationPerformance(input = {}, deps = {}) {
  const store = deps.store;
  if (!store) throw new Error('store_required');
  const {
    clientId,
    platform,
    contentCategory = '',
    format = '',
    publishedAt,
    impressions = 0,
    reactions = 0,
    comments = 0,
    shares = 0,
    clicks = 0,
    timezone = 'America/New_York',
  } = input;
  const engagement = reactions + comments + shares + clicks;
  const { dow, hourLocal } = localParts(publishedAt, timezone);
  await store.upsertPlatformSignal({
    clientId,
    platform,
    contentCategory,
    format,
    dow,
    hourLocal,
    sampleCount: 1,
    impressionsSum: impressions,
    engagementSum: engagement,
    engagementRateAvg: impressions > 0 ? engagement / impressions : null,
    lastObservedAt: publishedAt || new Date().toISOString(),
  });
  return { clientId, platform, dow, hourLocal, engagement, impressions };
}

async function ingestContentOutcomePublication(outcome = {}, deps = {}) {
  const platformMap = {
    facebook: 'facebook_page',
    linkedin: 'linkedin_page',
    gbp: 'google_business',
  };
  const platform = platformMap[outcome.channel] || outcome.platform;
  if (!platform) return null;
  const snapshot = outcome.latestPerformance || outcome.performance || {};
  return ingestPublicationPerformance({
    clientId: outcome.clientId,
    platform,
    contentCategory: outcome.contentCategory || outcome.topic || '',
    format: outcome.format || '',
    publishedAt: outcome.publishedAt,
    impressions: snapshot.impressions || 0,
    reactions: snapshot.reactions || 0,
    comments: snapshot.comments || 0,
    shares: snapshot.reposts || snapshot.shares || 0,
    clicks: snapshot.clicks || 0,
  }, deps);
}

module.exports = {
  ingestPublicationPerformance,
  ingestContentOutcomePublication,
  mapPlatformToOutcomeChannel,
};
