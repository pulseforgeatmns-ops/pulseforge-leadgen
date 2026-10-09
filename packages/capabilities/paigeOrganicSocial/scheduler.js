'use strict';

const { SCHEDULING_PRIORS } = require('./types');

const WEEKDAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function scoreSlot({ platform, category, format, dow, hour, minute }, signals = [], now = new Date()) {
  const matching = signals.filter(
    (s) => s.platform === platform
      && (s.contentCategory === category || s.contentCategory === '')
      && (s.format === format || s.format === '')
      && s.dow === dow
      && s.hourLocal === hour
  );
  if (matching.length) {
    const best = matching.reduce((a, b) => (a.engagementRateAvg || 0) > (b.engagementRateAvg || 0) ? a : b);
    const rate = Number(best.engagementRateAvg || 0);
    const samples = best.sampleCount || 0;
    const sampleBoost = Math.min(0.35, samples * 0.06);
    const evidenceWeight = samples >= 2 ? 0.65 : 0.15;
    return {
      score: rate + sampleBoost + evidenceWeight,
      source: 'anchor_history',
      sampleCount: samples,
    };
  }
  const priors = SCHEDULING_PRIORS[platform] || [];
  const prior = priors.find((p) => p.dow === dow && p.hour === hour && (p.minute || 0) === (minute || 0));
  if (prior) {
    return { score: prior.weight * 0.5, source: 'initial_prior', sampleCount: 0 };
  }
  // Soft weekday morning bias without hard-coding a single global time.
  const weekday = dow >= 1 && dow <= 5;
  const morning = hour >= 7 && hour <= 10;
  return { score: weekday && morning ? 0.35 : 0.2, source: 'exploration_default', sampleCount: 0 };
}

function recommendPublishWindow(input = {}, deps = {}) {
  const {
    platform,
    contentCategory,
    format,
    timezone = 'America/New_York',
    exploration = false,
    now = new Date(),
  } = input;
  const signals = deps.signals || [];
  const candidates = [];
  const base = new Date(now);
  for (let dayOffset = 1; dayOffset <= 14; dayOffset += 1) {
    for (let hour = 7; hour <= 17; hour += 1) {
      for (const minute of [0, 15, 30, 45]) {
        const candidate = new Date(base);
        candidate.setDate(candidate.getDate() + dayOffset);
        candidate.setHours(hour, minute, 0, 0);
        const dow = candidate.getDay();
        const scored = scoreSlot({ platform, category: contentCategory, format, dow, hour, minute }, signals, now);
        candidates.push({ at: candidate, dow, hour, minute, ...scored });
      }
    }
  }
  candidates.sort((a, b) => b.score - a.score || a.at - b.at);
  let pick = candidates[0];
  if (exploration && candidates.length > 3) {
    const explorePool = candidates.slice(Math.floor(candidates.length * 0.4), Math.floor(candidates.length * 0.75));
    pick = explorePool[Math.floor(Math.random() * explorePool.length)] || pick;
  }
  const dayName = WEEKDAY_NAMES[pick.dow];
  const hh = String(pick.hour).padStart(2, '0');
  const mm = String(pick.minute).padStart(2, '0');
  let rationale;
  if (pick.source === 'anchor_history' && pick.sampleCount >= 2) {
    rationale = `I'm recommending ${dayName} at ${hh}:${mm} for ${platformLabel(platform)} because Anchor's recent ${humanCategory(contentCategory)} posts in this ${formatLabel(format)} format have performed better in that window (${pick.sampleCount} observed posts).`;
  } else if (pick.source === 'initial_prior') {
    rationale = `I'm recommending ${dayName} at ${hh}:${mm} for ${platformLabel(platform)} using Anchor's initial scheduling prior while performance history is still thin.`;
  } else {
    rationale = `I'm recommending ${dayName} at ${hh}:${mm} for ${platformLabel(platform)} as a controlled timing experiment while Anchor builds more evidence.`;
  }
  return {
    proposedPublishAt: pick.at.toISOString(),
    schedulingRationale: rationale,
    meta: {
      timezone,
      exploration,
      score: pick.score,
      source: pick.source,
      sampleCount: pick.sampleCount,
    },
  };
}

function platformLabel(platform) {
  return {
    facebook_page: 'Facebook',
    linkedin_page: 'LinkedIn',
    google_business: 'Google Business Profile',
  }[platform] || platform;
}

function humanCategory(category) {
  return String(category || '').replace(/_/g, ' ');
}

function formatLabel(format) {
  return String(format || '').replace(/_/g, ' ');
}

module.exports = {
  recommendPublishWindow,
  scoreSlot,
};
