'use strict';

/**
 * SPEC-117 — explainable Inbox Health 0–100.
 * Never return a bare score.
 */

const { HEALTH_LABELS, WARMUP_STATUS, clamp, round1, pct } = require('./types');

const FACTOR_MAX = Object.freeze({
  spf: 8,
  dkim: 8,
  dmarc: 8,
  warmup: 12,
  bounce: 14,
  reply: 10,
  open: 8,
  age: 8,
  blacklist: 12,
  velocity: 6,
  consistency: 4,
  operator_override: 2,
});

function authPass(value) {
  const v = String(value || '').toLowerCase();
  return value === true
    || v === 'pass'
    || v === 'valid'
    || v === 'yes'
    || v === 'authenticated'
    || v === 'reject'
    || v === 'quarantine';
}

function dmarcScore(value) {
  const v = String(value || '').toLowerCase();
  if (v === 'reject' || v === 'quarantine' || value === true || v === 'pass') return FACTOR_MAX.dmarc;
  if (v === 'none' || v === 'p=none') return 4;
  return 0;
}

function healthLabel(score) {
  if (score >= 80) return HEALTH_LABELS.HEALTHY;
  if (score >= 60) return HEALTH_LABELS.WATCH;
  if (score >= 40) return HEALTH_LABELS.DEGRADED;
  return HEALTH_LABELS.CRITICAL;
}

function factor(key, score, reason, detail) {
  const max = FACTOR_MAX[key];
  const awarded = clamp(round1(score), 0, max);
  return {
    key,
    score: awarded,
    max,
    reason,
    detail: detail || null,
    weak: awarded < max * 0.5,
  };
}

function scoreInboxHealth(snapshot = {}) {
  const auth = snapshot.authentication || {};
  const warmup = snapshot.warmup || {};
  const bounceRate = pct(snapshot.bounceRate);
  const replyRate = pct(snapshot.replyRate);
  const openRate = pct(snapshot.openRate);
  const complaintRate = pct(snapshot.complaintRate);
  const ageDays = Math.max(0, Number(snapshot.inboxAgeDays || 0));
  const listed = snapshot.blacklist?.listed === true;
  const sentToday = Number(snapshot.sentToday || 0);
  const sentYesterday = Number(snapshot.sentYesterday || 0);
  const historicalDailyAvg = Number(snapshot.historicalDailyAvg || 0);
  const override = snapshot.operatorOverride || null;

  const factors = [];

  factors.push(factor(
    'spf',
    authPass(auth.spf) ? FACTOR_MAX.spf : 0,
    authPass(auth.spf) ? 'SPF authenticated' : 'SPF missing or failing'
  ));
  factors.push(factor(
    'dkim',
    authPass(auth.dkim) ? FACTOR_MAX.dkim : 0,
    authPass(auth.dkim) ? 'DKIM authenticated' : 'DKIM missing or failing'
  ));
  factors.push(factor(
    'dmarc',
    dmarcScore(auth.dmarc),
    dmarcScore(auth.dmarc) === FACTOR_MAX.dmarc
      ? 'DMARC policy in force'
      : dmarcScore(auth.dmarc) === 4
        ? 'DMARC present but p=none'
        : 'DMARC missing or failing'
  ));

  const warmupStatus = String(warmup.status || WARMUP_STATUS.NONE);
  let warmupPts = 6;
  let warmupReason = 'No warmup program recorded';
  if (warmupStatus === WARMUP_STATUS.HEALTHY) {
    warmupPts = FACTOR_MAX.warmup;
    warmupReason = 'Warm-up healthy';
  } else if (warmupStatus === WARMUP_STATUS.WARMING) {
    warmupPts = 8;
    warmupReason = 'Domain still warming';
  } else if (warmupStatus === WARMUP_STATUS.STALLED) {
    warmupPts = 2;
    warmupReason = 'Warm-up stalled';
  } else if (ageDays >= 30) {
    warmupPts = 10;
    warmupReason = 'Mature inbox without an active warmup program';
  } else if (ageDays < 14) {
    warmupPts = 4;
    warmupReason = 'Young inbox still establishing reputation';
  }
  factors.push(factor('warmup', warmupPts, warmupReason, { status: warmupStatus }));

  const bouncePts = bounceRate <= 0
    ? FACTOR_MAX.bounce
    : bounceRate >= 0.05
      ? 0
      : FACTOR_MAX.bounce * (1 - bounceRate / 0.05);
  factors.push(factor(
    'bounce',
    bouncePts,
    bounceRate <= 0
      ? 'No recent bounces'
      : bounceRate >= 0.02
        ? 'High bounce rate this week'
        : 'Bounce rate within watch range',
    { bounceRate }
  ));

  const replyPts = clamp((replyRate / 0.10) * FACTOR_MAX.reply, 0, FACTOR_MAX.reply);
  factors.push(factor(
    'reply',
    replyPts,
    replyRate >= 0.08 ? 'Reply rate supports current volume' : 'Reply rate is thin for this volume',
    { replyRate }
  ));

  const openPts = clamp((openRate / 0.55) * FACTOR_MAX.open, 0, FACTOR_MAX.open);
  factors.push(factor(
    'open',
    openPts,
    openRate >= 0.5 ? 'Open rate is healthy' : 'Open rate is below a healthy floor',
    { openRate }
  ));

  const agePts = clamp((ageDays / 60) * FACTOR_MAX.age, 0, FACTOR_MAX.age);
  factors.push(factor(
    'age',
    agePts,
    ageDays >= 45 ? 'Inbox age supports gradual volume' : 'Inbox is still young',
    { inboxAgeDays: ageDays }
  ));

  factors.push(factor(
    'blacklist',
    listed ? 0 : FACTOR_MAX.blacklist,
    listed ? 'Possible blacklist — stop immediately' : 'No blacklist concerns',
    { listed, sources: snapshot.blacklist?.sources || [] }
  ));

  const velocityRatio = sentYesterday > 0 ? sentToday / sentYesterday : (historicalDailyAvg > 0 ? sentToday / historicalDailyAvg : 1);
  let velocityPts = FACTOR_MAX.velocity;
  let velocityReason = 'Sending velocity is consistent';
  if (velocityRatio >= 3) {
    velocityPts = 0;
    velocityReason = 'Rapid increase in sending';
  } else if (velocityRatio >= 2) {
    velocityPts = 2;
    velocityReason = 'Rapid increase in sending';
  } else if (complaintRate > 0) {
    velocityPts = 1;
    velocityReason = 'Spam complaints present — velocity is unsafe';
  }
  factors.push(factor('velocity', velocityPts, velocityReason, { velocityRatio: round1(velocityRatio) }));

  const consistencyPts = historicalDailyAvg <= 0
    ? 2
    : Math.abs(sentToday - historicalDailyAvg) / Math.max(historicalDailyAvg, 1) > 1.5
      ? 1
      : FACTOR_MAX.consistency;
  factors.push(factor(
    'consistency',
    consistencyPts,
    consistencyPts === FACTOR_MAX.consistency
      ? 'Historical sending is consistent'
      : 'Historical sending is bursty',
    { historicalDailyAvg, sentToday }
  ));

  const overridePts = override?.pause ? 0 : FACTOR_MAX.operator_override;
  factors.push(factor(
    'operator_override',
    overridePts,
    override?.pause ? 'Operator paused sending' : 'No operator pause',
    override
  ));

  const score = Math.round(factors.reduce((sum, row) => sum + row.score, 0));
  const reasons = factors
    .filter((row) => row.weak || /high bounce|rapid increase|still warming|possible blacklist|operator paused|stalled|spam complaints/i.test(row.reason))
    .map((row) => row.reason);
  const blacklistReason = factors.find((row) => row.key === 'blacklist')?.reason;
  if (blacklistReason && !reasons.includes(blacklistReason)) reasons.push(blacklistReason);
  const positives = factors.filter((row) => !row.weak).map((row) => row.reason);

  return {
    kind: 'inbox_health',
    spec: 'SPEC-117',
    score: clamp(score, 0, 100),
    label: healthLabel(score),
    reasons: reasons.length ? reasons : positives.slice(0, 3),
    positives,
    factors,
    bounceRate,
    replyRate,
    openRate,
    complaintRate,
    inboxAgeDays: ageDays,
  };
}

module.exports = {
  FACTOR_MAX,
  scoreInboxHealth,
  healthLabel,
  authPass,
};
