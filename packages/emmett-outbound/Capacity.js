'use strict';

/**
 * SPEC-117 — Capacity Intelligence.
 * Provider ceilings are caps, not recommendations.
 */

const { WARMUP_STATUS, clamp, round2, pct } = require('./types');
const { authPass } = require('./InboxHealth');

function recommendCapacity(snapshot = {}, health = {}) {
  const ceiling = Math.max(1, Number(snapshot.providerCeiling || 50));
  const warmup = snapshot.warmup || {};
  const warmupCap = Number(warmup.dailyCap || ceiling);
  const hardCap = Math.min(ceiling, warmupCap);
  const ageDays = Math.max(0, Number(snapshot.inboxAgeDays || health.inboxAgeDays || 0));
  const bounceRate = pct(snapshot.bounceRate ?? health.bounceRate);
  const replyRate = pct(snapshot.replyRate ?? health.replyRate);
  const openRate = pct(snapshot.openRate ?? health.openRate);
  const complaintRate = pct(snapshot.complaintRate ?? health.complaintRate);
  const auth = snapshot.authentication || {};
  const authenticated = authPass(auth.spf) && authPass(auth.dkim) && authPass(auth.dmarc);
  const listed = snapshot.blacklist?.listed === true;
  const healthScore = Number(health.score || 0);
  const warmupStatus = String(warmup.status || WARMUP_STATUS.NONE);

  if (listed || complaintRate >= 0.001 || (bounceRate >= 0.04 && Number(snapshot.recentSends || 20) >= 20)) {
    return explainCapacity({
      recommended: 0,
      ceiling: hardCap,
      confidence: 0.95,
      outlook: 'pause',
      snapshot,
      health,
      factors: [
        listed ? 'Possible blacklist' : null,
        complaintRate >= 0.001 ? 'Spam complaints detected' : null,
        bounceRate >= 0.04 ? 'Bounce rate above emergency threshold' : null,
      ].filter(Boolean),
    });
  }

  const ageFactor = clamp(ageDays / 85, 0.08, 1);
  const healthFactor = clamp(healthScore / 100, 0.2, 1);
  const authFactor = authenticated ? 1 : 0.55;
  const bounceFactor = bounceRate <= 0 ? 1 : bounceRate >= 0.02 ? 0.35 : 0.7;
  const replyFactor = clamp(0.75 + replyRate * 2.2, 0.75, 1.12);
  const openFactor = clamp(0.8 + openRate * 0.4, 0.8, 1.08);
  let warmupFactor = 0.7;
  if (warmupStatus === WARMUP_STATUS.HEALTHY) warmupFactor = 0.8;
  else if (warmupStatus === WARMUP_STATUS.WARMING) warmupFactor = 0.45;
  else if (warmupStatus === WARMUP_STATUS.STALLED) warmupFactor = 0.15;

  const raw = hardCap * ageFactor * healthFactor * authFactor * bounceFactor * replyFactor * openFactor * warmupFactor;
  let recommended = Math.max(0, Math.round(raw));
  if (healthScore < 40) recommended = 0;
  else if (healthScore < 60) recommended = Math.min(recommended, Math.max(1, Math.round(hardCap * 0.2)));
  recommended = Math.min(recommended, hardCap);

  const confidence = round2(clamp(
    0.40
      + (authenticated ? 0.12 : 0)
      + (bounceRate === 0 ? 0.10 : 0)
      + (complaintRate === 0 ? 0.06 : 0)
      + (warmupStatus === WARMUP_STATUS.HEALTHY ? 0.08 : 0)
      + (ageDays >= 40 ? 0.08 : 0)
      - (warmupStatus === WARMUP_STATUS.WARMING ? 0.12 : 0),
    0.2,
    0.95
  ));

  let outlook = 'stable';
  if (recommended === 0) outlook = 'pause';
  else if (warmupStatus === WARMUP_STATUS.HEALTHY && bounceRate === 0 && healthScore >= 80) outlook = 'increase';
  else if (warmupStatus === WARMUP_STATUS.WARMING || bounceRate >= 0.015 || healthScore < 70) outlook = 'decrease';

  const factors = [
    `Inbox age: ${ageDays} days`,
    authenticated ? 'Domain: Properly authenticated' : 'Domain: Authentication incomplete',
    `Reply rate: ${(replyRate * 100).toFixed(1)}%`,
    `Open rate: ${Math.round(openRate * 100)}%`,
    `Recent bounces: ${bounceRate === 0 ? 0 : `${(bounceRate * 100).toFixed(1)}%`}`,
    `Spam complaints: ${complaintRate === 0 ? 0 : `${(complaintRate * 100).toFixed(2)}%`}`,
    `Warm-up: ${warmupStatus === WARMUP_STATUS.HEALTHY ? 'Healthy' : warmupStatus}`,
  ];

  return explainCapacity({
    recommended,
    ceiling: hardCap,
    confidence,
    outlook,
    snapshot,
    health,
    factors,
  });
}

function explainCapacity({ recommended, ceiling, confidence, outlook, snapshot, health, factors }) {
  const tomorrow = outlookTomorrow(recommended, outlook);
  return {
    kind: 'capacity',
    spec: 'SPEC-117',
    recommended,
    ceiling,
    confidence,
    outlook,
    tomorrow,
    statement: recommended > 0
      ? `Based on today's reputation, I recommend ${recommended}.`
      : 'Based on today\'s reputation, I recommend pausing entirely.',
    factors,
    healthScore: health.score,
    healthReasons: health.reasons || [],
    inboxAgeDays: Number(snapshot.inboxAgeDays || 0),
  };
}

function outlookTomorrow(recommended, outlook) {
  if (outlook === 'pause') return { low: 0, high: 0, note: 'Pause entirely until reputation recovers.' };
  if (outlook === 'increase') {
    const next = Math.max(recommended + 4, Math.round(recommended * 1.35));
    return { low: recommended, high: next, note: `Tomorrow this might become ${next}.` };
  }
  if (outlook === 'decrease') {
    const next = Math.max(0, Math.round(recommended * 0.7));
    return { low: next, high: recommended, note: `Tomorrow this might become ${next} or pause entirely.` };
  }
  return { low: Math.max(0, recommended - 4), high: recommended + 4, note: `Tomorrow this might become ${recommended + 4} or ${Math.max(0, recommended - 7)}.` };
}

module.exports = {
  recommendCapacity,
};
