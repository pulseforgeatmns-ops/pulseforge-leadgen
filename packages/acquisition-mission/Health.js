'use strict';

/**
 * SPEC-118 — mission health. Not "how is outreach?"
 */

const { HEALTH_LABELS, RISK_LEVELS, BLOCKER_KINDS, round2, clone } = require('./types');
const { currentBlocker } = require('./Blockers');

function riskFrom(ctx, blocker) {
  if (blocker && blocker.kind === BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK) return RISK_LEVELS.HIGH;
  if (ctx.bounceRate && ctx.bounceRate >= 0.03) return RISK_LEVELS.HIGH;
  if (blocker && blocker.kind !== BLOCKER_KINDS.WAITING_FOR_OPERATOR) return RISK_LEVELS.MEDIUM;
  return RISK_LEVELS.LOW;
}

function healthLabel(blocker, risk) {
  if (blocker && blocker.kind === BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK) return HEALTH_LABELS.PAUSED;
  if (risk === RISK_LEVELS.HIGH) return HEALTH_LABELS.AT_RISK;
  if (blocker && blocker.kind !== BLOCKER_KINDS.WAITING_FOR_OPERATOR) return HEALTH_LABELS.BLOCKED;
  return HEALTH_LABELS.HEALTHY;
}

function buildHealth(mission, ctx, extras = {}) {
  const blocker = currentBlocker(mission.blockers || []);
  const risk = extras.risk || riskFrom(ctx, blocker);
  const label = extras.label || healthLabel(blocker, risk);
  const blockerLabel = blocker
    ? (blocker.kind === BLOCKER_KINDS.WAITING_FOR_OPERATOR ? 'Operator approval' : blocker.label)
    : null;
  const replies = Number(extras.replies || ctx.replies || 0);
  const meetings = Number(extras.meetings || ctx.meetings || 0);
  const capacityRemaining = extras.capacityRemaining != null
    ? Number(extras.capacityRemaining)
    : (ctx.capacityRemaining != null ? Number(ctx.capacityRemaining) : null);

  return {
    spec: 'SPEC-118',
    missionId: mission.id,
    label,
    status: capitalize(label),
    confidence: round2(mission.confidence),
    currentBlocker: blockerLabel,
    blocker: blocker ? clone(blocker) : null,
    risk: capitalize(risk),
    riskLevel: risk,
    capacityRemaining,
    replies,
    meetings,
    learning: extras.learning || ctx.learningSummary || null,
  };
}

function formatHealth(health) {
  const lines = [
    'Mission Health',
    '',
    health.status,
    '',
    'Confidence',
    '',
    String(health.confidence),
    '',
    'Current Blocker',
    '',
    health.currentBlocker || 'None',
    '',
    'Risk',
    '',
    health.risk,
  ];
  if (health.capacityRemaining != null) {
    lines.push('', 'Capacity', '', `${health.capacityRemaining} remaining`);
  }
  lines.push('', 'Replies', '', String(health.replies));
  lines.push('', 'Meetings', '', String(health.meetings));
  if (health.learning) {
    lines.push('', 'Learning', '', health.learning);
  }
  return lines.join('\n');
}

function capitalize(value) {
  const text = String(value || '');
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

module.exports = {
  buildHealth,
  formatHealth,
};
