'use strict';

/**
 * SPEC-254 — Tenant-mailbox capacity reasoning.
 * Reuses Emmett cognition; unknown evidence stays unknown (not zero).
 */

const { scoreInboxHealth } = require('./InboxHealth');
const { recommendCapacity } = require('./Capacity');
const { evaluateGovernor } = require('./Governor');
const { GOVERNOR_OUTCOMES, WARMUP_STATUS, clamp, newId, nowIso } = require('./types');
const {
  evaluateAuthorizationSpacing,
  evaluateExecutionSpacing,
  evaluateCapacityBudget,
} = require('./TenantMailboxSpacing');
const {
  DEFAULT_MIN_SPACING_MINUTES: BOOTSTRAP_MIN_SPACING_MINUTES,
  DEFAULT_BUSINESS_HOURS: BOOTSTRAP_BUSINESS_HOURS,
} = require('./Bootstrap');

const DEFAULT_MIN_SPACING_MINUTES = 30;
const DEFAULT_SEND_WINDOW = Object.freeze({ startHour: 9, endHour: 17 });

function snapshotForEmmettCognition(snapshot = {}) {
  const adapted = { ...snapshot };
  adapted.bounceRate = snapshot.bounceRate == null ? 0 : snapshot.bounceRate;
  adapted.replyRate = snapshot.founderReplyRate != null
    ? snapshot.founderReplyRate
    : (snapshot.replyRate == null ? 0 : snapshot.replyRate);
  adapted.openRate = 0;
  adapted.complaintRate = 0;
  adapted.authentication = {
    spf: snapshot.authentication?.spf === 'UNKNOWN' ? false : snapshot.authentication?.spf,
    dkim: snapshot.authentication?.dkim === 'UNKNOWN' ? false : snapshot.authentication?.dkim,
    dmarc: snapshot.authentication?.dmarc === 'UNKNOWN' ? false : snapshot.authentication?.dmarc,
  };
  if (Number(snapshot.recentSends || 0) < 5) {
    adapted.bounceRate = 0;
  }
  return adapted;
}

function buildRiskFlags(snapshot, health, capacity, governor) {
  const flags = [];
  for (const unknown of snapshot.unknownEvidence || []) {
    flags.push(`unknown_${unknown}`);
  }
  if (snapshot.warmup?.status === WARMUP_STATUS.WARMING || snapshot.warmup?.status === 'warming') {
    flags.push('warmup_active');
  }
  if (Number(snapshot.failedSends || 0) > 0) {
    flags.push('smtp_failures_observed');
  }
  if (governor.outcome === GOVERNOR_OUTCOMES.SLOW) flags.push('slow_governor');
  if (governor.halt) flags.push(`governor_${governor.outcome}`);
  if (capacity.recommended <= 0) flags.push('zero_recommended_capacity');
  if (health.score < 70) flags.push('health_below_proceed');
  return flags;
}

function resolveMinimumSpacing(snapshot, capacity = {}) {
  if (capacity.bootstrap?.active && capacity.bootstrap.minSpacingMinutes != null) {
    return Number(capacity.bootstrap.minSpacingMinutes);
  }
  if (capacity.mode === 'bootstrap') {
    return Number(capacity.bootstrap?.minSpacingMinutes || BOOTSTRAP_MIN_SPACING_MINUTES);
  }
  const ramp = snapshot.warmup?.rampStage;
  if (ramp === 'early') return 60;
  if (ramp === 'mid') return 45;
  return DEFAULT_MIN_SPACING_MINUTES;
}

function resolveAllowedSendWindow(snapshot, opts = {}, capacity = {}) {
  const bootstrapWindow = capacity.bootstrap?.active
    ? (capacity.bootstrap.businessHours || BOOTSTRAP_BUSINESS_HOURS)
    : null;
  return {
    ...(opts.allowedSendWindow || bootstrapWindow || DEFAULT_SEND_WINDOW),
    timezone: snapshot.timeZone || opts.timeZone || 'America/New_York',
  };
}

function isWithinAllowedWindow(scheduledFor, window, timeZone) {
  const date = scheduledFor instanceof Date ? scheduledFor : new Date(scheduledFor);
  if (Number.isNaN(date.getTime())) return false;
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: window.timezone || timeZone || 'America/New_York',
    hour: 'numeric',
    hour12: false,
  }).formatToParts(date);
  const hour = Number(parts.find((p) => p.type === 'hour')?.value || 0);
  const start = Number(window.startHour ?? 9);
  const end = Number(window.endHour ?? 17);
  return hour >= start && hour < end;
}

function minutesSinceLastSend(recentTimestamps = [], now) {
  if (!recentTimestamps.length) return null;
  const latest = new Date(recentTimestamps[0]).getTime();
  const nowMs = (now instanceof Date ? now : new Date(now)).getTime();
  if (Number.isNaN(latest)) return null;
  return Math.floor((nowMs - latest) / 60000);
}

function assessTenantMailboxCapacity(snapshot = {}, opts = {}) {
  const adapted = snapshotForEmmettCognition(snapshot);
  const health = scoreInboxHealth(adapted);
  const capacity = recommendCapacity(snapshot, health);
  const governor = evaluateGovernor(adapted, health, capacity);
  const riskFlags = buildRiskFlags(snapshot, health, capacity, governor);
  const minimumSpacingMinutes = resolveMinimumSpacing(snapshot, capacity);
  const allowedSendWindow = resolveAllowedSendWindow(snapshot, opts, capacity);

  let maxSendsPerDay = Number(capacity.recommended || 0);
  if (governor.outcome === GOVERNOR_OUTCOMES.SLOW && governor.slowCap != null) {
    maxSendsPerDay = Math.min(maxSendsPerDay, Number(governor.slowCap));
  }
  if (governor.halt) maxSendsPerDay = 0;

  const sentCount = Number(snapshot.sentToday || 0);
  const scheduledCount = Number(snapshot.scheduledSends || 0);
  const executingCount = Number(snapshot.executingSends || 0);
  const consumed = sentCount + scheduledCount + executingCount;
  const remainingCapacity = Math.max(0, maxSendsPerDay - consumed);

  return {
    health,
    capacity,
    governor,
    riskFlags,
    minimumSpacingMinutes,
    allowedSendWindow,
    maxSendsPerDay,
    currentSentCount: sentCount,
    currentScheduledCount: scheduledCount,
    currentExecutingCount: executingCount,
    remainingCapacity,
    consumed,
    emmettContribution: {
      kind: 'tenant_mailbox_capacity',
      spec: 'SPEC-254',
      health,
      capacity,
      governor,
      channel: 'tenant_mailbox_smtp',
    },
  };
}

function buildCapacityEnvelope(snapshot, assessment, opts = {}) {
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const validHours = Number(opts.validHours || 6);
  const validUntil = new Date(now.getTime() + validHours * 3600000);

  return {
    envelopeId: opts.envelopeId || newId('env'),
    tenantId: snapshot.tenantId,
    mailboxIntegrationId: snapshot.mailboxIntegrationId,
    sendingIdentityId: snapshot.sendingIdentityId,
    senderEmail: snapshot.senderEmail,
    sendingDomain: snapshot.sendingDomain,
    localDate: snapshot.localDate,
    computedAt: nowIso(now),
    validFrom: nowIso(now),
    validUntil: nowIso(validUntil),
    maxSendsPerDay: assessment.maxSendsPerDay,
    maxSendsPerWindow: assessment.maxSendsPerDay,
    minimumSpacingMinutes: assessment.minimumSpacingMinutes,
    allowedSendWindow: assessment.allowedSendWindow,
    rampStage: snapshot.warmup?.rampStage || null,
    currentSentCount: assessment.currentSentCount,
    currentScheduledCount: assessment.currentScheduledCount,
    currentExecutingCount: assessment.currentExecutingCount,
    remainingCapacity: assessment.remainingCapacity,
    governorState: assessment.governor.outcome,
    riskFlags: assessment.riskFlags,
    evidenceSnapshot: {
      channel: snapshot.channel,
      successfulSends: snapshot.successfulSends,
      failedSends: snapshot.failedSends,
      hardBounces: snapshot.hardBounces,
      replies: snapshot.replies,
      founderReplies: snapshot.founderReplies,
      roleReplies: snapshot.roleReplies,
      unknownEvidence: snapshot.unknownEvidence,
      bounceRate: snapshot.bounceRate,
      replyRate: snapshot.replyRate,
      founderReplyRate: snapshot.founderReplyRate,
      openRate: snapshot.openRate,
      deliveryRate: snapshot.deliveryRate,
      inboxAgeDays: snapshot.inboxAgeDays,
      warmup: snapshot.warmup,
    },
    emmettContribution: assessment.emmettContribution,
    version: Number(opts.version || 1),
  };
}

function evaluateCapacityAuthorization(envelope, input = {}, opts = {}) {
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const scheduledFor = input.scheduledFor ? new Date(input.scheduledFor) : null;

  if (!envelope) {
    return blocked('emmett_envelope_missing', 'No Emmett capacity envelope exists for this sending identity.');
  }
  if (new Date(envelope.validUntil).getTime() <= now.getTime()) {
    return blocked('emmett_envelope_expired', 'Emmett capacity envelope has expired.');
  }
  if (envelope.governorState === GOVERNOR_OUTCOMES.PAUSE || envelope.governorState === GOVERNOR_OUTCOMES.EMERGENCY) {
    return blocked(`emmett_governor_${envelope.governorState}`, `Emmett governor is ${envelope.governorState}. Operator authorization cannot override capacity.`);
  }
  const budget = evaluateCapacityBudget(envelope, input);
  if (!budget.allowed) return budget;
  if (scheduledFor && !isWithinAllowedWindow(scheduledFor, envelope.allowedSendWindow, envelope.allowedSendWindow?.timezone)) {
    return blocked('emmett_outside_send_window', 'Requested send time is outside the allowed send window.');
  }
  const spacing = evaluateAuthorizationSpacing(envelope, input);
  if (!spacing.allowed) return spacing;
  return { allowed: true, envelope };
}

function evaluateCapacityExecution(envelope, input = {}, opts = {}) {
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());

  if (!envelope) {
    return blocked('emmett_envelope_missing', 'No Emmett capacity envelope exists for this sending identity.');
  }
  if (new Date(envelope.validUntil).getTime() <= now.getTime()) {
    return blocked('emmett_envelope_expired', 'Emmett capacity envelope expired before execution.');
  }
  if (envelope.governorState === GOVERNOR_OUTCOMES.PAUSE || envelope.governorState === GOVERNOR_OUTCOMES.EMERGENCY) {
    return blocked(`emmett_governor_${envelope.governorState}`, `Emmett governor moved to ${envelope.governorState} after authorization.`);
  }
  const budget = evaluateCapacityBudget(envelope, {
    alreadyConsumesCapacity: input.alreadyConsumesCapacity === true,
  });
  if (!budget.allowed) {
    return blocked('emmett_capacity_exhausted', 'Capacity exhausted since authorization.');
  }
  const scheduledFor = input.scheduledFor ? new Date(input.scheduledFor) : null;
  if (scheduledFor && !isWithinAllowedWindow(scheduledFor, envelope.allowedSendWindow, envelope.allowedSendWindow?.timezone)) {
    return blocked('emmett_outside_send_window', 'Requested send time is outside the allowed send window.');
  }
  const spacing = evaluateExecutionSpacing(envelope, input);
  if (!spacing.allowed) return spacing;
  return { allowed: true, envelope, action: 'send' };
}

function blocked(code, message) {
  return { allowed: false, code, reason: message };
}

module.exports = {
  assessTenantMailboxCapacity,
  buildCapacityEnvelope,
  evaluateCapacityAuthorization,
  evaluateCapacityExecution,
  snapshotForEmmettCognition,
  isWithinAllowedWindow,
  minutesSinceLastSend,
  DEFAULT_MIN_SPACING_MINUTES,
  DEFAULT_SEND_WINDOW,
  ...require('./TenantMailboxSpacing'),
};
