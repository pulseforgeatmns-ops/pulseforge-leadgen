'use strict';

/**
 * SPEC-255 — evidence-gated bootstrap capacity for young tenant mailboxes.
 * Does not bypass the governor; produces bounded allowance inside Emmett.
 */

const { WARMUP_STATUS, clamp } = require('./types');
const {
  AUTH_STATE,
  authPass,
  authFail,
  allAuthPass,
  anyAuthFail,
} = require('./AuthEvidence');

const BOOTSTRAP_MODE = 'bootstrap';
const NORMAL_MODE = 'normal';

function isTenantMailboxSnapshot(snapshot = {}) {
  return snapshot.deliverabilityObservability === 'limited'
    || Boolean(snapshot.sendingIdentityId)
    || snapshot.mailboxKind === 'tenant_smtp';
}

const DEFAULT_BUSINESS_HOURS = Object.freeze({ startHour: 9, endHour: 16 });
const DEFAULT_MIN_SPACING_MINUTES = 240;
const ESTABLISHED_AGE_DAYS = 30;
const ESTABLISHED_ACTIVE_SEND_DAYS = 14;
const ESTABLISHED_TOTAL_SENDS = 50;
const EXIT_AGE_DAYS = 14;
const EXIT_ACTIVE_SEND_DAYS = 7;
const EXIT_TOTAL_SENDS = 15;

function isEstablishedMailbox(snapshot = {}) {
  const ageDays = Number(snapshot.inboxAgeDays || 0);
  const activeSendDays = Number(snapshot.warmup?.activeSendDays || 0);
  const totalSends = Number(snapshot.totalOperationalSends ?? snapshot.recentSends ?? 0);
  return ageDays >= ESTABLISHED_AGE_DAYS
    && activeSendDays >= ESTABLISHED_ACTIVE_SEND_DAYS
    && totalSends >= ESTABLISHED_TOTAL_SENDS;
}

function hasBootstrapNegativeEvidence(snapshot = {}) {
  if (snapshot.blacklist?.listed === true) return { blocked: true, reason: 'blacklist' };
  if (Number(snapshot.complaintRate || 0) >= 0.001) return { blocked: true, reason: 'complaint' };
  if (snapshot.suppressed === true) return { blocked: true, reason: 'suppression' };
  if (snapshot.operatorOverride?.pause) return { blocked: true, reason: 'operator_pause' };
  if (snapshot.mailboxStatus && snapshot.mailboxStatus !== 'active') {
    return { blocked: true, reason: 'mailbox_inactive' };
  }
  if (snapshot.identityStatus && snapshot.identityStatus !== 'active') {
    return { blocked: true, reason: 'identity_inactive' };
  }
  const auth = snapshot.authentication || {};
  if (authFail(auth.smtp)) return { blocked: true, reason: 'smtp_failed' };
  if (anyAuthFail(auth, ['spf', 'dkim', 'dmarc'])) return { blocked: true, reason: 'authentication_failed' };
  if (Number(snapshot.hardBounceCount || 0) > 0) return { blocked: true, reason: 'hard_bounce' };
  if (Number(snapshot.bounceRate || 0) >= 0.02 && Number(snapshot.recentSends || 0) >= 5) {
    return { blocked: true, reason: 'bounce_rate' };
  }
  if (snapshot.governorEmergency === true) return { blocked: true, reason: 'governor_emergency' };
  return { blocked: false, reason: null };
}

function assessBootstrapEligibility(snapshot = {}, health = {}, normalCapacity = {}) {
  const reasons = [];
  const negative = hasBootstrapNegativeEvidence(snapshot);
  if (negative.blocked) {
    return { eligible: false, reasons: [negative.reason], negativeEvidence: negative.reason };
  }

  if (isEstablishedMailbox(snapshot)) {
    return { eligible: false, reasons: ['established_mailbox'] };
  }

  const auth = snapshot.authentication || {};
  if (!authPass(auth.smtp)) reasons.push('smtp_not_verified');
  if (!allAuthPass(auth, ['spf', 'dkim', 'dmarc'])) reasons.push('authentication_incomplete');
  if (reasons.length) return { eligible: false, reasons };

  const warmupStatus = String(snapshot.warmup?.status || WARMUP_STATUS.NONE);
  const activeSendDays = Number(snapshot.warmup?.activeSendDays || 0);
  const genuinelyNew = warmupStatus === WARMUP_STATUS.WARMING
    || warmupStatus === WARMUP_STATUS.NONE
    || activeSendDays < EXIT_ACTIVE_SEND_DAYS;
  if (!genuinelyNew) reasons.push('warmup_not_new');

  if (Number(normalCapacity.recommended || 0) > 0) {
    reasons.push('normal_capacity_sufficient');
  }

  if (Number(health.score || 0) < 40 && Number(snapshot.recentSends || 0) >= 10) {
    reasons.push('health_critical_with_history');
  }

  return {
    eligible: reasons.length === 0,
    reasons: reasons.length ? reasons : ['bootstrap_eligible'],
  };
}

function shouldExitBootstrap(snapshot = {}, health = {}) {
  const ageDays = Number(snapshot.inboxAgeDays || 0);
  const activeSendDays = Number(snapshot.warmup?.activeSendDays || 0);
  const totalSends = Number(snapshot.totalOperationalSends ?? snapshot.recentSends ?? 0);

  if (isEstablishedMailbox(snapshot)) return { exit: true, reason: 'established_mailbox' };
  if (ageDays >= EXIT_AGE_DAYS && activeSendDays >= EXIT_ACTIVE_SEND_DAYS && totalSends >= EXIT_TOTAL_SENDS) {
    return { exit: true, reason: 'sufficient_operational_history' };
  }
  if (Number(snapshot.replyRate || 0) > 0 && totalSends >= 5) {
    return { exit: true, reason: 'reply_evidence' };
  }
  if (Number(health.score || 0) >= 60 && totalSends >= EXIT_TOTAL_SENDS) {
    return { exit: true, reason: 'health_and_volume' };
  }
  return { exit: false, reason: null };
}

function resolveBootstrapAllowance(snapshot = {}) {
  const warmup = snapshot.warmup || {};
  const warmupCap = Math.max(1, Number(warmup.dailyCap || snapshot.providerCeiling || 3));
  const activeSendDays = Number(warmup.activeSendDays || 0);
  const stageScaled = Math.max(1, Math.ceil(warmupCap * (activeSendDays <= 0 ? 0.34 : 0.67)));
  return Math.min(warmupCap, stageScaled);
}

function applyBootstrapCapacity(snapshot = {}, health = {}, normalCapacity = {}) {
  const exit = shouldExitBootstrap(snapshot, health);
  if (exit.exit) {
    return {
      ...normalCapacity,
      mode: NORMAL_MODE,
      bootstrap: { active: false, exitReason: exit.reason },
    };
  }

  const eligibility = assessBootstrapEligibility(snapshot, health, normalCapacity);
  if (!eligibility.eligible) {
    return {
      ...normalCapacity,
      mode: NORMAL_MODE,
      bootstrap: { active: false, eligible: false, reasons: eligibility.reasons },
    };
  }

  const allowance = resolveBootstrapAllowance(snapshot);
  const warmupCap = Math.max(1, Number(snapshot.warmup?.dailyCap || snapshot.providerCeiling || allowance));
  const recommended = Math.max(1, Math.min(allowance, warmupCap));
  const businessHours = snapshot.businessHours || DEFAULT_BUSINESS_HOURS;
  const minSpacingMinutes = Number(snapshot.bootstrapMinSpacingMinutes || DEFAULT_MIN_SPACING_MINUTES);

  return {
    ...normalCapacity,
    kind: 'capacity',
    spec: 'SPEC-255',
    mode: BOOTSTRAP_MODE,
    recommended,
    ceiling: warmupCap,
    outlook: recommended > 0 ? 'bootstrap' : 'pause',
    statement: `Bootstrap allowance: ${recommended} controlled send${recommended === 1 ? '' : 's'} today while reputation evidence accumulates.`,
    bootstrap: {
      active: true,
      eligible: true,
      allowance: recommended,
      dailyBounded: true,
      identityScoped: Boolean(snapshot.sendingIdentityId || snapshot.inboxId),
      minSpacingMinutes,
      businessHours,
      temporary: true,
      reasons: eligibility.reasons,
      exitConditions: {
        maxAgeDays: EXIT_AGE_DAYS,
        minActiveSendDays: EXIT_ACTIVE_SEND_DAYS,
        minTotalSends: EXIT_TOTAL_SENDS,
      },
    },
    factors: [
      ...(normalCapacity.factors || []),
      'Bootstrap: evidence-gated controlled sending for authenticated young mailbox',
      `Bootstrap allowance capped at warm-up ceiling (${warmupCap})`,
    ],
  };
}

function deliverabilityFactors(snapshot = {}) {
  if (snapshot.deliverabilityObservability !== 'limited') {
    return null;
  }
  return {
    openFactor: 1,
    replyFactor: 1,
    note: 'Open/reply/inbox placement unobservable for tenant SMTP — treated as UNKNOWN',
  };
}

function neutralizeUnknownDeliverability(snapshot = {}, factors = {}) {
  const neutral = deliverabilityFactors(snapshot);
  if (!neutral) return factors;
  return {
    ...factors,
    openFactor: neutral.openFactor,
    replyFactor: neutral.replyFactor,
    deliverabilityNote: neutral.note,
  };
}

module.exports = {
  BOOTSTRAP_MODE,
  NORMAL_MODE,
  isTenantMailboxSnapshot,
  DEFAULT_BUSINESS_HOURS,
  DEFAULT_MIN_SPACING_MINUTES,
  isEstablishedMailbox,
  hasBootstrapNegativeEvidence,
  assessBootstrapEligibility,
  shouldExitBootstrap,
  resolveBootstrapAllowance,
  applyBootstrapCapacity,
  deliverabilityFactors,
  neutralizeUnknownDeliverability,
};
