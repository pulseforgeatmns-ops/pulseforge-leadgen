'use strict';

/**
 * SPEC-117 — Safe Send Governor.
 * Proceed / Slow / Pause / Emergency.
 * Max cannot override Pause or Emergency silently.
 */

const { GOVERNOR_OUTCOMES, SPECIALISTS, eoiError, pct, nowIso, newId } = require('./types');

function evaluateGovernor(snapshot = {}, health = {}, capacity = {}) {
  const bounceRate = pct(snapshot.bounceRate ?? health.bounceRate);
  const complaintRate = pct(snapshot.complaintRate ?? health.complaintRate);
  const listed = snapshot.blacklist?.listed === true;
  const recentSends = Number(snapshot.recentSends || snapshot.sentLast7Days || 0);
  const warmupStatus = String(snapshot.warmup?.status || '');
  const sentToday = Number(snapshot.sentToday || 0);
  const sentYesterday = Number(snapshot.sentYesterday || 0);
  const historicalDailyAvg = Number(snapshot.historicalDailyAvg || 0);
  const baseline = sentYesterday || historicalDailyAvg || 0;
  const rapid = baseline > 0 && sentToday >= baseline * 2;
  const recommended = Number(capacity.recommended || 0);
  const score = Number(health.score || 0);

  if (listed) {
    return decision(GOVERNOR_OUTCOMES.EMERGENCY, 'Possible blacklist. Stop immediately.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (complaintRate >= 0.001) {
    return decision(GOVERNOR_OUTCOMES.EMERGENCY, 'Spam complaints detected. Stop immediately.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (bounceRate >= 0.04 && recentSends >= 20) {
    return decision(GOVERNOR_OUTCOMES.EMERGENCY, 'Bounce rate is an emergency. Stop immediately.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (snapshot.operatorOverride?.pause) {
    return decision(GOVERNOR_OUTCOMES.PAUSE, 'Operator paused sending. Do not send.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (score < 40 || recommended <= 0) {
    return decision(GOVERNOR_OUTCOMES.PAUSE, 'Reputation risk too high. Do not send.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (bounceRate >= 0.02 && recentSends >= 20) {
    return decision(GOVERNOR_OUTCOMES.PAUSE, 'Reputation risk too high. Do not send.', {
      health, capacity, snapshot, halt: true,
    });
  }
  if (
    score < 70
    || warmupStatus === 'warming'
    || rapid
    || bounceRate >= 0.01
  ) {
    return decision(GOVERNOR_OUTCOMES.SLOW, 'Reduce today\'s volume. Increase tomorrow if metrics recover.', {
      health,
      capacity,
      snapshot,
      halt: false,
      slowCap: Math.max(1, Math.floor(recommended * 0.6)),
    });
  }
  return decision(GOVERNOR_OUTCOMES.PROCEED, 'Healthy. Safe. Send.', {
    health, capacity, snapshot, halt: false,
  });
}

function decision(outcome, reason, extra = {}) {
  return {
    kind: 'governor',
    spec: 'SPEC-117',
    outcome,
    reason,
    halt: extra.halt === true,
    slowCap: extra.slowCap || null,
    healthScore: extra.health?.score,
    recommendedCapacity: extra.capacity?.recommended,
    explain: {
      outcome,
      reason,
      health: extra.health?.score,
      healthReasons: extra.health?.reasons || [],
      capacity: extra.capacity?.recommended,
    },
  };
}

function actorIsOperator(actor = {}) {
  const role = String(actor.role || actor.source || '').toLowerCase();
  const name = String(actor.name || actor.id || '').toLowerCase();
  if (role === SPECIALISTS.MAX || name === SPECIALISTS.MAX) return false;
  return role === SPECIALISTS.OPERATOR || role === 'admin' || role === 'manager' || role === 'client';
}

function acknowledgeHalt(governor, actor = {}, note, now) {
  if (!governor || (governor.outcome !== GOVERNOR_OUTCOMES.PAUSE && governor.outcome !== GOVERNOR_OUTCOMES.EMERGENCY)) {
    throw eoiError('eoi_no_halt', 'There is no Pause or Emergency to acknowledge.');
  }
  if (!actorIsOperator(actor)) {
    throw eoiError(
      'eoi_operator_acknowledgement_required',
      'Max cannot override Pause or Emergency silently. Operator acknowledgement is required.'
    );
  }
  return {
    id: newId('ack'),
    kind: 'governor_ack',
    spec: 'SPEC-117',
    outcome: governor.outcome,
    acknowledged: true,
    acknowledgedAt: nowIso(now),
    operatorId: actor.id || actor.name || 'operator',
    note: note || null,
    resumeAllowed: governor.outcome === GOVERNOR_OUTCOMES.PAUSE,
    emergencyCleared: false,
  };
}

function evaluateSend(input = {}) {
  const {
    governor,
    capacity,
    approvedPlan,
    candidate = {},
    sentToday = 0,
    localDate,
    allowLegacySequences = false,
  } = input;

  if (!governor) {
    return blocked('governor_required', 'Every send must be evaluated by the Safe Send Governor.');
  }
  if (governor.outcome === GOVERNOR_OUTCOMES.EMERGENCY || governor.outcome === GOVERNOR_OUTCOMES.PAUSE) {
    return blocked(governor.outcome, governor.reason, { governor });
  }
  if (!approvedPlan || approvedPlan.status !== 'approved') {
    return blocked('operator_approval_required', 'Human approval remains mandatory before sends.');
  }
  if (localDate && approvedPlan.localDate && approvedPlan.localDate !== localDate) {
    return blocked('operator_approval_required', 'Today\'s send plan has not been approved.');
  }
  const cap = Number(
    approvedPlan.approvedCapacity != null ? approvedPlan.approvedCapacity : capacity?.recommended || 0
  );
  const slowCap = governor.outcome === GOVERNOR_OUTCOMES.SLOW
    ? Number(governor.slowCap || Math.floor(cap * 0.6))
    : cap;
  const limit = Math.min(cap, slowCap);
  if (sentToday >= limit) {
    return blocked('capacity_exhausted', `Today's safe capacity (${limit}) is already used.`, { governor, limit });
  }
  if (candidate.dnc === true || candidate.do_not_contact === true) {
    return blocked('dnc', 'Do-not-contact is set. Emmett will not send.', { governor });
  }
  const paige = candidate.paige || candidate.content || null;
  const fromPaige = paige && (paige.author === 'paige' || paige.source === 'paige' || candidate.contentSource === 'paige');
  const legacy = paige && (paige.source === 'legacy_sequence' || candidate.contentSource === 'legacy_sequence');
  if (!fromPaige) {
    if (!(legacy && (allowLegacySequences || approvedPlan.allowLegacySequences))) {
      return blocked('paige_content_required', 'Paige communicates. Emmett will not send copy it wrote or inherited without operator override.', { governor });
    }
  }
  if (!paige?.subject || !paige?.body) {
    return blocked('paige_content_required', 'Paige must provide subject and body before send.', { governor });
  }
  return {
    allowed: true,
    outcome: governor.outcome,
    reason: governor.reason,
    remaining: Math.max(0, limit - sentToday - 1),
    governor,
    spec: 'SPEC-117',
  };
}

function blocked(code, message, extra = {}) {
  return {
    allowed: false,
    code,
    reason: message,
    outcome: extra.governor?.outcome || code,
    remaining: 0,
    governor: extra.governor || null,
    spec: 'SPEC-117',
  };
}

module.exports = {
  evaluateGovernor,
  evaluateSend,
  acknowledgeHalt,
  actorIsOperator,
};
