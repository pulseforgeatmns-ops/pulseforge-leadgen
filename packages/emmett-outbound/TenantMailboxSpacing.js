'use strict';

/**
 * SPEC-256 — temporally correct spacing and single capacity-accounting contract.
 *
 * Authorization spacing is bidirectional against live commitments.
 * Execution spacing uses only prior actual/committed sends.
 * A timestamp later than evaluatedScheduledFor is never a valid lastSendAt.
 */

const LIVE_SCHEDULE_STATUSES = Object.freeze(['SCHEDULED', 'EXECUTING', 'SENT']);
const LIVE_RESERVATION_STATUSES = Object.freeze(['scheduled', 'executing', 'sent']);
const RELEASED_SCHEDULE_STATUSES = Object.freeze(['FAILED', 'SKIPPED', 'CANCELLED', 'PAUSED']);
const RELEASED_RESERVATION_STATUSES = Object.freeze(['released', 'skipped', 'failed', 'cancelled']);
const IN_FLIGHT_SCHEDULE_STATUSES = Object.freeze(['EXECUTING', 'SENT']);
const IN_FLIGHT_RESERVATION_STATUSES = Object.freeze(['executing', 'sent']);

function toMs(value) {
  if (value == null || value === '') return NaN;
  const ms = value instanceof Date ? value.getTime() : new Date(value).getTime();
  return Number.isFinite(ms) ? ms : NaN;
}

function toIso(ms) {
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

function normalizeStatus(value) {
  return String(value || '').trim();
}

function scheduleStatus(value) {
  return normalizeStatus(value).toUpperCase();
}

function reservationStatus(value) {
  return normalizeStatus(value).toLowerCase();
}

function isReleased(event = {}) {
  const sched = scheduleStatus(event.status);
  const res = reservationStatus(event.status);
  return RELEASED_SCHEDULE_STATUSES.includes(sched) || RELEASED_RESERVATION_STATUSES.includes(res);
}

function isLiveCommitment(event = {}) {
  if (isReleased(event)) return false;
  const sched = scheduleStatus(event.status);
  const res = reservationStatus(event.status);
  return LIVE_SCHEDULE_STATUSES.includes(sched) || LIVE_RESERVATION_STATUSES.includes(res);
}

function isInFlightCommitment(event = {}) {
  if (isReleased(event)) return false;
  if (event.sentAt) return true;
  const sched = scheduleStatus(event.status);
  const res = reservationStatus(event.status);
  return IN_FLIGHT_SCHEDULE_STATUSES.includes(sched) || IN_FLIGHT_RESERVATION_STATUSES.includes(res);
}

function eventId(event = {}) {
  return event.scheduleId || event.id || event.reservationId || null;
}

function matchesExclude(event, excludeScheduleId) {
  if (!excludeScheduleId) return false;
  const excluded = String(excludeScheduleId);
  return [event.scheduleId, event.id, event.reservationId]
    .filter(Boolean)
    .some((value) => String(value) === excluded);
}

function eventTimestamp(event = {}) {
  const sentMs = toMs(event.sentAt);
  if (Number.isFinite(sentMs)) {
    return { at: sentMs, source: 'sent_at' };
  }
  const scheduledMs = toMs(event.scheduledFor);
  if (Number.isFinite(scheduledMs)) {
    return { at: scheduledMs, source: 'scheduled_for' };
  }
  return null;
}

function isValidPriorAnchor(lastSendAt, evaluatedScheduledFor) {
  const last = toMs(lastSendAt);
  const evaluated = toMs(evaluatedScheduledFor);
  if (!Number.isFinite(last) || !Number.isFinite(evaluated)) return false;
  return last < evaluated;
}

function spacingMinutes(scheduledFor, lastSendAt) {
  if (!isValidPriorAnchor(lastSendAt, scheduledFor)) return null;
  return Math.floor((toMs(scheduledFor) - toMs(lastSendAt)) / 60000);
}

function findPriorSendAnchor(events = [], evaluatedScheduledFor, opts = {}) {
  const evaluated = toMs(evaluatedScheduledFor);
  if (!Number.isFinite(evaluated)) return null;

  let best = null;
  for (const event of events) {
    if (matchesExclude(event, opts.excludeScheduleId)) continue;
    if (!isInFlightCommitment(event) && scheduleStatus(event.status) !== 'SENT') continue;
    const ts = eventTimestamp(event);
    if (!ts || ts.at >= evaluated) continue;
    if (
      !best
      || ts.at > best.at
      || (ts.at === best.at && ts.source === 'sent_at' && best.source !== 'sent_at')
    ) {
      best = {
        at: ts.at,
        source: ts.source,
        scheduleId: eventId(event),
        status: event.status || null,
      };
    }
  }
  if (!best) return null;
  return {
    lastSendAt: toIso(best.at),
    source: best.source,
    scheduleId: best.scheduleId,
    status: best.status,
  };
}

function collectSpacingConflicts(requestedScheduledFor, commitments = [], minSpacingMinutes, opts = {}) {
  const requested = toMs(requestedScheduledFor);
  const minSpacing = Number(minSpacingMinutes || 0);
  if (!Number.isFinite(requested) || minSpacing <= 0) return [];

  const conflicts = [];
  const seen = new Set();
  for (const event of commitments) {
    if (matchesExclude(event, opts.excludeScheduleId)) continue;
    if (opts.inFlightOnly ? !isInFlightCommitment(event) : !isLiveCommitment(event)) continue;
    const ts = eventTimestamp(event);
    if (!ts) continue;
    const gap = Math.abs(Math.floor((requested - ts.at) / 60000));
    if (gap >= minSpacing) continue;
    const key = `${eventId(event) || ts.at}:${ts.at}`;
    if (seen.has(key)) continue;
    seen.add(key);
    conflicts.push({
      scheduleId: eventId(event),
      at: toIso(ts.at),
      gapMinutes: Math.floor((requested - ts.at) / 60000),
      status: event.status || null,
      source: ts.source,
    });
  }
  return conflicts;
}

function findScheduleSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, opts = {}) {
  return collectSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, {
    ...opts,
    inFlightOnly: false,
  });
}

function findInFlightSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, opts = {}) {
  return collectSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, {
    ...opts,
    inFlightOnly: true,
  });
}

function blocked(code, message) {
  return { allowed: false, code, reason: message };
}

function evaluateAuthorizationSpacing(envelope = {}, input = {}) {
  const minSpacing = Number(envelope.minimumSpacingMinutes || 0);
  if (minSpacing <= 0 || !input.scheduledFor) return { allowed: true };

  if (Array.isArray(input.scheduleConflicts)) {
    if (input.scheduleConflicts.length) {
      return blocked(
        'emmett_spacing_violation',
        `Minimum spacing of ${minSpacing} minutes is required between scheduled sends.`
      );
    }
    return { allowed: true };
  }

  if (input.lastSendAt && isValidPriorAnchor(input.lastSendAt, input.scheduledFor)) {
    const gap = spacingMinutes(input.scheduledFor, input.lastSendAt);
    if (gap != null && gap < minSpacing) {
      return blocked(
        'emmett_spacing_violation',
        `Minimum spacing of ${minSpacing} minutes is required between scheduled sends.`
      );
    }
  }
  return { allowed: true };
}

function evaluateExecutionSpacing(envelope = {}, input = {}) {
  const minSpacing = Number(envelope.minimumSpacingMinutes || 0);
  if (minSpacing <= 0 || !input.scheduledFor) return { allowed: true };

  if (Array.isArray(input.inFlightConflicts) && input.inFlightConflicts.length) {
    return blocked(
      'emmett_spacing_violation',
      `Minimum spacing of ${minSpacing} minutes is required between sends.`
    );
  }

  if (input.lastSendAt && isValidPriorAnchor(input.lastSendAt, input.scheduledFor)) {
    const gap = spacingMinutes(input.scheduledFor, input.lastSendAt);
    if (gap != null && gap < minSpacing) {
      return blocked(
        'emmett_spacing_violation',
        `Minimum spacing of ${minSpacing} minutes is required between sends.`
      );
    }
  }
  return { allowed: true };
}

function consumedFromEnvelope(envelope = {}) {
  return Number(envelope.currentSentCount || 0)
    + Number(envelope.currentScheduledCount || 0)
    + Number(envelope.currentExecutingCount || 0);
}

function evaluateCapacityBudget(envelope = {}, input = {}) {
  const max = Number(envelope.maxSendsPerDay || 0);
  const consumed = consumedFromEnvelope(envelope);
  if (input.alreadyConsumesCapacity) {
    if (consumed > max) {
      return blocked('emmett_capacity_exhausted', 'Daily Emmett capacity is exhausted for this sending identity.');
    }
    return { allowed: true, consumed, max };
  }
  const remaining = Number(envelope.remainingCapacity != null
    ? envelope.remainingCapacity
    : Math.max(0, max - consumed));
  if (remaining <= 0 || consumed >= max) {
    return blocked('emmett_capacity_exhausted', 'Daily Emmett capacity is exhausted for this sending identity.');
  }
  return { allowed: true, consumed, max, remaining };
}

function logicalCapacityKey(event = {}, fallbackPrefix = 'anon') {
  if (event.scheduleId) return `sched:${event.scheduleId}`;
  if (event.id && (event.kind === 'schedule' || LIVE_SCHEDULE_STATUSES.includes(scheduleStatus(event.status)))) {
    return `sched:${event.id}`;
  }
  if (event.outboundMessageId) return `msg:${event.outboundMessageId}`;
  if (event.messageId) return `msg:${event.messageId}`;
  if (event.id && event.kind === 'message') return `msg:${event.id}`;
  if (event.reservationId) return `res:${event.reservationId}`;
  if (event.id) return `${fallbackPrefix}:${event.id}`;
  return null;
}

function accountCapacityUnits({
  sentMessages = [],
  schedules = [],
  reservations = [],
} = {}) {
  const consumedKeys = new Set();
  const counts = { sent: 0, scheduled: 0, executing: 0 };

  function consume(key, kind) {
    if (!key || consumedKeys.has(key)) return;
    consumedKeys.add(key);
    counts[kind] += 1;
  }

  for (const message of sentMessages) {
    consume(logicalCapacityKey({ ...message, kind: 'message' }, 'msg'), 'sent');
  }

  for (const schedule of schedules) {
    const status = scheduleStatus(schedule.status);
    if (RELEASED_SCHEDULE_STATUSES.includes(status)) continue;
    const key = logicalCapacityKey({ ...schedule, kind: 'schedule', scheduleId: schedule.id || schedule.scheduleId }, 'sched');
    if (status === 'SENT') {
      const messageKey = schedule.outboundMessageId ? `msg:${schedule.outboundMessageId}` : null;
      if (messageKey && consumedKeys.has(messageKey)) {
        consumedKeys.add(key);
        continue;
      }
      consume(key, 'sent');
    } else if (status === 'EXECUTING') {
      consume(key, 'executing');
    } else if (status === 'SCHEDULED') {
      consume(key, 'scheduled');
    }
  }

  for (const reservation of reservations) {
    const status = reservationStatus(reservation.status);
    if (!LIVE_RESERVATION_STATUSES.includes(status)) continue;
    const key = logicalCapacityKey({
      ...reservation,
      scheduleId: reservation.scheduleId,
      reservationId: reservation.id || reservation.reservationId,
    }, 'res');
    if (!key || consumedKeys.has(key)) continue;
    if (reservation.scheduleId && consumedKeys.has(`sched:${reservation.scheduleId}`)) continue;
    if (status === 'sent') consume(key, 'sent');
    else if (status === 'executing') consume(key, 'executing');
    else consume(key, 'scheduled');
  }

  const consumed = counts.sent + counts.scheduled + counts.executing;
  return {
    ...counts,
    consumed,
    remainingFor(maxSendsPerDay) {
      return Math.max(0, Number(maxSendsPerDay || 0) - consumed);
    },
  };
}

module.exports = {
  LIVE_SCHEDULE_STATUSES,
  LIVE_RESERVATION_STATUSES,
  RELEASED_SCHEDULE_STATUSES,
  RELEASED_RESERVATION_STATUSES,
  IN_FLIGHT_SCHEDULE_STATUSES,
  IN_FLIGHT_RESERVATION_STATUSES,
  toMs,
  isValidPriorAnchor,
  spacingMinutes,
  isLiveCommitment,
  isInFlightCommitment,
  findPriorSendAnchor,
  findScheduleSpacingConflicts,
  findInFlightSpacingConflicts,
  evaluateAuthorizationSpacing,
  evaluateExecutionSpacing,
  evaluateCapacityBudget,
  accountCapacityUnits,
  consumedFromEnvelope,
};
