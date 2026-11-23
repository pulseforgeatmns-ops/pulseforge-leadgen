'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { researchEnabled } = require('./leviScoutEnablement');

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;

function weekOf(now = new Date()) {
  const day = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const offset = (day.getUTCDay() + 6) % 7;
  day.setUTCDate(day.getUTCDate() - offset);
  return day.toISOString().slice(0, 10);
}

function cycleIdFor(now = new Date()) {
  return `levi-week-${weekOf(now)}`;
}

function msUntilNextWeek(now = new Date()) {
  const current = new Date(`${weekOf(now)}T00:00:00.000Z`);
  return current.getTime() + WEEK_MS - now.getTime();
}

async function tick({ now = new Date(), env = process.env, run, lastCycleId = null } = {}) {
  const cycleId = cycleIdFor(now);
  if (!researchEnabled(env)) {
    return { status: 'disabled', cycleId, ran: false, outreachAttempted: false };
  }
  if (lastCycleId === cycleId) {
    return { status: 'already_ran', cycleId, ran: false, outreachAttempted: false };
  }
  if (typeof run !== 'function') {
    return { status: 'armed', cycleId, ran: false, outreachAttempted: false };
  }
  const result = await run({ cycleId, now, env });
  return { status: result.status || 'completed', cycleId, ran: true, result, outreachAttempted: false };
}

function scheduleNextWeek({ env, run, timers, from }) {
  const delay = Math.max(1000, msUntilNextWeek(from));
  if (timers && typeof timers.setTimeout === 'function') {
    timers.setTimeout(() => {
      const tickNow = new Date();
      tick({ now: tickNow, env, run })
        .catch(() => {})
        .finally(() => scheduleNextWeek({ env, run, timers, from: tickNow }));
    }, delay);
  }
  return delay;
}

function startLeviScoutWeeklyClock({ env = process.env, now = new Date(), run, timers = globalThis } = {}) {
  const delayMs = scheduleNextWeek({ env, run, timers, from: now });
  const armed = researchEnabled(env);
  // Catch up the current ISO week when armed. claimRun makes this idempotent
  // across Render restarts; disabled clocks stay no-op until the flag is on.
  const catchUp = armed && typeof run === 'function'
    ? tick({ now, env, run }).catch(() => ({ status: 'catch_up_failed', ran: false }))
    : Promise.resolve({ status: 'disabled', ran: false });
  return catchUp.then(catchUpTick => ({
    started: true,
    independentOfBackgroundWorkers: true,
    armed,
    cycleId: cycleIdFor(now),
    tenantId: TENANT_ID,
    delayMs,
    catchUpRan: Boolean(catchUpTick && catchUpTick.ran),
  }));
}

module.exports = { weekOf, cycleIdFor, msUntilNextWeek, tick, startLeviScoutWeeklyClock, scheduleNextWeek };
