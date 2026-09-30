'use strict';

/**
 * Rolling governed-outbound preparation refill.
 * Bounded batches only; never force-sends; never widens safety gates.
 */

const { hash, candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { buildAnchorLifecycleVariant } = require('../utils/anchorLifecycleEmail');

const PREPARATION_BATCH_LIMIT = 5;

function asNonNegInt(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.trunc(n));
}

function zonedParts(date, timeZone = 'America/New_York') {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
    weekday: 'short',
  }).formatToParts(date instanceof Date ? date : new Date(date)).map(p => [p.type, p.value]));
  return {
    weekday: parts.weekday,
    date: `${parts.year}-${parts.month}-${parts.day}`,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
  };
}

function minutesFromMidnight(parts) {
  return (Number(parts.hour) * 60) + Number(parts.minute) + (Number(parts.second) / 60);
}

function remainingDispatchCapacity({ dispatchCapacityNow = 0, sentToday = 0 } = {}) {
  return Math.max(0, asNonNegInt(dispatchCapacityNow) - asNonNegInt(sentToday));
}

function remainingScheduleSlots({
  now = new Date(),
  lastSendAt = null,
  allowedSendWindow = { startHour: 9, endHour: 17, timezone: 'America/New_York' },
  minSpacingMinutes = 60,
  dispatchDayAllowed = true,
} = {}) {
  if (dispatchDayAllowed === false) return 0;
  const tz = allowedSendWindow?.timezone || 'America/New_York';
  const startHour = Number(allowedSendWindow?.startHour);
  const endHour = Number(allowedSendWindow?.endHour);
  const spacing = Number(minSpacingMinutes);
  if (!Number.isFinite(startHour) || !Number.isFinite(endHour) || endHour <= startHour) return 0;
  if (!Number.isFinite(spacing) || spacing <= 0) return 0;

  const nowParts = zonedParts(now, tz);
  const nowMins = minutesFromMidnight(nowParts);
  const startMins = startHour * 60;
  const endMins = endHour * 60;
  if (nowMins >= endMins) return 0;

  let cursor = Math.max(nowMins, startMins);
  if (lastSendAt) {
    const lastParts = zonedParts(lastSendAt, tz);
    if (lastParts.date === nowParts.date) {
      cursor = Math.max(cursor, minutesFromMidnight(lastParts) + spacing);
    }
  }
  if (cursor >= endMins) return 0;

  let slots = 0;
  for (let t = cursor; t < endMins; t += spacing) slots += 1;
  return slots;
}

function evaluatePreparationRefill({
  pendingPreparedCount = 0,
  remainingDispatchCapacity: remainingCap = 0,
  remainingScheduleSlots: remainingSlots = 0,
  cleanInventory = 0,
  governor = 'proceed',
  grantActive = true,
  dailyAuthorizationRemaining = Infinity,
  totalAuthorizationRemaining = Infinity,
  batchLimit = PREPARATION_BATCH_LIMIT,
} = {}) {
  const pending = asNonNegInt(pendingPreparedCount);
  const dispatchRemaining = asNonNegInt(remainingCap);
  const slotRemaining = asNonNegInt(remainingSlots);
  const inventory = asNonNegInt(cleanInventory);
  const limit = Math.max(1, asNonNegInt(batchLimit, PREPARATION_BATCH_LIMIT));
  const snapshot = {
    pendingPrepared: pending,
    remainingDispatchCapacity: dispatchRemaining,
    remainingScheduleSlots: slotRemaining,
    cleanInventory: inventory,
    prepareRequested: 0,
    preparedAdded: 0,
    prepareSkippedReason: null,
    shouldPrepare: false,
  };

  const skip = (reason) => ({ ...snapshot, prepareSkippedReason: reason });

  const governorOutcome = String(governor?.outcome || governor || '').toLowerCase();
  if (governor === true || governor?.halt === true
    || ['halt', 'pause', 'emergency'].includes(governorOutcome)) {
    return skip('governor_halt');
  }
  if (grantActive === false) return skip('grant_inactive');
  if (asNonNegInt(dailyAuthorizationRemaining, 1) <= 0) return skip('daily_authorization_exhausted');
  if (asNonNegInt(totalAuthorizationRemaining, 1) <= 0) return skip('total_authorization_exhausted');
  if (dispatchRemaining <= 0) return skip('no_remaining_capacity');
  if (slotRemaining <= 0) return skip('no_remaining_slots');
  if (inventory <= 0) return skip('no_clean_inventory');
  if (pending >= dispatchRemaining) return skip('pending_covers_capacity');
  if (inventory <= pending) return skip('no_clean_inventory');
  if (pending >= limit) return skip('batch_limit_reached');
  if (pending >= slotRemaining) return skip('no_remaining_slots');

  const requested = Math.min(
    dispatchRemaining - pending,
    slotRemaining - pending,
    limit - pending,
    inventory - pending,
  );
  if (requested <= 0) return skip('pending_covers_capacity');
  return {
    ...snapshot,
    shouldPrepare: true,
    prepareRequested: requested,
    prepareSkippedReason: null,
  };
}

function observabilityFromRefill(plan = {}, extras = {}) {
  return {
    sentToday: asNonNegInt(extras.sentToday),
    pendingPrepared: asNonNegInt(plan.pendingPrepared ?? extras.pendingPrepared),
    remainingDispatchCapacity: asNonNegInt(plan.remainingDispatchCapacity ?? extras.remainingDispatchCapacity),
    remainingScheduleSlots: asNonNegInt(plan.remainingScheduleSlots ?? extras.remainingScheduleSlots),
    cleanInventory: asNonNegInt(plan.cleanInventory ?? extras.cleanInventory),
    prepareRequested: asNonNegInt(plan.prepareRequested),
    preparedAdded: asNonNegInt(plan.preparedAdded ?? extras.preparedAdded),
    prepareSkippedReason: plan.prepareSkippedReason || extras.prepareSkippedReason || null,
  };
}

function finalizePreparationObservability(snapshot = {}) {
  const prepareRequested = asNonNegInt(snapshot.prepareRequested);
  const preparedAdded = asNonNegInt(snapshot.preparedAdded);
  const prepareSkippedReason = snapshot.prepareSkippedReason || null;
  if (prepareRequested > 0 && preparedAdded === 0 && !prepareSkippedReason) {
    return { ...snapshot, prepareSkippedReason: 'preparation_not_executed' };
  }
  return snapshot;
}

async function selectRefillEntries({
  prepared,
  program,
  store,
  adapters,
  existingItems = [],
  limit,
}) {
  const existing = new Set(existingItems.flatMap(item => [
    String(item.candidate_id || item.candidateId || ''),
    String(item.prospect_id || item.prospectId || ''),
    String(item.email || '').toLowerCase(),
    String(item.company_id || item.companyId || ''),
  ].filter(Boolean)));
  const selected = [];
  const emails = new Set();
  const companies = new Set();
  for (const item of existingItems) {
    if (item.email) emails.add(String(item.email).toLowerCase());
    if (item.company_id || item.companyId) companies.add(String(item.company_id || item.companyId));
  }

  for (const row of prepared.candidates || []) {
    if (selected.length >= limit) break;
    const crm = await adapters.contact(row.candidateId);
    const reason = candidateReason(row.item, crm, row.message, program?.policy);
    const entry = {
      candidateId: String(row.candidateId),
      prospectId: String(crm?.prospect_id || crm?.id || ''),
      companyId: String(crm?.company_id || ''),
      email: String(row.item.email || crm?.email || '').toLowerCase(),
      message: row.message,
      sender: prepared.sender,
      revision: prepared.revision,
    };
    if (
      existing.has(entry.candidateId)
      || existing.has(entry.prospectId)
      || existing.has(entry.email)
      || existing.has(entry.companyId)
    ) continue;
    const suppressed = !reason && await store.suppression(entry);
    if (reason || suppressed || !entry.companyId || emails.has(entry.email) || companies.has(entry.companyId)) {
      continue;
    }
    selected.push(entry);
    emails.add(entry.email);
    companies.add(entry.companyId);
  }
  return selected;
}

function buildFirstTouchCopy(row, crm, sender) {
  const lifecycle = buildAnchorLifecycleVariant({
    candidate: {
      name: row.company || crm.company_name || crm.company || row.companyName,
      first_name: crm.first_name || crm.firstName || null,
      location: crm.company_location || crm.location || null,
    },
    crmRecord: {
      ...crm,
      first_name: crm.first_name || crm.firstName || null,
      company_name: row.company || crm.company_name || crm.company,
      company_fields: {
        name: row.company || crm.company_name || crm.company,
        location: crm.company_location || crm.location || null,
      },
    },
    senderName: sender?.senderName || 'Jacob Maynard',
  });
  const candidateId = String(row.candidateId || row.prospectId || crm.prospect_id || crm.id || '');
  return {
    subject: lifecycle.subject,
    body: lifecycle.body,
    candidateId,
    cta: lifecycle.cta || null,
  };
}

async function selectInventoryRefillEntries({
  cleanRows = [],
  store,
  adapters,
  prepared,
  program,
  existingItems = [],
  limit,
} = {}) {
  const cap = Math.max(0, asNonNegInt(limit));
  if (!cap || !Array.isArray(cleanRows) || !cleanRows.length) return [];
  const existing = new Set(existingItems.flatMap(item => [
    String(item.candidate_id || item.candidateId || ''),
    String(item.prospect_id || item.prospectId || ''),
    String(item.email || '').toLowerCase(),
    String(item.company_id || item.companyId || ''),
  ].filter(Boolean)));
  const selected = [];
  const emails = new Set();
  const companies = new Set();
  for (const item of existingItems) {
    if (item.email) emails.add(String(item.email).toLowerCase());
    if (item.company_id || item.companyId) companies.add(String(item.company_id || item.companyId));
  }

  for (const row of cleanRows) {
    if (selected.length >= cap) break;
    const candidateId = String(row.candidateId || row.prospectId || '');
    const crm = adapters.contact ? await adapters.contact(candidateId) : row;
    if (!crm) continue;
    const email = String(row.email || crm.email || '').toLowerCase();
    const companyId = String(row.companyId || crm.company_id || '');
    const prospectId = String(row.prospectId || crm.prospect_id || crm.id || '');
    if (
      existing.has(candidateId)
      || existing.has(prospectId)
      || existing.has(email)
      || existing.has(companyId)
    ) continue;
    if (!companyId || !email || emails.has(email) || companies.has(companyId)) continue;

    const message = buildFirstTouchCopy(row, crm, prepared?.sender);
    const queueItem = {
      email,
      sendable: true,
      dnc: false,
      prospectId,
      companyId,
      paige: { candidateId },
      refill: true,
    };
    const reason = candidateReason(queueItem, crm, message, program?.policy);
    const entry = {
      candidateId,
      prospectId,
      companyId,
      email,
      message,
      sender: prepared?.sender,
      revision: prepared?.revision,
      refill: true,
      ...queueItem,
    };
    const suppressed = !reason && await store.suppression(entry);
    if (reason || suppressed) continue;
    selected.push(entry);
    emails.add(email);
    companies.add(companyId);
    existing.add(candidateId);
    existing.add(prospectId);
    existing.add(email);
    existing.add(companyId);
  }
  return selected;
}

module.exports = {
  PREPARATION_BATCH_LIMIT,
  remainingDispatchCapacity,
  remainingScheduleSlots,
  evaluatePreparationRefill,
  observabilityFromRefill,
  finalizePreparationObservability,
  selectRefillEntries,
  selectInventoryRefillEntries,
  hash,
};
