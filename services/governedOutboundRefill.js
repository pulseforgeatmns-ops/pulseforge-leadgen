'use strict';

/**
 * Rolling governed-outbound preparation refill.
 * Bounded batches only; never force-sends; never widens safety gates.
 */

const { hash, candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { governedRecipientBindingReason } = require('../utils/governedRecipientBinding');
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
  planningDailyCapacity = null,
  batchLimit = PREPARATION_BATCH_LIMIT,
} = {}) {
  const pending = asNonNegInt(pendingPreparedCount);
  const dispatchRemaining = asNonNegInt(remainingCap);
  const slotRemaining = asNonNegInt(remainingSlots);
  // Held preparation uses the next authorized day's planning capacity. Dispatch
  // still rechecks the actual weekday, time, spacing and remaining send caps.
  const planning = planningDailyCapacity == null ? null : asNonNegInt(planningDailyCapacity);
  const preparationCapacity = planning == null ? dispatchRemaining
    : Math.min(planning, asNonNegInt(dailyAuthorizationRemaining, planning), asNonNegInt(totalAuthorizationRemaining, planning));
  const preparationSlots = planning == null ? slotRemaining : planning;
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
  if (preparationCapacity <= 0) return skip('no_remaining_capacity');
  if (preparationSlots <= 0) return skip('no_remaining_slots');
  if (inventory <= 0) return skip('no_clean_inventory');
  if (pending >= preparationCapacity) return skip('pending_covers_capacity');
  if (inventory <= pending) return skip('no_clean_inventory');
  if (pending >= limit) return skip('batch_limit_reached');
  if (pending >= preparationSlots) return skip('no_remaining_slots');

  const requested = Math.min(
    preparationCapacity - pending,
    preparationSlots - pending,
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
    ...(extras.preparationDecisions ? { preparationDecisions: extras.preparationDecisions } : {}),
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

function existingReason(entry, items) {
  if (items.some(x => String(x.candidate_id || x.candidateId || '') === entry.candidateId
    || String(x.prospect_id || x.prospectId || '') === entry.prospectId)) return 'already_in_envelope';
  if (items.some(x => String(x.email || '').toLowerCase() === entry.email)) return 'duplicate_email';
  if (items.some(x => String(x.company_id || x.companyId || '') === entry.companyId)) return 'duplicate_company';
  return null;
}

function decision(decisions, row, reason, source, error = null) {
  decisions.push({ candidateId: String(row.candidateId || row.prospectId || ''),
    prospectId: row.prospectId || null, source,
    outcome: reason ? (reason === 'preparation_limit_reached' ? 'deferred' : 'rejected') : 'selected',
    reason: reason || null, ...(error ? { errorCode: error.code || 'unknown_error' } : {}) });
}

async function selectRefillEntries({ prepared, program, store, adapters, existingItems = [], limit, decisions = [] }) {
  const cap = asNonNegInt(limit);
  const selected = [];
  for (const row of prepared.candidates || []) {
    try {
      const crm = await adapters.contact(row.candidateId);
      const entry = { candidateId: String(row.candidateId), prospectId: String(crm?.prospect_id || crm?.id || ''),
        companyId: String(crm?.company_id || ''), email: String(row.item.email || '').toLowerCase(),
        message: row.message, sender: prepared.sender, revision: prepared.revision };
      const reason = candidateReason(row.item, crm, row.message, program?.policy)
        || (!entry.companyId ? 'missing_company' : null)
        || existingReason(entry, [...existingItems, ...selected])
        || await store.suppression(entry)
        || (selected.length >= cap ? 'preparation_limit_reached' : null);
      decision(decisions, entry, reason, 'prepared');
      if (!reason) selected.push(entry);
    } catch (error) { decision(decisions, row, 'preparation_evaluation_failed', 'prepared', error); }
  }
  return selected;
}

function buildFirstTouchCopy(row, crm, sender) {
  const lifecycle = buildAnchorLifecycleVariant({
    candidate: {
      name: crm.company_name || crm.company,
      first_name: crm.first_name || crm.firstName || null,
      location: crm.company_location || crm.location || null,
    },
    crmRecord: {
      ...crm,
      first_name: crm.first_name || crm.firstName || null,
      company_name: crm.company_name || crm.company,
      company_fields: {
        name: crm.company_name || crm.company,
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
    companyId: String(crm.company_id),
    companyName: crm.company_name || crm.company,
    cta: lifecycle.cta || null,
  };
}

async function selectInventoryRefillEntries({ cleanRows = [], store, adapters, prepared, program,
  existingItems = [], limit, decisions = [] } = {}) {
  const cap = asNonNegInt(limit);
  const selected = [];
  for (const row of cleanRows) {
    try {
      const candidateId = String(row.candidateId || row.prospectId || '');
      const crm = await adapters.contact(candidateId);
      const entry = { candidateId, prospectId: String(row.prospectId || ''), companyId: String(row.companyId || ''),
        email: String(row.email || '').toLowerCase(), company: row.company, domain: row.domain,
        crmProspectId: row.prospectId, crmCompanyId: row.companyId,
        sender: prepared?.sender, revision: prepared?.revision, refill: true,
        sendable: true, dnc: false, paige: { candidateId } };
      let reason = governedContactReason(crm, program?.policy)
        || governedRecipientBindingReason(entry, crm)
        || (!entry.companyId ? 'missing_company' : null)
        || (!entry.prospectId ? 'missing_contact' : null)
        || existingReason(entry, [...existingItems, ...selected]);
      if (!reason) {
        entry.message = buildFirstTouchCopy(row, crm, prepared?.sender);
        reason = candidateReason(entry, crm, entry.message, program?.policy)
          || await store.suppression(entry)
          || (selected.length >= cap ? 'preparation_limit_reached' : null);
      }
      decision(decisions, entry, reason, 'inventory');
      if (!reason) selected.push(entry);
    } catch (error) { decision(decisions, row, 'preparation_evaluation_failed', 'inventory', error); }
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
