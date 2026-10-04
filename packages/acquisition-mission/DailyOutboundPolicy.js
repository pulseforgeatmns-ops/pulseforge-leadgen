'use strict';

const crypto = require('crypto');
const { validatePaigeVariantCopy } = require('../max/workspace/PaigeCopySafety');
const { governedContactReason, SENDABLE_CLASSES } = require('../../utils/governedContactEligibility');
const { governedRecipientBindingReason } = require('../../utils/governedRecipientBinding');

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])]));
  return value;
}
const hash = value => crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const fail = code => { throw Object.assign(new Error(code), { code }); };

function missionScope(mission) {
  // Legacy sources omit this optional field; the daily mission factory adds null.
  // Keep the legacy hash while still binding every non-null resolved objective.
  return { tenantId: String(mission.tenantId), objective: mission.objective,
    targetSegment: mission.targetSegment, structuredMission: mission.structuredMission,
    constraints: mission.constraints, resolvedObjective: mission.resolvedObjective ?? undefined };
}

function policy(input, now = new Date()) {
  const p = { tenantId: String(input.tenantId), sourceMissionId: input.sourceMissionId,
    senderEmail: String(input.senderEmail || '').trim().toLowerCase(),
    inboxIntegrationId: String(input.inboxIntegrationId || ''),
    aoOwnerIds: [...new Set(input.aoOwnerIds || [])],
    startsAt: new Date(input.startsAt || now).toISOString(),
    expiresAt: new Date(input.expiresAt).toISOString(),
    dailyCap: input.dailyCap ?? 5, totalCap: input.totalCap ?? 100,
    spacingMinutes: input.spacingMinutes ?? 60, timeZone: 'America/New_York',
    weekdays: [1, 2, 3, 4, 5], startHour: 9, endHour: 17,
    // This phase authorizes first touches only. A reply can never restart a sequence.
    maxSequenceStep: 1, enrichmentLimit: 15, preparationAttemptsPerDay: 3 };
  if (!['10', '13'].includes(p.tenantId) || !p.sourceMissionId || !p.senderEmail.includes('@') || !p.inboxIntegrationId) {
    fail('invalid_governed_outbound_scope');
  }
  if (p.tenantId === '10') {
    if (!p.aoOwnerIds.length || p.aoOwnerIds.length > 10 || p.aoOwnerIds.some(id => !Number.isInteger(id) || id < 1)) {
      fail('ao_owners_required');
    }
  } else if (p.tenantId === '13') {
    p.allowedContactClassifications = [...new Set(input.allowedContactClassifications || ['VERIFIED_FOUNDER_EMAIL'])];
    if (!p.allowedContactClassifications.length || p.allowedContactClassifications.some(x => !SENDABLE_CLASSES.includes(x))) fail('invalid_contact_classifications');
    p.sendingIdentityId = String(input.sendingIdentityId || '').trim();
    if (!p.sendingIdentityId) fail('sending_identity_required');
    // Bind the reviewed mailbox window; Emmett still revalidates execution.
    p.startHour = input.startHour ?? p.startHour;
    p.endHour = input.endHour ?? p.endHour;
    if (!Number.isInteger(p.startHour) || !Number.isInteger(p.endHour)
      || p.startHour < 9 || p.endHour > 17 || p.startHour >= p.endHour) fail('invalid_business_window');
    if (p.aoOwnerIds.length > 10 || p.aoOwnerIds.some(id => !Number.isInteger(id) || id < 1)) fail('ao_owners_required');
    if (input.operatorDelegatedMaximumDailyCapacity != null) {
      p.operatorDelegatedMaximumDailyCapacity = Number(input.operatorDelegatedMaximumDailyCapacity);
    }
  }
  // dailyCap / operatorDelegatedMaximumDailyCapacity define the operator outer envelope.
  // Emmett determines safe capacity within that envelope; neither field is a send floor.
  const delegatedMax = p.operatorDelegatedMaximumDailyCapacity != null
    ? Number(p.operatorDelegatedMaximumDailyCapacity)
    : p.dailyCap;
  if (!Number.isInteger(delegatedMax) || delegatedMax < 1 || delegatedMax > 50
    || !Number.isInteger(p.totalCap) || p.totalCap < 1 || p.totalCap > 100
    || delegatedMax > p.totalCap
    || !Number.isInteger(p.spacingMinutes) || p.spacingMinutes < 60 || p.spacingMinutes > 240) fail('invalid_bounds');
  if (Date.parse(p.expiresAt) <= Date.parse(p.startsAt)
    || Date.parse(p.expiresAt) - Date.parse(p.startsAt) > 30 * 86400000
    || Date.parse(p.expiresAt) <= +now) fail('invalid_expiry');
  return p;
}

function clock(now = new Date()) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', hourCycle: 'h23', weekday: 'short',
  }).formatToParts(now).map(p => [p.type, p.value]));
  return { day: `${parts.year}-${parts.month}-${parts.day}`, hour: Number(parts.hour),
    weekday: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(parts.weekday) };
}

function windowReason(p, now = new Date(), sending = true) {
  if (+now < Date.parse(p.startsAt)) return 'not_started';
  if (+now >= Date.parse(p.expiresAt)) return 'authorization_expired';
  const c = clock(now);
  if (sending && !p.weekdays.includes(c.weekday)) return 'weekend';
  if (sending && (c.hour < p.startHour || c.hour >= p.endHour)) return 'outside_business_hours';
  return null;
}

function candidateReason(item, crm, message, policy = {}) {
  const reason = governedContactReason(crm, policy);
  if (reason) return reason;
  if (crm.is_synthetic === true) return 'synthetic_contact';
  if (String(item.email || '').toLowerCase() !== String(crm.email || '').toLowerCase()) return 'recipient_changed';
  if (item.sendable !== true || item.dnc === true) return 'queue_not_sendable';
  if (!message?.subject?.trim() || !message?.body?.trim()) return 'missing_paige_copy';
  if ((item.paige?.subject && item.paige.subject !== message.subject)
    || (item.paige?.body && item.paige.body !== message.body)) return 'capacity_copy_binding_mismatch';
  if (!validatePaigeVariantCopy(message).safe) return 'unsafe_paige_copy';
  if (String(message.candidateId || '') !== String(item.paige?.candidateId || '')) return 'copy_binding_changed';
  const binding = governedRecipientBindingReason(item, crm, message);
  if (binding) return binding;
  return null;
}

// Max owns deterministic next actions; Riley supplies the classification.
function nextAction(classification) {
  return ({
    interested: ['engaged', 'ao_handoff'], quote_request: ['quote_requested', 'ao_handoff'],
    incumbent_vendor: ['incumbent_vendor', 'ao_handoff'], wrong_person: ['wrong_contact', 'research_contact'],
    unsubscribe: ['dnc', 'stop'], negative: ['closed', 'stop'],
    not_now: ['nurture', 'review_nurture'], out_of_office: ['paused', 'review_return_date'],
  })[classification] || ['reply_received', 'review_reply'];
}

module.exports = { hash, fail, missionScope, policy, clock, windowReason, candidateReason, nextAction };
