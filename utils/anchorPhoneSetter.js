'use strict';

// This is the approved immediate-cash contract for Anchor. These are sourced
// manually by Scout runs; this module never sends a call, SMS, or email.
const ANCHOR_PHONE_SETTER_CATEGORIES = Object.freeze([
  'cleaning_company_overflow',
  'str_manager',
  'property_manager',
  'realtor',
  'restoration_remodeling_partner',
  'commercial_office',
]);

const CATEGORY_PRIORITY = Object.freeze(Object.fromEntries(
  ANCHOR_PHONE_SETTER_CATEGORIES.map((category, index) => [category, index + 1])
));
const DRAFT_CHANNELS = new Set(['email', 'sms']);
const DRAFT_STATUSES = new Set(['draft', 'reviewed', 'dismissed', 'manual_sent']);
const DETAIL_KEYS = new Set([
  'category', 'contact_role', 'decision_maker_reached', 'interest_level',
  'objection_codes', 'next_step', 'follow_up_channel', 'manual_notes',
]);

function isAnchorPhoneSetter(clientId) {
  return Number(clientId) === 10;
}

function categoryPriority(vertical) {
  return CATEGORY_PRIORITY[String(vertical || '').trim()] || 99;
}

function priorityReason(vertical) {
  const priority = categoryPriority(vertical);
  return priority === 99 ? 'Anchor category not prioritized' : `Anchor priority ${priority}: ${vertical.replaceAll('_', ' ')}`;
}

function phoneSetterError(message, code = 'ANCHOR_PHONE_SETTER_INVALID', status = 400) {
  const error = new Error(message);
  error.code = code;
  error.status = status;
  return error;
}

function validateStructuredDetails(value, vertical) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw phoneSetterError('Structured call details must be an object');
  }
  const entries = Object.entries(value);
  if (!entries.length || entries.length > DETAIL_KEYS.size || entries.some(([key]) => !DETAIL_KEYS.has(key))) {
    throw phoneSetterError('Structured call details contain unsupported fields');
  }
  if (value.category !== vertical || !ANCHOR_PHONE_SETTER_CATEGORIES.includes(value.category)) {
    throw phoneSetterError('Structured call category must match the Anchor prospect category');
  }
  for (const field of ['contact_role', 'interest_level', 'next_step']) {
    if (typeof value[field] !== 'string' || !value[field].trim() || value[field].length > 200) {
      throw phoneSetterError(`Structured call details require ${field}`);
    }
  }
  if (typeof value.decision_maker_reached !== 'boolean') {
    throw phoneSetterError('Structured call details require decision_maker_reached');
  }
  if (value.objection_codes !== undefined && (!Array.isArray(value.objection_codes)
    || value.objection_codes.length > 10
    || value.objection_codes.some(item => typeof item !== 'string' || item.length > 80))) {
    throw phoneSetterError('objection_codes must be an array of short strings');
  }
  if (value.follow_up_channel !== undefined && !['phone', 'email', 'sms', 'none'].includes(value.follow_up_channel)) {
    throw phoneSetterError('follow_up_channel is invalid');
  }
  return {
    category: value.category,
    contact_role: value.contact_role.trim(),
    decision_maker_reached: value.decision_maker_reached,
    interest_level: value.interest_level.trim(),
    objection_codes: value.objection_codes || [],
    next_step: value.next_step.trim(),
    follow_up_channel: value.follow_up_channel || 'phone',
    manual_notes: typeof value.manual_notes === 'string' ? value.manual_notes.slice(0, 1000) : null,
  };
}

function validateDraftInput(input = {}) {
  const channel = String(input.channel || '').trim().toLowerCase();
  const body = String(input.body || '').trim();
  if (!DRAFT_CHANNELS.has(channel)) throw phoneSetterError('Draft channel must be email or sms');
  if (!body || body.length > 5000) throw phoneSetterError('Draft body is required and must be 5000 characters or fewer');
  return { channel, body };
}

module.exports = {
  ANCHOR_PHONE_SETTER_CATEGORIES,
  CATEGORY_PRIORITY,
  DRAFT_CHANNELS,
  DRAFT_STATUSES,
  categoryPriority,
  isAnchorPhoneSetter,
  phoneSetterError,
  priorityReason,
  validateDraftInput,
  validateStructuredDetails,
};
