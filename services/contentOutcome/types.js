'use strict';

/**
 * Content Outcome Intelligence types (SPEC-092 / planning draft SPEC-085).
 * Extends Outcome Intelligence (SPEC-013): record evidence, never mutate Paige strategy.
 */

const CHANNELS = Object.freeze({
  LINKEDIN: 'linkedin',
});

const OBJECTIVES = Object.freeze([
  'awareness',
  'category_creation',
  'audience_growth',
  'engagement',
  'thought_leadership',
  'lead_generation',
  'partnership_generation',
  'launch_runway',
]);

const BUSINESS_OUTCOME_TYPES = Object.freeze([
  'qualified_dm',
  'prospect_conversation',
  'partner_conversation',
  'builder_connection',
  'demo_interest',
  'meeting_booked',
  'pilot_interest',
  'customer_opportunity',
  'other',
]);

const ATTRIBUTION_LEVELS = Object.freeze([
  'direct',
  'likely',
  'possible',
  'unknown',
]);

const SIGNAL_TYPES = Object.freeze([
  'message_resonance',
  'audience_signal',
  'objection',
  'question',
  'language_adoption',
  'partnership_signal',
  'buyer_signal',
  'technical_interest',
  'unexpected_response',
  'other',
]);

const EVIDENCE_KINDS = Object.freeze({
  OPERATOR_OBSERVATION: 'operator_observation',
  EXTERNAL_OBSERVATION: 'external_observation',
});

class ContentOutcomeError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ContentOutcomeError';
    this.code = code;
    this.status = status;
  }
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function normalizeTenantId(value) {
  if (value == null || String(value).trim() === '') {
    throw new ContentOutcomeError('tenant_required', 'tenantId is required');
  }
  return String(value).trim();
}

/**
 * @param {unknown} value
 * @returns {number}
 */
function normalizeClientId(value) {
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0) {
    throw new ContentOutcomeError('client_required', 'clientId must be a positive integer');
  }
  return n;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @returns {number|null}
 */
function optionalNonNegativeInt(value, field) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 0) {
    throw new ContentOutcomeError('invalid_metric', `${field} must be a non-negative integer`);
  }
  return n;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @returns {string|null}
 */
function optionalText(value, field, maxLen = 8000) {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.length > maxLen) {
    throw new ContentOutcomeError('invalid_text', `${field} exceeds ${maxLen} characters`);
  }
  return text;
}

/**
 * @param {unknown} value
 * @param {readonly string[]} allowed
 * @param {string} field
 * @param {{ required?: boolean, allowUnknown?: boolean }} [opts]
 */
function optionalEnum(value, allowed, field, opts = {}) {
  if (value == null || value === '') {
    if (opts.required) {
      throw new ContentOutcomeError('required_field', `${field} is required`);
    }
    return null;
  }
  const text = String(value).trim().toLowerCase();
  if (!allowed.includes(text)) {
    if (opts.allowUnknown) return text;
    throw new ContentOutcomeError(
      'invalid_enum',
      `${field} must be one of: ${allowed.join(', ')}`
    );
  }
  return text;
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function requireIsoTimestamp(value, field = 'timestamp') {
  if (value == null || String(value).trim() === '') {
    throw new ContentOutcomeError('required_field', `${field} is required`);
  }
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new ContentOutcomeError('invalid_timestamp', `${field} must be a valid ISO timestamp`);
  }
  return d.toISOString();
}

/**
 * @param {unknown} value
 * @returns {string|null}
 */
function optionalIsoTimestamp(value, field = 'timestamp') {
  if (value == null || value === '') return null;
  return requireIsoTimestamp(value, field);
}

/**
 * @param {unknown} value
 * @returns {string[]}
 */
function normalizeAudience(value) {
  if (value == null) return [];
  const list = Array.isArray(value)
    ? value
    : String(value)
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
  return list.map((s) => String(s).trim()).filter(Boolean);
}

/**
 * @param {unknown} value
 * @returns {number|null}
 */
function optionalConfidence(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0 || n > 1) {
    throw new ContentOutcomeError('invalid_confidence', 'confidence must be between 0 and 1');
  }
  return Math.round(n * 1000) / 1000;
}

module.exports = {
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  EVIDENCE_KINDS,
  ContentOutcomeError,
  normalizeTenantId,
  normalizeClientId,
  optionalNonNegativeInt,
  optionalText,
  optionalEnum,
  requireIsoTimestamp,
  optionalIsoTimestamp,
  normalizeAudience,
  optionalConfidence,
};
