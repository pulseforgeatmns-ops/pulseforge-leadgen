'use strict';

const MIN_SUPPORTED_TIMESTAMP_MS = Date.parse('2000-01-01T00:00:00.000Z');
const MAX_SUPPORTED_TIMESTAMP_MS = Date.parse('2100-01-01T00:00:00.000Z');
const ISO_TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;
const INTEGER_RE = /^-?\d+$/;

class MaxTimestampNormalizationError extends Error {
  constructor(message, { source = 'unknown', field = 'event_timestamp', value, normalizationCode }) {
    super(`${source}.${field}: ${message}`);
    this.name = 'MaxTimestampNormalizationError';
    this.code = 'MAX_TIMESTAMP_INVALID';
    this.normalization_error_code = normalizationCode;
    this.source = source;
    this.field = field;
    this.raw_timestamp = rawTimestampForMetadata(value);
  }
}

function rawTimestampForMetadata(value) {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? 'Invalid Date' : value.toISOString();
  if (typeof value === 'number' || typeof value === 'string' || value == null) return value;
  return String(value);
}

function timestampError(message, context, value, normalizationCode) {
  throw new MaxTimestampNormalizationError(message, { ...context, value, normalizationCode });
}

function assertSupportedRange(date, context, value) {
  const time = date.getTime();
  if (!Number.isFinite(time)) timestampError('timestamp is not a valid date', context, value, 'INVALID_DATE');
  if (time < MIN_SUPPORTED_TIMESTAMP_MS) {
    timestampError('timestamp predates the supported range', context, value, 'OUT_OF_RANGE_HISTORICAL');
  }
  if (time > MAX_SUPPORTED_TIMESTAMP_MS) {
    timestampError('timestamp exceeds the supported range', context, value, 'OUT_OF_RANGE_FUTURE');
  }
  return new Date(time);
}

function normalizeEpoch(value, numeric, context) {
  if (!Number.isFinite(numeric)) timestampError('numeric timestamp must be finite', context, value, 'NON_FINITE_TIMESTAMP');
  if (!Number.isInteger(numeric)) timestampError('numeric timestamp must be an integer', context, value, 'AMBIGUOUS_NUMERIC_TIMESTAMP');
  if (numeric <= 0) timestampError('epoch timestamp must be positive', context, value, 'NON_POSITIVE_EPOCH');
  const milliseconds = numeric < 1e12 ? numeric * 1000 : numeric;
  return assertSupportedRange(new Date(milliseconds), context, value);
}

function normalizeEventTimestamp(value, { source = 'unknown', field = 'event_timestamp' } = {}) {
  const context = { source, field };
  if (value === null || value === undefined) timestampError('timestamp is required', context, value, 'MISSING_TIMESTAMP');
  if (value instanceof Date) return assertSupportedRange(value, context, value);
  if (typeof value === 'number') return normalizeEpoch(value, value, context);
  if (typeof value !== 'string') timestampError('unsupported timestamp type', context, value, 'UNSUPPORTED_TIMESTAMP_TYPE');
  if (value.length === 0 || value.trim().length === 0) timestampError('timestamp is empty', context, value, 'EMPTY_TIMESTAMP');
  if (value !== value.trim()) timestampError('timestamp may not contain surrounding whitespace', context, value, 'AMBIGUOUS_TIMESTAMP');
  if (INTEGER_RE.test(value)) return normalizeEpoch(value, Number(value), context);
  if (/^[+-]?(?:\d+\.\d*|\d*\.\d+)$/.test(value)) {
    timestampError('decimal numeric timestamps are not supported', context, value, 'AMBIGUOUS_NUMERIC_TIMESTAMP');
  }
  if (!ISO_TIMESTAMP_RE.test(value)) timestampError('timestamp must be ISO-8601 with an explicit timezone', context, value, 'MALFORMED_TIMESTAMP');
  return assertSupportedRange(new Date(value), context, value);
}

module.exports = {
  ISO_TIMESTAMP_RE,
  MAX_SUPPORTED_TIMESTAMP_MS,
  MIN_SUPPORTED_TIMESTAMP_MS,
  MaxTimestampNormalizationError,
  normalizeEventTimestamp,
  rawTimestampForMetadata,
};
