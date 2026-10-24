'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { MaxTimestampNormalizationError, normalizeEventTimestamp } = require('../utils/maxTimestamp');

const INCIDENT_ISO = '2026-07-15T16:02:54.000Z';

test('supported timestamp forms normalize deterministically to UTC Date values', () => {
  const seconds = 1784131374;
  const milliseconds = seconds * 1000;
  const values = [
    seconds,
    String(seconds),
    milliseconds,
    String(milliseconds),
    INCIDENT_ISO,
    '2026-07-15T12:02:54-04:00',
    new Date(INCIDENT_ISO),
  ];
  for (const value of values) {
    const normalized = normalizeEventTimestamp(value, { source: 'test', field: 'event_timestamp' });
    assert.ok(normalized instanceof Date);
    assert.equal(normalized.toISOString(), INCIDENT_ISO);
  }
});

test('the exact incident epoch is accepted as seconds and never preserved as a database value', () => {
  assert.equal(normalizeEventTimestamp('1784131374', { source: 'brevo', field: 'event_timestamp' }).toISOString(), INCIDENT_ISO);
});

test('invalid, ambiguous, missing, and out-of-range timestamps fail with structured context', () => {
  const invalid = [
    0, '0', -1, '-1', '', '   ', '1784131374.5', NaN, Infinity,
    '2100-01-01T00:00:00.001Z', '1999-12-31T23:59:59.999Z',
    '2026-99-99T00:00:00Z', 'July 15, 2026', null, undefined, new Date('invalid'),
  ];
  for (const value of invalid) {
    assert.throws(
      () => normalizeEventTimestamp(value, { source: 'brevo', field: 'event_timestamp' }),
      error => error instanceof MaxTimestampNormalizationError
        && error.code === 'MAX_TIMESTAMP_INVALID'
        && error.source === 'brevo'
        && error.field === 'event_timestamp'
    );
  }
});
