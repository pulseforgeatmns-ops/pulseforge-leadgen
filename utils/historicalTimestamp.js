'use strict';

const SUPPORTED_TIMEZONE = 'America/New_York';
const DERIVATION_HOUR = 12;

function localParts(timestamp, timezone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date(timestamp));
  return Object.fromEntries(parts.filter(part => part.type !== 'literal').map(part => [part.type, part.value]));
}

function deriveTimestampFromHistoricalDate(value) {
  if (!value || typeof value !== 'object') throw new Error('Historical date representation is required');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value.local_date || '')) throw new Error('local_date must use YYYY-MM-DD');
  if (value.timezone !== SUPPORTED_TIMEZONE) throw new Error(`timezone must be ${SUPPORTED_TIMEZONE}`);
  if (value.precision !== 'day') throw new Error('precision must be day');
  if (value.operator_confirmed !== true) throw new Error('operator_confirmed must be true');

  const [year, month, day] = value.local_date.split('-').map(Number);
  const probe = Date.UTC(year, month - 1, day, DERIVATION_HOUR, 0, 0);
  const probeParts = localParts(probe, value.timezone);
  const representedAsUtc = Date.UTC(
    Number(probeParts.year),
    Number(probeParts.month) - 1,
    Number(probeParts.day),
    Number(probeParts.hour),
    Number(probeParts.minute),
    Number(probeParts.second)
  );
  const offset = representedAsUtc - probe;
  const derived = new Date(Date.UTC(year, month - 1, day, DERIVATION_HOUR, 0, 0) - offset);
  const derivedParts = localParts(derived, value.timezone);
  if (`${derivedParts.year}-${derivedParts.month}-${derivedParts.day}` !== value.local_date
    || Number(derivedParts.hour) !== DERIVATION_HOUR
    || derivedParts.minute !== '00'
    || derivedParts.second !== '00') {
    throw new Error('Could not deterministically derive the historical date timestamp');
  }

  return {
    timestamp: derived.toISOString(),
    provenance: {
      local_date: value.local_date,
      timezone: value.timezone,
      precision: value.precision,
      operator_confirmed: true,
      derived: true,
      clock_time_observed: false,
      derivation_rule: 'local_noon_v1',
      derived_timestamp: derived.toISOString(),
    },
  };
}

module.exports = {
  DERIVATION_HOUR,
  SUPPORTED_TIMEZONE,
  deriveTimestampFromHistoricalDate,
};
