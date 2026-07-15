'use strict';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function tokenizeArgs(argv = []) {
  const values = new Map();
  const flags = new Set();
  for (let index = 0; index < argv.length; index++) {
    const raw = argv[index];
    if (!String(raw).startsWith('--')) throw new Error(`Unexpected argument: ${raw}`);
    const equals = raw.indexOf('=');
    if (equals > 2) {
      const name = raw.slice(0, equals);
      const value = raw.slice(equals + 1);
      if (!value) throw new Error(`${name} requires a value`);
      if (values.has(name) || flags.has(name)) throw new Error(`Duplicate argument: ${name}`);
      values.set(name, value);
      continue;
    }
    const next = argv[index + 1];
    if (next !== undefined && !String(next).startsWith('--')) {
      if (values.has(raw) || flags.has(raw)) throw new Error(`Duplicate argument: ${raw}`);
      values.set(raw, next);
      index++;
    } else {
      if (values.has(raw) || flags.has(raw)) throw new Error(`Duplicate argument: ${raw}`);
      flags.add(raw);
    }
  }
  return { values, flags };
}

function assertAllowed(parsed, { values = [], flags = [] } = {}) {
  const allowedValues = new Set(values);
  const allowedFlags = new Set(flags);
  for (const name of parsed.values.keys()) {
    if (!allowedValues.has(name)) throw new Error(`Unknown option: ${name}`);
  }
  for (const name of parsed.flags) {
    if (!allowedFlags.has(name)) throw new Error(`Unknown flag: ${name}`);
  }
}

function optionalPositiveInteger(value, label) {
  if (value === undefined || value === null || value === '') return null;
  if (!/^\d+$/.test(String(value))) throw new Error(`${label} must be a positive integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error(`${label} must be a positive integer`);
  return parsed;
}

function boundedInteger(value, label, { defaultValue, min = 1, max = 2000 } = {}) {
  const parsed = value === undefined || value === null ? defaultValue : optionalPositiveInteger(value, label);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(`${label} must be between ${min} and ${max}`);
  }
  return parsed;
}

function optionalUuid(value, label) {
  if (value === undefined || value === null || value === '') return null;
  if (!UUID_RE.test(String(value))) throw new Error(`${label} must be a UUID`);
  return String(value).toLowerCase();
}

function optionalTimestamp(value, label) {
  if (value === undefined || value === null || value === '') return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new Error(`${label} must be a valid timestamp`);
  return date.toISOString();
}

module.exports = {
  UUID_RE,
  assertAllowed,
  boundedInteger,
  optionalPositiveInteger,
  optionalTimestamp,
  optionalUuid,
  tokenizeArgs,
};
