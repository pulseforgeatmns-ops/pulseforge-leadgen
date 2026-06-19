const assert = require('node:assert/strict');
const { eventAt } = require('../utils/brevoEvents');

const timestamp = 1781878018;

assert.equal(
  eventAt({ ts: timestamp, date: '2026-06-19 10:06:58' }).toISOString(),
  '2026-06-19T14:06:58.000Z'
);

assert.equal(
  eventAt({ date: '2026-06-19 10:06:58' }).toISOString(),
  '2026-06-19T14:06:58.000Z'
);

const beforeFallback = Date.now();
const fallback = eventAt({}).getTime();
const afterFallback = Date.now();
assert.ok(fallback >= beforeFallback && fallback <= afterFallback);

console.log('Brevo eventAt tests passed');
