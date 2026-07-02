'use strict';

const assert = require('assert');
const {
  DISPOSITION_SET,
  applyProspectDisposition,
  resolveCallbackAt,
} = require('../utils/callDispositions');

for (const value of ['gatekeeper_relayed', 'gatekeeper_blocked', 'incumbent_all_set']) {
  assert(DISPOSITION_SET.has(value), `${value} should be a supported disposition`);
}

const now = new Date('2026-07-02T14:00:00.000Z');
const nurture = resolveCallbackAt('incumbent_all_set', null, now);
const nurtureDays = (nurture.getTime() - now.getTime()) / 86400000;
assert(nurtureDays >= 89 && nurtureDays <= 91, 'all-set callback should default to about 90 days');

async function captureUpdate(disposition, callbackAt = null) {
  let captured;
  const db = {
    async query(sql, params) {
      captured = { sql, params };
      return { rows: [{ id: params[0], client_id: params[1] }] };
    },
  };
  await applyProspectDisposition(db, {
    prospectId: 'prospect-1',
    clientId: 10,
    disposition,
    callbackAt,
  });
  return captured;
}

(async () => {
  const interested = await captureUpdate('answered_interested');
  assert.match(interested.sql, /status = 'warm'/);
  assert.match(interested.sql, /setter_status = 'follow_up'/);
  assert.match(interested.sql, /is_hot = true/);
  assert.deepStrictEqual(interested.params.slice(0, 2), ['prospect-1', 10]);

  const allSet = await captureUpdate('incumbent_all_set', nurture);
  assert.match(allSet.sql, /status = 'cold'/);
  assert.match(allSet.sql, /setter_status = 'follow_up'/);
  assert.strictEqual(allSet.params[2], nurture);

  const wrongNumber = await captureUpdate('wrong_number');
  assert.match(wrongNumber.sql, /phone = NULL/);
  assert.doesNotMatch(wrongNumber.sql, /do_not_contact/);
  assert.doesNotMatch(wrongNumber.sql, /SET\s+status = 'dead'/);
  assert.strictEqual(wrongNumber.params.length, 2);

  const rejected = await captureUpdate('answered_not_interested');
  assert.match(rejected.sql, /status = 'dead'/);
  assert.match(rejected.sql, /setter_status = 'dead'/);
  assert.strictEqual(rejected.params.length, 2);

  console.log('Call disposition tests passed');
})().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
