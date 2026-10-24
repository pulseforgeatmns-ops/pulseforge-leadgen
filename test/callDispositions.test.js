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
  assert.match(wrongNumber.sql, /setter_status = 'follow_up'/);
  assert.doesNotMatch(wrongNumber.sql, /do_not_contact/);
  assert.doesNotMatch(wrongNumber.sql, /SET\s+status = 'dead'/);
  assert.strictEqual(wrongNumber.params.length, 2);

  // Phase B nurture rule: "not interested" is a long-dated nurture, not Dead.
  const rejectedCallback = resolveCallbackAt('answered_not_interested', null, now);
  const rejectedDays = (rejectedCallback.getTime() - now.getTime()) / 86400000;
  assert(rejectedDays >= 89 && rejectedDays <= 91, 'not-interested callback should default to about 90 days');
  const rejected = await captureUpdate('answered_not_interested', rejectedCallback);
  assert.match(rejected.sql, /status = 'cold'/);
  assert.match(rejected.sql, /setter_status = 'follow_up'/);
  assert.match(rejected.sql, /is_hot = false/);
  assert.doesNotMatch(rejected.sql, /'dead'/);
  assert.strictEqual(rejected.params[2], rejectedCallback);

  // disqualified stays permanent Dead, without global suppression.
  const disqualified = await captureUpdate('disqualified');
  assert.match(disqualified.sql, /status = 'dead'/);
  assert.match(disqualified.sql, /setter_status = 'dead'/);
  assert.doesNotMatch(disqualified.sql, /do_not_contact/);

  // Phase B terminal suppression: do_not_call is Dead AND globally suppressed.
  assert(DISPOSITION_SET.has('do_not_call'), 'do_not_call should be a supported disposition');
  assert.strictEqual(resolveCallbackAt('do_not_call', null, now), null);
  const dnc = await captureUpdate('do_not_call');
  assert.match(dnc.sql, /status = 'dead'/);
  assert.match(dnc.sql, /setter_status = 'dead'/);
  assert.match(dnc.sql, /do_not_contact = true/);
  assert.match(dnc.sql, /callback_at = NULL/);

  console.log('Call disposition tests passed');
})().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
