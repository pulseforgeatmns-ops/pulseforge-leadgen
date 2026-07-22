const test = require('node:test');
const assert = require('node:assert/strict');
const { _test } = require('../scoutUnenrichedEnrichmentAgent');

test('Anchor unenriched retry worker runs only after 5 PM ET on weekdays', () => {
  assert.equal(_test.scheduledWindowOpen(new Date('2026-07-21T20:59:00.000Z')), false); // Mon 4:59 PM ET
  assert.equal(_test.scheduledWindowOpen(new Date('2026-07-21T21:00:00.000Z')), true);  // Mon 5:00 PM ET
  assert.equal(_test.scheduledWindowOpen(new Date('2026-07-25T21:00:00.000Z')), false); // Sat 5:00 PM ET
});

test('Anchor retry worker is limited to the current priority verticals', () => {
  assert.deepEqual(_test.ANCHOR_PRIORITY_VERTICALS, ['property_manager', 'str_manager', 'commercial_office']);
});
