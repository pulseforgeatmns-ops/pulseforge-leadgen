const assert = require('node:assert/strict');
const { eventAt, eventId, internalEventType } = require('../utils/brevoEvents');

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

const eventCoverage = {
  request: 'sent',
  requests: 'sent',
  sent: 'sent',
  delivered: 'delivered',
  opened: 'opened',
  uniqueOpened: 'opened',
  loadedByProxy: 'opened_proxy',
  proxyOpen: 'opened_proxy',
  uniqueProxyOpen: 'opened_proxy',
  clicks: 'clicked',
  deferred: 'deferred',
  soft_bounce: 'soft_bounce',
  softBounce: 'soft_bounce',
  softBounces: 'soft_bounce',
  hard_bounce: 'hard_bounce',
  hardBounce: 'hard_bounce',
  hardBounces: 'hard_bounce',
  invalid: 'invalid',
  blocked: 'blocked',
  error: 'error',
  spam: 'spam',
  complaint: 'spam',
  unsubscribe: 'unsubscribed',
  unsubscribed: 'unsubscribed',
};

for (const [rawType, canonicalType] of Object.entries(eventCoverage)) {
  assert.equal(internalEventType({ event: rawType }), canonicalType, rawType);
}

assert.equal(internalEventType({ event: 'not_a_brevo_event' }), null);

const openEvents = ['opened', 'uniqueOpened', 'loadedByProxy', 'proxyOpen', 'uniqueProxyOpen']
  .map(event => internalEventType({ event }));
assert.equal(openEvents.filter(event => event === 'opened').length, 2, 'default human-open numerator');
assert.equal(openEvents.filter(event => event === 'opened_proxy').length, 3, 'separate proxy-open count');
assert.equal(
  (openEvents.filter(event => event === 'opened').length / openEvents.length) * 100,
  40,
  'default open rate excludes proxy opens'
);

const sharedMessage = '<same-message@example.com>';
const sharedEvent = { date: '2026-07-02T14:00:00Z', messageId: sharedMessage };
assert.notEqual(
  eventId({ ...sharedEvent, event: 'opened' }, 'opened', 'recipient@example.com', sharedMessage),
  eventId({ ...sharedEvent, event: 'proxyOpen' }, 'opened_proxy', 'recipient@example.com', sharedMessage),
  'human and proxy opens retain distinct dedup identities'
);

console.log('Brevo eventAt tests passed');
