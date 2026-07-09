const assert = require('node:assert/strict');
const test = require('node:test');

const {
  OPEN_SOURCE,
  classifyOpenSource,
  DELIVERY_COINCIDENT_THRESHOLD_SECONDS,
} = require('../utils/openSignalGate');

function poolWithSend({ hasSend = false, deliveredAt = null } = {}) {
  return {
    async query(sql) {
      if (/MIN\(event_at\) AS delivered_at/.test(sql)) {
        return { rows: [{ delivered_at: deliveredAt }] };
      }
      if (/AS has_send/.test(sql)) {
        return { rows: [{ has_send: hasSend }] };
      }
      return { rows: [] };
    },
  };
}

test('Brevo proxy open classifies as proxy before send lookup', async () => {
  let queries = 0;
  const pool = { async query() { queries++; return { rows: [] }; } };
  const result = await classifyOpenSource(pool, {
    eventType: 'opened_proxy',
    eventAt: '2026-07-09T12:00:00Z',
    payload: { event: 'loadedByProxy' },
  });

  assert.equal(result.openSource, OPEN_SOURCE.PROXY);
  assert.equal(result.reason, 'brevo_proxy_event');
  assert.equal(queries, 0);
});

test('open without a corresponding send fails closed as unknown', async () => {
  const result = await classifyOpenSource(poolWithSend({ hasSend: false }), {
    eventType: 'opened',
    eventAt: '2026-07-09T12:00:00Z',
    payload: { event: 'opened' },
    prospectId: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    clientId: 1,
    recipientEmail: 'owner@example.com',
  });

  assert.equal(result.openSource, OPEN_SOURCE.UNKNOWN);
  assert.equal(result.reason, 'no_corresponding_send');
});

test('sent non-proxy open classifies human outside delivery threshold', async () => {
  const thresholdMs = (DELIVERY_COINCIDENT_THRESHOLD_SECONDS + 5) * 1000;
  const result = await classifyOpenSource(poolWithSend({
    hasSend: true,
    deliveredAt: new Date(Date.parse('2026-07-09T12:00:00Z') - thresholdMs),
  }), {
    eventType: 'opened',
    eventAt: '2026-07-09T12:00:00Z',
    payload: { event: 'opened' },
    prospectId: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    clientId: 1,
    recipientEmail: 'owner@example.com',
  });

  assert.equal(result.openSource, OPEN_SOURCE.HUMAN);
  assert.equal(result.reason, 'sent_non_proxy_open');
});

test('delivery-coincident open classifies proxy', async () => {
  const result = await classifyOpenSource(poolWithSend({
    hasSend: true,
    deliveredAt: '2026-07-09T11:59:50Z',
  }), {
    eventType: 'opened',
    eventAt: '2026-07-09T12:00:00Z',
    payload: { event: 'opened' },
    prospectId: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    clientId: 1,
    recipientEmail: 'owner@example.com',
  });

  assert.equal(result.openSource, OPEN_SOURCE.PROXY);
  assert.equal(result.reason, 'delivery_coincident');
});
