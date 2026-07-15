'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { DURABLE_TYPES, cursorFor, normalizeEmailRow, normalizeTouchpointRow, parseArgs } = require('../scripts/backfillMaxSignals');

const base = { client_id:1, prospect_id:'550e8400-e29b-41d4-a716-446655440000', source_record_id:'event-1', event_timestamp:'2026-07-15T12:00:00Z' };

test('historical email normalization keeps human, proxy, and unknown opens distinct', () => {
  assert.equal(normalizeEmailRow({...base,raw_type:'opened',open_source:'human'}).event_type,'email_human_opened');
  assert.equal(normalizeEmailRow({...base,raw_type:'opened',open_source:'proxy'}).event_type,'email_proxy_opened');
  assert.equal(normalizeEmailRow({...base,raw_type:'opened',open_source:'unknown'}).event_type,'email_unknown_opened');
  assert.equal(normalizeEmailRow({...base,raw_type:'invalid'}).event_type,'email_hard_bounced_confirmed_invalid');
});

test('reply normalization and durable classification are explicit', () => {
  assert.equal(normalizeTouchpointRow({...base,raw_type:'inbound_reply',payload:{classification:'interested'}}).event_type,'email_positive_reply');
  assert.equal(normalizeTouchpointRow({...base,raw_type:'out_of_office',payload:{}}).event_type,'email_out_of_office');
  assert.equal(DURABLE_TYPES.has('email_positive_reply'),true);
  assert.equal(DURABLE_TYPES.has('email_human_opened'),false);
});

test('historical backfill is dry-run, bounded, dated, and resumable by default', () => {
  const options=parseArgs(['--client-id=10','--limit','25','--cursor','cursor'],new Date('2026-07-15T12:00:00Z'));
  assert.equal(options.apply,false); assert.equal(options.limit,25); assert.equal(options.cursor,'cursor');
  assert.match(cursorFor({...base,source:'brevo',event_type:'email_human_opened'}),/^2026-07-15T12:00:00\.000Z\|brevo\|event-1/);
});
