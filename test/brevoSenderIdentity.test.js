'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  insertBrevoEvent,
  resolveEventSendingDomain,
} = require('../utils/brevoEvents');
const { ingestBrevoResult } = require('../services/emmettOutbound');
const { SENDER_IDENTITY_STATUS } = require('../utils/canonicalSenderIdentity');

function createMockPool({ tenantDomain = 'domain-a.com', prospect } = {}) {
  const inserts = [];
  const events = [];
  return {
    inserts,
    events,
    async query(sql, params = []) {
      const text = String(sql);
      if (/ALTER TABLE email_events/i.test(text) || /DO \$\$/i.test(text) || /CREATE INDEX/i.test(text)) {
        return { rows: [], rowCount: 0 };
      }
      if (/CREATE TABLE/i.test(text) || /CREATE TYPE/i.test(text)) {
        return { rows: [], rowCount: 0 };
      }
      if (/FROM clients/i.test(text) && /sending_domain/i.test(text)) {
        return { rows: tenantDomain ? [{ sending_domain: tenantDomain }] : [] };
      }
      if (/FROM prospects/i.test(text)) {
        return {
          rows: prospect ? [prospect] : [{
            id: 101,
            client_id: 10,
            email: 'alex@harborlaw.com',
            vertical: 'law_firm',
          }],
        };
      }
      if (/FROM outbound_execution_records/i.test(text) || /FROM amo_outbound/i.test(text)) {
        return { rows: [] };
      }
      if (/FROM agent_log/i.test(text) && /SELECT/i.test(text)) {
        return { rows: [] };
      }
      if (/INSERT INTO email_events/i.test(text)) {
        inserts.push({ sql: text, params });
        events.push({
          event_id: params[0],
          prospect_id: params[1],
          client_id: params[2],
          sending_domain: params[3],
          recipient_email: params[4],
          event_type: params[5],
          sender_identity_status: params[17],
          sender_identity_reason: params[18],
        });
        return {
          rowCount: 1,
          rows: [{ id: inserts.length, open_source: 'unknown', open_source_reason: null, inserted: true }],
        };
      }
      if (/INSERT INTO agent_log/i.test(text)) {
        return { rows: [], rowCount: 1 };
      }
      if (/FROM email_events/i.test(text)) {
        return { rows: [] };
      }
      return { rows: [], rowCount: 0 };
    },
  };
}

describe('Brevo event sending-domain identity', () => {
  it('resolves event domain from sender evidence only — never fabricates the tenant domain', () => {
    const known = resolveEventSendingDomain({
      event: 'opened',
      email: 'alex@harborlaw.com',
      sender: 'hello@domain-b.com',
    }, {});
    assert.equal(known.domain, 'domain-b.com');
    assert.equal(known.source, 'sender_email');

    const unknown = resolveEventSendingDomain({
      event: 'opened',
      email: 'alex@harborlaw.com',
    }, {});
    assert.equal(unknown.domain, null);
    assert.equal(unknown.source, null);
  });

  it('preserves a foreign-domain event for audit and excludes it from Emmett reputation', async () => {
    const pool = createMockPool({ tenantDomain: 'domain-a.com' });
    const result = await insertBrevoEvent({
      event: 'opened',
      email: 'alex@harborlaw.com',
      sender: 'notify@domain-b.com',
      client_id: 10,
      date: '2026-08-30T12:00:00Z',
    }, pool);

    assert.equal(result.sender_identity_status, SENDER_IDENTITY_STATUS.MISMATCH);
    assert.equal(result.reputation_ingest, false);
    assert.equal(result.event_sending_domain, 'domain-b.com');
    assert.equal(result.tenant_sending_domain, 'domain-a.com');
    assert.equal(result.sending_domain, 'domain-b.com');
    assert.equal(pool.inserts.length, 1);
    assert.equal(pool.events[0].sender_identity_status, 'mismatch');
    assert.equal(pool.events[0].sending_domain, 'domain-b.com');

    const ingested = await ingestBrevoResult(result, { persist: false });
    assert.equal(ingested.skipped, true);
    assert.equal(ingested.reason, 'sender_identity_mismatch');
  });

  it('processes a matching-domain provider event normally', async () => {
    const pool = createMockPool({ tenantDomain: 'domain-a.com' });
    const result = await insertBrevoEvent({
      event: 'opened',
      email: 'alex@harborlaw.com',
      sender: 'hello@domain-a.com',
      client_id: 10,
      date: '2026-08-30T12:00:00Z',
    }, pool);

    assert.equal(result.sender_identity_status, SENDER_IDENTITY_STATUS.MATCH);
    assert.equal(result.reputation_ingest, true);
    assert.equal(result.event_sending_domain, 'domain-a.com');
    assert.equal(result.skipped, undefined);
  });

  it('handles missing sender-domain evidence as unknown, distinct from mismatch', async () => {
    const pool = createMockPool({ tenantDomain: 'domain-a.com' });
    const result = await insertBrevoEvent({
      event: 'delivered',
      email: 'alex@harborlaw.com',
      client_id: 10,
      date: '2026-08-30T12:00:00Z',
    }, pool);

    assert.equal(result.sender_identity_status, SENDER_IDENTITY_STATUS.UNKNOWN);
    assert.equal(result.sender_identity_reason, 'missing_sender_domain');
    assert.equal(result.reputation_ingest, true);
    assert.equal(result.event_sending_domain, null);
    assert.notEqual(result.sender_identity_status, SENDER_IDENTITY_STATUS.MISMATCH);
  });
});
