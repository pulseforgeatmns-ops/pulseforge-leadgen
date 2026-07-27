'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { createKnowledgeRuntime, NODE_TYPES, personNodeId } = require('..');
const {
  KnowledgeWriter,
  OPERATIONAL_EVENTS,
  FLIGHT_STAGES,
  normalizeKnowledgeEvent,
  envelopeForCompany,
  envelopeForProspect,
  envelopeForOperationalEvent,
  operationalEventFromTouchpoint,
} = require('../dualWrite');

/** Minimal in-memory pool for outbox / flight / ledger tables used by KnowledgeWriter. */
function createMemoryPool() {
  const outbox = new Map();
  const flights = new Map();
  const ledger = new Map();
  let outboxSeq = 0;
  let flightSeq = 0;

  return {
    _outbox: outbox,
    _flights: flights,
    _ledger: ledger,
    async query(sql, params = []) {
      const q = String(sql).replace(/\s+/g, ' ').trim();

      if (q.startsWith('INSERT INTO knowledge_outbox')) {
        const [
          tenantId,
          key,
          eventType,
          entityId,
          entityType,
          source,
          payload,
          evidence,
          syncEnvelope,
        ] = params;
        const existing = [...outbox.values()].find(
          (r) => r.tenant_id === tenantId && r.idempotency_key === key
        );
        if (existing) {
          return { rows: [{ id: existing.id, status: existing.status, attempts: existing.attempts }], rowCount: 1 };
        }
        outboxSeq += 1;
        const id = `ob-${outboxSeq}`;
        const row = {
          id,
          tenant_id: tenantId,
          idempotency_key: key,
          event_type: eventType,
          entity_id: entityId,
          entity_type: entityType,
          source,
          payload: JSON.parse(payload),
          evidence: JSON.parse(evidence),
          sync_envelope: JSON.parse(syncEnvelope),
          status: 'pending',
          attempts: 0,
          last_error: null,
          next_retry_at: new Date(),
          created_at: new Date(),
          updated_at: new Date(),
          applied_at: null,
        };
        outbox.set(id, row);
        return { rows: [{ id, status: 'pending', attempts: 0 }], rowCount: 1 };
      }

      if (q.startsWith('UPDATE knowledge_outbox SET status = \'processing\'')) {
        const row = outbox.get(params[0]);
        if (row) {
          row.status = 'processing';
          row.attempts += 1;
          row.updated_at = new Date();
        }
        return { rows: [], rowCount: row ? 1 : 0 };
      }

      if (q.startsWith('UPDATE knowledge_outbox SET status = \'applied\'')) {
        const row = outbox.get(params[0]);
        if (row) {
          row.status = 'applied';
          row.applied_at = new Date();
          row.last_error = null;
          row.next_retry_at = null;
        }
        return { rows: [], rowCount: row ? 1 : 0 };
      }

      if (q.startsWith('UPDATE knowledge_outbox SET status = $2')) {
        const row = outbox.get(params[0]);
        if (row) {
          row.status = params[1];
          row.last_error = params[2];
          row.next_retry_at = new Date(Date.now() + Number(params[3] || 0));
        }
        return { rows: [], rowCount: row ? 1 : 0 };
      }

      if (q.startsWith('SELECT attempts FROM knowledge_outbox')) {
        const row = outbox.get(params[0]);
        return { rows: row ? [{ attempts: row.attempts }] : [], rowCount: row ? 1 : 0 };
      }

      if (q.includes('FROM knowledge_outbox') && q.includes('COUNT(*)')) {
        let rows = [...outbox.values()];
        if (params[0] != null && q.includes('tenant_id')) {
          rows = rows.filter((r) => r.tenant_id === String(params[0]));
        }
        let n = rows.length;
        if (q.includes("status IN ('pending', 'failed', 'processing')")) {
          n = rows.filter((r) =>
            ['pending', 'failed', 'processing'].includes(r.status)
          ).length;
        } else if (q.includes("status IN ('failed', 'dead')")) {
          n = rows.filter((r) => ['failed', 'dead'].includes(r.status)).length;
        } else if (q.includes("status = 'applied'")) {
          n = rows.filter((r) => r.status === 'applied').length;
        }
        return { rows: [{ n }], rowCount: 1 };
      }

      if (q.includes('FROM knowledge_outbox') && q.includes('status IN')) {
        if (q.includes('FOR UPDATE SKIP LOCKED')) {
          const rows = [...outbox.values()]
            .filter((r) => r.status === 'pending' || r.status === 'failed')
            .filter((r) => !r.next_retry_at || r.next_retry_at <= new Date())
            .filter((r) => r.attempts < Number(params[1]))
            .sort((a, b) => a.created_at - b.created_at)
            .slice(0, Number(params[2]));
          return { rows, rowCount: rows.length };
        }
        if (q.includes('ORDER BY applied_at')) {
          const applied = [...outbox.values()]
            .filter((r) => r.status === 'applied')
            .sort((a, b) => (b.applied_at || 0) - (a.applied_at || 0));
          const row = applied[0];
          return {
            rows: row
              ? [
                  {
                    applied_at: row.applied_at,
                    created_at: row.created_at,
                    event_type: row.event_type,
                    tenant_id: row.tenant_id,
                  },
                ]
              : [],
            rowCount: row ? 1 : 0,
          };
        }
      }

      if (q.includes('FROM knowledge_outbox') && q.includes('ORDER BY applied_at')) {
        const applied = [...outbox.values()]
          .filter((r) => r.status === 'applied')
          .filter((r) =>
            params[0] != null && q.includes('tenant_id')
              ? r.tenant_id === String(params[0])
              : true
          )
          .sort((a, b) => (b.applied_at || 0) - (a.applied_at || 0));
        const row = applied[0];
        return {
          rows: row
            ? [
                {
                  applied_at: row.applied_at,
                  created_at: row.created_at,
                  event_type: row.event_type,
                  tenant_id: row.tenant_id,
                },
              ]
            : [],
          rowCount: row ? 1 : 0,
        };
      }

      if (q.startsWith('INSERT INTO knowledge_flight_stages')) {
        const [flightId, tenantId, entityId, entityType, stage, status, occurredAt, metadata] =
          params;
        const key = `${flightId}::${stage}`;
        flightSeq += 1;
        const row = {
          id: flightSeq,
          flight_id: flightId,
          tenant_id: tenantId,
          entity_id: entityId,
          entity_type: entityType,
          stage,
          status,
          occurred_at: occurredAt ? new Date(occurredAt) : new Date(),
          metadata: JSON.parse(metadata || '{}'),
        };
        flights.set(key, row);
        return { rows: [row], rowCount: 1 };
      }

      if (q.startsWith('SELECT * FROM knowledge_flight_stages WHERE flight_id')) {
        const rows = [...flights.values()]
          .filter((r) => r.flight_id === params[0])
          .filter((r) => (params[1] == null ? true : r.tenant_id === params[1]))
          .sort((a, b) => a.occurred_at - b.occurred_at);
        return { rows, rowCount: rows.length };
      }

      if (q.includes('FROM knowledge_evidence')) {
        return { rows: [{ n: 0 }], rowCount: 1 };
      }

      // Ledger ops used if writer hits ledger via sync — sync has its own ledger
      if (q.includes('knowledge_sync_ledger')) {
        const key = `${params[0]}::${params[1]}`;
        if (q.startsWith('SELECT 1')) {
          return { rows: ledger.has(key) ? [{ '?column?': 1 }] : [], rowCount: ledger.has(key) ? 1 : 0 };
        }
        if (q.startsWith('INSERT')) {
          ledger.set(key, params[2]);
          return { rows: [], rowCount: 1 };
        }
      }

      throw new Error(`MemoryPool unhandled SQL: ${q.slice(0, 120)}`);
    },
  };
}

describe('SPEC-014 Knowledge Dual-Write', () => {
  let runtime;
  let pool;
  let writer;

  beforeEach(() => {
    runtime = createKnowledgeRuntime({ withSync: true, startIngestor: true });
    pool = createMemoryPool();
    writer = new KnowledgeWriter({ pool, sync: runtime.sync });
  });

  it('normalizes KnowledgeEvent contract without anonymous events', () => {
    assert.throws(() => normalizeKnowledgeEvent({ eventType: 'x', source: 'y' }), /tenantId/);
    const evt = normalizeKnowledgeEvent({
      tenantId: 10,
      eventType: OPERATIONAL_EVENTS.CONTACT_DISCOVERED,
      source: 'scout',
      entityId: 99,
      entityType: 'prospect',
    });
    assert.equal(evt.tenantId, '10');
    assert.ok(evt.id);
    assert.equal(evt.eventType, OPERATIONAL_EVENTS.CONTACT_DISCOVERED);
  });

  it('maps touchpoint actions to operational event types', () => {
    assert.equal(
      operationalEventFromTouchpoint('email', 'opened'),
      OPERATIONAL_EVENTS.EMAIL_OPENED
    );
    assert.equal(
      operationalEventFromTouchpoint('phone', 'voicemail'),
      OPERATIONAL_EVENTS.VOICEMAIL
    );
    assert.equal(
      operationalEventFromTouchpoint('meeting', 'cancelled'),
      OPERATIONAL_EVENTS.MEETING_CANCELLED
    );
  });

  it('dual-writes company + prospect idempotently into Knowledge', async () => {
    const company = {
      id: 101,
      client_id: 10,
      name: 'Anchor Law Office',
      industry: 'law_firm',
      location: 'Manchester NH',
      created_at: '2026-07-26T12:00:00.000Z',
    };
    const first = await writer.writeCompany(company, {
      operationalEventType: OPERATIONAL_EVENTS.COMPANY_DISCOVERED,
      source: 'scout',
    });
    assert.equal(first.status, 'applied');

    const replay = await writer.writeCompany(company, {
      operationalEventType: OPERATIONAL_EVENTS.COMPANY_DISCOVERED,
      source: 'scout',
    });
    assert.ok(replay.status === 'skipped' || replay.reason === 'already_applied');

    const prospect = {
      id: 501,
      client_id: 10,
      company_id: 101,
      first_name: 'Jordan',
      last_name: 'Lee',
      email: 'jordan@anchorlaw.test',
      job_title: 'Managing Partner',
      icp_score: 78,
      created_at: '2026-07-26T12:01:00.000Z',
    };
    const p = await writer.writeProspect(prospect, { source: 'scout' });
    assert.equal(p.status, 'applied');

    const person = await runtime.knowledge.findNode(
      '10',
      personNodeId('10', 501)
    );
    assert.equal(person.type, NODE_TYPES.PERSON);
    assert.equal(person.email, 'jordan@anchorlaw.test');

    const health = await writer.health({ tenantId: '10' });
    assert.ok(health.knowledgeEventsToday >= 1);
    assert.ok(health.lastSuccessfulWrite);
  });

  it('queues failed applies and retries via processOutbox', async () => {
    let failOnce = true;
    const flakySync = {
      apply: async (envelope) => {
        if (failOnce) {
          failOnce = false;
          throw new Error('knowledge temporarily unavailable');
        }
        return runtime.sync.apply(envelope);
      },
    };
    const flakyWriter = new KnowledgeWriter({ pool, sync: flakySync });
    const envelope = envelopeForCompany({
      id: 202,
      client_id: 10,
      name: 'Retry Co',
      created_at: '2026-07-26T13:00:00.000Z',
    });
    const failed = await flakyWriter.writeEnvelope(envelope, {
      eventType: OPERATIONAL_EVENTS.COMPANY_DISCOVERED,
      entityId: 202,
      entityType: 'company',
      source: 'test',
      markDiscovered: true,
    });
    assert.equal(failed.status, 'failed');

    // Make retry immediately due (production uses exponential backoff)
    for (const row of pool._outbox.values()) {
      row.next_retry_at = new Date(Date.now() - 1000);
    }

    const drained = await flakyWriter.processOutbox({ limit: 10 });
    assert.equal(drained.applied, 1);
  });

  it('writes operational recommendation/outcome evidence envelopes', async () => {
    const envelope = envelopeForOperationalEvent({
      id: 'rec-1',
      tenantId: '10',
      entityId: 'rec-1',
      entityType: 'recommendation',
      eventType: OPERATIONAL_EVENTS.REC_GENERATED,
      source: 'e2e',
      payload: { summary: 'Call Anchor Law' },
      evidence: { summary: 'Generated recommendation', confidence: 0.8 },
    });
    const result = await writer.writeEnvelope(envelope, {
      eventType: OPERATIONAL_EVENTS.REC_GENERATED,
      entityId: 'rec-1',
      entityType: 'recommendation',
      source: 'e2e',
    });
    assert.equal(result.status, 'applied');
  });

  it('records flight stages for discovery → knowledge written', async () => {
    await writer.writeProspect(
      {
        id: 777,
        client_id: 10,
        first_name: 'Sam',
        last_name: 'River',
        email: 'sam@river.test',
        created_at: '2026-07-26T14:00:00.000Z',
      },
      { source: 'scout' }
    );
    const { getFlightJourney } = require('../dualWrite/flightRecorder');
    const journey = await getFlightJourney(pool, {
      flightId: 'flight:10:prospect:777',
      tenantId: '10',
    });
    const discovered = journey.stages.find(
      (s) => s.stage === FLIGHT_STAGES.PROSPECT_DISCOVERED
    );
    const written = journey.stages.find(
      (s) => s.stage === FLIGHT_STAGES.KNOWLEDGE_WRITTEN
    );
    assert.equal(discovered.complete, true);
    assert.equal(written.complete, true);
  });

  it('exposes envelope helpers for company/prospect rows', () => {
    const c = envelopeForCompany({ id: 1, client_id: 10, name: 'X' });
    const p = envelopeForProspect({ id: 2, client_id: 10, first_name: 'A' });
    assert.ok(c.id);
    assert.ok(p.id);
    assert.equal(c.tenantId, '10');
  });
});
