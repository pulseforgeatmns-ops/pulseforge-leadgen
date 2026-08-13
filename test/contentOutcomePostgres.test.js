'use strict';

/**
 * Disposable Postgres coverage for Content Outcome Intelligence (SPEC-092).
 * Runs when CONTENT_OUTCOME_TEST_POSTGRES=true or MAX_SMOKE_DISPOSABLE_PG=true.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  createContentOutcomeService,
  createPostgresContentOutcomeStore,
} = require('../services/contentOutcome');

const enabled =
  process.env.CONTENT_OUTCOME_TEST_POSTGRES === 'true' ||
  process.env.MAX_SMOKE_DISPOSABLE_PG === 'true';

(enabled ? describe : describe.skip)('contentOutcome postgres', () => {
  it('applies migration idempotently and enforces tenant isolation', async (t) => {
    let postgres;
    try {
      postgres = await startDisposablePostgres('content-outcome-pg-');
    } catch (error) {
      t.skip(`disposable postgres unavailable: ${error.message.split('\n')[0]}`);
      return;
    }

    const pool = new Pool({ connectionString: postgres.connectionString });
    try {
      const sql = fs.readFileSync(
        path.join(__dirname, '..', 'migrations', '2026-08-13-content-outcome-intelligence.sql'),
        'utf8'
      );
      await pool.query(sql);
      await pool.query(sql); // idempotent

      const store = createPostgresContentOutcomeStore(pool);
      const service = createContentOutcomeService({ store });

      const a = await service.createPublication({
        client_id: 1,
        title: 'Tenant 1 post',
        channel: 'linkedin',
        published_at: '2026-08-01T12:00:00.000Z',
        objective: 'awareness',
      });
      await service.addPerformanceSnapshot('1', a.publication.id, {
        impressions: 1000,
        comments: 5,
      });
      await service.addBusinessOutcome('1', a.publication.id, {
        outcome_type: 'partner_conversation',
        attribution: 'direct',
        description: 'Inbound partner DM',
      });
      await service.addQualitativeSignal('1', a.publication.id, {
        signal_type: 'message_resonance',
        description: 'Operators engaged with the thesis',
      });

      const b = await service.createPublication({
        client_id: 2,
        title: 'Tenant 2 post',
        channel: 'linkedin',
        published_at: '2026-08-01T12:00:00.000Z',
      });

      await assert.rejects(() =>
        service.getPublicationOutcome('2', a.publication.id)
      );

      const full = await service.getPublicationOutcome('1', a.publication.id);
      assert.equal(full.performance_snapshots.length, 1);
      assert.equal(full.business_outcomes.length, 1);
      assert.equal(full.qualitative_signals.length, 1);

      // Second snapshot does not overwrite the first.
      await service.addPerformanceSnapshot('1', a.publication.id, {
        impressions: 5000,
        observed_at: '2026-08-02T12:00:00.000Z',
      });
      const again = await service.getPublicationOutcome('1', a.publication.id);
      assert.equal(again.performance_snapshots.length, 2);
      assert.equal(again.performance_snapshots[0].impressions, 1000);
      assert.equal(again.performance_snapshots[1].impressions, 5000);

      const list = await service.listContentOutcomes({ tenantId: '1' });
      assert.equal(list.items.length, 1);
      assert.equal(list.comparison.total_partner_conversations, 1);
      assert.ok(b.publication.id);
    } finally {
      await pool.end();
      await postgres.stop();
    }
  });
});
