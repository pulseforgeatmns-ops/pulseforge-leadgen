'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { PostgresSignalStore } = require('../storage/PostgresSignalStore');
const { ensureSignalSchema } = require('../storage/ensureSignalSchema');

describe('Signal V1 Postgres persistence', () => {
  /** @type {Awaited<ReturnType<typeof startDisposablePostgres>>|null} */
  let instance = null;
  /** @type {import('pg').Pool|null} */
  let pool = null;

  before(async (t) => {
    let Pool;
    try {
      require.resolve('pg');
      ({ Pool } = require('pg'));
    } catch (err) {
      t.skip(`pg module unavailable: ${err.message}`);
      return;
    }
    const { startDisposablePostgres } = require('../../../test/helpers/disposablePostgres');
    try {
      instance = await startDisposablePostgres(`signal-v1-pg-${process.pid}-`, {
        socketPrefix: 'sigpg-',
      });
    } catch (err) {
      t.skip(`PostgreSQL unavailable: ${err.message}`);
      return;
    }
    pool = new Pool({ connectionString: instance.connectionString });
    await ensureSignalSchema(pool);
  });

  after(async () => {
    if (pool) await pool.end();
    if (instance) await instance.stop();
  });

  it('events and market observations survive store reconstruction', async () => {
    const store1 = new PostgresSignalStore(pool);
    await store1.upsertToken({
      tokenAddress: 'PersistToken1111111111111111111111111111111111',
      chain: 'solana',
      ticker: 'PERSIST',
    });
    await store1.insertEvent({
      tokenAddress: 'PersistToken1111111111111111111111111111111111',
      eventType: 'CALL',
      occurredAt: new Date('2026-09-29T18:00:00Z'),
      observedAt: new Date('2026-09-29T18:00:00Z'),
      sourceType: 'telegram',
      payload: { message: 'persist test' },
    });
    await store1.insertMarketObservation({
      tokenAddress: 'PersistToken1111111111111111111111111111111111',
      occurredAt: new Date('2026-09-29T18:01:00Z'),
      priceUsd: 0.42,
      intervalSeconds: 60,
      provider: 'test',
    });

    const store2 = new PostgresSignalStore(pool);
    const events = await store2.getEventsForToken(
      'PersistToken1111111111111111111111111111111111'
    );
    const obs = await store2.getMarketObservationsForToken(
      'PersistToken1111111111111111111111111111111111'
    );

    assert.equal(events.length, 1);
    assert.equal(obs.length, 1);
    assert.equal(obs[0].priceUsd, 0.42);
  });
});
