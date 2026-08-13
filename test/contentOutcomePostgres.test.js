'use strict';

/**
 * Postgres integration for SPEC-092 content outcome intelligence.
 * Enable with CONTENT_OUTCOME_TEST_POSTGRES=true.
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const enabled = process.env.CONTENT_OUTCOME_TEST_POSTGRES === 'true';
const root = path.join(__dirname, '..');

(enabled ? describe : describe.skip)('contentOutcomeIntelligence postgres', () => {
  let stop;
  let pool;
  let createContentPublication;
  let addPerformanceSnapshot;
  let addBusinessOutcome;
  let getPublicationOutcome;
  let createPostgresStore;

  before(async () => {
    const { startDisposablePostgres } = require('./helpers/disposablePostgres');
    const instance = await startDisposablePostgres('content-outcome-pg-');
    stop = () => instance.stop();
    process.env.DATABASE_URL = instance.connectionString;
    process.env.DATABASE_SSL = 'false';
    delete require.cache[require.resolve('../db')];
    delete require.cache[require.resolve('../services/contentOutcomeIntelligence')];

    pool = new Pool({ connectionString: instance.connectionString });
    await pool.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
    await pool.query(
      fs.readFileSync(
        path.join(root, 'migrations', '2026-08-13-content-outcome-intelligence.sql'),
        'utf8'
      )
    );

    ({
      createContentPublication,
      addPerformanceSnapshot,
      addBusinessOutcome,
      getPublicationOutcome,
      createPostgresStore,
    } = require('../services/contentOutcomeIntelligence'));
  });

  after(async () => {
    if (pool) await pool.end();
    if (stop) await stop();
  });

  it('persists publication + snapshots + outcomes with tenant isolation', async () => {
    const store = createPostgresStore(pool);
    const pub = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'pg-artifact',
        objective: 'thought_leadership',
        channel: 'linkedin',
      },
      { store }
    );
    await addPerformanceSnapshot(
      pub.id,
      { clientId: 1, impressions: 900, comments: 3 },
      { store }
    );
    await addPerformanceSnapshot(
      pub.id,
      { clientId: 1, impressions: 2200 },
      { store }
    );
    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: 'partner_conversation',
        attribution: 'direct',
        evidenceId: 'ev-pg',
      },
      { store }
    );

    const full = await getPublicationOutcome(pub.id, { store, clientId: 1 });
    assert.equal(full.performanceSnapshots.length, 2);
    assert.equal(full.businessOutcomes.length, 1);
    assert.equal(full.performanceSnapshots[0].impressions, 900);

    await assert.rejects(
      () => getPublicationOutcome(pub.id, { store, clientId: 2 }),
      (err) => err && err.code === 'publication_not_found'
    );
  });
});
