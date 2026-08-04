'use strict';

/**
 * Postgres integration for SPEC-061 market intelligence ingestion.
 * Enable with MARKET_INTEL_TEST_POSTGRES=true (requires local postgres tooling).
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const enabled = process.env.MARKET_INTEL_TEST_POSTGRES === 'true';
const root = path.join(__dirname, '..');

function encodeBody(text) {
  return Buffer.from(text, 'utf8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
}

function fakeMessage({ id, threadId, from, subject, messageId, body, internalDate }) {
  return {
    id,
    threadId,
    internalDate: String(internalDate),
    payload: {
      mimeType: 'text/plain',
      body: { data: encodeBody(body) },
      headers: [
        { name: 'From', value: from },
        { name: 'Subject', value: subject },
        { name: 'Date', value: new Date(Number(internalDate)).toUTCString() },
        { name: 'Message-ID', value: messageId },
      ],
    },
  };
}

(enabled ? describe : describe.skip)('marketIntelligenceIngestion postgres', () => {
  let stop;
  let pool;
  let getCompanyTimeline;
  let importMarketIntelligence;

  before(async () => {
    const { startDisposablePostgres } = require('./helpers/disposablePostgres');
    const instance = await startDisposablePostgres('market-intel-pg-');
    stop = () => instance.stop();
    process.env.DATABASE_URL = instance.connectionString;
    process.env.DATABASE_SSL = 'false';
    delete require.cache[require.resolve('../db')];
    delete require.cache[require.resolve('../services/marketIntelligenceIngestion')];

    pool = new Pool({ connectionString: instance.connectionString });
    await pool.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
    await pool.query(fs.readFileSync(
      path.join(root, 'migrations', '2026-08-01-market-intelligence-ingestion.sql'),
      'utf8'
    ));

    ({ getCompanyTimeline, importMarketIntelligence } = require('../services/marketIntelligenceIngestion'));
  });

  after(async () => {
    if (pool) await pool.end();
    if (stop) await stop();
  });

  it('imports labeled messages, groups by company, and skips duplicates on re-run', async () => {
    const messages = [
      fakeMessage({
        id: 'g1',
        threadId: 't1',
        from: 'Apollo <hello@apollo.io>',
        subject: 'Touch 1',
        messageId: '<m1@apollo.io>',
        body: 'First touch https://apollo.io/1',
        internalDate: Date.parse('2024-05-04T12:00:00Z'),
      }),
      fakeMessage({
        id: 'g2',
        threadId: 't1',
        from: 'Apollo <hello@apollo.io>',
        subject: 'Touch 2',
        messageId: '<m2@apollo.io>',
        body: 'Second touch',
        internalDate: Date.parse('2024-05-07T12:00:00Z'),
      }),
      fakeMessage({
        id: 'g3',
        threadId: 't2',
        from: 'mystery-sender',
        subject: 'No domain',
        messageId: '<m3@local>',
        body: 'Unknown sender body',
        internalDate: Date.parse('2024-05-08T12:00:00Z'),
      }),
    ];

    const first = await importMarketIntelligence({
      pool,
      messages,
      dryRun: false,
      label: 'MARKET_INTEL',
      days: 365,
      limit: 1000,
    });

    assert.equal(first.imported, 3);
    assert.equal(first.duplicates, 0);
    assert.equal(first.unknownCompany, 1);
    assert.equal(first.importIntent, 'general_market_messaging');

    const companies = await pool.query('SELECT name, domain, is_unknown FROM market_companies ORDER BY name');
    assert.ok(companies.rows.some((row) => row.name === 'Apollo' && row.domain === 'apollo.io'));
    assert.ok(companies.rows.some((row) => row.is_unknown === true));

    const intents = await pool.query('SELECT DISTINCT import_intent FROM market_emails');
    assert.deepEqual(intents.rows.map((r) => r.import_intent).sort(), ['general_market_messaging']);

    const apollo = await pool.query(`SELECT id FROM market_companies WHERE domain = 'apollo.io'`);
    const timeline = await getCompanyTimeline(apollo.rows[0].id, { pool });
    assert.equal(timeline.length, 2);
    assert.equal(timeline[0].touch, 1);
    assert.equal(timeline[0].subject, 'Touch 1');
    assert.equal(timeline[0].importIntent, 'general_market_messaging');
    assert.equal(timeline[0].sourceIntent, 'general_market_messaging');
    assert.equal(timeline[1].touch, 2);
    assert.equal(timeline[1].subject, 'Touch 2');

    const second = await importMarketIntelligence({
      pool,
      messages: [
        ...messages,
        fakeMessage({
          id: 'g4',
          threadId: 't1',
          from: 'Apollo <hello@apollo.io>',
          subject: 'Touch 3',
          messageId: '<m4@apollo.io>',
          body: 'Third touch',
          internalDate: Date.parse('2024-05-12T12:00:00Z'),
        }),
        // Same Message-ID as g1 but different gmail id → duplicate
        fakeMessage({
          id: 'g1-dup',
          threadId: 't1',
          from: 'Apollo <hello@apollo.io>',
          subject: 'Touch 1 dup',
          messageId: '<m1@apollo.io>',
          body: 'dup',
          internalDate: Date.parse('2024-05-04T12:00:00Z'),
        }),
      ],
      dryRun: false,
    });

    assert.equal(second.imported, 1);
    assert.equal(second.duplicates, 4); // g1,g2,g3 existing + message-id dup
    const count = await pool.query('SELECT COUNT(*)::int AS n FROM market_emails');
    assert.equal(count.rows[0].n, 4);

    const timeline2 = await getCompanyTimeline(apollo.rows[0].id, { pool });
    assert.equal(timeline2.length, 3);
    assert.equal(timeline2[2].subject, 'Touch 3');
  });

  it('dry-run does not write rows', async () => {
    const before = await pool.query('SELECT COUNT(*)::int AS n FROM market_emails');
    const result = await importMarketIntelligence({
      pool,
      dryRun: true,
      messages: [
        fakeMessage({
          id: 'dry-1',
          threadId: 'td',
          from: 'HubSpot <mail@hubspot.com>',
          subject: 'Dry',
          messageId: '<dry@hubspot.com>',
          body: 'dry run',
          internalDate: Date.parse('2024-06-01T12:00:00Z'),
        }),
      ],
    });
    assert.equal(result.imported, 1);
    assert.equal(result.dryRun, true);
    assert.equal(result.importIntent, 'general_market_messaging');
    const after = await pool.query('SELECT COUNT(*)::int AS n FROM market_emails');
    assert.equal(after.rows[0].n, before.rows[0].n);
  });

  it('stores competitive_watch intent without changing schema philosophy', async () => {
    const result = await importMarketIntelligence({
      pool,
      importIntent: 'competitive_watch',
      messages: [
        fakeMessage({
          id: 'comp-1',
          threadId: 'tc',
          from: 'Rival <hello@rival.example>',
          subject: 'Watch',
          messageId: '<comp@rival.example>',
          body: 'competitive watch body',
          internalDate: Date.parse('2024-07-01T12:00:00Z'),
        }),
      ],
    });
    assert.equal(result.ok, true);
    assert.equal(result.importIntent, 'competitive_watch');
    const row = await pool.query(
      `SELECT import_intent FROM market_emails WHERE gmail_id = 'comp-1'`
    );
    assert.equal(row.rows[0].import_intent, 'competitive_watch');
    const sync = await pool.query(
      `SELECT import_intent FROM market_intel_sync_state WHERE id = 'default'`
    );
    assert.equal(sync.rows[0].import_intent, 'competitive_watch');
  });
});