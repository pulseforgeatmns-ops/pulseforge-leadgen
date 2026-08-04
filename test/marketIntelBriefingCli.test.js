'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseArgs,
  printHelp,
  main,
} = require('../scripts/marketIntelBriefing');

describe('marketIntelBriefing CLI', () => {
  it('parses human and JSON flags', () => {
    assert.deepEqual(parseArgs([]).json, false);
    assert.equal(parseArgs(['--json']).json, true);
    assert.equal(parseArgs(['--days=14', '--limit=5']).days, 14);
    assert.equal(parseArgs(['--days=14', '--limit=5']).limit, 5);
    assert.equal(
      parseArgs(['--intent=general_market_messaging']).intent,
      'general_market_messaging'
    );
    assert.equal(
      parseArgs(['--company-id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa']).companyId,
      'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    );
  });

  it('rejects unknown args and invalid numbers', () => {
    assert.throws(() => parseArgs(['--nope']), /Unknown argument/);
    assert.throws(() => parseArgs(['--days=0']), /--days must be a positive number/);
    assert.throws(() => parseArgs(['--limit=-1']), /--limit must be a positive number/);
  });

  it('printHelp mentions human and JSON output', () => {
    const lines = [];
    const original = console.log;
    console.log = (...args) => lines.push(args.join(' '));
    try {
      printHelp();
    } finally {
      console.log = original;
    }
    const text = lines.join('\n');
    assert.match(text, /--json/);
    assert.match(text, /human-readable/);
    assert.match(text, /isEvidence=false/);
  });

  it('main supports JSON and human output without mutation', async () => {
    const queries = [];
    const fakePool = {
      async query(sql) {
        queries.push(String(sql));
        if (String(sql).includes('to_regclass')) {
          return { rows: [{ name: 'public.stub' }] };
        }
        return { rows: [] };
      },
    };

    const logs = [];
    const original = console.log;
    console.log = (...args) => logs.push(args.join(' '));
    try {
      const jsonResult = await main(['--json', '--days=7', '--limit=3'], fakePool);
      assert.equal(jsonResult.ok, true);
      assert.equal(jsonResult.kind, 'market_intelligence_briefing');
      assert.equal(jsonResult.isEvidence, false);
      assert.ok(logs.some((line) => line.includes('"kind": "market_intelligence_briefing"')));

      logs.length = 0;
      const humanResult = await main(['--days=7'], fakePool);
      assert.equal(humanResult.ok, true);
      assert.ok(logs.some((line) => line.includes('Market Intelligence Briefing')));
      assert.ok(logs.some((line) => line.includes('Caveats:')));
    } finally {
      console.log = original;
    }

    assert.ok(queries.length > 0);
    assert.equal(
      queries.every((sql) => !/\b(INSERT|UPDATE|DELETE|ALTER)\b/i.test(sql)),
      true
    );
  });
});
