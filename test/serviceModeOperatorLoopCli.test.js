'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseArgs,
  printHelp,
  main,
} = require('../scripts/serviceModeOperatorLoop');

describe('serviceModeOperatorLoop CLI', () => {
  it('parses human and JSON flags', () => {
    assert.equal(parseArgs([]).json, false);
    assert.equal(parseArgs(['--json']).json, true);
    assert.equal(parseArgs(['--days=7']).days, 7);
    assert.equal(parseArgs(['--limit=5']).limit, 5);
    assert.equal(
      parseArgs(['--relationship-interaction-id=ri-1']).relationshipInteractionId,
      'ri-1'
    );
    assert.equal(parseArgs(['--company-id=co-1']).companyId, 'co-1');
    assert.equal(parseArgs(['--prospect-id=p-1']).prospectId, 'p-1');
    assert.equal(parseArgs(['--opportunity-id=o-1']).opportunityId, 'o-1');
  });

  it('rejects unknown args and invalid days/limit', () => {
    assert.throws(() => parseArgs(['--nope']), /Unknown argument/);
    assert.throws(() => parseArgs(['--days=0']), /--days must be a positive number/);
    assert.throws(() => parseArgs(['--limit=0']), /--limit must be a positive number/);
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
    assert.match(text, /SPEC-075/);
    assert.match(text, /--relationship-interaction-id/);
  });

  it('main supports JSON and human output without mutation', async () => {
    const queries = [];
    const fakePool = {
      async query(sql) {
        queries.push(String(sql));
        return { rows: [] };
      },
    };

    const fakeLoop = {
      ok: true,
      kind: 'service_mode_operator_loop',
      isEvidence: false,
      generatedAt: '2026-08-04T12:00:00.000Z',
      window: { days: 14, since: 'x', until: 'y' },
      summary: {
        candidatesScanned: 0,
        actionsReturned: 0,
        highPriorityCount: 0,
        caveatCount: 1,
      },
      actions: [],
      caveats: ['no_operator_candidates'],
      autonomousExecution: false,
    };

    const logs = [];
    const original = console.log;
    console.log = (...args) => logs.push(args.join(' '));

    try {
      const jsonResult = await main(['--json', '--days=7', '--no-market'], fakePool, {
        getServiceModeOperatorLoop: async () => fakeLoop,
      });
      assert.equal(jsonResult.ok, true);
      assert.equal(jsonResult.kind, 'service_mode_operator_loop');
      assert.ok(logs.some((line) => line.includes('"kind": "service_mode_operator_loop"')));

      logs.length = 0;
      const humanResult = await main(['--days=7', '--no-market'], fakePool, {
        getServiceModeOperatorLoop: async () => fakeLoop,
      });
      assert.equal(humanResult.ok, true);
      assert.ok(logs.some((line) => line.includes('Service Mode Operator Loop')));
      assert.ok(logs.some((line) => line.includes('No autonomous execution performed.')));
    } finally {
      console.log = original;
    }

    assert.equal(
      queries.every((sql) => !/\b(INSERT|UPDATE|DELETE|ALTER)\b/i.test(sql)),
      true
    );
  });
});
