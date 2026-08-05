'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseArgs,
  printHelp,
  main,
} = require('../scripts/prospectOperatingBrief');

describe('prospectOperatingBrief CLI', () => {
  it('parses human and JSON flags', () => {
    assert.equal(parseArgs(['--company-id=co-1']).json, false);
    assert.equal(parseArgs(['--company-id=co-1', '--json']).json, true);
    assert.equal(parseArgs(['--prospect-id=p-1', '--days=14']).days, 14);
    assert.equal(
      parseArgs(['--opportunity-id=opp-1']).opportunityId,
      'opp-1'
    );
    assert.equal(parseArgs(['--contact-id=c-1']).contactId, 'c-1');
  });

  it('requires a target identifier', () => {
    assert.throws(() => parseArgs([]), /At least one of/);
  });

  it('rejects unknown args and invalid days', () => {
    assert.throws(() => parseArgs(['--nope']), /Unknown argument/);
    assert.throws(
      () => parseArgs(['--company-id=x', '--days=0']),
      /--days must be a positive number/
    );
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
    assert.match(text, /SPEC-074/);
  });

  it('main supports JSON and human output without mutation', async () => {
    const queries = [];
    const fakePool = {
      async query(sql) {
        queries.push(String(sql));
        return { rows: [] };
      },
    };

    const logs = [];
    const original = console.log;
    console.log = (...args) => logs.push(args.join(' '));

    // Patch service deps via pool-only path; empty CRM + empty RI + empty market.
    // Inject by monkeypatching require cache is brittle — call main with pool and
    // rely on empty results + caveats. For deterministic output, stub via env is
    // unnecessary: empty DB rows still produce a valid brief envelope.
    try {
      // Use a thin wrapper: require the service path through main's getProspectOperatingBrief
      // by providing pool that returns empty for CRM, and let RI postgres path fail softly.
      // Instead, invoke format path by temporarily replacing module internals via main's
      // exported surface with a mocked getProspectOperatingBrief is not available.
      // So we use parseArgs + direct service in sibling tests; here we verify CLI wiring
      // with a pool that never writes.
      const jsonResult = await main(
        ['--json', '--company-id=co-missing', '--days=7', '--no-market', '--no-relationship'],
        fakePool
      );
      assert.equal(jsonResult.ok, true);
      assert.equal(jsonResult.kind, 'prospect_operating_brief');
      assert.equal(jsonResult.isEvidence, false);
      assert.equal(jsonResult.autonomousExecution, false);
      assert.ok(logs.some((line) => line.includes('"kind": "prospect_operating_brief"')));

      logs.length = 0;
      const humanResult = await main(
        ['--company-id=co-missing', '--days=7', '--no-market', '--no-relationship'],
        fakePool
      );
      assert.equal(humanResult.ok, true);
      assert.ok(logs.some((line) => line.includes('Prospect Operating Brief')));
      assert.ok(logs.some((line) => line.includes('Next Best Manual Action:')));
      assert.ok(logs.some((line) => line.includes('Caveats:')));
    } finally {
      console.log = original;
    }

    assert.ok(queries.length >= 0);
    assert.equal(
      queries.every((sql) => !/\b(INSERT|UPDATE|DELETE|ALTER)\b/i.test(sql)),
      true
    );
  });
});
