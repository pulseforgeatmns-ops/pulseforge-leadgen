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
    assert.equal(
      parseArgs(['--relationship-interaction-id=ri-1']).relationshipInteractionId,
      'ri-1'
    );
  });

  it('requires a target identifier', () => {
    assert.throws(() => parseArgs([]), /At least one of/);
  });

  it('accepts relationship-interaction-id as sole target', () => {
    const options = parseArgs(['--relationship-interaction-id=abc-123']);
    assert.equal(options.relationshipInteractionId, 'abc-123');
    assert.equal(options.companyId, null);
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

    const logs = [];
    const original = console.log;
    console.log = (...args) => logs.push(args.join(' '));

    try {
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

  it('CLI relationship-interaction-id path applies AS Cleaning raw_summary fallback', async () => {
    const AS_CLEANING_RAW =
      'Aji is the owner of AS Cleaning Co. The company is less than 6 months old, focused on commercial cleaning clients, and currently doing about ,500 in monthly recurring revenue. Aji expressed interest after the discovery call, asked for more information, and received a personalized 2-page overview for AS Cleaning Co. Need to follow up to confirm interest, clarify target client type, budget/timeline, decision process, and whether they want help generating commercial cleaning leads.';
    const LIVE_ID = '7b304188-cfc7-48e1-a21f-ee9d266e1879';

    assert.equal(
      parseArgs([`--relationship-interaction-id=${LIVE_ID}`]).relationshipInteractionId,
      LIVE_ID
    );

    const {
      createMemoryStore,
      startRelationshipInterview,
      summarizeRelationshipInterview,
      commitRelationshipInterview,
    } = require('../services/relationshipIntelligenceInterview');

    const store = createMemoryStore();
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-as-cleaning',
        contactId: 'aji',
        clientId: 1,
        notes: AS_CLEANING_RAW,
      },
      { store }
    );
    await summarizeRelationshipInterview(started.interviewId, { store });
    await commitRelationshipInterview(started.interviewId, { store });

    // Legacy thin insights only — forces raw_summary fallback on the brief path.
    const legacyInsights = (await store.listInsights(started.interviewId))
      .filter((i) => ['decision_maker', 'context'].includes(i.kind))
      .map((i) => ({
        kind: i.kind,
        label: i.label,
        value: i.value,
        confidence: i.confidence,
        sourceQuote: i.source_quote,
      }));
    await store.replaceInsights(started.interviewId, legacyInsights);

    const logs = [];
    const original = console.log;
    console.log = (...args) => logs.push(args.join(' '));
    try {
      // Exact operator command shape (id may be any committed interaction):
      // npm run prospect:brief -- --relationship-interaction-id=<id>
      const brief = await main(
        [`--relationship-interaction-id=${started.interviewId}`, '--no-market'],
        { async query() { return { rows: [] }; } },
        {
          store,
          loadCompanySnapshot: async () => ({
            found: true,
            companyId: 'co-as-cleaning',
            companyName: 'AS Cleaning Co.',
            contactName: 'Aji',
            contactId: 'aji',
            prospectId: 'aji',
            doNotContact: false,
          }),
        }
      );

      assert.equal(brief.ok, true);
      assert.equal(brief.target.relationshipInteractionId, started.interviewId);
      assert.ok(brief.sections.buyingSignals.length >= 1, 'expected buying signals');
      assert.ok(
        brief.sections.commitmentsAndNextSteps.length >= 1,
        'expected commitments/next steps'
      );
      assert.equal(
        brief.sections.relationshipSummary.rawSummaryFallbackApplied,
        true
      );
      assert.ok(
        brief.caveats.some((c) => c.includes('relationship_raw_summary_fallback'))
      );
      const text = logs.join('\n');
      assert.match(text, /Prospect Operating Brief/);
      assert.match(text, /Buying Signals:/);
      assert.match(text, /Commitments \/ Next Steps:/);
      assert.match(text, /expressed interest|asked for more information/i);
      assert.match(text, /overview|follow up|follow-up/i);
      assert.match(text, /relationship_raw_summary_fallback/);
      assert.equal(/\nBuying Signals:\n\(none\)/.test(text), false);
      assert.equal(/\nCommitments \/ Next Steps:\n\(none\)/.test(text), false);
    } finally {
      console.log = original;
    }
  });
});
