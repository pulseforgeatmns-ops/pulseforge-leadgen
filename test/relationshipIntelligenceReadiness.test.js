'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  REQUIRED_TABLES,
  emptyMetrics,
  emptyConstraintValidation,
  parseCheckInValues,
  compareEnumSets,
  deriveReadinessStatus,
  runRelationshipIntelligenceAcceptance,
  buildRelationshipIntelReadinessReport,
  formatReadinessReport,
  createMemoryStore,
  ACCEPTANCE_SOURCE,
  loadConstraintValidation,
} = require('../services/relationshipIntelligenceReadiness');

const interview = require('../services/relationshipIntelligenceInterview');
const { parseArgs } = require('../scripts/relationshipIntelReadiness');

function allTablesPresent() {
  return {
    tables: Object.fromEntries(REQUIRED_TABLES.map((t) => [t, true])),
    missing: [],
    allPresent: true,
  };
}

function validConstraints() {
  return {
    interactionTypes: compareEnumSets(
      interview.INTERACTION_TYPES,
      interview.INTERACTION_TYPES.slice()
    ),
    insightKinds: compareEnumSets(interview.INSIGHT_KINDS, interview.INSIGHT_KINDS.slice()),
    ok: true,
  };
}

describe('relationshipIntelligenceReadiness helpers', () => {
  it('parses CHECK IN constraint values', () => {
    const values = parseCheckInValues(
      "CHECK (interaction_type IN ('cold_call', 'discovery_call', 'other'))"
    );
    assert.deepEqual(values, ['cold_call', 'discovery_call', 'other']);
  });

  it('parses PostgreSQL ANY ARRAY rewritten CHECK constraints', () => {
    const types = parseCheckInValues(
      "CHECK ((interaction_type = ANY (ARRAY['cold_call'::text, 'discovery_call'::text, 'walkthrough'::text, 'estimate'::text, 'meeting'::text, 'demo'::text, 'proposal_review'::text, 'follow_up'::text, 'other'::text])))"
    );
    assert.deepEqual(types, [
      'cold_call',
      'demo',
      'discovery_call',
      'estimate',
      'follow_up',
      'meeting',
      'other',
      'proposal_review',
      'walkthrough',
    ]);

    const kinds = parseCheckInValues(
      "CHECK ((kind = ANY ((ARRAY['pain'::text, 'goal'::text, 'context'::text]))))"
    );
    assert.deepEqual(kinds, ['context', 'goal', 'pain']);
  });

  it('validates constraints from pg-style ANY ARRAY definitions via fake pool', async () => {
    const fakePool = {
      async query(sql, params) {
        if (/to_regclass/i.test(sql) && params) {
          return { rows: [{ name: String(params[0]) }] };
        }
        if (/pg_constraint/i.test(sql)) {
          return {
            rows: [
              {
                table_name: 'relationship_interactions',
                conname: 'relationship_interactions_interaction_type_check',
                definition:
                  "CHECK ((interaction_type = ANY (ARRAY['cold_call'::text, 'discovery_call'::text, 'walkthrough'::text, 'estimate'::text, 'meeting'::text, 'demo'::text, 'proposal_review'::text, 'follow_up'::text, 'other'::text])))",
              },
              {
                table_name: 'relationship_interactions',
                conname: 'relationship_interactions_status_check',
                definition:
                  "CHECK ((status = ANY (ARRAY['draft'::text, 'reviewed'::text, 'committed'::text])))",
              },
              {
                table_name: 'relationship_interaction_insights',
                conname: 'relationship_interaction_insights_kind_check',
                definition:
                  "CHECK ((kind = ANY (ARRAY['pain'::text, 'goal'::text, 'objection'::text, 'timeline'::text, 'budget'::text, 'decision_maker'::text, 'stakeholder'::text, 'competitor'::text, 'next_step'::text, 'commitment'::text, 'risk'::text, 'buying_signal'::text, 'open_question'::text, 'preference'::text, 'context'::text])))",
              },
            ],
          };
        }
        throw new Error(`unexpected sql: ${sql}`);
      },
    };

    const validation = await loadConstraintValidation(fakePool);
    assert.equal(validation.ok, true);
    assert.equal(validation.interactionTypes.ok, true);
    assert.equal(validation.insightKinds.ok, true);
    assert.equal(validation.interactionTypes.missing.length, 0);
    assert.equal(validation.insightKinds.missing.length, 0);
  });

  it('compareEnumSets reports missing and unexpected', () => {
    const result = compareEnumSets(['a', 'b'], ['b', 'c']);
    assert.equal(result.ok, false);
    assert.deepEqual(result.missing, ['a']);
    assert.deepEqual(result.unexpected, ['c']);
  });

  it('marks missing tables as blocked', () => {
    const derived = deriveReadinessStatus({
      tableReadiness: {
        tables: { relationship_interactions: false },
        missing: ['relationship_interactions'],
        allPresent: false,
      },
      constraintValidation: emptyConstraintValidation(),
      metrics: emptyMetrics(),
    });
    assert.equal(derived.status, 'blocked');
    assert.ok(derived.blockers.some((b) => b.includes('missing_table:relationship_interactions')));
    assert.ok(derived.nextActions.some((a) => a.includes('migrations/')));
  });

  it('marks constraint mismatch as blocked', () => {
    const cv = emptyConstraintValidation();
    cv.interactionTypes.ok = false;
    cv.interactionTypes.missing = ['walkthrough'];
    cv.ok = false;
    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      constraintValidation: cv,
      metrics: emptyMetrics(),
    });
    assert.equal(derived.status, 'blocked');
    assert.ok(derived.blockers.some((b) => b === 'interaction_types_constraint_mismatch'));
  });

  it('marks no committed ready interaction as partial', () => {
    const metrics = emptyMetrics();
    metrics.totalInteractions = 2;
    metrics.draftCount = 2;
    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      constraintValidation: validConstraints(),
      metrics,
    });
    assert.equal(derived.status, 'partial');
    assert.ok(
      derived.blockers.some((b) => b === 'no_committed_interaction_with_summary_and_insights')
    );
  });

  it('marks ready when a committed summary+insights interaction exists', () => {
    const metrics = emptyMetrics();
    metrics.totalInteractions = 1;
    metrics.committedCount = 1;
    metrics.insightsCount = 4;
    metrics.readyCommittedCount = 1;
    metrics.commitFlowExercised = true;
    metrics.latestInteractionAt = '2026-08-04T00:00:00.000Z';
    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      constraintValidation: validConstraints(),
      metrics,
    });
    assert.equal(derived.status, 'ready');
  });

  it('blocks when CRM mutation is detected', () => {
    const metrics = emptyMetrics();
    metrics.committedCount = 1;
    metrics.readyCommittedCount = 1;
    metrics.commitFlowExercised = true;
    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      constraintValidation: validConstraints(),
      metrics,
      crmMutation: { mutated: true, mutatedTables: ['opportunities'] },
    });
    assert.equal(derived.status, 'blocked');
    assert.ok(derived.blockers.some((b) => b.includes('crm_mutation_detected')));
  });
});

describe('relationshipIntelligenceAcceptance', () => {
  it('creates notes fixture, summarizes, commits, and stays off CRM tables', async () => {
    const store = createMemoryStore();
    const result = await runRelationshipIntelligenceAcceptance({
      store,
      skipCrmSnapshot: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.committedStatus, 'committed');
    assert.equal(result.queryable, true);
    assert.ok(result.insightCount >= 1);
    assert.equal(result.crmMutation.mutated, false);
    assert.deepEqual(
      [...new Set(result.sqlTables)].sort(),
      ['relationship_interaction_insights', 'relationship_interactions']
    );

    const tables = new Set(store.sqlLog.map((e) => e.table));
    assert.equal(tables.has('opportunities'), false);
    assert.equal(tables.has('prospects'), false);
    assert.equal(tables.has('companies'), false);
  });
});

describe('relationshipIntelligenceReadiness report builder', () => {
  it('returns blocked report when pool cannot see tables', async () => {
    const fakePool = {
      async query(sql) {
        if (/to_regclass/i.test(sql)) {
          return { rows: [{ name: null }] };
        }
        throw new Error('unexpected');
      },
    };
    const report = await buildRelationshipIntelReadinessReport({ pool: fakePool });
    assert.equal(report.kind, 'relationship_intelligence_readiness');
    assert.equal(report.status, 'blocked');
    assert.equal(report.tableReadiness.allPresent, false);
    assert.ok(report.crmGuardOk);
  });

  it('formats a human-readable report', () => {
    const text = formatReadinessReport({
      status: 'partial',
      generatedAt: '2026-08-04T00:00:00.000Z',
      tableReadiness: allTablesPresent(),
      constraintValidation: validConstraints(),
      metrics: emptyMetrics(),
      crmGuardOk: true,
      crmMutation: { detectable: false, mutated: false, mutatedTables: [] },
      blockers: ['no_committed_interaction_with_summary_and_insights'],
      nextActions: ['Run --accept'],
    });
    assert.match(text, /Relationship Intelligence Readiness/);
    assert.match(text, /Status: partial/);
    assert.match(text, /relationship_interactions/);
  });
});

describe('relationshipIntelReadiness CLI args', () => {
  it('parses flags', () => {
    const options = parseArgs(['--json', '--check', '--accept', '--company-id=co-1']);
    assert.equal(options.json, true);
    assert.equal(options.check, true);
    assert.equal(options.accept, true);
    assert.equal(options.companyId, 'co-1');
  });
});

describe('relationshipIntelligence readiness wiring', () => {
  it('registers npm script and readiness route', () => {
    const pkg = JSON.parse(
      fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8')
    );
    assert.equal(
      pkg.scripts['relationship:intel:readiness'],
      'node scripts/relationshipIntelReadiness.js'
    );

    const routeSource = fs.readFileSync(
      path.join(__dirname, '..', 'routes', 'relationshipIntelligence.js'),
      'utf8'
    );
    assert.match(routeSource, /\/api\/v1\/relationship-intel\/readiness/);
    assert.match(routeSource, /buildRelationshipIntelReadinessReport/);
  });

  it('acceptance source constant is stable', () => {
    assert.equal(ACCEPTANCE_SOURCE, 'readiness_acceptance');
    assert.ok(interview.INTERACTION_TYPES.includes('discovery_call'));
  });
});
