'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createEqlEngine,
  createEvidenceCatalog,
  catalogFromResult,
  parseEql,
  planEql,
  EqlParseError,
  EQL_RULES,
  STATEMENT_KINDS,
} = require('..');

function sampleCatalog() {
  return createEvidenceCatalog({
    subjects: [
      { id: 'BTC', subject: 'BTC' },
      { id: 'Company123', subject: 'Company123', companyId: 'Company123' },
    ],
    claims: [
      {
        id: 'momentum_continuation',
        claimType: 'momentum_continuation',
        subject: 'BTC',
        confidence: 0.91,
        statement: 'Momentum continues',
      },
      {
        id: 'momentum_exhaustion',
        claimType: 'momentum_exhaustion',
        subject: 'BTC',
        confidence: 0.4,
        statement: 'Momentum exhausting',
      },
      {
        id: 'expansion_ready',
        claimType: 'expansion_ready',
        subject: 'Company123',
        companyId: 'Company123',
        confidence: 0.82,
        statement: 'Company ready to expand',
      },
      {
        id: 'low_fit',
        subject: 'Company123',
        companyId: 'Company123',
        confidence: 0.3,
        statement: 'Low fit',
      },
    ],
    evidence: [
      {
        id: 'ev-support-1',
        claimId: 'momentum_continuation',
        role: 'supporting',
        observationId: 'obs-1',
        confidence: 0.8,
      },
      {
        id: 'ev-contra-1',
        claimId: 'momentum_continuation',
        role: 'contradicting',
        observationId: 'obs-2',
        confidence: 0.2,
      },
      {
        id: 'ev-crm-1',
        claimId: 'expansion_ready',
        role: 'supporting',
        observationId: 'touch-9',
      },
    ],
    observations: [
      {
        id: 'obs-1',
        subjectId: 'BTC',
        observationType: 'price_tick',
        observedAt: '2026-07-26T09:45:00Z',
      },
      {
        id: 'obs-2',
        subjectId: 'BTC',
        observationType: 'news_event',
        observedAt: '2026-07-26T09:50:00Z',
      },
      {
        id: 'touch-9',
        subjectId: 'Company123',
        observationType: 'meeting',
        observedAt: '2026-07-26T09:40:00Z',
      },
    ],
    recommendations: [
      {
        id: 'rec-1',
        subjectId: 'BTC',
        recommendedAction: 'observe',
        confidence: 0.7,
      },
      {
        id: 'rec-2',
        subjectId: 'Company123',
        recommendedAction: 'call',
        confidence: 0.85,
      },
    ],
    outcomes: [
      { id: 'out-1', subjectId: 'Company123', status: 'successful' },
    ],
    replay_sessions: [
      {
        id: 'session-btc',
        subjectId: 'BTC',
        startTime: '2026-07-26T09:30:00Z',
        endTime: '2026-07-26T10:00:00Z',
      },
      {
        id: 'session-crm',
        subjectId: 'Company123',
        startTime: '2026-07-26T08:00:00Z',
        endTime: '2026-07-26T09:00:00Z',
      },
    ],
    links: {
      supporting: {
        momentum_continuation: [
          { id: 'ev-support-1', claimId: 'momentum_continuation', role: 'supporting' },
        ],
      },
      contradicting: {
        momentum_continuation: [
          { id: 'ev-contra-1', claimId: 'momentum_continuation', role: 'contradicting' },
        ],
      },
    },
  });
}

describe('SPEC-020 EQL rules', () => {
  it('exports guiding rules and statement kinds', () => {
    assert.ok(EQL_RULES.DOMAIN_NEUTRAL);
    assert.ok(EQL_RULES.NO_MUTATION);
    assert.ok(EQL_RULES.NO_RUNTIME_BRANCHING);
    assert.deepEqual(
      [...STATEMENT_KINDS].sort(),
      ['COMPARE', 'EXPLAIN', 'FIND', 'REPLAY', 'SHOW'].sort()
    );
  });
});

describe('SPEC-020 Parser', () => {
  it('parses FIND with WHERE, ORDER BY, LIMIT', () => {
    const ast = parseEql(`
      FIND Claims
      WHERE subject = "BTC"
      AND confidence > 0.75
      ORDER BY confidence DESC
      LIMIT 10
    `);
    assert.equal(ast.kind, 'FIND');
    assert.equal(ast.target, 'claims');
    assert.equal(ast.where.length, 2);
    assert.equal(ast.orderBy.field, 'confidence');
    assert.equal(ast.orderBy.direction, 'DESC');
    assert.equal(ast.limit, 10);
  });

  it('parses SHOW Evidence SUPPORTING Claim', () => {
    const ast = parseEql(`
      SHOW Evidence
      SUPPORTING Claim("momentum_continuation")
    `);
    assert.equal(ast.kind, 'SHOW');
    assert.equal(ast.target, 'evidence');
    assert.equal(ast.relation, 'SUPPORTING');
    assert.equal(ast.related.id, 'momentum_continuation');
  });

  it('parses REPLAY window', () => {
    const ast = parseEql(`
      REPLAY
      FROM "2026-07-26T09:30:00Z"
      TO "2026-07-26T10:00:00Z"
    `);
    assert.equal(ast.kind, 'REPLAY');
    assert.equal(ast.from, '2026-07-26T09:30:00Z');
    assert.equal(ast.to, '2026-07-26T10:00:00Z');
  });

  it('parses FIND entity + trailing EXPLAIN', () => {
    const ast = parseEql(`
      FIND Claim("momentum_continuation")
      EXPLAIN
    `);
    assert.equal(ast.kind, 'FIND');
    assert.equal(ast.entity.id, 'momentum_continuation');
    assert.equal(ast.explain, true);
  });

  it('parses COMPARE', () => {
    const ast = parseEql(
      `COMPARE ReplaySession("session-btc") WITH ReplaySession("session-crm")`
    );
    assert.equal(ast.kind, 'COMPARE');
    assert.equal(ast.left.id, 'session-btc');
    assert.equal(ast.right.id, 'session-crm');
  });

  it('parses SHOW Calibration FOR Claim', () => {
    const ast = parseEql(`SHOW Calibration FOR Claim("momentum_continuation")`);
    assert.equal(ast.kind, 'SHOW');
    assert.equal(ast.target, 'calibrations');
    assert.equal(ast.relation, 'FOR');
    assert.equal(ast.related.target, 'claims');
    assert.equal(ast.related.id, 'momentum_continuation');
  });

  it('parses SHOW Accuracy FOR StrategyPack', () => {
    const ast = parseEql(`SHOW Accuracy FOR StrategyPack("market")`);
    assert.equal(ast.target, 'accuracies');
    assert.equal(ast.relation, 'FOR');
    assert.equal(ast.related.target, 'strategy_packs');
    assert.equal(ast.related.id, 'market');
  });

  it('rejects mutation keywords', () => {
    assert.throws(
      () => parseEql(`UPDATE Claims SET confidence = 1`),
      (err) => err instanceof EqlParseError && /read-only/i.test(err.message)
    );
    assert.throws(() => parseEql(`DELETE Claims`), EqlParseError);
    assert.throws(() => parseEql(`INSERT Claims`), EqlParseError);
  });
});

describe('SPEC-020 QueryPlanner', () => {
  it('builds deterministic steps for FIND', () => {
    const plan = planEql(
      parseEql(`FIND Claims WHERE confidence > 0.5 ORDER BY confidence DESC LIMIT 2`)
    );
    assert.equal(plan.kind, 'FIND');
    assert.deepEqual(
      plan.steps.map((s) => s.op),
      ['scan', 'filter', 'sort', 'limit', 'project']
    );
  });

  it('appends explain step when requested', () => {
    const plan = planEql(parseEql(`FIND Claim("momentum_continuation") EXPLAIN`));
    assert.ok(plan.steps.some((s) => s.op === 'explain'));
    assert.equal(plan.explain, true);
  });
});

describe('SPEC-020 Executor — domain neutrality', () => {
  it('runs the same FIND query for CRM and Market subjects (no branching)', async () => {
    const eql = createEqlEngine({ catalog: sampleCatalog() });

    const market = await eql.query(`
      FIND Claims
      WHERE subject = "BTC"
      AND confidence > 0.75
      ORDER BY confidence DESC
    `);
    const crm = await eql.query(`
      FIND Claims
      WHERE subject = "Company123"
      AND confidence > 0.75
      ORDER BY confidence DESC
    `);

    assert.equal(market.kind, 'FIND');
    assert.equal(crm.kind, 'FIND');
    assert.equal(market.mutatesProduction, false);
    assert.equal(crm.mutatesProduction, false);

    assert.equal(market.count, 1);
    assert.equal(market.rows[0].id, 'momentum_continuation');

    assert.equal(crm.count, 1);
    assert.equal(crm.rows[0].id, 'expansion_ready');

    // Same plan shape — domain-neutral execution path
    assert.deepEqual(
      market.plan.steps.map((s) => s.op),
      crm.plan.steps.map((s) => s.op)
    );
  });

  it('SHOW Evidence SUPPORTING Claim', async () => {
    const eql = createEqlEngine({ catalog: sampleCatalog() });
    const result = await eql.query(`
      SHOW Evidence
      SUPPORTING Claim("momentum_continuation")
    `);
    assert.equal(result.kind, 'SHOW');
    assert.ok(result.count >= 1);
    assert.ok(result.rows.every((r) => r.claimId === 'momentum_continuation'));
  });

  it('REPLAY returns sessions in window', async () => {
    const eql = createEqlEngine({ catalog: sampleCatalog() });
    const result = await eql.query(`
      REPLAY
      FROM "2026-07-26T09:30:00Z"
      TO "2026-07-26T10:00:00Z"
    `);
    assert.equal(result.kind, 'REPLAY');
    assert.ok(result.rows.some((r) => r.id === 'session-btc'));
  });

  it('EXPLAIN returns supporting / contradicting / confidence / trace surface', async () => {
    const eql = createEqlEngine({ catalog: sampleCatalog() });
    const result = await eql.query(`
      FIND Claim("momentum_continuation")
      EXPLAIN
    `);
    assert.equal(result.count, 1);
    assert.ok(result.explanation);
    assert.ok('supportingEvidence' in result.explanation);
    assert.ok('contradictingEvidence' in result.explanation);
    assert.ok('confidenceHistory' in result.explanation);
    assert.ok('reasoningTrace' in result.explanation);
  });

  it('COMPARE returns a side-by-side row', async () => {
    const eql = createEqlEngine({ catalog: sampleCatalog() });
    const result = await eql.query(
      `COMPARE ReplaySession("session-btc") WITH ReplaySession("session-crm")`
    );
    assert.equal(result.kind, 'COMPARE');
    assert.equal(result.count, 1);
    assert.equal(result.rows[0].leftId, 'session-btc');
    assert.equal(result.rows[0].rightId, 'session-crm');
    assert.equal(result.rows[0].equal, false);
  });

  it('SHOW Calibration FOR Claim and Accuracy FOR StrategyPack (SPEC-021)', async () => {
    const catalog = createEvidenceCatalog({
      claims: [
        {
          id: 'momentum_continuation',
          claimType: 'momentum_continuation',
          confidence: 0.82,
          accuracy: 0.7165,
          historicalCalibration: 0.7165,
        },
      ],
      calibrations: [
        {
          id: 'cal-1',
          claimId: 'momentum_continuation',
          confidence: 0.82,
          historicalCalibration: 0.67,
          adjustedConfidence: 0.745,
        },
      ],
      accuracies: [
        {
          id: 'acc-market',
          scope: 'strategy_pack',
          scopeId: 'market',
          strategyPack: 'market',
          accuracy: 0.7165,
          precision: 0.7165,
          recall: null,
          occurrences: 127,
        },
      ],
      strategy_packs: [
        { id: 'market', strategyPack: 'market', accuracy: 0.7165, occurrences: 127 },
      ],
    });
    const eql = createEqlEngine({ catalog });

    const cal = await eql.query(
      `SHOW Calibration FOR Claim("momentum_continuation")`
    );
    assert.equal(cal.kind, 'SHOW');
    assert.ok(cal.count >= 1);
    assert.equal(cal.rows[0].claimId, 'momentum_continuation');

    const acc = await eql.query(`SHOW Accuracy FOR StrategyPack("market")`);
    assert.equal(acc.kind, 'SHOW');
    assert.ok(acc.count >= 1);
    assert.equal(acc.rows[0].strategyPack || acc.rows[0].scopeId, 'market');
  });

  it('resolves companyId as subject without CRM-specific branching', async () => {
    const catalog = createEvidenceCatalog({
      claims: [
        { id: 'a', companyId: 'Company123', confidence: 0.9 },
        { id: 'b', subjectId: 'BTC', confidence: 0.9 },
      ],
    });
    const eql = createEqlEngine({ catalog });
    const crm = await eql.query(`FIND Claims WHERE subject = "Company123"`);
    const market = await eql.query(`FIND Claims WHERE subject = "BTC"`);
    assert.equal(crm.rows[0].id, 'a');
    assert.equal(market.rows[0].id, 'b');
  });
});

describe('SPEC-020 catalogFromResult', () => {
  it('projects replay results into a queryable catalog', async () => {
    const catalog = catalogFromResult({
      subjectId: 'BTC',
      startTime: '2026-07-26T09:30:00Z',
      endTime: '2026-07-26T10:00:00Z',
      confidence: 0.88,
      claims: {
        results: [
          { claimType: 'momentum_continuation', confidence: 0.9, subjectId: 'BTC' },
        ],
      },
      observations: [{ id: 'o1', subjectId: 'BTC', observedAt: '2026-07-26T09:45:00Z' }],
      recommendations: [{ id: 'r1', recommendedAction: 'observe' }],
      explanation: {
        supportingEvidence: [{ id: 'e1', claimId: 'momentum_continuation' }],
      },
      reasoningTrace: { steps: 3 },
      steps: [],
    });

    const eql = createEqlEngine({ catalog });
    const result = await eql.query(`
      FIND Claims
      WHERE subject = "BTC"
      AND confidence > 0.75
    `);
    assert.equal(result.count, 1);
    assert.equal(result.rows[0].claimType, 'momentum_continuation');
  });
});
