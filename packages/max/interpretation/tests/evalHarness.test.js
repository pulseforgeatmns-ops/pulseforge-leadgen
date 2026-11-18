'use strict';

/**
 * P0-MAX-REASONING-001 Workstream A: the offline evaluation harness itself.
 * Every model call here goes to an injected fake client. No network, no paid
 * call, no database.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { evaluatePromotionGates, CRITICAL_GROUPS } = require('../eval/promotionGates');
const { createEvalSpendCap } = require('../eval/evalSpendCap');
const { runModelEval, planModelRun, evaluationMetadata } = require('../eval/runInterpretationEval');
const { CASES: HELDOUT } = require('../eval/heldoutInterpretation');
const { CASES: EXTENDED } = require('../eval/extendedInterpretation');

const ROOT = path.resolve(__dirname, '../../../..');
const PRICED_MODEL = 'claude-sonnet-5-5';
const CASES = HELDOUT.slice(0, 4);

const QUESTION_FRAME = {
  intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
  evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
};

function fakeClient(respond) {
  const calls = [];
  return {
    calls,
    messages: {
      create: async (params, options) => {
        calls.push({ params, options });
        return respond(calls.length, params);
      },
    },
  };
}

const ok = (frame = QUESTION_FRAME) => ({
  stop_reason: 'end_turn',
  model: PRICED_MODEL,
  content: [{ type: 'text', text: JSON.stringify(frame) }],
  usage: { input_tokens: 1200, output_tokens: 120 },
});

function grade(id, category, pass, safety = true) {
  return { id, category, pass, dims: { safety } };
}

describe('promotion gates', () => {
  it('passes measured gates only on the §10 thresholds and never authorizes promotion', () => {
    const grades = [];
    for (const [group, cats] of Object.entries(CRITICAL_GROUPS)) {
      for (let i = 0; i < 10; i += 1) grades.push(grade(`${group}${i}`, cats[0], true));
    }
    const report = evaluatePromotionGates({
      grades, latencyMs: { p95: 900 }, calls: 100, degradedCalls: 1, tokens: { input: 1 }, estimatedUsd: 0.1,
    });
    assert.deepEqual(report.failing, []);
    assert.equal(report.promotionAuthorized, false);
    assert.ok(report.notMeasured.includes('independent_disagreement_review'));
    assert.ok(report.notMeasured.includes('observer_persistence_and_privacy_verified'));
  });

  it('fails on one safety violation, a slow p95, a high degraded rate, and a weak critical category', () => {
    const grades = [
      ...Array.from({ length: 9 }, (_, i) => grade(`n${i}`, 'negation', true)),
      grade('n9', 'negation', false, false),
      grade('x1', 'cross_tenant', false),
    ];
    const report = evaluatePromotionGates({ grades, latencyMs: { p95: 1501 }, calls: 100, degradedCalls: 3, tokens: {}, estimatedUsd: 0 });
    for (const id of ['authorization_safety_violations', 'interpreter_p95_latency_ms', 'degraded_or_invalid_rate_pct', 'critical_cross_tenant']) {
      assert.ok(report.failing.includes(id), id);
    }
    assert.equal(report.gates.find((g) => g.id === 'critical_negation').observed, 90);
  });

  it('reports an absent critical category as not measured, never as passing', () => {
    const report = evaluatePromotionGates({ grades: [grade('a', 'negation', true)] });
    const conditional = report.gates.find((g) => g.id === 'critical_conditional_authorization');
    assert.equal(conditional.status, 'not_measured');
    assert.equal(report.gates.find((g) => g.id === 'interpreter_p95_latency_ms').status, 'not_measured');
  });

  it('every corpus category named in a critical group exists in the extended corpus', () => {
    const categories = new Set([...HELDOUT, ...EXTENDED].map((c) => c.category));
    for (const cats of Object.values(CRITICAL_GROUPS)) for (const c of cats) assert.ok(categories.has(c), c);
  });
});

describe('in-process spend cap', () => {
  it('requires an explicit positive cap', () => {
    assert.throws(() => createEvalSpendCap({}), /eval_cost_cap_required/);
    assert.throws(() => createEvalSpendCap({ maxUsd: 0 }), /eval_cost_cap_required/);
  });

  it('refuses unpriced requests and becomes exhausted', async () => {
    const cap = createEvalSpendCap({ maxUsd: 1 });
    await assert.rejects(cap.reserve({ estimatedUsd: null }), /eval_unpriced_request/);
    assert.equal(cap.exhausted, true);
    await assert.rejects(cap.reserve({ estimatedUsd: 0.01 }), /eval_unpriced_request/);
  });

  it('counts open reservations against the cap and keeps the reservation for failed calls', async () => {
    const cap = createEvalSpendCap({ maxUsd: 0.1 });
    const a = await cap.reserve({ estimatedUsd: 0.04 });
    await cap.reserve({ estimatedUsd: 0.04 });
    await assert.rejects(cap.reserve({ estimatedUsd: 0.04 }), /eval_cost_cap_reached/);
    await cap.settle(a, { status: 'uncertain' });
    assert.equal(cap.snapshot().settledUsd, 0.04);
    assert.equal(cap.snapshot().uncertain, 1);
  });

  it('settles successful calls at the measured estimate', async () => {
    const cap = createEvalSpendCap({ maxUsd: 1 });
    const id = await cap.reserve({ estimatedUsd: 0.05 });
    await cap.settle(id, { status: 'ok', estimatedUsd: 0.002 });
    assert.equal(cap.snapshot().settledUsd, 0.002);
    assert.equal(cap.snapshot().openReservations, 0);
  });
});

describe('model evaluation runner (fake client)', () => {
  it('refuses without a model id, without a cap, or for an unpriced model, before any call', async () => {
    const client = fakeClient(() => ok());
    assert.equal((await runModelEval({ cases: CASES, client, maxUsd: 1 })).refused, 'eval_model_id_required');
    assert.equal((await runModelEval({ cases: CASES, client, model: PRICED_MODEL })).refused, 'eval_cost_cap_required');
    assert.equal((await runModelEval({ cases: CASES, client, model: 'claude-unpriced-test', maxUsd: 1 })).refused, 'eval_unpriced_model');
    assert.equal(client.calls.length, 0);
  });

  it('meters every attempt through the cap, never the shared database ledger', async () => {
    const pool = require(path.join(ROOT, 'db'));
    const original = { query: pool.query, connect: pool.connect };
    let dbTouches = 0;
    pool.query = async () => { dbTouches += 1; throw new Error('database access in offline eval'); };
    pool.connect = async () => { dbTouches += 1; throw new Error('database access in offline eval'); };
    try {
      const client = fakeClient(() => ok());
      const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, repeats: 2, maxUsd: 5 });
      assert.equal(result.complete, true);
      assert.equal(client.calls.length, CASES.length * 2);
      assert.equal(result.cost.attempts, CASES.length * 2);
      assert.equal(result.cost.openReservations, 0);
      assert.ok(result.cost.settledUsd > 0);
      assert.equal(result.tokens.calls, CASES.length * 2);
      assert.equal(result.summaries.length, 2);
      assert.equal(result.pooled.n, CASES.length * 2);
      assert.equal(result.gates.promotionAuthorized, false);
      for (const call of client.calls) assert.equal(call.options.maxRetries, 0);
      assert.equal(dbTouches, 0);
    } finally {
      Object.assign(pool, original);
    }
  });

  it('stops at the cap, reports the run incomplete, and never scores the refused call', async () => {
    const client = fakeClient(() => ok());
    const plan = planModelRun({ cases: CASES, model: PRICED_MODEL });
    // Room for roughly two reservations: settled actuals are much smaller, so
    // the run proceeds further, but must stop before exceeding the cap.
    const maxUsd = plan.maxPerCallReservationUsd * 1.5;
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, repeats: 50, maxUsd });
    assert.equal(result.complete, false);
    assert.equal(result.abortedReason, 'eval_cost_cap_reached');
    assert.equal(result.tokens.calls, client.calls.length, 'only calls that reached the provider are scored');
    assert.equal(result.pooled.n, client.calls.length);
    assert.ok(result.cost.committedUsd <= maxUsd);
  });

  it('accounts degraded calls and fails the degraded-rate gate', async () => {
    const client = fakeClient((n) => (n % 2 ? ok() : Promise.reject(Object.assign(new Error('overloaded'), { status: 529 }))));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, maxUsd: 5 });
    assert.equal(result.degraded.calls, 2);
    assert.deepEqual(result.degraded.byReason, { http_529: 2 });
    assert.equal(result.cost.uncertain, 2);
    assert.ok(result.gates.failing.includes('degraded_or_invalid_rate_pct'));
  });

  it('flags cases whose interpretation changes between repeats', async () => {
    const client = fakeClient((n) => ok(n <= CASES.length ? QUESTION_FRAME : { ...QUESTION_FRAME, intent: 'investigation' }));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, repeats: 2, maxUsd: 5 });
    assert.equal(result.stability.cases, CASES.length);
    assert.equal(result.stability.unstable.length, CASES.length);
    assert.deepEqual(result.stability.unstable[0].intents, ['question', 'investigation']);
  });

  it('collects failing cases with fixture text for independent review', async () => {
    const client = fakeClient(() => ok());
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, maxUsd: 5 });
    for (const d of result.disagreements) {
      assert.equal(typeof d.text, 'string');
      assert.ok(d.failedDimensions.length > 0);
    }
  });

  it('records reproducibility metadata', () => {
    const m = evaluationMetadata({ cases: CASES, corpus: 'test', cwd: ROOT });
    assert.match(m.corpusSha256, /^[0-9a-f]{64}$/);
    assert.match(m.producerPromptSha256, /^[0-9a-f]{64}$/);
    assert.equal(m.caseCount, CASES.length);
    assert.equal(m.node, process.version);
  });
});

describe('evaluation CLI', () => {
  // A preload that records any PostgreSQL connection attempt from the child.
  const preload = path.join(__dirname, 'fixtures', 'recordPgConnect.cjs');
  function cli(args, env = {}) {
    const childEnv = { ...process.env, DATABASE_URL: 'postgresql://must-not-be-used.invalid:5432/prod', ...env };
    delete childEnv.ANTHROPIC_API_KEY;
    return spawnSync(process.execPath, ['-r', preload, 'scripts/maxInterpretationEval.js', ...args], { cwd: ROOT, env: childEnv, encoding: 'utf8' });
  }

  it('emits parseable JSON with metadata and gates, and never connects to a database', () => {
    const r = cli(['--extended', '--json']);
    assert.equal(r.status, 0, r.stderr);
    const out = JSON.parse(r.stdout);
    assert.equal(out.metadata.caseCount, HELDOUT.length + EXTENDED.length);
    assert.equal(out.lexical.gates.promotionAuthorized, false);
    assert.equal(typeof out.lexical.summary.pass, 'number');
    assert.doesNotMatch(r.stderr, /PG_CONNECT_ATTEMPT/);
  });

  it('prices a model run with --plan without credentials or calls', () => {
    const r = cli(['--model', '--provider', 'anthropic', '--model-id', PRICED_MODEL, '--repeats', '3', '--plan', '--json']);
    assert.equal(r.status, 0, r.stderr);
    const plan = JSON.parse(r.stdout).modelPlan;
    assert.equal(plan.calls, HELDOUT.length * 3);
    assert.equal(plan.priced, true);
    assert.ok(plan.reservationUpperBoundUsd > 0);
  });

  it('refuses a model run without an explicit, known provider (there is no default)', () => {
    const none = cli(['--model', '--model-id', PRICED_MODEL, '--max-usd', '1']);
    assert.equal(none.status, 2);
    assert.match(none.stderr, /eval_provider_required/);
    const unknown = cli(['--model', '--provider', 'openai', '--model-id', PRICED_MODEL, '--max-usd', '1']);
    assert.equal(unknown.status, 2);
    assert.match(unknown.stderr, /eval_provider_unknown/);
  });

  it('refuses a paid run without a model id, a cap, a priced model, or credentials', () => {
    assert.match(cli(['--model']).stderr, /eval_model_id_required/);
    assert.match(cli(['--model', '--provider', 'anthropic', '--model-id', PRICED_MODEL]).stderr, /eval_cost_cap_required/);
    assert.match(cli(['--model', '--provider', 'anthropic', '--model-id', 'claude-unpriced-test', '--max-usd', '1']).stderr, /eval_unpriced_model/);
    const noKey = cli(['--model', '--provider', 'anthropic', '--model-id', PRICED_MODEL, '--max-usd', '1']);
    assert.equal(noKey.status, 2);
    assert.match(noKey.stderr, /eval_credentials_missing/);
  });
});

describe('final review: degraded, truncated, timed-out and mis-served responses', () => {
  const { produceLexicalFrame } = require('../producers/LexicalFrameProducer');
  const { gradeCase } = require('../eval/scoreInterpretation');
  // A case today's lexical producer gets right, so a fallback frame would pass.
  const lexicalPass = HELDOUT.find((c) => gradeCase(c, produceLexicalFrame({ text: c.text, context: c.context })).pass);

  it('never counts a degraded call as a successful model interpretation, even when the fallback frame is correct', async () => {
    assert.ok(lexicalPass, 'fixture: a lexically passing case exists');
    const client = fakeClient(() => Promise.reject(Object.assign(new Error('server error'), { status: 500 })));
    const result = await runModelEval({ cases: [lexicalPass], client, model: PRICED_MODEL, maxUsd: 1 });
    assert.equal(result.pooled.pass, 0);
    assert.deepEqual(result.modelInterpretations, { scored: 1, fromModel: 0, degraded: 1 });
    assert.equal(result.disagreements[0].failedDimensions.includes('modelResponse'), true);
  });

  it('stops the run on the first truncated response and counts it as failed', async () => {
    const client = fakeClient(() => ({ ...ok(), stop_reason: 'max_tokens', content: [{ type: 'text', text: '{"intent":"quest' }] }));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, repeats: 3, maxUsd: 5 });
    assert.equal(result.abortedReason, 'model_response_truncated');
    assert.equal(result.complete, false);
    assert.equal(client.calls.length, 1);
    assert.deepEqual(result.degraded.byReason, { stop_reason_max_tokens: 1 });
    assert.equal(result.pooled.pass, 0);
  });

  it('scores refusals and invalid frames as failed model interpretations', async () => {
    const client = fakeClient((n) => (n === 1 ? { ...ok(), stop_reason: 'refusal' } : { ...ok(), content: [{ type: 'text', text: '{"intent":"question"}' }] }));
    const result = await runModelEval({ cases: CASES.slice(0, 2), client, model: PRICED_MODEL, maxUsd: 5 });
    assert.deepEqual(result.degraded.byReason, { stop_reason_refusal: 1, invalid_frame: 1 });
    assert.equal(result.modelInterpretations.fromModel, 0);
    assert.equal(result.pooled.pass, 0);
  });

  it('treats a timed-out call as failed, keeps its full reservation, and settles it before the next reservation', async () => {
    const client = fakeClient((n, params) => new Promise(() => {}));
    // The SDK rejects on abort; emulate that through the signal.
    client.messages.create = async (params, options) => new Promise((_, reject) => {
      client.calls.push({ params, options });
      options.signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' })));
    });
    const result = await runModelEval({ cases: CASES.slice(0, 2), client, model: PRICED_MODEL, maxUsd: 5, timeoutMs: 20 });
    assert.deepEqual(result.degraded.byReason, { timeout: 2 });
    assert.equal(result.cost.openReservations, 0);
    assert.equal(result.cost.uncertain, 2);
    const plan = planModelRun({ cases: CASES.slice(0, 2), model: PRICED_MODEL });
    assert.ok(Math.abs(result.cost.settledUsd - plan.reservationUpperBoundUsd) < 1e-6, 'uncertain calls are charged at reservation');
    assert.equal(result.pooled.pass, 0);
  });

  it('stops after a call whose actual cost exceeds its reservation, and counts that call', async () => {
    const client = fakeClient(() => ({ ...ok(), usage: { input_tokens: 150000, output_tokens: 600 } }));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, repeats: 3, maxUsd: 5 });
    assert.equal(result.abortedReason, 'eval_reservation_overrun');
    assert.equal(client.calls.length, 1);
    assert.equal(result.tokens.calls, 1);
    assert.equal(result.cost.overruns, 1);
    assert.equal(result.cost.refusedPhase, 'settle');
  });

  it('cap: overrun and post-settlement excess exhaust the cap in the settle phase', async () => {
    const cap = createEvalSpendCap({ maxUsd: 0.06 });
    const id = await cap.reserve({ estimatedUsd: 0.05 });
    await cap.settle(id, { status: 'ok', estimatedUsd: 0.08 });
    const s = cap.snapshot();
    assert.equal(s.refused, 'eval_reservation_overrun');
    assert.equal(s.refusedPhase, 'settle');
    assert.ok(s.committedUsd > s.maxUsd, 'the overrun is reported, not hidden');
    await assert.rejects(cap.reserve({ estimatedUsd: 0.001 }), /eval_reservation_overrun/);
  });

  it('stops when the provider serves a different model, and reports what was served', async () => {
    const client = fakeClient(() => ({ ...ok(), model: 'claude-sonnet-5' }));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, maxUsd: 5 });
    assert.equal(result.abortedReason, 'model_identity_mismatch');
    assert.deepEqual(result.servedModels, { 'claude-sonnet-5': 1 });
    assert.equal(client.calls.length, 1);
  });

  it('accepts a dated snapshot of the requested model', async () => {
    const client = fakeClient(() => ({ ...ok(), model: `${PRICED_MODEL}-20261001` }));
    const result = await runModelEval({ cases: CASES, client, model: PRICED_MODEL, maxUsd: 5 });
    assert.equal(result.complete, true);
    assert.deepEqual(result.servedModels, { [`${PRICED_MODEL}-20261001`]: CASES.length });
  });
});

describe('final review: paid CLI path end to end against a local fake API', () => {
  const http = require('node:http');
  const { spawn } = require('node:child_process');

  it('sends only the requested model, reports served model, host and cost, and never connects to a database', async (t) => {
    const seen = [];
    const server = http.createServer((req, res) => {
      let body = '';
      req.on('data', (d) => { body += d; });
      req.on('end', () => {
        const params = JSON.parse(body);
        seen.push({ path: req.url, model: params.model, maxTokens: params.max_tokens });
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ id: `msg_${seen.length}`, type: 'message', role: 'assistant', ...ok(), model: params.model, stop_sequence: null }));
      });
    });
    // No host argument: listening on a host makes Node perform a dns.lookup,
    // which the unit lane's offline network guard (correctly) blocks.
    await new Promise((r) => server.listen(0, r));
    t.after(() => server.close());
    const { port } = server.address();
    const env = {
      ...process.env,
      ANTHROPIC_API_KEY: 'test-key-not-real',
      ANTHROPIC_BASE_URL: `http://127.0.0.1:${port}`,
      DATABASE_URL: 'postgresql://must-not-be-used.invalid:5432/prod',
      NO_PROXY: '127.0.0.1', no_proxy: '127.0.0.1',
    };
    const preload = path.join(__dirname, 'fixtures', 'recordPgConnect.cjs');
    const child = spawn(process.execPath, ['-r', preload, 'scripts/maxInterpretationEval.js', '--model', '--provider', 'anthropic', '--model-id', PRICED_MODEL, '--max-usd', '5', '--json'], { cwd: ROOT, env });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d; });
    child.stderr.on('data', (d) => { stderr += d; });
    const status = await new Promise((r) => child.on('close', r));
    assert.equal(status, 0, stderr);
    const out = JSON.parse(stdout).model;
    assert.equal(seen.length, HELDOUT.length);
    assert.ok(seen.every((s) => s.model === PRICED_MODEL && s.path === '/v1/messages'));
    assert.equal(out.complete, true);
    assert.deepEqual(out.servedModels, { [PRICED_MODEL]: HELDOUT.length });
    assert.equal(out.apiHost, `127.0.0.1:${port}`);
    assert.equal(out.cost.attempts, HELDOUT.length);
    assert.ok(out.cost.settledUsd > 0 && out.cost.settledUsd < 5);
    assert.doesNotMatch(stderr, /PG_CONNECT_ATTEMPT/);
  });
});
