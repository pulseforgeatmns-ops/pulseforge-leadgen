'use strict';

/**
 * P0-MAX-REASONING-001 — parallel Gemini requests (tail-latency mitigation).
 * Fake fetch only: no network, no paid call, no credential, no database.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createGeminiClient } = require('../../../../utils/meteredGemini');
const { createGeminiProvider, firstUsable } = require('../providers/GeminiProvider');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { compactTurnFrame } = require('../CompactTurnFrame');
const { runModelEval, planModelRun } = require('../eval/runInterpretationEval');
const { CASES: HELDOUT } = require('../eval/heldoutInterpretation');

const MODEL = 'gemini-3.1-flash-lite';
const CASES = HELDOUT.slice(0, 4);
const KEY = 'fixture-gemini-eval-key';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const QUESTION = {
  intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
  evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
};
const body = (frame = QUESTION, finishReason = 'STOP', extra = {}) => ({
  candidates: [{ content: { parts: [{ text: JSON.stringify(compactTurnFrame(frame)) }] }, finishReason }],
  usageMetadata: { promptTokenCount: 700, candidatesTokenCount: 100, totalTokenCount: 800 },
  modelVersion: MODEL,
  ...extra,
});

/** fetch whose n-th call (1-based) is answered by script[n-1] = { delay, respond }. */
function scriptedFetch(script) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    const n = calls.push({ url, body: JSON.parse(options.body) });
    const step = script[(n - 1) % script.length];
    await new Promise((resolve, reject) => {
      const t = setTimeout(resolve, step.delay || 0);
      options.signal && options.signal.addEventListener('abort', () => { clearTimeout(t); reject(Object.assign(new Error('aborted'), { name: 'AbortError' })); });
    });
    const r = typeof step.respond === 'function' ? step.respond() : step.respond;
    if (r instanceof Error) throw r;
    const status = r.status || 200;
    return { ok: status < 300, status, headers: { get: () => null }, json: async () => JSON.parse(JSON.stringify(r.body ?? r)) }; // fresh object, like a real response
  };
  return { calls, fetchImpl };
}

function recorder() {
  const events = [];
  return {
    events,
    reserve: async () => { const id = `r${events.filter((e) => e.t === 'reserve').length + 1}`; events.push({ t: 'reserve', id }); return id; },
    settle: async (id, o) => { events.push({ t: 'settle', id, status: o.status }); },
  };
}

function producerWith(script, { parallelRequests = 2, timeoutMs = 1500 } = {}) {
  const f = scriptedFetch(script);
  const meter = recorder();
  const client = createGeminiClient({ apiKey: KEY, fetchImpl: f.fetchImpl, reserve: meter.reserve, settle: meter.settle });
  const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client, parallelRequests }), model: MODEL, timeoutMs });
  return { producer, f, meter };
}

const input = { text: CASES[0].text, context: CASES[0].context, transcript: CASES[0].transcript || [] };
const untilSettled = (meter, n) => (async () => { while (meter.events.filter((e) => e.t === 'settle').length < n) await sleep(5); })();

describe('parallel requests: configuration', () => {
  it('defaults to the historical single request', async () => {
    const { producer, f } = producerWith([{ respond: body() }], { parallelRequests: 1 });
    const { telemetry } = await producer.produce(input);
    assert.equal(f.calls.length, 1);
    assert.equal(telemetry.parallel, null);
    assert.equal(createGeminiProvider({ client: { generateContent: async () => ({}) } }).parallelRequests, 1);
  });

  it('accepts only 1 or 2', () => {
    const client = { generateContent: async () => ({}) };
    for (const bad of [0, 3, 1.5, '2', null]) {
      assert.throws(() => createGeminiProvider({ client, parallelRequests: bad }), /parallelRequests/);
    }
  });

  it('sends the identical request twice', async () => {
    const { producer, f } = producerWith([{ respond: body() }, { respond: body() }]);
    await producer.produce(input);
    assert.equal(f.calls.length, 2);
    assert.deepEqual(f.calls[0].body, f.calls[1].body);
    assert.equal(f.calls[0].url, f.calls[1].url);
  });
});

describe('parallel requests: the first complete answer wins', () => {
  it('uses the faster request and reports which one won', async () => {
    const { producer, meter } = producerWith([{ delay: 120, respond: body() }, { delay: 10, respond: body() }]);
    const { frame, telemetry } = await producer.produce(input);
    assert.equal(frame.producer, 'model');
    assert.deepEqual(telemetry.parallel, { issued: 2, winner: 1 });
    assert.ok(telemetry.latencyMs < 110, `latency ${telemetry.latencyMs}`);
    await untilSettled(meter, 2);
    assert.deepEqual(meter.events.filter((e) => e.t === 'reserve').map((e) => e.id), ['r1', 'r2']);
    assert.deepEqual(meter.events.filter((e) => e.t === 'settle').map((e) => e.status), ['ok', 'ok']);
  });

  it('a fast truncated answer does not beat a slower complete one', async () => {
    const { producer } = producerWith([{ delay: 5, respond: body(QUESTION, 'MAX_TOKENS') }, { delay: 40, respond: body() }]);
    const { frame, telemetry } = await producer.produce(input);
    assert.equal(telemetry.degradedReason, null);
    assert.equal(telemetry.parallel.winner, 1);
    assert.equal(frame.producer, 'model');
  });

  it('a fast transport error does not beat a slower complete one', async () => {
    const { producer, meter } = producerWith([{ delay: 5, respond: new Error('socket hang up') }, { delay: 30, respond: body() }]);
    const { frame, telemetry } = await producer.produce(input);
    assert.equal(telemetry.degradedReason, null);
    assert.equal(frame.producer, 'model');
    await untilSettled(meter, 2);
    assert.deepEqual(meter.events.filter((e) => e.t === 'settle').map((e) => e.status).sort(), ['ok', 'uncertain']);
  });
});

describe('parallel requests: failures still degrade to the never-executable frame', () => {
  it('both too slow → timeout, both reservations settled as uncertain', async () => {
    const { producer, meter } = producerWith([{ delay: 400, respond: body() }, { delay: 400, respond: body() }], { timeoutMs: 60 });
    const { frame, telemetry } = await producer.produce(input);
    assert.equal(telemetry.degradedReason, 'timeout');
    assert.equal(frame.producer, 'model_degraded_to_lexical');
    assert.equal(frame.executionCandidate, false);
    await untilSettled(meter, 2);
    assert.deepEqual(meter.events.filter((e) => e.t === 'settle').map((e) => e.status), ['uncertain', 'uncertain']);
  });

  it('both truncated → stop_reason_max_tokens (the eval stop rule still fires)', async () => {
    const { producer } = producerWith([{ delay: 5, respond: body(QUESTION, 'MAX_TOKENS') }, { delay: 10, respond: body(QUESTION, 'MAX_TOKENS') }]);
    const { telemetry } = await producer.produce(input);
    assert.equal(telemetry.degradedReason, 'stop_reason_max_tokens');
  });

  it('a provider rejection is surfaced over a transport error (stop rule still fires)', async () => {
    const { producer } = producerWith([{ delay: 5, respond: new Error('reset') }, { delay: 10, respond: { status: 400, body: { error: { message: 'bad' } } } }]);
    const { telemetry } = await producer.produce(input);
    assert.equal(telemetry.degradedReason, 'http_400');
  });

  it('firstUsable: no usable answer and no rejection returns the first answer, else throws the first error', async () => {
    const r = await firstUsable([Promise.resolve({ finish: 'refused', text: null }), Promise.reject(new Error('x'))]);
    assert.equal(r.winner, 0);
    await assert.rejects(firstUsable([Promise.reject(new Error('a')), Promise.reject(new Error('b'))]), /a/);
  });
});

describe('parallel requests: safety outcome is identical to a single request', () => {
  it('the same adversarial answer validates to the same frame with 1 or 2 requests', async () => {
    const ctx = { ...CASES[0].context, pendingDecisionId: null };
    const adversarial = [
      { ...QUESTION, intent: 'authorization', requestedAction: { verb: 'approve', targetCandidateId: null, polarity: 'affirmative' }, confidence: 0.99 },
      { ...QUESTION, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'negated' }, confidence: 0.99 },
      { ...QUESTION, intent: 'instruction', foreignTenantReference: true, requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, confidence: 0.99 },
    ];
    for (const raw of adversarial) {
      const single = producerWith([{ respond: body(raw) }], { parallelRequests: 1 });
      const dual = producerWith([{ delay: 30, respond: body(raw) }, { delay: 5, respond: body(raw) }]);
      const a = await single.producer.produce({ ...input, context: ctx });
      const b = await dual.producer.produce({ ...input, context: ctx });
      assert.deepEqual(b.frame, a.frame);
      assert.equal(b.frame.executionCandidate, false);
    }
  });
});

describe('parallel requests: evaluation accounting', () => {
  it('the plan reserves for every parallel request', () => {
    const one = planModelRun({ cases: CASES, model: MODEL, repeats: 3, provider: 'gemini' });
    const two = planModelRun({ cases: CASES, model: MODEL, repeats: 3, provider: 'gemini', parallelRequests: 2 });
    assert.equal(two.calls, one.calls);
    assert.equal(two.providerRequests, one.calls * 2);
    assert.ok(Math.abs(two.reservationUpperBoundUsd - one.reservationUpperBoundUsd * 2) < 1e-5);
  });

  it('a run meters, settles and counts the tokens of every request, losers included', async () => {
    const f = scriptedFetch([{ delay: 20, respond: body() }, { delay: 5, respond: body() }]);
    const result = await runModelEval({
      cases: CASES, provider: 'gemini', model: MODEL, repeats: 2, maxUsd: 2, parallelRequests: 2,
      createClient: (meter) => createGeminiClient({ apiKey: KEY, fetchImpl: f.fetchImpl, reserve: meter.reserve, settle: meter.settle }),
    });
    assert.equal(result.complete, true);
    assert.equal(f.calls.length, CASES.length * 2 * 2);
    assert.equal(result.cost.attempts, CASES.length * 2 * 2);
    assert.equal(result.cost.settled, result.cost.attempts);
    assert.equal(result.cost.openReservations, 0);
    assert.equal(result.tokens.calls, CASES.length * 2);
    assert.deepEqual(result.parallel.allRequestTokens, { requests: CASES.length * 4, input: 700 * CASES.length * 4, output: 100 * CASES.length * 4 });
    assert.equal(result.parallel.turns, CASES.length * 2);
    assert.equal(result.gates.gates.find((g) => g.id === 'authorization_safety_violations').observed, 0);
  });

  it('parallel requests are refused for providers that do not implement them', async () => {
    const r = await runModelEval({ cases: CASES, provider: 'anthropic', model: 'claude-x', maxUsd: 1, parallelRequests: 2, client: { messages: { create: async () => ({}) } } });
    assert.equal(r.refused, 'eval_parallel_requests_unsupported');
    const bad = await runModelEval({ cases: CASES, provider: 'gemini', model: MODEL, maxUsd: 1, parallelRequests: 3, createClient: () => null });
    assert.equal(bad.refused, 'eval_parallel_requests_invalid');
  });
});
