'use strict';

/**
 * P0-MAX-REASONING-001 — provider-neutral interpretation port and the Gemini
 * adapter. Every Gemini call goes to a fake fetch or a loopback fake API: no
 * network, no paid call, no credential, no database.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const http = require('node:http');
const { spawn, spawnSync } = require('node:child_process');

const { createGeminiClient, API_ORIGIN } = require('../../../../utils/meteredGemini');
const { createGeminiProvider, buildRequest } = require('../providers/GeminiProvider');
const { buildRequestParams } = require('../providers/AnthropicProvider');
const { ModelFrameProducer, SYSTEM_PROMPT, buildUserContent, buildRequestParams: producerBuildRequestParams } = require('../producers/ModelFrameProducer');
const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { runModelEval, planModelRun, evaluationMetadata } = require('../eval/runInterpretationEval');
const { resolveInterpreterProvider, servedModelMatches } = require('../interpreterModel');
const { ShadowInterpreter } = require('../ShadowInterpreter');
const { CASES: HELDOUT } = require('../eval/heldoutInterpretation');
const { CASES: EXTENDED } = require('../eval/extendedInterpretation');

const ROOT = path.resolve(__dirname, '../../../..');
const MODEL = 'gemini-3.1-flash-lite';
const CASES = HELDOUT.slice(0, 4);
const FAKE_KEY = 'fixture-gemini-eval-key';

const QUESTION_FRAME = {
  intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
  evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
};

const okBody = (frame = QUESTION_FRAME, extra = {}) => ({
  candidates: [{ content: { role: 'model', parts: [{ text: JSON.stringify(frame) }] }, finishReason: 'STOP' }],
  usageMetadata: { promptTokenCount: 1800, candidatesTokenCount: 140, totalTokenCount: 1940 },
  modelVersion: MODEL,
  ...extra,
});

/** A fake fetch for createGeminiClient: records calls, answers with respond(n, url, body). */
function fakeFetch(respond) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options, body: options.body ? JSON.parse(options.body) : null });
    const r = await respond(calls.length, url, calls[calls.length - 1].body, options);
    if (r instanceof Error) throw r;
    const status = r.status || 200;
    return { ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => r.body ?? r };
  };
  return { calls, fetchImpl };
}

/** Gemini client metered by an injected reserve/settle (the eval cap, or a recorder). */
function meteredGemini(fetchImpl, meter) {
  return createGeminiClient({ apiKey: FAKE_KEY, fetchImpl, reserve: meter.reserve, settle: meter.settle });
}

function geminiEval(respond, opts = {}) {
  const f = fakeFetch(respond);
  return runModelEval({
    cases: CASES, provider: 'gemini', model: MODEL, maxUsd: 2, ...opts,
    createClient: (meter) => meteredGemini(f.fetchImpl, meter),
  }).then((result) => ({ result, calls: f.calls }));
}

const ctx = (c) => ({ text: c.text, context: c.context, transcript: c.transcript || [] });

describe('provider port: the Anthropic path is unchanged', () => {
  it('the Anthropic provider sends exactly the historical request body', () => {
    const c = CASES[0];
    const input = { model: 'claude-sonnet-5-5', ...ctx(c) };
    assert.deepEqual(buildRequestParams(input), producerBuildRequestParams(input));
    const body = buildRequestParams(input);
    assert.equal(body.system[0].text, SYSTEM_PROMPT);
    assert.equal(body.messages[0].content, buildUserContent(ctx(c)));
    assert.deepEqual(body.output_config.format.schema, TURN_FRAME_OUTPUT_SCHEMA);
  });

  it('a producer built from a client still uses the Anthropic transport', () => {
    const p = new ModelFrameProducer({ client: { messages: { create: async () => ({}) } }, model: 'claude-sonnet-5-5' });
    assert.equal(p.provider, 'anthropic');
    assert.throws(() => new ModelFrameProducer({ provider: { name: 'x' }, model: 'm' }), /valid interpretation provider/);
    assert.throws(() => new ModelFrameProducer({ model: 'm' }), /Anthropic-compatible client/);
  });
});

describe('Gemini adapter: request shape', () => {
  it('sends the same instruction and data payload as every provider, constrained to the TurnFrame schema', () => {
    const c = CASES[1];
    const { model, body } = buildRequest({ model: MODEL, ...ctx(c) });
    assert.equal(model, MODEL);
    assert.deepEqual(body.systemInstruction, { parts: [{ text: SYSTEM_PROMPT }] });
    assert.deepEqual(body.contents, [{ role: 'user', parts: [{ text: buildUserContent(ctx(c)) }] }]);
    assert.equal(body.generationConfig.maxOutputTokens, 256);
    assert.equal(body.generationConfig.temperature, 0);
    assert.equal(body.generationConfig.responseMimeType, 'application/json');
    assert.deepEqual(body.generationConfig.responseJsonSchema, TURN_FRAME_OUTPUT_SCHEMA);
    assert.deepEqual(body.generationConfig.thinkingConfig, { thinkingBudget: 0 });
  });

  it('goes through the metered client: dedicated header, model URL, reservation before the call, settlement after', async () => {
    const events = [];
    const f = fakeFetch(() => { events.push('fetch'); return okBody(); });
    const client = meteredGemini(f.fetchImpl, {
      reserve: async (m) => { events.push(`reserve:${m.provider}:${m.model}`); assert.ok(m.estimatedUsd > 0); return 'r1'; },
      settle: async (id, o) => { events.push(`settle:${id}:${o.status}`); },
    });
    const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client }), model: MODEL });
    const { frame, telemetry } = await producer.produce(ctx(CASES[0]));
    assert.deepEqual(events, [`reserve:google_gemini:${MODEL}`, 'fetch', 'settle:r1:ok']);
    assert.equal(f.calls[0].url, `${API_ORIGIN}/v1beta/models/${MODEL}:generateContent`);
    assert.equal(f.calls[0].options.headers['x-goog-api-key'], FAKE_KEY);
    assert.equal(frame.producer, 'model');
    assert.equal(frame.intent, 'question');
    assert.equal(telemetry.provider, 'gemini');
    assert.equal(telemetry.servedModel, MODEL);
    assert.equal(telemetry.stopReason, 'STOP');
    assert.deepEqual(telemetry.usage, { input_tokens: 1800, output_tokens: 140, thinking_tokens: null, cache_read_input_tokens: null, total_tokens: 1940 });
  });

  it('ignores thought parts when reading the answer', async () => {
    const f = fakeFetch(() => ({ ...okBody(), candidates: [{ finishReason: 'STOP', content: { parts: [{ thought: true, text: 'thinking…' }, { text: JSON.stringify(QUESTION_FRAME) }] } }] }));
    const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client: meteredGemini(f.fetchImpl, { reserve: async () => 'r', settle: async () => {} }) }), model: MODEL });
    const { frame, telemetry } = await producer.produce(ctx(CASES[0]));
    assert.equal(telemetry.degradedReason, null);
    assert.equal(frame.producer, 'model');
  });
});

describe('Gemini adapter: every non-answer degrades to the lexical, never-executable frame', () => {
  async function produceWith(respond, timeoutMs) {
    const f = fakeFetch(respond);
    const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client: meteredGemini(f.fetchImpl, { reserve: async () => 'r', settle: async () => {} }) }), model: MODEL, timeoutMs });
    return producer.produce(ctx(CASES[0]));
  }
  const cases = [
    ['MAX_TOKENS → truncated', () => ({ ...okBody(), candidates: [{ finishReason: 'MAX_TOKENS', content: { parts: [{ text: '{"intent":"quest' }] } }] }), 'stop_reason_max_tokens'],
    ['SAFETY → refusal', () => ({ ...okBody(), candidates: [{ finishReason: 'SAFETY', content: { parts: [] } }] }), 'stop_reason_refusal'],
    ['blocked prompt → refusal', () => ({ promptFeedback: { blockReason: 'SAFETY' }, usageMetadata: { promptTokenCount: 10, totalTokenCount: 10 } }), 'stop_reason_refusal'],
    ['unknown finish', () => ({ ...okBody(), candidates: [{ finishReason: 'OTHER', content: { parts: [] } }] }), 'stop_reason_other'],
    ['no candidate', () => ({ usageMetadata: { promptTokenCount: 10, totalTokenCount: 10 } }), 'stop_reason_missing'],
    ['no text', () => ({ ...okBody(), candidates: [{ finishReason: 'STOP', content: { parts: [] } }] }), 'no_text_block'],
    ['not JSON', () => ({ ...okBody(), candidates: [{ finishReason: 'STOP', content: { parts: [{ text: 'Sure! Here is the frame' }] } }] }), 'invalid_json'],
    ['schema violation', () => okBody({ intent: 'question' }), 'invalid_frame'],
    ['HTTP 400 (rejected request)', () => ({ status: 400, body: { error: { code: 400, status: 'INVALID_ARGUMENT', message: 'x' } } }), 'http_400'],
    ['HTTP 503', () => ({ status: 503, body: { error: { code: 503 } } }), 'http_503'],
    ['network error', () => Object.assign(new Error('socket hang up'), { name: 'FetchError' }), 'provider_error'],
  ];
  for (const [label, respond, reason] of cases) {
    it(label, async () => {
      const { frame, telemetry } = await produceWith(respond);
      assert.equal(telemetry.degradedReason, reason);
      assert.equal(frame.producer, 'model_degraded_to_lexical');
      assert.equal(frame.executionCandidate, false);
      assert.equal(frame.authority.grantsAuthority, false);
    });
  }

  it('timeout: degraded, and the request itself is aborted by the client at the same budget', async () => {
    let aborted = false;
    const { telemetry } = await produceWith((n, url, body, options) => new Promise((_, reject) => {
      options.signal.addEventListener('abort', () => { aborted = true; reject(Object.assign(new Error('aborted'), { name: 'AbortError' })); });
    }), 25);
    assert.equal(telemetry.degradedReason, 'timeout');
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(aborted, true, 'the underlying request did not keep running');
  });

  it('a model answer can never grant authority, whatever it claims', async () => {
    const claims = { ...QUESTION_FRAME, intent: 'authorization', authority: { grantsAuthority: true }, executionCandidate: true };
    const { frame } = await produceWith(() => okBody(claims));
    assert.equal(frame.authority.grantsAuthority, false);
    assert.equal(frame.executionCandidate, false);
  });
});

describe('Gemini evaluation run: cost controls and stop rules', () => {
  it('prices the exact Gemini request bodies; unpriced Gemini models and unknown providers are refused before any call', async () => {
    const plan = planModelRun({ cases: [...HELDOUT, ...EXTENDED], model: MODEL, repeats: 3, provider: 'gemini' });
    assert.equal(plan.provider, 'gemini');
    assert.equal(plan.priced, true);
    assert.equal(plan.calls, (HELDOUT.length + EXTENDED.length) * 3);
    assert.ok(plan.reservationUpperBoundUsd > 0 && plan.reservationUpperBoundUsd < 2, `upper bound ${plan.reservationUpperBoundUsd}`);
    const unpriced = await geminiEval(() => okBody(), { model: 'gemini-3.5-pro' });
    assert.equal(unpriced.result.refused, 'eval_unpriced_model');
    assert.equal(unpriced.calls.length, 0);
    assert.equal((await runModelEval({ cases: CASES, provider: 'openai', model: MODEL, maxUsd: 1 })).refused, 'eval_provider_unknown');
    assert.equal((await runModelEval({ cases: CASES, provider: 'gemini', model: MODEL, maxUsd: 1 })).refused, 'eval_client_required');
  });

  it('a complete run: every call reserved and settled against the in-process cap, scored, gated, never authorized', async () => {
    const { result, calls } = await geminiEval(() => okBody(), { repeats: 2 });
    assert.equal(result.provider, 'gemini');
    assert.equal(result.complete, true);
    assert.equal(calls.length, CASES.length * 2);
    assert.equal(result.cost.attempts, CASES.length * 2);
    assert.equal(result.cost.openReservations, 0);
    assert.ok(result.cost.settledUsd > 0 && result.cost.settledUsd < 0.05);
    assert.deepEqual(result.servedModels, { [MODEL]: CASES.length * 2 });
    assert.equal(result.modelInterpretations.fromModel, CASES.length * 2);
    assert.equal(result.gates.promotionAuthorized, false);
    assert.equal(result.tokens.input, 1800 * CASES.length * 2);
  });

  it('the cap refuses before the first call when one reservation would exceed it', async () => {
    const { result, calls } = await geminiEval(() => okBody(), { maxUsd: 0.0001 });
    assert.equal(result.abortedReason, 'eval_cost_cap_reached');
    assert.equal(calls.length, 0, 'nothing reached the provider');
    assert.equal(result.modelInterpretations.scored, 0, 'a refused call is never scored');
  });

  it('stops after the first truncated response', async () => {
    const { result, calls } = await geminiEval(() => ({ ...okBody(), candidates: [{ finishReason: 'MAX_TOKENS', content: { parts: [{ text: '{' }] } }] }), { repeats: 3 });
    assert.equal(result.abortedReason, 'model_response_truncated');
    assert.equal(calls.length, 1);
  });

  it('stops after the first rejected request (e.g. an unsupported structured-output field)', async () => {
    const { result, calls } = await geminiEval(() => ({ status: 400, body: { error: { code: 400, status: 'INVALID_ARGUMENT' } } }), { repeats: 3 });
    assert.equal(result.abortedReason, 'provider_request_rejected');
    assert.equal(calls.length, 1);
    assert.equal(result.cost.uncertain, 1, 'the rejected call stays charged at its reservation');
  });

  it('stops when a different model is served, and reports it', async () => {
    const { result, calls } = await geminiEval(() => okBody(QUESTION_FRAME, { modelVersion: 'gemini-3.1-flash' }));
    assert.equal(result.abortedReason, 'model_identity_mismatch');
    assert.deepEqual(result.servedModels, { 'gemini-3.1-flash': 1 });
    assert.equal(calls.length, 1);
  });

  it('stops when the served model is not reported (identity cannot be checked)', async () => {
    const body = okBody();
    delete body.modelVersion;
    const { result, calls } = await geminiEval(() => body);
    assert.equal(result.abortedReason, 'model_identity_unreported');
    assert.equal(calls.length, 1);
  });

  it('accepts a numbered stable version of the requested model', async () => {
    const { result } = await geminiEval(() => okBody(QUESTION_FRAME, { modelVersion: `${MODEL}-001` }));
    assert.equal(result.complete, true);
  });

  it('a degraded call is a failed model interpretation even when the fallback frame would pass', async () => {
    const { result } = await geminiEval(() => ({ status: 503, body: {} }));
    assert.equal(result.modelInterpretations.fromModel, 0);
    assert.equal(result.pooled.pass, 0);
  });

  it('the credential never appears in the result', async () => {
    const { result } = await geminiEval(() => okBody());
    assert.doesNotMatch(JSON.stringify(result), new RegExp(FAKE_KEY));
  });

  it('metadata (corpus, prompt and schema hashes) is provider-independent', () => {
    const a = evaluationMetadata({ cases: HELDOUT, corpus: 'heldout', cwd: ROOT });
    assert.match(a.corpusSha256, /^[0-9a-f]{64}$/);
    assert.match(a.producerPromptSha256, /^[0-9a-f]{64}$/);
  });
});

describe('model identity and provider configuration', () => {
  it('Gemini identity accepts only the requested id (optionally models/-prefixed or -NNN)', () => {
    assert.equal(servedModelMatches(MODEL, MODEL, 'gemini'), true);
    assert.equal(servedModelMatches(MODEL, `models/${MODEL}`, 'gemini'), true);
    assert.equal(servedModelMatches(MODEL, `${MODEL}-001`, 'gemini'), true);
    assert.equal(servedModelMatches(MODEL, 'gemini-3.1-flash', 'gemini'), false);
    assert.equal(servedModelMatches(MODEL, `${MODEL}-preview-11-2026`, 'gemini'), false);
    assert.equal(servedModelMatches(MODEL, `${MODEL}-20261001`, 'gemini'), false);
    assert.equal(servedModelMatches('claude-sonnet-5-5', 'claude-sonnet-5-5-20261001'), true, 'Anthropic rule unchanged');
  });

  it('the interpreter provider is explicit when set, historical when unset, refused when unknown', () => {
    assert.deepEqual(resolveInterpreterProvider({}), { configured: true, provider: 'anthropic', source: 'default_historical', explicit: false });
    assert.equal(resolveInterpreterProvider({ MAX_INTERPRETATION_PROVIDER: 'gemini' }).provider, 'gemini');
    assert.equal(resolveInterpreterProvider({ MAX_INTERPRETATION_PROVIDER: 'openai' }).reason, 'interpreter_provider_invalid');
  });
});

describe('shadow observer compatibility', () => {
  const context = { tenantId: '10', tenantName: 'T', candidates: [], pendingDecisionId: null, focusIds: [] };
  const result = { structured: { metadata: {} }, owner: 'reasoning' };

  it('records a Gemini observation: provider, served model, identity check, truncation flag and cost', async () => {
    const rows = [];
    const f = fakeFetch(() => okBody(QUESTION_FRAME, { modelVersion: `${MODEL}-001` }));
    const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client: meteredGemini(f.fetchImpl, { reserve: async () => 'r', settle: async () => {} }) }), model: MODEL });
    const shadow = new ShadowInterpreter({ env: { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL, MAX_INTERPRETATION_PROVIDER: 'gemini' }, producer, persist: async (row) => rows.push(row) });
    shadow.observe({ context, text: 'What is the status of the mission?', result });
    await shadow.drain();
    assert.equal(rows.length, 1);
    const row = rows[0];
    assert.equal(row.provider, 'gemini');
    assert.equal(row.frameSource, 'model');
    assert.equal(row.servedModel, `${MODEL}-001`);
    assert.equal(row.modelMismatch, false);
    assert.equal(row.truncated, false);
    assert.ok(row.estimatedUsd > 0 && row.estimatedUsd < 0.01, `priced at the requested model's rate: ${row.estimatedUsd}`);
    assert.equal(row.shadowOnly, true);
    assert.equal(row.frame.authority.grantsAuthority, false);
  });

  it('a truncated Gemini observation is recorded as truncated and incomparable', async () => {
    const rows = [];
    const f = fakeFetch(() => ({ ...okBody(), candidates: [{ finishReason: 'MAX_TOKENS', content: { parts: [{ text: '{' }] } }] }));
    const producer = new ModelFrameProducer({ provider: createGeminiProvider({ client: meteredGemini(f.fetchImpl, { reserve: async () => 'r', settle: async () => {} }) }), model: MODEL });
    const shadow = new ShadowInterpreter({ env: { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL }, producer, persist: async (row) => rows.push(row) });
    shadow.observe({ context, text: 'status?', result });
    await shadow.drain();
    assert.equal(rows[0].truncated, true);
    assert.equal(rows[0].disagreement, null);
    assert.equal(rows[0].frameSource, 'degraded_fallback');
  });

  it('selecting gemini without its credential makes no call and writes no row', async () => {
    const rows = [];
    const audit = [];
    const shadow = new ShadowInterpreter({ env: { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL, MAX_INTERPRETATION_PROVIDER: 'gemini' }, persist: async (row) => rows.push(row), audit: (e) => audit.push(e) });
    shadow.observe({ context, text: 'status?', result });
    await shadow.drain();
    assert.equal(rows.length, 0);
    assert.ok(audit.some((e) => e.reason === 'interpreter_credentials_missing'));
  });

  it('an unknown provider is reported, never guessed', async () => {
    const audit = [];
    const shadow = new ShadowInterpreter({ env: { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL, MAX_INTERPRETATION_PROVIDER: 'openai' }, persist: async () => {}, audit: (e) => audit.push(e) });
    shadow.observe({ context, text: 'status?', result });
    await shadow.drain();
    assert.ok(audit.some((e) => e.reason === 'interpreter_provider_invalid'));
  });
});

describe('evaluation CLI: Gemini provider', () => {
  const preload = path.join(__dirname, 'fixtures', 'recordPgConnect.cjs');
  function cli(args, env = {}) {
    const childEnv = { ...process.env, DATABASE_URL: 'postgresql://must-not-be-used.invalid:5432/prod', ...env };
    delete childEnv.ANTHROPIC_API_KEY;
    if (!('MAX_EVAL_GEMINI_API_KEY' in env)) delete childEnv.MAX_EVAL_GEMINI_API_KEY;
    if (!('GEMINI_API_KEY' in env)) delete childEnv.GEMINI_API_KEY;
    return spawnSync(process.execPath, ['-r', preload, 'scripts/maxInterpretationEval.js', ...args], { cwd: ROOT, env: childEnv, encoding: 'utf8' });
  }

  it('prices a Gemini run with --plan, with no credential and no call', () => {
    const r = cli(['--model', '--provider', 'gemini', '--model-id', MODEL, '--repeats', '3', '--extended', '--plan', '--json']);
    assert.equal(r.status, 0, r.stderr);
    const plan = JSON.parse(r.stdout).modelPlan;
    assert.equal(plan.provider, 'gemini');
    assert.equal(plan.priced, true);
    assert.equal(plan.calls, (HELDOUT.length + EXTENDED.length) * 3);
    assert.doesNotMatch(r.stderr, /PG_CONNECT_ATTEMPT/);
  });

  it('never uses the production GEMINI_API_KEY: only the dedicated eval key is accepted', () => {
    const prodOnly = cli(['--model', '--provider', 'gemini', '--model-id', MODEL, '--max-usd', '2'], { GEMINI_API_KEY: 'production-key-must-not-be-used' });
    assert.equal(prodOnly.status, 2);
    assert.match(prodOnly.stderr, /eval_credentials_missing/);
    assert.match(prodOnly.stderr, /MAX_EVAL_GEMINI_API_KEY/);
    assert.doesNotMatch(prodOnly.stderr + prodOnly.stdout, /production-key-must-not-be-used/);
  });

  it('refuses an unpriced Gemini model and a non-loopback base URL override', () => {
    assert.match(cli(['--model', '--provider', 'gemini', '--model-id', 'gemini-3.5-pro', '--max-usd', '2'], { MAX_EVAL_GEMINI_API_KEY: 'k' }).stderr, /eval_unpriced_model/);
    const remote = cli(['--model', '--provider', 'gemini', '--model-id', MODEL, '--max-usd', '2'], { MAX_EVAL_GEMINI_API_KEY: 'k', MAX_EVAL_GEMINI_BASE_URL: 'https://example.com' });
    assert.equal(remote.status, 2);
    assert.match(remote.stderr, /eval_base_url_not_loopback/);
  });

  async function fakeGeminiApi(t, { model = MODEL, preflight = {}, respond = () => okBody() } = {}) {
    const seen = [];
    const server = http.createServer((req, res) => {
      let body = '';
      req.on('data', (d) => { body += d; });
      req.on('end', () => {
        seen.push({ method: req.method, path: req.url, key: req.headers['x-goog-api-key'], body: body ? JSON.parse(body) : null });
        res.writeHead(200, { 'content-type': 'application/json' });
        if (req.method === 'GET') {
          return res.end(JSON.stringify({ name: `models/${model}`, version: '001', supportedGenerationMethods: ['generateContent', 'countTokens'], inputTokenLimit: 1048576, outputTokenLimit: 65536, ...preflight }));
        }
        return res.end(JSON.stringify(respond(seen.length)));
      });
    });
    // No host argument: listening on a host performs a dns.lookup the offline guard blocks.
    await new Promise((r) => server.listen(0, r));
    t.after(() => server.close());
    return { seen, port: server.address().port };
  }

  function runChild(args, env) {
    const child = spawn(process.execPath, ['-r', preload, 'scripts/maxInterpretationEval.js', ...args], { cwd: ROOT, env: { ...process.env, DATABASE_URL: 'postgresql://must-not-be-used.invalid:5432/prod', NO_PROXY: '127.0.0.1', no_proxy: '127.0.0.1', ...env } });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d; });
    child.stderr.on('data', (d) => { stderr += d; });
    return new Promise((r) => child.on('close', (status) => r({ status, stdout, stderr })));
  }

  it('--preflight makes one free models.get and reports the model', async (t) => {
    const api = await fakeGeminiApi(t);
    const r = await runChild(['--model', '--provider', 'gemini', '--model-id', MODEL, '--preflight', '--json'], { MAX_EVAL_GEMINI_API_KEY: FAKE_KEY, MAX_EVAL_GEMINI_BASE_URL: `http://127.0.0.1:${api.port}` });
    assert.equal(r.status, 0, r.stderr);
    assert.deepEqual(api.seen.map((s) => `${s.method} ${s.path}`), [`GET /v1beta/models/${MODEL}`]);
    const pre = JSON.parse(r.stdout).preflight;
    assert.equal(pre.available, true);
    assert.equal(pre.supportsGenerateContent, true);
    assert.doesNotMatch(r.stdout + r.stderr, new RegExp(FAKE_KEY));
  });

  it('a paid run refuses before any generation when the preflight finds the model unavailable', async (t) => {
    const api = await fakeGeminiApi(t, { preflight: { supportedGenerationMethods: ['embedContent'] } });
    const r = await runChild(['--model', '--provider', 'gemini', '--model-id', MODEL, '--max-usd', '2', '--json'], { MAX_EVAL_GEMINI_API_KEY: FAKE_KEY, MAX_EVAL_GEMINI_BASE_URL: `http://127.0.0.1:${api.port}` });
    assert.equal(r.status, 2);
    assert.match(r.stderr, /eval_model_unavailable/);
    assert.equal(api.seen.filter((s) => s.method === 'POST').length, 0, 'no generation call was made');
  });

  it('paid path end to end against a loopback fake API: preflight, only the requested model, cost under the cap, no database', async (t) => {
    const api = await fakeGeminiApi(t);
    const r = await runChild(['--model', '--provider', 'gemini', '--model-id', MODEL, '--max-usd', '2', '--json'], { MAX_EVAL_GEMINI_API_KEY: FAKE_KEY, MAX_EVAL_GEMINI_BASE_URL: `http://127.0.0.1:${api.port}` });
    assert.equal(r.status, 0, r.stderr);
    const out = JSON.parse(r.stdout);
    assert.equal(out.preflight.available, true);
    const posts = api.seen.filter((s) => s.method === 'POST');
    assert.equal(posts.length, HELDOUT.length);
    assert.ok(posts.every((s) => s.path === `/v1beta/models/${MODEL}:generateContent` && s.key === FAKE_KEY));
    assert.ok(posts.every((s) => s.body.generationConfig.responseMimeType === 'application/json'));
    assert.equal(out.model.provider, 'gemini');
    assert.equal(out.model.complete, true);
    assert.equal(out.model.apiHost, `127.0.0.1:${api.port}`);
    assert.ok(out.model.cost.settledUsd > 0 && out.model.cost.settledUsd < 2);
    assert.equal(out.model.gates.promotionAuthorized, false);
    assert.doesNotMatch(r.stderr, /PG_CONNECT_ATTEMPT/);
    assert.doesNotMatch(r.stdout + r.stderr, new RegExp(FAKE_KEY));
  });
});
