'use strict';

/**
 * P0-MAX-REASONING-001 — Claude Haiku 5.5 benchmark preparation.
 * Fake clients and spawned CLIs only: no network, no paid call, no credential.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { modelRates, estimateClaudeUsage, estimateClaudeReservation, CLAUDE_STANDARD_PROMPT_LIMIT } = require('../../../../utils/apiPricing');
const { createAnthropicProvider, buildRequestParams, THINKING_SETTINGS } = require('../providers/AnthropicProvider');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { SYSTEM_PROMPT, MAX_TOKENS, buildUserContent } = require('../producers/interpretationPrompt');
const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { servedModelMatches } = require('../interpreterModel');
const { runModelEval, planModelRun } = require('../eval/runInterpretationEval');
const { anthropicPreflight } = require('../eval/anthropicPreflight');
const { CASES: HELDOUT } = require('../eval/heldoutInterpretation');
const { CASES: EXTENDED } = require('../eval/extendedInterpretation');

const ROOT = path.resolve(__dirname, '../../../..');
const HAIKU = 'claude-haiku-5-5';
const CORPUS = [...HELDOUT, ...EXTENDED];
const input = (c) => ({ text: c.text, context: c.context, transcript: c.transcript || [] });

const QUESTION = {
  intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
  evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
};
const reply = (frame = QUESTION, extra = {}) => ({
  stop_reason: 'end_turn', model: HAIKU,
  content: [{ type: 'text', text: JSON.stringify(frame) }],
  usage: { input_tokens: 900, output_tokens: 150 },
  ...extra,
});
function fakeClient(respond) {
  const calls = [];
  return { calls, messages: { create: async (params, options) => { calls.push({ params, options }); return respond(calls.length, params); } } };
}

describe('model configuration', () => {
  it('the verified model id is matched exactly (and only as itself or a dated snapshot)', () => {
    assert.equal(servedModelMatches(HAIKU, HAIKU, 'anthropic'), true);
    assert.equal(servedModelMatches(HAIKU, `${HAIKU}-20261007`, 'anthropic'), true);
    assert.equal(servedModelMatches(HAIKU, 'claude-haiku-4-5', 'anthropic'), false);
    assert.equal(servedModelMatches(HAIKU, 'claude-sonnet-5-5', 'anthropic'), false);
  });

  it('thinking settings are closed: default (none) or disabled', () => {
    assert.deepEqual(THINKING_SETTINGS, [null, 'disabled']);
    const client = fakeClient(() => reply());
    for (const bad of ['adaptive', 'enabled', true, {}, '']) {
      assert.throws(() => createAnthropicProvider({ client, thinking: bad }), /thinking must be one of/);
      assert.throws(() => buildRequestParams({ model: HAIKU, ...input(CORPUS[0]), thinking: bad }), /thinking must be one of/);
    }
  });
});

describe('pricing (platform.claude.com, verified 2026-11-19)', () => {
  it('uses $0.10 input / $0.50 output per MTok and 0.1x cache reads', () => {
    assert.deepEqual(modelRates(HAIKU), [0.10, 0.50]);
    assert.deepEqual(modelRates(`${HAIKU}-20261007`), [0.10, 0.50]);
    // 100,000 tokens of each kind (the top of the standard tier): $/MTok x 0.1.
    const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-12, `${a} != ${b}`);
    near(estimateClaudeUsage(HAIKU, { input_tokens: 100000, output_tokens: 0 }), 0.010);
    near(estimateClaudeUsage(HAIKU, { input_tokens: 0, output_tokens: 100000 }), 0.050);
    near(estimateClaudeUsage(HAIKU, { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 100000 }), 0.001);
    // 5-minute cache write = 1.25x input ($0.125/MTok).
    near(estimateClaudeUsage(HAIKU, { input_tokens: 0, output_tokens: 0, cache_creation_input_tokens: 100000, cache_creation: { ephemeral_5m_input_tokens: 100000 } }), 0.0125);
    // A typical interpreter call: ~900 input + ~150 output tokens.
    near(estimateClaudeUsage(HAIKU, { input_tokens: 900, output_tokens: 150 }), 0.000165);
  });

  it('prompts over 100,000 tokens (higher-priced tier) are left unpriced, never under-reported', () => {
    assert.equal(CLAUDE_STANDARD_PROMPT_LIMIT[HAIKU], 100000);
    assert.notEqual(estimateClaudeUsage(HAIKU, { input_tokens: 100000, output_tokens: 1 }), null);
    assert.equal(estimateClaudeUsage(HAIKU, { input_tokens: 100001, output_tokens: 1 }), null);
    assert.equal(estimateClaudeUsage(HAIKU, { input_tokens: 60000, cache_read_input_tokens: 50000, output_tokens: 1 }), null);
    // Other models keep their limits.
    assert.notEqual(estimateClaudeUsage('claude-sonnet-5-5', { input_tokens: 150000, output_tokens: 1 }), null);
  });

  it('every corpus request is priced, and the full 62 x 3 plan is far under the $2 cap', () => {
    for (const c of CORPUS) assert.ok(estimateClaudeReservation(buildRequestParams({ model: HAIKU, ...input(c), thinking: 'disabled' })) > 0, c.id);
    const plan = planModelRun({ cases: CORPUS, model: HAIKU, repeats: 3, provider: 'anthropic', thinking: 'disabled' });
    assert.equal(plan.priced, true);
    assert.equal(plan.calls, 186);
    assert.equal(plan.thinking, 'disabled');
    assert.ok(plan.reservationUpperBoundUsd > 0 && plan.reservationUpperBoundUsd < 0.5, String(plan.reservationUpperBoundUsd));
  });
});

describe('request: production behaviour unchanged; thinking off only when configured', () => {
  it('the default request is the historical body, with no thinking field', () => {
    const c = CORPUS[3];
    assert.deepEqual(buildRequestParams({ model: HAIKU, ...input(c) }), {
      model: HAIKU,
      max_tokens: MAX_TOKENS,
      system: [{ type: 'text', text: SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: buildUserContent(input(c)) }],
      output_config: { effort: 'low', format: { type: 'json_schema', schema: TURN_FRAME_OUTPUT_SCHEMA } },
    });
    assert.equal('thinking' in buildRequestParams({ model: HAIKU, ...input(c) }), false);
    assert.equal(createAnthropicProvider({ client: fakeClient(() => reply()) }).thinking, null);
  });

  it('the benchmark request differs only by thinking: disabled, at effort low, canonical schema, no sampling params', () => {
    const c = CORPUS[5];
    const base = buildRequestParams({ model: HAIKU, ...input(c) });
    const bench = buildRequestParams({ model: HAIKU, ...input(c), thinking: 'disabled' });
    assert.deepEqual(bench, { ...base, thinking: { type: 'disabled' } });
    assert.equal(bench.output_config.effort, 'low');
    assert.deepEqual(bench.output_config.format.schema, TURN_FRAME_OUTPUT_SCHEMA);
    for (const k of ['temperature', 'top_p', 'top_k']) assert.equal(k in bench, false, k);
  });

  it('a producer built from a plain client still sends the historical request', async () => {
    const client = fakeClient(() => reply());
    await new ModelFrameProducer({ client, model: HAIKU }).produce(input(CORPUS[0]));
    assert.equal('thinking' in client.calls[0].params, false);
  });

  it('the configured provider sends thinking: disabled on every call', async () => {
    const client = fakeClient(() => reply());
    const producer = new ModelFrameProducer({ provider: createAnthropicProvider({ client, thinking: 'disabled' }), model: HAIKU });
    for (const c of CORPUS.slice(0, 5)) await producer.produce(input(c));
    assert.ok(client.calls.every((x) => x.params.thinking && x.params.thinking.type === 'disabled'));
    assert.ok(client.calls.every((x) => x.options.maxRetries === 0));
  });
});

describe('structured output handling', () => {
  const produce = async (response, thinking = 'disabled') => {
    const producer = new ModelFrameProducer({ provider: createAnthropicProvider({ client: fakeClient(() => response), thinking }), model: HAIKU });
    return producer.produce(input(CORPUS[0]));
  };

  it('a complete JSON answer becomes a model frame', async () => {
    const { frame, telemetry } = await produce(reply());
    assert.equal(telemetry.degradedReason, null);
    assert.equal(frame.producer, 'model');
    assert.equal(telemetry.servedModel, HAIKU);
  });

  it('a thinking block, if present, is ignored and the text block is read', async () => {
    const { frame } = await produce(reply(QUESTION, { content: [{ type: 'thinking', thinking: '', signature: 'x' }, { type: 'text', text: JSON.stringify(QUESTION) }] }));
    assert.equal(frame.producer, 'model');
  });

  it('refusal, truncation, invalid JSON and schema violations degrade to the never-executable frame', async () => {
    const cases = [
      [reply(QUESTION, { stop_reason: 'refusal', content: [] }), 'stop_reason_refusal'],
      [reply(QUESTION, { stop_reason: 'max_tokens', content: [{ type: 'text', text: '{"intent":"qu' }] }), 'stop_reason_max_tokens'],
      [reply(QUESTION, { content: [{ type: 'text', text: 'not json' }] }), 'invalid_json'],
      [reply({ intent: 'question' }), 'invalid_frame'],
    ];
    for (const [response, reason] of cases) {
      const { frame, telemetry } = await produce(response);
      assert.equal(telemetry.degradedReason, reason);
      assert.equal(frame.producer, 'model_degraded_to_lexical');
      assert.equal(frame.executionCandidate, false);
    }
  });
});

describe('authorization safety: thinking off changes nothing in validation', () => {
  const ADVERSARIAL = (ctx) => [
    { ...QUESTION, intent: 'authorization', requestedAction: { verb: 'approve', targetCandidateId: ctx.pendingDecisionId || null, polarity: 'affirmative' }, confidence: 0.99 },
    { ...QUESTION, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'negated' }, confidence: 0.99 },
    { ...QUESTION, intent: 'instruction', foreignTenantReference: true, requestedAction: { verb: 'run_agent', targetCandidateId: null, polarity: 'affirmative' }, confidence: 0.99 },
    { ...QUESTION, intent: 'prohibition', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, confidence: 0.99 },
    { ...QUESTION, intent: 'question', referents: [{ candidateId: 'invented_id', role: 'primary' }], confidence: 0.99 },
  ];

  it('identical validated frames with and without thinking: disabled, across all 62 corpus turns', async () => {
    let compared = 0;
    for (const c of CORPUS) {
      for (const raw of ADVERSARIAL(c.context)) {
        const a = await new ModelFrameProducer({ provider: createAnthropicProvider({ client: fakeClient(() => reply(raw)) }), model: HAIKU }).produce(input(c));
        const b = await new ModelFrameProducer({ provider: createAnthropicProvider({ client: fakeClient(() => reply(raw)), thinking: 'disabled' }), model: HAIKU }).produce(input(c));
        assert.deepEqual(b.frame, a.frame, c.id);
        assert.equal(b.frame.authority.grantsAuthority, false);
        if (raw.requestedAction.polarity !== 'affirmative' || raw.foreignTenantReference) assert.equal(b.frame.executionCandidate, false, c.id);
        compared += 1;
      }
    }
    assert.equal(compared, CORPUS.length * 5);
  });
});

describe('evaluation run with the benchmark configuration', () => {
  it('meters every call, sends thinking: disabled, and reports the configuration', async () => {
    const client = fakeClient(() => reply());
    const cases = CORPUS.slice(0, 4);
    const result = await runModelEval({ cases, provider: 'anthropic', client, model: HAIKU, repeats: 2, maxUsd: 2, thinking: 'disabled' });
    assert.equal(result.complete, true);
    assert.deepEqual(result.requestConfig, { thinking: 'disabled', parallelRequests: 1 });
    assert.equal(client.calls.length, 8);
    assert.ok(client.calls.every((x) => x.params.thinking.type === 'disabled' && x.params.model === HAIKU));
    assert.equal(result.cost.attempts, 8);
    assert.equal(result.cost.settled, 8);
    assert.ok(result.cost.settledUsd > 0 && result.cost.settledUsd < 0.01);
    assert.equal(result.gates.gates.find((g) => g.id === 'authorization_safety_violations').observed, 0);
  });

  it('refuses an unknown thinking value, or thinking on a provider that does not take it', async () => {
    assert.equal((await runModelEval({ cases: CORPUS.slice(0, 1), provider: 'anthropic', client: fakeClient(() => reply()), model: HAIKU, maxUsd: 1, thinking: 'adaptive' })).refused, 'eval_thinking_invalid');
    assert.equal((await runModelEval({ cases: CORPUS.slice(0, 1), provider: 'gemini', createClient: () => null, model: 'gemini-3.1-flash-lite', maxUsd: 1, thinking: 'disabled' })).refused, 'eval_thinking_unsupported');
  });
});

describe('free preflight (Models API lookup, no tokens)', () => {
  const info = (overrides = {}) => ({
    type: 'model', id: HAIKU, display_name: 'Claude Haiku 5.5', max_input_tokens: 1000000, max_tokens: 128000,
    capabilities: { structured_outputs: { supported: true }, thinking: { supported: true, types: { adaptive: { supported: true }, disabled: { supported: true } } } },
    ...overrides,
  });
  const client = (respond) => ({ models: { retrieve: async (id) => respond(id) } });

  it('passes for the exact model with structured outputs and disabled thinking accepted', async () => {
    const r = await anthropicPreflight(client(() => info()), HAIKU, { thinking: 'disabled' });
    assert.equal(r.available, true);
    assert.equal(r.structuredOutputs, true);
    assert.equal(r.thinkingDisabledAccepted, true);
    assert.equal(r.maxOutputTokens, 128000);
  });

  it('fails closed on the wrong id, missing structured outputs, rejected disabled thinking, or an HTTP error', async () => {
    assert.equal((await anthropicPreflight(client(() => info({ id: 'claude-haiku-4-5' })), HAIKU)).reason, 'preflight_model_id_mismatch');
    assert.equal((await anthropicPreflight(client(() => info({ capabilities: { structured_outputs: { supported: false } } })), HAIKU)).reason, 'preflight_structured_outputs_unsupported');
    assert.equal((await anthropicPreflight(client(() => info({ capabilities: { structured_outputs: { supported: true }, thinking: { types: { disabled: { supported: false } } } } })), HAIKU, { thinking: 'disabled' })).reason, 'preflight_thinking_disabled_unsupported');
    assert.equal((await anthropicPreflight(client(() => info({ max_tokens: 100 })), HAIKU)).reason, 'preflight_output_limit_below_request');
    assert.equal((await anthropicPreflight(client(() => { throw Object.assign(new Error('nf'), { status: 404 }); }), HAIKU)).reason, 'preflight_http_404');
    assert.equal((await anthropicPreflight({}, HAIKU)).reason, 'preflight_client_unsupported');
  });

  it('a rejected disabled-thinking capability only matters when the run turns thinking off', async () => {
    const r = await anthropicPreflight(client(() => info({ capabilities: { structured_outputs: { supported: true }, thinking: { types: { disabled: { supported: false } } } } })), HAIKU);
    assert.equal(r.available, true);
  });
});

describe('CLI guards (no network)', () => {
  function cli(args, env = {}) {
    const childEnv = { ...process.env, ...env };
    delete childEnv.MAX_EVAL_ANTHROPIC_API_KEY;
    Object.assign(childEnv, env);
    return spawnSync(process.execPath, ['scripts/maxInterpretationEval.js', ...args], { cwd: ROOT, env: childEnv, encoding: 'utf8' });
  }

  it('prices the benchmark with --plan and records thinking: disabled', () => {
    const r = cli(['--model', '--provider', 'anthropic', '--model-id', HAIKU, '--repeats', '3', '--extended', '--thinking', 'disabled', '--plan', '--json']);
    assert.equal(r.status, 0, r.stderr);
    const plan = JSON.parse(r.stdout).modelPlan;
    assert.equal(plan.priced, true);
    assert.equal(plan.calls, 186);
    assert.equal(plan.thinking, 'disabled');
  });

  it('never uses the production ANTHROPIC_API_KEY: the dedicated eval key is required', () => {
    const r = cli(['--model', '--provider', 'anthropic', '--model-id', HAIKU, '--preflight', '--thinking', 'disabled'], { ANTHROPIC_API_KEY: 'production-key-must-not-be-used' });
    assert.equal(r.status, 2);
    assert.match(r.stderr, /eval_credentials_missing.*MAX_EVAL_ANTHROPIC_API_KEY/);
  });

  it('refuses --thinking for gemini and unknown thinking values', () => {
    assert.match(cli(['--model', '--provider', 'gemini', '--model-id', 'gemini-3.1-flash-lite', '--thinking', 'disabled', '--plan']).stderr, /eval_thinking_unsupported/);
    assert.match(cli(['--model', '--provider', 'anthropic', '--model-id', HAIKU, '--thinking', 'adaptive', '--plan']).stderr, /eval_thinking_invalid/);
  });
});
