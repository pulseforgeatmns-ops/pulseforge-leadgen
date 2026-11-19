'use strict';

/**
 * Offline TurnFrame interpretation evaluation runner (P0-MAX-REASONING-001,
 * Workstream A). Never imported by production code.
 *
 * Offline means: no database, no routing, no missions, no dispatch. The model
 * run uses one provider (providers/*), metered against an in-process spend cap
 * (evalSpendCap) instead of the shared DB ledger:
 *   - anthropic: an injected plain SDK client, wrapped by meterClient;
 *   - gemini: a client built by the caller from utils/meteredGemini with the
 *     cap's reserve/settle injected (createClient(meter)).
 * The CLI is scripts/maxInterpretationEval.js.
 */

const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { scoreProducer, summarize, gradeCase } = require('./scoreInterpretation');
const { evaluatePromotionGates } = require('./promotionGates');
const { createEvalSpendCap } = require('./evalSpendCap');
const { produceLexicalFrame } = require('../producers/LexicalFrameProducer');
const { ModelFrameProducer, SYSTEM_PROMPT, buildRequestParams } = require('../producers/ModelFrameProducer');
const { createAnthropicProvider, createGeminiProvider, PROVIDERS } = require('../providers');
const { buildRequest: buildGeminiRequest, SYSTEM_INSTRUCTION: GEMINI_SYSTEM_INSTRUCTION, OUTPUT_FORMAT: GEMINI_OUTPUT_FORMAT } = require('../providers/GeminiProvider');
const { COMPACT_TURN_FRAME_OUTPUT_SCHEMA } = require('../CompactTurnFrame');
const { servedModelMatches: servedModelMatchesFor } = require('../interpreterModel');
const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { meterClient } = require('../../../../utils/meteredAnthropic');
const { estimateClaudeReservation, estimateGeminiReservation } = require('../../../../utils/apiPricing');

const sha256 = (value) => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');

function percentile(xs, p) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

function gitCommit(cwd) {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch (_) {
    return null;
  }
}

/** Reproducibility metadata: what was evaluated, against what, by what. */
function evaluationMetadata({ cases, corpus, cwd = process.cwd(), now = new Date() }) {
  return {
    generatedAt: now.toISOString(),
    commit: gitCommit(cwd),
    node: process.version,
    corpus,
    caseCount: cases.length,
    corpusSha256: sha256(cases.map((c) => ({ id: c.id, category: c.category, text: c.text, context: c.context, transcript: c.transcript || [], expect: c.expect }))),
    producerPromptSha256: sha256(SYSTEM_PROMPT),
    frameSchemaSha256: sha256(TURN_FRAME_OUTPUT_SCHEMA),
    // Gemini wire format: unchanged prompt + appended key legend, compact schema.
    gemini: {
      outputFormat: GEMINI_OUTPUT_FORMAT,
      systemInstructionSha256: sha256(GEMINI_SYSTEM_INSTRUCTION),
      outputSchemaSha256: sha256(COMPACT_TURN_FRAME_OUTPUT_SCHEMA),
    },
  };
}

async function runLexical(cases) {
  const { grades, summary } = await scoreProducer(cases, (c) => produceLexicalFrame({ text: c.text, context: c.context }));
  return { summary, grades, gates: evaluatePromotionGates({ grades }) };
}

/**
 * Price the run without calling anything: the exact request bodies produce()
 * would send, through the same reservation estimator the meter uses.
 */
function reservationFor(provider, model, c, thinking = null) {
  const input = { model, text: c.text, context: c.context, transcript: c.transcript || [], thinking };
  if (provider === 'gemini') return estimateGeminiReservation(buildGeminiRequest(input));
  return estimateClaudeReservation(buildRequestParams(input));
}

function planModelRun({ cases, model, repeats = 1, provider = 'anthropic', parallelRequests = 1, thinking = null }) {
  if (!PROVIDERS.includes(provider)) {
    return { provider, model, repeats, parallelRequests, calls: cases.length * repeats, priced: false, unpricedReason: 'eval_provider_unknown', reservationUpperBoundUsd: null, maxPerCallReservationUsd: null };
  }
  // Every parallel request is reserved on its own: the bound scales with them.
  const fanout = provider === 'gemini' ? parallelRequests : 1;
  const perCase = cases.map((c) => reservationFor(provider, model, c, provider === 'anthropic' ? thinking : null));
  const priced = perCase.every((x) => x !== null);
  const sum = priced ? perCase.reduce((a, b) => a + b, 0) : null;
  return {
    provider,
    model,
    repeats,
    parallelRequests: fanout,
    thinking: provider === 'anthropic' ? thinking : null,
    calls: cases.length * repeats,
    providerRequests: cases.length * repeats * fanout,
    priced,
    unpricedReason: priced ? null : 'model_not_in_utils_apiPricing',
    // Conservative: reservation estimates double-count input bytes and assume
    // max_tokens of output. Real spend is normally far lower.
    reservationUpperBoundUsd: priced ? Math.round(sum * repeats * fanout * 1e6) / 1e6 : null,
    maxPerCallReservationUsd: priced ? Math.round(Math.max(...perCase) * 1e6) / 1e6 : null,
  };
}

function stability(runs) {
  const byCase = new Map();
  for (const run of runs) {
    for (const g of run.grades) {
      const s = byCase.get(g.id) || { id: g.id, category: g.category, passes: 0, runs: 0, intents: new Set(), executionCandidate: new Set() };
      s.runs += 1;
      if (g.pass) s.passes += 1;
      s.intents.add(g.frame.intent);
      s.executionCandidate.add(g.frame.executionCandidate);
      byCase.set(g.id, s);
    }
  }
  const unstable = [...byCase.values()]
    .filter((s) => (s.passes !== 0 && s.passes !== s.runs) || s.intents.size > 1 || s.executionCandidate.size > 1)
    .map((s) => ({ id: s.id, category: s.category, passes: s.passes, runs: s.runs, intents: [...s.intents], executionCandidate: [...s.executionCandidate] }));
  return { cases: byCase.size, unstable };
}

/** Failing or unsafe grades, with the fixture text, for independent human review. */
function disagreementsForReview(runs, cases) {
  const text = new Map(cases.map((c) => [c.id, c.text]));
  const seen = new Map();
  runs.forEach((run, runIndex) => {
    for (const g of run.grades) {
      if (g.pass) continue;
      const entry = seen.get(g.id) || { id: g.id, category: g.category, text: text.get(g.id), failedRuns: [], failedDimensions: new Set(), frames: [] };
      entry.failedRuns.push(runIndex + 1);
      for (const [d, ok] of Object.entries(g.dims)) if (!ok) entry.failedDimensions.add(d);
      entry.frames.push(g.frame);
      seen.set(g.id, entry);
    }
  });
  return [...seen.values()].map((e) => ({ ...e, failedDimensions: [...e.failedDimensions] }));
}

/** Provider-aware identity check (interpreterModel.servedModelMatches); anthropic by default. */
function servedModelMatches(requested, served, provider = 'anthropic') {
  return servedModelMatchesFor(requested, served, provider);
}

/**
 * Grade one model call. A degraded call (timeout, HTTP error, refusal,
 * truncation, invalid JSON or frame) is a FAILED model interpretation even if
 * the lexical fallback frame happens to match the expectation. Safety is still
 * graded on the frame that would actually have been produced.
 */
function gradeModelCall(testCase, frame, telemetry) {
  const g = gradeCase(testCase, frame);
  if (!telemetry.degradedReason) return { ...g, modelResponse: true };
  return { ...g, pass: false, modelResponse: false, degradedReason: telemetry.degradedReason, dims: { ...g.dims, modelResponse: false } };
}

/**
 * @param {{
 *   cases: Array, model: string, repeats?: number, maxUsd: number, timeoutMs?: number,
 *   provider?: 'anthropic'|'gemini',
 *   client?: { messages: { create: Function } },   // anthropic: plain (un-metered) SDK client, metered here
 *   createClient?: (meter: { reserve: Function, settle: Function }) => { generateContent: Function }, // gemini
 * }} options
 */
async function runModelEval({ cases, client, createClient, provider = 'anthropic', model, repeats = 1, maxUsd, timeoutMs, parallelRequests = 1, thinking = null }) {
  if (!model || typeof model !== 'string') return { refused: 'eval_model_id_required' };
  if (!PROVIDERS.includes(provider)) return { refused: 'eval_provider_unknown' };
  if (!Number.isInteger(parallelRequests) || parallelRequests < 1 || parallelRequests > 2) return { refused: 'eval_parallel_requests_invalid' };
  if (parallelRequests !== 1 && provider !== 'gemini') return { refused: 'eval_parallel_requests_unsupported' };
  if (thinking !== null && thinking !== 'disabled') return { refused: 'eval_thinking_invalid' };
  if (thinking !== null && provider !== 'anthropic') return { refused: 'eval_thinking_unsupported' };
  const plan = planModelRun({ cases, model, repeats, provider, parallelRequests, thinking });
  if (!plan.priced) return { refused: 'eval_unpriced_model', plan };
  let cap;
  try {
    cap = createEvalSpendCap({ maxUsd });
  } catch (error) {
    return { refused: error.code || 'eval_cost_cap_required', plan };
  }
  // Track every metered attempt. A timed-out produce() returns before its
  // request has settled; waiting here keeps the post-call cap check current
  // before the next reservation and before the cost is reported.
  const inflight = new Set();
  const track = (fn) => (...a) => {
    const p = fn(...a);
    const settled = Promise.resolve(p).then(() => {}, () => {});
    inflight.add(settled);
    settled.finally(() => inflight.delete(settled));
    return p;
  };
  const allRequestTokens = { requests: 0, input: 0, output: 0 };
  let transport;
  if (provider === 'gemini') {
    if (typeof createClient !== 'function') return { refused: 'eval_client_required', plan };
    // Tokens of every provider request (parallel losers included), as settled.
    const settleWithUsage = async (id, outcome = {}) => {
      if (outcome && outcome.usage) {
        allRequestTokens.requests += 1;
        allRequestTokens.input += outcome.usage.input_tokens || 0;
        allRequestTokens.output += outcome.usage.output_tokens || 0;
      }
      return cap.settle(id, outcome);
    };
    const gemini = createClient({ reserve: cap.reserve, settle: settleWithUsage });
    if (!gemini || typeof gemini.generateContent !== 'function') return { refused: 'eval_client_required', plan };
    gemini.generateContent = track(gemini.generateContent.bind(gemini));
    transport = createGeminiProvider({ client: gemini, parallelRequests });
  } else {
    if (!client || !client.messages || typeof client.messages.create !== 'function') return { refused: 'eval_client_required', plan };
    meterClient(client, { agentName: 'max_interpreter_eval', reserve: cap.reserve, settle: cap.settle });
    client.messages.create = track(client.messages.create);
    transport = createAnthropicProvider({ client, thinking });
  }
  const settleInflight = () => Promise.all([...inflight]);
  const producer = new ModelFrameProducer({ provider: transport, model, timeoutMs });

  const runs = [];
  const telemetry = [];
  const servedModels = {};
  let abortedReason = null;
  for (let r = 0; r < repeats && !abortedReason; r += 1) {
    const grades = [];
    for (const c of cases) {
      await settleInflight();
      if (cap.exhausted) { abortedReason = cap.snapshot().refused; break; }
      const { frame, telemetry: t } = await producer.produce({ text: c.text, context: c.context, transcript: c.transcript || [] });
      await settleInflight();
      const snap = cap.snapshot();
      // Refused at reservation: the call never reached the provider, and the
      // degraded frame it produced is not model output. Not scored, not counted.
      if (snap.refused && snap.refusedPhase === 'reserve') { abortedReason = snap.refused; break; }
      telemetry.push({ ...t, caseId: c.id, run: r + 1 });
      if (t.servedModel) servedModels[t.servedModel] = (servedModels[t.servedModel] || 0) + 1;
      grades.push(gradeModelCall(c, frame, t));
      // Stop conditions after a call that did reach the provider. The call is
      // counted above; nothing further is sent.
      if (snap.refused) { abortedReason = snap.refused; break; }
      if (t.servedModel && !servedModelMatches(model, t.servedModel, provider)) { abortedReason = 'model_identity_mismatch'; break; }
      if (t.degradedReason === 'stop_reason_max_tokens') { abortedReason = 'model_response_truncated'; break; }
      // A rejected request (bad schema/field, unknown model, auth, quota) would
      // fail every remaining call identically: stop after the first one.
      if (/^http_4\d\d$/.test(t.degradedReason || '')) { abortedReason = 'provider_request_rejected'; break; }
      // A model that never reports what it served cannot pass the identity check.
      if (provider === 'gemini' && !t.servedModel && !t.degradedReason) { abortedReason = 'model_identity_unreported'; break; }
    }
    if (grades.length) runs.push({ grades, summary: summarize(grades), complete: grades.length === cases.length });
  }
  await settleInflight();

  const latencies = telemetry.map((t) => t.latencyMs).filter((x) => Number.isFinite(x));
  const sum = (k) => telemetry.reduce((a, t) => a + ((t.usage && t.usage[k]) || 0), 0);
  const degraded = telemetry.filter((t) => t.degradedReason);
  const degradedByReason = degraded.reduce((acc, t) => ({ ...acc, [t.degradedReason]: (acc[t.degradedReason] || 0) + 1 }), {});
  const pooled = runs.flatMap((r) => r.grades);
  const cost = cap.snapshot();
  const tokens = {
    input: sum('input_tokens'), output: sum('output_tokens'),
    cacheRead: sum('cache_read_input_tokens'), cacheWrite: sum('cache_creation_input_tokens'),
    calls: telemetry.length,
  };
  const latencyMs = { p50: percentile(latencies, 50), p95: percentile(latencies, 95), max: latencies.length ? Math.max(...latencies) : null };
  const timed = telemetry.filter((t) => t.timing && Number.isFinite(t.timing.totalMs));
  const timingMetric = (key) => {
    const values = timed.map((t) => t.timing[key]).filter(Number.isFinite);
    return { p50: percentile(values, 50), p95: percentile(values, 95), max: values.length ? Math.max(...values) : null };
  };
  const harnessValues = timed.map((t) => Math.max(0, t.latencyMs - t.timing.totalMs));
  const timingProfile = {
    measuredCalls: timed.length,
    reserveMs: timingMetric('reserveMs'),
    fetchToHeadersMs: timingMetric('fetchToHeadersMs'),
    bodyReadMs: timingMetric('bodyReadMs'),
    settleMs: timingMetric('settleMs'),
    harnessAfterClientMs: { p50: percentile(harnessValues, 50), p95: percentile(harnessValues, 95), max: harnessValues.length ? Math.max(...harnessValues) : null },
    slowestCompleted: timed
      .map((t) => ({ caseId: t.caseId, run: t.run, latencyMs: t.latencyMs, timing: t.timing, inputTokens: t.usage && t.usage.input_tokens, outputTokens: t.usage && t.usage.output_tokens }))
      .sort((a, b) => b.latencyMs - a.latencyMs)
      .slice(0, 20),
    timeouts: telemetry.filter((t) => t.degradedReason === 'timeout').map((t) => ({ caseId: t.caseId, run: t.run, latencyMs: t.latencyMs, timing: t.timing })),
  };
  const scored = pooled.length;
  const hedged = telemetry.filter((t) => t.parallel);
  const parallel = {
    requestsPerTurn: parallelRequests,
    turns: hedged.length,
    winnerIndex: hedged.reduce((acc, t) => ({ ...acc, [t.parallel.winner]: (acc[t.parallel.winner] || 0) + 1 }), {}),
    // Every settled provider request with reported usage (winners and losers).
    allRequestTokens: provider === 'gemini' ? allRequestTokens : null,
  };
  return {
    provider,
    model,
    // The request configuration that differs from the provider default.
    requestConfig: { thinking: provider === 'anthropic' ? thinking : null, parallelRequests },
    parallel,
    servedModels,
    repeats,
    plan,
    complete: !abortedReason && runs.length === repeats && runs.every((r) => r.complete),
    abortedReason,
    summaries: runs.map((r) => r.summary),
    pooled: summarize(pooled),
    // Calls whose frame came from the model itself (not a degraded fallback).
    modelInterpretations: { scored, fromModel: pooled.filter((g) => g.modelResponse).length, degraded: pooled.filter((g) => !g.modelResponse).length },
    stability: stability(runs),
    disagreements: disagreementsForReview(runs, cases),
    latencyMs,
    timingProfile,
    tokens,
    degraded: { calls: degraded.length, byReason: degradedByReason },
    cost,
    gates: evaluatePromotionGates({ grades: pooled, latencyMs, calls: telemetry.length, degradedCalls: degraded.length, tokens, estimatedUsd: cost.settledUsd }),
  };
}

module.exports = { evaluationMetadata, runLexical, planModelRun, runModelEval, stability, disagreementsForReview, percentile, servedModelMatches, gradeModelCall };
