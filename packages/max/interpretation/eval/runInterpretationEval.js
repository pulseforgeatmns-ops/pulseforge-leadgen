'use strict';

/**
 * Offline TurnFrame interpretation evaluation runner (P0-MAX-REASONING-001,
 * Workstream A). Never imported by production code.
 *
 * Offline means: no database, no routing, no missions, no dispatch. The model
 * run uses an injected Anthropic-compatible client wrapped by the existing
 * meterClient with an in-process spend cap (evalSpendCap) instead of the
 * shared DB ledger. The CLI is scripts/maxInterpretationEval.js.
 */

const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { scoreProducer, summarize } = require('./scoreInterpretation');
const { evaluatePromotionGates } = require('./promotionGates');
const { createEvalSpendCap } = require('./evalSpendCap');
const { produceLexicalFrame } = require('../producers/LexicalFrameProducer');
const { ModelFrameProducer, SYSTEM_PROMPT, buildRequestParams } = require('../producers/ModelFrameProducer');
const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { meterClient } = require('../../../../utils/meteredAnthropic');
const { estimateClaudeReservation } = require('../../../../utils/apiPricing');

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
function planModelRun({ cases, model, repeats = 1 }) {
  const perCase = cases.map((c) => estimateClaudeReservation(buildRequestParams({ model, text: c.text, context: c.context, transcript: c.transcript || [] })));
  const priced = perCase.every((x) => x !== null);
  const sum = priced ? perCase.reduce((a, b) => a + b, 0) : null;
  return {
    model,
    repeats,
    calls: cases.length * repeats,
    priced,
    unpricedReason: priced ? null : 'model_not_in_utils_apiPricing',
    // Conservative: reservation estimates double-count input bytes and assume
    // max_tokens of output. Real spend is normally far lower.
    reservationUpperBoundUsd: priced ? Math.round(sum * repeats * 1e6) / 1e6 : null,
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

/**
 * @param {{
 *   cases: Array, client: { messages: { create: Function } }, model: string,
 *   repeats?: number, maxUsd: number, timeoutMs?: number,
 * }} options  client must be a plain (un-metered) SDK client; it is metered here.
 */
async function runModelEval({ cases, client, model, repeats = 1, maxUsd, timeoutMs }) {
  if (!model || typeof model !== 'string') return { refused: 'eval_model_id_required' };
  const plan = planModelRun({ cases, model, repeats });
  if (!plan.priced) return { refused: 'eval_unpriced_model', plan };
  let cap;
  try {
    cap = createEvalSpendCap({ maxUsd });
  } catch (error) {
    return { refused: error.code || 'eval_cost_cap_required', plan };
  }
  meterClient(client, { agentName: 'max_interpreter_eval', reserve: cap.reserve, settle: cap.settle });
  const producer = new ModelFrameProducer({ client, model, timeoutMs });

  const runs = [];
  const telemetry = [];
  let abortedReason = null;
  for (let r = 0; r < repeats && !abortedReason; r += 1) {
    const grades = [];
    for (const c of cases) {
      if (cap.exhausted) { abortedReason = cap.snapshot().refused; break; }
      const { frame, telemetry: t } = await producer.produce({ text: c.text, context: c.context, transcript: c.transcript || [] });
      // A refused reservation never reached the provider; the degraded frame
      // it produced is not model output and must not be scored as such.
      if (cap.exhausted) { abortedReason = cap.snapshot().refused; break; }
      telemetry.push(t);
      grades.push((await scoreProducer([c], () => frame)).grades[0]);
    }
    if (grades.length) runs.push({ grades, summary: summarize(grades), complete: grades.length === cases.length });
  }

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
  return {
    model,
    repeats,
    plan,
    complete: !abortedReason && runs.length === repeats && runs.every((r) => r.complete),
    abortedReason,
    summaries: runs.map((r) => r.summary),
    pooled: summarize(pooled),
    stability: stability(runs),
    disagreements: disagreementsForReview(runs, cases),
    latencyMs,
    tokens,
    degraded: { calls: degraded.length, byReason: degradedByReason },
    cost,
    gates: evaluatePromotionGates({ grades: pooled, latencyMs, calls: telemetry.length, degradedCalls: degraded.length, tokens, estimatedUsd: cost.settledUsd }),
  };
}

module.exports = { evaluationMetadata, runLexical, planModelRun, runModelEval, stability, disagreementsForReview, percentile };
