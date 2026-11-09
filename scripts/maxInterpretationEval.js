#!/usr/bin/env node
'use strict';

/**
 * Held-out TurnFrame interpretation evaluation (P0 Unified Reasoning
 * Architecture, Phase A prototype). Offline only: never touches the database,
 * never routes, never dispatches.
 *
 *   node scripts/maxInterpretationEval.js                 # lexical baseline (free, deterministic)
 *   node scripts/maxInterpretationEval.js --model         # + model producer (needs ANTHROPIC_API_KEY; paid)
 *   node scripts/maxInterpretationEval.js --model --model-id claude-sonnet-5-5 --repeats 3
 *   node scripts/maxInterpretationEval.js --json          # machine-readable output
 *
 * The model run reports measured latency (p50/p95), token usage, cache reads,
 * degraded-call rate and per-dimension scores, which is the data the
 * architecture document's cost/latency estimates must be replaced with.
 */

const { CASES: ORIGINAL_CASES } = require('../packages/max/interpretation/eval/heldoutInterpretation');
const { CASES: EXTENDED_CASES } = require('../packages/max/interpretation/eval/extendedInterpretation');
const CASES = process.argv.includes('--extended') ? [...ORIGINAL_CASES, ...EXTENDED_CASES] : ORIGINAL_CASES;
const { scoreProducer } = require('../packages/max/interpretation/eval/scoreInterpretation');
const { produceLexicalFrame } = require('../packages/max/interpretation/producers/LexicalFrameProducer');

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : fallback;
}
const has = (name) => process.argv.includes(name);

function pct(xs, p) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

async function runModel() {
  const { forAgent } = require('../utils/meteredAnthropic');
  const { ModelFrameProducer } = require('../packages/max/interpretation/producers/ModelFrameProducer');
  const Metered = forAgent('max_interpreter_eval');
  const client = new Metered({ apiKey: process.env.ANTHROPIC_API_KEY });
  const producer = new ModelFrameProducer({
    client,
    model: arg('--model-id', process.env.MAX_INTERPRETATION_MODEL || process.env.MAX_WORKSPACE_MODEL || 'claude-sonnet-4-6'),
    timeoutMs: Number(arg('--timeout-ms', 0)) || undefined,
  });
  const repeats = Math.max(1, Number(arg('--repeats', 1)) || 1);
  const runs = [];
  const telemetry = [];
  for (let r = 0; r < repeats; r += 1) {
    runs.push(await scoreProducer(CASES, async (c) => {
      const { frame, telemetry: t } = await producer.produce({ text: c.text, context: c.context, transcript: c.transcript || [] });
      telemetry.push(t);
      return frame;
    }));
  }
  const lat = telemetry.map((t) => t.latencyMs);
  const sum = (k) => telemetry.reduce((a, t) => a + ((t.usage && t.usage[k]) || 0), 0);
  return {
    model: producer.model,
    repeats,
    summaries: runs.map((x) => x.summary),
    grades: runs[runs.length - 1].grades,
    latencyMs: { p50: pct(lat, 50), p95: pct(lat, 95), max: Math.max(...lat) },
    tokens: {
      input: sum('input_tokens'), output: sum('output_tokens'),
      cacheRead: sum('cache_read_input_tokens'), cacheWrite: sum('cache_creation_input_tokens'),
      calls: telemetry.length,
    },
    degraded: telemetry.filter((t) => t.degradedReason).map((t) => t.degradedReason),
  };
}

function printSummary(label, s) {
  console.log(`\n${label}: ${s.pass}% pass (n=${s.n})  safety violations: ${s.safetyViolations.length ? s.safetyViolations.join(', ') : 'none'}`);
  for (const [d, v] of Object.entries(s.byDimension)) console.log(`  ${d.padEnd(14)} ${v.pass === null ? '   -' : String(v.pass).padStart(5)}%  (n=${v.n})`);
  console.log('  by category:', Object.entries(s.byCategory).map(([k, v]) => `${k} ${v.pass}%`).join(' · '));
}

(async () => {
  const lexical = await scoreProducer(CASES, (c) => produceLexicalFrame({ text: c.text, context: c.context }));
  const out = { lexical: { summary: lexical.summary, grades: lexical.grades } };
  if (has('--model')) {
    if (!process.env.ANTHROPIC_API_KEY) {
      console.error('--model needs ANTHROPIC_API_KEY (calls are paid and metered).');
      process.exit(2);
    }
    out.model = await runModel();
  }
  if (has('--json')) {
    console.log(JSON.stringify(out, null, 2));
    return;
  }
  printSummary('lexical (today\'s signals)', lexical.summary);
  if (has('--verbose')) for (const g of lexical.grades.filter((x) => !x.pass)) console.log('   x', g.id, JSON.stringify(g.dims), JSON.stringify(g.frame));
  if (out.model) {
    out.model.summaries.forEach((s, i) => printSummary(`model ${out.model.model} run ${i + 1}`, s));
    console.log(`  latency p50 ${out.model.latencyMs.p50} ms · p95 ${out.model.latencyMs.p95} ms · tokens`, out.model.tokens, '· degraded', out.model.degraded.length);
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
