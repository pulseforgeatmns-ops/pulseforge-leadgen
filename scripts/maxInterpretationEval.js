#!/usr/bin/env node
'use strict';

/**
 * Held-out TurnFrame interpretation evaluation (P0 Unified Reasoning
 * Architecture; P0-MAX-REASONING-001 Workstream A). Offline only: never
 * touches the database, never routes, never dispatches.
 *
 *   node scripts/maxInterpretationEval.js                  # lexical baseline (free, deterministic)
 *   node scripts/maxInterpretationEval.js --extended       # + extended corpus
 *   node scripts/maxInterpretationEval.js --json           # machine-readable output
 *   node scripts/maxInterpretationEval.js --model --model-id M --repeats 3 --plan
 *        # prices the model run (no network, no key needed)
 *   node scripts/maxInterpretationEval.js --model --model-id M --repeats 3 --max-usd 2 --extended --json
 *        # PAID: needs ANTHROPIC_API_KEY, an approved model and an approved cap
 *
 * The model run needs an explicit --model-id and --max-usd. Spend is reserved
 * per attempt against an in-process cap using utils/apiPricing (no shared DB
 * ledger: the evaluation must stay offline from production databases). The run
 * stops at the cap and is reported incomplete; it never scores a refused call.
 * A model absent from utils/apiPricing is refused, because it cannot be capped.
 *
 * Exit codes: 0 ok · 2 refused configuration · 3 run stopped before completion.
 */

// Offline guarantee. The lexical producer reuses production helpers whose
// import graph constructs the shared pg pool (db.js). Pin it to an unroutable
// address before anything loads, so no query from this process can reach a
// real database whatever the shell's DATABASE_URL is (same convention as CI).
// dotenv never overrides an existing variable. Quiet keeps --json parseable.
process.env.DATABASE_URL = 'postgresql://localhost:1/max_interpretation_eval_offline';
process.env.DOTENV_CONFIG_QUIET = 'true';

const { CASES: ORIGINAL_CASES } = require('../packages/max/interpretation/eval/heldoutInterpretation');
const { CASES: EXTENDED_CASES } = require('../packages/max/interpretation/eval/extendedInterpretation');
const { evaluationMetadata, runLexical, planModelRun, runModelEval } = require('../packages/max/interpretation/eval/runInterpretationEval');

const has = (name) => process.argv.includes(name);
function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : fallback;
}

const extended = has('--extended');
const CASES = extended ? [...ORIGINAL_CASES, ...EXTENDED_CASES] : ORIGINAL_CASES;

function printSummary(label, s) {
  console.log(`\n${label}: ${s.pass}% pass (n=${s.n})  safety violations: ${s.safetyViolations.length ? s.safetyViolations.join(', ') : 'none'}`);
  for (const [d, v] of Object.entries(s.byDimension)) console.log(`  ${d.padEnd(14)} ${v.pass === null ? '   -' : String(v.pass).padStart(5)}%  (n=${v.n})`);
  console.log('  by category:', Object.entries(s.byCategory).map(([k, v]) => `${k} ${v.pass}%`).join(' · '));
}

function printGates(gates) {
  console.log('  promotion gates (§10; promotionAuthorized is always false):');
  for (const g of gates.gates) console.log(`    ${g.status.padEnd(28)} ${g.id}${g.observed === null ? '' : ` = ${g.observed}`} (${g.threshold})`);
}

function refuse(code, detail) {
  console.error(`refused: ${code}${detail ? ` — ${detail}` : ''}`);
  process.exit(2);
}

(async () => {
  const out = {
    metadata: evaluationMetadata({ cases: CASES, corpus: extended ? 'heldout+extended' : 'heldout' }),
    lexical: await runLexical(CASES),
  };

  if (has('--model')) {
    const model = arg('--model-id');
    const repeats = Math.max(1, Number(arg('--repeats', 1)) || 1);
    if (!model) refuse('eval_model_id_required', 'pass --model-id with the approved model; there is no default');
    const plan = planModelRun({ cases: CASES, model, repeats });
    if (has('--plan')) {
      out.modelPlan = plan;
    } else {
      if (!plan.priced) refuse('eval_unpriced_model', `${model} is not in utils/apiPricing; the cap cannot bound it`);
      const maxUsd = Number(arg('--max-usd'));
      if (!Number.isFinite(maxUsd) || maxUsd <= 0) refuse('eval_cost_cap_required', 'pass --max-usd with the approved cap');
      if (!process.env.ANTHROPIC_API_KEY) refuse('eval_credentials_missing', 'ANTHROPIC_API_KEY is required for the paid run');
      // A plain SDK client, metered by the runner against the in-process cap.
      const SDK = require('@anthropic-ai/sdk');
      const Anthropic = SDK.Anthropic || SDK;
      const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY, maxRetries: 0 });
      out.model = await runModelEval({
        cases: CASES, client, model, repeats, maxUsd,
        timeoutMs: Number(arg('--timeout-ms', 0)) || undefined,
      });
      if (out.model.refused) refuse(out.model.refused);
      // Where the calls went (ANTHROPIC_BASE_URL can redirect the SDK).
      out.model.apiHost = new URL(client.baseURL).host;
    }
  }

  if (has('--json')) {
    console.log(JSON.stringify(out, null, 2));
  } else {
    const m = out.metadata;
    console.log(`commit ${m.commit || 'unknown'} · node ${m.node} · corpus ${m.corpus} (${m.caseCount} cases, sha256 ${m.corpusSha256.slice(0, 12)})`);
    printSummary('lexical (today\'s signals)', out.lexical.summary);
    if (has('--verbose')) for (const g of out.lexical.grades.filter((x) => !x.pass)) console.log('   x', g.id, JSON.stringify(g.dims), JSON.stringify(g.frame));
    printGates(out.lexical.gates);
    if (out.modelPlan) console.log('\nmodel plan (no calls made):', out.modelPlan);
    if (out.model) {
      out.model.summaries.forEach((s, i) => printSummary(`model ${out.model.model} run ${i + 1}`, s));
      printSummary(`model ${out.model.model} pooled`, out.model.pooled);
      console.log('  api host:', out.model.apiHost, '· served models (provider-reported):', out.model.servedModels, '· model interpretations:', out.model.modelInterpretations);
      console.log(`  latency p50 ${out.model.latencyMs.p50} ms · p95 ${out.model.latencyMs.p95} ms · tokens`, out.model.tokens,
        '· degraded', out.model.degraded, '· cost', out.model.cost);
      console.log(`  unstable cases: ${out.model.stability.unstable.map((u) => u.id).join(', ') || 'none'}`);
      printGates(out.model.gates);
    }
  }
  if (out.model && !out.model.complete) {
    console.error(`run stopped before completion: ${out.model.abortedReason}`);
    process.exit(3);
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
