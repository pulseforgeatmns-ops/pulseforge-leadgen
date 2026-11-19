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
 *   node scripts/maxInterpretationEval.js --model --provider P --model-id M --repeats 3 --plan
 *        # prices the model run (no network, no key needed)
 *   node scripts/maxInterpretationEval.js --model --provider gemini --model-id M --preflight
 *        # free: confirms the model exists and supports generateContent (one models.get, no tokens)
 *   node scripts/maxInterpretationEval.js --model --provider P --model-id M --repeats 3 --max-usd 2 --extended --json
 *        # PAID: needs the provider's eval credential, an approved model and an approved cap
 *
 * Providers (explicit, no default): anthropic (ANTHROPIC_API_KEY) or gemini
 * (MAX_EVAL_GEMINI_API_KEY — a dedicated development key; the production
 * GEMINI_API_KEY is never read). A paid Gemini run always runs the preflight
 * first and refuses if the model is unavailable. MAX_EVAL_GEMINI_BASE_URL may
 * point the Gemini transport at a loopback test server only.
 *
 * The model run needs an explicit --provider, --model-id and --max-usd. Spend is reserved
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
const { PROVIDERS } = require('../packages/max/interpretation/providers');
const { MAX_TOKENS } = require('../packages/max/interpretation/producers/interpretationPrompt');
const { createGeminiClient, API_ORIGIN: GEMINI_API_ORIGIN } = require('../utils/meteredGemini');

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

const GEMINI_EVAL_KEY = 'MAX_EVAL_GEMINI_API_KEY';

/** Gemini transport for the evaluation: dedicated eval key, optional loopback-only base URL. */
function geminiTransport() {
  const apiKey = process.env[GEMINI_EVAL_KEY];
  if (!apiKey) {
    refuse('eval_credentials_missing', `${GEMINI_EVAL_KEY} (a dedicated development key) is required; the production GEMINI_API_KEY is never used`);
  }
  let origin = GEMINI_API_ORIGIN;
  const override = process.env.MAX_EVAL_GEMINI_BASE_URL;
  if (override) {
    let u;
    try { u = new URL(override); } catch (_) { refuse('eval_base_url_invalid', 'MAX_EVAL_GEMINI_BASE_URL'); }
    if (u.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname)) {
      refuse('eval_base_url_not_loopback', 'MAX_EVAL_GEMINI_BASE_URL may only point at a loopback test server');
    }
    origin = u.origin;
  }
  const fetchImpl = (url, options) => globalThis.fetch(String(url).replace(GEMINI_API_ORIGIN, origin), options);
  return { apiKey, fetchImpl, origin, host: new URL(origin).host };
}

/**
 * Free availability check: one models.get (no tokens, no spend). The model
 * must exist under its exact id and support generateContent.
 */
async function geminiPreflight({ apiKey, fetchImpl }, model) {
  const url = `${GEMINI_API_ORIGIN}/v1beta/models/${encodeURIComponent(model)}`;
  let res;
  let body = null;
  try {
    res = await fetchImpl(url, { method: 'GET', headers: { 'x-goog-api-key': apiKey } });
    body = await res.json().catch(() => null);
  } catch (err) {
    return { available: false, model, reason: `preflight_request_failed:${err && err.name ? err.name : 'error'}` };
  }
  if (!res.ok) return { available: false, model, httpStatus: res.status, reason: `preflight_http_${res.status}` };
  const methods = Array.isArray(body && body.supportedGenerationMethods) ? body.supportedGenerationMethods : [];
  const nameOk = body && (body.name === `models/${model}` || body.name === model);
  const info = {
    model,
    name: body && body.name ? String(body.name) : null,
    version: body && body.version ? String(body.version) : null,
    supportsGenerateContent: methods.includes('generateContent'),
    inputTokenLimit: body && Number.isSafeInteger(body.inputTokenLimit) ? body.inputTokenLimit : null,
    outputTokenLimit: body && Number.isSafeInteger(body.outputTokenLimit) ? body.outputTokenLimit : null,
  };
  if (!nameOk) return { available: false, ...info, reason: 'preflight_model_name_mismatch' };
  if (!info.supportsGenerateContent) return { available: false, ...info, reason: 'preflight_generate_content_unsupported' };
  if (info.outputTokenLimit !== null && info.outputTokenLimit < MAX_TOKENS) return { available: false, ...info, reason: 'preflight_output_limit_below_request' };
  return { available: true, ...info };
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
    const provider = arg('--provider');
    if (!provider) refuse('eval_provider_required', `pass --provider (${PROVIDERS.join(' | ')}); there is no default`);
    if (!PROVIDERS.includes(provider)) refuse('eval_provider_unknown', provider);
    const plan = planModelRun({ cases: CASES, model, repeats, provider });
    if (has('--plan')) {
      out.modelPlan = plan;
    } else if (has('--preflight')) {
      if (provider !== 'gemini') refuse('eval_preflight_unsupported', 'the preflight is implemented for gemini');
      out.preflight = await geminiPreflight(geminiTransport(), model);
      if (!out.preflight.available) {
        console.log(JSON.stringify({ preflight: out.preflight }, null, 2));
        refuse('eval_model_unavailable', out.preflight.reason);
      }
    } else {
      if (!plan.priced) refuse('eval_unpriced_model', `${model} is not in utils/apiPricing; the cap cannot bound it`);
      const maxUsd = Number(arg('--max-usd'));
      if (!Number.isFinite(maxUsd) || maxUsd <= 0) refuse('eval_cost_cap_required', 'pass --max-usd with the approved cap');
      const timeoutMs = Number(arg('--timeout-ms', 0)) || undefined;
      if (provider === 'gemini') {
        const gemini = geminiTransport();
        out.preflight = await geminiPreflight(gemini, model);
        if (!out.preflight.available) refuse('eval_model_unavailable', out.preflight.reason);
        out.model = await runModelEval({
          cases: CASES, provider, model, repeats, maxUsd, timeoutMs,
          createClient: (meter) => createGeminiClient({ apiKey: gemini.apiKey, fetchImpl: gemini.fetchImpl,
            agentName: 'max_interpreter_eval', reserve: meter.reserve, settle: meter.settle }),
        });
        if (out.model.refused) refuse(out.model.refused);
        out.model.apiHost = gemini.host;
      } else {
        if (!process.env.ANTHROPIC_API_KEY) refuse('eval_credentials_missing', 'ANTHROPIC_API_KEY is required for the paid run');
        // A plain SDK client, metered by the runner against the in-process cap.
        const SDK = require('@anthropic-ai/sdk');
        const Anthropic = SDK.Anthropic || SDK;
        const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY, maxRetries: 0 });
        out.model = await runModelEval({ cases: CASES, provider, client, model, repeats, maxUsd, timeoutMs });
        if (out.model.refused) refuse(out.model.refused);
        // Where the calls went (ANTHROPIC_BASE_URL can redirect the SDK).
        out.model.apiHost = new URL(client.baseURL).host;
      }
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
    if (out.preflight) console.log('\nprovider preflight (no tokens):', out.preflight);
    if (out.model) {
      out.model.summaries.forEach((s, i) => printSummary(`model ${out.model.model} run ${i + 1}`, s));
      printSummary(`model ${out.model.model} pooled`, out.model.pooled);
      console.log('  provider:', out.model.provider, '· api host:', out.model.apiHost, '· served models (provider-reported):', out.model.servedModels, '· model interpretations:', out.model.modelInterpretations);
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
