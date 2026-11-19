# P0-MAX-REASONING-001 — Provider-neutral interpretation and the Gemini adapter

Status: **implemented and tested offline. No paid call has been made. Nothing is deployed, migrated or enabled.** Date: 2026-11-18.

This PR **depends on #1131 (PR 1) and #1132 (PR 2)**. It is branched from `main` with both merged, using the documented resolution (`scripts/maxInterpretationEval.js` from PR 1, both `CLAUDE.md` additions). Review only the commits after the merge commit; merge after #1131 and #1132.

## Why

Pulseforge is moving away from Anthropic API dependencies. The TurnFrame interpreter, the offline evaluation and the semantic shadow observer were all bound to Anthropic's `messages.create` shape. Gemini is the only non-Anthropic text-generation provider already supported in this repo (`utils/meteredGemini`, `GEMINI_RATES`, the spend ledger and its caps). See `docs/P0-MAX-INTERPRETATION-EVAL-PROVIDER.md` on the integration branch.

## Design

```
interpretationPrompt (SYSTEM_PROMPT, buildUserContent)     ← unchanged text, unchanged prompt hash
        │
providers/  ── AnthropicProvider  (messages.create; request byte-identical to before)
            └─ GeminiProvider     (utils/meteredGemini generateContent)
        │      complete(...) → { finish: complete|truncated|refused|other, rawStopReason, text, servedModel, usage }
        ▼
ModelFrameProducer  ← the only place responses are validated (validateTurnFrame) or degraded (lexical, never executable)
        │
eval runner (in-process cap) · ShadowInterpreter (shared ledger)
```

- **Port.** Providers never read credentials, retry, or validate. Degradation reasons are stable across providers:
  - `stop_reason_max_tokens` (truncated);
  - `stop_reason_refusal`;
  - `stop_reason_<raw>` for any other finish;
  - `no_text_block`, `invalid_json`, `invalid_frame`, `timeout`, `http_<status>`, `provider_error`.
- **Anthropic path unchanged.** `new ModelFrameProducer({ client })` still builds the historical request (tested for deep equality). All 81 pre-existing interpretation tests pass.
- **Gemini request.**
  - Body: `systemInstruction` (the same prompt) and one user part (the same data payload).
  - `generationConfig`:
    - `maxOutputTokens: 600`, `temperature: 0`;
    - `responseMimeType: 'application/json'`, `responseJsonSchema: TURN_FRAME_OUTPUT_SCHEMA`;
    - `thinkingConfig: { thinkingBudget: 0 }`.
  - Response handling:
    - `finishReason` `STOP` → complete;
    - `MAX_TOKENS` → truncated;
    - `SAFETY` / `RECITATION` / `BLOCKLIST` / `PROHIBITED_CONTENT` / `SPII`, or `promptFeedback.blockReason` → refused;
    - anything else → other.
  - Thought parts are ignored. `modelVersion` is the served model. Usage is read from `usageMetadata`.
  - The producer's 1,500 ms budget is passed to the client, so a timed-out request is aborted rather than left running.

## Safety and cost controls (all preserved, some added)

| Control | Status |
|---|---|
| Corpus, isolation test, grader, 3 repeats, instability report | Unchanged |
| Promotion gates: ≥ 85% overall, critical categories, **0 authorization-safety violations**, p95, degraded rate; `promotionAuthorized` always false | Unchanged and provider-independent |
| Authority | A model frame never grants authority. A Gemini answer claiming `grantsAuthority`/`executionCandidate` is tested and forced to false. |
| Offline | `DATABASE_URL` pinned unroutable; Gemini metered through the in-process cap (`createGeminiClient({ reserve: cap.reserve, settle: cap.settle })`), never the shared DB ledger |
| Explicit configuration | `--provider` is **required** (no default), along with `--model-id` and `--max-usd` |
| Pricing | Exact Gemini request bodies priced with `estimateGeminiReservation`. Unpriced Gemini models are refused before any call (only `gemini-3.1-flash-lite` is priced). |
| Credentials | The eval reads **only** `MAX_EVAL_GEMINI_API_KEY`, a dedicated development key. The production `GEMINI_API_KEY` is never read, even when present (tested). |
| Availability | **New:** `--preflight` makes one free `models.get`. The model must exist under its exact id, support `generateContent`, and allow ≥ 600 output tokens. A paid run always preflights first and refuses otherwise, with 0 generation calls. |
| Model identity | Gemini: `modelVersion` must be the requested id (optionally `models/`-prefixed or a numbered `-NNN` version). Anything else stops the run (`model_identity_mismatch`), as does a missing `modelVersion` (`model_identity_unreported`). The Anthropic rule is unchanged. |
| Stop rules | Truncation (unchanged). **New:** any HTTP 4xx stops after the first call (`provider_request_rejected`). That catches an unsupported structured-output field, an unknown model, auth or quota after spending ≤ 1 call. |
| Test transport | `MAX_EVAL_GEMINI_BASE_URL` may only point at a loopback server; anything else is refused |

## Shadow observer compatibility

`ShadowInterpreter` resolves `MAX_INTERPRETATION_PROVIDER`:
- **Unset:** keeps the historical Anthropic transport, so no configuration changes behavior.
- **`gemini`:** uses `createGeminiClient` on the shared spend ledger, bound to each observation's tenant, with `GEMINI_API_KEY`.
  - Without that key there is no call and no row (`interpreter_credentials_missing`).
- **Unknown value:** reported (`interpreter_provider_invalid`).

Rows now carry `provider`. The identity check and cost are provider-aware: a matching numbered Gemini version is priced at the requested model's rate. Truncated or degraded observations stay incomparable. Readiness reports `semanticShadow.provider` with `{ id, explicit, credentialPresent }` (booleans, never a credential).

The shadow observer itself remains **default-off**. Enabling it with Gemini is a production configuration change that needs separate approval.

## Verification status of Gemini specifics

Google's documentation (`ai.google.dev`) is **not reachable** from this build environment (DNS blocked by the network policy). What is and isn't confirmed:

| Fact | Status |
|---|---|
| `generateContent` URL, `x-goog-api-key` header, `systemInstruction`, `contents`, `generationConfig.maxOutputTokens`, `thinkingConfig.thinkingBudget`, `usageMetadata` | Used in production by the presentation writer (`utils/meteredGemini`, `docs/P0-GEMINI-PRESENTATION-WRITER.md`) |
| `gemini-3.1-flash-lite` availability | Confirmed by a production `models.list` on 2026-11-12 (presentation-writer doc). **Must be re-confirmed with the dev key** via `--preflight`. |
| Price $0.25 / $1.50 per 1M input/output tokens | Verified 2026-11-12 (`utils/apiPricing.GEMINI_RATES`). Not re-checked today. |
| `responseMimeType` + `responseJsonSchema` acceptance and keyword support (`anyOf`, `additionalProperties`, `enum`) | **Unverified here.** If rejected, the first call returns 4xx and the run stops after ≤ 1 call (~$0.005). If accepted but loosened, `validateTurnFrame` degrades any non-conforming answer (counted as a failed interpretation, never executable). |
| `modelVersion` format | **Unverified.** An unexpected form stops the run after 1 call, and the identity rule is then widened only with that evidence. |

## Tests (offline)

`packages/max/interpretation/tests/geminiProvider.test.js`, 41 tests, all with fake fetch or a loopback fake API:
- **Anthropic parity:** the request is byte-identical; the producer still accepts a client.
- **Gemini request:** shape, header, URL, reserve → fetch → settle order.
- **Degradation, never executable:** `MAX_TOKENS`, `SAFETY`, blocked prompt, unknown finish, no candidate, no text, non-JSON, schema violation, HTTP 400 and 503, network error, timeout (with the request aborted), and an authority claim.
- **Evaluation run:**
  - pricing, unpriced and unknown providers, a missing client;
  - a complete run on the cap, the cap refusing before the first call;
  - truncation, 4xx, wrong model and unreported model each stopping after 1 call;
  - a numbered version accepted, degraded calls counted as failures, the key never in output.
- **Identity and provider configuration:** the rules above.
- **Shadow:** a Gemini row with cost and identity; truncated incomparable; no key → no call; unknown provider reported.
- **CLI:**
  - `--plan` with no key and no database;
  - the production key refused; unpriced model and non-loopback base URL refused;
  - `--preflight` is one free GET; a failed preflight blocks generation;
  - paid path end to end against a loopback API.

Mutation checks: all 7 safety mutations are caught. They are:
- no stop on a rejected request;
- no stop on an unreported model;
- falling back to the production key;
- `MAX_TOKENS` treated as complete;
- thought parts included;
- a failed preflight ignored;
- a loose identity rule.

## Running the authorized evaluation (once a dedicated dev key exists)

```
node scripts/maxInterpretationEval.js --model --provider gemini --model-id gemini-3.1-flash-lite --repeats 3 --extended --plan
MAX_EVAL_GEMINI_API_KEY=<dedicated dev key> node scripts/maxInterpretationEval.js --model --provider gemini --model-id gemini-3.1-flash-lite --preflight
MAX_EVAL_GEMINI_API_KEY=<dedicated dev key> node scripts/maxInterpretationEval.js --model --provider gemini --model-id gemini-3.1-flash-lite --repeats 3 --extended --max-usd 2 --json
```

Authorized cap: **$2**. Planned reservation upper bound for 62 × 3: ≈ $0.86. The environment must also allow `generativelanguage.googleapis.com`.

## Compact wire format: staging evaluation record (2026-11-19)

Gemini answers in `turn-frame-compact/v1` (`packages/max/interpretation/CompactTurnFrame.js`):
short keys and a flat `av`/`at`/`ap` action, every enum value unchanged, strictly
expanded to the canonical shape before the unchanged `repairUnsafeAuthorization` +
`validateTurnFrame`. The Anthropic request is byte-identical. The substantive prompt
is unchanged (`producerPromptSha256 b4a1f54d…`); Gemini receives it plus one appended
key legend (`systemInstructionSha256 10876473…`). Same corpus (`950eb659…`, 62 cases ×
3), same model (`gemini-3.1-flash-lite`), same 1,500 ms producer timeout, same gates.

| | Canonical (3707af3, run 435e5287) | Compact (7190358, run a9392646) |
|---|---|---|
| Complete / calls | yes / 186 | yes / 186 |
| Overall accuracy | 81.2% | 93.5% |
| Negation (critical) | 100% | 88.9% (2 timeouts) |
| Conditional authority (critical) | 63.0% | 96.3% |
| Ambiguity (critical) | 73.3% | 93.3% |
| Cross-tenant (critical) | 83.3% | 94.4% |
| Authorization safety violations | 0 | 0 |
| Timeouts → degraded | 35 (18.8%) | 12 (6.5%) |
| p50 / reported p95 / max | 1300 / 1501 / 1502 ms | 1168 / 1500 / 1502 ms |
| Provider fetch-to-headers p50 / p95 (completed) | 1246 / 1460 ms | 1156 / 1421 ms |
| Output tokens (total / mean) | 29,284 / 157 | 19,777 / 106 |
| Input tokens (total / mean) | 101,056 / 543 | 136,605 / 734 |
| Spend (cap $2) | $0.2128 | $0.1160 |
| Gates failing | 6 | 2 (negation, degraded rate) |

Findings:

- Every scored failure in both runs is a timeout degraded to the lexical producer;
  every frame the model returned (151/151, then 174/174) passed. Accuracy is bounded
  by latency, not interpretation quality.
- The compact format cut output tokens 32% but provider time only ~90 ms (≈1.8 ms
  per output token). Calls returning 76–98 output tokens still took 1.45–1.46 s.
  Latency is dominated by a fixed per-request cost plus server-side variance, so no
  further output trimming can bring the tail under 1,500 ms.
- The reported p95 of 1,500 ms is censored by the timeout (12/186 calls, 6.5%, were
  cut off at 1,500 ms, so the true p95 is above it). The latency gate's "pass" in
  this run must not be read as meeting the deadline.
- `promotionAuthorized: false`. No staging or production reasoning feature was
  activated.
