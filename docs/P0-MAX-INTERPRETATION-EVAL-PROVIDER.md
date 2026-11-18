# P0-MAX-REASONING-001 — Running the interpretation evaluation without Anthropic

Status: **recommendation implemented in #1140 (merged `fb8521b`; see `docs/P0-MAX-INTERPRETATION-GEMINI-ADAPTER.md`). The paid evaluation has not run.** No provider, credential, flag or production setting was changed. Date: 2026-11-18.

Direction: Pulseforge is moving away from Anthropic API dependencies. The Anthropic key and the Sonnet 5.5 evaluation are **not** pursued.

## 1. What already exists

| Provider | What is in the repo | Usable for TurnFrame interpretation? |
|---|---|---|
| **Google Gemini** | See the list below. | **Yes**, the only non-Anthropic text-generation provider already wired for metering, pricing and caps. |
| OpenAI | Whisper transcription only (`miraTranscriptionAgent.js`, `packages/max/voice/transcriptionAdapter.js`, `OPENAI_API_KEY`). There is no text-generation client, pricing table or spend metering. | Not without new infrastructure, which is a provider change and would need separate approval. |
| Anthropic | `utils/meteredAnthropic`. The interpretation stack is currently bound to it: `ModelFrameProducer` (`messages.create` shape), `runInterpretationEval` (`meterClient`, `estimateClaudeReservation`), `scripts/maxInterpretationEval.js` (`ANTHROPIC_API_KEY`), `ShadowInterpreter` (`meteredAnthropic.forAgent`). | Out of scope under the new direction. |

The Gemini pieces already in the repo:
- `utils/meteredGemini.createGeminiClient`, a bounded `generateContent` client:
  - `x-goog-api-key` header and a 30 s abort;
  - reservations and settlements through injectable `reserve`/`settle`, defaulting to the shared API-usage ledger.
- `utils/apiPricing`: `GEMINI_RATES` (`gemini-3.1-flash-lite`, $0.25 / $1.50 per 1M tokens, verified 2026-11-12), plus `estimateGeminiReservation`, `estimateGeminiUsage` and `sanitizedGeminiUsage`.
- Spend controls: `services/apiUsageService` already knows provider `google_gemini`, including pause and per-day/month caps.
- Production use: `PresentationEngine` (`docs/P0-GEMINI-PRESENTATION-WRITER.md`) with a **production** `GEMINI_API_KEY` in Railway.

**Recommendation: Gemini, `gemini-3.1-flash-lite`.** It is the only non-Anthropic model that is already priced, metered and verified available. Any other model needs a separately approved pricing entry and availability check first; see §5.

## 2. What has to change (code only, offline-testable)

The harness is provider-specific in three places. The change keeps every gate and simply swaps the transport.

1. **Provider port for the producer.** `ModelFrameProducer` should depend on a small normalized port, not on Anthropic's response shape:

   ```
   complete({ system, user, schema, maxOutputTokens, timeoutMs, signal })
     → { finish: 'complete'|'truncated'|'refused'|'other', text, servedModel, usage }
   ```

   There would be two adapters:
   - Anthropic: the current behavior, kept so existing tests and history stay comparable;
   - Gemini: wraps `createGeminiClient`.

   The producer's safety behavior is unchanged: `validateTurnFrame`, degrading to `model_degraded_to_lexical` on timeout, HTTP error, truncation, non-JSON or schema violation, the 1,500 ms timeout and no retries.
2. **Gemini request mapping.**
   - `systemInstruction` = the existing `SYSTEM_PROMPT`;
   - one user `contents` part = the existing `buildUserContent(...)`, so the model sees the same data;
   - `generationConfig = { maxOutputTokens: 600, temperature: 0, responseMimeType: 'application/json', <JSON schema field>: TURN_FRAME_OUTPUT_SCHEMA, thinkingConfig: { thinkingBudget: 0 } }`.

   Response mapping:
   - `finishReason: STOP` → `complete`;
   - `MAX_TOKENS` → `truncated` (degrade, as today);
   - safety or other reasons → `refused`/`other` (degrade);
   - text from `candidates[0].content.parts`; `modelVersion` → `servedModel`; usage via `sanitizedGeminiUsage`.

   **To verify against Google's current docs before implementing:** the exact structured-output field name and its schema subset. `TURN_FRAME_OUTPUT_SCHEMA` uses `type`, `enum`, `properties`, `required`, `items`, `anyOf` and `additionalProperties`. If the schema dialect rejects a keyword, keep the schema strict through `validateTurnFrame` (which already runs on every response) rather than loosening the contract.
3. **Pricing, cap and served-model check, provider-aware.**
   - `planModelRun` prices requests with `estimateGeminiReservation` for Gemini models; unpriced models stay refused (`eval_unpriced_model`).
   - The in-process `evalSpendCap` plugs straight into `createGeminiClient({ reserve: cap.reserve, settle: cap.settle })`. That keeps the evaluation **off the shared DB ledger**, exactly as today.
   - `servedModelMatches` gets a Gemini rule: `modelVersion` must equal the requested id or a documented versioned form of it. Anything else is reported as a substitution.
   - The CLI gains `--provider gemini`, with no default provider, and refuses without `GEMINI_API_KEY`. The Anthropic import becomes lazy, so a Gemini run never loads it.

Tests (hermetic, fake `fetchImpl`, in the existing `max-interpretation` CI workflow):
- request shape and header;
- `STOP` accepted;
- `MAX_TOKENS` degraded;
- safety block degraded;
- non-JSON and schema-violating output degraded;
- timeout degraded;
- served-model mismatch reported;
- cap refusal before the first call;
- unpriced model refused;
- the key never appears in output;
- the network-guarded unit lane stays green.

## 3. Gates and safety requirements: unchanged

All of these stay as they are. None is weakened or reinterpreted for a cheaper model:
- **Corpus and grading:**
  - the held-out corpus and its isolation test (no held-out utterances in production code);
  - corpus SHA in the metadata;
  - the same grader (`scoreInterpretation`);
  - 3 repeats with instability reporting.
- **Promotion gates** (`promotionGates.js`, `promotionAuthorized` always false):
  - overall ≥ 85%;
  - critical categories;
  - **0 authorization-safety violations**;
  - p95 latency;
  - degraded/invalid rate;
  - the external gates stay `not_measured_by_this_harness`.
- **Offline guarantees:** `DATABASE_URL` pinned to an unroutable address; no production database; no routing, dispatch or tool use.
- **Run controls:**
  - explicit `--model-id` and `--max-usd`;
  - `--plan` first, with no network;
  - stop at the cap and report the run incomplete (exit 3);
  - never score a refused call.
- **Authority:** a model frame never grants authority. `validateTurnFrame` forces `authority.grantsAuthority=false`, and the deterministic resolver (with #1093/#1096/#1138) stays the only authority path.

## 4. Credentials, network and cost (approval needed)

- **Key:** a **dedicated development** Gemini key. Not the production Railway `GEMINI_API_KEY`: the evaluation must not consume production quota or share its blast radius. Two options:
  - **network secret** for host `generativelanguage.googleapis.com` with header `x-goog-api-key`, injected by the proxy. The key never enters the container, and the CLI runs with a placeholder. Preferred.
  - **environment variable** `GEMINI_API_KEY`, readable by everyone using the environment.
- **Network:** this environment allows no extra hosts. `generativelanguage.googleapis.com` must be reachable. Saving it as a network secret also allows the host; otherwise it needs an allowlist entry. Package registries are unaffected.
- **Cost:** the conservative reservation upper bound, computed from the exact request bodies with `estimateGeminiReservation` (it over-counts input), is:
  - held-out corpus, 44 cases × 3: **≤ $0.62**;
  - held-out + extended, 62 cases × 3: **≤ $0.87**;
  - at most about $0.005 per call.

  Real spend is likely a fraction of that. A cap of **$2** leaves headroom for timeouts and retries-by-rerun while staying far below the previously authorized $7.

Proposed approval to request:

> Gemini `gemini-3.1-flash-lite`, held-out + extended corpus, 62 cases × 3 repeats, cap $2, dedicated development key as a network secret for `generativelanguage.googleapis.com`, no production credentials, no model substitution.

## 5. Caveats

- **Model capacity.** Flash-Lite is a small, low-latency model. It may not meet the 85% gate. The evaluation exists to find that out, and the gate must not be lowered to fit it. If it fails, the next candidate is a larger Gemini model. That needs:
  - a verified price added to `GEMINI_RATES`;
  - an availability check with the dev key;
  - separate approval.
- **Shadow interpreter.** `ShadowInterpreter` (PR 2) is still bound to `meteredAnthropic`. If Pulseforge drops Anthropic, the shadow observer needs the same provider port before it can ever be enabled. That is a separate change from running the evaluation.
- **History.** Earlier lexical-only results remain the baseline (47.7% held-out; 53.2% on the extended corpus). No model-backed result exists yet, from any provider.

## 6. Recommended sequence

1. Review this note.
2. Implement §2 as one small PR: provider port, Gemini adapter, provider-aware plan/cap/served-model, hermetic tests. No credentials are needed.
3. Approve the credential and network setup (§4) and the cap.
4. Run `--plan`, then the paid run. Report accuracy, categories, safety, degradation, latency, cost and instability against the unchanged gates.
