# P0-MAX-REASONING-001 — PR 1: baseline and evaluation harness

Status: **implemented and tested on a branch. Not merged, not deployed, not enabled.**
No production behaviour, routing, schema, flag or data changed. No paid model call was made.

## 1. Baseline (measured 2026-11-18)

Base commit `ee88320878a40664c7fb50b3ee50be742efde7dd`, Node `v22.23.5`, `npm ci --ignore-scripts`.

| Suite | Command | Result |
|---|---|---|
| Interpretation contracts | `npm run test:max:interpretation` | 39 / 39 pass (57 / 57 after this PR) |
| Semantic generalization ratchet | `npm run test:max:semantic` | 9 pass, 0 fail, 1 todo (the documented acceptance target) |
| Max unit lane | `npm run test:max:unit` | 2526 tests: 2525 pass, 0 fail, 1 todo |
| Authorization / pending decision / operational read / continuity | 16 focused files (below) | 575 pass, 0 fail in the hermetic set; see known failures |
| Context store + shadow migration (PostgreSQL) | `node --test packages/max/context/tests/conversationContextPostgres.test.js packages/max/interpretation/tests/shadowPostgres.postgres.cjs` | 2 / 2 pass (run as an unprivileged user, see §4) |
| Context unit + authenticated recovery | `node --test packages/max/context/tests/*.test.js` | 10 / 10 non-PostgreSQL pass |

Focused authorization/continuity files: `p0NegatedAuthorization`, `p0OperationalReadNegatedExecution`, `p0OperationalReadPendingDecision`, `spec197PendingDecisionResolution`, `spec202PendingDecisionTurnOwnership`, `specJev004PendingDecisionCaptureGuard`, `p0OperationalInspection1087`, `p0OperationalReadPrecedence`, `p0OperationalReadEvidenceFocus`, `p0ReviewOnlyCorrections`, `tenantContext`, `spec208MissionContinuation` (all under `packages/max/workspace/tests/`), and `test/maxOperationalAuthorization`, `test/maxConversationMemory`, `test/maxConversationContextCleanupCron`, `test/maxContinuityStagingRunner`.

### Lexical interpretation baseline (offline, deterministic)

`node scripts/maxInterpretationEval.js [--extended]`. Corpus sha256 (extended, 62 cases) `950eb659cd58…`.

| Corpus | Overall | Safety | Unsafe nominations |
|---|---|---|---|
| Held-out (44) | 47.7% | 90.9% | C6, E1, E4, N1 |
| Held-out + extended (62) | 53.2% | 88.7% | C6, E1, E4, N1, X0c, X1c, X2c |

Extended, by dimension: intent 80%, referent 59.1%, action 33.3%, clarification 25%, foreign tenant 0%.
By category: paraphrase 0, contrast 71.4, multi_turn 33.3, unfamiliar_process 100, conflicting_evidence 0, incomplete_data 100, cross_tenant 0, ambiguous_execution 40, negation 66.7, cancel_vs_exit 50, unfamiliar_domain 100, reference_continuity 100, conditional_authority 0, uncertain_authority 100, future_restriction 100.

§10 gates against the lexical producer: overall, all four critical groups and safety **fail**; latency, degraded rate and cost are not applicable to a lexical producer. These numbers match the architecture document. The lexical producer is a measurement of today's signals and was not tuned. Its pinned baseline test is unchanged.

The "unsafe nominations" are offline frame nominations, not executions. Two reproduce end to end (architecture doc §1.6). No authorization regression was observed in the existing suites.

## 2. Defects found in the paid-evaluation path, and fixed

| # | Defect | Effect | Fix |
|---|---|---|---|
| 1 | `forAgent()` reserves spend through `utils/apiSpendControl` → `db.js` | Under `railway run`, the "offline" evaluation writes `api_usage_events` rows to production (and runs `ensureApiUsageSchema`). Without a database, every reservation throws before the call, each frame degrades to lexical, and the run is still labelled "model". | The runner wraps a plain SDK client with the existing `meterClient` using an **in-process cap** (`eval/evalSpendCap.js`) as reserve/settle. Pricing still comes from `utils/apiPricing`. |
| 2 | No cost cap | Unbounded spend on an approved run | `--max-usd` required. A reservation past the cap stops the run (`exit 3`, `complete:false`). The refused call is never scored. Failed calls keep their full reservation. |
| 3 | Default model drift (`claude-sonnet-4-6` in the eval vs `claude-haiku-5-5` in the producer) | A run could silently evaluate an unapproved model | `--model-id` required; no default. |
| 4 | A model missing from `utils/apiPricing` is unpriced | The cap cannot bound it | Refused (`eval_unpriced_model`) before any call. |
| 5 | `--json` output was unparseable | dotenv's banner (from `db.js`, loaded via the lexical producer's import graph) was printed to stdout ahead of the JSON | The CLI sets `DOTENV_CONFIG_QUIET` and pins `DATABASE_URL` to an unroutable address before loading anything (CI convention), so no query from the process can reach a real database. |
| 6 | Only the last repeat's grades were kept; no gates, metadata or review set | Variance, promotion status and reproducibility weren't reportable | The output adds pooled scores over all repeats, per-case stability (pass or intent flips), failing cases with fixture text for independent review, §10 gate evaluation (`promotionAuthorized` always `false`; external gates `not_measured_by_this_harness`), and commit, Node, corpus, prompt and schema hashes. `--plan` prices the exact request bodies without calling. |

`ModelFrameProducer` now exports `buildRequestParams`, so `--plan` prices exactly the request `produce()` sends. Request behaviour is unchanged (the existing 12 producer tests pass).

## 3. Prepared paid evaluation (NOT run: needs approval)

```
node scripts/maxInterpretationEval.js --model --extended --model-id APPROVED_MODEL --repeats 3 --plan
MAX_EVAL_ANTHROPIC_API_KEY=… node scripts/maxInterpretationEval.js --model --extended \
  --model-id APPROVED_MODEL --repeats 3 --max-usd APPROVED_CAP --json > eval.json
```

`--plan` results (62 cases × 3 = 186 calls):

- `claude-sonnet-5-5`: reservation upper bound **$6.68**, max $0.037 per call. The bound double-counts input and assumes full output; actual spend should be far lower. A cap of about $7 completes the run without stopping early.
- `claude-haiku-5-5` (the producer default, and the architecture doc's recommendation): **refused, unpriced**. The rate isn't in `utils/apiPricing`. The current published rate is $0.10 / $0.50 per MTok, but adding it changes shared production spend accounting and needs its own approval.

Decisions needed: the model, the cap, and the credential/environment to run in. The run does not need `DATABASE_URL`; the CLI ignores it.

Risk to measure on the run: `ModelFrameProducer` sends `max_tokens: 600` with `effort: low`. Current Haiku/Sonnet models think adaptively by default, and thinking tokens count toward `max_tokens`. Truncation would show up as `stop_reason_max_tokens` degradations. If so, repair it with a regression test before trusting the scores.

## 4. Known failures and harness limitations

- `test/maxConversationMemory.test.js` M3 ("correction supersedes old binding") **fails at base**. It belongs to `packages/max/understanding` (field-note contact memory), not the reasoning stack, and no npm script or workflow runs it. It is recorded here, not fixed. That module is relevant to PR 3 (do not build a second correction memory).
- `initdb` refuses to run as root, so the disposable-PostgreSQL suites fail in a root container. They pass when run as an unprivileged user with `/usr/lib/postgresql/16/bin` on `PATH`. CI runs them as a normal user.
- `test/maxConversationContextCleanupCron.test.js` fails under `denyWebsiteEvidenceNetwork.cjs` because it uses loopback HTTP. It passes when run as the repo runs it (without that preload).
- The offline corpus is author-written (44 + 18). §10 still requires an independently authored holdout and reviewed disagreements. A mocked-client pass is a contract test, not model accuracy.
- The production baseline in the spec (readiness at 02:34 UTC) was **not revalidated** by this PR, which made no production reads or changes.

## 5. Gaps against the spec (inputs to PRs 2–5)

- **PR 2 (shadow readiness).** `ShadowInterpreter.snapshot()` hard-codes `pendingDecisionId: null`, so every model `authorization` frame is repaired to `ambiguous`. Shadow therefore cannot measure authorization interpretation. Its fallback model (`claude-sonnet-4-6`) differs from the producer default. The table and migration replay are verified (§1). Retention (90-day delete) is documented but not implemented.
- **PR 3 (ConversationLedger).** Not implemented. Inputs to consolidate: `session.operationalFocus`, `session.operationalEvidence`, and the `ConversationContextStore` projection (mission ids, focus, evidence timestamps; 16 KiB cap; CAS; 2 h TTL), which is the durability path to reuse. `packages/max/understanding` holds a separate contact-level conversation memory; the ledger must define its relation to it rather than duplicate it.
- **PR 4 (EvidenceRegistry).** Not implemented. The loaders to adapt are `OperationalInspection` (`readOnlyQueryable` SELECT-only guard), `OperatingEvidenceRetrieval` (found/empty/failed/unavailable status) and `OperationalReadIntent`. `TurnFrame.EVIDENCE_DOMAINS` already defines the eight domains a frame may request.
- **PR 5 (frame-directed composer).** Not implemented. It depends on PRs 3 and 4.
