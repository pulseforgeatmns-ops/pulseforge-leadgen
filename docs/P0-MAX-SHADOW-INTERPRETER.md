# Phase A: isolated semantic shadow observer

Depends on the offline architecture PR and the reviewed JEV persistence repair.
`WorkspaceEngine.ask` captures tenant-scoped candidate IDs, recent mission labels
and fresh focus IDs before the legacy path runs. After it produces its response,
a bounded setImmediate task runs the injected `ModelFrameProducer`. It uses the
existing metered Anthropic client, tenant-bound spend reservation/settlement,
structured output validation, no tools, no hidden retries and a 1.5-second timeout.

The observer has no route-selection or execution callback. It records a validated
frame and a comparison to the legacy handler's metadata; it never returns its
frame to the request. Tests also inject an approval nomination into the real
engine and verify that the execution audit remains empty. Default-off has no
model call, DB access or capture. Unset `MAX_INTERPRETATION_SHADOW` to roll back.

Persisted observations include tenant, unique audit ID, validated frame, legacy
handler, disagreement (or incomparable), ambiguity count, provider/degradation
reason, latency, input/output/cache tokens and estimated USD from existing pricing.
Unknown pricing is null, not a fabricated zero. Raw message text is never stored.
Frames contain candidate IDs but no customer records or evidence rows. The
observer does not claim those IDs or evidence are current authority.

Enablement (requires separate production approval):

1. Review/apply `migrations/2026-11-09-max-interpretation-shadow.sql` through the
   governed migration workflow. Verify JEV schema readiness separately.
2. Configure `MAX_INTERPRETATION_SHADOW=true`. Select an approved
   `MAX_INTERPRETATION_MODEL` (fallback `MAX_WORKSPACE_MODEL`, then the existing
   presentation default `claude-sonnet-4-6`). The managed service must already
   provide `ANTHROPIC_API_KEY`, `DATABASE_URL`, and its normal API spend controls.
   No new provider credentials or model service is introduced.
3. Schedule governed deletion of rows older than 90 days before rollout.
4. Review metadata with `node scripts/maxInterpretationShadowReview.js TENANT`.
   Correct operational inspection comparison depends on the JEV PR. The route
   comparison is deliberately coarse: it is a triage signal, not a semantic judge.

Offline verification uses injected clients, including provider failure, timeout,
refusal, truncation, malformed frames and out-of-candidate referents. A disposable
PostgreSQL test replays the migration and verifies persistence, degradation audit
and tenant-scoped retrieval. No paid evaluation or production DDL was run.

Evaluation: original 44 cases remain unchanged. The `--extended` corpus adds 18
turns across cold-chain logistics, fabric manufacturing and music education
(62 total, five tenant worlds), including unfamiliar descriptions, references,
uncertain/conditional authority and foreign-tenant requests. Isolation tests scan
production imports and utterances. Lexical baseline: original 47.7% overall;
extended 53.2% overall, 88.7% interpretation safety with seven unsafe nominations.
These are **offline frame nominations**, not executions. They demonstrate why
semantic routing must remain inactive. Mock-client success is a contract test,
not evidence of model understanding or a promotion score.

Later measured evaluation, in the configured managed environment:

```
node scripts/maxInterpretationEval.js --model --extended --model-id APPROVED_MODEL --repeats 3 --json
```

Capture the model ID/version, fixture revision, p50/p95 latency, degraded rate,
tokens/cache usage and ledger cost. The original architecture's numeric promotion
gates still apply; no readiness is inferred from the lexical baseline. An
independently authored holdout remains required before active routing promotion.

Known scope limit: this PR captures existing live-session referents; it does not
hydrate durable context or introduce active evidence retrieval. Durable continuity
has a separate default-off foundation PR. Cold turns without a trusted existing
session are skipped rather than deriving a tenant from untrusted message content.
