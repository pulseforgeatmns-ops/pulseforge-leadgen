# P0-MAX-REASONING-001 — Integration readiness and acceptance report

Status: **reviewable. Nothing merged, deployed, migrated or enabled.** Date: 2026-11-18.
Integration branch: `claude/p0-max-reasoning-001-integration`, which is `origin/main` (`ee88320`) plus PRs #1131–#1135 merged in order, CI promotion, and this report. The branch is for verification only. Do not merge it; merge the five PRs themselves.

## 0. Verdict

| | |
|---|---|
| Code merge readiness (PRs 1–5) | **Ready for human review, in order 1 → 5.** All five merge cleanly onto current `main` once PR 1/PR 2 is resolved as documented (§1.2). No production behavior changes while the flags are off (§1.3). |
| Activation readiness (shadow, ledger, frame reasoning) | **Not ready.** The model evaluation has not run (§4), so no §10 promotion gate is measured. Frame reasoning and the evidence registry are not wired into routing by design. |
| Authorization safety | **No new authority path.** Two pre-existing defects listed as open were already fixed on `main` (#1096). **One new legacy-path defect class (R-1) was found** and is recorded, not fixed (§3). |
| Blockers before any rollout | The model evaluation (needs the dev key in the environment); a separate fix PR for R-1; a decision on splitting out the field-note fix (§1.4). |

## 1. Phase 1 — integration review

### 1.1 Per-PR findings

| PR | Scope | Production files touched | Findings | Readiness |
|---|---|---|---|---|
| #1131 PR 1 — interpretation eval harness | `TurnFrame`, lexical/model producers, held-out eval, paid-eval CLI with in-process spend cap | `ModelFrameProducer`, `scripts/maxInterpretationEval.js` | Fixed during integration: the eval CLI fake-API test bound `127.0.0.1` explicitly, which the unit lane's network guard rejects (`81e8ae7`, now `listen(0)`). Truncation guard and cap are covered by tests. | Ready |
| #1132 PR 2 — semantic shadow readiness | Explicit-only interpreter model, pending-decision kind tee, degraded/incomparable frames, 90-day purge | `ShadowInterpreter`, `ModelFrameProducer`, `WorkspaceEngine` (observer tee), eval/review scripts, `maxOperationalReadiness` | Conflicts with PR 1 in `scripts/maxInterpretationEval.js` and `CLAUDE.md` (§1.2). The tee passes only the decision **kind**, never the prompt. With `MAX_INTERPRETATION_MODEL` unset, no calls are made. | Ready after PR 1 |
| #1133 PR 3 — ConversationLedger | Bounded per-(tenant, session) memory, durable as v2 of `max_conversation_context`, recovery | `ConversationContextStore`, `ConversationContinuity`, `packages/max/understanding/*` persistence/repository, `WorkspaceEngine` recorder + recovery | Two fixes pushed during review: redundant scope correction (§1.5) and a field-note regression (§1.4). Off: v1 rows are byte-identical. **Rollback note:** code from before PR 3 rejects v2 rows. That degrades to a fresh conversation, and rows expire in 2 h. | Ready; split recommended (§1.4) |
| #1134 PR 4 — EvidenceRegistry | Tenant-scoped, read-only registry over existing loaders | **none** | Not imported by production (structural test). All SQL goes through `readOnlyQueryable`. | Ready |
| #1135 PR 5 — frame-directed reasoning | Reasoner + presentation guard, cross-PR suite | **none** | Not imported by production (structural test). Normalizes the ledger `members`/`memberIds` shape divergence (§6). | Ready after 3 and 4 |

No external callers of a changed module were affected (checked by grep across `routes/`, `services/`, agents and `packages/`).

### 1.2 PR 1 / PR 2 conflict (documented resolution, verified)

Merge PR 1, then PR 2:
- `scripts/maxInterpretationEval.js`: take PR 1's version (`--ours` when merging PR 2 onto PR 1). PR 2's edits to it are a subset of PR 1's.
- `CLAUDE.md`: keep both additions.

The integration branch was built exactly this way. Everything listed in §2 passes on it.

### 1.3 Unintended production changes

None found while every flag is off:
- `MAX_INTERPRETATION_MODEL` (unset means no model calls),
- `MAX_CONVERSATION_LEDGER`,
- `MAX_CONVERSATION_CONTEXT`,
- `MAX_FRAME_REASONING`.

The default-off parity test (`conversationLedgerRecovery.test.js`) shows identical answers and no durable write. PRs 4 and 5 add only unreferenced modules.

### 1.4 Field-note memory fix: review separately

PR 3 includes a change to `packages/max/understanding/conversationMemoryPersistence.js`: a contact correction now supersedes that contact's previous `ACTIVE_CONTACT` records. This **changes existing state ingestion**, and it is live code, not behind the ledger flag.

- **Regression found and fixed during this review (`efe0b5d`).** The first version collapsed two different people with the same name at different accounts into one account-less record, which lost data. The fix supersedes only when every active and replacement record of that name involves at most one account. If the check is ambiguous, or the memory load fails, it supersedes nothing. Covered by M3b (two Daves kept), M3c (unreadable memory supersedes nothing) and a Postgres case.
- **Recommendation:** move this change into its own small PR ahead of PR 3, so it gets a reviewer who owns field-note memory and its own rollback. PR 3's ledger does not depend on it.

### 1.5 PR 3 redundant-correction fix

Before the fix, "only the review-only ones", when the subject already had that scope, opened an `unresolved_correction`. The smallest safe fix (`ae8ab5f`) treats a correction to a scope the subject already has as a confirmation. It records a `record_scope:` preference and no open question. One branch in `applyTurn`; a mutation check confirms the new test catches a regression.

### 1.6 CI promotion

`.github/workflows/max-frame-reasoning.yml`:
- an `offline` job (hermetic suites),
- a `cross-pr` job: `setup-postgres-environment`, then `node --test packages/max/frameReasoning/integration/combinedArchitecture.integration.cjs`.

The path filters cover interpretation, context, evidence, understanding, frameReasoning, `WorkspaceEngine`/`WorkspaceTypes`, `OperationalInspection`, `ProspectRecordIntelligence`, `disposablePostgres` and the context migrations. The cross-PR suite fails, and never skips, when a PR is missing. It therefore belongs in CI only once all five PRs are on `main`. The workflow lives on the integration branch and should be moved onto PR 5 (or a follow-up) at merge time.

## 2. Combined test results (integration branch, local)

| Suite | Result |
|---|---|
| Max unit lane (`scripts/maxTestLanes.cjs unit`, offline network guard) | 2,568 tests: 2,567 pass, 0 fail, 1 todo |
| Interpretation (`test:max:interpretation`) | 81/81 |
| Semantic ratchet (`test:max:semantic`) | 9 pass, 1 todo; baseline unchanged |
| Frame reasoning + evidence (hermetic) | 34/34 |
| Ledger, context, field-note, readiness | 58/59. The failure is the pre-existing stale `implementedInDeployedCode` assertion in `test/maxOperationalReadiness.test.js:34`, which fails identically on `main`. |
| Decision service (JEV) | 115/115 |
| Authorization suites | 576/576 |
| All Postgres suites, including the cross-PR acceptance suite | 38/38 |
| **New:** `p0AuthorizationReadiness.test.js` (§3) | 116 tests: 84 pass, 0 fail, 32 todo (R-1, every todo fails as expected) |
| Lexical interpretation baseline | 53.2% (n=62, corpus `950eb659cd58`). Safety violations: C6, E1, E4, N1, X0c, X1c, X2c. These are frame nominations, never dispatches. The original 47.7% record is unchanged. |

Other pre-existing failure on `main`, unchanged: `test/aoAdminImpersonation.test.js`. GitHub CI has not run on the integration branch yet (§8).

## 3. Phase 2 — authorization safety

### 3.1 Status of the defects listed as open

| Defect | Status on `main` | Evidence |
|---|---|---|
| §1.6 hedged/conditional approvals ("Maybe approve the plan? Not sure yet.", "Approve it, but hold the emails until Monday.") | **Fixed by #1096** (Nov 8). The architecture doc, `CLAUDE.md` and the PR 5 doc still said "open". All three are corrected on this branch. | `p0NegatedAuthorization.test.js`: 11 qualified phrasings × 6 gate kinds (resolver), × 4 reachable gates end to end. 0 dispatch, mission fingerprint unchanged. |
| #1094 bare "Cancel the mission." treated as exit | **Fixed by #1096.** With a bound mission it reaches the governed router as `CANCEL_PLAN`. "Exit this mission." never cancels. An unbound cancellation dispatches nothing. | Same file: "bare cancellation reaches the governed router", "leaving the conversation never cancels". **The GitHub issue is still open.** Recommend closing it with a reference to #1096 (owner's call). |

### 3.2 New characterization: `packages/max/workspace/tests/p0AuthorizationReadiness.test.js`

This file drives the real `WorkspaceEngine.ask()` at the plan, discovery, prioritization and execution gates. It does not change authorization behavior.

| Class | Cases | Result |
|---|---|---|
| Ambiguous | "Hmm.", "ok?", "Yes and no.", "I think so, maybe.", "Not sure." | **Hold** at every gate. 0 dispatch, decision pending. |
| Withdrawn, same turn | "Approved. Actually, wait — hold on.", "Yes, go ahead. Never mind.", "Approve it. On second thought, don't.", "Approved — no, wait.", "Yes. Disregard that." | **Hold.** |
| Withdrawn, same turn: retraction idioms | "Go ahead. Scratch that.", "Go ahead. Forget it.", "Approved. Strike that.", "Approve it. Cancel that." | **DEFECT R-1** (see below). Recorded as `todo`. |
| Withdrawn, next turn | "Approved." then "Wait, I take that back." | Holds: neither approves the next gate nor cancels. Nothing undoes the first approval; that is by design, since reversal is an explicit cancel. |
| Stale approval across restart | Package shown → restart → recover → "Approved." | **Safe, but defended late.** Chat still submits `APPROVE_EXECUTION`, with no reviewed revision, because the presentation binding is intentionally not restored. The TME stage rolls it back as unreviewed: nothing committed, mission unchanged, reply "Not executed … Nothing was approved or sent." |
| Pending decision across recovery | Approve the plan → restart → "Same as before." / "Do what I approved earlier." / "Continue with what we decided." | Hold. Recovered decisions carry `authority: none`, `consumableApprovals` is empty, and the pending decision comes from the mission store. |
| Replayed bare approval | "Approved." sent twice at the plan gate | Approves plan, **then discovery**. Pinned as characterization. Not a defect by current design, but a **product decision**: chat approvals are not bound to the gate card they answer, and there is no message idempotency key. Discovery approval can spend provider budget. |

**Defect R-1: unmarked retraction idioms.** A retraction with no negation or uncertainty marker is ignored, so the earlier clause resolves:
- "Go ahead. Scratch that." / "Forget it." / "Strike that." dispatch the gate's approval at every gate. After the package is shown, that includes `APPROVE_EXECUTION`, which consumes the approval.
- "Approve it. Cancel that." dispatches **`CANCEL_PLAN`**: the operator meant the approval by "that", and the mission is cancelled instead.

Exposure today is limited, because outbound is in maintenance with `recovery_kill_switch` active. But plan, discovery and prioritization approvals advance mission state, and a cancellation is a durable state change.

Recommended fix, as a separate reviewable PR in the style of #1096, with no new phrase list:
- In `PendingDecisionResolver`, a clause that follows an authorization and refers back to it ("that", "it") with a retraction or dismissal verb makes the turn AMBIGUOUS, which means a confirmation request.
- Anaphoric "cancel that" after an approval must not resolve to `CANCEL_PLAN`.
- Remove the `todo` markers in the new suite as the acceptance test.

A durable fix is the two-key gate (architecture doc §3.4, Phase F).

### 3.3 Context never becomes authority (the new system)

By construction and by test:
- `TurnFrame.authority.grantsAuthority` is always false, and the validator rejects anything else.
- The ledger rejects authority-bearing rows on load. Recovered decisions have `authority: none`.
- The EvidenceRegistry is SELECT-only. `FrameDirectedReasoner` never calls a runtime, router or approval. State-changing frames return `not_executed`, naming the deterministic pathway.
- Nothing in routing reads the ledger, registry or reasoner (three structural tests).
- The cross-PR suite asserts an empty router audit and an unchanged mission fingerprint across scenarios A–G.

R-1 is a defect of the **legacy** deterministic resolver, not of the new path. On the new path, an R-1 phrasing yields a frame that is at most a nomination, and the reasoner cannot act on it.

## 4. Phase 3 — model evaluation: blocked

- A fresh session in the `pulseforge-leadgen` environment (checkout `ccr-9960efae-b7t0gi` @ `81e8ae7`) reported `ANTHROPIC_API_KEY` **not set**. It stopped before `npm ci`, the availability check, the dry-run plan and the paid run.
- **$0 spent on evaluation.** The session's own agent usage was about $0.43; that is not evaluation spend.
- Nothing was substituted, and no production credential was used.
- No accuracy, category, safety, degradation, latency, cost or instability figures exist. **The 85% promotion gate is unmeasured.** Do not treat the lexical 53.2% as a stand-in.
- To unblock: add the dedicated development key to the environment (environment settings, or the secure credential form; never chat). Then re-send the evaluation request (`claude-sonnet-5-5`, 62 × 3, `--max-usd 7`; dry-run plan first) to session `session_01WVeSoRYsjBdwba1Xi82Pj2`, or start a new one.

## 5. Evidence quality and blind spots (PR 4/5)

- Scout acquisition state is not consulted, because its store runs DDL on read. Company provenance stays "on file", never "Scout-found".
- The Anchor outbound gate is a fixture in the Postgres tests, not the production program row.
- Record reads are bounded at 1,000. Beyond that, ids are `not_verifiable_within_bound`, never "absent".
- Deleted and transferred-to-another-tenant records are indistinguishable (`not_found_in_tenant`), by design: no cross-tenant read.
- Contradictions are surfaced and never reconciled. Unknowns are never treated as proof that something did not happen.
- The presentation guard is pattern-based. The deterministic text stays the default, and any model presenter must pass the guard.

## 6. Known shape/contract divergence

The ledger's `resolveFocus()` returns `memberIds`, while stored referents use `members`. PR 5 normalizes both, and `LedgerReferentContract` accepts `memberIds`. Recommend aligning the ledger on one name in a follow-up, before any production consumer reads the ledger.

## 7. Production schema requirements (none run)

| Object | File | Status | Needed for |
|---|---|---|---|
| `max_interpretation_shadow` | `migrations/2026-11-09-max-interpretation-shadow.sql` (+ `.rollback.sql`) | On `main`. Application status in production not checked here. | Shadow observations (PR 2). Schedule `scripts/maxInterpretationShadowPurge.js` (90 days). |
| `max_conversation_context` | `migrations/2026-11-09-max-conversation-context.sql` | On `main`, previously reviewed | Durable ledger (`MAX_CONVERSATION_CONTEXT=true`). Schedule `purgeExpired()`. PR 3 adds no table. |
| JEV shadow persistence | JEV-006 operator migration | Production writes failing (`schema_column_missing`) per the architecture doc | Not required by these PRs; required for the decision-shadow corpus used by Phase F gates |

## 8. Proposed staging and shadow rollout gates

1. **Merge gate (code only):**
   - human review of PRs 1 → 5 in order;
   - the field-note change split out (§1.4);
   - the CI workflow moved onto PR 5;
   - GitHub CI green, including `cross-pr`.

   Flags stay off.
2. **Model evaluation gate:** the authorized Sonnet 5.5 run completes within $7, with 0 truncations, 0 authority-granting frames, and per-category accuracy recorded. The §10 thresholds (85% semantic interpretation) decide whether shadowing is worth enabling. Do not promote below them.
3. **R-1 fix gate:** a separate PR. The new suite's `todo` cases pass, and the #1093/#1096 suites stay green.
4. **Staging shadow (read-only Render staging, #1128):**
   - `MAX_INTERPRETATION_MODEL` set;
   - shadow migration applied in staging;
   - purge scheduled;
   - 0 response-path latency change;
   - persisted observations > 0;
   - degraded rate measured.
5. **Ledger in staging:** `MAX_CONVERSATION_LEDGER` + `MAX_CONVERSATION_CONTEXT`. Default-off parity is re-verified against staging traffic, restart recovery is exercised across a redeploy, and 0 authority-bearing rows are accepted.
6. **Production shadow** only after 4–5 hold for an agreed window, with owner approval.
7. Frame reasoning and two-key authority (Phases E/F) stay out of scope until a model-backed evaluation and a production disagreement corpus exist.

## 9. Explicit blockers

1. No model-backed evaluation: the key is missing from the environment, and $0 has been spent on it.
2. R-1 retraction defect (legacy path) is open. A fix PR is needed before relying on chat approvals more broadly.
3. Field-note ingestion change is bundled in PR 3. Recommend splitting it out.
4. Pre-existing failing tests on `main`: `maxOperationalReadiness` (stale assertion) and `aoAdminImpersonation`. They are unrelated, but they keep the full suite from being green.
5. GitHub CI has not yet run on the integrated set.
6. JEV production shadow persistence is still failing (pre-existing; blocks the Phase F corpus).
7. Product decision pending: whether chat approvals should be bound to the gate card they answer (the replayed "Approved." case).

## 10. Recommended next actions

1. Review and merge PRs #1131 → #1135 in order, after splitting out the field-note change. Keep all flags off.
2. Open the R-1 fix PR, using the new suite as acceptance.
3. Add the dev key to the environment and run the authorized evaluation. Then decide on the staging shadow.
4. Close #1094 with a reference to #1096. Fix or update the two stale tests on `main`.
5. Align the ledger `members`/`memberIds` naming before any production consumer.
