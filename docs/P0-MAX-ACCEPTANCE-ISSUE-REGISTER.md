# P0 Max acceptance — open issue register

Status: open. Companion to `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md`.

These issues are **not** caused or fixed by PR #1127. Every acceptance-suite
failure listed here occurs identically on `main` at `c6b075c`. They are
recorded so they are not dismissed because they predate the PR. All
reproductions are local and offline: no production database, no provider
calls.

Reproduction prefix used below (offline unit lane):

```sh
NODE_ENV=test env -u DATABASE_URL node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test <file>
```

Disposable-PostgreSQL suites must run as a non-root user (`initdb` refuses
root) and **without** the offline harness, which also blocks loopback DNS.

## Register

| ID | Issue | Evidence | Acceptance impact | Blocks S2? |
|---|---|---|---|---|
| R1 | "No paid ads" / "No spending" prohibit provider research | The spend rule in `packages/acquisition-mission/AuthorizationConstraints.js` (`id: 'spend'`) maps `paid|ads|spend…|budget|money|cost…` to `paid_media` **and** `provider_research`. `node -e "console.log(require('./packages/acquisition-mission').extractAuthorizationConstraints('Find law firms in Manchester NH. No paid ads.'))"` → `prohibited: ['paid_media','provider_research']` | An ordinary acquisition mission with a common spend restriction can never run discovery. After PR #1127 its status reads "Discovery not authorized" (truthful), but the restriction itself may not match operator intent. **Product decision needed**: should "no paid ads" forbid Places-based discovery? Mapping deliberately unchanged in PR #1127. | No (S2 is review-only) |
| R2 | Discovery wording approves a pending plan (unrestricted missions) | `PendingDecisionResolver.js` `KIND_TARGETS[plan_approval] = ['plan','discovery']`. `packages/acquisition-mission/tests/spec135.test.js` → "discovery does not execute on ask path before plan is locked": "Begin discovery." leaves discovery approval pending (`true !== false`, line 146) — the plan was approved by discovery wording. | Command interpretation (S5/S6): an operator asking to start discovery silently approves the plan (planning only; Scout does not run). PR #1127 refuses this only where discovery is prohibited. | No |
| R3 | SPEC-170 runtime boundary not enforced in `Scout.discover` | `packages/acquisition-mission/tests/spec170MissionRuntimeOwnership.test.js` → "Scout.discover rejects cross-runtime sync when Mission Engine is supplied for AMO mission": `Missing expected rejection.` | Routing integrity (SPEC-170/171): a Mission Engine can be synchronised from an AMO mission's Scout run. | No |
| R4 | S8 outbound verification path unproven | `spec071ExecuteOutbound` (7: e.g. `result.summary.sent >= 1` false; `providerCalls.length >= 1` false; maxSends/replay; provider success/failure evidence; idempotency), `spec186CanonicalSender` (3: canonical email/name to provider; sender change alters revision; READY→approval→EXECUTE binding), `spec212ProspectMessageBinding` (file), `specExecutionApprovalHydration` (1), `spec252PreparedOutreachCadence` (1). Fixtures no longer reach a send. | S8 (verified send, replay suppression, canonical sender binding) has no passing unit evidence on `main`. Failures are fail-closed (nothing sent). S8 is already gated by its hard precondition. | No |
| R5 | S4 durability unit coverage broken by test fakes | `spec139` (3), `spec169` (4), `adr075TransactionalPersistence` (2), `spec140` (file), `spec138` (1): `PersistenceError: Unhandled SQL in amo memory pool: ALTER TABLE acquisition_mission_observations` — in-memory SQL fakes lag the schema. | S4 (durable recovery) relies on the real-PostgreSQL suites (`maxReliabilityRecoveryPostgres`, `p0ReviewOnlyAmendmentPostgres`, `p0LegacyMissionHydrationPostgres`), which pass; the unit-level persistence/hydration tests do not exercise code. | No |
| R6 | S5 discovery blocking classification drift | `spec175` (3: "available" path ends `blocked`; unavailable reason `existing_intelligence_unavailable` vs `External Discovery Capability Unavailable`; TME precondition does not throw without a provider), `spec185` (1: `/403|http_403/` not matched). The acquisition-mission run recorded 9 network attempts blocked by the offline harness, so part of this is inconclusive offline. | S5 must confirm live that discovery returns an explicit `blocked`/`outcome_unknown` with the provider reason, never synthetic. | No |
| R7 | Autonomous chaining expectation | `spec157AutonomousDiscoveryPolicy` → "approve plan via ask path with autonomous session chains to Scout" (strictEqual mismatch). | S6 progression behaviour differs from its spec test. | No |
| R8 | Contract hash is key-order sensitive | `hashStructuredMission` stringifies without key sorting; after a JSONB round trip the recomputed hash differs from the stored `contractHash` (observed in `test/p0LegacyMissionHydrationPostgres.test.js` development). No production code recomputes it today. | Any future verification of a persisted plan against its hash would falsely fail. | No |
| R9 | Legacy "approved" path on missions with no pending decision | On a mission with no pending decision (outside the review-only resting state PR #1127 guards), a bare "approved" can reach a legacy path that replies "Operator approved — Discovering" and records an approval contribution. | Truthfulness (S1/S7): a false progress claim and an approval record without a decision. | No |
| R10 | Max mission explanation / progress text | `test/acquisitionMission.test.js` (2): "answers why is this mission here…" (`/Mission exists because/` not matched); "routes progress inspection…" (`/Mission Progress/` not matched). | S1/S7 explanation wording. | No |

### Production-relevant outside S1–S9 (live outbound safety)

| ID | Evidence | Impact |
|---|---|---|
| P1 | `test/anchorPreparationBinding.test.js` (5) incl. "MoxiWorks incident is blocked at scrape, provider persistence, preparation and final candidate gate" | Contact-provenance gate for Anchor (tenant 10) preparation. Tenant 10 outbound is currently halted (`authorization_expired`). |
| P2 | `test/babrunOperatorDelegatedMigration.test.js` (2) incl. "program totalCap bounds cumulative sends after prior attempts" | Cumulative send-cap enforcement. |
| P3 | `test/regenerateAnchorCapacityRevision.test.js` (3): "Prepared outreach is ready but execution approval is not pending"; unhandled SQL in a revision fake | Capacity revision/approval state for Anchor. |
| P4 | `test/scoutCredibilityFramework.test.js` (1) "separates trust from confidence" | Scout evidence scoring. |

### Not P0-relevant / environmental

- `spec249PennyPaidAcquisition` (7): paid-media (Penny) feature; prohibited on review-only missions; not in S1–S9.
- `amo.test.js` (2), `spec153WorkspaceMode` (1, "Acquisition approach decision is required before Prepare"), `spec153DiscoveryCoverage` (1), `anchorFreshMissionAudit` (1), `auditDiscoveryFixes` (2): lifecycle/presentation fixture drift.
- Environmental (pass 77/77 when run as a non-root user without the offline harness): `spec250OperatorJudgmentCanonical`, `spec251OperatorJudgmentCommit`, `anchorScopedResumeCrmPreparationAdversarial`, `anchorScopedResumeActionRoute`.

### Low

- Region answers starting with an auxiliary verb ("Will County IL", "May County") are no longer captured as answers (introduced with the question-opener rule in PR #1127; safe direction).
- "Post Falls ID" as a region answer reads as a content command and is refused when content is prohibited (safe direction).
- Constraints-only amendments do not bump the mission version (pre-existing convention; reopening amendments now do).
