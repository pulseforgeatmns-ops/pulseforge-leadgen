# P0 Max — Response Integrity and Release Gates

Stacked on PR #1065. Nothing here was merged, deployed, or run against production.

## 1. Root cause: where "Acknowledged." came from

`Acknowledged.` comes from **one place only**:
`packages/max/workspace/SessionConfigurationAcknowledgement.js` (pre-fix lines 19 and 22).
No model completion or provider error path returns that string.

How a request to run work got there:

1. `SessionDirectiveRegistry.js:136-150` (`execution_normal`) treats execution commands as a
   *session setting* (execution policy = normal): "let's run / launch / proceed / approve",
   "go ahead and execute", "resume execution", "enable execution", "stop theorizing".
   `execution_autonomous` (lines 124-134) does the same for "execute autonomously". All are `standalone: true`.
2. `OperatorObjectiveResolutionEngine.js:305` turns any interpretable session setting into a
   `WORKSPACE_OPERATION` candidate. If no higher-priority objective matches, that candidate wins.
   This happens always when no mission is active, and even with an active mission for
   "Let's launch", "Go ahead and launch the campaign", "Enable execution" and "Stop theorizing".
3. `WorkspaceEngine.js:627-633` bypasses reasoning, ownership, the mission runtime and the execution router.
4. The default execution policy is already `normal` (`SessionState.js:57`), so `changed === false`
   and the reply was exactly **`Acknowledged.`**: HTTP 200, no specialist dispatched,
   nothing executed, and nothing in the response says so.
5. `PresentationEngine.js:130` passes that string through verbatim and never calls the model.

Reproduced on the PR #1065 head (`f49e4fe`). Each phrase returned "Acknowledged." and nothing was routed:

| Operator text | No mission | Active mission |
|---|---|---|
| Let's run it / Go ahead and run Scout / Let's proceed / Go ahead and approve / Resume execution / Execute autonomously | Acknowledged. | routed to the mission |
| Let's launch / Go ahead and launch the campaign / Enable execution / Stop theorizing | Acknowledged. | **Acknowledged.** |

## 2. Other paths that answer without a successful model completion

| Path | Location | Before | After |
|---|---|---|---|
| Provider auth (401/403), rate limit (429), overload (5xx/529), timeout, network, spend-guard refusal | `PresentationEngine.js` `present()` catch | Silent deterministic prose. `metadata.presentationError` carried the **raw provider message** to the client | `metadata.modelCompletion = {attempted, ok:false, errorCode, action}`. Error code sanitized. A log line with the code and HTTP status only |
| No timeout on the presentation call | `PresentationEngine._presentWithClaude` | SDK default of 10 min. SDK retries are already off (`utils/meteredAnthropic.js:19`) | 30 s timeout. A timeout is reported as `timeout` |
| Model not configured or disabled | `routes/maxWorkspace.js:235-246`, `PresentationEngine` | `presentation:'fallback'` only | `modelCompletion.attempted:false`, `errorCode:'model_not_configured'` or `'model_disabled'` |
| Routing fallback when execution was expected | `RoutingFallbackIntegrity.js` (PR #1065) | Already an explicit "Not executed" reply | Unchanged. Its detector is reused here |
| Missing execution evidence | `AcquisitionMissionExecution.applyExecutionTruth`, `ExecutionEvidence.js` (PR #1065) | Already separates internal commit from provider evidence | Unchanged |
| Route exception | `routes/maxWorkspace.js:222-232` | Sanitized 500 that says "No review or action is confirmed" | Unchanged |

Important: the Max workspace **answer is always deterministic**. The model only rephrases a verified structured object.
So an Anthropic failure never invents content. Before this change it simply could not be detected.

## 3. Patch (minimal, no schema, no new config)

- `SessionConfigurationAcknowledgement.js`: when the operator's affirmative (non-negated) words command execution, reply
  `Not executed: this message was handled as a session setting, so nothing was run. No specialist was dispatched …`.
  The reply names the resulting execution policy and how to actually run work. An unchanged pure setting now reads
  `Acknowledged. No session settings changed. Nothing was executed.`
  Every session-configuration turn carries `executionState:'not_executed'` and `executionVerified:false`.
- `RoutingFallbackIntegrity.js`: exports `detectExecutionCommand` (owner-independent detector, same rules as before).
- `WorkspaceEngine.js`: passes `question` into the builder. This is a one-line change.
- `PresentationEngine.js`: sanitized `classifyModelError`, `modelCompletion` metadata, 30 s timeout.

Classification and routing precedence are unchanged. These messages still never dispatch.
They now say so instead of looking like acceptance.
Tenant scoping, session isolation, authorization and audit paths are not touched.

Regression tests: `packages/max/workspace/tests/p0MaxResponseIntegrity.test.js`, 28 tests.
26 of them fail on the pre-fix code.

## 4. Release-gate matrix

**A** = safe now: no external side effects (CI or local, disposable PostgreSQL).
**P** = needs explicit production authorization (IDs as in `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md`).

| # | Gate | A: offline evidence (this branch) | P: production check still required | Status |
|---|---|---|---|---|
| 1 | Max real model response and routing | `test:max:unit` 1709/1709; `test:max` 1728/1728; fixtures 20/20; response-integrity 28/28 | One authenticated workspace turn per path, as admin on the acceptance tenant: (a) a question, where `metadata.modelCompletion.ok === true` and `presentation === 'claude'`; (b) "Go ahead and launch the campaign", which must return `Not executed:`; (c) S1 continuation; (d) S7 failure reporting. Confirm `MAX_WORKSPACE_MODEL` / default `claude-sonnet-4-6` is served for this key. The operator's 200 proves auth only. Paige/Mira 401s were logged on 11-05 and 11-06 | **Blocked (A1 + A2)** |
| 2 | Mission persistence and recovery | `test:max:expectations:postgres` 44/44 (incl. `maxReliabilityRecoveryPostgres`); PR id-collision tests | Read-only schema check §1.2 (A5); S4 restart recovery (A6) | **Blocked (A5, A6)** |
| 3 | Cross-tenant isolation | `tenantWorkspace`, `audit022WorkspaceAskIdentity`, P0 readiness/gaps/recovery: 95/95; spreadsheet cross-tenant PG and AO routing PG pass; new session-isolation test | S1–S3 on the acceptance tenant only, with Anchor (10) and MSHI (2) state unchanged before and after | **Blocked (A1, A2)** |
| 4 | Autonomous prospect replenishment | 5 offline replenishment suites 48/48; Anchor P0 and reliability PostgreSQL suites pass | S5 live Scout discovery with no synthetic fallback; check `replenishment.status` truthfulness (A3, ≤ $2) | **Blocked (A3)** |
| 5 | Authorized outbound execution and evidence | `p0MaxReliabilityGaps` evidence states, `anchorDailyOutbound` pass. `spec071ExecuteOutbound` fails 7/12 **identically on PR head and this branch** (stale `test:amo` fixture set, not run by CI) | S6 dispatch evidence; S8 single send to the approved test recipient only if the queue holds exactly that recipient (A4). No recipient allowlist exists, so S8 is expected to be recorded as blocked or waived | **Blocked (A4)**; S8 likely waived |
| 6 | Google Places spending controls | `apiUsageControl` and Places cost/failure/savings, plus research-only grant: 74/74; `apiUsageControlPostgres` 6/6 (disposable DB) | Read `api_spend_policies` row for `google_places` (§1.3). **No row = no limit.** Record ledger before S5 and after S9 | **Blocked (A5)**; A3b if no row |

## 5. Recommendation

- **Code (PR #1065 + this patch): GO for review.** All offline and PostgreSQL gates are green. No new schema, config or side effects.
- **Deploy: NO-GO** until the PR #1065 checklist §2 "GO for deploy" items hold. Merging deploys immediately (`checkSuites:false`).
  Gate 1 needs one production Max turn showing `modelCompletion.ok === true`, so the HTTP 200 is proven for Max itself.
- **P0 closure: NO-GO** until S1–S7 pass in production (S8 passed or waived, S9 passed or recorded as unreachable).

Nonblocking (not changed, feature freeze): `routes/maxWorkspace.js` `/open` returns raw `err.message` on 500.
`/api/max/ask` (legacy) was not reviewed in this pass.

---

## 6. Follow-up: execution-intent routing

The commit after `9829760` covers this.

### Root cause
1. `SessionDirectiveRegistry.js` `execution_normal` has standalone aliases. Some are real settings
   ("enable execution", "stop theorizing"). Others are **operational commands**
   ("let's / go ahead and run | launch | proceed | approve | execute", "time to execute").
2. `MessageTypeClassifier.isSessionConfigurationMessage` treated any interpretable directive as configuration.
   It had no carve-out for commands.
3. `OperatorObjectiveResolutionEngine.extractSegmentCandidates` then:
   - suppressed the `MISSION_EXECUTION` candidate whenever the text was "session configuration" and no mission was active
     (`!(isSessionConfigurationMessage && !hasActiveMission)`);
   - never produced it for "launch the campaign", even with an active mission, because `launch` / `campaign` are not in
     `MISSION_EXECUTION_COMMAND_RES`.
4. With `WORKSPACE_OPERATION` as the only candidate, the turn took the session-setting bypass.

A second defect was hidden behind the first. `PendingDecisionResolver` treats `go ahead` / `proceed` / `send` as "yes"
to **whatever** decision is pending. So once routed, "Go ahead and send the emails" would have approved a pending
*plan* or *discovery* decision.

### Correction (no schema, no config, no new execution path)
| Case | Behaviour | Where |
|---|---|---|
| 1. Explicit request | `isOperationalExecutionCommand` (no session scope, no reset) is never session configuration. With an active mission it binds to `MISSION_EXECUTION`, which reaches the existing pending-decision resolver → CER → `routeExecutionRequest` → TME, with authorization constraints and audit unchanged. With no mission it gets an explicit "Not executed: there is no mission…" (the PR #1065 routing-fallback guard, extended to these commands) | `SessionDirectiveRegistry.js`, `MessageTypeClassifier.js`, `OperatorObjectiveResolutionEngine.js`, `RoutingFallbackIntegrity.js`, `WorkspaceEngine.js` |
| 2. Contextual approval | Resolves only against the pending decision. If the words **name** an action (outbound / discovery / plan / prioritization / investigation) that does not match the pending kind, the decision is not approved: "Not executed: you asked for X, but that is not the decision waiting on you…" and the pending decision is re-asked. Generic approvals ("go ahead", "let's run it") behave as before. Plan approval also accepts "discovery", because it only advances to the discovery decision | `PendingDecisionResolver.js`, `PendingDecisionTurn.js` |
| 3. Real setting | Setting applied. "Acknowledged. Session settings updated. No operational work was executed." plus the snapshot. When unchanged, it says so | `SessionConfigurationAcknowledgement.js` |

**Behaviour change to review.** "Go ahead and launch the campaign" or "Go ahead and send the emails", sent while an
**execution approval** is pending, now reaches `classifyExecutionApproval`. It approves outbound exactly like
"Go ahead, send it" already did. Before, it was swallowed as a setting. That is the governed path working as designed,
but it is an outbound-gate behaviour change, and production acceptance S6/S8 must exercise it.

### Regression results (Node 22, no network)
- `p0ExecutionIntentRouting.test.js`: 31/31. 21 fail on `9829760`.
- `p0MaxResponseIntegrity.test.js`: 22/22 (updated for the new routing).
- `test:max:unit` 1734/1734; `test:max` 1753/1753; `test:max:fixtures` 20/20; `test:decision` 107/107.
- PostgreSQL 16 (disposable): `test:max:expectations:postgres` 44/44; `test:max:attention:postgres` 33/33.
- P0 readiness, gaps and recovery, tenant workspace, and Anchor daily outbound: 90/90.
- `test:amo` 498/544: the **identical** 46-failure set before and after. Pre-existing stale fixtures; not run by CI.

### Placement
This is a separate, narrowly scoped commit stacked on `9829760`. It cannot target `main` alone, because it depends on
#1065 (`RoutingFallbackIntegrity`, `NegatedDirective`, execution-evidence states). Merging #1065 deploys it,
so both commits should be folded into #1065 before that merge and reviewed as distinct commits.

---

## 7. Final authorization review (before integration into #1065)

### Findings
| Check | Result |
|---|---|
| Phrase scope | At an execution decision, "launch the campaign" or "send the emails" resolves only `APPROVE_EXECUTION`. That step never sends: the mission stays at READY, the prose says "Prepared — not sent", and chat never produces `EXECUTE_OUTBOUND`. "Run Scout", "approve the plan" and "approve prioritization" are refused as mismatched. A bare "Go ahead" authorizes nothing. At plan or discovery decisions, outbound wording never approves (section 6) |
| Preparation / authorization / sending | Preparation is Paige and Emmett contributions; it creates no approval. Authorization (`APPROVE_EXECUTION`) makes the prepared sends **eligible** (`ExecutionApproval.buildExecutionReview`: "become eligible for provider execution"). Actual sending is `EXECUTE_OUTBOUND` only, reached from the governed outbound adapter or an explicit API call. It requires a valid revision-bound approval, otherwise `tme_execution_not_approved` with zero sends. "Scheduling" and "activation" are the governed daily envelope / outbound program state, outside chat. Chat cannot set either |
| Tenant / mission / action / scope | Tenant: engine reads are tenant-scoped (`amo_tenant_mismatch`) for chat and API. Mission: the approval payload carries `missionId`, and the session's presented revision is per mission. Action: the CER intent comes from the resolved pending-decision kind (section 6 mismatch guard). Scope: the approval freezes `preparedArtifactRevision`, which hashes the Max/Paige/Emmett contribution ids, queue targets (first 25), queue count, governor outcome and sender. The approval also records `plannedSendCount` |
| **Gap found, fixed:** binding to the *reviewed* decision | The engine re-derives the pending execution decision on every contribution. So a "yes" was evaluated against whatever package was current at commit time, which could be one the operator never saw. **Fix:** every human-facing surface (chat, approval button, command deck, voice, API) must name the reviewed `expectedPreparedArtifactRevision`, and it is checked inside the TME stage. Chat supplies the revision this session last **rendered** (`## Execution Ready` block); a turn that only mentions the decision does not count. Missing → `tme_execution_approval_unreviewed`; changed → `tme_execution_approval_stale`. Both are rolled back, reported as `blocked`, and answered with "Not executed: … Nothing was approved or sent." plus the current package. The governed daily path keeps its own snapshot-hash binding |
| Stale reuse | A consumed approval stops counting once the package changes (`findValidExecutionApproval` is revision-bound), and `EXECUTE_OUTBOUND` is then refused. A refused approval leaves the pending decision intact |

Also fixed: a rolled-back execution approval used to be labelled "Discovery could not execute". It now reads "Execution approval".

### Known limits (nonblocking, unchanged)
- The revision hashes only the first 25 queue target ids (plus the total count).
- The REST caller still chooses `source`. Every human source now requires the revision. Direct in-process calls to
  `advanceExecutionAfterApproval` without an execution request (operator scripts such as `executeAnchorOneOutbound`)
  are unchanged and remain separately operator-approved.
- An execution request refused for a missing approval is classified `failed`, not `blocked`. It is still a refusal with zero sends.

### Regression results
- `p0ExecutionApprovalBinding.test.js` 16/16 (6 fail before the fix); `p0ExecutionIntentRouting` 31/31; `p0MaxResponseIntegrity` 22/22.
- `test:max:unit` 1750/1750; `test:max` 1769/1769; fixtures 20/20; `test:decision` 107/107.
- `test:amo` 498/544, the identical failure set before and after. 96 `test/` files that touch missions, approvals or
  outbound: identical results before and after (one flaky rerun confirmed). Two existing tests now pass the reviewed
  revision when they approve through the router (`adr067StageContracts`, `spec070ReadyExecutionApproval`).
- Acceptance S8 step 1 updated to approve with the reviewed revision.

---

## 8. Complete-content approval binding (`33ed2d9`)

### Finding
`computePreparedArtifactRevision` hashes the Max/Paige/Emmett contribution ids, the queue count, the **first 25** target ids,
the governor outcome and the sender email/domain. It does not hash recipient addresses, per-target sendable/DNC flags,
copy bindings, Paige copy content or the sender name. At send time, `buildExecutionBundle` reads the **live** queue and copy.
Any rewrite under the same contribution id therefore kept an existing approval: through `store.updateContribution`, or
through the durable `persistContribution` upsert `ON CONFLICT (id) DO UPDATE SET payload`. The rewrite could change target 26
onwards, any target's address or flags, or the copy. No current code path performs such a rewrite. The gap is real but
latent, which makes the fix defense in depth.

### Correction
- `computePreparedContentDigest`: a canonical digest (sorted keys at every depth, stable across JSONB round trips) of every
  queue item in send order (all positions, duplicates included), the full Paige artifact, the full sender identity and the
  governor outcome.
- New approvals store `preparedContentDigest`. `findValidExecutionApproval` and the send-time `verifyArtifactRevision` void
  an approval whose digest no longer matches.
- **The package revision id is deliberately unchanged.** Governed envelopes, Anchor recovery approvals, CRM-preparation
  receipts, evidence and schedules store and compare it. Changing its format would fail every existing governed envelope
  at deploy and leave every READY mission with a consumed approval SPEC-136-inconsistent (skipped on hydrate). Legacy
  approvals (no digest) keep revision-only semantics; they are listed by the pre-deploy inventory (checklist R.4).
- Review binding: human-facing approvals carry both `expectedPreparedArtifactRevision` and `expectedPreparedContentDigest`.

### Protected after the fix (new approvals)
Approved content (full Paige artifact and per-target copy binding), sender identity (email, domain, name), recipient scope
(every target, address, order, duplicates, sendable/DNC), send count (`queueCount`, `plannedSendCount`, plus the queue
itself). Tests: `p0PreparedArtifactRevision.test.js` (16).

### Remaining limits (not changed in the freeze)
- An in-place rewrite of a package **after** a new (digest-bearing) approval voids it, and the mission then fails SPEC-136
  consistency (READY, no pending decision, no valid approval). That fails closed (nothing can send), but it needs
  engineering recovery. No code path performs such a rewrite.
- Legacy approvals that exist before deploy keep the prefix-only binding until revised or re-approved.
- Pre-existing: send dedupe is keyed by `(mission, prospect, revision)`. After any **legitimate** revision (SPEC-215) and
  re-approval, the canonical EXECUTE path could send the first touch again to a prospect already reached under the earlier
  revision. The governed daily path tracks per-candidate sends separately. A mission-and-prospect guard was prototyped but
  not shipped, to keep the send path unchanged during the freeze. Recommended as the first post-release change.
- Missing-approval refusals are classified `failed`, not `blocked` (zero sends either way).
