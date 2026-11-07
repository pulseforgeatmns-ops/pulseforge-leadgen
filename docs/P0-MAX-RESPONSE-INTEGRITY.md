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
