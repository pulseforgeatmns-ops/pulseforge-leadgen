# P0 Max Reliability — Production Acceptance Procedure

Status: **prepared, not executed.** Companion to `docs/SPEC-MAX-RELIABILITY-004.md`.

This procedure produces the production evidence required before the P0 can be
declared resolved. It is repeatable and non-destructive: it uses a dedicated
acceptance tenant, never touches Anchor (tenant 10), MSHI or other live client
data, never modifies Railway configuration, and its only external effects are
(a) bounded live Scout discovery and (b) one email to an operator-owned,
pre-approved test recipient.

---

## 0. Authorizations required before starting

Each item is a separate, explicit human decision. Do not start until every box
is recorded (who, when) in the run log.

| # | Authorization | Owner |
|---|---|---|
| A1 | PR #1065 reviewed and merged; deployment of that commit approved | Jacob |
| A2 | Acceptance tenant creation via `/admin/clients` (SPEC-114) — name `P0 Acceptance`, `enabled_agents=['max']` | Admin |
| A3 | Live Scout discovery for the acceptance tenant, spend cap ≤ **$2** Google Places for the run. The cap is enforced by the existing **provider-wide** `api_spend_policies` row for Places (`daily_usd_limit`), not per tenant — see §1.3 | Jacob |
| A3b | Only if §1.3 shows insufficient headroom control: a temporary change to the Places `daily_usd_limit` (production configuration mutation; affects every tenant, including Anchor research) and its restoration afterwards | Jacob |
| A4 | One outbound email to the approved test recipient (operator-owned mailbox, recorded address), from the acceptance tenant's canonical sender — **only** under the S8 hard precondition | Jacob |
| A5 | Read-only schema checks (§1.2, §1.3) on production PostgreSQL by an operator with DB access | Admin |
| A6 | One redeploy/restart of the **same** SHA for S4 | Jacob |

Abort immediately on any unexpected external effect, any send to a non-test
address, or any spend beyond A3. Record the abort and stop.

---

## 1. Preflight (read-only)

### 1.1 Deployment identity
- Railway deployment shows the merged commit SHA of PR #1065.
- `GET /api/status` → 200.
- Server log contains **no** `[missionRuntime] mission persistence unavailable — failing closed`.
  If it does, stop: missions are unavailable by design (fail closed); fix the
  database before continuing.

### 1.2 Schema (A5 — read-only `SELECT`s, no DDL)
```sql
SELECT column_name FROM information_schema.columns
 WHERE table_name = 'prospects'
   AND column_name IN ('ao_current_status','ao_next_action','ao_disqualification_reason');
-- expect 3 rows (migration 2026-09-28-ao-crm-001 applied; required by #1064)

SELECT to_regclass('acquisition_missions'), to_regclass('acquisition_mission_contributions'),
       to_regclass('acquisition_mission_execution_audit'),
       to_regclass('missions'), to_regclass('mission_audit_events');
-- expect all non-null. Mission persistence now FAILS CLOSED: if startup
-- schema initialization cannot complete, missions are unavailable (no
-- silent in-memory fallback). Existing tables mean startup DDL is a no-op.

SELECT count(*) FROM acquisition_missions WHERE id IS NULL;   -- expect 0
```
If the AO CRM columns are missing, **stop**: #1064's working-eligibility query
would fail in production. Applying the migration is a separate operator
decision, outside this procedure.

### 1.3 Places spend control (A5 read-only; A3b only if authorized)
```sql
SELECT provider, paused, daily_usd_limit, daily_request_limit, monthly_usd_limit
  FROM api_spend_policies WHERE provider = 'google_places';

-- Ledger snapshot (run before S5 and after S9; spend = difference):
SELECT count(*) AS calls, COALESCE(sum(COALESCE(estimated_usd, reserved_usd)), 0) AS usd,
       count(*) FILTER (WHERE tenant_id = :T) AS tenant_calls
  FROM api_usage_events
 WHERE provider = 'google_places'
   AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
```
- Record the row. `reserveApiCall` refuses any Places call that would exceed
  `daily_usd_limit` (shared across all tenants for the UTC day).
- Record today's Places ledger total before S5 and after S9. The run's spend is
  the difference and must be ≤ A3.
- If there is no limit, or the remaining headroom exceeds A3, the cap is only
  procedural. Either stop and obtain A3b, or accept a procedural cap explicitly
  in the run log (Jacob). Never change the policy without A3b.

### 1.4 Acceptance tenant (A2)
- Create via `/admin/clients`; record tenant id `T`.
- Configure the canonical sender for `T` (sender email, name, verified sending
  domain) per the existing tenant outreach setup. Record the identity.
- Confirm `GET /api/v1/tenant/context` returns `T` after activation.

All later steps run as a signed-in **admin** with `T` active in the shell
client selector. Record every request id / mission id / transaction id.

---

## 2. Scenarios

Each scenario lists the action, the expected result, and the evidence to
capture. A scenario passes only when **every** expected item is observed.

### S1 — Existing-workflow continuation
1. In Max (`/api/v1/max/workspace/ask`), paste a preparation-only canary
   request ending with *"Do not create a mission. Do not launch, execute,
   approve, print, or mail anything."* (use the fixture text from
   `packages/max/workspace/tests/missionRouting.test.js`, `CANARY_WORK_ORDER_PROMPT`,
   with neutral company names).
2. Follow up with *"Convert this to a fillable table."*
3. Follow up with *"Start a new mission for dental offices in Nashua NH."*

Expected:
- Turns 1–2: `workspaceOwnership.owner = active_mission`, no mission created
  (`GET /api/v1/amo/missions` count unchanged), table returned.
- Turn 3: `owner = mission_creation` (`explicit_new_objective` precedence) —
  a mission is created or a clarification requested; no generic briefing text.

Evidence: the three response bodies (`workspaceOwnership`, `routingTrace`), mission list before/after.

### S2 — Review-only mission restrictions
1. Ask Max: *"Set up a review-only contact intake mission for an existing
   customer. No outreach, calls, content, or campaign activation."*
2. `GET /api/v1/amo/missions/:id/authorization`.
3. Approve the plan, then ask *"Approve discovery."* and *"Send the emails."*
4. `POST /api/v1/amo/missions/:id/execute` with `{"intent":"EXECUTE_OUTBOUND"}`.

Expected:
- Mission created; authorization shows `reviewOnly: true` and prohibits
  `provider_research, outreach, outreach_authorization, paid_media, content`.
- Step 3: explicit "Not executed … forbids …" turns; approval not consumed;
  no Scout call (no Places spend recorded).
- Step 4: HTTP **403** `authorization_constraint_blocked`.

Evidence: authorization JSON + hash, response bodies, Places meter unchanged.

### S3 — Governed amendment
1. On the S2 mission, as a **manager** account: `POST …/authorization/amendments`
   with `{"lift":["outreach"], …}` → expect **403** `authorization_amendment_forbidden`.
2. As admin, without step-up → expect **403** `fresh_confirmation_required`.
3. As admin: `POST /api/v1/auth/fresh-confirmation` with
   `purpose: "mission_authorization_amendment"`, then amend with
   `{"add":[],"lift":["content"],"reason":"Acceptance: allow drafts",
   "expectedConstraintsHash":"<from S2>","amendmentId":"p0-acc-<date>-1"}`.
4. Repeat step 3's amendment body (new step-up) → expect 200 `alreadyApplied: true`.
5. Re-send step 3 with the **old** hash and a new `amendmentId` → expect **409**
   `authorization_amendment_stale`.

Expected: only `content` lifted; `revision: 1`; one operator `constraints`
contribution with `approvedBy.role = admin`; external classes still prohibited.

Evidence: responses, `GET …/authorization` before/after.

### S4 — Durable mission recovery
1. Record the S2 mission's `GET /api/v1/amo/missions/:id` (stage, version,
   authorization hash).
2. Trigger one service restart (redeploy of the **same** SHA — A6; no
   configuration change).
3. Re-read the mission.

Expected: identical stage, version, `authorizationConstraints` (incl. the S3
amendment) and contributions. No `failing closed` log line.

### S5 — Real Scout discovery, no synthetic fallback (A3)
1. Create a mission in `T` with a narrow, real objective (e.g. one vertical,
   one city, target count ≤ 10). Approve plan, approve discovery.
2. Do **not** pass `allowFixtureFallback`.

Expected:
- `execution.state` is `completed_verified` (live results with a discovery
  record) **or** an explicit `blocked` / `outcome_unknown` with the provider
  reason — never `synthetic_result`.
- `GET /api/v1/amo/missions/:id` → mission has **no** `syntheticEvidence`;
  the discovery contribution has no `provenance: synthetic_fixture`.
- Places spend (ledger difference, §1.3) ≤ A3 cap.

### S6 — Specialist dispatch and verified outcomes
Continue the S5 mission through prioritization, Paige variants and Emmett
capacity (approve each step explicitly).

Expected per step: `execution.state = committed_internal` with a
`transactionId`; Max prose names the step; no "completed" language for any
step whose state is not verified.

### S7 — Explicit failure reporting
1. With no mission selected, ask *"Approve discovery."*
2. Ask *"Run Scout discovery now."* in a fresh workspace for `T` with no mission.

Expected: "Not executed: there is no mission in this workspace…" with
`metadata.routingFallback.executionExpected = true`. No generic briefing text.

### S8 — Authorized outbound send to the approved test recipient (A4)
**Hard precondition — otherwise S8 is NOT RUN and recorded as blocked.**
The system has no recipient allowlist: `maxSends: 1` sends the first item of
the prepared queue. After live S5 discovery that queue will normally contain
real businesses. S8 may proceed only if the execution review that will be
bound by the approval lists **exactly one** recipient and it is byte-for-byte
the A4 test address. Two operators confirm this from
`GET /api/v1/amo/missions/:id` (`executionReview.targets`) before approving.
If any other address is present, stop: running S8 safely then requires a
separately authorized mechanism (out of scope for this PR).

Additional preconditions: no `syntheticEvidence` on the mission; canonical
sender for `T` resolves (no `canonical_sender_*` block).
1. Approve execution (operator approval) explicitly.
2. `POST …/execute` `{"intent":"EXECUTE_OUTBOUND","payload":{"maxSends":1}}`
   (the router reads `payload.maxSends`).
3. Replay the same request.

Expected:
- Step 2: `execution.state = completed_verified`, `verification.levelAchieved =
  provider_accepted`, one `providerMessageId`; the test mailbox receives exactly
  one email from the canonical sender.
- Step 3: `already_executed`; no second email.
- Brevo webhook events for the message correlate to the mission execution record.

### S9 — Synthetic isolation (staging-equivalent, no external effect)
On a throwaway mission in `T`, call `POST …/execute` with
`{"intent":"APPROVE_DISCOVERY","allowFixtureFallback":true}` **only if** live
discovery returns zero results (otherwise skip and record "not reachable").

Expected: `execution.state = synthetic_result`; mission stamped
`syntheticEvidence`; subsequent `APPROVE_EXECUTION` → **403**
`synthetic_evidence_blocked`; no CRM rows created for the tenant.

---

## 3. Exit criteria

- S1–S7 pass; S8 passes **or** is recorded as blocked by its hard precondition
  (then P0 closure needs an explicit decision on outbound evidence); S9 passes
  or is recorded as not reachable.
- No unexpected external effect; total Places spend ≤ A3; exactly one email sent.
- Run log with every id, response body and timestamp attached to PR #1065.

Only then may the P0 be considered for closure, by human decision.

## 4. Cleanup (non-destructive)

- Leave the acceptance tenant and its missions in place as evidence; set
  `enabled_agents=['max']` (unchanged) and do not delete rows.
- Revoke nothing that A1–A5 did not grant.
