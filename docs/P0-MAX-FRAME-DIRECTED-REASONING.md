# P0-MAX-REASONING-001 PR 5 — Frame-directed reasoning

Status: **implemented and tested on a branch. Not merged, not deployed, not enabled.**
- Default-off (`MAX_FRAME_REASONING`). Nothing in production imports `packages/max/frameReasoning/` (structural test), so the existing Max path is unchanged by construction.
- No migration. No production file is modified.

Definition of success: one path interprets an operational question, keeps conversational context, reads current tenant-scoped evidence, and answers truthfully without changing state or inventing facts.

## Architecture

```
validated TurnFrame ─► memory port (ConversationLedger, PR 3) ─► referent resolution
                                                                     │
          evidence port (EvidenceRegistry, PR 4) ◄─ enum domains + exact ids
                     │
                     ▼
             ReasoningResult ─► ReasoningPresentation ─► buildStructuredResponse (existing)
             (observed · calculated · inferred · contradictions · unknowns · proposals · execution status)
```

- **`FrameDirectedReasoner`** (`packages/max/frameReasoning/FrameDirectedReasoner.js`) is a small typed coordinator. It adds **no** classifier, planner, agent loop or tool calls. Every decision reads validated-frame fields, the memory's resolved focus, or the evidence report.
  - **Referents.**
    - Explicit frame referents (memory candidate ids) win.
    - A follow-up with no explicit referent uses the memory focus. A follow-up that names its own subject ("and what about Emmett?") needs none.
    - A correction with no explicit target moves to the earlier subject of the same kind only when exactly one exists. Otherwise it asks which.
    - A follow-up with nothing remembered asks; it never reuses stale context.
  - **Domains.** `frame.evidenceNeeds`, else a fixed subject→domain table, plus referent and facet additions. Enum-only, at most 4.
  - **Evidence.** One `retrieve({ tenantId, domains, referents })`, revalidating the remembered ids. The answer is **scoped to the resolved subject**: other records or missions read alongside it are not presented, and contradictions about them are dropped from this answer.
  - **Calculations** use observed timestamps and tiers only:
    - "most recently updated", ties shown;
    - priority by recorded engagement tier;
    - "fewer than the five asked for", with exclusion reasons.
  - **Inferences** come only from the diagnosis port. Each must cite a basis, and unsupported ones are dropped.
  - **Execution.** Never performed. A state-changing frame yields `not_executed`, or `held_by_operator` for a prohibition. The result names the requested action, the constraints (remembered plus same-turn), and the deterministic pathway that alone may act (pending-decision resolver, `routeExecutionRequest`, TME). Targets are still identified by reading.
  - **Ambiguity**, including conditional or hedged authorization, yields a targeted clarification with options that name the missions. Nothing is approved, consumed or executed.
  - **Refusals**: an invalid tenant, an invalid frame (authority claimed, or another tenant's frame) and any foreign-tenant frame are refused **before any read**.
- **`ReasoningPresentation`** is a deterministic renderer with separate sections: Observed (verified from current records), Calculated, Remembered references that could not be confirmed, Inferred (not verified), Contradictions (unresolved), Unknown or unavailable, Recommended next steps (proposals only), and the execution line.
  - An optional rephrasing presenter (`PresentationEngine` or a model) is **rejected** if its text claims a completed action that did not happen, or mentions ids or dates absent from the result. The deterministic text is then used.
  - Output goes through the existing `buildStructuredResponse`. `metadata.items` carries the turn's subject records in the shape the ledger adapter reads.

### Ports (contracts, no module dependency on PRs 3 and 4)

| Port | Shape | Provided by |
|---|---|---|
| memory | `{ focus: { referent: { id, kind, label, scope, memberIds, missionId }, requiresRevalidation }, view: { remembered: { focus[], constraints[] } }, referents[] }`. Referents may use `members` (normalized). | `ConversationLedgerRecorder.context(session)` plus `session.conversationLedger.referents` (PR 3) |
| evidence | `{ retrieve(req) → EvidenceReport, diagnose: { scout, emmett, rank } }` | `EvidenceRegistry` plus `EvidenceDiagnosis` (PR 4) |

## Tests and measured results (2026-11-18)

### PR 5 alone (on `main`)

- `packages/max/frameReasoning/tests/frameDirectedReasoner.test.js` — **22/22**, hermetic, with contract-conformant fakes of both ports. Covers:
  - scenarios A–F;
  - default-off, structural (no importer, no DB/router/execution calls), refusal before reads;
  - presenter rejection (completion claims, invented ids and dates);
  - failed, foreign or missing reports asserting nothing;
  - subject scoping; same-turn constraints; the `members`/`memberIds` port shapes.

### Combined tree (`main` + PRs 1–5), temporary worktree

PRs merged in order. The only conflicts were the known PR 1 / PR 2 pair: the eval script (PR 1's kept) and one `CLAUDE.md` sentence (both kept).

| Suite | Result |
|---|---|
| Cross-PR scenarios A–G (`packages/max/frameReasoning/integration/combinedArchitecture.integration.cjs`, disposable PG, synthetic data) | **9/9** (7 scenarios + read-only proof + suite) |
| All disposable-PG suites from PRs 1–5 (shadow ×2, context store, ledger cross-process, field-note correction, HTTP continuity, staging continuity, evidence registry, JEV decision shadow, cross-PR) | **38/38** |
| `test:max:interpretation` (PRs 1 + 2: eval harness, shadow readiness, contracts) | **81/81** |
| `test:max:semantic` | 9 pass, 1 todo (unchanged ratchet) |
| Frame reasoning hermetic | 22/22 |
| ConversationLedger, context, recovery and field-note memory | 52/52 |
| EvidenceRegistry hermetic | 12/12 |
| JEV `test:decision` | 115/115 |
| Authorization / pending-decision / mission continuity / execution binding / operational read and inspection | **576/576** |
| Lexical interpretation baseline | **unchanged**: 47.7% (44 cases), 53.2% (62 cases), same unsafe nominations, corpus sha256 `950eb659cd58…` |
| Max unit lane (combined tree) | **2,568 tests: 2,567 pass, 0 fail, 1 todo**. The first combined run found 1 failure: PR 1's eval-CLI fake-API test tripped the unit lane's offline network guard (`server.listen` with a host does a `dns.lookup`). It was fixed on PR 1 (#1131, `81e8ae7`) and re-run clean. |

The cross-PR suite proves every evidence statement was SELECT/WITH and that no business table hash changed beyond the test's own fixture edits.

**The 85% semantic interpretation promotion gate has NOT been evaluated.** No model-backed run has happened; the approved Sonnet 5.5 run is still waiting for the dev key in a new session. The scenario frames are hand-authored to represent validated model output. With only the lexical producer, which never marks corrections or subjects, most of these turns degrade to clarification, which is the safe direction.

### Evidence-grounded examples (verbatim from the combined run)

> **Operator:** Show me Levi's five strongest existing borrower prospects and explain what's missing.
> *Calculated:* 4 record(s) are open and meet the recorded outreach gates, fewer than the five asked for. Excluded: …2103 (opportunity closed); …2105 (opportunity expired). / Priority by recorded engagement tier: 1. …2101, 2. …2102, 2. …2106, 4. …2104.
> *Observed:* Record …2102: pipeline warm, setter new, opportunity open; contact channels: none recorded; … Missing: contact channel.
> *Contradictions (unresolved):* pipeline status "closed" is terminal but setter status "follow_up" is active.

> **Operator:** Which of those has the freshest information?
> Referring to 4 existing prospect records from this conversation, rechecked against current records. … *Calculated:* Most recently updated: record …2102.

> **Operator:** Get whatever verified contact information we can into those five records. Don't send anything.
> Not executed: I identified a request to update records but did not carry it out. Frame-directed reasoning is read-only; no records, missions, approvals or outreach were changed. Your constraints are recorded: no send. If you want this done, it goes through CRM writes via an authorized specialist through routeExecutionRequest, with explicit authorization.

> **Operator (Anchor):** And what about Emmett?
> *Observed:* Outbound gate mode: maintenance (recovery_kill_switch). Outbound program prog1: mode paused; authorization expired 2026-10-23.
> *Inferred (not verified):* The outbound gate is in "maintenance"; sending is blocked while it is. [based on outbound_authorization:outbound_gate:gate] …
> *Unknown:* emmett execution log: no emmett runs recorded in agent_log — absence of a log does not establish that emmett never ran.

> **Operator:** Look at the active mission. → Which one do you mean? Options: acquisition mission m_anchor_idle (r1); acquisition mission m_anchor_live (r2).
> **Operator:** No, the one we discussed earlier. → Mission m_anchor_idle: status active, classified stale (…40 days…). *Contradictions:* mission status "active" but no recorded activity for 40 days.

> **Operator:** Maybe approve it, but hold the emails until Monday. → I cannot tell safely what you want me to do. … Not executed: … nothing was approved, consumed or executed. (The mission's `plan_approval` is still pending in PostgreSQL.)

> **After a restart (recovered ledger), operator:** Which of those has the freshest information now?
> … Remembered references that could not be confirmed: prospect …2104: not found in tenant — … no replacement is inferred.

## Feature flags and rollback

| Flag | Effect | Rollback |
|---|---|---|
| `MAX_FRAME_REASONING` | Gate for future wiring of this path. Currently read by no production code. | unset |
| `MAX_CONVERSATION_LEDGER` (PR 3) | Ledger maintenance | unset |
| `MAX_CONVERSATION_CONTEXT` (existing) | Durable ledger and continuity | unset (rows expire in 2 h) |
| `MAX_INTERPRETATION_SHADOW` + `MAX_INTERPRETATION_MODEL` (PR 2) | Model frames in shadow | unset |

No database objects are added by PRs 3–5. PR 2's shadow table has its own rollback SQL.

## Known limitations and unresolved defects

1. **No model-backed evaluation yet.** The §10 gates (≥ 85% overall, ≥ 90% critical, p95 ≤ 1.5 s, ≤ 2% degraded, zero safety violations) are unmeasured. The lexical producer is 53.2% / 88.7% safety on the extended corpus.
2. **Not wired into `WorkspaceEngine`.** Wiring is the next authorized step, after PRs 3 and 4 merge (checklist below).
3. **Cross-PR interplay to tidy in PR 3.** A redundant scope correction ("No, I meant the existing prospects" when the focus already is existing) makes the ledger open an `unresolved_correction` question. The coordinator still reads the correct records. The ledger's stored referents use `members` while its focus uses `memberIds`; PR 5 normalizes at the boundary.
4. **Mission answers use deterministic templates**, not `OperationalMissionReasoning`. That composer works on the session's `operationalEvidence` shape. Unifying the two is a follow-up.
5. **The presentation guard is pattern-based.** It catches agentive and passive completion claims and ungrounded ids or dates. The deterministic text remains the default, and any presenter must pass the guard.
6. **Inherited from PR 4:** Scout acquisition state is not consulted (its store runs DDL on read). The Anchor outbound gate is a fixture in the PG tests.
7. **Legacy-path defects** recorded in the architecture doc are unchanged in production: §1.6 (hedged approvals resolve as approvals) and #1094 (exit vs cancel). This path answers scenario F with a clarification, but it is not active.
8. **Pre-existing failures on `main`**, unchanged: `test/aoAdminImpersonation.test.js` (composer actor source assertion) and the stale `implementedInDeployedCode` assertion in `test/maxOperationalReadiness.test.js`.

## Integration and production-acceptance checklist

1. Review and merge #1131 (eval harness), #1132 (shadow readiness), #1133 (ledger), #1134 (registry) and this PR, resolving the documented PR 1 / PR 2 conflict (PR 1's eval script).
2. Enable `combinedArchitecture.integration.cjs` in CI (Postgres job). It fails, never skips, without PRs 3 and 4.
3. Run the approved model-backed evaluation (`claude-sonnet-5-5`, ≤ $7, dev key) and record the §10 gate results. Do not promote below the gates.
4. Production schema review and approval: `max_interpretation_shadow` (PR 2), already-reviewed `max_conversation_context`, purge schedules.
5. Enable in shadow only, per tenant: `MAX_INTERPRETATION_MODEL` → `MAX_INTERPRETATION_SHADOW` → `MAX_CONVERSATION_LEDGER` (+ `MAX_CONVERSATION_CONTEXT`). Monitor two weeks: degraded ≤ 2%, truncated 0, model mismatch 0, p95 ≤ 1.5 s.
6. Wire `FrameDirectedReasoner` into `WorkspaceEngine` behind `MAX_FRAME_REASONING`, **read-only frames only** (question / investigation / hypothetical / correction), per tenant, shadow-rendered first. Legacy owners keep every other turn. The existing routing stays the default.
7. Promotion to answer operators needs the §10 gates, independently reviewed disagreements, two weeks of shadow agreement, and green P0 suites. Any execution integration later must go through two-key authority (frame narrows only) and the unchanged `routeExecutionRequest` / TME / `AuthorizationConstraints` / SPEC-136 checks.
