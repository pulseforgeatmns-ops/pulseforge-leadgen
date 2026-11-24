# test:amo known-failure register (MAX-P0-002)

**Status:** tracking register, not an acceptance waiver.

The baseline in `test/baselines/amo-known-failures.json` is a **temporary regression control**. CI fails on any failure outside it, so existing failures cannot spread, but they are **not accepted**. Rules:

1. **S1 (safety-critical execution, authorization or routing) must be resolved before P0 closure.** Resolved means the test passes on real code paths. Rewriting a test to match a defect does not count.
2. **S2 (safety-relevant) must be resolved, or individually dispositioned by executive decision, before P0 closure.** Durability items count as S2 because a broken fake means the safety behaviour is *not being tested at all*.
3. S3 (presentation, fixture drift, out-of-scope features) may close after P0, but each entry needs an owner and a target.
4. When a fix lands, remove the entry from the baseline in the same PR; the ratchet reports fixed entries. Adding an entry needs explicit review in the PR description.
5. The baseline was produced on the deployed commit `357e747` (+ MAX-P0-002 changes), offline harness, no `DATABASE_URL`. It is deterministic across repeated runs.

Classification legend:
- **S1** safety-critical: send verification, sender/message/approval binding, authorization, runtime-boundary routing.
- **S2** safety-relevant: autonomy, truthfulness of blocked/unavailable states, masked durability coverage, unguarded external calls.
- **S3** non-safety: presentation, fixture drift, features out of P0 scope (must stay disabled).

Node:test lists a failing suite and its failing subtest as separate keys, so one defect can appear as two rows.

**Counts:** S1 21 · S2 27 · S3 24 · unclassified 0 · total 72

| ID | Class | Category | Suite | Failing test (node:test path) | Meaning |
|---|---|---|---|---|---|
| AMO-001 | S2 | Durability coverage masked | `adr075TransactionalPersistence` | ADR-075 — Transactional Persistence Exclusivity | Transactional persistence exclusivity not exercised (R5) |
| AMO-002 | S2 | Durability coverage masked | `adr075TransactionalPersistence` | ADR-075 — Transactional Persistence Exclusivity > plan and discovery TME commits keep contributionIds aligned with durable store | Transactional persistence exclusivity not exercised (R5) |
| AMO-003 | S2 | Durability coverage masked | `adr075TransactionalPersistence` | ADR-075 — Transactional Persistence Exclusivity > runtime.create persists exclusively through persistStageCommit bundle | Transactional persistence exclusivity not exercised (R5) |
| AMO-004 | S3 | Presentation / fixture drift | `amo` | SPEC-118 health and blockers | Mission health and timeline presentation (SPEC-118) |
| AMO-005 | S3 | Presentation / fixture drift | `amo` | SPEC-118 health and blockers > answers mission health instead of a vague outreach status | Mission health and timeline presentation (SPEC-118) |
| AMO-006 | S3 | Presentation / fixture drift | `amo` | SPEC-118 timeline | Mission health and timeline presentation (SPEC-118) |
| AMO-007 | S3 | Presentation / fixture drift | `amo` | SPEC-118 timeline > records every meaningful event in chronological order | Mission health and timeline presentation (SPEC-118) |
| AMO-008 | S3 | Presentation / fixture drift | `anchorFreshMissionAudit` | Anchor fresh mission audit | Anchor fresh mission audit fixture |
| AMO-009 | S3 | Presentation / fixture drift | `anchorFreshMissionAudit` | Anchor fresh mission audit > 8d — production Scout path: Places location-only rows do not hard-block discovery_evidence | Anchor fresh mission audit fixture |
| AMO-010 | S3 | Presentation / fixture drift | `auditDiscoveryFixes` | AMO discovery audit fixes | Discovery audit fixture |
| AMO-011 | S3 | Presentation / fixture drift | `auditDiscoveryFixes` | AMO discovery audit fixes > buildDelegationFromAmoMission binds immutable mission objective | Discovery audit fixture |
| AMO-012 | S3 | Presentation / fixture drift | `auditDiscoveryFixes` | AMO discovery audit fixes > infers target segment from mission objective only | Discovery audit fixture |
| AMO-013 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-014 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > EXECUTE_OUTBOUND CER routes through ExecutionRouter to outbound adapter | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-015 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > EXECUTE_OUTBOUND maxSends=1 sends one queue item and replay is suppressed | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-016 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > Paige integrity: subject/body originate from Paige contribution | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-017 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > idempotency: same missionId + prospectId + revision sends once | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-018 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > provider failure persists explicit failed result without false sent state | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-019 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > provider success persists mission-bound execution evidence | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-020 | S1 | Safety-critical execution | `spec071ExecuteOutbound` | SPEC-071 — Canonical EXECUTE Outbound Adapter > stage entry: READY + valid EXECUTION_APPROVAL → EXECUTE | Verified send, provider evidence, maxSends, replay suppression, idempotency (R4) |
| AMO-021 | S1 | Safety-critical authorization | `spec135` | SPEC-135 — Mission Planning Gate Before Discovery | Discovery wording approves a pending plan (R2) |
| AMO-022 | S1 | Safety-critical authorization | `spec135` | SPEC-135 — Mission Planning Gate Before Discovery > discovery does not execute on ask path before plan is locked | Discovery wording approves a pending plan (R2) |
| AMO-023 | S2 | Durability coverage masked | `spec138` | SPEC-138 — production reset | In-memory SQL fake lags schema (R5) |
| AMO-024 | S2 | Durability coverage masked | `spec138` | SPEC-138 — production reset > deleteAllAmoData clears all AMO tables in FK-safe order | In-memory SQL fake lags schema (R5) |
| AMO-025 | S2 | Durability coverage masked | `spec139` | SPEC-139 — Transactional Durable Mission Persistence | In-memory SQL fake lags schema; durability not exercised (R5) |
| AMO-026 | S2 | Durability coverage masked | `spec139` | SPEC-139 — Transactional Durable Mission Persistence > assertPersistedMatchesEngine rejects mismatched durable snapshots | In-memory SQL fake lags schema; durability not exercised (R5) |
| AMO-027 | S2 | Durability coverage masked | `spec139` | SPEC-139 — Transactional Durable Mission Persistence > discovery approval persists and survives hydration | In-memory SQL fake lags schema; durability not exercised (R5) |
| AMO-028 | S2 | Durability coverage masked | `spec139` | SPEC-139 — Transactional Durable Mission Persistence > plan approval persists and survives hydration | In-memory SQL fake lags schema; durability not exercised (R5) |
| AMO-029 | S2 | Unguarded external call | `spec140` | packages/acquisition-mission/tests/spec140.test.js | Subtests pass; code under test makes 1 unexpected network attempt (runtime ownership path) — investigate |
| AMO-030 | S3 | Presentation / fixture drift | `spec153DiscoveryCoverage` | SPEC-153 — Discovery Coverage Engine | Discovery coverage fixture |
| AMO-031 | S3 | Presentation / fixture drift | `spec153DiscoveryCoverage` | SPEC-153 — Discovery Coverage Engine > normalizes discovery payload with coverage report fields | Discovery coverage fixture |
| AMO-032 | S3 | Presentation / fixture drift | `spec153WorkspaceMode` | SPEC-153 — Mission Workspace Modes | "Acquisition approach decision is required before Prepare" |
| AMO-033 | S3 | Presentation / fixture drift | `spec153WorkspaceMode` | SPEC-153 — Mission Workspace Modes > Test 3: mission in execution → EXECUTION mode with execution controls | "Acquisition approach decision is required before Prepare" |
| AMO-034 | S2 | Safety-relevant autonomy | `spec157AutonomousDiscoveryPolicy` | SPEC-157 — Autonomous Discovery Approval Policy | Autonomous chaining differs from spec (R7) |
| AMO-035 | S2 | Safety-relevant autonomy | `spec157AutonomousDiscoveryPolicy` | SPEC-157 — Autonomous Discovery Approval Policy > Test 5: approve plan via ask path with autonomous session chains to Scout | Autonomous chaining differs from spec (R7) |
| AMO-036 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification | In-memory SQL fake lags schema (R5) |
| AMO-037 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 1 — Mission Plan approval | In-memory SQL fake lags schema (R5) |
| AMO-038 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 1 — Mission Plan approval > canonical projections are identical after plan approval | In-memory SQL fake lags schema (R5) |
| AMO-039 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 2 — Discovery approval | In-memory SQL fake lags schema (R5) |
| AMO-040 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 2 — Discovery approval > canonical projections are identical after discovery approval | In-memory SQL fake lags schema (R5) |
| AMO-041 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 3 — Prioritization approval | In-memory SQL fake lags schema (R5) |
| AMO-042 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 3 — Prioritization approval > canonical projections are identical after prioritization approval | In-memory SQL fake lags schema (R5) |
| AMO-043 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 5 — Structured projection diff on mismatch | In-memory SQL fake lags schema (R5) |
| AMO-044 | S2 | Durability coverage masked | `spec169` | SPEC-169 — Canonical Mission Verification > Scenario 5 — Structured projection diff on mismatch > returns first divergent field, memory value, persisted value, and reason | In-memory SQL fake lags schema (R5) |
| AMO-045 | S1 | Safety-critical routing | `spec170MissionRuntimeOwnership` | ADR-089 / SPEC-170 — Mission runtime ownership boundaries | Cross-runtime Scout sync not rejected (R3) |
| AMO-046 | S1 | Safety-critical routing | `spec170MissionRuntimeOwnership` | ADR-089 / SPEC-170 — Mission runtime ownership boundaries > Scout.discover rejects cross-runtime sync when Mission Engine is supplied for AMO mission | Cross-runtime Scout sync not rejected (R3) |
| AMO-047 | S2 | Safety-relevant truthfulness | `spec175` | SPEC-175 — External Discovery Capability | Discovery blocked/unavailable classification drift (R6) |
| AMO-048 | S2 | Safety-relevant truthfulness | `spec175` | SPEC-175 — External Discovery Capability > Scenario 1: Google Places available — candidate universe populated, discovery continues | Discovery blocked/unavailable classification drift (R6) |
| AMO-049 | S2 | Safety-relevant truthfulness | `spec175` | SPEC-175 — External Discovery Capability > Scenario 2: Google Places unavailable — mission pauses before discovery execution with capability blocker | Discovery blocked/unavailable classification drift (R6) |
| AMO-050 | S2 | Safety-relevant truthfulness | `spec175` | SPEC-175 — External Discovery Capability > Scenario 6: TME precondition gate blocks discovery when no external provider is configured | Discovery blocked/unavailable classification drift (R6) |
| AMO-051 | S2 | Safety-relevant truthfulness | `spec185` | SPEC-185 — Blocked Discovery Telemetry Continuity | Provider 403 classification (R6) |
| AMO-052 | S2 | Safety-relevant truthfulness | `spec185` | SPEC-185 — Blocked Discovery Telemetry Continuity > blocked provider failure preserves providerExecution from universe.providerReports | Provider 403 classification (R6) |
| AMO-053 | S1 | Safety-critical execution | `spec186CanonicalSender` | SPEC-186 — Canonical single-sender enforcement | Canonical sender reaches provider; approval binds to sender revision (R4) |
| AMO-054 | S1 | Safety-critical execution | `spec186CanonicalSender` | SPEC-186 — Canonical single-sender enforcement > READY → approval → EXECUTE still correlates with canonical sender binding | Canonical sender reaches provider; approval binds to sender revision (R4) |
| AMO-055 | S1 | Safety-critical execution | `spec186CanonicalSender` | SPEC-186 — Canonical single-sender enforcement > provider receives the exact canonical email and name | Canonical sender reaches provider; approval binds to sender revision (R4) |
| AMO-056 | S1 | Safety-critical execution | `spec186CanonicalSender` | SPEC-186 — Canonical single-sender enforcement > sender/domain change alters prepared artifact revision | Canonical sender reaches provider; approval binds to sender revision (R4) |
| AMO-057 | S1 | Safety-critical execution | `spec212ProspectMessageBinding` | packages/acquisition-mission/tests/spec212ProspectMessageBinding.test.js | Message ↔ prospect binding (R4) |
| AMO-058 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence | Penny paid acquisition; must remain disabled |
| AMO-059 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > Max inspection can explain Penny contribution from durable mission state | Penny paid acquisition; must remain disabled |
| AMO-060 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > canonical Penny execution does not invoke legacy pennyAgent.js | Penny paid acquisition; must remain disabled |
| AMO-061 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > compares multiple channels and does not default to Google merely because Google evidence exists | Penny paid acquisition; must remain disabled |
| AMO-062 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > defers when budget or readiness evidence is missing and does not invent economics | Penny paid acquisition; must remain disabled |
| AMO-063 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > is callable through canonical AMO execution and persists a Penny contribution | Penny paid acquisition; must remain disabled |
| AMO-064 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > missing platform credentials do not crash canonical reasoning | Penny paid acquisition; must remain disabled |
| AMO-065 | S3 | Out of P0 scope (paid media) | `spec249PennyPaidAcquisition` | SPEC-249 — Canonical Penny paid acquisition intelligence > produces a valid SEC result with evidence, confidence, unknowns, and stop/continue/scale conditions | Penny paid acquisition; must remain disabled |
| AMO-066 | S1 | Safety-critical execution | `spec252PreparedOutreachCadence` | SPEC-252 Prepared Outreach Cadence | Prepared outreach cadence gate (R4) |
| AMO-067 | S1 | Safety-critical execution | `spec252PreparedOutreachCadence` | SPEC-252 Prepared Outreach Cadence > includes outreachSequence steps in prepared artifact revision hash | Prepared outreach cadence gate (R4) |
| AMO-068 | S1 | Safety-critical execution | `specExecutionApprovalHydration` | SPEC-136 — execution_approval hydration | Execution approval survives hydration (R4) |
| AMO-069 | S1 | Safety-critical execution | `specExecutionApprovalHydration` | SPEC-136 — execution_approval hydration > putMission rejects execution_approval when contributions are not yet in store | Execution approval survives hydration (R4) |
| AMO-070 | S3 | Presentation wording | `acquisitionMission` | SPEC-118 Max Ask | Mission explanation/progress text (R10) |
| AMO-071 | S3 | Presentation wording | `acquisitionMission` | SPEC-118 Max Ask > answers why is this mission here from evidence through Max | Mission explanation/progress text (R10) |
| AMO-072 | S3 | Presentation wording | `acquisitionMission` | SPEC-118 Max Ask > routes progress inspection before durable retrieval when a mission is active | Mission explanation/progress text (R10) |

## Resolution groups (for planning)

| Group | IDs (suites) | Class | Root cause (from register / reproduction) | Must close before P0 exit |
|---|---|---|---|---|
| G1 Verified outbound path | spec071, spec186, spec212, specExecutionApprovalHydration, spec252 | S1 | Fixtures no longer reach a send; replay/idempotency and sender/message binding unproven (R4) | **Yes** |
| G2 Authorization wording | spec135 | S1 | Discovery wording approves a pending plan (R2) | **Yes** |
| G3 Runtime boundary | spec170 | S1 | Scout.discover accepts cross-runtime sync (R3) | **Yes** |
| G4 Durability test fakes | spec138, spec139, spec169, adr075 | S2 | In-memory SQL fake lags the schema (R5); real-PostgreSQL suites pass, so the units exercise no code | Yes, or executive disposition |
| G5 Discovery truthfulness | spec175, spec185 | S2 | Blocked/unavailable classification drift (R6) | Yes, or disposition |
| G6 Autonomy | spec157 | S2 | Autonomous chaining differs from spec (R7) | Yes, or disposition |
| G7 Unguarded network call | spec140 | S2 | One unexpected network attempt in the runtime-ownership path | Yes, or disposition |
| G8 Paid media | spec249 | S3 | Penny paid acquisition, out of P0 scope; keep disabled | No |
| G9 Presentation drift | amo, spec153*, anchorFreshMissionAudit, auditDiscoveryFixes, acquisitionMission | S3 | Wording and lifecycle fixture drift (R10) | No |
