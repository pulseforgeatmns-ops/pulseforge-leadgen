# AUDIT-006 — Scout Discovery Execution Audit

| Field | Value |
|---|---|
| **Status** | Completed |
| **Date** | 2026-08-19 |
| **Related** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Object.md), [SPEC-100A](../specs/SPEC-100A_Scout_Acquisition_Discovery_Foundation.md), [SPEC-024](../specs/SPEC-024_Prospect_Discovery.md), AUDIT-003 |
| **Scope** | Trace Discovery execution from Stage Executor through Scout to response composition |

## Executive summary

Scout Discovery **does execute** when the operator approves Discovery on an active Mission. `ScoutDiscoveryExecutor` is invoked and runs the `prospect_discovery` capability (active external search via Google Places or fixture). It does **not** merely query the Market Intelligence store.

However, two parallel discovery implementations exist and are **not fully wired together**:

| Path | Invoked by Stage Executor? | Strategy | Market Intelligence? |
|---|---|---|---|
| `prospect_discovery` capability (SPEC-024) | **Yes** | External Discovery | Not consulted |
| `runScoutAcquisitionIntelligence` (SPEC-100A) | **No** | Hybrid (retrieve PF + gap discover) | Not consulted (uses companies/prospects DB) |

The observed advisory prose — *"I do not yet have enough live market or prospect evidence..."* — originates from **Client Intelligence Engine (CIE)** advisory routing (`ClientIntelligenceContext.formatEvidenceDependentGapAnswer`), not from Scout Discovery execution. That message appears only when a turn is handled as an evidence-dependent client question **without** Mission-first routing owning the response.

When Mission-first routing succeeds and `ScoutDiscoveryExecutor` runs, the operator now receives an explicit **Mission execution outcome** (AUDIT-006 instrumentation + response composition fix).

---

## Acceptance criteria answers

| Question | Answer |
|---|---|
| Did Scout execute? | **Yes** — `MISSION_EXECUTOR_INVOKED` → `ScoutDiscoveryExecutor` → `MissionExecutor.execute()` → `prospect_discovery`. |
| Which discovery strategy was selected? | **External Discovery** on the Stage Executor path. Hybrid retrieve-before-discover exists only on the separate SPEC-100A path. |
| Which evidence sources were attempted? | Discovery Profile Store (required), External Search (Places/fixture), optional CRM dedupe. Market Intelligence Store, Company Store, and Prospect Store are **skipped** on this path. |
| Was external discovery attempted? | **Yes**, when a Discovery Profile resolves and a search provider is available. |
| Was only stored Market Intelligence queried? | **No.** Market Intelligence is not read during Stage Executor discovery. |
| What execution outcome occurred? | Mapped to `DISCOVERY_COMPLETED`, `DISCOVERY_PARTIAL`, `DISCOVERY_BLOCKED`, or `DISCOVERY_FAILED` via pipeline gate + capability status. |
| Was Mission state updated? | **Yes** — mission status, plan steps, progress, blockingIssues, and deliverables updated by `MissionExecutor`. |
| Did the operator receive a Mission outcome rather than advisory reasoning? | **After AUDIT-006 fix:** yes, when Mission-first routing owns the turn. CIE gap prose remains on the separate advisory path. |

---

## Step 1 — Executor invocation

**Verified:** `MISSION_EXECUTOR_INVOKED` with executor `ScoutDiscoveryExecutor`.

Trigger: operator message matching `Approved. Begin Scout discovery.` → `classifyMessage` → `EXECUTE_STAGE` → `ActiveMissionResolver` → `StageExecutionOrchestrator.executeCurrentStage()`.

Captured metadata (`buildScoutDispatchPayload`):

- Mission ID
- Objective (from mission plan or objectiveText)
- Target segment (vertical / plan subject)
- Geography (constraints.locationHint)
- Timestamp (audit event ISO timestamp)
- Approval state

**Finding:** `scoutPayload` is audit metadata. It is not passed into `runScoutAcquisitionIntelligence`.

---

## Step 2 — Discovery strategy

| Audit label | Stage Executor path | SPEC-100A Scout path |
|---|---|---|
| Stored Market Intelligence | No | No (uses companies/prospects DB, not MI store) |
| External Discovery | **Yes** (primary) | Yes (gap-fill via Places) |
| Hybrid | No | **Yes** (retrieve existing PF + discover gap) |
| No Strategy Selected | When Discovery Profile missing/blocked | When search definition invalid |

**Emitted:** `SCOUT_DISCOVERY_STRATEGY` (AUDIT-006).

---

## Step 3 — Evidence sources

### Stage Executor path (`prospect_discovery`)

| Source | Attempted | Notes |
|---|---|---|
| Discovery Profile Store | Yes | Required; blocks if missing |
| External Search | Yes | Google Places or fixture |
| CRM Lookup | Optional | Dedupe only |
| Market Intelligence Store | Skipped | Not wired |
| Company Store | Skipped | Not retrieved as candidates |
| Prospect Store | Skipped | Not retrieved as candidates |
| Enrichment | Skipped | Later pipeline stage |
| Social Intelligence | Skipped | Not on this path |

**Emitted:** `SCOUT_EVIDENCE_SOURCE` per source (AUDIT-006).

### SPEC-100A path (not reached by Stage Executor)

| Source | Attempted | Notes |
|---|---|---|
| Existing PF (companies + prospects) | Yes | `loadRepository()` |
| Public business data (Places) | Yes | When adapters available |
| Social stubs | Checked | Always unavailable, non-blocking |

---

## Step 4 — Discovery outcome

Pipeline gate outcomes map to audit vocabulary:

| Audit outcome | Mission Engine gate | Condition |
|---|---|---|
| `DISCOVERY_COMPLETED` | `completed` | prospectCount > 0, meets target |
| `DISCOVERY_PARTIAL` | `completed_with_warnings` | Shortfall vs targetCount |
| `DISCOVERY_BLOCKED` | `blocked` | Zero prospects, missing profile, no provider |
| `DISCOVERY_FAILED` | `failed` | Capability failed/cancelled |

**Emitted:** `SCOUT_DISCOVERY_OUTCOME` (AUDIT-006).

**Prior gap:** Outcomes collapsed into generic prose ("Scout discovery executed") without strategy, sources, or block reason.

---

## Step 5 — Blocked reason

When Discovery blocks, explicit reasons are captured — never *"I don't know."*

Examples observed in code paths:

| Block reason | Source |
|---|---|
| No Discovery Profile | ProfileSelector blocked |
| No external discovery connector | Places key missing |
| Zero verified companies | PipelineGate.validateDiscovery |
| Capability failed | MissionExecutor gate |

**Emitted:** `SCOUT_BLOCK_REASON` when outcome is blocked (AUDIT-006).

---

## Step 6 — Mission update

Regardless of outcome, Mission state updates via `MissionExecutor`:

- `mission.status` → `executing`, `review_required`, `waiting`, or `failed`
- `plan.steps[].status`, `outcome`, blockingIssues
- `progress.stageOutcome`, `currentStage`
- `deliverables.stepResults`, `lastGate`

**Emitted:** `MISSION_DISCOVERY_UPDATE` (AUDIT-006).

**Gap (unchanged):** SPEC-118 Acquisition Mission `scoutComplete` is updated only via `attachScoutDiscovery()` on the SPEC-100A path with `missionId`, not by Stage Executor discovery alone.

---

## Step 7 — Response composition

### Before AUDIT-006

```
Mission Updated
Scout discovery executed for stage Discovery.
```

No outcome, block reason, evidence sources, or next recommendation.

### After AUDIT-006

```
Mission Updated

Stage: Discovery
Outcome: BLOCKED
Reason: Discovery returned zero verified companies...

Scout executed successfully via prospect_discovery (External Discovery). No verified prospects were returned.

Next Recommendation: Broaden Discovery Profile geography or industry targets, then retry Discovery.
```

**Emitted:** `MISSION_DISCOVERY_RESPONSE` (AUDIT-006).

### CIE advisory path (separate)

The exact string *"I do not yet have enough live market or prospect evidence..."* is produced only by `formatEvidenceDependentGapAnswer` when:

1. An approved Blueprint exists, and
2. The question scores as evidence-dependent (`isEvidenceDependentClientRequest`), and
3. Mission-first routing does **not** own the turn.

This is **not** Discovery execution — it is advisory fail-closed reasoning (SPEC-103/103B).

---

## Step 8 — Capability boundary

| Capability | Question answered |
|---|---|
| **Discovery (execution)** | What can we learn now? |
| **Database query / retrieval** | What do we already know? |
| **CIE advisory gap** | What can Blueprint say without live evidence? |

**Verdict:** Stage Executor Discovery is **active external discovery**, not passive Market Intelligence retrieval. The SPEC-100A Scout loop adds hybrid retrieve-before-discover but is **not** invoked by `ScoutDiscoveryExecutor` today.

---

## Failure matrix

| ID | Condition | Status |
|---|---|---|
| A | Executor never invoked | **Pass** — AUDIT-003 confirms invocation |
| B | Discovery strategy never selected | **Pass** — External Discovery selected on executor path |
| C | Only Market Intelligence queried | **Pass** — MI not queried on executor path |
| D | External discovery attempted but unavailable | **Observed possible** — blocked with explicit provider reason |
| E | Discovery outcome not recorded | **Fixed** — AUDIT-006 emits outcomes + mission update |
| F | Operator receives advisory prose instead of execution outcome | **Partial** — fixed for Mission-first path; CIE path unchanged by design |

---

## Required instrumentation

Implemented in `packages/mission-engine/ScoutDiscoveryAudit.js` and wired through `ScoutDiscoveryExecutor` + `discoveryExecutionReport.js`:

| Event | Status |
|---|---|
| `SCOUT_DISCOVERY_STRATEGY` | Implemented |
| `SCOUT_EVIDENCE_SOURCE` | Implemented |
| `SCOUT_DISCOVERY_OUTCOME` | Implemented |
| `SCOUT_BLOCK_REASON` | Implemented |
| `MISSION_DISCOVERY_UPDATE` | Implemented |
| `MISSION_DISCOVERY_RESPONSE` | Implemented |

Every event includes: mission ID, discovery strategy, evidence sources (where applicable), outcome, block reason (when blocked), timestamp.

Tests: `packages/mission-engine/tests/scoutDiscoveryExecutionAudit.test.js`

---

## Architectural invariant

> Discovery is an execution capability, not a retrieval capability.

**Current implementation on the Stage Executor path:** satisfies the invariant for external discovery. Market Intelligence remains a **possible** evidence source but is not yet wired into either discovery path. Wiring SPEC-100A hybrid discovery into `ScoutDiscoveryExecutor` and syncing SPEC-118 AMO `scoutComplete` remain follow-up integration work.

---

## Recommended follow-ups

1. Wire `ScoutDiscoveryExecutor` to `runScoutAcquisitionIntelligence` (or unify behind a single discovery facade).
2. Connect Market Intelligence store as an optional evidence source per SPEC-118 contract.
3. Sync SPEC-022 mission discovery outcomes to SPEC-118 AMO Scout contributions.
4. Add workspace routing audit to detect when CIE gap prose is returned despite active Mission continuation (cross-check AUDIT-005 ownership trace with AUDIT-006 events).
