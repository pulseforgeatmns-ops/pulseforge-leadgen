# AUDIT-015 — AMO Presentation Regression (First Failure)

| Field | Value |
|---|---|
| **Status** | Completed — first presentation regression identified |
| **Date** | 2026-08-21 |
| **Related** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md), [SPEC-121](../specs/SPEC-121_Mission_Oriented_Communication.md) (mission communication), SPEC-124 (Acquisition Ownership), SPEC-128 / SPEC-133 (AMO execution / discovery presentation) |
| **Scope** | Response generation after the AMO mission has been selected. Creation, persistence, hydration, Scout execution, and discovery artifact attach are treated as already verified. |
| **Stop rule** | Stop at the first point where the response leaves the AMO presentation contract. No downstream tracing. No fix proposal. |

---

## Test case (as given)

Fresh Command Deck session. Operator:

> I want to acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area. Create a new Acquisition Mission in the AMO runtime for this objective. Do not resume or attach to any legacy SPEC-022 mission.

**Known good (previous build):** Sources `acquisition_mission`, `scout`. Mission state reflected the AMO runtime.

**Current regression:** Sources `Mission Engine`, `Client Intelligence`, with Blueprint attached, Mission Engine presentation, legacy mission semantics, and `Continue in mission workspace?`

---

## Trace (post-selection only)

```text
AMO Mission selected
        ↓
Execution Result (verified — not consumed by this presenter)
        ↓
Presentation Selection
        ↓
Response Composer
        ↓
Rendered Mission Card
```

After `maybeHandleAcquisitionOwnershipTurn` has created or resumed the AMO mission (`engine.inspect` at `AcquisitionOwnership.js:437`), presentation is handed to `buildOwnershipMissionResponse` (`AcquisitionOwnership.js:438`).

That function still *starts* on the AMO contract:

```187:199:packages/max/workspace/AcquisitionOwnership.js
  const baseComm = buildAcquisitionMissionCommunication(
    {
      mission,
      workspace: snapshot.workspace,
      missionContext: {
        stage: mission.stage,
        stageLabel: mission.status,
        progress: mission.progressPercent,
        confidence: mission.confidence,
      },
    },
    { kind: 'workspace' }
  );
```

`buildAcquisitionMissionCommunication` is the SPEC-118 / SPEC-121 AMO helper (`MissionCommunication.js:510`). Its default sources are `['Mission State']`.

**Stop. The next assignment leaves that contract.**

---

## Acceptance answers

### 1. Presentation Owner

| | |
|---|---|
| **Expected** | AMO Presentation |
| **Actual** | **Mission Engine presentation** (SPEC-124 ownership composer), with Client Intelligence attached as evidence — not as the owner |

The AMO owner for this result is `buildExecutionMissionResponse` (`AcquisitionMissionExecution.js`). After discovery execution it sets `sources: ['acquisition_mission', 'scout']` (`AcquisitionMissionExecution.js:293`). That function is not called on this turn.

---

### 2. First Divergence — STOP

| | |
|---|---|
| **File** | `packages/max/workspace/AcquisitionOwnership.js` |
| **Function** | `buildOwnershipMissionResponse` |
| **Line** | **220** |
| **Expected owner** | AMO Presentation (`buildAcquisitionMissionCommunication` / `buildExecutionMissionResponse`) |
| **Actual owner** | Mission Engine presentation |

```220:221:packages/max/workspace/AcquisitionOwnership.js
  const sources = ['Mission Engine'];
  if (ciEvidence && ciEvidence.attached) sources.push('Client Intelligence');
```

This is the first line after AMO selection where presentation ownership changes away from the AMO contract. `baseComm` was still AMO-shaped. Line 220 replaces AMO source attribution with the SPEC-022 / SPEC-124 Mission Engine label.

Invoked from `maybeHandleAcquisitionOwnershipTurn` at line 438, immediately after the AMO snapshot is taken. The execution result and Scout discovery artifact are not passed in.

**Downstream effects are not traced.**

---

### 3. Response Composer

The operator-visible card (Sources, Evidence Status, Current Understanding, Operator Decision, mission prose) is constructed by **`buildOwnershipMissionResponse`**.

It is **Mission Engine presentation**, not:

- AMO presentation (`buildExecutionMissionResponse`)
- Client Intelligence presentation (`maybeHandleClientIntelligenceTurn` / CIE advisory)
- Generic Workspace presentation (`composeResponse`)

Mechanics inside that function (not a second regression): it overlays Mission Engine fields onto `baseComm` via `buildMissionCommunication` (`AcquisitionOwnership.js:223`), then `formatMissionProse` / `applyMissionCommunication` render the card. `PresentationEngine.present` only echoes `structured.answer` when `metadata.missionCommunication === true`; it does not choose the contract.

---

### 4. Source Attribution

Rendered sources are `Mission Engine` and `Client Intelligence` because **`buildOwnershipMissionResponse` line 220–221** populated them.

They are not `acquisition_mission` / `scout` because `buildExecutionMissionResponse` (the AMO composer that writes those values at `AcquisitionMissionExecution.js:293`) never ran for this response.

Client Intelligence is appended only when `ciEvidence.attached` is true (approved Blueprint loaded earlier in `maybeHandleAcquisitionOwnershipTurn` via `attachClientIntelligenceContext`). That is evidence contribution inside the Mission Engine composer, not AMO source attribution.

---

### 5. Blueprint

`✓ Blueprint attached` is rendered because **`buildOwnershipMissionResponse` line 246** sets:

```246:246:packages/max/workspace/AcquisitionOwnership.js
    evidenceStatus: ciEvidence && ciEvidence.attached ? '✓ Blueprint attached' : 'Mission context only',
```

This is an **intentional choice inside the Mission Engine / ownership composer** (SPEC-124: Client Intelligence contributes structured evidence). It is **not** an AMO presentation fallback after failing to find Scout discovery evidence.

The AMO presentation contract was already bypassed at line 220. The ownership composer never reads `executionResult.discovery` or `workspace.scout`. Blueprint is shown because that composer’s evidence rule is CI-attachment, not because AMO presentation lacked a discovery artifact.

`Continue in mission workspace?` is the same composer’s resume `operatorDecision` (`AcquisitionOwnership.js:245`) when `created === false`.

---

## Why the operator received the legacy Mission Engine card

After the AMO mission was selected, the ask path composed the operator response with `buildOwnershipMissionResponse` instead of `buildExecutionMissionResponse`.

That ownership composer:

1. Starts from AMO communication (`buildAcquisitionMissionCommunication`).
2. At **line 220**, retitles sources to **Mission Engine** (+ **Client Intelligence** when a Blueprint is attached).
3. Renders the SPEC-124 Mission Engine card: Blueprint evidence status, legacy mission semantics, `Continue in mission workspace?`

The AMO execution result (Scout / `acquisition_mission` + `scout`) is not used for this render.

**First presentation regression identified. Stop.**
