# AUDIT-022 — Workspace Request / Response Identity

| Field | Value |
|---|---|
| **Scope** | Browser → `POST /api/v1/max/workspace/ask` → Express → `WorkspaceEngine.ask()` → `res.json()` → browser → `appendMaxResponse()` → DOM |
| **Out of scope** | Discovery execution, Scout, planning, lifecycle transitions, presentation contracts |
| **Assumption** | Discovery execution in `WorkspaceEngine.ask()` is functioning correctly |
| **Test case** | Stage `Discover`, `structuredMissionApproved: true`, `pendingOperatorDecision.kind: discovery_approval`, operator sends `approved` |
| **Observed symptom** | Browser shows `Mission Plan Approved`, Stage `Discover`, `Approve discovery?` |

---

## Part A — Browser Request

| Field | Finding |
|---|---|
| Generated request id | **None** — browser does not attach a client request id |
| `sessionId` | `workspaceSessionId` from prior `/open` (or null on first ask) |
| `workspaceSessionId` | Module-level variable in `command-deck.js` |
| Mission id before request | From `workspaceContext.missionId` / `workspaceContext.acquisitionMissionId` when set |
| Request body | `{ sessionId, question: "approved", context: workspaceContext }` |
| Request timestamp | Not recorded client-side |

**Verify — exactly one HTTP request?** **Yes.**

- `askWorkspace()` sets `workspaceAskInFlight = true` before fetch and clears it in `finally` after `appendMaxResponse()` completes.
- Duplicate submits are dropped at `command-deck.js:4614`.
- Only one call site reaches `/api/v1/max/workspace/ask` in the public UI.

---

## Part B — Express

| Field | Finding |
|---|---|
| Route | `routes/maxWorkspace.js` → `POST /api/v1/max/workspace/ask` |
| Request id | Not generated server-side |
| Session id | `req.body.sessionId` |
| Tenant id | `resolveTenantId(req)` |
| Mission id | Resolved inside `WorkspaceEngine.ask()` from session context + AMO hydration |
| Response object identity | Object returned by `max.askWorkspace()` (not raw `workspace.ask()`) |

**Verify — `WorkspaceEngine.ask()` invoked exactly once?** **Yes.**

```
routes/maxWorkspace.js:186-190
  let result = await max.askWorkspace({ sessionId, question, context });
       ↓
packages/max/index.js:319-320
  const result = await workspace.ask(input);
```

No retry, fan-out, or secondary ask on the same HTTP turn.

---

## Part C — WorkspaceEngine (immediately before return)

For the test case (discovery approval pending, utterance `approved`), AUDIT-008 and SPEC-136 lock the AMO execution path:

| Field | Expected (Discovery executed) |
|---|---|
| Request id | None |
| Session id | Bound workspace session |
| Mission id | Active AMO mission |
| Stage | `discover` → advances to `understand` after execution |
| Execution action | `discovery_approved` |
| Response headline | `Mission Updated` |
| Structured response identity | `metadata.missionCommunicationPayload.headline === "Mission Updated"` |
| Response hash | Deterministic fingerprint over mission id + headline + prose (see `WorkspaceAskIdentityAudit.js`) |

**Verify — returned object is the Discovery execution response?** **Yes** when mission state matches the test case preconditions.

Evidence: `packages/max/workspace/tests/audit008AmoDiscoveryApprovalExecutionTrace.test.js`, `packages/acquisition-mission/tests/spec136.test.js`.

The observed symptom text **`Mission Plan Approved`** is **not** produced by the `discovery_approved` branch. It is produced only by the `plan_approved` branch:

```209:220:packages/max/workspace/AcquisitionMissionExecution.js
    const comm = buildMissionCommunication({
      headline: 'Mission Plan Approved',
      ...
      stage: 'Discover',
      ...
      operatorDecision: 'Approve discovery?',
```

That template is the correct response for the **plan approval** turn, not the discovery turn.

---

## Part D — HTTP Response (`res.json(...)`)

**Verify — does Express serialize the exact object returned by `WorkspaceEngine.ask()`?** **No.**

Express serializes the **`askWorkspace()` wrapper result**, which mutates the engine return in place:

```343:345:packages/max/index.js
    if (awareness.headline && result.prose) {
      result.prose = `${awareness.headline}\n\n${result.prose}`;
    }
```

```401:401:packages/max/index.js
      result.suggestions = operator.suggestions(suggestionContext, tenantId);
```

| Checkpoint | Identity impact |
|---|---|
| `awareness.headline` prepend | Changes `prose` and response hash; does **not** change structured mission headline |
| `result.suggestions` replacement | Adds field; does not change mission communication payload |
| `presentMaxResultForClient()` | Admin/manager path skips this; client role softens prose only |

**First technical identity mutation:** `packages/max/index.js`, function `askWorkspace`, lines **343–345** (prose prepend).

This mutation cannot convert `Mission Updated` into `Mission Plan Approved`.

---

## Part E — Browser Reception

At `apiRequest('/api/v1/max/workspace/ask', ...)` (`command-deck.js:4659-4666`):

- Returns parsed JSON payload verbatim.
- No client-side cache, deduplication, or response substitution.
- Updates `workspaceSessionId` and `workspaceContext` from the same payload.

**Verify — browser receives the same payload Express emitted?** **Yes**, modulo normal JSON parse round-trip (verified in `audit022WorkspaceAskIdentity.test.js`).

---

## Part F — DOM Rendering

At `appendMaxResponse(result, ...)` (`command-deck.js:4200-4295`):

- Reads `result.prose` and `result.structured` from the received payload.
- Does **not** refetch workspace/mission state (`apiRequest`, `fetch`, `loadDeck`, or `openMissionWorkspace` are absent from this function).
- Appends a new `.mx-msg` node; does not overwrite prior turns.
- `presentCompletedResponse()` reveals the same `prose` captured at function entry.

**Verify — renders received payload vs overwriting from workspace state?** **Renders received payload.**

---

## First Identity Divergence (Deliverable)

### For the stated test case assuming Discovery executed correctly

**No divergence exists between the Discovery execution response and the DOM renderer (Parts C → F).**

The pipeline preserves a single payload identity from AMO execution composition through HTTP to `appendMaxResponse()`. No layer swaps a `discovery_approved` payload for a `plan_approved` payload.

The observed browser content:

```text
Mission Plan Approved
Stage: Discover
Approve discovery?
```

is the **canonical `plan_approved` template** (`AcquisitionMissionExecution.js:209-220`). That content is identity-consistent end-to-end **for the plan-approval HTTP request**. It cannot appear as the rendered output of a `discovery_approved` execution result without a break in Parts C–F, and no such break exists in code.

### First technical mutation in the audited chain

| | |
|---|---|
| **File** | `packages/max/index.js` |
| **Function** | `askWorkspace` |
| **Line** | 343–345 |
| **Expected identity** | Exact `WorkspaceEngine.ask()` return object |
| **Actual identity** | Same object reference with mutated `prose` (awareness prepend) and replaced `suggestions` |

This is **not** the cause of the observed plan-vs-discovery headline mismatch.

### Reconciling the observed symptom

Under the audit assumption that Discovery executed on the operator's `approved` turn:

1. **Most likely:** The visible `Mission Plan Approved` message is the **prior plan-approval turn** still present in the thread. `appendMaxResponse()` appends; it does not replace the previous Max message.
2. **If that message is the latest Max reply after the discovery request:** Part C returned `plan_approved`, not `discovery_approved` — an execution-routing/state issue (explicitly out of scope for this audit). The request/response identity chain remains intact; the wrong action was executed on the same request.

---

## Regression lock

`packages/max/workspace/tests/audit022WorkspaceAskIdentity.test.js` fingerprints discovery approval identity through HTTP simulation and asserts `appendMaxResponse()` does not refetch workspace state.
