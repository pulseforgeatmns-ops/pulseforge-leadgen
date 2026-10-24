# AUDIT-088 — Execution Approval Clarification Bypasses READY Review

## READ-ONLY AUDIT FINDINGS

**Date:** 2026-08-30  
**Production State:** stage=ready, pendingOperatorDecision.kind=execution_approval, SPEC-210 deployed  
**Operator Input:** "continuee"  

---

## QUESTION 1: Trace "continuee" with execution_approval through system

**INPUT:** "continuee" (typo for "continue")

**TRACE PATH:**

1. **WorkspaceEngine.js:1457** — `maybeHandlePendingDecisionTurn()` is called
2. **BoundedTypoNormalization.js:200** — "continuee" normalized to "continue"
   - `normalizePendingDecisionTypos("continuee")` returns "continue" 
   - Levenshtein distance 1, within TYPO_MAX_EDIT_DISTANCE
3. **PendingDecisionResolver.js:344** — `resolvePendingOperatorDecision()` classifies the input
   - Input normalized as "continue"
   - Calls `classifyByKind('execution_approval', 'continue')`
4. **PendingDecisionResolver.js:450-468** — `classifyExecutionApproval('continue')` returns null
   - Pattern `/\b(?:approv(e|al|ed)|authoriz(e|ed|ation)|execute|launch|go ahead|proceed|outbound|send)\b/i` does NOT match "continue"
   - `isGenericAffirmative('continue')` returns false (not in vocab)
   - No match → returns null
5. **PendingDecisionResolver.js:351** — Classification is null
   - Calls `buildUnresolvedResolution(mission, pending, RESOLUTION_OUTCOMES.AMBIGUOUS)`
   - Returns: `{ pending: true, resolved: false, outcome: 'ambiguous', decisionKind: 'execution_approval', ... }`

**OUTCOME:** AMBIGUOUS (not executed, not resolved)

---

## QUESTION 2: Is SPEC-210 READY presentation branch reached?

**ANSWER: NO**

**Why:**
- SPEC-210 does not exist in the codebase (grep found zero references)
- No code path for READY-stage-specific execution review presentation exists
- The pending decision turn ownership model (SPEC-202) wins at line 1457 in WorkspaceEngine.js and returns immediately without invoking READY presentation

**Evidence:**
- [Engine.js:730](packages/acquisition-mission/Engine.js#L730) builds `executionReview` only when `mission.stage === STAGES.READY`
- [PendingDecisionTurn.js:140-142](packages/max/workspace/PendingDecisionTurn.js#L140-L142) calls `engine.inspect()` which includes executionReview
- But [PendingDecisionTurn.js:155-164](packages/max/workspace/PendingDecisionTurn.js#L155-L164) returns immediately with `clarify` action without surfacing executionReview
- [WorkspaceEngine.js:1457-1498](packages/max/workspace/WorkspaceEngine.js#L1457-L1498) returns `pending_decision_turn_ownership` and bypasses all downstream presentation

---

## QUESTION 3: Exact code producing the messages

**Message 1:** "I didn't catch a clear yes or no for the pending decision."

| File | Function | Line |
|------|----------|------|
| [packages/max/workspace/PendingDecisionTurn.js](packages/max/workspace/PendingDecisionTurn.js#L58) | `buildClarifyProse()` | 58 |

```javascript
prefix = "I didn't catch a clear yes or no for the pending decision.\n\n";
```

Condition: `resolution.outcome === RESOLUTION_OUTCOMES.AMBIGUOUS`

**Message 2:** "Authorize external execution of prepared outreach?"

| File | Function | Line |
|------|----------|------|
| [packages/max/workspace/PendingDecisionTurn.js](packages/max/workspace/PendingDecisionTurn.js#L60) | `buildClarifyProse()` | 60 |

```javascript
return `${prefix}${prompt}`.trim();
```

Where `prompt = pending.prompt || 'Authorize external execution of prepared outreach?'`

Sourced from [packages/acquisition-mission/PendingOperatorDecision.js:149](packages/acquisition-mission/PendingOperatorDecision.js#L149):

```javascript
? (pending.prompt || 'Authorize external execution of prepared outreach?')
```

---

## QUESTION 4: Unresolved pending-decision data availability

**At boundary:** `buildPendingDecisionStructured()` in [PendingDecisionTurn.js:65-105](packages/max/workspace/PendingDecisionTurn.js#L65-L105)

**Data available:**
- `snapshot` (full mission inspection including `executionReview`)
- `mission` (mission object with stage=READY)
- `resolution` (AMBIGUOUS outcome, execution_approval kind)
- `prose` (clarification message)

**Data ignored:**
- `snapshot.executionReview` — The canonical execution review object is present but not surfaced
- `snapshot.contributions` — Max, Paige, Emmett contribution payloads available but not examined
- `snapshot.progression.pause` — The mission pause state with context is available but not forwarded

**What was NOT projected:**
- No `executionReview` key in returned structured response
- No `artifactBinding` or `preparedArtifactRevision`
- No queue targets, communication variants, infrastructure status, or blocker list
- No decision summary: "Authorize external execution of the prepared outreach queue."
- No `plannedSendCount` or `blockers[]`

---

## QUESTION 5: Separate read-only phrase rendering of executionReview

**Query:** Would "show me what I'm approving" canonically render executionReview today?

**Answer:** NO

**Code path check:**
- Such a phrase would be caught by `isQuestionAboutDecision()` in [PendingDecisionResolver.js:60-74](packages/max/workspace/PendingDecisionResolver.js#L60-L74) (matches `\bwhat(?:'s| is| are)\b` and `\?`)
- Resolution would be `{ outcome: RESOLUTION_OUTCOMES.QUESTION, ... }`
- `pendingDecisionOwnsTurn()` at [PendingDecisionResolver.js:328-334](packages/max/workspace/PendingDecisionResolver.js#L328-L334) would still return **true** (outcome is QUESTION, not UNRELATED)
- Would flow to `buildClarifyProse()` which calls `contextualPendingAnswer()` at [PendingDecisionTurn.js:23-44](packages/max/workspace/PendingDecisionTurn.js#L23-L44)
- `contextualPendingAnswer()` does NOT handle execution_approval questions specifically — only discovery_approval
- Returns generic: "I can answer follow-up questions, but I still need your decision below."
- Same clarification-prose pattern, no executionReview rendered

**Result:** No canonical code path to surface executionReview at this boundary.

---

## QUESTION 6: Safety behavior verification

**Claim:** "continuee" did NOT approve execution.

| Action | Verified | Evidence |
|--------|----------|----------|
| Approve execution | ✓ NO | `resolution.resolved === false`, `resolution.executionIntent === null` |
| Clear pendingOperatorDecision | ✓ NO | Mission state unchanged, no store mutation called |
| Create execution approval contribution | ✓ NO | No operator contribution recorded, no store.addContribution() in return path |
| Cause external send | ✓ NO | No action='approve_execution', executionIntent chain blocked |
| Advance into external execution | ✓ NO | Pending decision remains unresolved, mission.stage stays READY |

**Conclusion:** System correctly rejected ambiguous input and preserved mission state. No unwanted execution occurred.

---

## PRIMARY ARCHITECTURAL DIVERGENCE

**Expected Behavior (SPEC-210 undefined state):**
At an execution_approval human-judgment boundary, an unresolved or ambiguous response must preserve the pending decision AND present the canonical bound executionReview required to make that decision.

**Actual Behavior (Code-verified):**

| Property | Expected | Actual | Reason |
|----------|----------|--------|--------|
| File | (SPEC-210 not yet defined) | [PendingDecisionTurn.js](packages/max/workspace/PendingDecisionTurn.js) | SPEC-210 does not exist; SPEC-202 (pending decision turn ownership) defines behavior |
| Function | (not defined) | `maybeHandlePendingDecisionTurn()` + `buildClarifyProse()` + `buildPendingDecisionStructured()` | SPEC-202 turn ownership model pre-empts READY presentation |
| Line | — | [WorkspaceEngine.js:1457](packages/max/workspace/WorkspaceEngine.js#L1457) and [PendingDecisionTurn.js:155](packages/max/workspace/PendingDecisionTurn.js#L155) | Return path bypasses mission inspection downstream to execution review rendering |
| Expected | Canonical execution review with targets, variants, infrastructure, blockers, and approval summary | Context-free yes/no prompt: "I didn't catch a clear yes or no...\n\nAuthorize external execution of prepared outreach?" | Pending turn ownership gates all responses through clarification prose; executionReview is available in snapshot but intentionally not surfaced by buildPendingDecisionStructured() |
| Actual | Present executionReview before requiring operator judgment | Suppress executionReview; return clarification-only response | **BLOCKING ISSUE:** `buildPendingDecisionStructured()` never examines or surfaces `snapshot.executionReview`, even though it is present and canonical |
| Reason | Operator needs bound artifact context to make informed approval judgment | SPEC-202 reduces unresolved pending decisions to a constrained clarification turn with minimal context | Turn-ownership model (SPEC-202) and execution-review presentation model (SPEC-210, undefined) have not been reconciled. SPEC-202 wins by code order. |

---

## ROOT CAUSE

**Boundary:** [PendingDecisionTurn.js:140-164](packages/max/workspace/PendingDecisionTurn.js#L140-L164)

```javascript
const snapshot = engine.inspect(mission.id, { ... });  // ✓ includes executionReview

const prose = buildClarifyProse(input.question, resolution, snapshot);
const structured = buildPendingDecisionStructured(
  prose,
  resolution,
  snapshot,  // ← passed but never used for executionReview
  snapshot.mission || mission
);

return {
  reason: 'pending_decision_turn_ownership',
  action: 'clarify',
  prose,
  structured,  // ← does not include executionReview
  mission: snapshot.mission || mission || null,
  pendingDecisionResolution: resolution,
};
```

**The Issue:**
- `buildPendingDecisionStructured()` receives `snapshot` (which contains `executionReview`)
- Function signature at [line 65](packages/max/workspace/PendingDecisionTurn.js#L65) includes snapshot parameter
- But function body never accesses `snapshot.executionReview` or any execution-related fields
- Result: executionReview exists but is discarded

**Architectural Tension:**
- **SPEC-202 (Pending Decision Turn Ownership):** Unresolved decisions retain turn ownership and return clarification-only prose
- **SPEC-210 (undefined):** Intended to present bound execution reviews for informed operator judgment
- **No reconciliation:** No code path provides both turn ownership AND execution review presentation

---

## AUDIT CHECKLIST

- [x] 1. Trace "continuee" input ✓ (execution blocked at AMBIGUOUS classification)
- [x] 2. Determine SPEC-210 READY branch reached ✓ (NO — does not exist)
- [x] 3. Identify exact code producing messages ✓ (PendingDecisionTurn.js:58, 60)
- [x] 4. Inspect unresolved-pending-decision data ✓ (executionReview available, ignored)
- [x] 5. Test "show me what I'm approving" phrase ✓ (NO canonical path)
- [x] 6. Verify safety behavior ✓ (NO execution occurred)
- [x] 7. Report architectural divergence ✓ (Turn ownership model conflicts with execution review presentation)

---

## SUMMARY

Production state is **safe**. Operator's ambiguous input "continuee" is correctly rejected, mission remains unexecuted, no database mutations occurred. However, the system presents a **context-free clarification prompt** instead of the **canonical bound execution review** required for informed operator judgment at the execution_approval boundary.

**SPEC-210 does not exist in the codebase.** The pending-decision turn-ownership model (SPEC-202) pre-empts execution-review presentation. Reconciliation between these two concerns is needed before execution_approval can render as a fully informed human-judgment gate.

