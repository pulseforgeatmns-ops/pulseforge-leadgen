# P0 — Gate-bound chat approvals

Status: **implemented, tested, default-off** (`MAX_GATE_BOUND_APPROVAL`). Not enabled anywhere. Recommended for staging once reviewed.

## Problem

A chat approval ("Approved.", "Go ahead.") resolves against the decision the mission has pending **when the message is processed**. That is not always the decision the operator was looking at:

| Situation | Today |
|---|---|
| Another surface (approval button, another session, API) approved the plan while this conversation still showed the plan card | This conversation's "Approved." approves **discovery** |
| The plan or package was revised after it was shown | "Approved." approves the **revised** package without it being shown |
| Autonomous progression or a background stage moved the mission to its next gate | "Approved." approves the new gate |
| Server restart, or a new conversation that never displayed the decision | "Approved." approves whatever is pending |

The execution gate already closes this for outbound: the TME requires the reviewed prepared-package revision (`tme_execution_approval_unreviewed` / `_stale`). Plan, discovery, investigation and prioritization gates had no equivalent. Discovery approval can spend provider budget, and every approval is consumed.

## Design

An approval applies only to the **decision package this conversation last presented**.

1. **Identity.** `pendingDecisionKey(mission)` = `<kind>:` + a SHA-256 digest of the mission id and the pending decision object, with volatile keys (`…At`, timestamps) removed. The object covers kind, stage, prompt, plan understanding, field, prepared revision and digest. Any change to the package changes the key.
2. **Presentation.** After every mission turn, `WorkspaceEngine` records `session.context.presentedDecision = { missionId, kind, key }` for the bound mission. This uses the same hook as the execution-package recorder. A mission turn with nothing pending clears it; other turns leave it unchanged.
3. **Binding.** In `maybeHandleAcquisitionMissionExecution`, before a chat approval intent is submitted (`APPROVE_PLAN`, `APPROVE_DISCOVERY`, `CONTINUE_INVESTIGATION`, `APPROVE_PRIORITIZATION`, `APPROVE_EXECUTION`, `OPERATOR_APPROVED`), the current key must equal the presented key. If it doesn't, nothing is dispatched. Max answers *"Not executed: the decision you are approving has not been shown in this conversation yet / the pending decision changed after it was shown … Nothing was approved, sent, or cancelled."* and shows the current decision. That answer is itself a presentation, so the next explicit "Approved." answers the decision the operator has now seen.

### What it does not change (no bypass, no silent contract change)

- **It only narrows.** It adds one check at the chat surface, ahead of the existing path. `routeExecutionRequest`, the TME, `ExecutionRequest` and every other surface are unchanged.
- **The execution gate keeps its own binding.** The TME package-revision check still applies on top; a test covers both.
- **Not bound:**
  - cancellation, revision, edit and clarification answers (they are not approvals);
  - approvals the operator names in the same message ("Approved, begin discovery" → discovery is chained from the plan approval, as today);
  - the autonomous execution policy.
- **Default-off.** Enabling it is an explicit contract change for chat: an approval typed into a conversation that never displayed the decision now asks the operator to review first. That is why it is a flag and not a silent default.

### Known limit: duplicate messages

A duplicated "Approved." (double send) still approves the plan **and then** discovery. The first approval's answer presents the discovery decision, so the second message is, by this rule, an approval of a presented decision. Presentation state cannot tell a deliberate second approval from a duplicate. The fix belongs to the client:
- the dashboard sends a `clientMessageId`, and Max ignores replays;
- or it echoes the `decisionKey` of the card the operator clicked or answered.

The `decisionKey` is already computed here, so that is an additive follow-up. The test suite pins this limit so it stays visible.

## Tests

`packages/max/workspace/tests/p0GateBoundApproval.test.js` (real `WorkspaceEngine.ask()`), 21 tests:
- the identity is stable, distinct per gate and changes with the package; the flag must be exactly `true`;
- default-off parity: an unpresented approval and a drifted gate behave exactly as today;
- at the plan, discovery, prioritization and execution gates:
  - a presented decision is approved as before;
  - a never-presented one is refused with 0 dispatch and an unchanged mission, the current decision is shown, and the next approval answers it;
  - a package changed after presentation is refused;
- drift to the next gate is refused, then approved after it is shown;
- another conversation's presentation does not count;
- cancellation is unaffected; the execution package binding still applies;
- the documented duplicate-message limit.

Mutation check: with the binding disabled, 10 of the 21 fail.

## Rollout recommendation

1. Review and merge with the flag off (no behavior change).
2. In staging, set `MAX_GATE_BOUND_APPROVAL=true` and run the Max unit lane with it on. Each existing test that approves without first presenting the decision documents the contract change and needs a presenting turn, not a weaker check.
3. Add the client `clientMessageId` / `decisionKey` echo for duplicate protection.
4. Production only with owner approval.

Rollback: unset the flag. With the flag on, `session.context.presentedDecision` is the only state added; it is not persisted.
