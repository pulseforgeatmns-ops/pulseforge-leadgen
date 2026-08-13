# SPEC-094 — Max to Paige Campaign Content Delegation

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-13 |

## Objective

Let Max remain the primary operator interface for operator-level launch and growth campaigns while delegating content experiment recommendations to Paige using existing campaign context and SPEC-092/093 outcome intelligence. The runtime is a thin bridge: Max gathers the durable campaign/objective context, asks the existing Paige learning/recommendation service for a scoped recommendation, then presents the recommendation back to the operator with evidence, uncertainty, and next-action options.

## Vision References

- [SPEC-089 First Campaign Planning Conversation](SPEC-089_First_Campaign_Planning_Conversation.md)
- [SPEC-090 Max Conversational Reasoning Layer](SPEC-090_Max_Conversational_Reasoning_Layer.md)
- [SPEC-092 Content Outcome Intelligence](SPEC-092_Content_Outcome_Intelligence.md)
- [SPEC-093 Paige Outcome Learning Loop](SPEC-093_Paige_Outcome_Learning_Loop.md)
- [SPEC-022 Mission Engine and Agent Orchestration](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-023 Capability Framework](SPEC-023_Capability_Framework.md)
- ADR-045 Evidence Before Reasoning

## Problem

Pulseforge is close to supporting operator-level campaigns like:

> We are preparing a public Max launch over the next few weeks. Build qualified attention around the ideas behind Pulseforge, progressively expose the problem, then reveal Max and drive qualified demos.

The needed primitives already exist:

- Max operator surfaces and workspace runtime
- durable Mission and Client Intelligence state
- campaign planning artifacts and campaign memory
- content publication/outcome records from SPEC-092
- content learnings and recommendations from SPEC-093

The missing piece was the bridge. Max did not package campaign context and delegate a content recommendation to Paige. Paige recommendation exists as `generateContentRecommendation()`, but Max campaign flow and Paige content intelligence were not wired together.

## Scope

- A Max-side delegation adapter that calls `services/contentLearning.generateContentRecommendation()`.
- Campaign context assembly from existing durable sources:
  - Client Intelligence campaign planning state
  - Mission objective/deliverables where available
  - explicit operator message/context
  - SPEC-092 publication `campaign_id` where available
- Structured `PaigeCampaignContentRecommendation` response consumed by Max.
- Operator-facing Max response that includes recommendation, rationale, supporting learning/publication IDs, uncertainty, and next options.
- Read-only recommendation first. No autonomous publishing.
- Tenant/client isolation.
- Tests proving Max can delegate without adding new infrastructure.

## Out of Scope

- New campaign subsystem, orchestration framework, agent framework, event bus, vector store, or content warehouse.
- Autonomous content generation or publishing.
- Automatic LinkedIn metric ingestion.
- Replacing `paigeAgent.js`.
- Making Paige an always-on background planner.
- Multi-agent chat sessions or persistent LLM threads.
- New database tables unless implementation discovers a hard blocker. Current expectation: no migration.

## Dependencies

- `services/contentLearning.js`
  - `generateContentRecommendation()`
  - `getRelevantContentLearnings()`
- `services/contentOutcomeIntelligence.js`
  - `content_publications.campaign_id`
  - `toIntelligencePayload()`
- `services/clientIntelligenceInterview.js`
  - `interview_state.campaignPlanning`
  - `interview_state.campaignMemory`
  - campaign review artifacts
- `utils/maxRuntime.js`
- `packages/max/workspace/WorkspaceEngine.js`
- `packages/mission-engine`
- Existing auth/client scoping helpers

## Architecture

This is an adapter, not a new runtime.

```text
Operator
   ↓
Max Workspace / Campaign Conversation
   ↓
Max resolves durable campaign context
   ↓
Paige campaign content delegation adapter
   ↓
services/contentLearning.generateContentRecommendation()
   ↓
SPEC-093 content_learnings + SPEC-092 outcomes
   ↓
Structured recommendation
   ↓
Max explains recommendation and asks for operator decision
```

The LLM/API call remains transient. Durable intelligence remains in existing tables and JSON state.

## Data Model

No new tables in v1.

Use existing records:

- `cie_interview_sessions.interview_state.campaignPlanning`
- `cie_interview_sessions.interview_state.campaignMemory`
- `cie_interview_sessions.interview_state.firstCampaignPlanPreview`
- `cie_interview_sessions.interview_state.outreachStrategyPreview`
- `missions.id`
- `missions.objective_text`
- `missions.deliverables`
- `content_publications.campaign_id`
- `content_learnings.scope`

Recommended in-memory contract:

```js
{
  tenantId,
  clientId,
  campaignId,
  source,
  objective,
  learningObjective,
  topic,
  audience,
  channel,
  campaignContext,
  operatorMessage
}
```

Output contract:

```js
{
  kind: 'paige_campaign_content_recommendation',
  campaignId,
  objective,
  recommendedDirection,
  reason,
  confidence,
  uncertainties,
  experiment,
  supportingLearningIds,
  supportingPublicationIds,
  source: 'spec_093_content_learning',
  generatedAt
}
```

The adapter may normalize the existing SPEC-093 snake_case response into Max-facing camelCase, but it must preserve raw IDs and evidence references.

## Implementation Plan

1. Add `services/maxPaigeCampaignDelegation.js`.
   - Build a normalized Paige recommendation request.
   - Call `generateContentRecommendation()`.
   - Preserve evidence IDs and uncertainty.
   - Return a Max-facing structured payload.

2. Add a Max Workspace routing hook.
   - Detect operator requests for content, launch runway, public launch, thought leadership, LinkedIn, category creation, or "ask Paige".
   - Only route when the current context contains a campaign/objective or the operator message supplies one.
   - Fall back to existing Max behavior when confidence is low.

3. Add a campaign-context resolver.
   - Prefer explicit `context.campaignId` / `context.interviewId`.
   - Reuse current workspace/session active work context when present.
   - Optionally resolve mission by `missionId`.
   - Do not invent campaign state from free text beyond the current request objective.

4. Add Max response formatting.
   - Response must say Paige is recommending, not executing.
   - Include evidence basis and uncertainty.
   - Offer review-first next options: accept direction, revise direction, ask for another experiment, hold.

5. Add tests.
   - Adapter unit tests with stubbed learning service.
   - Workspace routing test for launch runway / LinkedIn / ask Paige.
   - Tenant isolation test.
   - No-autonomous-publish guard.

6. Update docs/state after implementation.
   - Mark this spec implemented only when the bridge is wired and tests pass.

## Migration Strategy

No migration expected.

Existing SPEC-092 `content_publications.campaign_id` and SPEC-093 learning retrieval already support campaign-aware filtering. If future implementation needs recommendation decision history, defer that to a later spec after operator use proves it is necessary.

Rollback posture: remove/disable the adapter route/hook. Existing Max, Paige, SPEC-092, and SPEC-093 behavior should remain unchanged.

## Testing

- Unit: `test/maxPaigeCampaignDelegation.test.js`
- Workspace/routing: `packages/max/workspace/tests/paigeCampaignDelegation.test.js`
- Service contract: ensure adapter works with `generateContentRecommendation()` response shape.
- Negative tests:
  - no client/tenant leaks
  - no publish/send/CRM action fields emitted
  - no recommendation when campaign context is absent and operator asks generic pipeline question
  - no hallucinated supporting IDs

## Acceptance Criteria

- [x] Max can delegate a campaign content recommendation to Paige from an operator campaign context.
- [x] Max remains the only operator-facing responder.
- [x] Paige receives objective, channel, topic/audience, campaign ID when available, and tenant/client scope.
- [x] SPEC-093 learnings are retrieved and cited by ID.
- [x] SPEC-092 supporting publication IDs are preserved when present.
- [x] Response includes recommendation, rationale, confidence, uncertainty, and review-first next options.
- [x] No publishing, CRM write, send, Buffer call, or account mutation occurs.
- [x] No new tables or infrastructure are introduced.
- [x] Tests cover adapter behavior, routing behavior, tenant isolation, and no-autonomy guardrails.

## Future Work

- Operator accept/modify/reject tracking for Paige recommendations, only after manual use proves the feedback is valuable.
- Optional Paige draft-generation context once recommendation quality is trusted.
- Campaign sequence intelligence across multiple content experiments.
- Automatic SPEC-092 publication creation from approved Paige artifacts, still review-first.

**Max owns the campaign conversation. Paige contributes content intelligence. The database, not the API session, carries the memory.**
