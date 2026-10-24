# Intelligence Architecture (Vision)

This document describes the **intended** intelligence stack. Implementation status lives in CURRENT_STATE and specs.

## Layers

```text
┌─────────────────────────────────────────┐
│  Experience: conversation + role UIs    │
├─────────────────────────────────────────┤
│  Reasoning: Max (+ specialist planners) │
├─────────────────────────────────────────┤
│  Memory: Business Knowledge Graph       │
├─────────────────────────────────────────┤
│  Evidence: events, touchpoints, audits  │
├─────────────────────────────────────────┤
│  Execution: agents, outbox, approvals   │
└─────────────────────────────────────────┘
```

## Evidence

Raw facts: email opens/clicks, calls, inquiry events, Scout inserts, approvals, calendar bookings. Prefer append-only logs with stable idempotency keys.

## Memory (SPEC-001)

The Knowledge Graph stores entities (client, company, prospect, inquiry, opportunity), relationships (employs, contacted, booked), and derived claims with provenance.

## Reasoning (SPEC-002)

Max consumes graph + evidence via `packages/max` to produce:

- Ranked structured recommendations (score + independent confidence)
- Supporting and contradicting evidence
- Explanation chains (ADR-002)
- Optional draft actions routed to approval (ADR-003) — not wired in v0.8.0

## Opportunity → Decision (SPEC-164 / SPEC-165)

```text
Scout: What is true?
Opportunity Intelligence: What matters?
Strategic Decision: What should the business actually do today?
```

Opportunity Intelligence ranks businesses on independent dimensions (ADR-084). Strategic Decision allocates finite hours and AOs with explicit tradeoffs, expected business outcome, and confidence (ADR-085). Activities are never recommended because they are inherently good.

## Memory (SPEC-003)

Max remembers **transitions**, not facts:

- Append-only reasoning snapshots
- Deterministic diffs and change events
- Trends, history, and watch detection (no notifications yet)
- Temporal explanations: Why → Evidence → History → Change → Reason

## Briefing (SPEC-004)

Max assembles Knowledge + Reasoning + Memory into deterministic operator briefings:

- `brief({ tenantId, asOf, period })` — daily / weekly / monthly
- Structured sections only (summary, priorities, changes, watches, risks, recommendations, metrics)
- Never computes — never calls `evaluate()` during assembly
- Presentation Adapter extension point for CLI / dashboard / assistant surfaces

## Policy (SPEC-005)

Max evaluates every recommendation against explicit tenant policy before operator or automation:

- `decide({ tenantId, recommendation, context })` — allow / warn / requireApproval / block
- Modular Rule Registry (confidence, contradiction, tenant, risk, cooldown, contact, freshness)
- Immutable audit trail + explainability chain
- Never executes — evaluation only

## Command Deck Composer (SPEC-007)

Max presents the stack as one immutable view model for the operator surface:

- `compose({ tenantId, asOf, period })` — Morning Brief, Highest Leverage Action, Watch Alerts, Market Trends, Priority Queue
- Common `IntelligenceCard` contract + composer-owned empty states
- Explainability metadata on every card (`sources`, `reasoningId`, `policyId`, `briefingId`)
- May sort / merge / rank / summarize / group — never reason / score / infer / invent
- HTTP: `GET /api/v1/command-deck`
- UI: `GET /command-deck` (SPEC-008) — render-only from `CommandDeckModel`

## Max Intelligence Workspace (SPEC-009)

Max presents verified stack output as a contextual conversation (ADR-005):

- Explicit `MaxContext` envelope from Command Deck (and future pages)
- Deterministic `StructuredResponseObject` from envelope + stack facts
- Claude PresentationEngine translates only — never scores, ranks, or invents
- HTTP: `POST /api/v1/max/workspace/open`, `POST /api/v1/max/workspace/ask`
- UI: full-height Intelligence Workspace modal on `/command-deck`

## Intelligence Navigation (SPEC-010)

Operators explore a continuous graph without dead ends:

- Intelligence trail (investigation breadcrumbs) on `/command-deck`
- Related Intelligence on every node (company / recommendation / evidence)
- Progressive evidence depth; MaxContext synced to trail focus
- Composers: `composeRecommendation` / `composeCompany` (assemble only — never re-score)
- HTTP: `GET /api/v1/recommendations/:id`, `GET /api/v1/companies/:id/intelligence`

## Live Intelligence Loop (SPEC-011)

Intelligence evolves in place ([ADR-006](../adr/ADR-006_Live_Intelligence_Evolution.md)):

- Common `IntelligenceEvent` + lifecycle (Detected → Verified → Strengthened → Contradicted → Resolved → Archived)
- `LiveLoopEngine` observes Command Deck compose + memory changes
- Soft-poll: `GET /api/v1/intelligence/live?since=` — gentle UI evolution, not hard refresh
- Morning Brief accumulates evolution entries
- Max awareness during workspace sessions; investigation continuity banner
- Notifications only for material events

## Operator Intelligence (SPEC-012)

Pulseforge learns how operators engage — without changing facts ([ADR-007](../adr/ADR-007_Operator_Intelligence.md)):

- `InteractionEvent` model + `RecommendationLearning` aggregates
- Explicit outcome lifecycle (Recommended → … → Successful / Dismissed / Expired / Contradicted)
- Adaptive presentation (order / visual dominance only — never hide)
- Max suggestion personalization from tenant conversational preferences
- Internal trust/usefulness signal (never replaces confidence)
- Internal quality dashboard for Pulseforge improvement
- Hard boundary: may personalize presentation; may never alter evidence, confidence, reasoning, or policy

## Outcome Intelligence (SPEC-013)

Pulseforge measures whether intelligence was right — without changing reasoning ([ADR-008](../adr/ADR-008_Outcome_Intelligence.md)):

- `RecommendationOutcome` + lifecycle (Generated → Reviewed → Approved → Executed → Observed → Successful | Unsuccessful | Inconclusive)
- Strategy-level performance metrics (internal only)
- Confidence calibration reports (empirical success by confidence band)
- Drift detection (alerts engineers, not customers)
- Internal Intelligence Review dashboard
- Hard boundary: may evaluate / measure / calibrate / report; may never rewrite history, alter reasoning, manipulate confidence, or change recommendations

## Execution

Specialist agents and outbox adapters perform channel work. Shadow mode records intent without side effects until flags and approvals permit.

## Separation from dashboards

Dashboards read the same truth; they are not a second brain. Conversation and command surfaces query the same graph/services.

See also: [architecture/Memory_Architecture.md](../architecture/Memory_Architecture.md), [architecture/Knowledge_Graph_Architecture.md](../architecture/Knowledge_Graph_Architecture.md), [architecture/Agent_Architecture.md](../architecture/Agent_Architecture.md).
