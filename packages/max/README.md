# @pulseforge/max

Max Reasoning Engine + Temporal Memory + Briefing Engine + Policy Engine + Command Deck Composer + Intelligence Workspace + Intelligence Navigation + Live Intelligence Loop + Operator Intelligence + Outcome Intelligence — deterministic recommendations, operator briefings, an explicit safety layer, a single view model for the operator surface, contextual conversation over verified Structured Response Objects, continuous investigation, in-place evolution via IntelligenceEvents, presentation learning from operator behavior, and empirical evaluation of whether intelligence was right.

**SPEC-002** · v0.8.0 · **SPEC-003** · v0.8.1 · **SPEC-004** · v0.9.0 · **SPEC-005** · v0.9.1 · **SPEC-007** · v0.9.2 · **SPEC-009** · v1.0.0 · **SPEC-010** · v1.0.0 · **SPEC-011** · v1.0.0 · **SPEC-012** · v1.0.0 · **SPEC-013** · v1.0.0 · **ADR-005** · **ADR-006** · **ADR-007** · **ADR-008**

## Philosophy

Max does not make decisions. Max constructs arguments — remembers how those arguments change — assembles them into operational briefings — evaluates whether actions are **allowed** — presents today's Command Deck as one immutable model — and, when asked, **presents** verified intelligence conversationally. The deck does not “refresh”; it **evolves**. Operator Intelligence then learns how humans engage — and adjusts **presentation only**. Outcome Intelligence measures whether recommendations were right — and never changes reasoning.

- Reasoning: what should happen?
- Policy: what is allowed to happen?
- Composer: what should the operator see?
- Workspace: how should verified intelligence be spoken? (LLM = presentation only)
- Navigation: how does the operator explore without dead ends?
- Live loop: how does intelligence mature during the day?
- Operator Intelligence: how does the operator engage — and how should we surface?
- Outcome Intelligence: was the intelligence itself right?

No invented business intelligence. No silent execution. Claude never scores, ranks, or invents evidence. Operator learning never alters evidence, confidence, reasoning, or policy. Outcome evaluation never rewrites history or confidence.

## Architecture

```text
Operator → Max → ReasoningEngine → KnowledgeService (Query Engine) → Graph
                 MemoryEngine   → SnapshotStore (append-only)
                 BriefingEngine → assembles Knowledge + Reasoning + Memory
                 PolicyEngine   → evaluates recommendations against rules
                 CommandDeckComposer → Briefing + Policy → CommandDeckModel
                 IntelligenceComposer → Company / Recommendation detail + Related
                 LiveLoopEngine → IntelligenceEvent store + lifecycle + awareness
                 OperatorEngine → InteractionEvent + learning + adaptive presentation
                 OutcomeEngine  → RecommendationOutcome + calibration + drift (evaluate only)
                 WorkspaceEngine → MaxContext → StructuredResponseObject → PresentationEngine
                        ↓
              GET /api/v1/command-deck → UI (SPEC-008) + live + presentation (SPEC-011/012)
              GET /api/v1/recommendations/:id · GET /api/v1/companies/:id/intelligence (SPEC-010)
              GET /api/v1/intelligence/live|notifications|timeline/:id (SPEC-011)
              POST /api/v1/operator/events|outcomes · GET …/quality (SPEC-012)
              POST /api/v1/outcome/records|lifecycle · GET …/review|calibration (SPEC-013)
              POST /api/v1/max/workspace/open|ask → Intelligence Workspace (SPEC-009)
```

## Use (in-repo)

```js
const { createMaxReasoningRuntime } = require('@pulseforge/max');

const max = createMaxReasoningRuntime();

const { recommendation } = await max.evaluate({ tenantId: '10', companyId, asOf });
await max.remember({ tenantId: '10', companyId, asOf, timestamp });
const briefing = await max.brief({ tenantId: '10', period: 'daily', asOf });

max.policy.configureTenant('10', {
  minimumConfidence: 0.75,
  maximumRisk: 0.4,
  approvalRequired: ['email', 'linkedin'],
  blockedDays: ['Sunday'],
});

const decision = await max.decide({
  tenantId: '10',
  recommendation,
  context: { channel: 'email', evidenceAgeDays: 12 },
});
// decision.allowed | requiresApproval | blocked | matchedRules | audit

const deck = await max.compose({ tenantId: '10', period: 'daily', asOf });
// deck.morningBrief | … | live | presentation (sectionOrder / sectionDominance)
// OutcomeEngine registers Generated recommendations without mutating the deck

max.trackOperator({
  type: 'ViewedRecommendation',
  tenantId: '10',
  recommendationId: recommendation.id,
});
const quality = max.operatorQuality('10');
// internal metrics — not customer-facing

max.outcomeLifecycle({
  tenantId: '10',
  recommendationId: recommendation.id,
  lifecycle: 'successful', // after generated→…→observed
  force: true, // demo only — normal path steps through the lifecycle
});
const review = max.outcomeReview('10');
// internal Intelligence Review — calibration, strategy performance, drift

const detail = await max.composeRecommendation({
  tenantId: '10',
  recommendationId: recommendation.id,
});
const company = await max.composeCompany({ tenantId: '10', companyId });

const live = max.liveSince({ tenantId: '10', since: deck.live.cursor });

const opened = max.openWorkspace({
  page: 'command-deck',
  tenantId: '10',
  visibleCards: deck.cards,
  briefing: deck.morningBrief,
  deck,
});
// opened.suggestions — personalized when preferences exist (SPEC-012)
const answer = await max.askWorkspace({
  sessionId: opened.sessionId,
  question: 'Why is the top opportunity ranked first?',
});
```

## Tests

```bash
npm run test:max
```

Includes `packages/max/live/tests/`, `packages/max/operator/tests/`, and `packages/max/outcome/tests/`.

## Boundaries

- Runtime agents remain unwired (library entrypoint).
- Live + operator + outcome stores are process-scoped (same durability as Workspace sessions).
- Fail closed when knowledge dual-write is empty.
- Operator Intelligence never changes facts — only presentation and suggestions.
- Outcome Intelligence never changes reasoning — only evaluates, measures, calibrates, reports.
