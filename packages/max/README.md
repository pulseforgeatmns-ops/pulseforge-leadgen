# @pulseforge/max

Max Reasoning Engine + Temporal Memory + Briefing Engine + Policy Engine + Command Deck Composer — deterministic recommendations, operator briefings, an explicit safety layer, and a single view model for the operator surface.

**SPEC-002** · v0.8.0 · **SPEC-003** · v0.8.1 · **SPEC-004** · v0.9.0 · **SPEC-005** · v0.9.1 · **SPEC-007** · v0.9.2

## Philosophy

Max does not make decisions. Max constructs arguments — remembers how those arguments change — assembles them into operational briefings — evaluates whether actions are **allowed** — and presents today's Command Deck as one immutable model.

- Reasoning: what should happen?
- Policy: what is allowed to happen?
- Composer: what should the operator see?

No LLM. No invented prose. No silent execution.

## Architecture

```text
Operator → Max → ReasoningEngine → KnowledgeService (Query Engine) → Graph
                 MemoryEngine   → SnapshotStore (append-only)
                 BriefingEngine → assembles Knowledge + Reasoning + Memory
                 PolicyEngine   → evaluates recommendations against rules
                 CommandDeckComposer → Briefing + Policy → CommandDeckModel
                        ↓
              GET /api/v1/command-deck → UI (SPEC-008 render-only)
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
// deck.morningBrief | highestLeverageAction | watchAlerts | marketTrends | priorityQueue | cards
```

## Layers

| Layer | Package | Role |
|---|---|---|
| Knowledge | `@pulseforge/knowledge` | What we know |
| Reasoning | `packages/max` strategies + engine | What it means |
| Memory | `packages/max/memory` | How it changed |
| Briefing | `packages/max/briefing` | How to communicate |
| Policy | `packages/max/policy` | What is allowed |
| Composer | `packages/max/commandDeck` | What the operator sees today |

## Tests

```bash
npm run test:max
# or: npm test --prefix packages/max
```

## Out of scope (this package)

Runtime agent side effects, Command Deck UI (SPEC-006), LLM summaries, autonomous outbound, push notifications, adaptive policies.
