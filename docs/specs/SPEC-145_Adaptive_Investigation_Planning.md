# SPEC-145 — Adaptive Investigation Planning

**Status:** Implemented  
**Builds on:** SPEC-142 (Investigation Engine), SPEC-143 (Memory), SPEC-144 (Credibility)

## Objective

Scout continuously re-plans an investigation as new evidence arrives, rather than executing a fixed provider sequence.

## Philosophy

**Before:** Plan → Google → Website → LinkedIn → Rank (pipeline-driven)

**After:** Question → Evidence → What changed? → What's still unknown? → Highest-value next question → Choose provider → Repeat

## Investigation Loop

```
Understand Market
  → Identify Unknowns
  → Choose Best Next Question
  → Choose Cheapest Provider (with highest expected gain)
  → Collect Evidence
  → Update Beliefs
  → Choose Next Question
  → Repeat
  → Coverage Satisfied
```

## Components

| Module | Purpose |
|--------|---------|
| `InvestigationBoard.js` | Live Known / Unknown / Persistent board with value-of-information scores |
| `InvestigationJournal.js` | Reasoning trail for every step (debugging + operator trust) |
| `ProviderLearning.js` | Second Brain learns provider × gap effectiveness |
| `InvestigationPlanner.js` | Adaptive step selection — question chooses provider |
| `InvestigationLoop.js` | Wires board, journal, stop conditions, learning feedback |

## Value of Information

Every unknown gets impact (0–1), difficulty (0–1), and expected value:

```
expectedValue = impact × (1 - difficulty)
```

Scout always investigates the highest-value unknown first.

## Dynamic Provider Selection

Provider choice is driven by the gap, not pipeline order:

- Need decision maker → LinkedIn
- Need ownership → Secretary of State / county records
- Need property count → County assessor

Expected information gain:

```
gain = gapImpact × providerEffectiveness × coverage × reliability
```

## Stop Conditions

Investigation stops when:

1. **Coverage complete** — key gaps satisfied at ≥91% coverage threshold
2. **Diminishing returns** — best next step expected gain < 2%
3. **Confidence threshold** — overall confidence met with no open gaps
4. **Cost budget** — spend exceeds configured budget
5. **Persistent unknowns only** — remaining gaps require human conversation

Every stop includes `stopExplanation` in the investigation journal.

## Dead-End Recognition

If three providers fail to answer the same unknown, Scout marks it:

```
status: persistent
resolution: requires_human_conversation
```

## Investigation Journal

Every investigation produces a reasoning trail:

```
Started with: Understand market and identify highest-value unknowns
Need decision maker
Resolve decision maker via LinkedIn — expected gain 42%
Verified decision_maker
Next question became: Need portfolio size
Stopped because: Coverage 91%, decision maker and buying signals satisfied
```

## Learning Feedback

Provider effectiveness is updated after each step and persisted to SPEC-143 memory:

```
County records → excellent for property counts
LinkedIn → excellent for operations leaders
Google Maps → poor for ownership verification
```

## Acceptance Criteria

At every step, Scout can answer:

1. **What is the single most important unknown?** → `investigationBoard.topPriorityUnknown`
2. **Why is it the highest priority?** → impact vs difficulty expected value
3. **Which provider is expected to answer it best?** → `stepSelection.chosenProvider` + `expectedInformationGain`
4. **Why are we stopping when we stop?** → `stopExplanation` + `completionReason`

A Scout investigation is no longer describable as a fixed provider pipeline.

## API

Adaptive planning is enabled by default in `Scout.investigate()` / `runInvestigationEngine()`.

Disable with `opts.adaptivePlanning: false` for legacy cost-only selection.

### Key options

| Option | Default | Purpose |
|--------|---------|---------|
| `coverageThreshold` | 0.91 | Stop when key gap coverage reached |
| `minExpectedGain` | 0.02 | Diminishing returns threshold |
| `adaptivePlanning` | true | Enable value-of-information planning |

## Tests

`test/scoutAdaptiveInvestigation.test.js`
