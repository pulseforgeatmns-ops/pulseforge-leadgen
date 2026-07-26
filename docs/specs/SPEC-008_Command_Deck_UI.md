# SPEC-008 — Command Deck UI

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Highest |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Version** | v1.0.0 |

## Objective

Render the intelligence workspace using **only** `CommandDeckModel`.

## Philosophy

This spec intentionally contains almost no business logic.

Everything interesting has already happened.

The UI's responsibility is to:

- reveal intelligence
- establish hierarchy
- maintain calm
- invite investigation

Nothing more.

## Rule #1

The UI must never:

- calculate
- rank
- filter
- sort
- merge
- infer

Everything comes directly from:

```text
GET /api/v1/command-deck
```

## Vision References

- `docs/vision/Product_Constitution.md` (§11 Cognitive load)
- `docs/vision/Product_Experience.md`
- `docs/vision/Intelligence_Architecture.md`
- [SPEC-006](SPEC-006_Command_Deck.md) — product surface / Ask Max workspace / company & recommendation pages
- [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) — `CommandDeckModel` producer
- [ADR-002](../adr/ADR-002_Explainable_AI.md)

## Problem

`CommandDeckModel` is ready (`GET /api/v1/command-deck`). Operators still land on CRM-shaped dashboards. Nothing presents the composer payload as a single calm vertical briefing.

## Scope

- Command Deck page at `/command-deck`
- Morning Brief, Highest Leverage Action, secondary Intelligence Cards, Priority Queue
- Ask Max launcher (pinned; never auto-open)
- Staged-reveal loading; composer-driven empty states; calm error recovery
- Accessibility and responsive stacked layout
- Presentation only — zero intelligence logic in UI components

## Out of Scope

- Ask Max conversation / Intelligence Workspace modal (SPEC-006)
- Company Intelligence page (SPEC-006)
- Recommendation Detail page (SPEC-006)
- Recreating Knowledge / Reasoning / Memory / Briefing / Policy / Composer logic
- Autonomous execution
- Replacing `/dashboard` as default landing (migration decision later)

## Dependencies

- ✅ SPEC-007 Command Deck Composition Engine (v0.9.2)
- ✅ `GET /api/v1/command-deck` → `CommandDeckModel`
- → Completes the render half of [SPEC-006](SPEC-006_Command_Deck.md) landing experience

## Architecture

```text
GET /api/v1/command-deck
        │
        ▼
CommandDeckModel
        │
        ▼
Command Deck UI (render-only)
        │
        ▼
Ask Max launcher (invitation only)
```

## Screen Layout

```text
┌────────────────────────────────────────────┐
 Navigation
─────────────────────────────────────────────
 Morning Brief
─────────────────────────────────────────────
 Highest Leverage Action
─────────────────────────────────────────────
 Watch Alerts | Trends | Risks*
─────────────────────────────────────────────
 Priority Queue
─────────────────────────────────────────────
 Ask Max
└────────────────────────────────────────────┘
```

\* Risks column renders only when `CommandDeckModel` supplies risk cards; the UI never invents them.

Exactly one vertical flow. No dashboards inside dashboards.

## Sections

### Morning Brief

Large typography. Editorial, not SaaS.

Example:

```text
Good morning.

Your market shifted overnight.
```

Everything below should reinforce that headline. Headline and summary come from `morningBrief` (and/or the matching `IntelligenceCard`).

### Highest Leverage Action

Largest visual element. Always.

It should immediately answer: **"What should I do first?"**

No scrolling required to see it.

### Intelligence Cards

Cards should feel consistent.

Every card renders from `IntelligenceCard`.

Nothing custom. No bespoke layouts.

### Priority Queue

Simple. Dense. Readable.

- Movement indicator
- Opportunity
- Confidence
- One-line summary

Nothing else. Fields come from `priorityQueue` items / priority `IntelligenceCard` payloads.

### Ask Max Launcher

Pinned to the bottom. Always visible. Always calm.

- Never auto-open
- Never demand attention
- Invitation, not interruption

## Visual Hierarchy

The eye should naturally move:

```text
Headline
  ↓
Primary recommendation
  ↓
Supporting intelligence
  ↓
Priority queue
  ↓
Conversation
```

If someone has to hunt for what matters, we've failed.

## Motion

Subtle only.

- Cards fade
- Rank changes animate gently
- No bouncing
- No flashy transitions

The interface should feel composed. Honor `prefers-reduced-motion`.

## Loading State

No skeleton armies. Prefer staged reveal:

```text
Morning Brief appears.
  ↓
Recommendation appears.
  ↓
Cards appear.
  ↓
Queue appears.
```

The system should feel like it's assembling today's briefing.

## Empty States

Render exactly what the composer returns (`emptyStates` / empty `IntelligenceCard`s).

No UI-generated messaging.

## Error States

If the endpoint fails:

```text
Today's briefing is unavailable.

[ Retry ]

[ View last successful briefing ]
```

Don't dump stack traces or generic API errors.

Cache the last successful `CommandDeckModel` in the browser for the recovery path only.

## Accessibility

- Keyboard navigable
- Visible focus states
- Screen-reader labels
- Reduced-motion support
- High-contrast compatible

## Responsive Behavior

| Viewport | Behavior |
|---|---|
| Desktop | Command Deck |
| Tablet | Identical hierarchy |
| Mobile | Stacked cards |

Never collapse the information hierarchy.

## Data Model

Consumer of `CommandDeckModel` from SPEC-007:

- `morningBrief`
- `highestLeverageAction`
- `watchAlerts` (IntelligenceCard[])
- `marketTrends` (IntelligenceCard[])
- `priorityQueue`
- `cards`
- `emptyStates`
- `meta`

## Implementation Plan

1. Spec + docs index
2. `/command-deck` HTML shell + CSS (hierarchy, motion, responsive)
3. Render module: fetch model → staged reveal → section renderers (no business logic)
4. Error + last-successful cache
5. Wire route + shell nav; keep `/dashboard` available

## Migration Strategy

- Ship at `/command-deck` while `/dashboard` remains default
- No schema migration
- Rollback: remove or hide nav link; API unchanged

## Testing

- Manual: load `/command-deck` authenticated → hierarchy reads in &lt; 30s
- Empty tenant: composer empty states render verbatim
- API failure: calm error + retry + last successful
- Reduced motion: no staged fade animations
- Responsive: mobile stacks without reordering sections
- No client-side sort/filter/rank of intelligence arrays

## Acceptance Criteria

- [x] Entire screen rendered from `CommandDeckModel`
- [x] Zero business logic in presentation components
- [x] Responsive across desktop / tablet / mobile
- [x] Motion system implemented (with reduced-motion)
- [x] Empty and error states driven by backend / cache contract
- [x] Component hierarchy mirrors information hierarchy
- [x] UI feels like a morning briefing, not a CRM
- [x] Ask Max launcher pinned, calm, never auto-opens

## Future Work

- Full Ask Max Intelligence Workspace ([SPEC-009](SPEC-009_Max_Intelligence_Workspace.md))
- Recommendation Detail + Company Intelligence pages (SPEC-006)
- Default landing cutover from `/dashboard` when acceptance passes
- Risks column when composer surfaces risk cards

## Final Design Principle

When an operator opens the Command Deck, they should feel that the system has already done the thinking. Their job is to see what matters—and decide.
