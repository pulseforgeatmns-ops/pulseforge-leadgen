# SPEC-097 — Living Command Deck

| Field | Value |
|---|---|
| **Status** | Implemented (Phase 1–2 foundation) |
| **Scope** | Command Deck UX/UI evolution |
| **Primary interface** | Max |
| **Related** | [SPEC-006](SPEC-006_Command_Deck.md), [SPEC-008](SPEC-008_Command_Deck_UI.md), [SPEC-096 Max Specialist Direction](SPEC-096_Max_Specialist_Direction_and_Operator_Rationale.md) |

> **Note:** SPEC-096 was previously used for Specialist Direction. This spec is numbered **097** to avoid collision.

## Summary

Transforms the Command Deck from a static vertical mission/campaign list into a **living spatial representation** of Max's operating domains. Max remains visually anchored; domains organize around him based on discrete priority bands derived from existing intelligence signals.

## Implementation (v1)

### Backend
- `packages/max/commandDeck/spatial/DomainPriority.js` — deterministic domain signal collection and priority bands
- `packages/max/commandDeck/sections/SpatialOverview.js` — composes `spatialOverview` on `CommandDeckModel`
- `services/commandDeckPriority.js` — persists priority transitions per tenant/domain
- `migrations/2026-08-16-living-command-deck-priority.sql`

### Frontend
- `public/command-deck/spatial-deck.js` — render-only spatial canvas, domain drawer, list fallback
- `public/command-deck/command-deck.css` — spatial layout, gold intelligence edge, reduced-motion support
- Integrated into `command-deck.js` as primary surface when `model.spatialOverview` is present

### Domains (v1)
| Domain | Sources |
|---|---|
| Acquisition | Priority queue, watch alerts, AO operator brief |
| Content | Pending/refined Paige content recommendations |
| Clients | Active operator objectives (client scope) |
| Campaigns | Mission queue (compressed; historical contained) |

### Priority bands
`monitored` · `normal` · `elevated` · `urgent` — discrete only; proximity to Max changes on band crossing, not continuous scores.

### Progressive disclosure
Spatial overview → domain drawer → mission/workspace/detail surfaces.

### Accessibility
- **View as list** toggle (persisted in `localStorage`)
- Reduced motion: ordering, labels, illumination without animation
- Mobile: prioritized card/list representation (< 640px)

## Deferred (Phase 3)
- Operator reprioritization through Max with visible spatial response
- Durable cross-session conversation selector (workspace sessions remain in-process)
- Multi-specialist AO synthesis beyond existing signals

## Acceptance tests
See `packages/max/commandDeck/tests/spatialOverview.test.js` and `test/livingCommandDeck.test.js`.
