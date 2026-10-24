# SPEC-024 — Prospect Discovery Capability

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, ADR-010, ADR-011 |
| **Consumed by** | Mission Engine, Command Deck Operations |

## Objective

Implement the first production capability for the Mission Engine: **Prospect Discovery**.

Operators never think about Scout. They say “Build Campaign 001 for Anchor Cleaning” and Max begins with profile-driven discovery → verified companies → evidence → confidence → next capability.

## Scope Delivered

- Discovery Profiles as first-class, versioned, tenant-aware business assets
- Mission Planner selects (or synthesizes) a profile from the objective
- Prospect Discovery capability: search → filter/dedupe → verify → rank → review package
- Transparent ranking signals that reference the profile
- Progress events: Searching → Filtering → Verifying → Ranking → Completed
- Review package with summary, ranked list, discovery notes, operator actions
- Postgres migration + in-memory store for profiles
- Fixture provider for deterministic tests (not silent production fabrication)

## Package layout

```text
packages/capabilities/discovery/
  types.js
  seedProfiles.js
  DiscoveryProfileStore.js
  PostgresDiscoveryProfileStore.js
  ProfileSelector.js
  ranking.js
  verification.js
  dedupe.js
  ProspectDiscovery.js
  providers/PlacesProvider.js
  providers/FixtureProvider.js
```

## Operator experience

```text
Build Campaign 001 for Anchor Cleaning
        ↓
Using Discovery Profile: Commercial Cleaning – Manchester
        ↓
Discovering Prospects (Searching… Filtering… Verifying… Ranking… Completed)
        ↓
Review package (approve / exclude / lock / regenerate / continue to enrichment)
```

## Acceptance

- [x] Discovery uses a profile rather than hardcoded targeting
- [x] Profiles are stored as first-class objects (in-memory + Postgres)
- [x] Mission Planner selects or creates a profile
- [x] Rankings reference profile signals
- [x] Profiles are versioned; historical versions remain readable
- [x] Profile modifications require operator approval before becoming active
- [x] Progress visible via capability progress events → mission audit / Operations
- [x] Shortfall returns counts, rejection reasons, and suggested next actions

## Runtime notes

- Live search uses Google Places when `GOOGLE_PLACES_KEY` is set
- Without a Places key, discovery completes with warnings (no fabricated companies)
- Tests inject `useFixture: true` / `FixtureProvider` explicitly
- Optional `DISCOVERY_FIXTURE_FALLBACK=1` enables fixture for local demos only
