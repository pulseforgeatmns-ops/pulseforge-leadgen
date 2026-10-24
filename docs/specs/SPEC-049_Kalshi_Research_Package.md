# SPEC-049 — Kalshi Research Package (Monorepo Isolation)

| Field | Value |
|---|---|
| **Status** | Implemented (v1 migration) |
| **Release** | Unreleased |
| **ADR** | [ADR-033](../adr/ADR-033_Kalshi_Research_Stays_Isolated.md) |
| **Package** | `packages/kalshi-research` (`@pulseforge/kalshi-research`) |

## Objective

Migrate the proven Kalshi BTC paper/replay research prototype into the Pulseforge monorepo as an isolated package, preserving deterministic read-only research behavior and forbidding live trading, order placement, and production deployment.

## Vision References

- Product Constitution: research may inform product intelligence; it must not autonomously execute financial risk
- [ADR-033](../adr/ADR-033_Kalshi_Research_Stays_Isolated.md) — isolation decision
- Package product spec: [`packages/kalshi-research/SPEC.md`](../../packages/kalshi-research/SPEC.md)

## Problem

The prototype validated capture → resolve → fee-aware replay → feature-report without a durable home in Pulseforge git. A standalone repo would fragment ownership; wiring it into production would create unacceptable trading risk.

## Scope

- Copy the Python research package, tests, CLI, and package-local docs into `packages/kalshi-research`
- Preserve paper/replay-only execution modes and read-only market adapters
- Document isolation in ADR-033, package README, CURRENT_STATE, and this spec
- Add a monorepo `package.json` metadata stub and root `npm run test:kalshi-research` that invokes pytest locally
- Version via existing Pulseforge git (no new repository)

## Out of Scope

- Live trading, order placement, funded accounts, or trading credentials for submission
- Railway / production deployment of this package as a service
- Importing this package from `server.js`, cron, Mission Engine, or Capability Registry
- Training ML models
- Committing local SQLite research databases (`.db` remains gitignored)

## Dependencies

- Local Python ≥ 3.11 + package `pyproject.toml` deps (httpx, pydantic, sqlalchemy, pytest)
- Does **not** depend on `@pulseforge/*` Node packages
- Must not become a dependency of the root production app

## Architecture

```
packages/kalshi-research/
  kalshi_research/     # Python library + CLI (paper/replay only)
  tests/               # pytest
  SPEC.md              # package product spec (research methodology)
  README.md            # local usage + isolation banner
  pyproject.toml
  package.json         # monorepo metadata only — not a production Node module
```

Isolation rules:

1. No live order client or `ExecutionMode` beyond `paper` | `replay`
2. No production route registration
3. No deploy entrypoint override
4. Research DB stays local (`sqlite:///…`, gitignored)

## Implementation Plan

1. Branch in Pulseforge monorepo
2. Migrate source/tests/docs from the local prototype
3. Land ADR-033 + this monorepo integration spec
4. Verify `pytest` inside the package
5. Commit / PR / merge — **do not deploy**

## Migration Strategy

- Code move only; no production schema migrations
- Operators who used the prototype copy their local `.db` beside the package if needed (not committed)

## Testing

```bash
cd packages/kalshi-research
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
pytest
# or from repo root (requires package venv/deps on PATH):
npm run test:kalshi-research
```

## Acceptance Criteria

- [x] Code lives at `packages/kalshi-research`
- [x] No standalone GitHub repository created for this package
- [x] `pytest` passes for the package
- [x] Package is not wired into production execution or deploy
- [x] Isolation documented (ADR-033 + README + CURRENT_STATE)

## Future Work

- Deterministic feature-based strategy rules driven by `feature-report`
- Optional read-only bridges to Evidence Laboratory / Replay packages — only after an ADR that keeps execution isolation intact
