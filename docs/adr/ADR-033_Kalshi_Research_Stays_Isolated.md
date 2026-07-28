# ADR-033 — Kalshi Research Stays Isolated From Production Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-049](../specs/SPEC-049_Kalshi_Research_Package.md) |
| **Supersedes** | — |

## Context

A local Kalshi BTC paper/replay prototype proved the research direction (snapshot capture, fee-aware threshold replay, train/test splits, and deterministic feature reporting). That prototype lived outside the Pulseforge monorepo with no git history. The team needs the code versioned inside Pulseforge without creating a standalone repository and without exposing any live-trading or production execution path.

Pulseforge already runs production Express services on Railway. Accidentally wiring market research into `server.js`, cron routes, credentials, or deploy scripts would violate the research safety boundary.

## Decision

1. House the prototype at `packages/kalshi-research` inside the existing Pulseforge monorepo (no new GitHub repository).
2. Keep the package **research-only**: `paper` and `replay` execution modes only; paper fills only; read-only public market/BTC adapters.
3. Keep the package **completely isolated from production execution**:
   - Not imported by `server.js`, cron, webhooks, Mission Engine, or Capability Registry
   - Not a production npm dependency
   - Not deployed as a Railway/Docker service
   - No live order endpoints, order credentials, or authenticated trading clients
4. Version the package through normal Pulseforge git (feature branch → PR → merge). Do not invent a separate deploy pipeline for it.

## Consequences

### Positive

- Research code is reviewed and versioned with the rest of Pulseforge
- Safety boundary is explicit and discoverable (this ADR + package README/SPEC)
- Feature research and replay tooling remain usable locally without touching production

### Negative / tradeoffs

- Python tooling lives beside a mostly Node monorepo (local venv / pytest, not `node server.js`)
- Docker `COPY . .` may include the package files in the image, but they are inert — they must never become the process entrypoint or a production require

### Follow-ups

- Use `feature-report` to shortlist entry-time features, then encode deterministic strategy rules still inside this package
- Never add `ExecutionMode.LIVE` or order-placement clients without a new ADR that supersedes this one
