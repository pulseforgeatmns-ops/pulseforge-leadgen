# ADR-073 — Execution Is Observable

**Status:** Accepted  
**Related:** SPEC-152, SPEC-151, SPEC-147

## Decision

Execution plans are runtime objects. Every execution step produces observable state stored as an event-sourced log. The operator may inspect execution at any time. The planner never reconstructs current execution from memory or inference — **Execution State is the authoritative runtime record**.

## Consequences

- Multi-Intent Execution Planner transitions append immutable events and reproject `ExecutionState`.
- Operator introspection questions (`What are you doing?`, `Why did you stop?`, etc.) read stored Execution State.
- Pause reasons, blocking contracts, and next steps are persisted at the moment execution pauses or blocks.
