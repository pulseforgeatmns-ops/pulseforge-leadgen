# ADR-051 — Provision Tenant Before Intelligence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-114](../specs/SPEC-114_Client_Tenant_Creation.md) |
| **Related** | [ADR-049](ADR-049_Understand_Market_Before_Selling.md), [ADR-050](ADR-050_Compile_Market_Knowledge_Before_Runtime.md), Product Constitution §3 Tenancy |

## Context

PulseForge already has Client Intelligence (who the client is) and Acquisition Intelligence (the market they sell into). Both assume a tenant exists.

Today that tenant is created by a developer: hardcoded `INSERT` rows in `ensureClientArchitecture()`, plus manual SQL for anything new. Fedir — the Pilot 0 AIM client — has a seed model (`clientKey=fedir`) but no live `clients` row. Starting onboarding still requires database work.

The product brief called this SPEC-112. Repository SPEC-112 is the Acquisition Intelligence Model. This decision is numbered **SPEC-114 / ADR-051**.

## Decision

1. **A tenant is provisioned before intelligence begins.** Create Client is a product action, not a migration.
2. **Every tenant starts from the same empty foundation.** No Client Intelligence, no published AIM, no prospects, missions, outcomes, or knowledge.
3. **Namespaces are isolated.** Knowledge, mission, prospect, outcome, and AIM records for a tenant are never visible to another tenant. Platform intelligence is a separate namespace and is never copied in automatically.
4. **Activation is explicit.** Max executes inside the selected `active_client_id`. Missing tenant fails closed: `No active client selected.`
5. **The Fedir AIM seed is not a tenant.** Creating a Fedir client does not attach the hand-authored seed. The operator compiles and publishes an AIM through SPEC-113.
6. **No outreach at provision time.** Enabled agents start at `max` only.

## Consequences

### Positive

- Operators can start Pilot 0 (Fedir) without a developer
- New tenants cannot inherit Pulseforge, Anchor, or seed AIM intelligence by accident
- Max context resolution has a single fail-closed gate

### Negative / tradeoffs

- Existing sessions still default `active_client_id = 1` at login for backward compatibility. Max workspace/context APIs added here fail closed when that session value is absent.
- Hand-authored AIM seeds remain available to the AIM engine for tests; they are not bound to a live tenant until published against that `client_id`.

### Follow-ups

- [ ] AIC publish writes `aim_models.client_id` from the active tenant
- [ ] Stop defaulting new sessions to Pulseforge once operators always select a tenant
