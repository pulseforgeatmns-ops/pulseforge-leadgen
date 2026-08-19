# SPEC-114 — Client Tenant Creation & Activation

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-112](SPEC-112_Acquisition_Intelligence_Model.md), [SPEC-113](SPEC-113_Acquisition_Intelligence_Compiler.md), [SPEC-104](SPEC-104_Persistent_Operator_Context.md) |
| **ADR** | [ADR-051 Provision Tenant Before Intelligence](../adr/ADR-051_Provision_Tenant_Before_Intelligence.md) |
| **Pilot 0** | Create **Fedir** through the product — no manual database work |

> **Numbering note:** The product brief called this SPEC-112. Repository SPEC-112 is [Acquisition Intelligence Model](SPEC-112_Acquisition_Intelligence_Model.md). This spec is numbered **114**.

## Objective

Introduce a governed tenant creation workflow that provisions a new PulseForge client workspace **before** Client Intelligence or Acquisition Intelligence begins.

Every client owns:

- Business Blueprint
- Acquisition Intelligence Models (AIMs)
- Prospects
- Missions
- Knowledge
- Outcomes

No business intelligence is shared across tenants unless explicitly published as platform knowledge.

Success for v1: an operator creates Fedir in the product, activates that tenant, and Max executes every request inside `tenant_id = Fedir` with an empty onboarding workspace.

## Vision References

- [ADR-051](../adr/ADR-051_Provision_Tenant_Before_Intelligence.md)
- [Product Constitution](../vision/Product_Constitution.md) — tenancy and privacy
- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-112 Acquisition Intelligence Model](SPEC-112_Acquisition_Intelligence_Model.md)
- [SPEC-113 Acquisition Intelligence Compiler](SPEC-113_Acquisition_Intelligence_Compiler.md)

## Problem

Today a new client requires developer setup:

```text
Client → Manual database work → Developer setup → Begin
```

Required:

```text
Client → Create Tenant → Provision Workspace → Business Blueprint → AIM → Prospecting
```

The Fedir AIM seed (`clientKey=fedir`) is hand-authored market intelligence. It is **not** a live tenant. If an operator has to `INSERT INTO clients` to start Pilot 0, the product has a gap.

## Philosophy

```text
Today
Client → Manual database work → Developer setup → Begin

Desired
Client → Create Tenant → Provision Workspace → Business Blueprint → AIM → Prospecting
```

Every new tenant starts from the exact same foundation. Understanding is not copied from Pulseforge, Anchor, or the Fedir seed.

## Scope (v1 thin slice)

1. Operator creates a client from required + optional fields (no SQL)
2. Automatic provision: Tenant → Workspace → Knowledge / Mission / Prospect / Outcome / AIM namespaces
3. Activate tenant (`session.active_client_id`)
4. Max context resolution: Active Tenant → Blueprint → Published AIM → Knowledge → Mission → Reasoning
5. Fail closed when no tenant is selected: `No active client selected.`
6. Empty initial workspace (CIE not started, no published AIM, empty missions/prospects/outcomes/knowledge)
7. Max onboarding greeting on the tenant dashboard
8. Isolation: no cross-tenant visibility; platform intelligence stays in a separate namespace
9. Admin UI at `/admin/clients` + Create Client control in the shell

## Out of Scope

- Auto-attaching the hand-authored Fedir AIM seed to a newly created Fedir tenant
- Inventing Client Intelligence, geography, or prospects at provision time
- Sharing Pulseforge / Anchor / MSHI intelligence into the new tenant
- Running Scout, Emmett, or outreach as part of create
- Replacing CIE, AIM, or AIC — this only provisions the empty workspace they run inside
- Changing existing seeded clients (Pulseforge, MSHI, Anchor, AS Cleaning)

## Runtime Flow

### Step 1 — Create Client

Required: Company Name · Primary Contact · Email · Industry · Country · Time Zone

Optional: Website · Logo · Phone · Notes

### Step 2 — Provision Tenant

PulseForge automatically creates isolated namespaces. Nothing is copied from another tenant.

### Step 3 — Activate

The operator selects **Current Active Client**. Every Max request then executes inside that `tenant_id`.

## Initial Workspace

| Surface | Status |
|---|---|
| Client Intelligence | Not Started |
| AIM | No Published AIM |
| Missions | Empty |
| Prospects | Empty |
| Outcomes | Empty |
| Knowledge | Empty |

## Tenant Dashboard greeting

```text
Welcome, Fedir.
Let's begin by understanding your business.
The first step is completing Client Intelligence so I can understand your company.
After that we'll build your Acquisition Intelligence Model and begin prospect discovery.
```

## Max Context Resolution

```text
Active Tenant → Business Blueprint → Published AIM → Knowledge → Mission → Reasoning

No tenant → Fail Closed → No active client selected.
```

A published AIM for this tenant is loaded only from tenant-scoped AIM records (`aim_models.client_id`). The in-memory Fedir seed is **not** this tenant's AIM until the operator compiles and publishes one (SPEC-113).

## Governance

| Artifact | Scope |
|---|---|
| Business Blueprint | Tenant-scoped |
| AIM | Tenant-scoped |
| Prospects | Tenant-scoped |
| Campaigns / Missions | Tenant-scoped |
| Outcomes | Tenant-scoped |
| Platform Intelligence | Separate namespace — never automatically shared |

## Data Model

`clients` gains operator-facing columns: `primary_contact`, `industry`, `country`, `timezone`, `logo_url`, `notes`.

`tenant_workspaces` records the provisioned namespaces for one `client_id`. Namespaces are logical isolation keys (`tenant:{id}:knowledge`, etc.), not shared schemas.

## Acceptance Criteria

- [x] Operator creates client without developer intervention
- [x] Tenant automatically provisioned
- [x] Workspace available immediately
- [x] Max recognizes active tenant
- [x] No Client Intelligence present initially
- [x] No AIM initially
- [x] No cross-tenant visibility
- [x] Ready for onboarding

## Pilot 0

Create **Fedir** through the product. Then run onboarding without touching the database:

Create tenant → Activate tenant → Client Intelligence → Upload AIM source documents → Compile AIM → Approve AIM → Publish AIM → Ask Max: *"Find founders likely struggling with founder dependency."*

If any step requires a manual SQL write, that step is the next product gap.

## Future Work

- Bind compiled AIM `client_key` to `tenant_workspaces.aim_namespace` automatically on AIC publish
- Client-role users creating their own tenant — implemented in [SPEC-115](SPEC-115_Client_Registration_and_Workspace_Provisioning.md)
- Logo upload storage (v1 stores a URL)
