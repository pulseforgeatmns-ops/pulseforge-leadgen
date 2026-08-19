# SPEC-115 — Client Registration & Workspace Provisioning

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-19 |
| **Depends on** | [SPEC-114](SPEC-114_Client_Tenant_Creation.md), [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-112](SPEC-112_Acquisition_Intelligence_Model.md), [SPEC-104](SPEC-104_Persistent_Operator_Context.md) |
| **ADR** | [ADR-052 Workspace-First Registration](../adr/ADR-052_Workspace_First_Registration.md) |
| **Pilot 1** | A stranger creates an account, provisions a workspace, and reaches Max — no SQL |

## Objective

Allow a brand-new customer to create an account, provision a secure isolated workspace, and begin onboarding without developer intervention.

Registration is not simply account creation. Registration is the beginning of the Client Intelligence lifecycle. Every successful registration ends with a new operator speaking to Max inside an isolated workspace.

## Vision References

- [ADR-052](../adr/ADR-052_Workspace_First_Registration.md)
- [ADR-051](../adr/ADR-051_Provision_Tenant_Before_Intelligence.md)
- [SPEC-114 Client Tenant Creation](SPEC-114_Client_Tenant_Creation.md)
- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- Product Constitution — tenancy and privacy

## Problem

SPEC-114 lets an operator create a tenant. A brand-new customer still cannot:

```text
Developer → SQL → User → Tenant → Login
```

Required:

```text
Sign Up → Verify Identity → Create Workspace → Provision Intelligence → Welcome by Max → Begin Client Intelligence
```

Developer intervention is never required.

## Philosophy

PulseForge is built around businesses, not users. The first thing created is a workspace. Users belong to workspaces — not the other way around.

Max owns onboarding. No checklist. No wizard. The first interaction is Max.

Every workspace begins empty. Max never pretends to know the business.

## Scope (v1 thin slice)

1. Public sign-up: name, email, password (optional phone)
2. Email verification before the session is established
3. Workspace creation: company name, industry, country, time zone (optional website, logo URL, team size)
4. Workspace is created first; the user is then bound to that workspace as `role=client`
5. Automatic provision of empty intelligence namespaces (Blueprint, AIM, Knowledge, Missions, Prospects, Campaigns, Outcomes, Memory)
6. Login lands on the dashboard inside that workspace
7. Max greets with the Client Intelligence opening — one button: **Begin Client Intelligence**
8. After login, resolve User → Workspace → Active Workspace → Blueprint → Published AIM → Reasoning Context
9. Fail closed if the user has no workspace
10. Explicit workspace lifecycle from Registered through Learning
11. Existing operator-created tenants (SPEC-114) stay unchanged

## Out of Scope

- Team invites / role assignment (same workspace, no duplicated intelligence — Future Work)
- Logo file upload (v1 stores a URL)
- Replacing CIE, AIM, or AIC
- Copying Pulseforge / Anchor / MSHI / Fedir seed intelligence
- Outreach, Scout, or campaign execution at registration
- Changing login defaults for operator roles (admin/manager still default `active_client_id = 1` when unbound)

## Registration Flow

### Step 1 — Create Account

Required: Name · Email · Password

Optional: Phone

### Step 2 — Create Workspace

Required: Company Name · Industry · Country · Time Zone

Optional: Website · Logo · Team Size

### Step 3 — Provision Workspace

Automatically create empty:

Workspace · Business Blueprint · Acquisition Intelligence · Knowledge · Missions · Prospects · Campaigns · Outcomes · Memory

### Step 4 — Verify + Login

Verify the email. The user lands inside the dashboard of their workspace.

### Step 5 — Max Greeting

```text
Welcome to PulseForge.
Before I can help you grow your business, I need to understand it.
Everything I recommend will be grounded in what you teach me.
Let's begin with Client Intelligence.
```

One button: **Begin Client Intelligence**

## Runtime State (immediately after registration)

| Surface | Status |
|---|---|
| Workspace | Provisioned |
| Business Blueprint | Not Started |
| AIM | No Published AIM |
| Prospects | 0 |
| Campaigns | 0 |
| Knowledge | 0 |
| Outcomes | 0 |

Nothing fake. Everything earned.

## Authentication

```text
User → Workspace → Active Workspace → Blueprint → Published AIM → Reasoning Context

No workspace → Fail Closed
```

A client-role user is locked to `users.client_id`. Login never defaults that user to Pulseforge (`client_id = 1`).

## Workspace Lifecycle

```text
Registered
  → Provisioned
  → Client Intelligence In Progress
  → Blueprint Approved
  → AIM In Progress
  → AIM Published
  → Prospecting Active
  → Campaign Active
  → Learning
```

Lifecycle is explicit and derived from earned artifacts. Registration writes `provisioned`. Later stages advance only when real Blueprint / AIM / prospect / campaign / outcome records exist.

## Team Growth (Future)

```text
Invite User → Assign Role → Grant Permissions → Same Workspace
```

No duplicated intelligence.

## Max Onboarding Journey

```text
Welcome → Client Intelligence → Approve Blueprint → Upload Market Knowledge
  → Compile AIM → Approve AIM → Publish AIM → Prospect Discovery
  → Campaign Creation → Learning
```

One continuous experience. This spec only owns Welcome → Begin Client Intelligence.

## Data Model

- `users` gains `phone`, `email_verified`, `email_verified_at`
- `account_verification_tokens` holds hashed one-time verify tokens
- `clients.team_size` is optional workspace metadata
- `tenant_workspaces` gains `origin`, `lifecycle`, `campaign_namespace`, `memory_namespace`

Existing operator tenants backfill `origin = operator` and `lifecycle = provisioned`. Existing users backfill `email_verified = true` so the current team is not locked out.

## Acceptance Criteria

- [x] Completely new customer can register without SQL
- [x] Account verification is required before the session
- [x] Workspace is created before the user is bound
- [x] User can login and reach the dashboard
- [x] User can speak with Max
- [x] One button begins Client Intelligence
- [x] Runtime state starts empty (no fake Blueprint / AIM / prospects)
- [x] No workspace fails closed
- [x] No developer intervention or admin tools required

## Pilot 1 Success

Someone who has never seen PulseForge can:

1. Create an account
2. Create their workspace
3. Reach Max
4. Complete Client Intelligence
5. Publish their first Blueprint
6. Build and publish an AIM
7. Ask: *"Max, who should I talk to first?"*

…without a single developer touching Postgres.

Steps 4–7 reuse SPEC-083 / SPEC-113. This spec proves steps 1–3 and the empty foundation for the rest.

## Future Work

- Invite user → assign role → same workspace
- Logo upload storage
- SMS / extra identity factors
- Stop defaulting unbound operator sessions to Pulseforge once every operator always selects a tenant
