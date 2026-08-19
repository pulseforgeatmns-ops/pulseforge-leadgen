# SPEC-115 — Pilot 0 Self-Service Client Onboarding

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-19 |
| **Depends on** | [SPEC-114](SPEC-114_Client_Tenant_Creation.md), [SPEC-113](SPEC-113_Acquisition_Intelligence_Compiler.md), [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-112](SPEC-112_Acquisition_Intelligence_Model.md) |
| **ADR** | [ADR-052 Pilot 0 Principle](../adr/ADR-052_Workspace_First_Registration.md) |
| **Pilot 0** | Admin provisions a client in the product. The client logs in, changes the temporary password, and reaches Max → Scout without SQL. |

> **Supersedes** the earlier SPEC-115 draft that described public self-service signup and email verification. Those flows remain in the codebase as unused product surface. They are **explicitly out of scope** for Pilot 0.

## Objective

Allow a real client to go from invited user → operational PulseForge workspace without developer intervention.

Pilot 0 is not validating features. It is validating whether PulseForge can onboard a customer without engineering support.

**Developer intervention is a product bug.**

If an engineer must edit SQL, activate a tenant manually, publish data manually, or modify records — that becomes a product requirement.

## Vision References

- [ADR-052 Pilot 0 Principle](../adr/ADR-052_Workspace_First_Registration.md)
- [ADR-051 Provision Tenant Before Intelligence](../adr/ADR-051_Provision_Tenant_Before_Intelligence.md)
- [SPEC-114 Client Tenant Creation](SPEC-114_Client_Tenant_Creation.md)
- [SPEC-113 Acquisition Intelligence Compiler](SPEC-113_Acquisition_Intelligence_Compiler.md)
- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)

## Problem

SPEC-114 lets an operator create a tenant. SPEC-113 compiles an AIM. CIE produces a Blueprint. A brand-new customer still cannot complete the journey without an engineer:

```text
Developer → SQL → User → Temporary password in Postgres → Login → Manual AIM publish
```

Required:

```text
Admin → Create Tenant → Create User → Assign User → Temporary Password
  → Client Login → Force Password Change → Client Intelligence → Approve Blueprint
  → Upload AIM Documents → Compile AIM → Review AIM → Publish AIM
  → Ask Max → Run Scout → Review Prospects → Approve Outreach
```

No SQL. No code. No Postgres.

## Philosophy

Developer intervention is a product bug.

During Pilot 0, every manual engineering action required to onboard or operate a client must be treated as a missing product capability. The objective is not merely to validate PulseForge's intelligence, but to validate that the platform itself can onboard, teach, and operate for a real customer through its own interfaces.

## Success Definition

A new client should complete this workflow:

```text
Admin
↓
Create Tenant
↓
Create User
↓
Assign User
↓
Temporary Password
↓
Client Login
↓
Force Password Change
↓
Client Intelligence
↓
Approve Blueprint
↓
Upload AIM Documents
↓
Compile AIM
↓
Review AIM
↓
Publish AIM
↓
Ask Max
↓
Run Scout
↓
Review Prospects
↓
Approve Outreach
```

## Scope (v1 thin slice)

### Phase 1 — Authentication

Authenticate users.

Do **NOT** implement self-service registration.
Do **NOT** require email verification.

**Admin creates**

- tenant
- client user
- temporary password

**Client login:** Email · Password

**First login** — instead of entering the workspace:

```text
Welcome to PulseForge.

Before we begin,
please choose a new password.
```

Required: new password.

After success:

```text
Password Updated

Continue →
```

Flag: `password_change_required = false`

### Phase 2 — Workspace Entry

If Client Intelligence is incomplete, Max should not pretend to know the business.

Workspace greeting:

```text
Welcome, Fedir.

Let's begin by understanding your business.

The first step is completing Client Intelligence.

Everything I learn from you
becomes the foundation for
prospecting, reasoning,
and recommendations.
```

Only CTA: **Begin Client Intelligence**

### Phase 3 — Client Intelligence

Operator completes interview. Blueprint generated. Review. Approve.

No AIM. No Scout. No Outreach. Until approval.

### Phase 4 — AIM

Workspace shows Acquisition Intelligence Model status:

- ○ No Documents
- ○ Ready To Compile
- ○ Draft
- ○ Published

Workflow: Upload → Compile → Review → Approve → Publish

### Phase 5 — Max Unlock

Only after **Blueprint Approved** AND **Published AIM** should Max answer acquisition questions.

Otherwise:

```text
I don't know enough yet.

Complete Client Intelligence
and publish your Acquisition
Intelligence Model first.
```

### Phase 6 — Scout

Now discovery begins.

Example: *Find founders struggling with founder dependency.*

Scout → Discovery → Evidence → Ranking → Review

Prospects are tenant-scoped. Never borrowed from another workspace.

### Phase 7 — Outreach

Only unlock when:

- ✓ AIM Published
- ✓ Prospect Approved
- ✓ Domain Healthy
- ✓ Sending Capacity Available
- ✓ Campaign Approved

Emmett governs deliverability. Paige produces messaging. Max orchestrates.

Until those gates pass, the workspace states the missing requirement instead of silently failing.

## Failure States

Instead of silent failures.

| State | Message |
|---|---|
| No Tenant | No active workspace. Select or activate a tenant. |
| No Blueprint | Client Intelligence has not been completed. |
| No AIM | Your Acquisition Intelligence Model has not been published. |
| Password Change Required | Password must be updated before continuing. |

## Out of Scope

- Email verification
- Self-service signup
- MFA
- Billing
- Teams
- Multiple users
- Invite coworkers
- OAuth
- SSO
- Notifications

## Dependencies

- SPEC-114 tenant provision (`/admin/clients`, `POST /api/clients`)
- SPEC-083 Client Intelligence (`/client-intel`)
- SPEC-113 AIC compile / review / publish
- SPEC-112 published AIM as Scout runtime knowledge
- `users` table (admin create / assign / temporary password)

## Architecture

```text
Admin UI
  → tenants (SPEC-114)
  → users (temporary password + password_change_required)

Client session
  → password_change_required? → /change-password
  → workspace/me → lifecycle + AIM status + unlock gates
  → CIE until Blueprint approved
  → AIM workspace until Published
  → Max (acquisition answers)
  → Scout (tenant-scoped prospects)
  → Outreach (gated)
```

Max, Scout, and Outreach fail closed with the explicit messages above. They do not invent a business, borrow another tenant's AIM, or send without campaign approval.

## Data Model

`users` gains:

- `password_change_required BOOLEAN NOT NULL DEFAULT FALSE`

Admin-created and password-reset users start with `password_change_required = true`. Existing operators backfill `false` so the current team is not locked out.

AIC workspaces persist in `aic_workspaces` (payload JSONB) so compile / review / publish survives a process restart. Published AIMs write to `aim_models` with `client_id` set to the tenant — Scout loads only that row.

## Implementation Plan

1. Forced password change on first login
2. Guided workspace greeting (CIE-only CTA)
3. Gate Scout / Outreach / Max acquisition until Blueprint + published AIM
4. Client-accessible AIM workspace (upload → compile → review → publish)
5. Persist published AIM to `aim_models` with `client_id`
6. Tenant-scoped Scout review list
7. Explicit outreach unlock checklist

## Migration Strategy

Idempotent `ALTER TABLE users ADD COLUMN IF NOT EXISTS password_change_required`. Existing rows stay `false`. AIC / AIM tables already exist from SPEC-113.

## Testing

- Admin create user sets `password_change_required`
- First login cannot enter the workspace until password change
- Greeting copy + single CIE CTA
- Max locked without Blueprint + published AIM
- AIM status progression
- Scout writes `client_id` and does not leak cross-tenant
- Failure messages are explicit
- Admin UI and client UI contain no SQL

## Acceptance Criteria

- [x] A developer provisions a brand-new client using only the admin UI
- [x] The client logs in successfully with a temporary password
- [x] The client is required to create a new password
- [x] The client lands in a guided onboarding workspace
- [x] Completes Client Intelligence (existing CIE)
- [x] Approves their Blueprint (existing CIE)
- [x] Uploads AIM source documents
- [x] Compiles, reviews, and publishes an AIM
- [x] Asks Max an acquisition question (unlocked only after Blueprint + published AIM)
- [x] Scout returns tenant-scoped prospects
- [x] No SQL queries, manual database edits, or developer intervention are required anywhere in the flow

## Future Work

- Email verification and self-service signup (explicitly deferred)
- Team invites into the same workspace
- Domain health / sending-capacity product surfaces beyond the unlock checklist
- MFA, billing, OAuth, SSO, notifications
