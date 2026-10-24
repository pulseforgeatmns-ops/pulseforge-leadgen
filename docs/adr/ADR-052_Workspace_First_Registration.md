# ADR-052 — Pilot 0 Principle

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-19 |
| **Spec** | [SPEC-115](../specs/SPEC-115_Client_Registration_and_Workspace_Provisioning.md) |
| **Related** | [ADR-051](ADR-051_Provision_Tenant_Before_Intelligence.md), Product Constitution §3 Tenancy |

## Context

SPEC-114 / ADR-051 made tenant creation a product action for operators. SPEC-113 compiles market documents into a published AIM. A real customer still cannot complete onboarding without an engineer: inserting a user row, hashing a password in Postgres, activating a tenant by hand, or publishing AIM records outside the product.

Pilot 0 is not a feature checklist. It is a test of whether PulseForge can onboard a customer through its own interfaces.

The earlier ADR-052 draft ("workspace-first public registration") described self-service signup and email verification. Those are **out of scope** for Pilot 0. Admin provisions the tenant and the first user. The client authenticates and is forced to replace the temporary password.

## Decision

**Developer intervention is a product bug.**

During Pilot 0, every manual engineering action required to onboard or operate a client must be treated as a missing product capability. The objective is not merely to validate PulseForge's intelligence, but to validate that the platform itself can onboard, teach, and operate for a real customer through its own interfaces. Each intervention becomes a roadmap item until the entire onboarding journey is self-service.

Concretely:

1. **Admin provisions in the product.** Create tenant, create user, assign the user to the tenant, set a temporary password. No SQL.
2. **First login forces a password change.** `password_change_required` blocks the workspace until the client chooses a new password.
3. **Max does not pretend.** Until Client Intelligence is complete, the only CTA is Begin Client Intelligence.
4. **Intelligence is earned.** No AIM, Scout, or Outreach until the Blueprint is approved. Max answers acquisition questions only after Blueprint Approved **and** Published AIM.
5. **Failures are explicit.** No tenant, no Blueprint, no AIM, and password-change-required each have a product message. Silent failure is a bug.
6. **Self-service signup, email verification, MFA, billing, teams, OAuth, and SSO stay out of scope.**

## Consequences

### Positive

- Pilot 0 customers start without a developer touching Postgres
- Missing steps become product requirements instead of runbooks
- Max, Scout, and Outreach cannot run on an empty or foreign tenant

### Negative / tradeoffs

- An operator must still create the first user (no public registration)
- Temporary passwords are set in the admin UI and must be communicated out of band
- Unverified email is accepted for Pilot 0

### Follow-ups

- [ ] Team invites into the same workspace
- [ ] Domain health and sending-capacity as first-class product surfaces
- [ ] Public registration only after Pilot 0 proves admin-provisioned onboarding
