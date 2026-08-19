# ADR-052 — Workspace-First Registration

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-19 |
| **Spec** | [SPEC-115](../specs/SPEC-115_Client_Registration_and_Workspace_Provisioning.md) |
| **Related** | [ADR-051](ADR-051_Provision_Tenant_Before_Intelligence.md), Product Constitution §3 Tenancy |

## Context

SPEC-114 / ADR-051 made tenant creation a product action for operators. A stranger still cannot start. The first customer experience is still "ask a developer to INSERT a user and a client."

PulseForge is a business intelligence product. If registration creates a user and later maybe a tenant, Max has no workspace to speak into, and login defaults that user toward Pulseforge (`client_id = 1`). That leaks platform intelligence and feels unfinished.

## Decision

1. **The workspace is created first.** Account fields identify the first operator. Workspace fields identify the business. Provisioning creates the tenant and empty intelligence namespaces, then the user row is bound with `role=client` and `users.client_id = workspace`.
2. **Identity is verified before the session.** Email verification is required. An unverified password is not a login.
3. **Client-role login fail-closes without a workspace.** It never defaults `active_client_id` to Pulseforge.
4. **Max owns the first conversation.** After login the dashboard opens with the Client Intelligence greeting and one button. No wizard. No checklist. No invented Blueprint or AIM.
5. **Lifecycle is explicit.** Every workspace records `registered → provisioned → … → learning`. Registration writes `provisioned`. Later stages are earned from real artifacts.
6. **Operator create (SPEC-114) remains.** Self-service registration is `origin=self_service`. Admin-created tenants stay `origin=operator` and keep the SPEC-114 greeting.

## Consequences

### Positive

- Pilot 1 customers start without SQL or admin tools
- Max context has a workspace before any recommendation
- New workspaces cannot inherit Pulseforge intelligence by a login default

### Negative / tradeoffs

- Email delivery depends on the transactional mailer (Brevo). Without it, the verify URL is logged for operators — the account still cannot sign in until the token is used.
- Operator roles still default unbound sessions to `client_id = 1` for backward compatibility.

### Follow-ups

- [ ] Team invites into the same workspace
- [ ] Stop defaulting unbound operator sessions to Pulseforge
