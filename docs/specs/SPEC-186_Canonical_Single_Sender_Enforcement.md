# SPEC-186 — Canonical Single-Sender Enforcement

| Field | Value |
|---|---|
| **Status** | In Progress |
| **Priority** | Critical |
| **Repairs** | AUDIT-085 |
| **Depends on** | SPEC-117, SPEC-071, SPEC-118 |

## Objective

One tenant has one canonical sender identity. Emmett CAPACITY, EXECUTE, the Brevo adapter, and provider-event ingestion all refer to `clients.sender_email` / `sender_name` / `sending_domain`. Environment sender values are not tenant identity.

## Canonical contract

```js
resolveCanonicalSenderIdentity({ tenantId, clientId })
→ {
    tenantId,
    clientId,
    senderEmail,
    senderName,
    sendingDomain
  }
```

Authority: `clients` only. `FROM_EMAIL`, `BREVO_SENDER_EMAIL`, and `hello@gopulseforge.com` must not execute tenant acquisition sends.

CIE `sender_identity` is onboarding context, not execution authority, until persisted into `clients.sender_*`.

## Execution-block conditions

| Condition | Code / result |
|---|---|
| Tenant id missing | `canonical_sender_tenant_required` |
| Client row missing | `canonical_sender_client_not_found` |
| Email, name, or domain missing | `canonical_sender_incomplete` |
| Email domain ≠ `sending_domain` | `canonical_sender_domain_mismatch` |
| Explicit sender object omitted | `canonical_sender_required` |
| CAPACITY missing inbox/domain | `capacity_sender_identity_missing` |
| CAPACITY identity ≠ live canonical | `capacity_sender_identity_stale` |
| Brevo sender inactive / domain unauthenticated | `canonical_sender_not_ready` |
| Provider call without explicit sender | `missing_explicit_sender` |

## Prepared-artifact identity binding

`computePreparedArtifactBinding` includes `senderEmail` and `sendingDomain` from the Emmett CAPACITY payload. Identity drift changes the revision. Live EXECUTE also compares CAPACITY inbox/domain to the resolved canonical sender so a `clients.sender_*` change without regenerating CAPACITY cannot ride an old approval.

## Provider sender

Canonical AMO sends pass:

```js
sender: { email: canonical.senderEmail, name: canonical.senderName }
requireExplicitSender: true
```

The adapter does not choose tenant identity. Env fallbacks remain only for unrelated internal/ops email.

## Webhook mismatch handling

| Event domain vs `clients.sending_domain` | Status | Reputation |
|---|---|---|
| Equal | `match` | Ingest normally |
| Different | `mismatch` | Persist for audit; exclude from Emmett reputation and side effects |
| Absent | `unknown` | Distinct from mismatch; existing safe ingestion continues |

Raw provider payload is never rewritten to the tenant domain.

## Non-goals

Multi-inbox tables, reputation-by-inbox, warmup identity changes, CIE onboarding redesign, unrelated ops senders.
