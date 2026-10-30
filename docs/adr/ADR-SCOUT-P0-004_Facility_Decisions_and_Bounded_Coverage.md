# ADR-SCOUT-P0-004: Facility decisions and bounded research coverage

Status: implementation for review, default off. Production activation is unapproved.
Related: SPEC-SCOUT-P0-004, SPEC-SCOUT-P0-003, ADR-107.

## Decision

For Tenant 10 research-only cycles, separate business category, premises suitability,
country/state/town eligibility, facility identity, procurement authority and final
research fit. A category or a common domain/address cannot authorize an independent
purchasing opportunity. No new field represents invented size, need or ownership.

Enable the proposed behavior only when the native research-only branch and
`SCOUT_ANCHOR_FACILITY_RECOVERY_ENABLED=true` both apply. This flag grants no research
authority: the original exact revision/mission/grant, page, pause and spending checks
remain mandatory. Default-off behavior and other tenants retain their original path.

The Places provider already owns query generation. On its first market identity
request in the bounded pass, reuse the complete pinned mission's existing concepts
and cities, rather than letting the first segment monopolize the page allowance.
Rank exact approved roots using durable Tenant 10 attempt counts by category, town,
concept and root, then a stable diagonal order. Every attempt still goes through the
same checkpoint session, tenant memory and one-new-page allocation. Unknown,
already-paid or exhausted roots are skipped by their existing gates, never reset.
Store planning rationale and actual selected/skipped query receipts in the cycle.

Preserve the address components and operational status already returned by the
unchanged Places detail field mask. Preserve them through native memory, adapters
and identity resolution. In this isolated path, different Places IDs and distinct
service locations survive domain-based company matching. Original raw memberships
remain unchanged and are the authoritative facility evidence when the existing
research ledger's unique domain/name guard admits only one row.

## Consequences

- Generic/ambiguous evidence remains held. No website fetch is added or authorized.
- Procurement relationships are review hints until explicit verified evidence exists.
- No migration, historical rewrite, grant-field change or parked dependency-key change.
- No automatic promotion of the 34 records; the offline evaluator never writes back.
- Historical screenshots lack raw IDs/types/websites in places. Full-cohort production
  acceptance remains pending the read-only 79-record export and reviewed before/after.
- Disabling the isolated flag restores the prior path. Evidence is retained; paid
  history and consumed grants/cycles must never be reset or refunded.
