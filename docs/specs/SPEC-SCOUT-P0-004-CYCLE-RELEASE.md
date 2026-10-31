# SPEC-SCOUT-P0-004: durable per-cycle release

Approved for implementation and tests only. No merge, deployment, migration,
flag change, grant installation, production research or outbound is authorized.

The grant's immutable eight-cycle ceiling is not an execution allowance. A new
grant releases exactly one cycle when the administrator installs it. A separate
append-only ledger records that initial release and every later administrative
release. Existing grants receive no backfilled release or fabricated approval;
without a ledger entry they remain held and must be revoked/replaced through the
existing approval mechanism.

Each subsequent release increases the cumulative allowance by exactly one, only
after the preceding allowance is consumed and no unexpired claim remains. The
administrator must use a live, verified Tenant 10 session, preview the exact
release, and provide a fresh single-use password confirmation bound to the
cycle-release purpose. The audit preserves the administrator, session digest,
grant, old/new allowances, database timestamp, statement and authorization
reference. A stale or replayed approval cannot increase it twice.

Claims and releases serialize on the same grant row within the existing recovery
lock/transaction. A claim counts every consumed cycle, including failed,
abandoned and expired leases, against both the release allowance and the original
ceiling. An allowance hold returns `awaiting_cycle_release`; ceiling exhaustion
retains the existing `research_only_cycles_exhausted` halt. The receipt exposes
both allowance and ceiling, plus the durable release history. Database triggers
also reject out-of-order releases, audit edits/deletion and claims beyond the
allowance, including after an application rollback.

No release changes the immutable grant, revision, expiry, mission, spending
ceiling, tenant or permissions. Every claim still checks the closed recovery
state, provider restrictions, exact revision, account, mission, quarantine,
window and Google budget. No startup DDL or automatic authority renewal.

Acceptance requires native PostgreSQL proof for repeated scheduler invocations,
simultaneous workers, failed claims, restart/retry and expired leases, explicit
cycle-two release, duplicate/stale approvals, invalid/foreign/unverified actors,
single-use purpose binding, expiry, revocation, revision mismatch, unavailable
guards and unchanged downstream/tenant/provider policies. Tests use synthetic
data and denied external transports. Existing eight-cycle coverage tests must
approve each later cycle explicitly.

Deployment is a separate approval: apply the reviewed additive migration with
research execution disabled, verify its guards, deploy and report the new exact
revision. Activation then requires a fresh grant pinned to that revision. Normal
rollback disables research execution, revokes open authority and retains every
audit/claim/spend record and database guard. Do not remove the release guard while
rolling back application code. Physical schema removal is permitted only by the
provided unused-schema rollback, which refuses research history or open grants.
