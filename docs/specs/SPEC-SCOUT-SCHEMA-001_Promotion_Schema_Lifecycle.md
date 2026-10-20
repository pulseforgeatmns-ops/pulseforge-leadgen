# SPEC-SCOUT-SCHEMA-001: Promotion schema lifecycle

Status: In progress, authorized local-only recurring-failure repair. Publication,
merge, deployment, production schema execution, and provider actions are excluded.

## Objective and problem

Correct the inherited `findOrCreateCompanyForClient` schema/advisory lock inversion.
A process-global business-name ensure promise currently resolves after transactional
DDL but before commit. Another transaction can take the company advisory lock and
then wait on that DDL's relation lock while the first waits on the advisory lock.
This can deadlock even when columns already exist. A missing-column DDL rollback
also leaves cached success behind, causing undefined-column errors on later calls.

## Vision references and dependencies

Preserve the safety, explicit tenant scope, and small-diff principles in
`PROJECT_CONTEXT.md`. Depend only on the current main promotion, business-name
helper, and existing disposable PostgreSQL harness. No unmerged admission-policy
changes or research-pause implementation are dependencies.

## Scope and architecture

- Await schema readiness on the owning pool before promotion checks out a client
  or begins its transaction, including pools with only one connection.
- Coalesce only in-flight ensures per query object. Do not retain settled success
  across transactions, rollback, sequential client search-path changes, or independent pools.
- Check the actual visible `companies` relation's existing columns through
  `pg_catalog`; perform the existing idempotent ALTER only when columns are missing.
- Keep direct transaction-client compatibility. Such an ensure reports readiness
  in that transaction only; other query objects never consume its pending promise.
- Preserve advisory-lock ordering, dedup, tenant predicates, error propagation,
  company fields, provider guards, and transaction ownership.

## Data model and migration strategy

No new columns, indexes, migrations, endpoints, or maintenance workflows. Existing
lazy DDL remains supported and can take an exclusive relation lock when required.
A successful pool ensure completes its autocommit before company transaction work.
A caller-owned transaction retains responsibility for its own commit or rollback.

## Implementation and testing

First add failing deterministic native PostgreSQL tests with controlled query
barriers, then repair the helper and promotion ordering. Cover missing and existing
columns, actual parallel company transactions, committed visibility, rejected
ensure retry, rollback/cache reset, independent pools/search paths, max-one pool,
canonical dedup, tenant isolation, and propagated SQL/identity failures. Run adjacent
regressions and replay the exact repair over frozen PR967 without modifying it.

## Acceptance criteria

Both historical deterministic deadlocks must become successful deduplicated calls;
rolled-back schema creation must not poison later ensures; all new native tests
must run without skips. Freeze exact source trees, patch checksum, commands, results,
and known unrelated failures. Keep all work local and distinguish local synthetic
commit provenance from the upstream base commit.

## Risk, rollback, and future work

Pool connections must use a consistent search_path: a pool's catalog query and ALTER
can run on different sessions. Mixed per-session search paths within a single pool
are not supported or solved here. Explicit clients retain their own search path.
The added catalog read is a small per-ensure cost. Missing schema still causes lazy
DDL blocking; existing columns avoid unnecessary ALTER locks. The change does not
solve arbitrary caller-created transaction lock inversions or external concurrent
schema removal. Reverting the code can restore the inherited race; it does not undo
existing columns or authorize a production rollback. Any future migration-only
schema lifecycle is a separate design and approval.
