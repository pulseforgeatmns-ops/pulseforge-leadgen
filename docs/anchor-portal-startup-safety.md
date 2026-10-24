# Local startup safety and hardened rollback candidate

Prepared 2026-10-14. Local engineering evidence only. No publication, merge, production inspection, deployment, accounts, credentials, scheduler start, or server.js execution is authorized or performed.

## Change boundary

- Forward base: PR 923 head `96958fc609d5333469a786c9afcbb0047cae124f`.
- Auth, portal, and closer role refreshers now share one pinned-client transaction helper and transaction-scoped advisory key `91720260517`. The third closer writer was discovered in review; leaving it unchanged would preserve the race.
- The helper replaces only the exact app-owned `users_role_check` when PostgreSQL deparses it as a recognized role-only canonical/subset list. Unknown/custom/unvalidated definitions fail closed for operator review. Other checks are retained, including other role-dependent checks. No user rows or custom roles are rewritten. The resolved users table must be public.users.
- Lock wait is bounded at 10 seconds; each statement at 60 seconds. COMMIT/ROLLBACK releases transaction locks; a failed rollback discards that connection. Portal creation and additive upgrades are one transaction with the role refresh, so a later failure restores earlier DDL and the old constraint.
- Portal creation requires public as current schema and rejects existing shadow portal tables. Upgrade verification checks five column types/nullability, purpose default, validated completed_by foreign key, and both valid/ready partial unique author/request indexes. It is not a comprehensive drift auditor for every legacy field, trigger, policy, or permission.
- Portal traffic gets noncached 503 until auth then portal schema initialization completes. Auth/schema failure leaves it closed until an operator fixes the cause and restarts; unrelated app routes and startup routines retain their existing behavior. The gate precedes sessions, request parsing, static serving and routes. Encoded/normalized static aliases are covered, and raw protected API prefixes remain gated even when encoded route parameters normalize outside that prefix. A generic process/port health check remains insufficient.
- Default demo seeding remains disabled; explicit seeding still requires a supplied password of at least 16 characters. No fallback credential is added.

## Tests

Run with the repository dependencies and disposable PostgreSQL binaries available:

`node --test test/anchorPortal*.test.js test/globalProductionStartupSmoke.test.js`

New tests cover 16 concurrent pooled refreshes, actual advisory lock contention across connections, preserved unrelated checks and same-name tables, nonpublic users rejection, failed/partial DDL rollback, no held transaction advisory locks, six concurrent actual portal initializations, seed-free repeatability, preserved legacy history, wrong named index/nullability failure, pending/failed readiness, all three callers, and HTTP static alias readiness. Existing portal acceptance/security/UI suites cover synthetic reports/issues/photos and session/tenant isolation. Final forward portal/global run: 35 passed, zero failed/skipped. Expanded CI-plan, Signal startup/distribution/package, registration and tenant regression: 278 passed, zero failed/skipped. No full monorepo suite or production behavior certification is claimed.

## Data-preserving rollback artifact

The separate hardened rollback candidate is based on exact pre-portal PR base `b1eea811e0f04c4e0d2c34aa61c2b7860d196038`, with only these retained safety changes:

1. Shared transactional role helper for auth, closer and portal.
2. Atomic legacy portal table creation and public-schema checks.
3. Ordered auth/portal readiness gate, including static aliases.
4. Default demo seeding off and removal of fallback password.

Its UI/routes return to the pre-PR version. It does not reverse the additive database migration, drop indexes/columns, delete reports/photos/issues, reset statuses, remove users/assignments, or rewrite author/retry/purpose fields. Older UI does not expose the new fields and may present evidence differently. This is a review candidate, not an approved deployment artifact.

Synthetic real-PostgreSQL validation first initializes the forward schema, writes a labelled report, resolved issue, resolution photo and assignment, then invokes hardened rollback initialization twice. Full row snapshots remain equal, both retry indexes remain valid/unique, and explicit seeding without a password rejects without changing data. The validation script and JSON result accompany the frozen patch artifacts. Whole-server startup and complete legacy UI/browser behavior were not run.

## Operator release/rollback sequence (requires separate approvals)

1. Review exact forward and rollback tree/patch identities and required CI. Capture live deployment identity, schema/role/check definitions, backup/restore readiness, target, operator and lock window through separately authorized inspection.
2. Ensure all role-writing app instances use the shared helper before relying on the concurrency guarantee. Old replicas still run unsafe session-lock writers; mixed-version rollout is not proven safe. Old leaked advisory locks may cause the bounded new initializer to fail closed. Do not kill sessions or alter live locks without approval.
3. Apply the separately reviewed additive migration before exposing forward traffic, using approved migration tooling/timeouts. Do not run server.js as a migration or read-only schema check. Restart still triggers broad existing auth/client/tenant/scheduler behavior outside this patch.
4. Under explicitly approved deployment controls, release the exact reviewed code. Verify authenticated, DB-backed portal reads and absence of readiness errors, not only generic health. No live write/demo provisioning test is implied.
5. If reverting code is approved, stop new portal writes through an approved operational control when correctness is uncertain; deploy the reviewed hardened rollback artifact. Leave additive schema and every new row intact. Verify protected reads and data invariants. Do not deploy raw base, reseed demo identities, restore the whole DB, or drop new columns/indexes as a code rollback shortcut.
6. Schema drift, failed role validation, partial mixed-version state, backup restoration or constraint restoration needs a separately reviewed exact repair and explicit approval. Preserve/reconcile intervening writes. This patch does not authorize production repair.

## Known pre-existing limitations

The unrelated broad application startup still writes schemas/settings and may start workers. Login/auth startup can bootstrap configured admin accounts. The server's existing raw-path HTML authorization checks can be bypassed by encoded static aliases once the readiness gate opens; the static HTML contains no fetched tenant data, and API authorization remains enforced, but this patch is not a claim to fix that independent static-path authorization issue. No production state or Railway release setting has been inspected here.
