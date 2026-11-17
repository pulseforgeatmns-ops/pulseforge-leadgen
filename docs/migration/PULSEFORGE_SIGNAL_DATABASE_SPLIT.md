# Pulseforge / Signal database separation: migration gate

Status: DESIGN + SOURCE AUDIT ONLY. No production writes, no new infrastructure, no cutover authorization.

## Confirmed code coupling
- `db.js` constructs the shared Pool from `DATABASE_URL`.
- `routes/signalV1.js:getStore()` explicitly imports `../db` when `DATABASE_URL` exists, so Signal V1 currently shares the Pulseforge DB.
- `services/signalDashboard/store.js` receives an injected pool and writes `signal_dashboard_records` and `signal_dashboard_provider_budget`; callers must be audited before rerouting.
- `standalone/signal-dashboard/migrate.js` currently uses `DATABASE_URL` and runs four dashboard migrations when explicitly approved. Do not run it as part of this audit.
- `migrations/2026-10-25-signal-dashboard-opportunities.sql` defines `signal_opportunity_decisions` and `signal_opportunity_notifications` without declared foreign keys in that migration. This is NOT proof that the live DB has no dependencies.

## Target ownership
Pulseforge: tenants, authentication, agents, prospects, campaigns, billing, outreach and operational audit.
Signal: research observations, caller feeds, market candles, opportunity decisions, dashboard records, positions, provider budgets and Signal-only audit.
Cross-product relationships: stable opaque IDs and authenticated API contracts; no cross-database joins or shared write credentials.

## Read-only discovery gates
1. Execute `scripts/migration/railwaySourceInventory.sql` with read-only Railway credentials. Save output securely; redact secrets and customer data.
2. Produce a table-by-table ownership manifest from actual live schemas and application references. Classify shared tables and every inbound/outbound FK, trigger, view, function, and materialized view.
3. Collect precise relation sizes and row counts for critical tables, growth rates and retention constraints. Estimate restore headroom for *both* databases; do not assume 10 GB is sufficient.
4. Identify all Signal consumers, background workers, migrations, scheduled tasks, and code paths that use the shared Pool. Require independent pool injection and fail-closed startup before any split.
5. Establish backup, restore, row-count/hash reconciliation, cutover and rollback runbooks. Ensure write-freeze/delta replay design before switching sources.
6. Price the dedicated Signal database and confirm authorization under the $68/month base ceiling (or request separate approval).

## No-go conditions
- No dropping, truncating, moving, or archiving production data as a shortcut.
- No copying live Stripe credentials to active staging app routes, no autonomous outbound, no Signal scheduler.
- No public Render PostgreSQL inbound allowlist expansion.
- No cutover or Railway service change until separately authorized after validated restores.

## Next concrete blocker
Railway connector has no SQL query or pg_dump action. Run the inventory through a secure read-only database session or a purpose-built isolated read-only job with approved credentials. Never paste database URLs into chat or commit them to GitHub.
