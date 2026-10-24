# Max test isolation and remaining acceptance gates

The original `npm run test:max` is unchanged and remains required in the existing CI workflows, including the HTTP authorization and cron-route tests. A unit-lane pass is **not** a full Max regression pass.

- `npm run test:max:unit`: explicit unit manifest with the strict all-network guard preloaded in every worker. Swallowed network exceptions still fail the worker at exit.
- `npm run test:max:integration`: unchanged HTTP authorization and cron-route tests, requiring a socket-capable environment. This lane is mandatory before acceptance, not an optional exclusion. Existing full-suite CI executes both files already.
- Existing `test:max:expectations:postgres` and `test:max:attention:postgres` remain separate required database evidence.
- `node scripts/maxTestLanes.cjs --list`: exact expanded manifests. At this snapshot: 135 unit files + 2 integration files = 137 full-suite files. Counts are files, not assertions.

## Unit fixture contract

`workspaceTestHarness.js` gives each workspace a fresh in-memory CIE store and AMO runtime only when the caller has not supplied that dependency. Explicit runtime providers, runtimes, CIE services and memory stores retain identity. LLM use requires an explicit fixture. Direct retrieval helpers share the same CIE contract. Supplied failing services remain failing; empty fixtures are not production-success evidence. The guard remains mandatory and catches unprovided consequential dependencies.

The first validated migration covers 15 workspace fixture families (237 guarded assertions). Mission/runtime and cognition/ownership migrations are separate follow-up groups and require their own acceptance. It is deliberately not a blind global rewrite. Assertion text is unchanged. It does not alter production defaults.

## Known incomplete isolation

The initial operating-evidence capability fallbacks now use explicit unavailable fixtures, and objective rebuilds now preserve named injected dependencies through a separately reviewed dependency-only seam. Caller tenant/event fields cannot override the saved objective identity. Public-runtime workspace dependency forwarding is separately reviewed and covered by construction-time identity/default behavior regressions.

The full strict unit lane now passes locally: 1,554 assertions, zero failures, cancellations or skips. Unit execution removes DATABASE_URL and retains the network-denial guard. Both CI workflows now require the strict unit lane, fixture contracts, original full aggregate, and existing database lanes. CI execution itself remains unverified.

Integration initially found seven actual packet/canary routing regressions. The exact same mission suite passed 71/71 before navigation changes. A separately reviewed ownership correction now preserves all existing assertions: combined mission and routing verification passes 149/149. This does not establish browser rendering or production reliability.

The original full Max command now passes locally: 1,573 assertions across the full 137-file manifest. Its 19 HTTP authorization/cron assertions also pass independently. The HTTP validation permits only literal loopback connections to active fixture-server ports and denies external sockets, TLS, DNS queries, custom resolvers and UDP. The separate guard contracts cover caught-denial failures, post-close ports and host confusion. Strict unit guard behavior is unchanged.

Additional combined local checks: selected prospecting 295/295, default-off scheduler 36/36, fixture contracts 15/15 and loopback guard contracts 16/16. Counts overlap and must not be summed as unique coverage. The selected prospecting command excludes cron by pattern; HTTP cron coverage is stated separately. Expanded legacy Scout suites previously produced the same 29-pass/8-fail result on baseline and first-tranche branch and are not presented as a full Scout aggregate pass.

No live PostgreSQL lane, CI environment parity, actual provider execution/reconciliation, browser rendering, production deployment, sustained reliability, or 95% production routing acceptance is claimed by these local checks. PostgreSQL prerequisites initdb and pg_ctl are absent. The proposed scheduler SQL remains unapplied and the scheduler remains default-off.
