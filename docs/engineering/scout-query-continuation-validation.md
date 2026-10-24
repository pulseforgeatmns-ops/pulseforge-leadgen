# Local continuation validation — 2026-10-19

Base: `8cbeba9d1c95d74d6a658108ca9840a7044744ed`.

## Offline checks

```
node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test \
  test/scoutQueryContinuation.test.js \
  test/scoutQueryCheckpoints.test.js \
  test/scoutQueryCheckpointRamp.test.js
```

Acceptance comparison: copy the new continuation test and pure page service into an otherwise untouched detached base worktree and run only the `three restarted cycles` test. Baseline fails specifically at `each cycle produces unseen business identity`, because the second cycle replays the first 20 businesses. The patched test admits 20 new business identities in each of three sessions before reaching finite exhaustion. Neither version calls external APIs.

## Required database check

```
SCOUT_CHECKPOINT_REQUIRE_POSTGRES=true node --test test/scoutQueryContinuationPostgres.test.js
```

Use only the disposable local cluster harness. Do not supply production database environment variables. A skip or inability to start the local server is not a pass.

See SPEC-SCOUT-CONT-001 for the remaining legacy-coverage and rollout dependencies.

## Recorded local results

- 2026-10-19: 17 continuation tests passed with network denied, including the source-fixture 21/54/216 query-count audit.
- Required real PostgreSQL continuation and legacy checkpoint gates passed 2/2, no skips, using PostgreSQL 17 and the existing local TCP-only wrapper. The initial Unix-socket launch failed under the sandbox and was not counted as a pass.
- Final broader affected suites passed 129/129 with network denied, no skips, including query checkpoints/ramp, evidence-native execution, provider receipt continuity, Places cost/failure-cache/request-savings, geography-cycle and tenant-generalization coverage.
- The broader run exposed a pre-existing evidence-native provider fixture that attempted live database spend accounting and omitted HTTP/application success statuses. It failed identically on the base. That fixture now mocks persistence/spend and uses valid provider response statuses; production behavior was not changed for it.
- Independent pagination review caught and verified fixes for partial unstaged results, pre-slice oversized-page detection, and successful search retries. Ticket-store/controller integration needs its own combined-artifact review.
