# Facility Assessment email safety

The September 29, 2026 Riverside Law burst was caused by test captures using a
mock database while the unmocked notification sender retained Brevo credentials.
`node --test` does not set `NODE_ENV=test`. Tests must not run through
`railway run` or with production credentials.

Before deploying this change, apply
`migrations/2026-09-30-walkthrough-notification-claims.sql`. The sender fails closed
if that table cannot be read/written; the intake still saves the dashboard action.

The notification boundary now blocks Node/Jest/Vitest test runtimes, runtimes
without `NODE_ENV=production`, non-production Railway environments, reserved
example/test email domains, fictional 555-01xx numbers, and explicit `is_test`,
`is_demo`, `is_synthetic`, or `submission_mode=test|demo|preview|seed|synthetic`
markers. Validation preserves these markers. Automated production smoke checks
must set a marker and use a reserved email address. Do not use real prospect
identities for synthetic checks.

`ANCHOR_WALKTHROUGH_NOTIFY_ENABLED=false` pauses these notifications without
disabling lead capture or other Anchor email workflows.

Each real notification must reference an existing tenant-10 walkthrough action.
A PostgreSQL claim deduplicates normalized identical contact/form details for
24 hours across processes and restarts. Provider errors/timeouts keep their
claim, since the provider may already have accepted a message. Inspect the
dashboard action and `walkthrough_notification_claims` before any manual retry.
This suppresses duplicate emails; repeat form submissions remain visible as
separate dashboard actions. Delivery is bounded by a ten-second provider timeout.

Verification (no production credentials; provider calls are mocked):

```
node --test test/walkthroughNotification.test.js test/walkthroughRoutes.test.js
WALKTHROUGH_NOTIFICATION_TEST_POSTGRES=true node --test test/walkthroughNotificationPostgres.test.js
```
