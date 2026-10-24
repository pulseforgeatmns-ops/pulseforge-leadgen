# Anchor Service Assurance reference redesign

## Scope

Local UI-only work based on `3da768ef` (main as available October 15, 2026). No API, database, authentication, deployment, or production configuration changes. The standalone runtime still serves only the existing HTML/JS assets; the header icon is trusted inline SVG and adds no static route or external dependency.

Reference pixels inspected:
- https://goanchorcleaning.com/assets/service-assurance/client-dashboard.png
- https://goanchorcleaning.com/assets/service-assurance/issues-resolution.png
- https://goanchorcleaning.com/assets/service-assurance/cleaner-checklist.png

## Implemented

- Navy horizontal header with working Dashboard, Visits, Issues, Scope, and Account navigation.
- Bold sans-serif headings, compact summary cards, off-white canvas, white bordered panels, gold accents, reference-shaped icon discs and status pills.
- Dashboard with Recent Visits table, Service Scope, facility/contact sidebar, and Recent Quality & Issue Resolution.
- Side-by-side issue report form and issue history, real recorded issue counts, and contact card.
- Cleaner checklist styling and live count/progress of recorded outcomes. Recorded outcomes are deliberately not called completed rooms: exceptions and not-applicable outcomes are included.
- Account view using existing authenticated user and assigned-facility data.
- Optional scope/activity failures preserve available dashboard data. Busy navigation lock, stale-response generation guard, CSRF token, escaped text, photo allowlist, photo lightbox, upload/form behavior and operator actions retained.

## Fidelity boundaries

This is a reference-led redesign, **not a verified pixel-exact match**. Browser rendering is not verified in this executor.

- No architectural hero image or fictional facility photo. The current API has no facility-image field. Missing photos are explicitly labeled. If issue evidence is available it is labeled as issue photo evidence, not a facility photograph.
- Facility address and next scheduled service come from the assigned facility. No invented square footage, building designation, account manager identity, or schedule recurrence.
- Visits API does not provide notes in its summary; the final column opens the actual report instead of fabricating notes.
- Issue counts use actual API counts (open, reported in 90 days, resolved for issues reported in 90 days), not the reference's unsupported response-time or resolution-rate claims.
- Actual report form retains its required area, urgency, emergency notice, supported image formats, and cancellation controls. No fictional acknowledgment timestamps or customer-feedback workflow.
- Cleaner checklist retains accessible select inputs and exception reasons rather than inventing collapsible room groups or unsupported Messages/Resources/Profile navigation.
- Demo labels, assigned-facility picker, account sign-out and role-specific report actions remain visible as required by the working portal.

## Verification

Passed locally:

- `node --check public/anchor-portal.js`
- `node --test test/anchorPortalReferenceUi.test.js test/anchorPortalUiSafety.test.js test/anchorPortalSecurity.test.js test/anchorPortal.test.js test/anchorPortalStartupSafety.test.js`: 27 tests passed.
- `npm run test:global-smoke`: 10 tests passed.
- `git diff --check`.

Aggregate attempts (not passing):

- `npm run test:anchor-portal`: before the final two new unit tests, 25 passed and 12 integration cases failed because PostgreSQL `initdb`/`pg_ctl` were not discoverable. No application assertion failure was established in those blocked cases.
- `npm run test:anchor-portal:standalone`: all 7 cases blocked by the same PostgreSQL prerequisite.
- Local Chromium fixture runner: launch blocked by sandbox socket permissions; escalation failed before command execution. No screenshots were produced and no browser interaction/responsive pass is claimed. The separate cloud browser cannot reach this executor's loopback host and only accepts HTTP(S) navigation.

Existing browser acceptance selectors were updated for the new navigation and heading composition. They still need execution on a supported browser/test host.

## Reproduce visual checks on a supported host

`node scripts/verifyAnchorPortalVisual.js`

Uses a loopback synthetic API and blocks external browser requests. Requires Chromium at `/usr/bin/chromium` or `CHROMIUM_PATH`. Writes screenshots to `/tmp/anchor-portal-reference` or `ANCHOR_SCREENSHOT_DIR`. Checks desktop/mobile widths, Account, visit details and Back, issue history/Cancel, optional scope failure, empty dashboard, and cleaner progress. No live login, credentials, database, or customer data are used.

For a manual local fixture only:

`ANCHOR_FIXTURE_ONLY=1 node scripts/verifyAnchorPortalVisual.js`

Open `http://127.0.0.1:4179/` on that host. Optional `ANCHOR_FIXTURE_ROLE=cleaner` or `operator` selects synthetic role. The fixture serves no mutations. This is not a deployment/preview publishing command.
