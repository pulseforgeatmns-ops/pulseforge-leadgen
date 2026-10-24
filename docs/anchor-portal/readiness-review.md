# Exeter service portal: local readiness review

Target: Phillips Exeter Academy walkthrough, Wednesday 21 October 2026, 2:15 p.m. America/New_York. Base: main b1eea811e0f04c4e0d2c34aa61c2b7860d196038. Local work only; no publication, deployment, live database access, real account setup or real photo upload performed.

## Scope

- Assigned-building portal: Overview, Service Reports, Facility Areas, Recent Activity, Open Issues, Resolved Issues, Report a Problem and Photo Proof.
- Admin/manager and assigned cleaner report authoring: service date, areas, notes, photos and server-recorded completing user.
- Atomic cleaner checklist/notes/photo completion; duplicate completion preserves original timestamp, author and photos.
- Persisted problem image, urgency, type and room; manager In Progress and resolution note/image; client readback.
- Tenant and location authorization, cleaner visit ownership, no arbitrary linked visit from a different building, private parent-resource photo reads, no-store responses, escaped dynamic text and raster-only image sources.
- Idempotent report/problem submit keys, transaction rollback, canceled resolution performs no write, photo size/type feedback and bounded client resizing.
- Default startup demo seeding disabled; fallback demo credential removed. Existing demo accounts are not changed or deleted.
- Additive migration only: completed_by, request_id, issue_type, evidence purpose, unique retry indexes. Existing acknowledged status is preserved in storage and presented as In Progress.

## Review findings addressed

1. Global role-only location access replaced by Anchor tenant checks and assignment validation. Null-client platform admins remain explicitly supported; real demo operator identity/tenant still needs verification.
2. Dynamic HTML text and attributes escaped; photo sources restricted to raster data URLs.
3. Portal JSON body limit scoped to 8 MB before the general parser. Service images max 1.8 MB decoded, three per new report; client resize max 1.5 MB each.
4. Image validation rejects bare signatures/truncation and limits dimensions to 12 MP and 8,192 pixels per side. PNG checks chunks, CRCs, bounded inflation and scanline filters; JPEG/WebP check container/frame structure and dimensions. JPEG/WebP entropy data is not fully decoded/re-encoded server-side: passing validation is not proof of authenticity or a visual-quality review.
5. HTTPS-origin writes compare against validated canonical APP_URL, avoiding reverse-proxy protocol mismatch. Production APP_URL must match the browser's actual origin. Caller-supplied forwarded headers are not trusted for authorization.
6. Issue update defaults are derived under row lock, preventing concurrent partial updates from reopening a resolved issue.
7. Changed issue responses use an exact authorized read; issue history supports filtered status and paginated offsets so resolved records are not hidden by 100 open records.
8. Scope editor is absent from demo navigation. Its legacy replacement endpoint remains available to authorized operators and is outside this change's acceptance claim; it has not been redesigned or transaction-tested here.

## Ten-step rehearsal checklist

“Test-only pass” means executed against real HTTP routes and disposable PostgreSQL or isolated JS assertions. It does not mean a person completed the flow in a browser.

1. Sign in with the intended real demo client. UNVERIFIED: disposable tests create a synthetic local session; production login, password handling and account setup were not exercised.
2. Land on only Phillips Exeter Academy — Demo Building with obvious sample-data notice. TEST-ONLY PASS: assignment, tenant isolation and selected-demo label assertions; visual layout unverified.
3. Open three sample Service Reports with service dates, areas, notes and completed-by names. TEST-ONLY PASS: persisted report data and author checks.
4. Enlarge three illustrative images and close/back without losing navigation. UNVERIFIED BROWSER: image data persists and UI gallery/escaping tests pass; actual display, lightbox and phone layout need rehearsal.
5. Create an admin/cleaner report with photo, then reload. TEST-ONLY PASS: real database report/photo readback; actual mobile file picker and compression display unverified.
6. As client, submit Report a Problem with room, type, urgency and image. TEST-ONLY PASS: issue/photo persistence and atomic invalid-image rejection.
7. As manager, see the problem and set In Progress. TEST-ONLY PASS: authorized role/location workflow; wrong-tenant access denied.
8. Cancel resolution, then save a resolution note and image. TEST-ONLY PASS: cancel/state logic and persisted resolution proof; real browser interaction unverified.
9. Re-enter the client session and see Resolved Issues with resolution note/image. TEST-ONLY PASS: session reload, database readback and history pagination.
10. Repeat on phone and desktop, including interrupted upload, repeated clicks, Back/Cancel and expired login. PARTIAL TEST-ONLY PASS: duplicate lock, stable retry keys, stale render prevention, rollback and expired-session assertions. Actual visual/mobile/desktop acceptance remains blocked.

## Remaining approval and deployment gates

- Review the exact local diff/tree and approve any publication separately. No PR/push has been performed.
- Run CI against the actual published commit; the local full repository test suite was not run.
- Obtain a supported browser/computer environment for actual desktop/mobile rehearsal. Local Chromium cannot create its singleton Unix socket; elevated launch fails environment setup before execution. Do not work around these restrictions or expose the synthetic local session server publicly.
- Verify target deployed commit, canonical APP_URL, schema state and intended operator/client roles/assignments read-only before proposing deployment.
- Approve additive production schema/deployment and any explicitly bounded demo provisioning separately. No real customer data should be copied into the demonstration.
- Check whether old Riverside demo accounts exist and whether their credentials require retirement/rotation. Removing a source-code fallback does not revoke already-created credentials; any credential change must use the approved secure user flow.
- Current local image fixtures are original procedural illustrations with explicit synthetic captions. They demonstrate storage and image rendering contracts, not actual Exeter service or genuine photographic evidence. Replace only with approved illustrative assets or properly authorized photos, preserving honest labels.

## Verification commands

- npm run test:anchor-portal
- node --test test/globalProductionStartupSmoke.test.js test/ciPlanPr.test.js test/signalDistribution*.test.js packages/signal-v1/tests/*.test.js
- npm run test:anchor-portal:browser (prepared; blocked locally before assertions)
- npm run anchor:portal:demo (disposable loopback only; never public)

Final exact test counts and tree/patch checksums accompany the parent handoff.

## Final local verification result

Final combined gate: **235 passed, 0 failed, 0 skipped**, 21 suites. This includes 24 portal tests (10 PostgreSQL acceptance, 7 UI-state/safety, 4 security, 3 existing portal tests), plus 211 Signal/global-entrypoint/CI-plan regressions. JavaScript syntax checks and git diff whitespace checks passed. Independent security review reran 17 acceptance/UI tests and found no additional blocker for the isolated synthetic demo; its deterministic issue ordering and APNG rejection suggestions are included in the final combined gate.

This is not a full repository-suite pass or a browser/mobile pass. Scope of publication review is this portal implementation, additive migration, synthetic local-only fixtures, tests, documentation and focused CI workflow. Publication would create code review artifacts only; merge, deployment, production migration, demo accounts and live uploads still need their separate approvals.
