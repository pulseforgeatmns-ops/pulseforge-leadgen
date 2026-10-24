# Phase A2 — Canonical Prospect Workspace and Lifecycle Convergence

Status: implemented, tested, awaiting review approval. Nothing merged.

---

## 1. Architecture changes

### Backend

| Piece | What changed |
|---|---|
| `services/lifecycleService.js` (new) | The single canonical lifecycle writer. `transitionProspectLifecycle()` locks the prospect row, validates the transition, updates `status` + `setter_status` through one mapping layer, writes exactly one `prospect_lifecycle_events` row, reconciles both callback stores, keeps the booked handoff idempotent, and supports idempotency keys. `scheduleProspectCallback()` is the single callback write path (both stores atomically + audit event). |
| `services/prospectWorkspace.js` (new) | The canonical `ProspectWorkspace` read model: identity, normalized phone, canonical stage, legacy fields, callback with precedence + conflict surfacing, next action, merged history (lifecycle events + touchpoints + activity_log), known facts with sources, operator/legacy notes, call attempt summary, opportunity summary, role-derived permissions. |
| `services/callPreparation.js` (new) | Deterministic call preparation from verified prospect facts + per-vertical templates. `generationMode: 'deterministic'`; hypotheses are explicitly labeled and never mixed with facts. No AI calls. |
| `routes/workspace.js` (new) | `GET /api/prospects/:id/workspace`, `GET /api/prospects/:id/call-preparation`, `POST /api/prospects/:id/notes`, `POST /api/prospects/:id/lifecycle`. Tenant-scoped (setter/sales locked to their assigned client; operators use session client) and role-gated. |
| `routes/setter.js` | **Adapter, not rewrite.** `PATCH .../status`, `POST .../call-disposition`, and `PATCH .../callback` keep their exact request/response contracts but now delegate lifecycle writes to the canonical service. Booked side effects were extracted into `applyBookedHandoff()` shared by both the Pipeline move and the `meeting_booked` disposition. |
| `routes/api.js` | Dashboard `dead`/`disqualified` prospect status writes route through the canonical service. Pipeline setter SQL reads the tenant threshold. |
| `utils/lifecycleSchema.js` (new) | Idempotent runtime reconciliation of the Phase A2 schema (mirrors the migration). |
| `utils/phone.js` (new) | One normalization/display/`tel:` path (US 10-digit NANP validation). |
| `utils/qualificationThreshold.js` (new) | Centralized 70/40 thresholds + tenant-configurable `getQueueDisplayThreshold()`. |
| `utils/callDispositions.js`, `utils/setterQuality.js` | Added the `meeting_booked` disposition to the inventory, contracts, and structured-notes validation (requires a next step = meeting details). |
| `server.js` | Mounts `/shared` static assets and the workspace router; runs the lifecycle schema reconciler at startup. |

### Frontend (`public/shared/`, loaded by BOTH HTML shells)

| Module | Purpose |
|---|---|
| `tokens.css` | Shared design tokens (surfaces, brand, stage colors, type, touch-target/z-index scales) with a light-mode override block. |
| `shell.css` | Unified nav strip, call-continuity banner, workspace dialog, focus-visible states, `.pf-sr-only`, live region. |
| `shell.js` | Injects the unified PULSEFORGE navigation (Home / Pipeline / Calls / Customers / Analytics + operations group for admin/manager), tenant context via `/api/me` + `/api/clients`, `#pf-tab=` deep links. |
| `api-client.js` | One fetch wrapper (credentials, JSON, 401 → login) + typed helpers with generated idempotency keys. |
| `prospect-workspace.js` | The reusable Prospect Workspace dialog (opened from Pipeline or Calls): header (company/contact/phone/email/stage/priority/source/last interaction/next action), actions (Call, Copy phone, Log outcome, Schedule callback, Add note, Opportunity), tabs (Overview, Known facts, Call prep, History, Notes, Log outcome, Opportunity). All writes go through the canonical endpoints. |
| `phone.js` | Client mirror of `utils/phone.js` + the dial handoff controller (`sessionStorage` persist before `tel:`, restore banner on `visibilitychange`/`pageshow`/load with Log outcome / Resume workspace / Dismiss, 4-hour TTL). Does **not** attempt to force a native calling app. |
| `lifecycle.js` | Client stage/disposition catalog kept in lockstep with the server (tested). |
| `accessibility.js` | Focus trap + Escape + focus restoration, tablist arrow-key navigation, `aria-live` announcements. |

### Four reported setter problems → fixes

1. **Missing dynamic call context** → `GET /api/prospects/:id/call-preparation` + the workspace Call prep tab (objective, opener, verified facts with sources, discovery questions, labeled hypotheses, objection responses, outcomes).
2. **Mobile dial behavior / lost state** → dial handoff controller persists `pulseforge.activeCall` before opening `tel:` and restores a "Call in progress" banner with an immediate **Log outcome** action on return.
3. **Missing phone visibility** → normalized, clickable phone numbers in the Calls queue, Due Today, pipeline cards, lead detail drawer, workspace header, call prep, outcome view, dashboard prospect modal, and dashboard setter-ops queue.
4. **Disconnected Pipeline/Calls workflows** → both surfaces read the same workspace model and write through the same lifecycle service; a `pulseforge:lifecycle-changed` event refreshes each page after a canonical write.

---

## 2. Database migrations

- `migrations/2026-07-21-phase-a2-canonical-lifecycle.sql` — additive only: `prospect_lifecycle_events`, `prospect_notes`, `clients.setter_qualification_threshold`. No legacy field touched.
- `migrations/2026-07-21-phase-a2-canonical-lifecycle.rollback.sql` — reversible, with a guard that refuses to drop canonical history once real events/notes exist (mirror of the Anchor rollback pattern).
- `utils/lifecycleSchema.js` runs the same DDL idempotently at startup, so deploy order is safe either way.

## 3. Route adapters (legacy contracts preserved)

| Legacy route | Behavior now |
|---|---|
| `PATCH /setter/api/leads/:id/status` | Same request/response; internally `transitionProspectLifecycle` (source `setter_status_endpoint`) + `applyBookedHandoff` for booked. |
| `POST /setter/api/leads/:id/call-disposition` | Same contract (idempotency key, structured notes, Anchor details); internally maps the disposition through `dispositionStageEffects` and calls the same transition service inside the existing transaction, then the same `applyBookedHandoff` for `meeting_booked`. |
| `PATCH /setter/api/leads/:id/callback` | Same contract; internally `scheduleProspectCallback` (both stores + audit event). |
| `PATCH /api/prospects/:id/status` (dashboard) | `dead`/`disqualified` converge on the canonical service; other statuses unchanged. |
| `/setter` page route | Unchanged; user-facing label renamed to **Calls**. |

## 4. Disposition mapping table (reviewed inventory, not guessed)

| Disposition | Canonical stage | Legacy status effect | Extra effects |
|---|---|---|---|
| `voicemail` | contacted | preserve | callback optional |
| `no_answer` | contacted | preserve | callback optional |
| `gatekeeper_relayed` | follow_up | preserve | default next-business-day callback |
| `gatekeeper_blocked` | follow_up | preserve | — |
| `answered_callback` | follow_up | preserve | callback required (SLA) |
| `answered_interested` | follow_up | `status='warm'` | `is_hot=true` |
| `qualified` | follow_up | `status='warm'` (see note) | `is_hot=true` |
| `incumbent_all_set` | follow_up | `status='cold'` | `is_hot=false`, 60–120d nurture callback |
| `answered_not_interested` | dead | `status='dead'` | callback cleared — **UNDER REVIEW** |
| `disqualified` | dead | `status='dead'` | callback cleared |
| `wrong_number` | dead | preserve | phone cleared — **UNDER REVIEW** |
| `disconnected` | dead | preserve | phone cleared — **UNDER REVIEW** |
| `meeting_booked` (new) | booked | preserve | booked handoff, idempotent, no duplicate opportunity |

Notes: production wrote `status='hot'` for `qualified`, which the startup normalizer immediately rewrote to `warm`; the canonical map writes the observable result (`warm` + `is_hot`) directly. The three UNDER REVIEW rows keep production behavior and are listed in `DISPOSITIONS_UNDER_REVIEW` pending an explicit product rule (nurture vs dead; data-remediation state vs dead).

## 5. Callback and note compatibility rules

**Callback read precedence** (`services/prospectWorkspace.js resolveCallback`): pending `setter_callbacks` row (canonical) → `prospects.callback_at` (legacy fallback). If both exist and disagree, the canonical value is served, `conflict: true` and `legacyDueAt` are surfaced, and the workspace shows a visible conflict banner. Nothing is silently overwritten.
**Callback writes**: one service updates both stores atomically and records an audit lifecycle event (`callback_scheduled`, unchanged stage). Legacy storage is not deleted; backfill is a later migration.

**Notes**: new notes write to `prospect_notes` (typed: operator/call/research/system, with author + source). The workspace surfaces four labeled streams: structured operator notes, interaction events, the legacy setter scratchpad (after the `--- setter notes ---` marker, read-only, dashed border + amber label), and the Scout base record. Historical notes are never rewritten; the status endpoint still appends handoff/disqualification text to legacy notes exactly as production did.

## 6. Before/after screenshots

`artifacts/phase-a2/screenshots/`:
- Desktop before: `before-setter-desktop.png`, `before-dashboard-desktop.png`
- Desktop after: `after-calls-desktop.png`, `after-dashboard-desktop.png`, `after-workspace-overview.png`, `after-workspace-callprep.png`, `after-workspace-outcome.png`
- Mobile (390×844): `before-setter-mobile.png`, `after-calls-mobile.png`

Captured against a seeded disposable-Postgres preview (`scripts/phaseA2Preview.js`). Browser smoke test confirmed: unified nav, Calls rename, clickable formatted phones, workspace open → call prep (facts vs amber hypotheses) → no-answer outcome save → stage chip moved to Contacted → attempts incremented → Escape closes → queue metrics updated.

## 7. Changed-file list

New: `services/lifecycleService.js`, `services/prospectWorkspace.js`, `services/callPreparation.js`, `routes/workspace.js`, `utils/lifecycleSchema.js`, `utils/phone.js`, `utils/qualificationThreshold.js`, `public/shared/{tokens.css, shell.css, shell.js, api-client.js, prospect-workspace.js, lifecycle.js, phone.js, accessibility.js}`, `migrations/2026-07-21-phase-a2-canonical-lifecycle{,.rollback}.sql`, `scripts/thresholdDeltaReport.js`, `scripts/phaseA2Preview.js`, `test/{lifecycleConvergence,phoneContinuity,workspaceLifecyclePostgres}.test.js`.

Modified: `routes/setter.js`, `routes/api.js`, `server.js`, `utils/callDispositions.js`, `utils/setterQuality.js`, `utils/setterVisibility.js`, `public/dashboard.html`, `public/setter-dashboard.html`.

Untouched (per safety boundaries): auth model, approvals, revenue feature flags, Max permissions, cron/webhooks, all legacy routes, all legacy data.

## 8. Tests added and results

New suites:
- `test/lifecycleConvergence.test.js` (7 tests, always run) — canonical stage map, full disposition mapping coverage, transition guards, `deriveCanonicalStage` precedence, client/server catalog lockstep, source-level convergence assertions (no independent writers), centralized threshold.
- `test/phoneContinuity.test.js` (6 tests, always run) — server phone utils, browser/server normalization parity (vm), dial handoff persistence + `tel:` navigation, refusal without a dialable number, 4-hour TTL expiry, both shells load the shared foundation + rename.
- `test/workspaceLifecyclePostgres.test.js` (9 subtests, disposable PostgreSQL, gated on `MAX_SMOKE_DISPOSABLE_PG=true`) — workspace shape + normalized phone + permissions; tenant isolation (Tenant A ↛ Tenant B, both directions, including admin client scoping); role access (viewer read yes / write 403, anonymous 401); lifecycle endpoint (dual-field write, single event, idempotent replay, dead-requires-reason); callback dual-store write + canonical-wins conflict surfacing; **meeting_booked → booked with exactly one handoff, idempotent replay, and a subsequent Pipeline booked move producing zero duplicate handoffs**; Pipeline booked reflected in workspace + queue with opportunity present; deterministic call preparation (+ tenant isolation); structured notes write + legacy notes read-only.

Results (2026-07-21):
- `MAX_SMOKE_DISPOSABLE_PG=true npm test`: **345 tests, 340 pass, 2 skipped, 3 fail — all 3 failures are pre-existing** and unrelated: `disposablePostgresHarness.test.js` (2, fail identically on a clean tree in this environment) and `maxTransactionClient.test.js` (passes standalone; fails only under full-suite parallel disposable-PG contention).
- All 22 new Phase A2 tests pass, including the full PostgreSQL integration suite.

E2E scenario coverage: the required flows are covered by the PG HTTP suite (dial → log → converged stage; Pipeline→Booked→one opportunity; meeting_booked→Booked→no duplicate; tenant isolation) plus the browser smoke test above. A scripted keyboard-only Puppeteer run is deferred to Phase B (limitation §10) — the primitives (focus trap, tablist arrows, Escape, live announcements) are implemented and the browser session verified Escape-close and outcome save.

## 9. Threshold-delta report

`node scripts/thresholdDeltaReport.js [--json]` produces, per tenant: current queue count (≥40), count at 70, exclusion delta, and the affected prospect ids. **No production change is made.** The queue filter now reads `clients.setter_qualification_threshold` with the current default 40, so membership is unchanged until the report is reviewed and a tenant value is explicitly set. Run it against the production `DATABASE_URL` and attach the output to the approval.

## 10. Known limitations

1. **Timezone** is not stored per prospect; `prospect.timezone` is `null` in the read model (contract slot reserved).
2. **Notes summary** (`notes.summary`) is `null` — AI summarization is a later phase; the contract field exists.
3. **Opportunity linkage**: where the revenue `opportunities` table/flags don't apply, the booked handoff (`booked_at` + closer assignment + agent_actions card) is reported as the operational opportunity (`stage: 'booked_handoff'`). No `estimatedValueCents` is fabricated.
4. **Keyboard-only E2E** is manual/smoke-verified, not yet a scripted Puppeteer test.
5. **Dashboard nav duplication**: the unified strip renders above each page's existing local nav rather than replacing it (deliberate — removal is Phase B visual work).
6. `scoreCleaningLead` heuristics, Anchor no-closer rules, and all revenue flags are untouched.
7. The three UNDER-REVIEW disposition mappings keep production behavior until product sign-off.
8. Pre-existing test-environment failures listed in §8 were not addressed (out of scope).

## 11. Phase B — visual redesign spec (exact)

**Objective**: with identity/lifecycle now unified, Phase B is purely visual/UX. No new lifecycle writers, no schema changes beyond additive UI preferences.

1. **Replace per-page navigation** with the shared shell nav as the only nav: delete the local topbar/sidebar markup from `dashboard.html` and `setter-dashboard.html`; move theme toggle, clock, client selector, and logout into the shell; nav = Home, Pipeline, Calls, Customers, Revenue (flag-gated), Campaigns, Analytics, Settings, plus an Operations group (Agents, Approvals, Actions, Activity, Users) for admin/manager.
2. **Migrate page styles onto tokens**: replace each page's local CSS variables with `--pf-*` tokens; delete duplicated palette blocks; one typography scale (Bebas display / DM Sans body / JetBrains Mono meta).
3. **Pipeline board rebuild**: render kanban cards from the workspace read model (stage chips, phone, next action, priority); card click opens the shared workspace (retire `openProspectModal` overview in favor of workspace tabs; keep Edit/Assign as workspace actions).
4. **Calls surface rebuild**: queue = Due callbacks → Hot → New, powered by `/setter/api/leads`; row click opens the workspace directly (retire the legacy detail drawer and Log Call modal once workspace parity is signed off — they remain fallbacks until then).
5. **Feed unification**: one activity component with explicit stream types (prospect / system / agent / errors+approvals); Calls defaults to prospect+operator streams; system remediation noise moves to Home/Operations.
6. **Accessibility completion**: scripted keyboard-only Puppeteer E2E (workspace open → disposition save → close), axe-core pass on both shells, contrast audit of `--pf-text-muted` on `--pf-bg`.
7. **Mobile**: workspace becomes a full-screen sheet under 760px; bottom action bar with Call / Log outcome; verify 44px targets.
8. **Definition of done**: zero references to per-page nav classes, Lighthouse a11y ≥ 95 on both shells, all Phase A2 tests still green, before/after visual diff approved.

## 12. Safety boundaries honored

No AI voice calling, no automatic call initiation, no automated email/SMS sends (closer handoff email unchanged and skipped without `BREVO_API_KEY`), Max permissions unchanged, approvals unchanged, tenant isolation strengthened (not weakened), no retention automation, revenue feature flags untouched, no legacy route removed, no destructive data cleanup (rollback guarded).
