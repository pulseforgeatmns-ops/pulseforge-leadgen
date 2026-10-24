# Phase B — Unified Operator Experience

Status: implemented, tested, awaiting visual/behavioral review approval. **Do not merge until approved.**

Phase A2 remains the lifecycle authority. Phase B redesigns the operator surfaces without enabling autonomous outreach and without weakening auth, approvals, tenant isolation, Max permissions, or revenue flags.

---

## 1. Baseline failure comparison

| Tree | Commit | Result |
|---|---|---|
| Working tree (Phase A2 + B) | `fix/revenue-phase16b-root-cause` + uncommitted Phase A2/B | **337 tests, 323 pass, 0 fail, 14 skipped** |
| Clean protected main | `origin/main` @ `a576755` (worktree `/tmp/pulseforge-phaseb-main-baseline`) | **322 tests, 310 pass, 0 fail, 12 skipped** |

### About the “three failing tests”

Phase A2 previously recorded 3 environment failures under full-suite disposable-PG contention:

1. `disposablePostgresHarness.test.js` — concurrent instance cleanup (×2 under shm pressure)
2. `maxTransactionClient.test.js` — fails only under parallel disposable-PG contention; passes standalone

On this machine (after SysV shared-memory cleanup earlier in the Phase B session) **those failures no longer reproduce** on either clean `origin/main` or the working tree. Signatures from the Phase A2 report still match the historical failure mode; current runs are green on both trees. Phase B did not “fix” those harness issues — the environment stopped triggering them.

Phase B adds tests; it does not change the pre-existing harness.

---

## 2. Changed-file list

### New

| Path | Role |
|---|---|
| `public/shared/{tokens,shell}.{css,js}` | Warm cream / deep navy visual system + unified primary nav |
| `public/shared/{accessibility,api-client,phone,lifecycle,prospect-workspace,activity-panel}.js` | Shared operator foundations |
| `services/{lifecycleService,prospectWorkspace,callPreparation}.js` | Canonical lifecycle + workspace read model + call prep (A2, retained) |
| `routes/workspace.js` | Workspace / lifecycle / notes / call-prep HTTP |
| `utils/{lifecycleSchema,phone,qualificationThreshold}.js` | Schema reconciler, phone, thresholds |
| `migrations/2026-07-21-phase-{a2,b}-*.sql` | Additive lifecycle tables + `lifecycle_reason` |
| `scripts/{thresholdDeltaReport,phaseA2Preview,phaseBCaptureScreenshots}.js` | Read-only threshold report + preview/screenshot harness |
| `test/{lifecycleConvergence,phoneContinuity,workspaceLifecyclePostgres,phaseBLifecyclePostgres,phaseBAccessibility}.test.js` | A2 + B coverage |

### Modified

| Path | Role |
|---|---|
| `public/dashboard.html` | Shared shell; Command Feed → five-filter Activity; theme/identity chrome de-duplicated |
| `public/setter-dashboard.html` | Calls landing, queue/pipeline redesign, Performance secondary view, activity panel |
| `routes/setter.js` | Lifecycle adapters + last disposition / lifecycle_reason / mrr on lead payloads |
| `routes/api.js`, `server.js` | Workspace mount / threshold reads (A2) |
| `utils/callDispositions.js`, `utils/setterQuality.js` | Phase B disposition semantics (nurture / remediation / DNC) |
| `utils/setterVisibility.js` | Threshold centralization (A2) |
| `test/callDispositions.test.js` | Expectations updated for Phase B rules |

Untouched per safety boundaries: approvals, revenue flags, Max permissions, cron/webhooks, legacy route paths, historical note/callback/disposition rows (no destructive rewrite).

---

## 3. Disposition UI matrix

| UI flow id | Server disposition | Canonical stage | Lifecycle reason | Primary fields | Consequence shown to operator |
|---|---|---|---|---|---|
| `no_answer` | `no_answer` | contacted | — | notes, optional callback | Stays in queue |
| `voicemail` | `voicemail` | contacted | — | notes, optional callback | Stays in queue |
| `decision_maker_not_reached` | `gatekeeper_relayed` | follow_up | — | gatekeeper, notes, suggested callback | Follow-up + next-day callback suggestion |
| `callback_requested` | `answered_callback` | follow_up | — | summary*, next step*, callback* | Schedules callback |
| `interested` | `answered_interested` | follow_up | — | summary*, next step*, optional callback | Warm + hot |
| `meeting_booked` | `meeting_booked` | booked | — | summary*, meeting details* | Booked + one closer handoff |
| `answered_not_interested` | `answered_not_interested` | follow_up | **nurture** | summary*, reason*, ~90d callback | Alive nurture — not permanent Dead |
| `wrong_number` | `wrong_number` | follow_up | **data_remediation** | notes | Phone cleared; find new number |
| `disconnected` | `disconnected` | follow_up | **data_remediation** | notes | Phone cleared; find new number |
| `do_not_call` | `do_not_call` | dead | **terminal_suppression** | summary*, verbatim reason*, confirm | Dead + global DNC; no callback |

\*required. Secondary outcomes (gatekeeper blocked, incumbent all-set, qualified, disqualified) remain reachable under “More outcomes”.

---

## 4. Lifecycle reason implementation

Additive column `prospect_lifecycle_events.lifecycle_reason` with check constraint:

- `nurture`
- `data_remediation`
- `terminal_suppression`

Wiring:

- `services/lifecycleService.js` — `LIFECYCLE_REASONS`, disposition map, writer param, event insert
- `utils/lifecycleSchema.js` + `migrations/2026-07-21-phase-b-lifecycle-reasons{,.rollback}.sql`
- `utils/callDispositions.js` / `utils/setterQuality.js` — legacy + quality contracts
- `routes/setter.js` — passes `suppress` / `lifecycleReason`; rejects callback on DNC
- `services/prospectWorkspace.js` — next-action labels by reason
- `public/shared/lifecycle.js` + workspace UI chips

**Product rules honored:** nurture and data remediation are **not** collapsed into permanent Dead. Terminal suppression is Dead **and** `do_not_contact=true`. Plain `disqualified` remains Dead without global DNC.

---

## 5. Responsive behavior report

| Surface | Desktop | Mobile (≤920 / ≤768) |
|---|---|---|
| Shell nav | Full primary links + identity + theme + logout | Horizontally scrollable nav; no duplicate local hamburger for Calls |
| Calls landing | Two-column home grid; Start next call CTA | Stacked cards; 44px Start next call |
| Queue | Redesigned table (why now / next action / due / priority / Call) | Cardized rows via existing responsive table rules |
| Pipeline | Kanban cards without native stage dropdown; Call / Move / Details | Horizontal stage scroll |
| Workspace | Three panes: context+history \| prep \| outcome | Persistent header + phone + Call; Script/Context/History/Outcome tabs; sticky action bar |
| Activity | Right rail, five filters; Calls defaults Prospect+Operator | Bottom sheet / expandable right panel |

---

## 6. Accessibility results

Scripted coverage in `test/phaseBAccessibility.test.js` (6/6 pass):

| Requirement | Coverage |
|---|---|
| Keyboard Tab cycle inside workspace dialog | `trapFocus` Tab / Shift+Tab wrap |
| Escape closes + focus restoration | `trapFocus` Escape + `release()` restores prior focus |
| Tablist arrow / Home / End | `enableTablistKeyboard` |
| Dynamic outcome form announcements | `PulseforgeA11y.announce` on flow change + validation |
| Screen-reader live region | `#pf-live-region` role=status aria-live |
| 44×44 touch targets | `--pf-touch-target: 44px` applied in shell CSS + start-call / sticky actions |
| Dial handoff / return | Retained from Phase A2 `phone.js` (sessionStorage + banner); Continuity tests still green |
| Both shells load shared a11y/workspace | Asserted against `dashboard.html` + `setter-dashboard.html` |

Puppeteer visual capture also exercised workspace open on desktop and mobile viewports (`after-workspace-*.png`).

Full axe-core Lighthouse pass is still recommended as a review gate (see Known limitations).

---

## 7. Threshold delta report (read-only, production)

Generated `2026-07-21T17:28:47Z` against Railway Postgres. **No membership change applied.**

Artifacts: `artifacts/phase-b/threshold-delta.{txt,json}`

| | at ≥40 | at ≥70 | delta | reduction |
|---|---:|---:|---:|---:|
| **TOTAL** | **82** | **71** | **11** | **13.4%** |
| Client 1 Pulseforge | 5 | 4 | 1 | 20% |
| Client 2 MSHI | 8 | 8 | 0 | 0% |
| Client 5 Nashville | 3 | 3 | 0 | 0% |
| Client 10 Anchor Cleaning | 66 | 56 | 10 | 15.2% |

Vertical distribution (scores 40–69):

- Pulseforge: `auto_repair` ×1
- Anchor: `law_firm` ×8, `accounting` ×2

Historical outcomes (latest disposition, scores 40–69):

- Pulseforge: no disposition logged ×1
- Anchor: no disposition logged ×8, `gatekeeper_relayed` ×2

Affected prospect IDs are listed in the JSON artifact (11 total).

---

## 8. Full test results (Phase A2 + B)

Focused Phase B / A2 suites (this session):

- `lifecycleConvergence` — pass
- `callDispositions` — pass (updated for nurture / DNC)
- `phaseBAccessibility` — 6/6 pass
- `phaseBLifecyclePostgres` (`MAX_SMOKE_DISPOSABLE_PG=true`) — 5/5 subtests pass (remediation, nurture, DNC, dead≠DNC, Pipeline/Calls stream parity)
- `workspaceLifecyclePostgres` — previously 10/10 pass (incl. no duplicate handoff)

Full working-tree: **337 / 323 pass / 0 fail / 14 skipped**.

---

## 9. Before / after screenshots

All under `artifacts/phase-b/screenshots/`:

| File | What |
|---|---|
| `before-calls-desktop.png` / `before-calls-mobile.png` | Pre-Phase-B Calls (metric-first / Log Call) |
| `after-calls-desktop.png` / `after-calls-mobile.png` | Calls landing + Start next call + Activity filters |
| `before-dashboard-desktop.png` / `after-dashboard-desktop.png` | Command Center shell |
| `after-workspace-desktop.png` / `after-workspace-mobile.png` | Prospect workspace (3-pane / mobile sticky) |

Capture harness: `node scripts/phaseBCaptureScreenshots.js` (uses `phaseA2Preview.js` + system Chrome).

---

## 10. Known limitations

1. Preview harness can show transient `ERROR` live-state during concurrent schema migrations / deadlocks on disposable PG — production path is single-process; not a product defect.
2. Dashboard local sidebar (Agents/Approvals/…) remains as **secondary** in-page nav under the shared shell; primary IA is the shell strip. Full sidebar retirement is optional polish.
3. Legacy Log Call modal + detail drawer remain as compatibility fallbacks; workspace is the default Call path.
4. Full axe-core / Lighthouse ≥95 not automated in CI yet.
5. Timezone still unset on workspace (`prospect.timezone = null`).
6. Threshold membership unchanged pending separate approval (report only).
7. Screenshot “before” pages are `git show HEAD:…` (branch tip), which already includes Phase A2 shell fragments; true pre-A2 visuals remain in `artifacts/phase-a2/screenshots/before-*`.
8. No autonomous calling, AI voice, auto email/SMS, Max permission changes, approval weakening, tenant-isolation weakening, or revenue-flag changes.

---

## 11. Exact Phase C spec

Phase C owns the remaining primary destinations that Phase B only stubbed in the shell (links exist; dedicated operator experiences do not).

### C1 — Home
- Role-aware landing: setter → Calls summary strip; admin/manager → ops snapshot (approvals pending, agent health, revenue pulse); viewer/client → read-only digest.
- Single composition: next actions + exceptions only (no duplicate full dashboards).
- Widgets read existing APIs only; no new writers.

### C2 — Customers
- Tenant-safe company/prospect directory with workspace deep links.
- Filters: vertical, stage, lifecycle reason, has-phone, due state.
- No bulk autonomous outreach; enrichment remains explicit operator action.

### C3 — Revenue
- Flag-gated. Surface projections / commissions / booked pipeline value already owned by revenue modules.
- Cards link to booked workspace; never invent `estimatedValueCents`.
- Read-only for non-admin unless existing revenue permissions already allow writes.

### C4 — Campaigns
- Emmett sequence + Paige/social approval status board.
- Queue depth, next send windows, approval blockers.
- Approvals remain mandatory; Campaigns must not bypass `pending_comments`.

### C5 — Analytics
- Consolidate email_performance, post_analytics, setter quality metrics into one analytics surface.
- Calls Performance metrics move here as a drill-down; Calls home stays action-first.
- No threshold membership changes without an explicit change-control step (reuse threshold delta report).

### Phase C definition of done
- Every shell nav item lands on a real surface (no soft-404 / hash stubs).
- Shared shell is the only primary nav; page-local sidebars removed or demoted to tertiary tools.
- All Phase A2 + B tests still green; add surface-level smoke tests per destination.
- Visual review against the warm cream / navy / gold system; no pastel CRM drift.
- Still no autonomous outreach.

---

## 13. UX/Product approval checklist (Calls Workspace spec)

| Criterion | Status |
|---|---|
| Real prospect-specific call brief + dynamic script (Why now, facts, opener, discovery questions) | **Met** — `callPreparation.whyNow` + workspace Call brief |
| Visible phone in queue and workspace | **Met** — normalized `tel:` display |
| `tel:` dial handoff with same-prospect restore (“Back from call with [Prospect] — log outcome”) | **Met** — `phone.js` banner + sessionStorage; never FaceTime |
| Readable notes/history (sectioned; notes newest-first with author/date) | **Met** — Notes / Calls / Emails / Lifecycle / System sections |
| Clear outcome consequences + lifecycle labels (nurture / remediation ≠ Dead; DNC confirmed) | **Met** — grouped outcomes + consequence + dynamic save labels |
| Cohesive mobile flow; sticky phone + Call / Save / Queue; no critical action hidden | **Met** — sticky bar swaps to Save on Outcome tab |
| Queue filters: due now, overdue, callbacks, warm, data remediation, nurture | **Met** |
| Row select opens workspace (not Pipeline) | **Met** |
| Score + urgency on queue rows | **Met** |
| Shared global nav is sole primary nav | **Met** (Calls view tabs remain secondary) |

Still awaiting human visual/mobile walkthrough approval before merge.

---

## 14. Safety boundaries honored

No automation of calling, AI voice, auto email/text, Max permission changes, approval weakening, tenant isolation weakening, revenue flag alteration, legacy route deletion, or destructive rewrite of historical notes/callbacks/activity/dispositions.

