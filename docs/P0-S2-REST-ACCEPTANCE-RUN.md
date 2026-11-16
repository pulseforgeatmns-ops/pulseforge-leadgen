# P0 S2 — REST authorization acceptance, tenant 25

Mission `mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169` · deployment `cf87e494` · commit `98c57d6`.
Companion to `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md` §S2 step 4.

## Status: INCONCLUSIVE — REST step not executed

The 2026-11-14 cloud session had **no authenticated application session** for
production. Its Railway access (account owner, read-only use) cannot produce one
without extracting secrets (`SESSION_SECRET`, `DATABASE_URL`) or forging a
session, which this run forbids. No REST request was sent. No result is claimed.

### Evidence collected in that session (read-only, Railway logs/metadata)

| Check | Result |
|---|---|
| Live deployment of `pulseforge-leadgen` (project `charming-trust`, production) | `cf87e494-26ce-490f-b193-a606f0425f47`, `SUCCESS`, commit `98c57d6306f0…`, created 2026-11-13 12:33:45 UTC |
| HTTP log, path `/api/v1/amo/missions/mission_bdf1e016…/execute`, since deploy | **0 requests** |
| Deploy log, `[amo] execute` / `mission persistence unavailable` / `authentication_error`, since deploy | **0 lines** |
| All non-cron HTTP after 12:15 UTC 2026-11-14 | one `GET /api/v1/intelligence/live` (200) at 12:17 UTC |
| Static review at `98c57d6` | all six intents map to classes in the persisted set (`intentActionClass`); `routeExecutionRequest` evaluates constraints **before** any handler, sender lookup or TME; `fail()` maps `authorization_constraint_blocked` → 403 |

## Running it from the Android tablet (existing signed-in session)

The script runs same-origin in the tablet browser, so it uses the session
cookie the browser already has and never reads it. It halts rather than
just logging errors:

- Preconditions **throw** and nothing is sent:
  - the origin is production, and the active tenant is 25;
  - `GET …/authorization` shows `reviewOnly: true`, exactly the five classes, and a hash.
    `revision` must be present and equal to the number `0`. If it is missing, `null`, `"0"` or `1`, the run halts.
    Missions always persist `revision` via `mergeAuthorizationConstraints`, so a correct mission passes;
  - `GET /api/v1/amo/missions` lists **exactly one** tenant 25 mission, it is the target mission,
    and its status is exactly `Plan approved (review only)`;
  - `GET …/:id` returns a `mission` object with the same id and status, no pending decision,
    and no synthetic evidence.
  An expired session (redirect to `/login`) halts as `read_non_json`.
- A `confirm()` dialog is required before the first POST.
- Requests are sequential and each is sent once. Nothing is retried, including on a network
  error, because the outcome is then unknown.
- The restriction hash is re-read before each POST. Any change halts the run.
- Any response other than `403` + `authorization_constraint_blocked` halts the run. The remaining intents are not sent.
- Post-checks: the hash, status and mission version are unchanged, there is still one tenant 25 mission, and there is still no pending decision.
- Body is only `{"intent": X, "allowFixtureFallback": false}`.

Steps:

1. In Chrome on the tablet, sign in as **admin**. Select **tenant 25** in the
   client selector, then open `https://pulseforge-leadgen-production.up.railway.app/dashboard`.
2. Bookmark that page (☆). Edit the bookmark: name it `s2run`, and replace
   its URL with the full single line from
   `scripts/s2-rest-acceptance/bookmarklet.txt`. It starts with `javascript:`.
3. On the dashboard tab, type `s2run` into the address bar and tap the
   **bookmark** suggestion. Do not pick the search suggestion. Chrome strips
   `javascript:` when the code is pasted straight into the address bar, so you
   have to run it from the bookmark.
4. Read the precondition panel, then confirm. The panel shows the JSON log.
   On success it is also copied to the clipboard.
5. Paste the log back into the session. Do not re-run after a `HALTED` verdict.
6. An operator with DB access runs `scripts/s2-rest-acceptance/postAudit.sql`.
   It runs in a `READ ONLY` transaction with `run_start` = the log's `startedAt`.

Source: `scripts/s2-rest-acceptance/s2RestAcceptance.js`. Rebuild all three bookmarklets with
`node scripts/s2-rest-acceptance/buildBookmarklet.js` (the script writes the files itself; `--check` verifies they are current).
Tests, with no network to production:
- `node --test scripts/s2-rest-acceptance/bookmarklet.mock.test.js` runs the built `bookmarklet.txt` against a mock. There are 28 cases, and each halt case asserts that no POST was sent.
- `node --test scripts/s2-rest-acceptance/bookmarklets.static.test.js` checks the exact `javascript:` strings: they are fresh, one line, use a safe charset, end with the end marker, parse, and the diagnostics make no network call.
- `node --test scripts/s2-rest-acceptance/bookmarklets.browser.test.js` clicks the exact `javascript:` strings in real Chromium. Every request is intercepted locally.

### Tablet troubleshooting: "s2run is selected, nothing visible happens" (2026-11-16)

If the acceptance script starts, it paints a full-screen panel **before** its first request.
That was true in the earlier build too: it showed a 1280×720 black panel in Chromium.
So "nothing visible" means the script never started. A `javascript:` bookmark can fail silently in these cases:

1. **Parse failure.** If the 11–12 KB line was truncated or altered when it was pasted into the bookmark,
   the whole program is rejected before it runs, including its `.catch`/`alert`.
   The only trace is a console SyntaxError. `bookmarklets.browser.test.js` reproduces this.
   Every built line now ends with `void%20'S2_END_OF_BOOKMARKLET'%3B`. Check the end of the bookmark URL for it.
2. **Chrome Android ignores `javascript:` bookmarks opened from the Bookmarks screen or star menu.**
   It runs one only from the address-bar suggestion list.
3. **Page CSP.** A `script-src` without `'unsafe-inline'` blocks bookmarklets in Chrome, again silently (test reproduces).
   `/dashboard` sets no CSP in this repo. `/admin/scout-research-approval`, the anchor approval page and the identity form do, so run bookmarklets only on `/dashboard`.
4. **Old Chrome:** `async`/`await` needs Chrome 55 or later. `inset` (Chrome 87+) is no longer used.

The acceptance script itself had one blind spot. An unexpected exception (not a `halt`) was reported only by `alert`.
Now it renders `verdict: "SCRIPT_ERROR"` on the panel with `postsSent`.
A startup line is shown before any request (`S2 acceptance bookmarklet STARTED (build <id>) … No request sent yet`).
Its guards, their order and every stop condition are unchanged.

**Bisect with two zero-network bookmarklets.** They make no request, change nothing on the server and contain no intent:
- `diagnostic.bookmarklet.txt` (~1.5 KB, ES5): shows a green box with `S2 DIAGNOSTIC: bookmarklet RAN`, the page path and the Chrome version. Tap the box to close it.
- `sizeprobe.bookmarklet.txt`: the same diagnostic, padded to at least the acceptance bookmarklet's length. The box shows its length.

| diagnostic | size probe | meaning |
|---|---|---|
| no box | — | bookmarklets do not run here (launch method, CSP, page): fix that first |
| box | no box | long-URL paste/storage problem: re-paste from the raw file and check the end marker |
| box | box | the launch path works; re-paste `bookmarklet.txt` and check the end marker. The new startup line proves it started |

### `postAudit.sql` schema validation

- **Timestamps:** `acquisition_mission_contributions`, `acquisition_mission_events` and `acquisition_mission_execution_audit`
  use `at`. `api_usage_events`, `touchpoints` and `pending_comments` use `created_at`.
  `acquisition_missions.payload` is the whole mission object, so `payload->'authorizationConstraints'` is correct.
- **`pending_comments`:** `client_id`, `channel`, `status` and `created_at` are read by live production code
  (`dbClient.js:223`, `services/contentReview.js:46`, `routes/api.js:612–614`). The table has no DDL in the repo.
- **Live check:** the script checks every table and column it uses against the live `information_schema`
  before running any audit query. If one is missing it raises an error and stops (`ON_ERROR_STOP`, exit 3).
  It runs as `BEGIN TRANSACTION READ ONLY … ROLLBACK`, and a missing `run_start` also exits 3.
- **Local validation on PostgreSQL 16:** run against the repo's startup DDL (`ensureAcquisitionMissionSchema`,
  `ensureExecutionAuditSchema`, captured statement by statement) plus `migrations/2026-10-14-api-usage-control.sql`:
  - the full audit passes with exit 0;
  - a missing `pending_comments.created_at`, a missing `pending_comments.client_id`, or
    `acquisition_mission_events.at` renamed each stops at the preflight with no audit output;
  - an `UPDATE` inside the transaction is refused.
  This shows the SQL is correct and fails closed. It does not replace the live preflight, which is the production check.

## Determination rule

- **PASS**: verdict `ALL_SIX_BLOCKED_403` (six 403s with `authorization_constraint_blocked`),
  post-checks unchanged, six `[CER_EXECUTION_AUDIT]` `blocked_by_constraint` log lines and no
  `[amo] execute` errors in the run window, and `postAudit.sql` at its expected values.
- **FAIL**: any intent not blocked (2xx, or a different error or status), or any write,
  provider call or outbound in the audit.
- **INCONCLUSIVE**: a precondition halt, a network halt, or no read-only audit.
