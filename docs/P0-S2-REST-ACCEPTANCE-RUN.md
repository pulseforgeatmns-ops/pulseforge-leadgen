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

- Preconditions **throw** and nothing is sent: origin is production; active tenant is 25;
  `GET …/authorization` shows `reviewOnly: true`, exactly the five classes, revision 0, a hash;
  `GET …/:id` shows tenant 25, no pending decision, no synthetic evidence. An expired
  session (redirect to `/login`) halts as `read_non_json`.
- A `confirm()` dialog is required before the first POST.
- Requests are sequential and each is sent once. Nothing is retried, including on a network
  error, because the outcome is then unknown.
- The restriction hash is re-read before each POST. Any change halts the run.
- Any response other than `403` + `authorization_constraint_blocked` halts the run. The remaining intents are not sent.
- Post-checks: the hash and the mission version are unchanged, and there is still no pending decision.
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

Source: `scripts/s2-rest-acceptance/s2RestAcceptance.js`. Rebuild with
`node scripts/s2-rest-acceptance/buildBookmarklet.js > scripts/s2-rest-acceptance/bookmarklet.txt`.

## Determination rule

- **PASS**: verdict `ALL_SIX_BLOCKED_403` (six 403s with `authorization_constraint_blocked`),
  post-checks unchanged, six `[CER_EXECUTION_AUDIT]` `blocked_by_constraint` log lines and no
  `[amo] execute` errors in the run window, and `postAudit.sql` at its expected values.
- **FAIL**: any intent not blocked (2xx, or a different error or status), or any write,
  provider call or outbound in the audit.
- **INCONCLUSIVE**: a precondition halt, a network halt, or no read-only audit.
