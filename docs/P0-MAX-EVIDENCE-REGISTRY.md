# P0-MAX-REASONING-001 PR 4 — EvidenceRegistry

Status: **implemented and tested on a branch. Not merged, not deployed, not enabled.** It is a library, and nothing in production imports it (a structural test enforces this), so behaviour with the feature off is unchanged by construction. No migration. No production file is modified.

The standard it enforces: **Max must never present a plausible explanation as a verified operational fact.**

## Interface (`packages/max/evidence/`)

```js
const registry = new EvidenceRegistry({ pool, now, env, acquisitionMissionRuntime, loaders });
await registry.retrieve({ tenantId, domains, question?, referents? });   // EvidenceReport
await registry.retrieveForFrame(validatedTurnFrame, { tenantId, question, referents });
```

- `domains` must come from `TurnFrame.EVIDENCE_DOMAINS`. Anything else is listed in `rejectedDomains` and never read:
  - `mission_lifecycle`
  - `scout_execution`
  - `outbound_delivery`
  - `outbound_authorization`
  - `crm_pipeline`
  - `agent_execution`
  - `campaign_performance`
  - `operator_decisions`
- Ids are validated (`[A-Za-z0-9_.:-]{1,160}`, at most 25). **No caller and no model supplies SQL.**
- `retrieveForFrame` refuses a foreign-tenant frame, a frame for another tenant, or any frame claiming authority, all before any read.

### EvidenceReport

| Part | Meaning |
|---|---|
| `domains[d].items[]` | **Observed** items. Each carries `source` (name + provenance), `tenantId`, `observedAt` (record time), `retrievedAt`, `freshness` (`ageDays`, `stale`, threshold, basis) and `missing` fields. Prospect items carry no names, emails, phones or notes. |
| `contradictions[]` | Never reconciled (`resolution: 'unresolved'`): `status_conflict` (terminal pipeline vs active setter status, close or booking date vs status), `status_vs_activity` (mission marked active, no recorded activity past 14 days), `mission_vs_agent_log` (a mission records a specialist event in the last 7 days, but `agent_log` has no run of that agent within 7 days of it) |
| `unknowns[]` | Missing or unretrievable evidence, with the reason. "No Scout runs recorded" is an unknown saying absence *does not establish* Scout never ran. |
| `revalidation` | Remembered ids against current records: `current` (with fresh evidence), `not_found_in_tenant` (deleted, transferred or inaccessible, indistinguishable by design), `not_verifiable_within_bound` (bounded read incomplete), `retrieval_failed`. **No replacement record is ever suggested.** |
| `readOnly` | Reads and blocked writes counted by the guard |
| `authority` | Always `{ grantsAuthority: false, executionPermitted: false }` |

`EvidenceDiagnosis` is the **only** place inferences are made. Each inference is `certainty: 'inferred'` and cites the observed item refs it rests on.

- `diagnoseScout(report)`: no or stale runs, failed runs, zero qualified, no recent additions. Scout acquisition state is reported as not consulted (see limits).
- `diagnoseEmmett(report)`: outbound gate mode, program mode, expired authorization, last tick error, failed Emmett runs, no recent email touchpoints, no eligible prospects.
- `rankOpportunities(report)`: open, eligible records by **recorded engagement tier**. Ties share a position. Closed, expired or ineligible records are excluded with a reason.
- `opportunityStaleness(item)`: closed or expired, record age, contact age, no contact date.

Verdicts are `candidate_explanations` or `insufficient_evidence`, never "verified".

## Reuse, and the three new reads

| Domain | Source |
|---|---|
| missions, prospects (aggregate), touchpoints, agent runs, outbound gate | `OperationalInspection.loadOperationalSnapshot` (existing loaders, `classifyMission`) |
| single mission (revalidation) | `OperationalInspection.loadMissionById` |
| named-client refusal | `OperationalInspection.checkNamedClientScope` (before any operating read) |
| prospect records | `ProspectRecordIntelligence.loadRecords` (keyset, ≤ 1,000, tenant-scoped) and `evaluate` (missing info, eligibility, tier) |
| **new**: mission event history | `acquisition_mission_events`, tenant + mission, `LIMIT 50`, at most 10 missions, cached per retrieval (in-process runtime fallback without a DB) |
| **new**: outbound program authorization | `acquisition_outbound_programs`, tenant-scoped, `LIMIT 3` (mode, authorized/expires, last tick, error flag only) |
| **new**: campaign performance | `email_performance` aggregate (one row). Absent table means `unavailable`. |

Every query goes through `OperationalInspection.readOnlyQueryable` (SELECT/WITH only, no row locks). A second boundary drops any row whose `client_id` is not the tenant's. Pool resolution matches `OperationalInspection`: an explicit pool, or `db.js` only when `DATABASE_URL` is set.

## ConversationLedger integration (PR 3, #1133) — contract, not dependency

`LedgerReferentContract.referentsFromMemory()` accepts the plain shape PR 3's `resolveFocus()` returns (`{ referent: { kind, memberIds, missionId }, requiresRevalidation, verified: false }`) without importing ledger code. `revalidateRemembered(registry, { tenantId, memory })` revalidates exactly those ids.

Verified (not committed) in a worktree merging PR 3 with PR 4: a real ledger focus, live and after projection and recovery, is revalidated; ids no longer in the tenant are reported `not_found_in_tenant`.

## Execution boundaries

The registry never writes, never resolves or consumes approvals, never calls the router, TME, Scout, Emmett or outbound control, and never modifies mission state. These are tested at a real pending plan-approval gate: the decision is reported as `pending`, the execution audit stays empty, and the mission, events and contributions are byte-identical. In PostgreSQL every statement is recorded and must be SELECT/WITH, and every table is hashed before and after.

## Tests

- `packages/max/evidence/tests/evidenceRegistry.test.js` (12, hermetic): pending approval, enum domains, frame refusal, invalid tenant, write attempt blocked by the guard, structural scan (no write SQL, no execution calls, no production importer), malformed, throwing and foreign-row loaders, no-DB unavailability, incomplete-read honesty, missing agent logs, ledger contract shape and bounds, tied priorities.
- `test/maxEvidenceRegistryPostgres.test.js` (9 scenarios, disposable PostgreSQL, synthetic fixtures):
  - Levi's five borrower prospects: missing contact, closed, expired, stale, contradiction;
  - restart recheck with a deleted and a transferred record;
  - Anchor top opportunities;
  - Scout and Emmett diagnoses;
  - mission vs agent-log and status vs activity contradictions;
  - pending approval;
  - cross-tenant ids and named-client refusal (only the directory read);
  - absent campaign table and unreachable database;
  - SELECT-only and unchanged-table proof.

## Known limits

- **Scout acquisition state** is not consulted: its store creates its table on read (`ensureTable`), which the read-only guard refuses. Reported as an unknown. A read-only accessor would lift this.
- **The Anchor outbound gate** in the PostgreSQL test is a fixture loader. The real reader is `services/anchorEmailRecovery.loadRecoveryState`, covered by its own suites.
- **Bounded reads.** Prospect reads are capped at 1,000 records. Beyond that, remembered ids are `not_verifiable_within_bound`, never "deleted".
- **Deleted vs transferred vs inaccessible** are indistinguishable by design (tenant isolation): all are `not_found_in_tenant`.
- **Wiring.** Not wired into routing or the composer. That is PR 5 (frame-directed reasoning).
