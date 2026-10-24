# Babrun read-only diagnostic snapshot plan

Prepared October 15, 2026 against `c8b828993580c605964a50157b5577298b4994c1`.
This document is a plan, not execution approval. No query below was run against production.

## Scope and safeguards

After separate approval, use the already authorized read-only database channel for tenant 13. Do not request or export credentials. Record database observation time, deployed revision, and transaction timestamp. Do not start the application, import app/migration modules, invoke cron/health handlers, or execute sourcing, verification, preparation or sending scripts. Do not change mission criteria, send flags, rates or contact gates.

Use a read-only repeatable-read transaction with short deadlines. Inspect catalog metadata first to verify the named tables/columns and tenant isolation. Abort on missing/different schema; do not migrate it. Set deadlines before executing bounded SELECT statements. Roll back when finished.

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';
SELECT transaction_timestamp() AS observed_at;
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('acquisition_outbound_events', 'acquisition_outbound_programs',
    'acquisition_outbound_preparation', 'acquisition_outbound_envelopes',
    'acquisition_outbound_items', 'specialist_results', 'specialist_delegations',
    'tenant_outreach_messages')
ORDER BY table_name, ordinal_position;
```

## 1. Latest actual control measurements

Use only the durable control event's explicit fields, not Scout ramp logging defaults. Null/missing is unknown, not zero. Measurements reflect the event timestamp, not necessarily current stock.

```sql
SELECT id, created_at, program_id,
  payload->>'cycleId' AS cycle_id,
  payload->'cleanInventoryBefore' AS clean_before,
  payload->'cleanInventoryAfter' AS clean_after,
  payload->'cleanInventoryExclusions' AS exclusion_counts,
  payload->'pendingPrepared' AS pending_prepared,
  payload->'pendingPreparedMeasured' AS pending_prepared_measured,
  payload->'prepareRequested' AS prepare_requested,
  payload->'preparedAdded' AS prepared_added,
  payload->'sentToday' AS sent_today,
  payload->'sentTodayMeasured' AS sent_today_measured,
  payload->'dispatchCapacityNow' AS dispatch_capacity,
  payload->'scoutDiscoveryAttempted' AS discovery_attempted,
  payload->'scoutSearchGeneration' AS search_generation,
  payload->'newPromotions' AS new_promotions,
  payload->'recoveredExisting' AS recovered_existing
FROM acquisition_outbound_events
WHERE tenant_id = '13' AND event_type = 'max_outbound_control'
ORDER BY created_at DESC LIMIT 5;
```

Legacy pendingPrepared may be a fallback zero when envelope reads failed. Missing measurement flags in old events do not establish a successful read. Read present envelope/item status separately. Keep contact-qualified, canonical-clean, pending-prepared and dispatch-capacity counts separate. Recompute qualified/clean inventory only with a separately reviewed, tenant-bounded snapshot that applies the existing mission, evidence, suppression, ownership and prior-attempt predicates; raw verified-email counts are not clean inventory.

## 2. Current program, preparation and envelope stock

```sql
SELECT id, source_mission_id, mode, policy_hash, last_tick_at
FROM acquisition_outbound_programs
WHERE tenant_id = '13' ORDER BY created_at DESC LIMIT 5;

SELECT p.id AS program_id, r.local_day, r.mission_id, r.attempts, r.last_attempt_at
FROM acquisition_outbound_programs p
JOIN acquisition_outbound_preparation r ON r.program_id = p.id
WHERE p.tenant_id = '13'
ORDER BY r.local_day DESC LIMIT 5;

WITH recent AS (
  SELECT id, local_day, status FROM acquisition_outbound_envelopes
  WHERE tenant_id = '13' ORDER BY local_day DESC LIMIT 3
)
SELECT e.id, e.local_day, e.status AS envelope_status, i.status AS item_status,
  count(i.id) AS item_count
FROM recent e
LEFT JOIN acquisition_outbound_items i ON i.envelope_id = e.id AND i.tenant_id = '13'
GROUP BY e.id, e.local_day, e.status, i.status
ORDER BY e.local_day DESC, i.status;
```

An attempted timestamp alone is not send confirmation. Do not count cancelled/expired envelopes as dispatchable. Pending items still require the existing dispatch gates.

## 3. Last actual send evidence

```sql
SELECT id, mission_id, sent_at, status,
  provider_message_id IS NOT NULL AS has_provider_receipt
FROM tenant_outreach_messages
WHERE tenant_id = '13' AND direction = 'outbound'
  AND sent_at IS NOT NULL
ORDER BY sent_at DESC LIMIT 5;
```

Corroborate confirmed-send status and provider receipt with the corresponding governed item/canonical execution using the inspected schema and tenant-scoped identifiers. Do not print address/body/subject or raw provider identifiers. A mailbox IMAP FETCH of an old reply, an attempted item or a scheduled item is not a new send. If status/receipts disagree, report uncertain rather than sent.

## 4. Root Scout outcome

Inspect a bounded batch of tenant-13 `specialist_results` joined to `specialist_delegations` on both delegation ID and tenant ID, specialist Scout, ordered by creation time (limit 5). Verify its mission/cycle correlation against the observed control event; temporal proximity alone is not proof. The schema includes a JSONB `payload` via the separate specialist-result-payload migration, so verify its presence first.

Project only result IDs, timestamps, exact allowlisted statuses, fixed blocker categories and numerical coverage counts. Use the diagnostic helper's fixed code mapping for `errors` and `payload.providerExecution` in a separately reviewed safe read adapter; do not output raw summaries, errors, provider payloads, query strings or source URLs. Distinguish provider denial/quota/unavailable/error, invalid target, insufficient coverage, genuine zero candidates and unknown. Inspect whether `persistCompanies` was reached; default admission zeros do not establish successful external discovery.

## Completion criteria

Report: observation/deployment timestamps; verified qualified/clean/prepared counts or explicit unknowns; latest confirmed send or uncertainty; earliest proven failure in discovery → admission → enrichment → classification → preparation → dispatch; one bounded fix supported by those records. Do not infer the root provider failure from generic `discovery_evidence` or recommend broadening criteria solely from its default message.

```sql
ROLLBACK;
```
