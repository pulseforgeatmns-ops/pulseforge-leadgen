# Max canonical lifecycle-event source assessment

## Scout prospect discovered

- Canonical source: successful `prospects` insert in `leadgen.js`.
- Reliability: high; emitted only after the row is returned by PostgreSQL.
- Addressability: exact prospect UUID and configured client ID.
- Timestamp: ingestion time, normalized centrally to UTC.
- Idempotency: `prospect:<prospect UUID>` with event type in the unique identity.
- Status: already wired, shadow-only, safe.

## Scout prospect qualified

- Canonical source: successful `setSetterVisibility` result with `setter_visible=true`.
- Reliability: high for the existing Scout qualification gate.
- Addressability: exact prospect UUID and configured client ID.
- Timestamp: gate completion time, normalized centrally to UTC.
- Idempotency: `setter-visibility:<prospect UUID>` with event type in the unique identity.
- Status: already wired, shadow-only, safe.

## Meeting booked

- Canonical live source: Cal/Bland callback only after a Google Calendar event is created.
- Historical source: `prospects.booked_at`, explicitly marked `historical_backfill`.
- Reliability: high for the live Cal path; the historical field does not identify cancellation/show outcomes.
- Addressability: callback prospect UUID and database-resolved client ID.
- Timestamp: agreed ISO timestamp when supplied, otherwise the Cal audit timestamp.
- Idempotency: canonical call ID for live events; `prospect:<UUID>:booked` for historical backfill.
- Status: already wired, shadow-only, safe.

## Meeting cancelled

- Candidate sources: Google Calendar cancellation webhook or a durable calendar-event row.
- Current repository evidence: no canonical cancellation webhook or durable event identity was found.
- Setter/closer status alone is insufficient because it can be manually edited and lacks a canonical meeting event ID.
- Status: not wired. Smallest safe change is a signed Google Calendar event callback persisted with calendar event ID, prospect UUID, client ID, and cancellation timestamp before Max ingestion.

## Meeting showed

- Candidate sources: a durable closer disposition audit event tied to a booked calendar/call ID.
- Current repository evidence: `closer_status='showed'` can describe the state, but no append-only disposition event with a stable meeting identity was found.
- Status: not wired. Smallest safe change is an append-only meeting disposition record containing calendar/call ID, prospect UUID, client ID, disposition, and observed timestamp.

## Meeting no-showed

- Candidate sources: the same durable closer disposition event proposed for `meeting_showed`.
- Current repository evidence: no canonical no-show event with stable identity and timestamp was found.
- Status: not wired. Do not infer no-show from elapsed booking time or absence of a “showed” status.

Historical and inferred fields must never be reported as live coverage. No calendar, booking, Scout, sequence, or outreach behavior is changed by this assessment.
