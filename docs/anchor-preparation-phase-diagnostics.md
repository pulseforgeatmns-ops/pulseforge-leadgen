# Preparation failure diagnostics (observation only)

The explicit CRM preparation operation permanently consumes its bounded claim
before the canonical mission runtime is hydrated and the first child stage is
committed. A consumed claim is **not** a completed draft. Strict tenant-wide
hydration can reject inconsistent unrelated historical mission data. A stage
transaction can also fail and roll back all of its child/events/audit writes.
An absent durable child or audit alone does not distinguish those failures.

`anchorCrmPreparationDiagnostics` records one best-effort structured failure
event at the preparation boundary. It does not change validation, locking,
hydration, schema, specialist behavior, consumption, public responses or retry
policy. It grants no execution or recovery authority.

Allowed fields are a fixed event/version, fixed phase, internally derived opaque
operation hash, zero-based bounded stage number and fixed execution intent,
allowlisted error/cause codes and TME error class, and claim outcome. Raw error
messages/stacks, SQL, row contents, mission IDs/payloads, contact data, copy,
operator identifiers, credentials and sessions are not serialized. Unknown
codes remain `unknown`; nested causes are depth/cycle bounded. Logging failures
must never replace the original error.

Claim outcome means:
- `not_attempted`: the operation-consumption call was not entered;
- `unknown`: it was entered but did not return confirmed success (including a
  possible commit followed by response loss);
- `committed`: the consumption call returned success.

These are observations at the failure boundary, not a refund or retry decision.
Only the existing exact authenticated receipt describes current durable state.
A failed or interrupted operation stays non-retryable. No automatic replay,
reset, regrant, extension, substitution or history repair is introduced.

## Interpretation limits

Synthetic historical inconsistency is a reproducible possible cause, not proof
of any previous live failure. A `canonical_hydration` failure with
`MISSION_STATE_INCONSISTENT` identifies that phase in an instrumented invocation;
it does not identify a historical row or authorize modifying it. A
`stage_execution` failure with a persistence class and SQL constraint code is a
different failure despite the same consumed/no-child receipt. No prior generic
failure can be retroactively assigned a cause from these new logs.

Do not weaken strict hydration or edit historical data based on speculation.
Deploying diagnostics or performing any new live attempt requires separate
authorization. Existing consumed operations remain closed.
