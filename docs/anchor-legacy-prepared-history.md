# Anchor preparation: non-executable legacy history

## Failure mechanism

Prepared-outreach revision hashing gained the `outreachSequenceSteps` field in
September 2026 (PR #613). Existing execution approvals were deliberately not
rewritten. A historical READY mission can therefore retain a consumed approval
for the earlier hash format and no pending decision, while the current canonical
validator correctly refuses to treat that approval as current authority.

The exact CRM preparation action hydrates tenant history after its one-use claim
commits. Previously, that unrelated historical record aborted strict hydration
before a new child mission existed. Native PostgreSQL differential tests reproduce
this: current-format history permits READY; changing only the historical approval
revision to the cadence-omitted format causes consumed-one/no-child uncertainty.

A bounded read-only audit confirmed this condition in current Anchor history.
It does not retrospectively recover the exception hidden by the original live
attempt's generic response. There is no evidence supporting another migration.

## Narrow compatibility boundary

Only the existing guarded Anchor CRM preparation bridge supplies the internal
hydration context, derived from its locked source and deterministic child IDs.
The context is not a new HTTP input, permission, or alternate execution path.

An unrelated, uncached historical record may be omitted from the active mission
store only when its exact canonical failure is the missing execution decision
at READY and its coherent artifacts match exactly one active consumed approval under the
old cadence-omitted format, but not under the current format. The source and new
child are excluded from this exception. A cached record fails closed rather than
being removed or repaired. All other canonical failures still propagate.

This classification **does not validate a legacy approval**. Current revision
hashing, current execution-approval predicates, source and child validation,
transactional stage commits, first-ID-wins contribution semantics, and default
hydration behavior remain unchanged. Historical missions, approvals, events,
outcomes, and learning are not rewritten. No pending approval is fabricated.

## Unchanged operational boundaries

- Preparation still requires current authenticated exact review and a bounded
  preparation-only grant, all existing identity/source/health/capacity checks,
  and a separate explicit action.
- No sending, discovery, paid provider calls, activation, migration, or automatic
  retry is introduced.
- A consumed claim stays consumed after a failure or revocation. The old failed
  live operation cannot be resumed, refunded, or regranted by this change.
- A cached legacy-format mismatch and unrecognized canonical corruption remain fail-closed
  cases; this patch is not a general history repair or cache recovery mechanism.
- Local synthetic success is not live acceptance. Publication/deployment and any
  fresh live test require their own authorization and current safety checks.
  A future test must use a genuinely distinct eligible operation; changing a
  grant, window, or manufacturing a source solely to evade the consumed key is
  not a supported recovery. If no genuine distinct operation is available,
  live acceptance remains blocked while the closed attempt stays closed.
