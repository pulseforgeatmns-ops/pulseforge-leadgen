# SPEC-252 — Canonical Prepared Outreach Cadence

## Summary

Extends prepared AMO artifacts with a durable, revision-bound outreach sequence schedule so SPEC-251 `ObserveCadence` can resolve follow-up timing without reading legacy templates at OBSERVE time.

## Canonical contract

Paige `variants` contributions carry:

```js
outreachSequence: {
  id?: string,
  channel: 'email',
  calendarDays: true,
  steps: [{ step, day, channel?, variantId?, candidateId? }],
  source: { kind, templateKey?, clientId?, vertical?, normalizedAt }
}
```

Execution approval payloads freeze the same normalized `outreachSequence` at READY authorization.

## Historical backfill (additive only)

Table: `acquisition_mission_prepared_cadence_annotations`

Never mutates:
- execution approval contributions
- outbound execution records
- historical `preparedArtifactRevision`

Annotation `source.kind = historical_backfill` with provenance fields.

## Loader precedence

1. Approval snapshot cadence (present at approval time)
2. Historical cadence annotation for execution/revision
3. Paige contribution cadence
4. unresolved

## Mission-wide cadence assumption

v1 uses **mission `targetSegment` + client catalog** as the homogeneous sequence contract. If Max ranked targets expose conflicting CRM verticals mapped to different templates, PREPARE returns no sequence (`unresolved` at OBSERVE). Candidate-scoped cadence is deferred.

## Observe reaction re-evaluation (SPEC-251 supersession)

Initial webhook reactions remain append-only/idempotent on `(observation_id)` where `evaluation_kind = initial`.

When canonical cadence inputs change after the original observation (historical annotation backfill), `--reEvaluateReactions` inserts an additive supersession row:

- `evaluation_kind = cadence_reevaluation`
- `reevaluation_trigger_kind = historical_cadence_annotation`
- `reevaluation_trigger_id = <annotation.id>`
- `supersedes_reaction_id = <initial reaction id>`

Duplicate webhook replay still inserts no second initial row. Duplicate re-evaluation is idempotent on `(observation_id, reevaluation_trigger_id)`.

Latest effective reaction per observation is selected by highest `evaluation_sequence`, then `at DESC`. Candidate observe state folds from that effective row.

## Validation

Forward missions: cadence on Paige + frozen approval.

Backus historical:

```bash
node scripts/backfillPreparedOutreachCadence.js --confirm-production \
  --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750 \
  --execution-id amo_send_37a03a00-2686-4804-8360-9cf93edb52ba \
  --reEvaluateReactions

node scripts/validateBackusObserveReaction.js --confirm-production \
  --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750 \
  --execution-id amo_send_37a03a00-2686-4804-8360-9cf93edb52ba
```

Success: `validation.effectiveHumanOpenTiming.waitDays = 4`, `kind = wait_until`, `cadenceSource = prepared_sequence`, and `usesEffectiveReevaluation = true` when re-eval rows exist.

Operational follow-up (after re-eval):

- AO lead status `needs_follow_up` + warm open task due on `recommendedTiming.dueAt`
- `prospect_notes` row `note_type = research` with exact timing metadata JSON (`source = spec252_observe_reeval:<annotationId>`)

```bash
node scripts/syncBackusObserveOperationalFollowUp.js --confirm-production \
  --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750 \
  --execution-id amo_send_37a03a00-2686-4804-8360-9cf93edb52ba
```

`--reEvaluateReactions` on the cadence backfill script runs this sync automatically when re-evaluation completes.
