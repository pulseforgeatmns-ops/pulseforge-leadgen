# JEV labeled offline evaluation

Run from repository root:

```sh
node --test test/jevEvaluation.test.js
node packages/decision-service/jevEvaluationCli.js /path/to/reviewed-cases.json
```

Input must be a nonempty JSON array. Each case includes a unique `id`, `category`, independently labeled `expected_route`, `label_status` (`human_reviewed` or `unreviewed`), `provenance` (`synthetic`, `historical_redacted`, or `prospective_shadow`), optional `legacy_route`, and optional `jev_decision` conforming to the existing decision schema. Predicted JEV decisions must originate from actual JEV evaluations to establish model accuracy; do not fabricate them. Never commit private operator prompts or production secrets.

The report separates JEV and legacy accuracy against reviewed labels, gives route confusion and category breakdowns, flags incorrectly recommended execution-adjacent routes, and explicitly declares `production_acceptance_eligible: false` and `activation_authorized: false`. Missing predictions and unreviewed cases do not count as JEV successes.

This evaluates stored decisions only. It never calls JEV, Max, the database, or external services, and cannot mutate routing or execution. Next evidence step: collect consented/redacted operator examples, have expected routes labeled independently, run actual JEV predictions through a separate isolated, no-side-effects evaluator, then pass the resulting decisions to this tool. Synthetic test cases only validate the scorer. Actual production seven-day gate acceptance must remain a separately audited, unfiltered prospective cohort and cannot be satisfied by offline replay.
