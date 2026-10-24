# AUDIT-108 / SPEC-226 Result

## Scope and Stop Point

Read-only audit of the CIE operator-message path during Blueprint review. No application, migration, canonical, or test code was changed. The audit stops at the first proven pre-approval semantic divergence.

## First Divergence

`services/clientIntelligenceInterview.js`: `extractNotesIntoSections(notes)` in the resumed Blueprint refinement path.

A normal interview message submitted while the session is in `CLIENT_REVIEW` is rejected by `postInterviewMessage()` as `awaiting_review`. The available free-form review path is:

1. `POST /api/v1/interview/:id/resume` calls `resumeInterview()`.
2. `resumeInterview()` sets `interview_state.refinementPass = true` and returns the session to `DISCOVERY`.
3. `POST /api/v1/interview/:id/message` calls `postInterviewMessage()`.
4. Its `!q && state.refinementPass` branch calls `extractNotesIntoSections(text)`.
5. That function splits text into sentences and assigns at most one sentence per Blueprint section using keyword regexes.
6. Each assigned sentence is passed to `applySectionUpdate(..., 'CLIENT_EDITED', ...)` and then into `normalizedFacts`.

The first semantic divergence is step 5. `extractNotesIntoSections()` has no proposition, prior belief, polarity, semantic slot, taxonomy, or operator intent. It treats every retained sentence as additive business text and routes it using the first matching keyword. A sentence mentioning metrics can therefore be sent to `successMetrics` even when it asserts "not a metric". A sentence naming an offer and outcome cannot reclassify a prior belief.

## Correction Intent Handling

Ordinary discovery answers are classified by `clientIntelligenceReasoning.classifyReasoningMessage()` as `direct_answer`, `add_on`, `correction`, `refinement_feedback`, and other conversation-routing classes. `correction` requires a limited correction preamble or phrase such as `actually`, `correction`, or `replace that`.

During question-bank discovery, `parseCorrectionMessage()` targets one coarse Blueprint section and `applyCorrectionToNormalizedFacts()` merges/replaces field values. This is not semantic correction. During free-form Blueprint refinement, classification is bypassed entirely in favor of `extractNotesIntoSections()`. The structured `POST /api/v1/blueprint/:id/revise` path also accepts section summaries and calls `applySectionUpdate()` without an operation model.

| Operation | Support |
| --- | --- |
| `ASSERT` | Implicit additive ingest only |
| `CORRECT` | Coarse section replacement/merge only |
| `RETRACT` | Missing |
| `RECLASSIFY` | Missing |
| `CLARIFY` | Conversation/probe behavior only; no belief operation |

## Working Semantic State

The current pre-approval authority is mutable `session.interview_state.normalizedFacts`, with parallel derived structures:

- `sectionState`: rendered summaries, confidence, unknowns, evidence IDs.
- `reasoningMemory.acceptedFacts`: append-only accepted text.
- `reasoningMemory.pendingCorrections`: `{ section, substance, status }`, later marked `applied`; it cannot identify or deactivate a prior fact.
- `supplementalContext`: raw correction context, later merged into section prose.
- `business_facts`: appends `BusinessFact` records. Its optional `supersedes` field is never resolved or used as active-belief authority.

There is no active/inactive proposition set, nor a single working-state record containing subject, predicate, old proposition, new proposition, operation, polarity, classification, evidence, confidence, and provenance.

## Stale Belief Cause

`applyCorrectionToNormalizedFacts()` is an imperative field merger. Services, ICP, avoided customers, geography, and metrics use `uniquePush()` or merge behavior. It removes prior content only through a narrow service-specific heuristic, and never deactivates entries retained in `business_facts`, `acceptedFacts`, evidence, or supplemental context.

`generateBlueprint()` reads `normalizedFacts` plus `mergeSupplementalIntoSections()`. `sectionsFromNormalizedFacts()` synthesizes from mutable lists and can fall back to prior section prose. The Executive Business Brief reads those sections and normalized facts, not a latest-active-fact resolver. Stale values can therefore survive into the Brief, readiness/confidence, scorecard inputs, and adapter input.

## Negation Behavior

Negation is not a proposition attribute or correction operation. `classifyEpistemicState()` recognizes selected uncertainty and customer-avoidance phrases, but does not represent `not a metric`, `not geographically constrained`, `not a separate service`, or `we did not establish premium positioning` as retraction/exclusion.

For `successMetrics`, normal ingest and correction handling split retained text into list items and store them as positive candidates. Thus "lead volume is not a success metric" can become a metric string. For target markets, when place extraction finds nothing, `applyCorrectionToNormalizedFacts()` stores the remaining sentence as literal geography. These paths explain both observed failures.

## Reclassification Support

Missing. CIE can route a whole sentence to one of nine presentation sections, but cannot move an identified proposition from `METRIC` to `PAIN`, `OFFER` to `OUTCOME`, or `KNOWN` to `HYPOTHESIS`. Cross-section taxonomy corrections are unsafe because keyword routing selects one section rather than preserving multiple semantic relations.

## Canonical Boundary

SPEC-224 is not the first divergence. `lib/cieCanonicalAdapter.js` reads `blueprint.normalizedFacts`; it does not interpret review text or repair stale belief. It would receive the already-corrupted active-looking state. SPEC-223 has canonical `SUPERSEDES` and `CORRECTION_OF` relations, but those are canonical persistence concepts and must not mutate an unapproved canonical snapshot.

## Minimum Repair Boundary

Add a pre-approval working semantic belief layer at the CIE review/refinement boundary, before `extractNotesIntoSections()` and before `applySectionUpdate()` / `applyCorrectionToNormalizedFacts()`.

It must parse review turns into proposition-level operations with subject and semantic slot, resolvable prior proposition, asserted value or classification, `ASSERT` / `CORRECT` / `RETRACT` / `RECLASSIFY` / `CLARIFY`, explicit polarity with fail-closed ambiguity, evidence, confidence, provenance, and active/superseded state. Blueprint sections, readiness/confidence, scorecard generation, and the SPEC-224 adapter must project only active beliefs. Raw review text and inactive beliefs must not be synthesis inputs.

## Schema Sufficient

NO.

The current `normalizedFacts` object and `BusinessFact` shape do not provide an operational active-belief store. `BusinessFact.supersedes` is an unused optional pointer; it lacks operation, explicit polarity/exclusion, semantic classification, active/inactive status, and reliable prior-fact linkage. The missing capability is a durable pre-approval proposition ledger (or equivalent working-state object) that resolves active semantic belief while preserving superseded/retracted history.

## Validation

`node --test test/clientIntelligenceReasoning.test.js test/clientIntelligenceInterview.test.js` passed: 73 tests, 0 failures. Current correction tests cover simple section-targeted replacements, not the Babrun correction turn, negated metrics, unconstrained geography, reclassification, or stale-belief elimination.