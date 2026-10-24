# SPEC-228 RESULT

## FIRST REPAIRED BOUNDARY
`services/clientIntelligenceInterview.js` — `projectWorkingSemanticOperations()` (and its two write entry points, `applyCorrectionToNormalizedFacts()` case `'identity'` and `ingestAnswerIntoNormalizedFacts()` case `'identity'`).

## Root cause
`business_description` was outside the SPEC-226 operation schema. Nothing ever cleared or normalized it, so a description that already carried a `"<name> is a ..."` wrap (written by the structured Save Edits path, `applyCorrectionToNormalizedFacts` case `'identity'`, which stored the operator's raw substance verbatim) survived every correction pass. Downstream identity synthesis (`sectionsFromNormalizedFacts`) always renders `"${name} is a ${description}"`, so the wrap duplicated on every round: `"Babrun is a Babrun is a coaching programs for founders"`.

A second, independent class of defects was found while building a truly production-equivalent fixture: several `reviewCorrectionOperations()` regexes were written against a paraphrased test sentence shape and did not match the actual verbatim production correction turn (colon-based "offer:", plural "businesses"/"transformation areas or outcomes... services", "constraint" vs "constrained", hyphenated "discovery-to-enrollment", natural-language ICP/geography phrasing, non-adjacent "segments to test" clause). These caused several corrections (offer replacement, transformation-area reclassification, geography retraction, ICP segments, differentiation proposition) to silently fail to fire against the real operator utterance, which is exactly the "original fixture not production-equivalent" finding AUDIT-110 flagged for `business_description` and that also applied more broadly to the correction-extraction regex layer.

## Fields/behavior added to correction projection
- `business_description`: new `sanitizeIdentityDescription(name, description)` helper strips a self-referential `"<name> is (a|an) ..."` wrap (and a bare leading name repeat), applied at every write site (`ingestAnswerIntoNormalizedFacts`, `applyCorrectionToNormalizedFacts`) and once more as an invariant at the end of `projectWorkingSemanticOperations()` so it holds regardless of which slots a given correction targeted.
- CORRECT `services` (primary-offer correction) now also replaces `business_description` when the prior description semantically duplicates the pre-correction service confusion, so the identity/offer conflict named in the spec's example can't survive merely because it lived in `business_description`.
- `geography` RETRACT now threads `operation.epistemic_state` through instead of hardcoding `UNKNOWN`; the geography-negation regex now emits `NOT_APPLICABLE` (an existing `EPISTEMIC_STATES` value) for "not a meaningful targeting constraint" language, and `sectionsFromNormalizedFacts()` gained a `NOT_APPLICABLE` rendering branch for `targetMarkets` ("Geography is not currently a meaningful targeting constraint; targeting is based on business stage and characteristics instead."). No new ontology was invented — `NOT_APPLICABLE` already existed and was unused for this case.
- Differentiation RETRACT (`premium positioning`) now also clears `brand_voice` if it independently carries the same stale value.
- New end-of-projection invariant purges any `success_metrics`/`geography`/`differentiation`/`brand_voice` value containing raw correction-instruction leakage phrases (`"do not interpret"`, `"not a (success) metric"`, `"did not establish"`, `"not established"`, etc.), moving lead-volume-shaped leakage into `excluded_metrics` as the canonical `"raw lead volume"` value instead of the raw sentence.
- `reviewCorrectionOperations()` regex fixes (all additive/broadening, existing SPEC-226 test fixture still passes unchanged except one epistemic-state value):
  - offer extraction accepts `offer: X` (colon) in addition to `offer is/= X`.
  - outcome/transformation-area reclassification now matches plural `"... are transformation areas or outcomes, not separate services"` and extracts the full comma/and-separated list (previously singular-only, previously captured only one item).
  - geography retraction matches `"...targeting constraint"` (noun) as well as `"...constrained"` (adjective).
  - ICP `"operating small business(es)"` now matches the plural.
  - new natural-language founder-bottleneck detector: `"founder ... too central to operations"` (previously only the literal phrase `"founder operational bottleneck"`).
  - ICP segment extraction (`cleaning/home services`, `e-commerce`, `fitness`) is now independent of the `operating small business` sentence and triggers off its own `"segments to test"` / `"initial segments"` clause — previously nested inside the ICP sentence's `if` block and could never fire when segments appeared in a separate sentence.
  - metrics list regex now tolerates hyphens (`discovery-to-enrollment conversion`), previously only matched whitespace/`->`/`→`.
  - learning-signal regex now matches `"pain patterns"` (plural, no trailing "frequency").
  - differentiation-hypothesis value extraction now prefers colon-based extraction (`"... hypothesis: <proposition>"`) instead of a fragile chained-regex strip that could truncate the proposition into garbage on real prose.

## Metadata behavior
`business_description` is now description-only and self-sanitizing; it can never re-enter as `"<name> is a ..."` regardless of entry point (structured edit, first-pass ingestion, or correction projection).

## Business description accumulation eliminated
YES

## Negation active-state behavior
PASS

## Reclassification behavior
PASS

## Geography behavior
PASS

## ICP behavior
PASS

## Differentiation hypothesis preserved
YES

## Hard normalizedFacts state gate
PASS

## Blueprint regeneration gate
PASS

## Raw correction prose leakage
NONE

## Multi-round refinement
PASS (3 cycles; identity, services, and geography stable, no re-accumulation)

## Original SPEC-226 fixture upgraded to production-equivalent
YES — new `test/clientIntelligenceInterview.test.js` describe block `SPEC-228 complete pre-approval correction projection` adds a contaminated fixture (identity accumulation, stale services/metric confusion, literal correction prose stored as geography/metrics, prior Blueprint sections capable of stale fallback, existing `workingSemanticCorrections` history) and applies the exact verbatim production correction turn from the spec. The original SPEC-226 fixture/tests were left in place (still pass) since they remain valid regression coverage for the simpler case; one pre-existing assertion (`epistemic_states.geography === 'UNKNOWN'`) was updated to `'NOT_APPLICABLE'` to reflect the more accurate semantic state.

## Structured Save Edits regression
PASS — `applyCorrectionToNormalizedFacts()` is untouched except for the same `sanitizeIdentityDescription` call on its `'identity'` case; it is never routed through `reviewCorrectionOperations()`. New test confirms a structured identity edit followed by conversational refinement produces one coherent, non-duplicated active state.

## Focused SPEC-228 tests
9 pass, 0 fail (new `SPEC-228 complete pre-approval correction projection` describe block)

## CIE regression
237 pass, 0 fail (`test/clientIntelligence*.test.js`, 71 suites)

## SPEC-226/227 regression
All prior SPEC-226 tests in `test/clientIntelligenceInterview.test.js` pass (55 pass, 0 fail for that file, including the updated `NOT_APPLICABLE` assertion); SPEC-227 structured-edit coverage (`Executive Brief field normalization — clean entity lists`) passes unchanged.

## Canonical 223/224/225 regression
82 pass, 0 fail (`spec223CanonicalSemanticPersistence`, `spec223dBabrunRoundTrip`, `spec224CIECanonicalIntegration`, `spec225MaxCanonicalConsumer`, `spec221DurableEpistemicState`)

## FIRST REMAINING DIVERGENCE
NONE

## SPEC-228
PASS

## PRE-APPROVAL CORRECTION PROJECTION
COMPLETE

STOP.
