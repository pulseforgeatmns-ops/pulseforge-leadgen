# SPEC-221 — Durable Epistemic State for Business Understanding

| Field | Value |
|---|---|
| **Status** | Proposed |
| **Owner** | Max / Client Intelligence |
| **Priority** | High — blocks approval of Babrun Executive Business Brief |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md); [SPEC-110 Business Intelligence Synthesis](SPEC-110_Business_Intelligence_Synthesis.md) |


## Problem

Max currently determines whether an interview response expresses uncertainty using phrase-based classification.

This creates an architectural failure when natural-language uncertainty does not match the recognized phrase set.

Production evidence from the Babrun onboarding:

Operator meaning:

`We haven't defined a formal brand voice yet.`

Intended epistemic state:

`UNKNOWN / NOT YET ESTABLISHED`

Actual system interpretation:

`DIRECT_ANSWER`

The response is subsequently normalized and persisted as a populated business fact.

Downstream systems then correctly operate on an **incorrect canonical representation**, producing outputs such as:

`The business' brand voice reinforces its positioning by sounding We haven't defined a formal brand voice yet.`

Similar behavior allowed an explicitly unknown differentiation state to contribute to a positive differentiation assessment.

The root problem is therefore **not Executive Brief wording**.

The root problem is that Max's durable business understanding does not reliably preserve the epistemic state of operator knowledge.


---

## Principle

**Max must preserve not only what the operator said, but what status the operator assigned to that knowledge.**

The following statements are semantically different:

`Our customers choose us because implementation takes one day.`

`We think customers choose us because implementation is fast.`

`We don't know why customers choose us yet.`

`We haven't investigated why customers choose us.`

They MUST NOT produce equivalent canonical facts.


---

## Required Epistemic States

Canonical business understanding MUST support, at minimum:

**KNOWN**

The operator establishes the proposition as currently true.

Example:

`Our primary market is the United States.`

**HYPOTHESIS**

The operator presents the proposition as a belief, assumption, test, or working theory rather than established fact.

Example:

`We think service businesses will respond best.`

**UNKNOWN**

The operator explicitly indicates that the answer is not currently known or established.

Examples:

`We don't know yet.`

`We haven't defined that.`

`I'm not sure yet.`

`We haven't established that.`

**UNRESOLVED**

The system lacks sufficient evidence to establish the field's state or the interview has not yet obtained a meaningful answer.

This differs from `UNKNOWN`: UNKNOWN is itself operator-provided knowledge; UNRESOLVED represents insufficient system understanding.

**NOT_APPLICABLE**

The operator establishes that the concept does not apply to the business.

Example:

`We don't have employees and don't plan to hire any.`

when answering a genuinely employee-specific business question.


---

## Canonical Representation

A business-understanding field MUST NOT be represented solely by a populated text value.

Conceptually:

```json
BusinessFact {
  "subject": "brand_voice",
  "value": null,
  "epistemic_state": "UNKNOWN",
  "confidence": 0.98,
  "evidence": "operator statement",
  "provenance": "interview_turn_123"
}
```

Exact schema is implementation-dependent.

The important invariant is:

**value and epistemic state are separate canonical concerns.**

`value = "We haven't defined our brand voice"`

MUST NOT be interpreted as:

`brand_voice = "We haven't defined our brand voice"`

Instead the durable understanding should represent the semantic equivalent of:

```json
{
  "subject": "brand_voice",
  "epistemic_state": "UNKNOWN",
  "value": null,
  "evidence": "operator statement"
}
```

The original operator language remains available as evidence/provenance.


---

## Classification Requirement

Epistemic classification MUST be based on the **semantic meaning of the response in the context of the active question**, not solely on matching a closed list of uncertainty phrases.

Phrase recognition MAY remain as a deterministic signal or fast path.

It MUST NOT be the architectural authority for epistemic state.

The system must correctly interpret materially equivalent statements such as:

- We don't know.
- We haven't defined that yet.
- We've never really established that.
- That's still something we're figuring out.
- I couldn't tell you yet.
- We haven't tested that.
- That's only a theory right now.
- My guess would be X.
- We believe X, but don't have evidence yet.
- There isn't enough evidence to say.

These examples are test cases, **not a replacement phrase dictionary**.


---

## Persistence Invariant

Once epistemic state has been established, downstream normalization MUST preserve it.

No stage may silently promote:

`UNKNOWN → KNOWN`

`UNRESOLVED → KNOWN`

`HYPOTHESIS → KNOWN`

merely because text exists in the field.

Promotion to stronger epistemic state requires new qualifying evidence or explicit operator confirmation.


---

## Confidence Invariant

**Confidence and epistemic state are orthogonal.**

Max may have very high confidence that something is unknown.

Example:

```json
{
  "epistemic_state": "UNKNOWN",
  "confidence": 0.98
}
```

means:

`Max is highly confident that the operator has not established this information.`

It does NOT mean Max has 98% confidence in a business fact.

Therefore confidence MUST NOT cause UNKNOWN/HYPOTHESIS/UNRESOLVED fields to be presented or scored as established facts.


---

## Downstream Consumer Contract

All consumers of canonical business understanding MUST respect epistemic state.


### Executive Business Brief

KNOWN:

`Primary market: United States.`

HYPOTHESIS:

`Current hypothesis: service businesses may represent the strongest initial market.`

UNKNOWN:

`Brand voice: Not yet defined.`

UNRESOLVED:

`Brand voice: Requires further understanding.`

NOT_APPLICABLE:

omit or explicitly identify as not applicable where useful.

The Brief MUST NOT manufacture affirmative prose around an UNKNOWN or UNRESOLVED value.


### Assessments

UNKNOWN and UNRESOLVED dimensions MUST NOT receive positive maturity/clarity evidence merely because an interview response exists.

For example:

`Differentiation: UNKNOWN`

cannot support:

`Differentiation ★★★☆☆`

on the basis of the uncertainty statement itself.

Assessments should distinguish **lack of knowledge** from **low-quality known information**.


### Blueprint

Unknowns and hypotheses MUST remain visible as such.

They MUST NOT silently become approved business assumptions.


### Specialists

Specialists consuming Max's understanding MUST be able to determine whether a relevant business fact is KNOWN, HYPOTHESIS, UNKNOWN, or UNRESOLVED.

A specialist MUST NOT treat a hypothesis or unknown as an operator-approved constraint.


---

## Evidence Preservation

Raw operator statements remain durable evidence.

This SPEC does **not** remove or weaken evidence retention.

It separates:

**what the operator said**

from

**what Max understands that statement to establish.**

Both are required.


---

## Regression Requirements

Add coverage demonstrating semantic equivalence across different uncertainty formulations.

At minimum:

```
"I don't know."
"We haven't defined that yet."
"We've never established that."
"We're still figuring that out."
"There isn't enough evidence to answer."
```

must not create KNOWN facts.

Hypothesis coverage:

```
"I think X."
"Our working assumption is X."
"X seems likely, but we haven't validated it."
```

must preserve `HYPOTHESIS`.

Known coverage:

```
"Our primary market is the United States."
```

must remain `KNOWN`.

Not-applicable coverage must demonstrate that explicit non-applicability is not confused with uncertainty.


---

## Babrun Production Regression

The Babrun onboarding becomes the canonical production regression for this failure.

Given:

`We haven't defined a formal brand voice yet.`

Max MUST NOT populate brand voice with that sentence as an affirmative value.

Given differentiation is explicitly unknown, Max MUST NOT claim a known competitive advantage or use the uncertainty statement as positive differentiation evidence.

The resulting Executive Business Brief must preserve those unknowns without semantic distortion.


---

## Non-Goals

This SPEC does **not**:

- redesign Executive Brief prose generally;
- fix generic fragment concatenation unrelated to epistemic state;
- redesign the Operator Scorecard;
- change Babrun's business information;
- determine Babrun's differentiation or brand voice;
- redesign Scout;
- replace raw interview evidence;
- solve every Client Intelligence synthesis issue exposed by Babrun.

Those remain separate concerns.


---

## Acceptance Criteria

SPEC is complete when:

1. Canonical business understanding explicitly represents epistemic state.
2. Natural-language uncertainty is not dependent on a closed phrase list for correctness.
3. KNOWN, HYPOTHESIS, UNKNOWN, UNRESOLVED, and NOT_APPLICABLE remain distinguishable through persistence.
4. Normalization cannot silently promote uncertainty/hypothesis into fact.
5. Raw operator evidence remains preserved.
6. Executive Brief rendering respects epistemic state.
7. Assessment/confidence logic cannot treat explicit uncertainty as positive evidence for the underlying business dimension.
8. Blueprint generation preserves epistemic state.
9. Specialist-facing understanding preserves the distinction.
10. Regression tests cover multiple semantically equivalent formulations rather than one patched phrase.
11. The Babrun production case passes without `"haven't defined"` or equivalent wording being stored/rendered as an affirmative business fact.
12. Existing known-fact interview behavior remains intact.


---

## Architectural Invariant

**No downstream system may infer that a proposition is true merely because Max possesses text about that proposition.**

Durable understanding requires both:

**the proposition**
and
**the epistemic status of the proposition.**
