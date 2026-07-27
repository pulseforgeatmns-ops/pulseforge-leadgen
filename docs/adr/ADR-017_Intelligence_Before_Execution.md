# ADR-017 — Intelligence Before Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-030](../specs/SPEC-030_Company_Intelligence_Capability.md) |
| **Supersedes** | Unfinished SPEC-025 “Company Enrichment” scope (expanded, not abandoned) |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-005](ADR-005_LLM_Presentation_Engine.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-014](ADR-014_Personalized_by_Default.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md) |

## Context

Mission flow already plans Discovery → Enrichment → Ranking → Campaign → (Proposal) → Execution. Ranking, Campaign Builder, and Proposal Generator expect enrichment-shaped fields (contacts, buying signals, firmographics, personalization angles). The shipped step is still a **stub that fabricates** placeholder emails and phones.

Contact backfill alone is insufficient. Operators need a canonical **Company Intelligence Package**: company facts, decision makers with confidence, business signals, evidence-backed personalization, and an Opportunity Brief — plus a clean Knowledge handoff (facts vs inferences).

[ADR-016](ADR-016_Execution_Does_Not_Decide.md) forbids Execution from inventing strategy or contact data. If Execution ships before real intelligence, campaigns either invent context or run blind. That fails explainability ([ADR-002](ADR-002_Explainable_AI.md)) and the rule that presentation never invents intelligence ([ADR-005](ADR-005_LLM_Presentation_Engine.md)).

## Decision

1. **Company Intelligence replaces thin enrichment as the product capability** between Discovery and Ranking. Registry id may remain `company_enrichment` for planner compatibility; operator copy is “Company Intelligence.”
2. **Never fabricate.** Missing contacts, signals, or hooks stay missing. Placeholder identities are forbidden in production paths.
3. **Verified facts become evidence; uncertain claims remain inferences.** Knowledge Update must not promote guesses to facts.
4. **Every recommendation traces to evidence** (`evidenceRefs` or explicit uncertainty).
5. **The Company Intelligence Package is the canonical briefing artifact** for a discovered company before Ranking / Campaign / Proposal / Execution.
6. **Downstream capabilities consume the package without modification** — additive fields only; no rewrite of Ranking / Campaign / Proposal contracts required to start consuming.
7. **Ship Company Intelligence before Execution.** Execution carries approved strategy over evidenced packages; it must not be the first place contact or context appears.

## Consequences

### Positive

- Ranking and personalization become honest about thin evidence
- Proposal hooks and outreach angles stay company-specific (ADR-014)
- Execution can fail-closed on missing contacts without inventing them (ADR-016)
- Knowledge graph quality improves (fact vs inference discipline)
- Clears the unfinished SPEC-025 enrichment gap with a stronger product contract

### Negative / tradeoffs

- Packages will often be thinner than the fabricating stub looked — correct, not a regression
- Provider miss rates become visible; operators must accept enrichment shortfalls
- Slightly more build cost than “email/phone only” enrichment

### Follow-ups

- [ ] Implement [SPEC-030](../specs/SPEC-030_Company_Intelligence_Capability.md) live capability
- [ ] Implement [SPEC-031](../specs/SPEC-031_Business_Signals_Capability.md) Active Business Signals (lifecycle + decay) / [ADR-018](ADR-018_Time_Matters.md)
- [ ] Remove fabricating stub from production registry (keep non-fabricating stub for tests)
- [ ] Wire Knowledge Update to consume `knowledgeWrites`
- [ ] Proceed with [SPEC-029](../specs/SPEC-029_Execution_Engine.md) only after packages (or explicit empty packages) exist on the mission path
