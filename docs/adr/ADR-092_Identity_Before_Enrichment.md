# ADR-092 — Identity Before Enrichment

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-158](../specs/SPEC-158_Market_Definition_Hypothesis_Engine.md) (market hypotheses); `packages/scout/coverage/CandidateMinimumContract.js` (identity contract) |
| **Related** | [ADR-037](ADR-037_Reason_About_Businesses_Not_Companies.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md), [ADR-080](../architecture/ADR-080_Understanding_Emerges_From_Evidence.md), [ADR-082](../architecture/ADR-082_Business_Judgment_Through_Reusable_Heuristics.md) |

## Context

Scout evolved from a search tool into an investigative intelligence system. The legacy pipeline treated a website as proof that a business exists:

```
Search → Website → Candidate
```

That conflates **evidence** with **identity**. A website is one signal among many — it increases confidence, contactability, and research depth. It must not be the gate that decides whether a business is real.

Anchor immediate-cash categories (`cleaning_company_overflow`, `str_manager`, `property_manager`, `realtor`, `restoration_remodeling_partner`, `commercial_office`) are already more sophisticated than keyword search, but they were still stored as flat seed-term lists — search queries, not market hypotheses.

The platform philosophy is **Evidence → Understanding → Judgment**. Identity establishment belongs before enrichment and before prioritization.

## Decision

1. **A business identity is established by independent evidence of existence.** Enrichment attributes shall never determine whether a business exists.
2. **Identity signals** — any combination may establish a candidate; no single enrichment attribute is mandatory:
   - Google Business Profile
   - Place ID
   - Name
   - Address
   - Phone
   - Review history
   - Government registry
   - Website *(confidence+, contactability+, research depth+ — not `exists?`)*
   - Social profile
3. **Candidate pipeline order:**

   ```
   Search → Identity → Candidate → Evidence → Enrichment → Prioritization
   ```

4. **Vertical keys are market hypotheses, not search terms.** Example: `property_manager` means *"Property managers are likely buyers."* A hypothesis expands into many **search strategies** across independent sources (Google Places, business registry, LinkedIn, Facebook, brokerage sites, local chambers, industry associations, …). Scout reasoning stays independent of any one data source.
5. **Website-only rows are enrichment candidates, not identity candidates.** They may enter enrichment or unenriched queues but must not pass the identity gate alone.

## Rationale

Sales and setter workflows need reachable businesses, not domains. Google Places frequently returns name, address, phone, reviews, and Place ID without a website — especially for local operators Scout is designed to find. Rejecting those rows because Prospeo needs a domain inverts the investigative model.

Separating identity from enrichment also aligns Scout with the intelligence package pipeline (Market Definition → Hypothesis → Evidence → Qualification) and with ADR-037's distinction between facts (evidence) and business understanding.

## Consequences

### Positive

- Places-primary Scout (Anchor, cleaning buyer) discovers operators previously dropped at `if (!details?.website) continue`
- Candidate minimum contract (`CandidateMinimumContract.js`, SPEC-175) and ADR-092 share one identity model
- Market hypotheses become the stable abstraction above `CLIENT_SCOUT_PLANS` seed terms
- Enrichment (Prospeo, Hunter, website scrape) runs only after identity is established — credits are not spent proving existence

### Negative / tradeoffs

- Identity-only candidates may land in `scout_unenriched` until contact enrichment succeeds
- Within-run dedupe must key on Place ID / name+address, not domain alone
- Legacy SerpAPI rows still lean on domain dedupe; mixed runs need both keys

### Follow-ups

- [x] `packages/scout/identity/BusinessIdentity.js` — shared identity gate
- [x] `packages/scout/hypothesis/MarketHypothesisRegistry.js` — hypothesis → multi-source search strategies
- [x] `leadgen.js` — Places search and save path use identity before domain
- [ ] Full `leadgen.js` refactor to delegate search planning to `HypothesisDrivenDiscovery`
- [ ] Extend hypothesis registry with live registry/chamber/association adapters as they ship
