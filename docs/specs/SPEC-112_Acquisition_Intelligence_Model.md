# SPEC-112 — Acquisition Intelligence Model (AIM)

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md), [SPEC-048](SPEC-048_Sales_Intelligence_Engine.md), [SPEC-053](SPEC-053_Business_Intelligence_Engine.md), [SPEC-094](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md) |
| **ADR** | [ADR-049 Understand the Market Before Selling Into It](../adr/ADR-049_Understand_Market_Before_Selling.md) |
| **Initial validation tenant** | Fedir (`clientKey=fedir`) |

> **Numbering note:** The product brief called this SPEC-110. Repository SPEC-110 is [Business Intelligence Synthesis](SPEC-110_Business_Intelligence_Synthesis.md) — Max's operator-facing conclusions. This spec is numbered **112**.

## Objective

Transform a client's expertise into structured intelligence that PulseForge can reason over **before acquisition begins**.

PulseForge should understand a market before it attempts to sell into it.

Success for v1: Fedir's AIM is complete and inspectable; Scout qualifies prospects against that model (not a directory search); Paige receives pain/language/proof/CTA from AIM instead of starting from zero; the six-dimension qualification model is working; pilot status reports technical vs business milestones honestly.

## Vision References

- [ADR-049](../adr/ADR-049_Understand_Market_Before_Selling.md)
- [ADR-017 Intelligence Before Execution](../adr/ADR-017_Intelligence_Before_Execution.md)
- [ADR-032 Strategy Before Language](../adr/ADR-032_Strategy_Before_Language.md)
- [ADR-037 Reason About Businesses, Not Companies](../adr/ADR-037_Reason_About_Businesses_Not_Companies.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)
- [Intelligence Architecture](../vision/Intelligence_Architecture.md)

## Problem

Scout searches. Paige drafts. Qualification is a single ICP number. Client expertise never becomes a model.

| Today | Required |
|---|---|
| Scout matches industry + geography | Scout reasons over mission, ICP, pain, and signals |
| Paige starts from a blank prompt | Paige starts from likely pain, language, proof, CTA |
| One `icp_score` | ICP Fit · Pain Match · Evidence Quality · Buying Readiness · Confidence · Recommendation |
| Pain knowledge lives in someone's head | Pain knowledge is stored and reusable |
| Acquisition begins before market understanding | AIM is complete before Scout sells into the market |

## Principle

```text
Understand the market
        ↓
Teach Scout the pain ontology
        ↓
Qualify with evidence
        ↓
Capture what we learn
        ↓
Give Paige the brief
        ↓
Only then begin outreach
```

## Scope (v1 thin slice)

1. First-class AIM object: mission, reasoning-based ICP, desired transformation
2. Pain ontology with problems + observable signals (People Management, Customer Growth, Finance)
3. Six-dimension qualification model
4. Knowledge capture records per pain
5. Messaging intelligence for Paige
6. Fedir seed AIM
7. Scout attaches AIM qualification when an AIM is present (existing commercial-cleaning fit unchanged when AIM is absent)
8. Paige `aim_briefing` when qualification is supplied
9. In-memory store + Postgres migration
10. GET/POST inspect + qualify APIs, CLI
11. Pilot-status reporter (technical vs business milestones)

## Out of Scope

- Inventing Fedir case studies, geography, or revenue bands the client did not supply
- Replacing SPEC-110 operator Business Intelligence objects
- Replacing CIE Business Blueprints or Client Playbooks
- Running a live 50-prospect Fedir Scout campaign
- Autonomous outreach
- Operator AIM editor UI
- Treating AIM findings as SPEC-106 operating fact

## Architecture

```text
Client expertise (Fedir)
        ↓
AIM
  Mission
  ICP (reasoning, not demographics)
  Desired Transformation
  Pain Ontology
  Knowledge
        ↓
     SCOUT
  matches signals → pain
  scores six dimensions
        ↓
Qualification
        ↓
  Paige briefing (pain, language, proof, CTA)
```

AIM is **client-expertise intelligence**. Observed prospect signals are evidence. Qualification maps evidence onto the ontology. Empty knowledge fields stay empty.

### Relationship to prior specs

| Spec | Owns | AIM does not replace |
|---|---|---|
| SPEC-083 CIE | Who the client is (Blueprint) | AIM is how we acquire *for* them |
| SPEC-028 Playbook | How PulseForge grows them | AIM is market/pain intelligence, not campaign strategy |
| SPEC-053 BI Engine | How a *prospect* operates | AIM is the *client's* market model |
| SPEC-048 Sales Intel | Per-prospect sell strategy | AIM supplies the ontology Sales Intel can later consume |
| SPEC-100 Scout loop | Max ↔ Scout investigation | AIM is the model Scout reasons over |
| SPEC-110 BI Synthesis | Operator-facing Max conclusions | Different consumer |

## Data Model

### AIM

| Field | Meaning |
|---|---|
| `id` / `clientKey` | Stable identity (`fedir`) |
| `status` | `draft` \| `complete` \| `superseded` |
| `mission` | Transformation this client creates |
| `icp` | Company / founder / size / geography / exclusions — each a *reasoning* field, not a demographic checkbox |
| `transformation` | Current state → future state |
| `painOntology` | Categories → problems → observable signals |
| `knowledge` | Captured pain records |
| `pilot` | Technical + business milestone tracker |

### Qualification (per prospect)

| Dimension | Meaning |
|---|---|
| `icpFit` | Does this resemble the businesses the client transforms? |
| `painMatch` | Which ontology pains have observable signal support? |
| `evidenceQuality` | Independence, recency, and source diversity of signals |
| `buyingReadiness` | Timely pressure (hiring, growth, financing) — not intent invention |
| `confidence` | How much of the model is evidenced vs unknown |
| `overallRecommendation` | `pursue` \| `nurture` \| `watch` \| `reject` \| `unknown` |

Every dimension includes `score` (0–100), `reasons[]`, and `unknowns[]`. No score without an explanation.

### Pain knowledge

`definition`, `observableEvidence`, `commonObjections`, `typicalLanguage`, `recommendedMessaging`, `discoveryQuestions`, `caseStudies`, `successStories`

Case studies and success stories are **not fabricated**. Unknown stays unknown.

### Persistence

```text
aim_models            one AIM per clientKey + version
aim_qualifications    prospect-scoped six-dimension scores
aim_pain_knowledge    captured knowledge per pain id
```

v1 also ships an in-memory store so tests and CLI do not require Postgres.

## Fedir seed (Phase 1)

**Mission.** Transform founder-led businesses into business-machine businesses.

**ICP (reasoning).**

| Category | Reasoning |
|---|---|
| Company | Founder-led businesses with traction that still run through the founder — agencies, trades, professional services, operator-owned firms. Not a NAICS list. |
| Founder | Still in the operating loop. Stage: "I do everything myself" and the cost of that is becoming visible. |
| Size | Small enough that the founder is still in delivery/management; large enough that chaos is expensive. Exact headcount/revenue unknown until Fedir names bands. |
| Geography | Not a primary constraint until Fedir names a beachhead. Unknown, not invented. |
| Exclusions | Already systemized with managers; PE-backed / professionally governed; pre-revenue hobbies; non-founder-led enterprise; founders who have opted out of systemization. |

**Transformation.**

```text
Current  "I do everything myself."
   ↓
Future   "My business operates through systems and managers."
```

**Pain ontology (Phase 2).**

| Category | Problems | Observable signals |
|---|---|---|
| People Management | Founder dependency, delegation, hiring, accountability | Hiring repeatedly, owner replying to reviews, job postings, growth announcements |
| Customer Growth | Inconsistent pipeline, poor lead generation, weak sales process | Referral requests, discounting, irregular marketing |
| Finance | Cash flow, pricing, profitability | Financing, price increases, cost-cutting |

## Messaging Intelligence (Phase 5)

If Scout reports `Founder Dependency: 92%`, Paige already knows:

- likely pain
- typical language
- proof (only if knowledge has it)
- CTA

Paige does not invent case studies. Missing proof is an explicit unknown.

## Pilot Success (Phase 6)

### Technical

- AIM completed
- Scout understands market (AIM attached, pain matching)
- Qualification model working
- 50 qualified prospects (operational count; v1 reports 0 until live)
- Outreach begins (operational; v1 reports not started)

### Business

Forms · Meetings · Learning · Sales — reported as unknown until observed. Never inferred.

## Implementation Plan

1. Spec + ADR + registry
2. `packages/aim` engine + Fedir seed + tests
3. Scout opt-in qualification attach
4. Paige `aim_briefing`
5. Memory store + SQL migration
6. Routes + CLI
7. Competency `acquisition_intelligence_model`

## Migration Strategy

Additive `aim_*` tables. Rollback drops those tables. Existing Scout/Paige paths unchanged when no AIM is loaded.

## Testing

- `packages/aim/tests/aim.test.js`
- `test/acquisitionIntelligenceModel.test.js`
- Scout AIM attach in `packages/max/workspace/tests/scoutAimQualification.test.js`

## Acceptance Criteria

- [x] Fedir AIM answers the mission question: founder-led → business-machine
- [x] ICP is reasoning (company / founder / size / geography / exclusions), not demographics
- [x] Transformation is current → future state
- [x] Pain ontology includes People Management, Customer Growth, Finance with problems and signals
- [x] Qualification emits all six dimensions with explanations
- [x] Founder Dependency ~92% produces a Paige brief with likely pain, language, proof/unknown, CTA
- [x] Knowledge capture stores definition, evidence, objections, language, messaging, discovery questions; case studies stay empty until supplied
- [x] Scout without AIM is unchanged
- [x] Scout with AIM reasons over pain signals
- [x] Pilot status distinguishes technical vs business milestones and does not invent 50 prospects
- [x] AIM findings are not persisted as operating fact

## Future Work

- Operator AIM editor
- Live Fedir client_id + Scout campaign to 50 qualified prospects
- Sales Intelligence consuming AIM pain scores
- Additional pain categories only when Fedir's ontology proves insufficient
- Durable AIM versions after client edits
- ~~Compiler from unstructured market documents~~ — SPEC-113
