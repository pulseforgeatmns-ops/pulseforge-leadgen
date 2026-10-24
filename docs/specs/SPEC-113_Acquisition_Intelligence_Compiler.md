# SPEC-113 — Acquisition Intelligence Compiler (AIC)

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-112](SPEC-112_Acquisition_Intelligence_Model.md), [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) |
| **ADR** | [ADR-050 Compile Market Knowledge Before Runtime](../adr/ADR-050_Compile_Market_Knowledge_Before_Runtime.md) |

> **Numbering note:** The product brief called this SPEC-111. Repository SPEC-111 is [Operator Intent Taxonomy](SPEC-111_Operator_Intent_Taxonomy.md). This spec is numbered **113**.

## Objective

Transform unstructured market knowledge into a structured, governable Acquisition Intelligence Model (AIM) that Scout can reason over.

Just as the Client Intelligence Engine transforms interviews into an approved Business Blueprint, the Acquisition Intelligence Compiler transforms market knowledge into an approved AIM.

**The compiler never executes outreach.** Its sole responsibility is converting knowledge into understanding.

Success for v1: an operator can upload market documents, Max extracts structured concepts (not summaries), the compiler constructs a reasoning graph, the operator reviews every concept against source evidence, an AIM publishes, and Scout loads that published AIM instead of raw documents.

## Vision References

- [ADR-050](../adr/ADR-050_Compile_Market_Knowledge_Before_Runtime.md)
- [ADR-049 Understand the Market Before Selling Into It](../adr/ADR-049_Understand_Market_Before_Selling.md)
- [ADR-003 Human Approval](../adr/ADR-003_Human_Approval.md)
- [ADR-021 Human Approval Before Execution](../adr/ADR-021_Human_Approval_Before_Execution.md)
- [ADR-039 Separate Understanding from Execution](../adr/ADR-039_Separate_Understanding_from_Execution.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)
- [SPEC-112 Acquisition Intelligence Model](SPEC-112_Acquisition_Intelligence_Model.md)
- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)

## Problem

SPEC-112 gave PulseForge a first-class AIM and taught Scout to reason over it. The Fedir seed is hand-authored. There is still no governed path from market documents to a published AIM.

Without a compiler:

| Today | Required |
|---|---|
| LLM reads everything and answers | Market knowledge is compiled, then approved |
| Scout would have to reread PDFs | Scout loads a published AIM |
| Concepts are implied in prose | Concepts are first-class, sourced, and reviewable |
| Understanding is ephemeral | Understanding is durable and versioned |

Every search would start from scratch. With AIC, PulseForge learns a market once and reasons from it repeatedly.

## Philosophy

```text
Current AI workflow
Prompt → LLM reads everything → Answer

PulseForge workflow
Market Knowledge → Compile → Human Approval → Published AIM → Scout Runtime
```

Understanding becomes durable. Everything is evidence. Nothing is assumed.

## Scope (v1 thin slice)

1. Draft Acquisition Intelligence Workspace from uploaded documents
2. Deterministic concept extraction (mission, transformation, ICP, pain, signals, buying triggers, objections, language, evidence, confidence, disqualifiers, unknowns)
3. Ontology construction (concepts become relationships: supported_by, observed_through, maps_to, excludes)
4. Human review: accept / edit / merge / remove — every concept links back to source evidence
5. Publication of a versioned AIM (`status = published`) — nothing publishes automatically
6. Scout loads only published/complete AIMs; never document bodies
7. In-memory store + Postgres migration
8. GET/POST compiler APIs + CLI
9. Competency `acquisition_intelligence_compiler`

## Out of Scope

- Executing outreach, Scout campaigns, or Paige publishing
- LLM-adaptive extraction (v1 is deterministic section/label parsing so tests are stable)
- Operator AIM editor UI beyond APIs/CLI
- Replacing CIE Business Blueprints or SPEC-112 qualification
- Treating compiled AIM findings as SPEC-106 operating fact
- Inventing mission, geography, case studies, or confidence not present in source documents
- Binary PDF parsing (v1 accepts text/markdown bodies)

## Principle

```text
Market Knowledge
        ↓
Ingestion
        ↓
Extraction (concepts, not summaries)
        ↓
Ontology (reasoning graph)
        ↓
Human Review (CIE-identical gate)
        ↓
Published AIM
        ↓
Scout Runtime
```

The compiler constructs understanding. Scout consumes the published model. Outreach is someone else's job.

## Compilation Pipeline

### Stage 1 — Ingestion

Operator uploads any combination of:

Pain research · customer interviews · discovery calls · sales transcripts · founder interviews · playbooks · landing pages · ICP notes · objection documents · case studies · outcome reports

Compiler creates a **Draft Acquisition Intelligence Workspace**.

### Stage 2 — Extraction

Max identifies structured concepts. Not summaries. Concepts.

Mission · Transformation · ICP · Pain Categories · Observable Signals · Buying Triggers · Objections · Language · Evidence · Confidence · Disqualifiers · Unknowns

Every concept stores provenance: source document, section, evidence excerpt. Missing fields stay unknown.

### Stage 3 — Ontology Construction

Concepts become relationships. Example:

```text
Founder Dependency
        ↓ supported_by
Hiring
        ↓ observed_through
Job Posts
        ↓ confidence
```

The compiler is constructing a reasoning graph.

### Stage 4 — Human Review

Exactly like CIE. Nothing publishes automatically.

Operator reviews:

- Accept
- Edit
- Merge
- Remove

Every concept is explainable. Every concept links back to source evidence.

### Stage 5 — Publication

```text
AIM v1.0  Published
```

Only published AIMs become runtime knowledge. Prior published versions for the same client are superseded.

## AIM Structure (compiler output)

The published artifact is a SPEC-112 AIM plus compiler provenance:

| Section | Meaning |
|---|---|
| Mission | What transformation exists? |
| ICP | Ideal customer as reasoning, not demographics |
| Pain Ontology | Categories, problems, relationships, priority |
| Observable Signals | Every pain maps to evidence |
| Buying Signals | What indicates urgency? |
| Disqualifiers | Who should NOT receive outreach? |
| Messaging Context | Language, vocabulary, proof, examples |
| Confidence Rules | What increases / decreases certainty; unknowns |
| Provenance | Source document, section, evidence, operator approval |

No hallucinated knowledge.

## Scout Runtime

Scout never reads PDFs.

```text
Published AIM → Reasoning Model → Prospect Search
```

When Scout searches it loads Mission → AIM → ICP → Observable Signals → Evidence → Confidence → Recommendation. Every recommendation is explainable.

Draft compiler output is treated as **absent**. Existing commercial-cleaning fit is unchanged when no published AIM is loaded.

## Relationship to prior specs

| Spec | Owns | AIC does not replace |
|---|---|---|
| SPEC-083 CIE | Who the client is (Blueprint) | AIC compiles how we acquire *for* them |
| SPEC-112 AIM | The runtime market model | AIC is how unstructured knowledge becomes that model |
| SPEC-100 / 100A Scout | Investigation + qualification | Scout consumes published AIM; never compiler documents |
| SPEC-028 Playbook | How PulseForge grows them | AIC is market/pain intelligence, not campaign strategy |

CIE analog:

```text
CIE:  interviews → evidence → Blueprint → human approve → published understanding
AIC:  documents  → concepts → AIM graph → human approve → published AIM
```

## Data Model

### Workspace lifecycle

Allowed transitions only (no skipping to published):

```text
NEW → INGESTING → EXTRACTED → ONTOLOGY_READY → IN_REVIEW → APPROVED → PUBLISHED
```

`compile` may advance INGESTING → IN_REVIEW in one operator action (extract + ontology), but **never** to PUBLISHED.

### `aic_workspaces`

`id`, `client_key`, `client_id`, `status`, `version`, `aim_id`, `payload` JSONB, timestamps, `is_operating_fact = false`

### `aic_documents`

`id`, `workspace_id`, `title`, `kind`, `filename`, `body`, `uploaded_at`

### `aic_concepts`

`id`, `workspace_id`, `type`, `label`, `statement`, `confidence`, `status` (`proposed` \| `accepted` \| `edited` \| `merged` \| `removed`), provenance JSONB, `evidence_excerpt`

### `aic_edges`

`id`, `workspace_id`, `from_concept_id`, `to_concept_id`, `relation`

### `aic_reviews`

`id`, `workspace_id`, `concept_id`, `action`, `operator`, `payload` JSONB, `created_at`

v1 also ships an in-memory store so tests and CLI do not require Postgres.

## Public API

| Method | Path |
|---|---|
| POST | `/api/v1/aic/workspaces` |
| POST | `/api/v1/aic/workspaces/:id/documents` |
| POST | `/api/v1/aic/workspaces/:id/compile` |
| GET | `/api/v1/aic/workspaces/:id` |
| POST | `/api/v1/aic/concepts/:id/review` |
| POST | `/api/v1/aic/workspaces/:id/approve` |
| POST | `/api/v1/aic/workspaces/:id/publish` |
| GET | `/api/v1/aic/workspaces/:id/aim` |

Review body: `{ action: accept \| edit \| merge \| remove, statement?, mergeInto?, absorbedIds? }`

## Implementation Plan

1. Spec + ADR + registry
2. `packages/aic` engine + fixtures + tests
3. Publication into SPEC-112 AIM store (`status = published`)
4. Scout loads only runtime AIMs (`published` or grandfathered `complete` seeds)
5. Memory store + SQL migration
6. Routes + CLI
7. Competency `acquisition_intelligence_compiler`

## Migration Strategy

Additive `aic_*` tables. Extend `aim_models.status` to allow `published`. Rollback drops compiler tables and restores the prior AIM status check. Existing Fedir `complete` seed remains a runtime AIM.

## Testing

- `packages/aic/tests/aic.test.js`
- `test/acquisitionIntelligenceCompiler.test.js`
- Scout ignores draft AIM / documents; reasons from published AIM

## Acceptance Criteria

- [x] A market document can be uploaded into a draft workspace
- [x] Structured concepts are extracted (not summaries), each with source provenance
- [x] Ontology relates pain → supporting concept → observable signal, with confidence
- [x] Operator can accept, edit, merge, or remove every concept
- [x] Nothing publishes automatically; publish requires approval
- [x] Published AIM is a SPEC-112 model Scout can qualify against
- [x] Scout never receives document bodies; draft AIM is treated as absent
- [x] Unknowns stay unknown; compiler does not invent mission, geography, or proof
- [x] Compiler cannot execute outreach
- [x] AIM findings are not persisted as operating fact

## Future Work

- LLM-adaptive extraction with the same provenance + approval gate
- Operator compiler UI
- Binary PDF / audio transcript ingest
- Incremental recompile that preserves prior operator edits
- Sales Intelligence consuming compiler provenance
