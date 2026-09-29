# SPEC-222 — Canonical Semantic Business Understanding

| Field | Value |
|---|---|
| **Status** | Proposed — architectural review required; do not implement |
| **Owner** | Max / Client Intelligence |
| **Priority** | High — production semantic corruption reaches every Client Intelligence consumer |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md); [SPEC-110 Business Intelligence Synthesis](SPEC-110_Business_Intelligence_Synthesis.md); [SPEC-221 Durable Epistemic State](SPEC-221_Durable_Epistemic_State_for_Business_Understanding.md) |


## Status and Review Gate

This SPEC defines an architecture for review. It does not authorize implementation, schema migration, backfill, or changes to interview behavior.

Implementation MUST NOT begin until the canonical model, interpretation boundary, migration order, and failure policy in this document are approved.


---

## Problem

Client Intelligence preserves raw operator evidence, but its operational `normalizedFacts` are produced by field-specific regular expressions, comma/`and` splitting, word-count filters, and fragment extraction.

Those compatibility fields are then treated as business understanding by:

- interview recap;
- Blueprint sections;
- Executive Business Brief synthesis;
- Max workspace context;
- downstream specialist context.

Consequently, coherent operator meaning can become malformed before presentation or specialist consumption. The system retains the original words as evidence, but its operational representation no longer expresses what those words mean.

The Babrun production interview reproduced this deterministically:

```text
Identity: Babrun is a building coaching and transformation programs for owners of small, founder-led businesses.

Services: Today the business delivers Today, manage employees, identify performance problems earlier, buying decisions, sell around value rather than features, move opportunities forward, pursue larger or more valuable customers, stronger differentiation, better economics, growth potential, potentially a stronger business model.

Ideal Customer: Ideal customers are but whose capabilities as a manager, generally with 1–10 employees.
```

The root failure is not primarily presentation grammar. The compatibility fields supplied to presentation are already fragmentary or semantically misclassified.


---

## Architectural Invariant

**Natural-language operator answers must be semantically interpreted into canonical business concepts before they are normalized for downstream consumption.**

Regex/list-fragment extraction MUST NOT be the canonical business-understanding layer.

The authoritative flow is:

```text
raw operator evidence
  → semantic interpretation
  → canonical BusinessFacts and business entities
  → deterministic derived projections
  → recap + Blueprint + Brief + Max + specialists
```

Raw evidence remains durable and immutable for provenance. No downstream consumer may independently re-parse raw interview prose when canonical semantic facts are available.


---

## Current Architecture

The current interview path has two representations with conflicting authority:

1. Raw operator statements are retained as evidence and, under SPEC-221, may also be copied into proposition records.
2. `ingestAnswerIntoNormalizedFacts()` separately derives scalar/list compatibility fields through section-specific extraction.
3. `sectionsFromNormalizedFacts()` treats those compatibility fields as semantic inputs.
4. `buildReflection()` takes the first sentence of those generated section summaries.
5. Blueprint and Executive Brief generation rebuild from the same compatibility fields.
6. Max workspace context prefers attached `normalizedFacts` over section-summary fallback.
7. Specialist context receives the resulting Client Intelligence attachment.

Relevant current implementation surfaces include:

- `services/clientIntelligenceInterview.js`
  - `cleanRawAnswer()`
  - `extractServiceList()`
  - `extractCustomerSegments()`
  - `ingestAnswerIntoNormalizedFacts()`
  - `sectionsFromNormalizedFacts()`
  - `buildReflection()`
  - `generateBlueprint()`
  - `buildExecutiveSummary()`
- `services/clientIntelligenceEpistemic.js`
  - `createBusinessFact()`
  - `extractBusinessFacts()`
  - `projectBusinessFacts()`
- `services/maxSynthesis/BusinessFactNormalizer.js`
  - `normalizeBusinessFacts()`
- `packages/max/workspace/ClientIntelligenceContext.js`
  - `normalizeBlueprintSummary()`
  - `buildClientIntelligenceAttachment()`

`BusinessFactNormalizer` is not the origin of the Babrun corruption. It currently receives already-corrupted compatibility fields and can introduce secondary truncation or formatting changes.


---

## Evidence Is Not Meaning

Evidence records what the operator communicated:

```text
Our ideal customer is the owner or founder of an operating small business in the United States, generally with 1–10 employees...
```

Canonical semantic facts record the business propositions supported by that evidence:

```text
customer.role = owner/founder
customer.business_stage = operating
customer.geography = United States
customer.employee_range = 1–10
```

The full answer MUST remain available as evidence. It MUST NOT be used as the value of each proposition, and arbitrary substrings of it MUST NOT become concepts merely because they survive token or word-count filters.


---

## Proposed Canonical Semantic Model

Canonical business understanding consists of three related record types:

1. `BusinessEvidence` preserves source material.
2. `BusinessEntity` gives stable identity to concepts that require relationships or attached descriptions.
3. `BusinessFact` makes a typed proposition about an entity or typed value.

### BusinessEvidence

```json
{
  "evidence_id": "evidence_turn_123",
  "source_type": "operator_interview_turn",
  "source_id": "turn_123",
  "raw_text": "Our ideal customer is the owner or founder...",
  "recorded_at": "timestamp",
  "tenant_id": "tenant_42"
}
```

Evidence is immutable. Corrections create new evidence and superseding facts; they do not rewrite the original statement.

### BusinessEntity

```json
{
  "entity_id": "offer_management_people",
  "entity_type": "offer_program",
  "canonical_label": "Management / People",
  "tenant_id": "tenant_42"
}
```

Entities are required when meaning depends on attachment or relationships. For example, a program description belongs to its program; it is not an independent service.

### BusinessFact

```json
{
  "fact_id": "fact_123",
  "subject_ref": "customer_profile_primary",
  "predicate": "employee_range",
  "object": {
    "type": "integer_range",
    "minimum": 1,
    "maximum": 10,
    "unit": "employees"
  },
  "qualifiers": {},
  "epistemic_state": "KNOWN",
  "confidence": 0.98,
  "evidence_refs": [
    {
      "evidence_id": "evidence_turn_123",
      "support": "direct",
      "span": "generally with 1–10 employees"
    }
  ],
  "provenance": {
    "interpreter": "client_intelligence_semantic_interpreter",
    "interpretation_id": "interpretation_456"
  },
  "status": "active",
  "supersedes": null
}
```

The canonical proposition is the tuple of subject, predicate, typed object, and qualifiers. Display prose is never canonical.

### Relationship Facts

Relationships use the same fact structure:

```json
{
  "subject_ref": "offer_management_people",
  "predicate": "specialization_of",
  "object": { "type": "entity_ref", "value": "offer_12_week_1_to_1" }
}
```

```json
{
  "subject_ref": "offer_management_people",
  "predicate": "intended_outcome",
  "object": {
    "type": "concept",
    "value": "delegate effectively and identify employee performance problems earlier"
  }
}
```

This prevents program descriptions, outcomes, pains, and delivery modes from collapsing into a flat service list.

### Canonical Reference Types

References are typed and tenant-scoped. Strings that resemble IDs are not valid references.

```json
EntityRef {
  "ref_type": "ENTITY",
  "entity_id": "entity_stable_id",
  "entity_type": "PROGRAM",
  "tenant_id": "tenant_42"
}

FactRef {
  "ref_type": "FACT",
  "fact_id": "fact_stable_id",
  "tenant_id": "tenant_42",
  "resolution": "EXACT"
}
```

`BusinessFact.subject_ref` MUST be an `EntityRef`. Its object MUST be a typed literal, `EntityRef`, or `FactRef` allowed by the predicate registry. `condition_refs`, contradiction links, supersession links, and metric-to-objective relationships use `FactRef` when they depend on a proposition rather than merely an entity.

Every reference MUST resolve within the same tenant and canonical snapshot. Cross-tenant and dangling references invalidate the interpretation batch.

`FactRef.resolution` is `EXACT` or `CURRENT_IN_CONFLICT_SET`. `EXACT` always identifies the immutable historical fact, even if superseded. `CURRENT_IN_CONFLICT_SET` resolves through the target fact's supersession chain to the current authoritative fact; condition refs use this mode. Supersession never rewrites existing references.

### Canonical Entity Vocabulary v1

The minimum closed entity vocabulary is:

| Entity type | Meaning | Identity basis |
|---|---|---|
| `BUSINESS` | The tenant business being understood | One canonical business per tenant |
| `OFFER` | A sellable or planned commercial offer | Business + normalized commercial offer identity; temporal/modal changes do not create a new entity |
| `PROGRAM` | A named track, component, or variant within an offer | Owning offer + normalized program identity |
| `CUSTOMER_PROFILE` | A scoped audience whose attributes belong together | Business + profile purpose/scope + lifecycle |
| `PAIN` | A meaningful customer problem or pressure | Normalized concept identity within tenant vocabulary |
| `CAPABILITY` | An ability an offer/program develops or a customer lacks | Normalized concept identity within tenant vocabulary |
| `OUTCOME` | A desired business/customer result | Normalized result identity plus owning context |
| `OBJECTIVE` | A business objective with optional horizon/target | Business + objective identity + applicable period |
| `METRIC` | A defined measure of an objective or outcome | Business + metric definition + unit |

Each entity MUST contain a stable opaque `entity_id`, registered `entity_type`, `tenant_id`, `canonical_label`, evidence-supported `aliases`, `lifecycle_status`, vocabulary version, optional `merged_into`, and creation evidence references.

Entity IDs are generated once and remain stable across label changes, corrections, interpretation versions, and projections. IDs MUST NOT be derived solely from mutable labels. Within a tenant, an active entity MUST be unique for its entity type and documented identity basis. Similar labels do not prove identity; ambiguous candidates remain separate until resolved.

Entity allocation is idempotent within the canonical commit transaction: replay of a successful interpretation batch returns its previously allocated IDs, while later evidence resolves against existing entity identity bases before allocating a new opaque ID. Entity IDs MUST NOT incorporate interpreter version because reinterpretation must not change established identity.

Aliases are names for the same entity, not additional entities. Alias addition is append-only. Alias conflict does not trigger automatic merge.

Entity merge is explicit and append-only: the losing entity becomes `MERGED`, identifies `merged_into`, and remains resolvable for history. Facts are not rewritten; current resolution follows the merge edge. Cyclic, cross-tenant, and cross-type merges are invalid.

Merge decisions are append-only records included in snapshots. An erroneous merge is corrected by a new operator-confirmed merge-resolution record that deactivates that merge edge for subsequent snapshots and restores the prior entity identities; it does not mutate or delete the historical merge. Splitting concepts that were genuinely conflated creates new stable entities and explicit successor relations. Merge resolution is deterministic per snapshot and MUST reject cycles before commit.

Merge correction is not retroactive. Historical snapshots retain the merge graph and conflict-set assignments active when they were created; later snapshots apply the deactivation record. Queries resolve merges using the selected snapshot, never the latest merge graph independently.

Entity lifecycle values are `ACTIVE`, `MERGED`, and `RETIRED`. `RETIRED` does not assert that historical propositions are false; temporal applicability belongs to facts.

New entity types require a vocabulary version, domain definition, identity basis, allowed predicates, projection behavior, and migration review. Interpreters MUST NOT create arbitrary entity types.

### Canonical Predicate Registry v1

Canonical predicates come from a closed, versioned registry. Each entry defines domain, range, forward and inverse cardinality, conflict behavior, and permitted temporal/modal states.

| Predicate | Domain | Range | Cardinality / conflict behavior |
|---|---|---|---|
| `offers` | `BUSINESS` | `OFFER` | forward `0..*`, inverse `1`; set-valued by offer identity |
| `contains_program` | `OFFER` | `PROGRAM` | forward `0..*`, inverse exactly `1` active owner |
| `has_delivery_mode` | `OFFER`, `PROGRAM` | typed `DELIVERY_MODE` literal | forward `0..*`; set-valued by mode |
| `targets_customer_profile` | `BUSINESS`, `OFFER`, `PROGRAM` | `CUSTOMER_PROFILE` | forward `0..*`; set-valued by profile identity |
| `teaches_capability` | `PROGRAM`, `OFFER` | `CAPABILITY` | forward `0..*`, inverse `0..*`; set-valued by capability identity |
| `targets_outcome` | `PROGRAM`, `OFFER`, `OBJECTIVE` | `OUTCOME` | forward `0..*`; set-valued by outcome identity |
| `addresses_pain` | `PROGRAM`, `OFFER`, `CUSTOMER_PROFILE` | `PAIN` | forward `0..*`; set-valued by pain identity |
| `has_role` | `CUSTOMER_PROFILE` | typed role literal | forward `1..*`; set-valued by role |
| `has_business_stage` | `CUSTOMER_PROFILE`, `BUSINESS` | typed stage literal | exclusive per subject + temporal window |
| `has_characteristic` | `CUSTOMER_PROFILE`, `BUSINESS` | typed concept literal | set-valued by concept identity |
| `has_geography` | `CUSTOMER_PROFILE`, `BUSINESS`, `OBJECTIVE` | typed geography literal/entity | set-valued by geography + scope |
| `has_employee_range` | `CUSTOMER_PROFILE`, `BUSINESS` | typed integer range | exclusive per subject + temporal window; unit employees |
| `has_vertical` | `CUSTOMER_PROFILE`, `BUSINESS`, `OBJECTIVE` | typed vertical literal/entity | set-valued by vertical + validation scope |
| `excludes_customer_profile` | `BUSINESS`, `OFFER`, `PROGRAM` | `CUSTOMER_PROFILE` | set-valued by profile; requires strength qualifier |
| `has_description` | any registered entity | typed semantic text | exclusive per entity + language + temporal window |
| `measures_objective` | `METRIC` | `OBJECTIVE` or objective `FactRef` | forward `1..*`; set-valued by objective identity |
| `depends_on` | any registered entity | `FactRef` | forward `0..*`; set-valued by referenced conflict set |
| `has_buying_reason` | `BUSINESS`, `OFFER` | typed concept or null | set-valued candidate reasons; actual-reason UNKNOWN occupies its own semantic slot |
| `has_brand_voice` | `BUSINESS` | typed brand-direction concept or null | established voice is exclusive; candidate directions are set-valued |
| `avoids_brand_trait` | `BUSINESS` | typed brand-trait concept | set-valued by trait identity |
| `has_validation_status` | `BUSINESS`, `OFFER`, `CUSTOMER_PROFILE`, `OBJECTIVE` | typed validation-status literal or null | exclusive per subject + validation scope |

Unregistered predicates are invalid canonical facts. Extension requires a registry version increment, domain/range/cardinality rules, projection semantics, and compatibility review. Free-text predicates remain candidates only.

A semantic slot is the deterministic conflict key declared by the registry: tenant + merge-resolved subject + predicate + the listed scope qualifiers (such as language, validation scope, object identity for set-valued predicates, and temporal window). Exclusive facts in one slot share a conflict set. Set-valued facts receive one slot per declared object identity and may coexist. Interpreters do not invent slot keys.

Descriptions, capabilities, pains, and outcomes are distinct types. A `CAPABILITY` connected through `teaches_capability` MUST NOT be projected as an `OFFER`, `PROGRAM`, or service.

### Ownership and Attachment Invariants

- Every active `PROGRAM` MUST have exactly one active `contains_program` owner in v1.
- Every program description uses `has_description` with that `PROGRAM` as subject.
- Every program-specific capability, outcome, or pain uses that program as subject through `teaches_capability`, `targets_outcome`, or `addresses_pain`.
- A concept may be shared by multiple programs only through separate relationship facts with independent evidence and epistemic/temporal metadata.
- Shared concept identity does not imply shared ownership or applicability.
- An interpreter MUST NOT lift a program relationship to its parent offer or business without separate evidence.
- A projector traverses explicit relationships only; textual proximity and labels do not establish attachment.

For Babrun, the required path is:

```text
Babrun --offers--> 12-week 1:1 coaching
12-week 1:1 coaching --contains_program--> Management / People
Management / People --teaches_capability--> delegation
```

`delegation` is a capability of `Management / People`, not an independent Babrun service.

### Temporal and Modal Semantics

SPEC-221 epistemic state is orthogonal to temporal/modal applicability.

**Epistemic state answers whether we believe or know the proposition.**

**Temporal/modal state answers when and in what mode the proposition applies.**

Every fact MUST carry `temporal_status`, optional `valid_from`/`valid_to`, `modality`, and `condition_refs`.

`temporal_status` values are `CURRENT`, `PLANNED`, `HISTORICAL`, and `RETIRED`. `modality` values are `ACTUAL`, `INTENDED`, and `CONDITIONAL`.

Missing dates do not erase temporal status. `PLANNED` facts MUST use `INTENDED` or `CONDITIONAL`, never `ACTUAL`. Current-offer projections require both `CURRENT` and `ACTUAL`.

The current window is the projection/query evaluation instant or explicit requested interval, not a rolling duration. Commit validation evaluates intervals at commit time; later time passage does not mutate a snapshot. A new current-resolution query may exclude an expired fact, and any lifecycle/status change requires a new append-only fact and snapshot.

Temporal expiration alone does not create a RETIRED fact or mutate lifecycle. Projectors exclude facts outside the requested interval. A business-meaning change requires new evidence and an append-only corrective or retirement fact.

Conditions use `FactRef`; copied condition text is not authoritative. A conditional fact with missing, unresolved, or non-KNOWN conditions cannot project as actual.

`Babrun intends to develop group programs after validation` is a KNOWN present intention represented as `PLANNED` plus `INTENDED` or `CONDITIONAL`. It MUST NOT appear as a current offer.

UNKNOWN, UNRESOLVED, and NOT_APPLICABLE facts do not use sentinel text objects. Their typed object is `null`, as required by SPEC-221; subject and predicate identify what is unknown. A related HYPOTHESIS uses a separate fact with its own typed object.

### Confidence Semantics

One scalar MUST NOT represent two kinds of confidence.

- `interpretation_confidence` is calibrated confidence that entities, predicates, objects, attachment, and scope correctly express the source language.
- `epistemic_confidence` is confidence/support for the business proposition under SPEC-221.

Both range from 0 through 1 and carry separate calibration versions. High interpretation confidence that a proposition is UNKNOWN is valid. Interpretation confidence MUST NOT promote epistemic state or increase epistemic confidence.

Automatic canonical commit requires schema validity, complete evidence linkage, no unresolved domain/range/cardinality violation, and `interpretation_confidence >= 0.85`. Lower-confidence facts remain candidates and affected concepts become `UNRESOLVED`; operator confirmation may commit them with `OPERATOR_CONFIRMED` support.

Direct, unambiguous operator assertions may commit as `KNOWN` without second confirmation after passing that gate. Inferences MUST remain `HYPOTHESIS` or `UNRESOLVED` until operator-confirmed; model confidence alone cannot make them KNOWN.

### Customer Profile Composition

`CUSTOMER_PROFILE` is the aggregation boundary for audience meaning. `has_role`, `has_business_stage`, `has_geography`, `has_employee_range`, `has_vertical`, `has_characteristic`, and `addresses_pain` attach to a specific profile. Offers/programs target it through `targets_customer_profile`.

Different audiences require different profiles unless the operator explicitly combines them. Facts from separate profiles MUST NOT be unioned into a global ICP. Every projection identifies its source profile IDs and preserves boundaries.

A new profile is required when role, business stage, geography/market scope, exclusion status, or offer relationship is materially different and the operator has not stated that the attributes describe one audience. Repeated or synonymous descriptions remain candidates for the same profile only when identity resolution passes the interpretation threshold; otherwise they remain separate pending confirmation. “We serve both X and Y” creates separate profiles unless the shared attributes are explicitly stated to apply to both.

Excluded audiences are also profiles. `excludes_customer_profile` requires a `strength` qualifier of `HARD_EXCLUSION` or `LOW_PRIORITY`, plus condition `FactRef`s where applicable. Exclusion reasons remain attached to the excluded profile rather than appended to another profile's prose.

### Evidence Reference Model

Evidence is immutable, tenant-owned, and stored once. Any number of facts may reference one evidence record.

An evidence reference contains `evidence_id`, `support_type`, and a stable locator: half-open UTF-16 `start`/`end` offsets plus the immutable source text's SHA-256 hash. Multiple non-contiguous spans use multiple references. This remains deterministic when text repeats.

UTF-16 offsets are canonical because the current Node.js/browser runtime indexes JavaScript strings as UTF-16 code units. Ingestion computes offsets against the exact immutable raw string before any normalization. Non-JavaScript consumers MUST convert offsets explicitly after validating the SHA-256 hash; they MUST NOT reinterpret them as bytes or Unicode code points. Hash validation detects source mismatch and is not an evidence-deduplication rule.

Support types are `DIRECT`, `INFERRED`, and `OPERATOR_CONFIRMED`. Evidence records retain source type/ID, actor, timestamp, tenant, immutable raw payload, content hash, and ingestion provenance. Evidence text is not copied into fact values to preserve provenance.

### Correction, Contradiction, and Current Resolution

Canonical understanding is append-only. Facts and evidence are never destructively overwritten.

Every fact belongs to a `conflict_set_id` based on tenant, merge-resolved subject, predicate, and semantic slot/qualifiers that determine mutual exclusivity. The predicate registry declares multi-valued versus exclusive slots.

Fact lifecycle values are `CANDIDATE`, `ACTIVE`, `SUPERSEDED`, `CONTRADICTED`, and `REJECTED`.

- A correction creates new evidence and a new fact with `correction_of: FactRef` and `supersedes: [FactRef]`.
- A contradiction creates `contradicts: [FactRef]` and places competing facts in one conflict set; neither is deleted.
- A partial correction supersedes only identified facts; sibling facts remain active.
- Entity merge preserves proposition history while reference resolution follows `merged_into`.

Current resolution MUST: resolve merges; filter by requested temporal window/modality; exclude rejected/superseded facts; prefer an operator-confirmed correction only over facts it explicitly supersedes; return mutually exclusive active KNOWN facts as an `UNRESOLVED` conflict; retain registry-declared compatible values; and return selected fact IDs plus conflict IDs.

Supersession is transitively resolved within a conflict set. If C supersedes B and B supersedes A, C is current and A/B remain historical. C need only directly reference B; cycle detection and transitive closure are validator responsibilities. A correction may supersede multiple facts only when they occupy compatible slots explicitly named by the correction. `EXACT` references continue to resolve to A or B; `CURRENT_IN_CONFLICT_SET` references resolve to C.

Supersession and correction graphs MUST be acyclic. A conditional fact may affect current projection only when every `CURRENT_IN_CONFLICT_SET` condition resolves without conflict to an ACTIVE fact whose epistemic state is KNOWN and whose typed object satisfies the predicate registry's truth rule. `OPERATOR_CONFIRMED` evidence support does not substitute for KNOWN epistemic state.

Recency, confidence, and non-null text alone never establish authority.

### Canonical Snapshot and Interpretation Batch

The durable read authority is an immutable `CanonicalBusinessSnapshot`, not mutable session JSON:

```json
{
  "snapshot_id": "snapshot_123",
  "tenant_id": "tenant_42",
  "semantic_model_version": 1,
  "predicate_registry_version": 1,
  "entity_ids": ["entity_1"],
  "fact_ids": ["fact_1"],
  "conflict_set_ids": [],
  "committed_interpretation_batch_ids": ["batch_1"],
  "created_at": "timestamp",
  "supersedes_snapshot_id": null
}
```

Each accepted turn produces one candidate `InterpretationBatch` containing interpreter/version identifiers, source evidence IDs, candidate entities/facts, interpretation confidence per candidate, validation errors, and an idempotency key over tenant ID + immutable evidence content hash + source ID + interpreter version + semantic schema version.

Every successful canonical commit creates exactly one new immutable snapshot that supersedes the prior active snapshot. Validation runs before the transaction writes canonical records. A failed batch may be retained as a non-canonical `FAILED` batch for diagnostics, but creates no entities, facts, conflicts, or snapshot. Replaying the same idempotency key returns the prior success or failure. A changed evidence hash or interpreter/schema version intentionally creates a new candidate batch; stable entity identity and conflict resolution prevent duplicate active meaning.

Readers either bind to the latest committed snapshot at request start or explicitly pin a historical snapshot ID. A request never changes snapshots mid-read; later requests may observe a newer committed snapshot.

Validation is deterministic and side-effect free. Before commit it MUST enforce:

- registered entity types, predicates, typed objects, and enum values;
- predicate domain/range and cardinality;
- same-tenant, non-dangling EntityRefs and FactRefs;
- no prohibited self-reference or merge cycle;
- at least one immutable evidence reference per directly interpreted fact; derived facts also require `derivation_refs` to supporting canonical facts;
- valid evidence hashes and locator bounds;
- `valid_from <= valid_to` and no CURRENT fact whose explicit interval lies wholly outside the current window;
- CONDITIONAL modality has at least one condition ref;
- PLANNED facts are not ACTUAL;
- attachment and customer-profile boundary invariants;
- explicit conflict-set assignment and lifecycle consistency;
- interpretation-confidence and confirmation policy.

Validation failure commits no entities, facts, conflicts, or snapshot. Reprocessing the same idempotency key returns the prior batch result and cannot duplicate canonical records.


---

## Canonical Concept Catalog

The semantic model MUST support at least the following concepts without requiring each one to become a separate database column.

| Domain | Canonical concepts |
|---|---|
| Business identity | display name, legal/trading identity where known, business description, business stage |
| Offers and services | offer, program, service, delivery mode, duration, parent/child offer structure, program description, intended transformation |
| Ideal customer | role, founder/owner status, business stage, business type, business characteristic, geography, vertical, employee range, pain pattern |
| Exclusions | excluded customer profile, exclusion reason, hard exclusion versus preference, applicability conditions |
| Market | geography, vertical, segment, market-validation scope, priority, temporal qualifier |
| Differentiation | known buying reason, candidate buying reason, differentiating capability, validation status |
| Brand | established tone trait, prohibited tone trait, brand preference, brand hypothesis |
| Objectives | objective, desired outcome, time horizon, target value where known |
| Metrics | metric definition, unit, target, baseline, time window, measurement status |

Canonical predicates and object types MUST be governed by a versioned vocabulary. Free text may be used for a meaningful concept value, but not as an untyped fragment bucket.

One answer may produce multiple entities and facts. Multiple answers may support, qualify, contradict, or supersede the same fact.


---

## Extraction and Interpretation Boundary

### Semantic Interpretation Required

Semantic interpretation is required where meaning depends on sentence context, relationships, scope, or attachment, including:

- what the company does;
- what an offer actually is;
- offer hierarchy and delivery structure;
- which description or outcome belongs to which offer;
- who the ideal customer is;
- customer characteristics and business stage;
- pain patterns;
- exclusions and their reasons;
- differentiation and buying reasons;
- brand preferences;
- objectives and metrics expressed in business language.

The interpreter MUST produce schema-constrained candidate entities and facts, with evidence references and SPEC-221 epistemic metadata. The architecture does not require a particular model vendor, but it does require semantic interpretation rather than substring survival.

### Narrow Deterministic Extraction

Deterministic parsers or regular expressions remain appropriate for narrow structural values when context is unambiguous, including:

- employee counts and explicit numeric ranges;
- URLs and email addresses;
- explicit prices and currencies;
- dates, durations, and percentages;
- phone numbers;
- geography names where the entity and scope are unambiguous.

These extractors produce typed candidate values. They do not independently decide the business proposition to which a value belongs. For example, extracting `1–10` does not establish whether it describes employees, locations, customers, or a target range without semantic context.

Regexes MAY assist with deterministic validation, candidate detection, or normalization after interpretation. A larger phrase dictionary or a larger collection of field-specific regexes is explicitly rejected as the canonical solution.

### Interpretation Contract

For each accepted operator turn, the semantic interpreter MUST return:

- zero or more candidate entities;
- zero or more typed candidate facts;
- evidence references, optionally with source spans;
- epistemic state and confidence per fact;
- relationships among facts/entities;
- unresolved ambiguities;
- interpretation/schema version;
- validation errors, if any.

The result MUST pass structural validation before canonical commit. Unsupported predicates, dangling entity references, untraceable facts, and invalid typed values fail validation.


---

## Relationship to SPEC-221

SPEC-221 defines the epistemic status of business propositions. This SPEC defines their semantic identity and structure.

Every canonical `BusinessFact` MUST carry:

- `epistemic_state`: `KNOWN`, `HYPOTHESIS`, `UNKNOWN`, `UNRESOLVED`, or `NOT_APPLICABLE`;
- confidence in the interpretation/status;
- evidence references;
- provenance;
- supersession or contradiction state where applicable.

Epistemic state applies per proposition, not per answer and not per section. One answer may establish a KNOWN market-validation stage, a HYPOTHESIS about the best segment, and an UNKNOWN customer buying reason.

Projection MUST NOT promote `HYPOTHESIS`, `UNKNOWN`, `UNRESOLVED`, or `NOT_APPLICABLE` to established facts merely because those propositions have text or evidence.


---

## Projection Strategy

Existing `normalizedFacts` may remain temporarily as a backward-compatible read model. They cease to be an independent source of truth.

The only permitted projection direction is:

```text
canonical entities + canonical BusinessFacts
  → versioned deterministic projector
  → normalizedFacts compatibility view
```

Projection rules MUST:

- select facts by typed predicate and epistemic state;
- preserve entity relationships and attachment before flattening;
- use deterministic ordering defined by the projection contract;
- label or omit non-KNOWN facts according to consumer needs;
- include canonical fact/entity IDs for traceability;
- never inspect raw evidence text to recover missing meaning;
- never split canonical prose on commas or conjunctions;
- declare projection version and source semantic-model version.

Every projection MUST include `projection_schema`, `projection_version`, `semantic_model_version`, `source_snapshot_id`, source fact/entity IDs, `generated_at`, `completeness`, `freshness`, and unresolved conflict IDs.

Completeness is `COMPLETE` only when all required predicates resolve without invalid references or unresolved conflicts; otherwise it is `PARTIAL` or `UNAVAILABLE`. Freshness is `CURRENT` only when the source snapshot is the tenant's active canonical snapshot and the projector version is current.

Projection generation is a pure deterministic function of canonical snapshot, projection contract version, and requested temporal window. Equal inputs MUST produce semantically equal output.

Only the canonical projector may write projection fields. Application services, migrations, renderers, and interpreters MUST NOT write `normalizedFacts` directly. Projections MUST NOT create, repair, or backfill canonical entities/facts. Reverse reconstruction is prohibited.

A migrated request chooses exactly one authority: `canonical_semantic` or an explicitly isolated legacy mode. It MUST NOT combine canonical facts with independently parsed legacy fields. Canonical availability is snapshot-level, not field-level; incomplete canonical projections fail closed or use a labeled whole-request legacy fallback under rollout policy.

Projection contracts are immutable by version. A projector declares which semantic-model and predicate-registry versions it can read. A consumer requests an explicit supported projection version; there is no implicit downgrade. Historical snapshots use a compatible historical projector or return `UNAVAILABLE`. Semantic-model migration creates a new snapshot and preserves the old snapshot; it does not reinterpret old projections in place.

Where a legacy field cannot faithfully represent the canonical model, the projector MUST provide a conservative summary and canonical references. It MUST NOT flatten distinct concepts into misleading list items.


---

## Downstream Migration Plan

Consumers migrate in the following order so operator-visible output validates the model before execution-oriented consumers rely on it.

### 1. Interview Recap

Recap reads canonical entities/facts through a recap view model. It does not read regex-derived `normalizedFacts` and does not re-parse raw evidence. Recap output includes only established facts plus explicitly labeled hypotheses/unknowns.

### 2. Blueprint

Blueprint persists canonical semantic-model version and canonical fact/entity references. Section prose becomes a presentation generated from canonical concepts, not an alternative fact store.

### 3. Executive Business Brief

Brief synthesis consumes the same canonical facts used by Blueprint. It may vary prose and emphasis, but may not independently infer or extract business concepts from evidence or Blueprint prose.

### 4. Max Workspace Context

`ClientIntelligenceContext` loads a canonical semantic attachment. Legacy scalar/list fields may be included as projections, but canonical entities/facts and epistemic state remain available for reasoning.

### 5. Specialist Context

Specialist context uses the canonical attachment or a capability-scoped projection generated from it. Specialists MUST NOT infer operator-approved constraints from raw evidence, section prose, or unlabeled hypotheses.

A capability-scoped projection MUST be relationship-closed. For every included fact/entity it includes:

- the owning entity chain and every traversed relationship;
- referenced condition and objective facts;
- epistemic and interpretation confidence;
- temporal status, modality, and validity interval;
- evidence-reference metadata and provenance;
- unresolved conflicts affecting applicability.

For example, projecting `delegation` requires:

```text
delegation (CAPABILITY)
  <- teaches_capability -- Management / People (PROGRAM)
  <- contains_program -- 12-week 1:1 coaching (OFFER)
  <- offers -- Babrun (BUSINESS)
```

A specialist MUST NOT receive `delegation` as a service-like scalar without this chain. If closure cannot be satisfied, the concept is omitted and the projection is marked `PARTIAL`; the specialist does not re-read raw evidence to repair it.

If a shared capability, pain, or outcome has relationships to multiple programs, relationship closure includes every applicable owner path under the requested scope. Paths are never deduplicated by choosing one owner. Consumers receive the shared entity once plus all typed relationship edges.

If multiple customer profiles are relevant, the projection returns separate profile subgraphs keyed by profile ID. It does not union their attributes. The requesting capability may constrain profile selection; an unconstrained multi-profile request returns all relevant profile subgraphs and marks their offer/program relationships explicitly.

During migration, each consumer records whether it used `canonical_semantic`, `legacy_projection`, or `legacy_sections`. This is required for rollout observability.


---

## Legacy Compatibility

Existing sessions and approved Blueprints may lack canonical semantic entities/facts.

Compatibility policy:

1. Existing raw evidence remains authoritative provenance.
2. New interviews write canonical semantic understanding and derive legacy projections.
3. Existing sessions are not silently reinterpreted during ordinary reads.
4. A versioned, auditable backfill may reinterpret historical raw evidence after review.
5. Backfilled facts record the source session, interpretation version, timestamp, and backfill origin.
6. Historical records without raw evidence remain `legacy_sections`; their prose may be displayed but MUST NOT be represented as newly verified canonical facts.
7. If canonical interpretation and legacy fields disagree, canonical facts win for migrated consumers; the discrepancy is recorded.
8. Rollback disables canonical reads for a consumer without deleting canonical records or rewriting legacy evidence.

No legacy compatibility path may parse a malformed recap or Brief and promote the result into canonical understanding.


---

## Failure Handling

Semantic interpretation fails closed.

If interpretation times out, returns invalid structure, lacks evidence references, or cannot resolve meaning:

- preserve the raw evidence;
- do not overwrite prior canonical facts;
- record an interpretation failure with reason and version;
- mark affected concepts `UNRESOLVED` where appropriate;
- permit bounded retry or operator review;
- do not fall back to regex/list-fragment extraction as canonical facts;
- do not present extracted fragments as confirmed understanding.

Canonical commit of one interview turn MUST be atomic across its entities, facts, evidence links, and supersession updates. Partial semantic writes are not visible to downstream consumers.

Repeated processing of the same source turn and interpretation version MUST be idempotent.

Contradictory evidence adds a contradiction/supersession relationship and requests clarification where policy requires. It does not silently replace an established fact.


---

## Babrun Acceptance Fixture

The following exact production answers are canonical regression fixtures.

### Identity Answer

```text
The business is Babrun. We are building coaching and transformation programs for owners of small, founder-led businesses.

Today, the primary offer is a 12-week 1:1 coaching program focused on one of three areas: management and people, sales and customers, or the business idea/business model.

The goal is practical transformation rather than just education. The founder learns new capabilities while applying them directly inside their actual business.

Right now, we're in a market-validation stage in the U.S. The 1:1 model lets us learn which pains are strongest, what founders are willing to pay to solve, which offers convert, what objections arise, and what pricing the market accepts. Longer term, the intention is to use what we learn to develop scalable group transformation programs.
```

Required concepts include:

- business name: Babrun;
- business description: coaching and transformation programs for small-business founders;
- current delivery: 12-week 1:1 coaching;
- current stage: U.S. market validation;
- current learning purpose: pains, willingness to pay, offer conversion, objections, and accepted pricing;
- longer-term direction: scalable group transformation programs.

### Services Answer

```text
Today, the primary service is 12-week 1:1 coaching for small-business founders.

There are three main coaching programs:

Management / People: helping founders delegate effectively, manage employees, identify performance problems earlier, reduce the amount of work that comes back to the founder, and build a business that depends less on them personally.

Sales / Customers: helping founders better understand customer needs and buying decisions, sell around value rather than features, move opportunities forward, pursue larger or more valuable customers, and develop a more repeatable sales process.

Product / Business Idea: helping founders think more like entrepreneurs rather than functional experts — identifying higher-value opportunities, stronger differentiation, better economics, growth potential, and potentially a stronger business model.

For now, these are delivered 1:1. The longer-term direction is to develop group-based 12-week transformation programs once the market and offers are validated.
```

Required concepts include:

- parent/current offer: 12-week 1:1 coaching;
- audience: small-business founders;
- program: Management / People, with its complete description attached;
- program: Sales / Customers, with its complete description attached;
- program: Product / Business Idea, with its complete description attached;
- current delivery relationship: all three are delivered 1:1;
- future direction: group-based 12-week transformation programs after validation.

Program outcomes such as `manage employees`, `move opportunities forward`, and `stronger differentiation` MUST remain attached to their respective program. They MUST NOT become independent services.

### Ideal Customer Answer

```text
Our ideal customer is the owner or founder of an operating small business in the United States, generally with 1–10 employees.

Initially, we want to focus on service businesses, especially businesses where results depend heavily on employees and their behavior. We don't need to restrict ourselves to one specific service vertical yet.

More important than the exact industry is the presence of a specific pattern of pain.

That could include problems with employees, having to constantly supervise or do everything themselves, being unable to step away from the business, burnout, inconsistent customers or sales, poor-quality customers, weak profit or cash flow, or feeling that the amount of work they're putting into the business isn't producing enough financial return.

The underlying pattern we're interested in is a founder who has built a real operating business, but whose capabilities as a manager, salesperson, or entrepreneur haven't necessarily evolved as quickly as the business itself.
```

Required concepts include:

- customer role: owner/founder;
- customer business stage: operating;
- geography: United States;
- employee range: generally 1–10;
- initial vertical focus: service businesses, without claiming one validated service vertical;
- business characteristic: results depend heavily on employees and employee behavior;
- pain patterns: employee problems, constant supervision/founder dependency, inability to step away, burnout, inconsistent sales/customers, poor-quality customers, weak profit/cash flow, and insufficient financial return for effort;
- capability gap: founder management, sales, or entrepreneurial capability has not evolved with the business.


---

## Babrun Acceptance Criteria

### Expected Canonical Graph

The fixture MUST produce a graph semantically equivalent to this graph. IDs are illustrative stable IDs; storage IDs may differ.

```text
BUSINESS babrun
  --offers [KNOWN, CURRENT, ACTUAL]--> OFFER coaching_12_week_1_to_1

OFFER coaching_12_week_1_to_1
  --has_delivery_mode [KNOWN, CURRENT, ACTUAL]--> "1:1"
  --has_description [KNOWN, CURRENT, ACTUAL]--> "12-week coaching for small-business founders"
  --targets_customer_profile [KNOWN, CURRENT, ACTUAL]--> CUSTOMER_PROFILE founder_led_small_business_validation
  --contains_program [KNOWN, CURRENT, ACTUAL]--> PROGRAM management_people
  --contains_program [KNOWN, CURRENT, ACTUAL]--> PROGRAM sales_customers
  --contains_program [KNOWN, CURRENT, ACTUAL]--> PROGRAM product_business_idea
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME practical_transformation_applied_in_the_founders_business

PROGRAM management_people
  --has_description [KNOWN, CURRENT, ACTUAL]--> "Management / People"
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY delegation
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY employee_management
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY early_performance_diagnosis
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME reduced_founder_dependency

PROGRAM sales_customers
  --has_description [KNOWN, CURRENT, ACTUAL]--> "Sales / Customers"
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY customer_need_understanding
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY value_based_selling
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY opportunity_progression
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME repeatable_sales_process
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME higher_value_customers

PROGRAM product_business_idea
  --has_description [KNOWN, CURRENT, ACTUAL]--> "Product / Business Idea"
  --teaches_capability [KNOWN, CURRENT, ACTUAL]--> CAPABILITY entrepreneurial_opportunity_identification
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME stronger_differentiation
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME better_economics
  --targets_outcome [KNOWN, CURRENT, ACTUAL]--> OUTCOME stronger_business_model

CUSTOMER_PROFILE founder_led_small_business_validation
  --has_role [KNOWN, CURRENT, ACTUAL]--> "owner/founder"
  --has_business_stage [KNOWN, CURRENT, ACTUAL]--> "operating"
  --has_geography [KNOWN, CURRENT, ACTUAL]--> "United States"
  --has_employee_range [KNOWN, CURRENT, ACTUAL]--> 1..10 employees (qualifier: generally)
  --has_vertical [HYPOTHESIS, CURRENT, INTENDED]--> "service businesses"
  --has_characteristic [HYPOTHESIS, CURRENT, INTENDED]--> "results depend heavily on employee behavior"
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN employee_problems
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN constant_supervision_and_founder_dependency
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN inability_to_step_away
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN burnout
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN inconsistent_sales_or_customers
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN poor_quality_customers
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN weak_profit_or_cash_flow
  --addresses_pain [HYPOTHESIS, CURRENT, INTENDED]--> PAIN insufficient_return_for_effort
  --has_characteristic [KNOWN, CURRENT, ACTUAL]--> "management, sales, or entrepreneurial capability has not evolved with the business"

BUSINESS babrun
  --has_business_stage [KNOWN, CURRENT, ACTUAL]--> "U.S. market validation"
  --has_validation_status [UNKNOWN, CURRENT, ACTUAL]--> null (FACT market_validation_complete)
  --offers [KNOWN, PLANNED, CONDITIONAL; condition_ref: FACT market_validation_complete]--> OFFER group_transformation_programs

BUSINESS babrun
  --has_buying_reason [UNKNOWN, CURRENT, ACTUAL]--> null (FACT actual_customer_buying_reason)
  --has_buying_reason [HYPOTHESIS, CURRENT, INTENDED]--> "practical transformation may influence buying"
  --has_brand_voice [UNKNOWN, CURRENT, ACTUAL]--> null (FACT established_formal_brand_voice)
  --has_brand_voice [HYPOTHESIS, CURRENT, INTENDED]--> "practical, direct, founder-to-founder, outcome-focused"
  --avoids_brand_trait [KNOWN, CURRENT, ACTUAL]--> "overly academic language"
  --avoids_brand_trait [KNOWN, CURRENT, ACTUAL]--> "generic motivational-coaching language"
  --avoids_brand_trait [KNOWN, CURRENT, ACTUAL]--> "stereotypical business-guru tone"
```

`market_validation_complete` is a referenced `has_validation_status` fact, not copied condition text. Until a later fact resolves KNOWN and true under the correction/current-resolution policy, `group_transformation_programs` remains planned and cannot satisfy a current-offers query.

The dedicated profile and temporal/modal metadata scope service-business targeting to the current validation effort. It MUST NOT become a permanent/global ICP fact.

The expected graph is exhaustive for the semantic concepts required from these fixtures in v1. Supporting phrasing may enrich descriptions/evidence spans, but an interpreter MUST NOT omit a listed node/edge or invent an additional canonical proposition not supported by the answers. “Practical transformation rather than just education” is represented by `practical_transformation_applied_in_the_founders_business`, not discarded as presentation copy.

1. The exact three fixture answers produce schema-valid canonical entities and BusinessFacts.
2. Every accepted fact references the source evidence and carries SPEC-221 epistemic state, confidence, and provenance.
3. The complete raw answers remain unchanged as evidence.
4. The required identity, services, program relationships, ideal-customer attributes, and pain patterns are represented as independently meaningful concepts.
5. Program descriptions remain attached to the correct program entities.
6. The canonical model does not contain independent concepts with values `Today`, `but whose capabilities as a manager`, or `salesperson`.
7. `salesperson` may appear only inside a meaningful capability-gap proposition when supported by the source answer; it may not appear as an ideal-customer segment.
8. Recap, Blueprint, Executive Brief, Max context, and specialist context can all be generated from the same canonical semantic understanding without re-parsing raw answers.
9. Those consumers preserve the same business meaning even when their presentation differs.
10. SPEC-221 UNKNOWN, HYPOTHESIS, UNRESOLVED, and NOT_APPLICABLE propositions remain distinguishable and are never promoted through projection.
11. Existing known-fact interviews remain readable during migration.
12. A semantic interpretation failure preserves evidence and does not emit regex-derived fragments as facts.
13. The expected graph passes domain/range, cardinality, attachment, reference, temporal/modal, and evidence validation.
14. Current-offer projection includes 12-week 1:1 coaching and its three programs but excludes planned group programs.
15. Program capability/outcome projections preserve the owning program and parent-offer chain.
16. Service-business focus and candidate pain patterns remain scoped to the validation profile with HYPOTHESIS + CURRENT + INTENDED metadata.
17. Differentiation and brand preserve the separate UNKNOWN, HYPOTHESIS, and KNOWN propositions shown above.


---

## Round-Trip Semantic Acceptance

Starting from one committed canonical snapshot, each consumer MUST represent semantically equivalent business understanding without reading or re-parsing raw evidence:

1. interview recap;
2. Blueprint;
3. Executive Business Brief;
4. Max workspace;
5. specialist context.

Presentation wording may differ. Entity identity, relationship attachment, epistemic state, temporal/modal scope, conditions, and current-resolution outcome MUST remain equivalent.

Acceptance compares consumer semantic manifests, not prose strings. The canonical manifest schema is:

```json
{
  "source_snapshot_id": "snapshot_123",
  "semantic_model_version": 1,
  "projection_version": 1,
  "entity_ids": ["sorted stable IDs"],
  "facts": [
    {
      "fact_id": "fact_1",
      "subject_entity_id": "entity_1",
      "predicate": "registered_predicate",
      "object_identity": "canonical literal hash or referenced stable ID",
      "epistemic_state": "KNOWN",
      "temporal_status": "CURRENT",
      "modality": "ACTUAL",
      "condition_fact_ids": [],
      "conflict_set_id": "slot_1"
    }
  ],
  "relationship_edges": ["sorted subject/predicate/object triples"],
  "profile_subgraph_ids": [],
  "unresolved_conflict_ids": []
}
```

Two manifests are semantically equivalent when they reference the same source snapshot and contain identical sets of stable entity IDs, selected fact IDs, canonical object identities, relationship edges, epistemic/temporal/modal states, condition/conflict IDs, and profile boundaries. Array order and presentation prose are ignored. A deliberately capability-scoped specialist manifest may be a relationship-closed subset; every included item must equal the corresponding canonical manifest item, and its declared scope determines the expected subset.

A round trip fails if a consumer promotes a planned offer to current, flattens a capability/outcome into a service, merges different customer profiles, drops or promotes epistemic state, loses a condition/ownership edge, or parses evidence/rendered prose to restore meaning.


---

## Implementation Phases

These phases describe sequencing only. They are not implementation authorization.

### Phase 0 — Contract and Fixture Approval

- approve the v1 entity/fact schema and closed predicate registry defined here;
- approve the three exact Babrun fixtures and expected canonical graph;
- inventory every `normalizedFacts` reader and writer;
- define semantic-manifest equivalence and projection contract tests from the normative invariants here;
- verify proposed persistence DDL/API details against the already-decided authority, snapshot, reference, and transaction invariants; physical table/index design is not an additional architecture decision.

### Phase 1 — Canonical Store and Interpretation Boundary

- add versioned evidence/entity/fact persistence contracts;
- add schema-constrained semantic interpreter interface;
- add deterministic structural-value parsers as subordinate candidate signals;
- add validation, atomic commit, idempotency, and failure records.

### Phase 2 — Shadow Interpretation and Compatibility Projection

- interpret new interview turns in shadow mode;
- derive compatibility `normalizedFacts` only from canonical facts;
- compare canonical projections with legacy outputs without changing production reads;
- instrument source selection and semantic discrepancies.

### Phase 3 — Operator-Visible Read Migration

- migrate interview recap;
- migrate Blueprint generation/persistence;
- migrate Executive Business Brief;
- gate each transition on Babrun and existing interview regression suites.

### Phase 4 — Max and Specialist Read Migration

- migrate Max workspace attachment to canonical semantic context;
- migrate capability-scoped specialist projections;
- retain observable legacy fallback only for unmigrated historical sessions.

### Phase 5 — Historical Backfill and Legacy Retirement

- run reviewed, versioned backfill for eligible historical evidence;
- surface unresolved or conflicting interpretations for operator review;
- remove legacy regex-derived fields as semantic authority;
- retain compatibility projections only while named consumers still require them.


---

## Non-Goals

This SPEC does not:

- redesign Scout;
- modify the Operator Scorecard;
- implement the semantic interpreter;
- select a model vendor;
- patch or expand existing extraction regexes;
- rewrite Babrun's business information;
- redesign interview question policy;
- remove raw evidence or provenance;
- authorize historical backfill.


---

## Phase-0 Architectural Decisions

These decisions are normative and are not deferred to implementation:

1. **Vocabulary:** Canonical state uses entity vocabulary v1 and the closed predicate registry in this SPEC. Extension requires a versioned architectural change.
2. **Persistence authority:** Canonical evidence, entities, facts, conflict sets, interpretation batches, and snapshots are durable tenant-owned records in the primary PostgreSQL system of record. Session JSON and Blueprint prose are not canonical authority. Approved Blueprints pin an immutable canonical `snapshot_id` and semantic-model version.
3. **Commit boundary:** One accepted turn commits its evidence link, entities, facts, relationships, conflict changes, and new snapshot atomically. Consumers see only committed snapshots.
4. **Confirmation policy:** Direct unambiguous operator assertions may auto-commit as KNOWN after schema validation and the interpretation-confidence threshold. Inferences cannot auto-commit as KNOWN. Low-confidence or structurally ambiguous interpretations require confirmation or remain UNRESOLVED.
5. **Confidence:** Interpretation and epistemic confidence are separate fields with separate calibration/version metadata. Neither implies the other.
6. **Temporal/modal semantics:** Every fact carries the required temporal status and modality. Current-offer reads require CURRENT + ACTUAL. Epistemic state does not determine temporality.
7. **Corrections:** Canonical history is append-only and resolved through typed correction, supersession, contradiction, and conflict-set rules. Recency alone never wins.
8. **Projection authority:** `normalizedFacts` and consumer view models are immutable generated projections from one canonical snapshot. Direct writes and reverse reconstruction are prohibited.
9. **Legacy authority:** A request uses either one canonical snapshot or a whole-request legacy fallback. Field-level mixing is prohibited. Historical prose without source evidence cannot become verified canonical facts.
10. **Specialist consumption:** Capability-scoped projections are relationship-closed and carry epistemic, temporal, conflict, and provenance metadata. Raw evidence is not a specialist reconstruction mechanism.
11. **Historical backfill eligibility:** Only sessions with immutable raw operator evidence and tenant/source identity are eligible. Backfill remains separately reviewed and is not authorized here.
12. **Rollout evidence:** Required measures are invalid-fragment rate, attachment preservation, epistemic/temporal preservation, unresolved/conflict rate, operator correction rate, semantic-manifest equivalence across consumers, and legacy fallback rate. Prose preference alone is not success evidence.

Implementation may choose table names, indexes, serialization details, and interpreter vendor within these constraints. Those choices may not alter canonical authority or semantics.


---

## Completion Criteria

This SPEC is complete only when:

1. raw evidence and semantic facts have separate durable representations;
2. canonical facts express typed business propositions and relationships rather than arbitrary fragments;
3. SPEC-221 metadata attaches per canonical proposition;
4. compatibility fields are projections, never independent semantic authority;
5. every named downstream consumer has a migration and fallback contract;
6. failure cannot silently restore regex/list-fragment extraction as canonical understanding;
7. the exact Babrun fixtures pass across recap, Blueprint, Brief, Max, and specialist projections;
8. legacy sessions remain readable without promoting malformed historical prose into verified facts;
9. implementation remains blocked pending architectural approval.


## Architectural Decision Requested

Approve, revise, or reject the canonical evidence → entity/fact → projection model and migration sequence defined above.

**Stop for architectural review. Do not implement.**