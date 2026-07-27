# Evidence Core — Domain Separation Audit

| Field | Value |
|---|---|
| **Status** | Architectural validation only |
| **Date** | 2026-07-26 |
| **Related** | [ADR-009](../adr/ADR-009_Evidence_Platform_Architecture.md), [SPEC-015](../specs/SPEC-015_Market_Intelligence_Domain.md), [SPEC015_FEASIBILITY.md](SPEC015_FEASIBILITY.md) |
| **Scope** | Report only — no runtime, schema, or package changes |

## Purpose

Identify every place where CRM / outbound assumptions leak into the Evidence Core, distinguish core from domain adapters, and recommend small generality refactors that do not change current CRM behavior.

**Evidence Core** (intended meaning in this audit): Knowledge Graph runtime, Evidence/Claim stores, Confidence math, Dual-write / outbox / ledger infrastructure, Memory runtime math, Reasoning orchestration shell (context → strategies → aggregate → explain).

**Domain layer** (intended meaning): entity vocabularies, provider mappers, strategy packs, recommendation action taxonomies, operational event taxonomies, UI/product stages.

---

## Executive summary

| Layer | Domain-agnostic? | Notes |
|---|---|---|
| Graph storage + `KnowledgeService` ensure/find/search | Mostly yes | Closed CRM ontology (`company`/`person`/`interaction`) |
| Evidence + Claim engines + noisy-OR confidence | Yes | Pure ID/confidence math |
| Dual-write outbox / ledger / apply | Infrastructure yes; CRM helpers no | `writeCompany`/`Prospect`/`Touchpoint` are adapters |
| Sync mappers / relational rebuild | No | Hard CRM SQL and column shapes |
| Reasoning orchestration shell | Yes (injectable) | Defaults are CRM |
| Default strategies + recommendation actions | No | Outreach / ICP / email heuristics |
| Memory snapshot/diff math | Yes | Public API keyed on `companyId` |
| Hypothesis engine | N/A | Does not exist — `ClaimEngine` is the stand-in |

**Bottom line:** Math and durable ingest infrastructure can support multiple domains. Wiring (ontology, evaluate subject, default strategies, CRM sync) cannot host a Market Domain via adapters alone without either (a) force-fitting markets into CRM types, or (b) additive, domain-neutral extensions described in the recommendations below.

---

## 1. Generic components

Already domain-agnostic (or injectable without CRM vocabulary):

### Knowledge

| Component | Location | Why generic |
|---|---|---|
| Base node contract | `packages/knowledge/types/baseNode.js` | `id`, `tenantId`, `type`, timestamps, opaque `metadata` |
| `GraphRepository` | `packages/knowledge/repositories/GraphRepository.js` | CRUD + neighbors + find |
| Persistent / in-memory repos | `PersistentGraphRepository.js`, `InMemoryGraphRepository.js` | Storage swap; body JSONB not CRM mirror |
| `KnowledgeService` core writes/reads | `services/KnowledgeService.js` | `createNode` / `ensureNode` / `updateNode` / `createEdge` / `ensureEdge` / `findNode` / `search` |
| Query traversal / path / timeline | `query/Traversal.js`, `query/Timeline.js`, path APIs | Graph algorithms |
| Evidence nodes + `EvidenceEngine` | `nodes/Evidence.js`, `evidence/EvidenceEngine.js` | `sourceType` / `sourceId` / `confidence` / `payload` |
| Claim lifecycle | `claims/ClaimEngine.js` | Statement + SUPPORTS + ABOUT |
| Confidence combine | `confidence/calculateConfidence.js` | Noisy-OR over numeric confidences |
| Event bus envelope | `events/KnowledgeEventBus.js` | `{ id, type, tenantId, payload, occurredAt }` |
| `ensure*` ingest pattern | `events/KnowledgeIngestor.js` (ensure paths) | Idempotent upserts |
| Generic sync mutation | `sync/mappers.js` → `mapEntityMutation` | Any `entityKind` + nested `knowledgeEvent` |
| Sync ledger + outbox tables | `knowledge_sync_ledger`, `knowledge_outbox` | Tenant + key infrastructure |
| `GraphSyncEngine.apply` | `sync/GraphSyncEngine.js` | Idempotent apply given envelope |
| `createKnowledgeRuntime` | `packages/knowledge/index.js` | Wires service + bus + optional sync |

### Max

| Component | Location | Why generic |
|---|---|---|
| `StrategyInterface` / `confidenceFromEvidence` | `strategies/StrategyInterface.js` | Count × avg confidence → 0–100 |
| `StrategyRegistry.evaluateAll` | `strategies/StrategyRegistry.js` | Executor; defaults are separate |
| `ScoreAggregator` | `aggregation/ScoreAggregator.js` | Weighted normalize of score/confidence |
| `ReasoningEngine` orchestration | `reasoning/ReasoningEngine.js` | Accepts injected `contextBuilder` + `registry` |
| Snapshot build / diff math | `memory/SnapshotEngine.js`, `memory/diff/DiffEngine.js` | Numeric + id-set compares |
| Policy rules (threshold/time) | Confidence / Freshness / Contradiction / Risk / Cooldown | Mostly domain-neutral thresholds |

---

## 2. CRM coupling

Places where outbound / CRM assumptions leak into or sit inside the “core” packages.

### 2.1 Closed ontology (Evidence Core vocabulary — business, not SQL)

| Assumption | Location | Core vs adapter? |
|---|---|---|
| Node types only `company` \| `person` \| `interaction` \| `evidence` \| `claim` | `types/nodeTypes.js` | **Core ontology today** — not multi-domain |
| Edge types `HAS_CONTACT`, `WORKS_FOR`, `PARTICIPATED_IN`, `KNOWS`, … | `edges/edgeTypes.js` | Mixed: `SUPPORTS`/`ABOUT`/`GENERATED` generic; employment/contact CRM |
| Bus events `company_observed` / `person_observed` / `interaction_recorded` | `events/KnowledgeEventBus.js` | CRM-shaped ingest vocabulary |
| Person fields `email`, `title` | `nodes/Person.js` | Soft CRM |
| Interaction fields `channel`, `actionType`, `summary` | `nodes/Interaction.js` | Soft CRM (touchpoint-shaped) |

**Judgment:** Evidence/Claim are domain-agnostic. Company/Person/Interaction are a **B2B CRM ontology baked into core**. Market entities (Asset, Regime, Hypothesis, …) cannot be first-class without additive type registry work — or dishonest aliasing (Asset → Company).

### 2.2 Sync & dual-write (hard CRM — belongs in adapters)

| Assumption | Location | Belongs? |
|---|---|---|
| `SYNC_EVENTS` company/prospect/touchpoint | `sync/syncEvents.js` | **Adapter** |
| `mapCompanyRow` / `mapProspectRow` / `mapTouchpointRow` | `sync/mappers.js` | **Adapter** |
| `client_id` → `tenantId`, `icp_score`, `vertical`, CRM ids | `sync/mappers.js` | **Adapter** |
| `PostgresRelationalSource` SQL on `companies` / `prospects` / `touchpoints` | `adapters/PostgresRelationalSource.js` | **Adapter** |
| `rebuildFromRelational` hardcoded CRM entity plan | `GraphSyncEngine.js` | **Adapter plan on core engine** |
| `envelopeForCompany/Prospect/Touchpoint` | `dualWrite/envelopes.js` | **Adapter** |
| `KnowledgeWriter.writeCompany/Prospect/Touchpoint` | `dualWrite/KnowledgeWriter.js` | **Adapter helpers** |
| Operational taxonomy `prospect.*`, `comm.email_*`, Brevo mapping | `dualWrite/operationalEvents.js`, `envelopes.js` | **Product / CRM adapter** |
| Flight stages Command Deck / briefing | `dualWrite/operationalEvents.js` | **Product ops, not Evidence Core** |
| `dbClient` / Scout `safeWrite*` | `utils/knowledgeDualWrite.js`, `dbClient.js`, `leadgen.js` | **App adapters (correct layer)** |

**Not found in knowledge package:** Campaign entities, email sequences as first-class graph types, setter/closer pipeline fields, commissions. Touchpoints are the interaction projection; Brevo appears only as channel/action mapping.

### 2.3 Query & ingest convenience (CRM façade on core)

| Assumption | Location | Belongs? |
|---|---|---|
| `findCompanies` / `findPeople` / `findInteractions` | `KnowledgeService.js`, `QueryEngine.js` | Thin CRM façade — purer core would be `find({ type, predicate })` only |
| Industry / location / tech filters | `query/Filters.js` | Company metadata heuristics |
| Auto `WORKS_FOR` / `HAS_CONTACT` on person observe | `KnowledgeIngestor.js` | CRM relationship inference |

### 2.4 Reasoning / Memory / Policy (CRM evaluate path)

| Assumption | Location | Belongs? |
|---|---|---|
| `evaluate({ tenantId, companyId })` | `ReasoningEngine.js` | Subject naming — should be domain-neutral `subjectId` |
| Context requires `company`, `people`, `interactions` | `ReasoningContextBuilder.js`, `ReasoningTypes.js` | **CRM context pack** |
| Default 7 strategies (opportunity, engagement, relationship, DM, overflow, technology, risk) | `strategies/*`, `createDefaultStrategyRegistry` | **CRM strategy pack** |
| Engagement open/click/reply/bounce/unsubscribe | `EngagementStrategy.js` | **CRM** |
| Recommendation actions `follow_up_outreach`, `nurture_sequence`, `request_intro` | `RecommendationBuilder.js`, `ReasoningTypes.js` | **CRM action taxonomy** |
| Memory APIs keyed on `companyId` | `MemoryEngine.js`, `SnapshotRepository` | Naming coupling |
| Change types `NEW_DECISION_MAKER`, `NEW_HIRING_SIGNAL` | Memory change detection | **CRM labels** |
| Default policy email/LinkedIn/DM/outreach limits | `PolicyTypes.DEFAULT_TENANT_POLICY` | **CRM policy profile** |

### 2.5 Hypothesis engine

**There is no Hypothesis engine.** Closest substrate:

- Graph `claim` nodes + `ClaimEngine.evaluateClaim`
- Strategy keyword matches against claim text (CRM strategies)

SPEC-015’s Hypothesis object is aspirational relative to current code.

---

## 3. Recommended refactors

Small refactors that improve generality **without changing CRM behavior** when defaults remain CRM packs. Describe only — not implemented by this task.

| ID | Recommendation | Effort | Risk | Migration impact |
|---|---|---|---|---|
| **R1** | **Subject alias** — accept `subjectId` (or `entityId`) alongside `companyId` in Reasoning `evaluate`, Memory `remember`/`history`/`trend`, snapshot ids; deprecate nothing yet | S | Low | Compat shim; CRM callers unchanged |
| **R2** | **Pluggable context builder** — document/inject `ContextBuilder` (already partially supported on `ReasoningEngine`); CRM default remains `ReasoningContextBuilder` | S | Low | Market runtime passes builder later; CRM default path identical |
| **R3** | **Named strategy packs** — rename conceptually: `createDefaultStrategyRegistry` → CRM pack export; keep current function as alias so tests/CRM unchanged | S | Low | Zero behavior change if alias preserved |
| **R4** | **Claim-as-Hypothesis convention** — document metadata `{ kind: 'hypothesis', sampleSize }` on claims; use `evaluateClaim` for confidence updates; defer `NODE_TYPES.HYPOTHESIS` | S | Low | Docs + convention only; no schema |
| **R5** | **Generic find façade** — add `findNodes({ type, filters })` as the primary API; keep `findCompanies`/`findPeople`/`findInteractions` as wrappers | S | Low | Additive API; CRM wrappers stay |
| **R6** | **Extract CRM sync adapters (docs boundary first)** — mark `sync/mappers` CRM row mappers, `PostgresRelationalSource`, CRM `write*` helpers as “CRM adapter surface inside package”; no move yet | S | None | Documentation / ownership only |
| **R7** | **Pluggable rebuild plan** — `rebuildFromRelational` accepts entity plan + mapper functions; default plan = companies→prospects→touchpoints | M | Med | Must keep default plan identical for CRM rebuild parity |
| **R8** | **Domain-neutral recommendation envelope** — separate score/confidence/signals from CRM `RECOMMENDED_ACTIONS`; CRM builder maps to outreach actions | M | Med | Policy/UI that switch on action strings need dual map |
| **R9** | **Policy profiles** — `DEFAULT_TENANT_POLICY` remains CRM; add documented market research profile shape (no outreach channels) for later | S | Low | Config only when market lands |
| **R10** | **Soft-gate CRM change labels** — hiring/DM change types only when CRM strategies present in snapshot; generic score/confidence/claim diffs always | S | Low | Existing CRM watches keep working |

**Do not do in readiness phase:** new packages, market adapters, new node types, migrations, moving files.

---

## 4. Proposed stable interfaces (describe only)

Interfaces for multi-domain ingest. **Not implemented.**

### 4.1 EntityAdapter

```text
EntityAdapter {
  domain: string                    // 'crm' | 'market' | …
  entityKinds: string[]             // provider/domain kinds
  toGraphEntity(raw): {
    tenantId, type, id, properties, metadata, revision?
  }
}
```

Maps domain records → graph node intents. CRM today: company/prospect → `company`/`person`. Market later: asset/contract → future types or claim/evidence-only Phase 1.

### 4.2 EventAdapter

```text
EventAdapter {
  domain: string
  source: string                    // 'coinbase' | 'brevo' | …
  normalize(providerPayload): KnowledgeEvent | SyncEnvelope
  idempotencyKey(normalized): string
}
```

All providers converge on SPEC-014-shaped events / sync envelopes. Downstream never sees provider schemas.

### 4.3 RelationshipAdapter

```text
RelationshipAdapter {
  inferEdges(entities, events): Array<{
    fromId, toId, edgeType, metadata, sourceEventId
  }>
}
```

CRM today: prospect→company `WORKS_FOR`, company→person `HAS_CONTACT`. Market later: Asset→VolatilitySpike `EXPERIENCED`, etc. Core only `ensureEdge`s; it does not infer domain semantics.

### 4.4 EvidenceAdapter

```text
EvidenceAdapter {
  toEvidence(normalizedEvent): {
    sourceType, sourceId, confidence, payload, subjectIds[]
  }
  toClaim?(normalizedEvent, evidenceIds): ClaimIntent | null
}
```

Separates “what happened” (evidence) from “what we believe” (claim/hypothesis). Confidence numbers may be domain-suggested but must be recomputable by core noisy-OR / `evaluateClaim`.

### 4.5 How they relate to today’s surfaces

| Proposed interface | Today’s closest surface |
|---|---|
| EntityAdapter | `mapCompanyRow` / `mapProspectRow` / `mapTouchpointRow` |
| EventAdapter | `envelopeFor*` + `normalizeKnowledgeEvent` + `mapEntityMutation` |
| RelationshipAdapter | `KnowledgeIngestor` auto-edges + mapper-emitted `EDGE_REQUESTED` |
| EvidenceAdapter | Evidence fields inside envelopes + `EvidenceEngine.record` |

`GraphSyncEngine.apply`, outbox, ledger, `KnowledgeService`, Claim/Evidence engines remain **consumers** of adapter output — not aware of Coinbase vs Scout.

---

## 5. Replay readiness audit

### What exists

| Store | Contents | Replay role |
|---|---|---|
| `knowledge_outbox` | Full `sync_envelope` JSONB + payload/evidence + idempotency key | Primary durable dual-write log; `processOutbox` re-applies |
| `knowledge_sync_ledger` | Key + small record (`syncType`, entity refs) | Idempotency gate — **not** full event store |
| `knowledge_flight_stages` | Stage checklist | Observability only |
| Graph nodes/edges | Materialized state | Result of apply |
| `KnowledgeEventBus._history` | In-process array | Tests only; not durable |

### What works

- Stable node ID helpers (`sync/stableIds.js`)
- `ensureNode` / `ensureEdge` / `ensureEvidence`
- Ledger skip on same key
- Outbox unique `(tenant_id, idempotency_key)`

### Gaps for deterministic multi-domain replay

No migrations in this task — list only:

1. **No global ordered event log** — no sequence/causality across producers; CRM rebuild order is fixed (companies → prospects → touchpoints), not live interleaving.
2. **Ledger omits envelopes** — cannot rebuild from ledger alone.
3. **Revision often CRM timestamp or `'v1'`** — missing `updated_at` collapses revisions; updates may be skipped.
4. **Touchpoint revision uses `created_at` only** — content changes may not new-key.
5. **`CLAIM_PROPOSED` not ensure-stable** — force-replay can duplicate claims if sync key does not block.
6. **Bus IDs default `randomUUID()`** — producers must pass stable ids (dual-write does; ad-hoc publish may not).
7. **Outbox `ON CONFLICT` no-ops payload refresh** — same key keeps first envelope.
8. **No bi-temporal valid-from / observed-at on all nodes** — architecture wants it; only partially present (`occurredAt` on interactions).
9. **No domain / producer sequence metadata** — insufficient for cross-domain deterministic merge later.
10. **In-memory bus history** — lost on process restart for non-outbox publishes.

**Practical replay today:** (A) re-apply `knowledge_outbox.sync_envelope` with `force`, or (B) `rebuildFromRelational` from CRM (CRM-only).

**Market implication:** adapters must write complete sync envelopes into outbox (or equivalent append-only log) with stable idempotency keys and content-aware revisions; otherwise MID cannot replay deterministically.

### Missing metadata checklist (for a future replay-hardening spec)

- Monotonic `sequence` or `(producer, offset)` per tenant
- Full envelope retained in ledger **or** dedicated `knowledge_event_log`
- Content hash / revision policy documented per entity kind
- Stable claim IDs on propose/ensure
- Explicit `observedAt` / `validFrom` / `validTo` where relevant
- `domain` (or adapter id) on every envelope
- Ordered drain semantics for mixed CRM + market producers

---

## 6. Placement map

```text
Evidence Core (keep / harden)
  BaseNode · GraphRepository · Persistent schema
  KnowledgeService ensure/find/search · Evidence · Claim · Confidence
  Event bus envelope · ensure* ingest · mapEntityMutation
  GraphSyncEngine.apply · outbox · ledger infra
  ReasoningEngine shell · ScoreAggregator · StrategyRegistry executor
  Memory snapshot/diff math

CRM domain (treat as adapters; partially inside packages/knowledge today)
  NODE company/person/interaction façades · CRM edge inference
  sync CRM mappers · PostgresRelationalSource · rebuild CRM plan
  writeCompany/Prospect/Touchpoint · operational Brevo taxonomy
  ReasoningContextBuilder · CRM strategy pack · outreach actions
  Memory companyId naming · hiring/DM change labels
  DEFAULT_TENANT_POLICY outreach rules

App producers (correct)
  utils/knowledgeDualWrite · dbClient · Scout hooks
```

---

## 7. References

- [SPEC-015 Market Intelligence Domain](../specs/SPEC-015_Market_Intelligence_Domain.md)
- [SPEC-014 Knowledge Dual-Write](../specs/SPEC-014_Knowledge_Dual_Write.md)
- [SPEC015_FEASIBILITY.md](SPEC015_FEASIBILITY.md)
- [Knowledge_Graph_Architecture.md](Knowledge_Graph_Architecture.md)
- [ADR-004 Knowledge Graph](../adr/ADR-004_Knowledge_Graph.md)
