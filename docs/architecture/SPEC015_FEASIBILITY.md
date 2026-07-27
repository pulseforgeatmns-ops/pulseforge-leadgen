# SPEC-015 Feasibility — Market Domain via Adapters

| Field | Value |
|---|---|
| **Status** | Architectural validation only |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-015](../specs/SPEC-015_Market_Intelligence_Domain.md) (Draft) |
| **Companion** | [EVIDENCE_CORE_DOMAIN_AUDIT.md](EVIDENCE_CORE_DOMAIN_AUDIT.md) |
| **Scope** | Feasibility answers only — no implementation |

## Question

Can a Market Intelligence Domain be introduced using **domain adapters only**, leaving the Evidence Core runtimes unchanged?

SPEC-015 asserts: adapters translate; Knowledge / Memory / Reasoning remain unchanged; no trades.

---

## Verdict

**Conditional — not “adapters only” as literally stated.**

| Path | Feasible? | Meaning |
|---|---|---|
| **A. Strict zero-change to all runtimes + default CRM wiring** | **No** | Closed ontology, `companyId` evaluate path, and CRM strategy/action packs cannot answer market regime / analog / hypothesis questions correctly |
| **B. Adapters + Claim-as-Hypothesis + injectable market context/strategy packs** (core math & apply path unchanged) | **Yes** | Matches SPEC-015 *intent*; requires treating strategy/context packs and ontology extension as domain layer, not “engine forks” |
| **C. Force-fit markets as fake companies/people/interactions** | **Technically yes, productively no** | Violates SPEC entity model and yields outreach-shaped recommendations |

**Recommended reading of SPEC-015 for implementation planning:** Path **B**. Update the Draft’s “unchanged Evidence Core” language to mean: **unchanged Confidence math, Claim/Evidence engines, GraphSync apply/outbox, Reasoning orchestration shell, Memory diff math** — not “unchanged default CRM evaluate path.”

---

## 1. Can these remain unchanged?

### 1.1 Knowledge Graph runtime

| Aspect | Unchanged? | Why |
|---|---|---|
| Repository + `ensureNode`/`ensureEdge`/`findNode` | **Yes** | Storage-agnostic |
| Evidence / Claim engines | **Yes** | Subject/evidence IDs only |
| Closed `NODE_TYPES` / CRM bus events / CRM query façades | **No** (for first-class market entities) | Only `company` \| `person` \| `interaction` \| `evidence` \| `claim`. Asset / Regime / Hypothesis / Indicator are not representable without additive types **or** Claim/Evidence-only Phase 1 |
| CRM sync mappers / relational rebuild | N/A (adapters) | Must not be used for market; market uses Event/Entity adapters → `mapEntityMutation` / outbox |

**Answer:** **Partial.** Runtime storage and Evidence/Claim **yes**. Ontology and CRM ingest vocabulary **no** if SPEC-015 entity table is taken literally. Phase 1 can defer new node types by mapping hypotheses → **claims** and observations → **evidence** (audit R4).

### 1.2 Memory runtime

| Aspect | Unchanged? | Why |
|---|---|---|
| Snapshot / diff / trend math | **Yes** | Compares score, confidence, claim/evidence id sets |
| Public API (`companyId`, `listByCompany`, hiring/DM change types) | **Partial / No** | Subject key and CRM change labels are company-shaped |

**Answer:** **Partial.** Diff engine unchanged. Useful market memory needs subject generalization (audit R1) and soft-gating of CRM change labels (R10). Alias `companyId` ← asset id is a shim, not zero design debt.

### 1.3 Reasoning runtime

| Aspect | Unchanged? | Why |
|---|---|---|
| `ReasoningEngine` orchestration (build → strategies → aggregate → recommend → explain) | **Yes** | Already accepts injected `contextBuilder` + `registry` |
| Default `ReasoningContextBuilder` | **No** | Requires company / people / interactions |
| `createDefaultStrategyRegistry()` CRM pack | **No** | Outreach / ICP / email heuristics |
| `RecommendationBuilder` CRM actions | **No** | pursue / intro / nurture_sequence |

**Answer:** **No** as a *useful* market reasoner with defaults untouched. **Yes** for the orchestration shell if market supplies context builder + strategy pack + action mapping (audit R2, R3, R8).

### 1.4 Evidence scoring

| Aspect | Unchanged? | Why |
|---|---|---|
| Noisy-OR `calculateConfidenceFromEvidence` | **Yes** | Pure numeric |
| Max `confidenceFromEvidence` in strategies | **Yes** (formula) | Domain-agnostic formula |
| *Which* evidence counts / keywords | **No** under default strategies | Selection is CRM-field and keyword heuristics |

**Answer:** **Partial.** Scoring **math** unchanged. Scoring **meaning** requires market strategies (or adapters that only write evidence while a market pack interprets it).

### 1.5 Hypothesis engine

| Aspect | Unchanged? | Why |
|---|---|---|
| Dedicated Hypothesis engine | **N/A** | Does not exist in code |
| `ClaimEngine` as hypothesis stand-in | **Yes** | `createClaim` / `evaluateClaim` / SUPPORTS |

**Answer:** **Partial.** There is nothing named Hypothesis to leave unchanged. SPEC-015 Hypothesis objects map cleanly to **claims** in Phase 1. First-class `Hypothesis` node type is a later additive Knowledge change — not adapter-only.

### 1.6 Confidence updates

| Aspect | Unchanged? | Why |
|---|---|---|
| `ClaimEngine.evaluateClaim` recompute | **Yes** | Generic |
| Automatic update on every market tick | **No today** | Nothing auto-re-evaluates claims on ingest; Max confidence is recomputed on each `evaluate` |
| Memory confidence deltas | Observational only | Records successive snapshot confidences after `remember` |

**Answer:** **Partial.** Recompute primitives exist and are unchanged. A market “confidence updates automatically” loop must be **wired by the domain** (on ingest or scheduled evaluate) — not assumed present in core.

---

## 2. Feasibility matrix (SPEC-015 success questions)

| Success question | Adapters only + Claim Phase 1 + market packs? | Strict zero engine/wiring change? |
|---|---|---|
| What regime are we observing? | Yes — claim/regime evidence + market strategy | No — no regime concept in CRM strategies |
| Nearest historical matches? | Yes — Memory analogs if subject keyed + snapshots remembered | Partial — Memory works if subject aliased; CRM change labels irrelevant |
| Hypotheses gaining/losing confidence? | Yes — claims + evaluateClaim + remember trends | No — no Hypothesis engine; CRM evaluate wrong subject |
| Supporting evidence? | Yes — Evidence nodes + explain() | Partial — explain works; default strategies cite wrong signals |
| How unusual is today? | Yes — market unusualness strategy over analogs | No — not in CRM pack |
| What changed in the last hour? | Yes — Memory `whatChanged` / timeline if snapshots frequent enough | Partial — API is company-named; needs regular `remember` |
| Zero trades / no execution UI | Yes — Research Workspace must omit CRM outreach actions | Yes if UI is separate; default RecommendationBuilder still emits outreach types if reused blindly |

---

## 3. What must stay frozen vs what is domain

### Freeze (Evidence Core — do not fork for markets)

- `GraphRepository` / persistent graph apply semantics
- `EvidenceEngine` / `ClaimEngine` / noisy-OR confidence
- `GraphSyncEngine.apply` + outbox/ledger infrastructure
- `ReasoningEngine` orchestration + `ScoreAggregator` + `StrategyRegistry` executor
- Memory snapshot/diff algorithms
- Explanation chain shape (evidence-backed why)

### Domain layer (adapters + packs — expected for SPEC-015)

- Market Event / Entity / Relationship / Evidence adapters (see audit §4)
- Provider normalizers (Coinbase, Kalshi, …)
- Market context builder + strategy pack + research action taxonomy
- Optional additive node/edge types when Claim-as-Hypothesis is insufficient
- Research Workspace composition (Command Deck pattern, no execution)
- Confidence update **scheduler** / post-ingest hooks calling existing `evaluateClaim` / `evaluate`

### Explicitly out of scope (unchanged from SPEC-015)

Live trading, brokers, sizing, portfolio, risk engine, automated execution, strategy optimization.

---

## 4. Adapter-only boundary (honest contract)

SPEC-015 is feasible **if** “adapters only” means:

```text
Market providers
  → EventAdapter / EntityAdapter / RelationshipAdapter / EvidenceAdapter
  → KnowledgeEvent / SyncEnvelope (SPEC-014 contract)
  → existing outbox + GraphSyncEngine.apply
  → Evidence + Claim (hypothesis stand-in)
  → ReasoningEngine(evaluate) with injected market contextBuilder + registry
  → Memory.remember(subject)
  → Research Workspace (read-only)
```

and does **not** mean:

```text
Market providers → CRM mappers → default Max CRM evaluate → Command Deck outreach cards
```

---

## 5. Prerequisites before implementation (no code in this task)

Drawn from the domain audit; readiness only:

1. Treat CRM sync/dual-write helpers as **CRM adapters** (documentation boundary — R6).
2. Agree Phase 1 Hypothesis = Claim metadata convention (R4).
3. Plan subject alias + pluggable context/strategy packs as the first SPEC-015 implementation slice (R1–R3) — still no market providers required for those shims.
4. Replay: require market adapters to emit complete outbox envelopes with stable keys; schedule a future replay-hardening spec for missing sequence/revision metadata (audit §5).
5. Amend SPEC-015 Draft wording: “unchanged Evidence Core” → list frozen subsystems explicitly (this doc §3).

---

## 6. Direct answers (checklist)

| Subsystem | Remain unchanged? |
|---|---|
| Knowledge Graph **runtime** (store + ensure + evidence/claim) | **Yes** |
| Knowledge Graph **ontology / CRM ingest vocabulary** | **No** (unless Claim/Evidence-only Phase 1) |
| Memory **runtime** (diff math) | **Yes** |
| Memory **API naming / CRM change types** | **Partial** |
| Reasoning **runtime** (orchestration shell) | **Yes** |
| Reasoning **default CRM path** | **No** |
| Evidence **scoring math** | **Yes** |
| Evidence **selection heuristics** | **No** (domain strategies) |
| Hypothesis engine | **N/A** — use ClaimEngine (**Yes** as stand-in) |
| Confidence **update primitives** | **Yes** |
| Confidence **automatic market loop** | **No** — domain must wire |

---

## 7. Conclusion

The Evidence Core **can** reason about financial markets without becoming a trading system and without forking Confidence, Claim/Evidence, apply/outbox, or the Reasoning/Memory **math**.

It **cannot** do so by dropping market ticks into today’s CRM-default Max path unchanged.

SPEC-015 should proceed as: **domain adapters + domain packs on an unchanged core shell**, with Claim-as-Hypothesis in Phase 1, and additive graph types only when necessary — after SPEC-014 dual-write is solid for CRM.

No runtime behavior was changed by this validation.
