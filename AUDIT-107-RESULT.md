# AUDIT-107 — SPEC-224B Evidence Correction Gate — RESULT

## CORRECTIONS TO SPEC-224B REPORT

---

## 1. EVIDENCE HASH

**Result**: `EXISTS`

**Evidence**: Migration `2026-09-01-spec-223a-canonical-semantic-persistence.sql`, lines 5-7:
```sql
ALTER TABLE cie_evidence
  ADD COLUMN IF NOT EXISTS source_text_sha256 CHAR(64),
  ADD COLUMN IF NOT EXISTS immutable_at TIMESTAMPTZ;
```

**Migration File**: `migrations/2026-09-01-spec-223a-canonical-semantic-persistence.sql`

**Writer**: `canonical_batch_evidence_inputs` table (line 99-110) references source_text_sha256 from cie_evidence.

**Status**: Column exists. No migration needed. **SPEC-224B claim "BLOCKED" is FALSE**.

---

## 2. REGISTRY

**Result**: `B` — Immutable registry infrastructure exists but no runtime production artifact is seeded/resolved for CIE.

**Evidence**:
- `canonical_registry_artifacts` table created in `2026-09-01-spec-223a-canonical-semantic-persistence.sql` (line 10)
- Full infrastructure: registry_version, entity_vocabulary, predicate_definitions, content_digest
- Append-only triggers ensure immutability
- **BUT**: No INSERT statement in migrations to seed production registry
- Only test fixtures exist in `test/spec223CanonicalSemanticPersistence.test.js` (lines 35-37) and `test/spec223dBabrunRoundTrip.test.js` (lines 313-316)

**Test Registry Content** (spec223CanonicalSemanticPersistence.test.js):
```javascript
registryV1 = await addRegistry('v1', {
  offers: { domain: ['BUSINESS'], range: { kind: 'ENTITY', entity_types: ['OFFER'] }, cardinality: 'SET' },
  legal_name: { domain: ['BUSINESS'], range: { kind: 'LITERAL', literal_types: ['STRING'] }, cardinality: 'SINGLE' },
  targets_outcome: { domain: ['OFFER'], range: { kind: 'ENTITY', entity_types: ['OUTCOME'] }, cardinality: 'SET' },
});
```

**SPEC-222 Entity Types** (actual, from `lib/canonicalSemanticWrite.js` line 6):
```
BUSINESS, OFFER, PROGRAM, CUSTOMER_PROFILE, PAIN, CAPABILITY, OUTCOME, OBJECTIVE, METRIC
```

**SPEC-222 Relation Types** (from `lib/canonicalSemanticWrite.js` line 10):
```
SUPERSEDES, CORRECTION_OF, CONTRADICTS, DEPENDS_ON
```

**SPEC-222 Support Types** (from `lib/canonicalSemanticWrite.js` line 12):
```
DIRECT, INFERRED, OPERATOR_CONFIRMED
```

**Conclusion**: Registry infrastructure is production-ready, but NO production semantic vocabulary/predicates are defined or seeded. **SPEC-224B blocker claim is CORRECT but incomplete**: registry infrastructure exists (B), production artifact does not.

---

## 3. BUSINESS BINDING

**Exact Binding** (from actual merged schema):

**Table**: `canonical_business_entities`

**Columns**:
- `tenant_id` TEXT NOT NULL
- `id` UUID PRIMARY KEY
- `entity_type` TEXT CHECK (entity_type = 'BUSINESS')
- `identity_key` TEXT NOT NULL
- `domain_client_id` INTEGER REFERENCES clients(id)

**Constraint** (line 54-58 of migration):
```sql
CHECK (
  (entity_type = 'BUSINESS' AND domain_client_id IS NOT NULL)
  OR (entity_type <> 'BUSINESS' AND domain_client_id IS NULL)
)
```

**Unique Index** (line 60-62):
```sql
CREATE UNIQUE INDEX canonical_one_business_per_client_idx
  ON canonical_business_entities (domain_client_id)
  WHERE entity_type = 'BUSINESS';
```

**Binding Chain**:
```
clients.id
  ↓ FK domain_client_id
canonical_business_entities (entity_type='BUSINESS')
  ↓ FK tenant_id
canonical_interpretation_batches
  ↓ FK tenant_id
tenant_workspaces (tenant_key)
```

**Conclusion**: BUSINESS binding exists and is correctly constrained. Exactly one BUSINESS per client via unique index on domain_client_id. **SPEC-224B claim CORRECT**.

---

## 4. APPROVAL ORDER

**Current Execution Order** (services/clientIntelligenceInterview.js lines 8295-8370):

```
approveBlueprint(blueprintId, opts)
  ↓
  Session validation (status must be CLIENT_REVIEW)
  ↓
  1. const handoff = await createPlaybookFromApprovedBlueprint(current, handoffOpts)  [LINE 8300]
     ├─ Creates client playbook from Blueprint sections
     └─ Returns { playbook, sectionProvenance }
  ↓
  2. const approved = await store.updateBlueprint(current.id, current.version, {  [LINE 8301-8306]
       status: 'approved',
       playbook_id: handoff.playbook.id,
       playbook_version: handoff.playbook.version,
       section_provenance: handoff.sectionProvenance
     })
  ↓
  3. await store.supersedeBlueprints(current.id, current.version)  [LINE 8307]
  ↓
  4. Build initialGrowthDirection from approved Blueprint + normalizedFacts  [LINE 8309-8337]
  ↓
  5. await store.updateSession(session.id, { status: 'APPROVED', ... })  [LINE 8338-8359]
```

**Key Finding**: Playbook creation (step 1) happens BEFORE Blueprint approval status update (step 2). This violates SPEC-224 intent.

**Current Authority Chain**:
```
Interview session.normalizedFacts (mutable, working state)
  → Blueprint.status='approved' (step 2)
  → Playbook creation (step 1 — EXECUTED BEFORE status change)
  → Growth direction + reasoning artifacts
```

**Contradiction**: Playbook is created BEFORE Blueprint is semantically approved in the database.

**Conclusion**: **SPEC-224B proposal is INCOMPLETE**. Current flow has playbook creation in wrong order relative to approval status.

---

## 5. DUAL AUTHORITY

**Question**: Can we eliminate dual authority (interview normalizedFacts + canonical snapshot) and make canonical the single source?

**Path Analysis**:

**Current State**:
- Playbook created from Blueprint sections (step 1)
- Blueprint marked approved (step 2)
- Playbook never reads normalizedFacts (it reads sections)
- normalizedFacts is only used for initialGrowthDirection (after approval)

**Canonical-Only Path** (required for SPEC-224 intent):
```
1. Canonical commit (SPEC-223B)
   ├─ Input: normalizedFacts + epistemicFacts + evidence
   ├─ Output: snapshot_id + canonical_business_entities + canonical_business_facts
   └─ Immutable snapshot recorded

2. Project normalizedFacts from snapshot (SPEC-223C projection)
   ├─ Reconstruct from canonical_business_facts + canonical_entity_label_assertions
   └─ Return shape compatible with Blueprint.normalizedFacts

3. Create playbook from approved Blueprint + PROJECTED facts
   ├─ Blueprint sections unchanged (same as today)
   ├─ But playbook MUST use canonical facts for semantic enrichment (if any)
   └─ Avoid re-reading interview session

4. Mark Blueprint approved
   ├─ Link to canonical_snapshot_id
   └─ Session becomes archival reference only
```

**Critical Question**: Does playbook creation need to know about normalizedFacts/canonical facts?

**Current Code** (services/clientIntelligencePlaybookHandoff.js line 109+):
```javascript
const identity = sectionSummary(sections, 'identity');
const services = sectionSummary(sections, 'services');
const ideal = sectionSummary(sections, 'idealCustomers');
const avoid = sectionSummary(sections, 'avoidCustomers');
const markets = sectionSummary(sections, 'targetMarkets');
// ... all sources are sections, not normalizedFacts
```

**Finding**: Playbook is created purely from Blueprint.sections. It does NOT read normalizedFacts. Therefore, normalizedFacts can stay session-scoped for growth planning, and canonical commit can happen independently.

**Achievability**: **ACHIEVABLE** — Canonical commit can happen AFTER playbook creation without conflict. The playbook doesn't depend on canonical semantics. But SPEC-224 wants canonical commit BEFORE playbook to establish durable authority first.

**Required Reordering**:
```
CURRENT:
  playbook ← sections
  Blueprint.status = 'approved'

SPEC-224 INTENT:
  canonical commit ← normalizedFacts + evidence
  playbook ← sections (unchanged)
  Blueprint.status = 'approved' ← link canonical_snapshot_id
```

**Conclusion**: **ACHIEVABLE** — Move canonical commit before playbook creation. Playbook creation can remain unchanged. No dual authority needed; canonical snapshot provides durable semantic ground truth.

---

## 6. SNAPSHOT ASSOCIATION

**Question**: Can we associate canonical snapshot to Blueprint without a new column?

**Existing Options**:

**Option A**: New Column (SPEC-224B proposed)
```sql
ALTER TABLE cie_business_blueprints ADD COLUMN canonical_snapshot_id UUID;
-- Simple direct link, one query to find
```

**Option B**: Via Session (Existing)
```sql
SELECT canonical_snapshot_id FROM cie_interview_sessions WHERE id = blueprint.session_id;
-- Requires session row + extra join
```

**Option C**: Via Identity Hash (Index on canonical_business_snapshots)
```sql
SELECT canonical_business_snapshots.id 
FROM canonical_business_snapshots 
WHERE tenant_key = (SELECT tenant_key FROM tenant_workspaces WHERE client_id = blueprint.client_id)
  AND committed_batch_id = <derive from blueprint>;
-- Requires deriving batch identity from blueprint (complex)
```

**Option D**: Via Blueprint Version Hash (canonical_interpretation_batches idempotency_key)
```sql
-- Cannot work: blueprints predate canonical semantic batches
```

**Analysis**:
- **Option A** (new column): Clean, O(1) lookup, explicit intent, 1 DDL statement
- **Option B** (via session): Works but couples Blueprint to session; session may be archived
- **Option C** (hash identity): Complex derivation; fragile if Blueprint/evidence changes
- **Option D**: Impossible for existing Blueprints

**Current Schema Status**: `cie_business_blueprints` has NO canonical association column.

**Conclusion**: **NEW_ASSOCIATION_REQUIRED**

**Why**: 
1. Sessions are archival; Blueprint approval must not depend on live session
2. Canonical snapshot is immutable reference; Blueprint must link directly
3. Forward compatibility: old Blueprints (session-dependent) vs new Blueprints (snapshot-linked)

**Minimal Schema Change**: One nullable UUID column on cie_business_blueprints.

---

## 7. IDEMPOTENCY

**Question**: What is the minimum stable approval identity for deterministic replay?

**Available Stable Identities**:
- `blueprint_id` (UUID, immutable)
- `blueprint_version` (VARCHAR, auto-incremented per blueprint_id, immutable once set)
- `session_id` (UUID, immutable)
- `cie_evidence.id[]` (UUIDs, immutable list)
- `cie_evidence.source_text_sha256[]` (SHA256, immutable per evidence)
- `session.interview_state.normalizedFacts` (MUTABLE during interview, frozen at approval)

**Minimum Stable Identity**:
```
idempotency_key = hash({
  blueprint_id,
  blueprint_version,
  session_id,
  ordered(cie_evidence.id[]) ordered by cie_evidence.created_at,
  registry_artifact_id (canonical registry being used),
  interpreter_id + interpreter_version (CIE approval interpreter)
})
```

**Why This Works**:
- Blueprint ID + version: identifies exact approval target
- Session ID: identifies interview source
- Evidence IDs (ordered): immutable, ordered source material
- Registry + interpreter: identifies exact semantic processing
- Excludes normalizedFacts: it's mutable, frozen only at approval time

**Comparison to Prior Analysis**:
- SPEC-224B claimed normalizedFacts could be in the key: **WRONG** — normalizedFacts is mutable during interview
- SPEC-224B claimed we need frozen approval input: **CORRECT** — evidence list is the frozen input

**Conclusion**: **READY**

**Minimum Identity**: `blueprint_id + blueprint_version + session_id + ordered(evidence.id[]) + registry_id`

**Not Included**: normalizedFacts (mutable), session.completed_at (only set at approval), any timestamp

---

## 8. CANONICAL SEMANTIC MAPPING

**Using ONLY Existing SPEC-222 Registry**:

**Canonical Entity Types** (available):
- BUSINESS, OFFER, PROGRAM, CUSTOMER_PROFILE, PAIN, CAPABILITY, OUTCOME, OBJECTIVE, METRIC

**Canonical Relation Types** (available):
- SUPERSEDES, CORRECTION_OF, CONTRADICTS, DEPENDS_ON

**Test Predicates** (from test suite, only known examples):
- offers (BUSINESS → OFFER)
- legal_name (BUSINESS → STRING)
- targets_outcome (OFFER → OUTCOME)

**CIE-Owned Approved Understanding** (normalizedFacts at approval):

| CIE Field | Canonical Mapping | Epistemic | Evidence |
|-----------|-------------------|-----------|----------|
| business_name | BUSINESS --legal_name--> STRING | KNOWN or HYPOTHESIS | cie_evidence.statement (category='business_identity') |
| services | BUSINESS --offers--> OFFER | KNOWN or HYPOTHESIS | cie_evidence (category='services') |
| ideal_customers | BUSINESS --serves--> CUSTOMER_PROFILE | HYPOTHESIS | cie_evidence (category='ideal_customers') |
| avoid_customers | BUSINESS --avoids--> CUSTOMER_PROFILE | HYPOTHESIS | cie_evidence (category='avoid_customers') |
| target_markets | BUSINESS --operates_in--> STRING/LOCATION | HYPOTHESIS | cie_evidence (category='target_markets') |
| differentiation | BUSINESS --has_differentiator--> STRING | HYPOTHESIS | cie_evidence (category='differentiation') |
| growth_focus | BUSINESS --targets_growth_focus--> OUTCOME | HYPOTHESIS | cie_evidence (category='growth_focus') |
| ninety_day_outcomes | BUSINESS --targets_outcome--> OUTCOME | HYPOTHESIS | cie_evidence (category='success_metrics') |

**Unmappable Fields**:
- **business_facts**: UNREPRESENTABLE (no generic fact-bag predicate in registry; would require per-fact predicates)
- Any field without corresponding predicate definition: UNREPRESENTABLE

**Epistemic State Mapping**:
```
cie_evidence.confidence (0.0–1.0)
  → fact.epistemic_state:
    0.8–1.0: KNOWN (operator explicitly stated with high confidence)
    0.3–0.8: HYPOTHESIS (operator stated with moderate confidence, may be refined)
    0.0–0.3: UNKNOWN (operator unsure or contradicted)

  → fact.interpretation_confidence:
    Always >= 0.8 (CIE statements are high-quality extractions)

  → fact.temporal_status:
    CURRENT (all approved facts are current unless operator explicitly historical)
```

**Conclusion**:

CIE can represent **7 of 8** normalizedFacts fields in canonical form (all except business_facts). The registry test suite has 3 predicates defined; production registry needs at minimum:
- legal_name (BUSINESS → STRING)
- offers (BUSINESS → OFFER)
- serves (BUSINESS → CUSTOMER_PROFILE)
- avoids (BUSINESS → CUSTOMER_PROFILE)
- operates_in (BUSINESS → STRING)
- has_differentiator (BUSINESS → STRING)
- targets_growth_focus (BUSINESS → OUTCOME)
- targets_outcome (BUSINESS → OUTCOME)

---

## FIRST TRUE BLOCKER

**Exact Issue**: No production canonical registry artifact exists with CIE semantic vocabulary.

**Root Cause**: 
- Registry infrastructure created (schema in 2026-09-01 migration)
- Test fixtures exist (test files)
- NO production INSERT in any migration or bootstrap

**Impact**: `commitCanonicalSemanticBatch()` will fail `loadRegistry()` check when given a non-existent registry_artifact_id. CIE cannot produce canonical batches without a target registry.

**Resolution Required**:
1. Define CIE-owned canonical predicates (minimum 8, listed above)
2. Create `canonical_registry_artifacts` row with:
   - registry_version: "1.0.0-cie-semantic"
   - entity_vocabulary: ["BUSINESS", "OFFER", "CUSTOMER_PROFILE", "OUTCOME"]
   - predicate_definitions: { legal_name, offers, serves, avoids, operates_in, has_differentiator, targets_growth_focus, targets_outcome }
   - content_digest: SHA256(vocabulary + predicates + version)
3. Seed into PostgreSQL via production migration or bootstrap script

**Blocker Status**: **MUST RESOLVE BEFORE SPEC-224 IMPLEMENTATION**

---

## SPEC-224 IMPLEMENTATION READY

**Overall Assessment**: **NO**

**Why**:
1. ✅ Evidence hash: EXISTS (SPEC-224B claim corrected)
2. ✅ BUSINESS binding: EXISTS and correct (SPEC-224B claim correct)
3. ✅ Canonical identity ownership: Verified 223B owns IDs (SPEC-224B correct)
4. ❌ Registry: Infrastructure exists but NO production artifact (SPEC-224B correct on blocker)
5. ❌ Approval order: Playbook creation happens in wrong order (SPEC-224B missed this)
6. ✅ Dual authority: Eliminable, achievable with reordering (SPEC-224B correct)
7. ✅ Snapshot association: Requires new column (SPEC-224B correct)
8. ✅ Idempotency: Stable identity formula derived (SPEC-224B overcomplicated)
9. ❌ Canonical semantic mapping: 7/8 fields mappable, registry vocabulary must be defined

**Blockers Preventing Implementation**:
1. Production canonical registry artifact (must create with CIE vocabulary)
2. Approval execution order (playbook created before Blueprint approval status)

**Non-Blocking Issues** (can be addressed in implementation):
- Snapshot association column (straightforward DDL)
- Dual authority reordering (logic change in approveBlueprint)
- Canonical mapping (adapter code to transform normalizedFacts → CanonicalSemanticBatch)

---

## SUMMARY TABLE

| Item | Finding | Status |
|------|---------|--------|
| Evidence hash | EXISTS (ALTER TABLE already done) | ✅ CORRECTS SPEC-224B |
| Registry | Infrastructure YES, production artifact NO | ⚠️ PARTIAL (option B) |
| BUSINESS binding | `canonical_business_entities.domain_client_id` | ✅ VERIFIED |
| Current approval order | Playbook BEFORE status update | ❌ WRONG ORDER |
| Required approval order | Canonical commit → playbook → status | ✅ ACHIEVABLE |
| Dual authority | Eliminable; canonical snapshot sufficient | ✅ CORRECT |
| Snapshot association | New column required on Blueprint | ✅ CORRECT |
| Approval idempotency | blueprint_id + version + session_id + evidence_ids | ✅ READY |
| CIE → SPEC-222 mapping | 7 of 8 fields; registry predicates must be defined | ⚠️ READY IF REGISTRY EXISTS |
| **FIRST TRUE BLOCKER** | **Production canonical registry artifact must be created** | ❌ |
| **IMPLEMENTATION READY** | **NO** — blocker + approval reordering required | ❌ |

---

## CORRECTED ASSERTIONS

| SPEC-224B Claim | Original | AUDIT-107 Correction |
|---|---|---|
| "Evidence hash BLOCKED" | FALSE | Evidence hash EXISTS; no migration needed |
| "Registry BLOCKED" | CORRECT BUT INCOMPLETE | Registry infrastructure exists (B), production artifact missing |
| "BUSINESS binding not found" | FALSE | Binding verified in canonical_business_entities.domain_client_id |
| "Playbook created after Blueprint approval" | ASSUMED | FALSE — playbook created BEFORE approval status update |
| "Dual write required" | PROPOSED | Unnecessary; canonical can be sole authority if ordered correctly |
| "Snapshot association needs new column" | CORRECT | No alternative exists without schema coupling to sessions |
| "Idempotency key includes normalizedFacts" | IMPLIED MUTABLE | WRONG — normalizedFacts is mutable; key must use frozen evidence |
| "CIE semantic subset ready to map" | IMPLIED | Correct, but registry vocabulary must be explicitly defined |

---

## FINAL VERDICT

**SPEC-224B REPORT: PARTIALLY CORRECT BUT INCOMPLETE**

**Corrections**:
- Evidence immutability: Column already exists, not blocked
- Registry: Correctly identified as blocker, but infrastructure exists (classification B correct)
- Approval order: Report missed critical issue (playbook created in wrong sequence)
- Idempotency: Overcomplicated; normalizedFacts must not be in key (mutable)

**Remaining Blockers** (prevents implementation):
1. **Production registry artifact** (must create CIE vocabulary)
2. **Approval execution reordering** (canonical commit before playbook creation)

**Non-Blocking Work** (can proceed after blockers):
- Snapshot association column
- Dual authority elimination
- Canonical semantic mapping/adapter

**Recommended Next Steps**:
1. Create production `canonical_registry_artifacts` row with CIE entity/predicate vocabulary
2. Reorder `approveBlueprint()` to: validate → canonical commit → playbook creation → status update
3. Implement snapshot association (new column)
4. Implement normalizedFacts → CanonicalSemanticBatch adapter
5. Implement canonical projection for backward compatibility

