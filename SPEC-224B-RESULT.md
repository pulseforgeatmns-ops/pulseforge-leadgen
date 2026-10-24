# SPEC-224B — Approval Commit Integration Boundary — RESULT

## CANONICAL COMMIT BOUNDARY
**APPROVAL_COMMIT**

The canonical semantic snapshot must be committed during `approveBlueprint()`, after normalizedFacts normalization but before Blueprint status transition to 'approved'.

---

## EXACT APPROVAL FUNCTION
**File**: services/clientIntelligenceInterview.js
**Function**: `approveBlueprint(blueprintId, opts = {})`
**Lines**: 8239–8357

```javascript
async function approveBlueprint(blueprintId, opts = {}) {
  // ... validation ...
  const handoff = await createPlaybookFromApprovedBlueprint(current, handoffOpts);
  // ← CANONICAL COMMIT INSERTION POINT (line 8300)
  const approved = await store.updateBlueprint(current.id, current.version, {
    status: 'approved',
    canonical_snapshot_id: commitResult.snapshot_id  // ← NEW: Link to snapshot
  });
  // ... session update ...
}
```

---

## CANONICAL COMMIT INSERTION POINT
**After line 8298**: `const handoff = await createPlaybookFromApprovedBlueprint(current, handoffOpts);`

**Implementation Pattern**:
```javascript
// NEW: Build and commit canonical batch
const batch = await buildCanonicalBatchFromApprovedBlueprint(
  current,  // approved Blueprint object
  session,  // interview session with normalizedFacts
  {
    registry_artifact_id: opts.registryArtifactId,
    registry_version: opts.registryVersion,
    registry_content_digest: opts.registryContentDigest,
    interpreter_id: 'cie-approval-v1',
    interpreter_version: '1.0.0'
  }
);

const commitResult = await commitCanonicalSemanticBatch(pool, batch);
// Returns: { snapshot_id, canonical_business_entity_id, ... }
// If throws: Blueprint remains draft, no side effects
```

---

## INTERVIEW NORMALIZEDFACTS ROLE
**WORKING_STATE**

- Created during interview discovery loop in session.interview_state
- Mutable as operator refines answers
- Transformed to epistemicFacts at Blueprint generation
- **At approval**: Converted to CanonicalSemanticBatch semantic_entities/semantic_facts
- **After approval**: Canonical snapshot becomes durable authority; session data becomes archival

---

## CURRENT DURABLE AUTHORITY
**Path**: `cie_business_blueprints` table

Approved Blueprint stores:
- `sections`: raw discovery answers
- `section_provenance`: epistemicFacts (fallback projection source)
- `status = 'approved'`: immutable status flag
- `playbook_id`: linked acquisition playbook

**After SPEC-224B Integration**:
- `canonical_snapshot_id`: FK to canonical_business_snapshots (new column)
- Authority shifts: snapshot becomes semantic truth, Blueprint becomes reference record

---

## EXISTING BUSINESS BINDING
**Type**: DIRECT MAPPING (no invention needed)

**Binding Chain**:
```
clients (id=client_id)
  ↓
cie_business_blueprints (client_id)
  ↓
cie_evidence (client_id)
  ↓
canonical_business_entities (domain_client_id = client_id)
  ↓
tenant_workspaces (client_id) + canonical_business_snapshots (tenant_id)
```

**Authority Check** (lib/canonicalSemanticWrite.js:525):
```sql
SELECT 1 FROM tenant_workspaces tw 
  JOIN clients c ON c.id=tw.client_id
  WHERE tw.tenant_key=$1 AND tw.client_id=$2
```

**Observation**: Binding is stable, no new identity system required. CIE provides `client_id`, canonical system links to `domain_client_id` on BUSINESS entity.

---

## CANONICAL IDENTITY OWNER
**223B**

Owns:
- Entity UUID generation (canonical_business_entities.id)
- Proposition/assertion key derivation
- Fact ID generation (canonical_business_facts.id)
- Evidence link ID generation (canonical_fact_evidence_refs.id)
- Deduplication via idempotency_key
- Snapshot creation and lineage

**CIE Must NOT Invoke**:
- Do not generate canonical entity UUIDs
- Do not assume entity ID format
- Do not manage deduplication

**CIE Supplies**:
- semantic_entities array (entity_type, domain_client_id, name hints)
- semantic_facts array (subject/predicate/object, epistemic_state, confidence)
- ordered_evidence_input_ids array (UUIDs pointing to cie_evidence rows)
- Registry artifact reference (registry_artifact_id, registry_version, registry_content_digest)

---

## REGISTRY
**Status**: **BLOCKED**

**Required Registry Artifact**:
- **Source**: Must be created as canonical_registry_artifacts row
- **Content**: 
  - entity_vocabulary: ["BUSINESS", "OFFER", "CUSTOMER_PROFILE", "PAIN", "OBJECTIVE", "OUTCOME", "METRIC", "CAPABILITY"]
  - predicate_definitions: { name, serves, avoids, operates_in, has_capability, targets_outcome, differentiator, growth_focus, ... }
  - registry_version: "1.0.0" (CIE semantic model version)
- **Digest**: SHA256 of vocabulary + predicates + version

**Current Status**:
- Only test fixtures exist in spec223CanonicalSemanticPersistence.test.js
- No production registry published
- No canonical-registry-artifacts row in PostgreSQL

**Blocker**: Production registry must exist before approveBlueprint() can call commitCanonicalSemanticBatch(). Cannot proceed without entity/predicate vocabulary.

**Resolution Path**: 
1. Create canonical_registry_artifacts row with CIE vocabulary
2. Publish registry version (immutable artifact)
3. Reference registry_artifact_id in approval flow

---

## EVIDENCE MAPPING
**Status**: **BLOCKED**

**CIE Evidence Table** (cie_evidence):
```
id (UUID), client_id, session_id, source, source_turn_id,
category, statement, confidence, type, created_at
```

**Required for Canonical Batch** (canonical_batch_evidence_inputs):
```
evidence_id (UUID reference to cie_evidence.id),
source_text_sha256 (SHA256 of statement, for immutability verification)
```

**Current Gap**: cie_evidence table lacks source_text_sha256 column.

**Canonical Validation** (lib/canonicalSemanticWrite.js:121):
```javascript
if (!row.source_text_sha256 || row.source_text_sha256 !== row.computed_digest) 
  fail('EVIDENCE_DIGEST_INVALID', ...);
```

**Evidence Mapping Status**:

| CIE Field | Canonical Field | Status |
|---|---|---|
| cie_evidence.id | ordered_evidence_input_ids[n] | DIRECT_MAPPING |
| cie_evidence.statement | source_text (for SHA256) | DIRECT_MAPPING |
| cie_evidence.category | fact predicate/domain hint | DERIVABLE |
| cie_evidence.confidence | fact epistemic_state + confidence_score | DERIVABLE |
| cie_evidence.type | evidence support_type (DIRECT/INFERRED/OPERATOR_CONFIRMED) | DERIVABLE |
| source_text_sha256 | **MISSING** | **BLOCKER** |

**Resolution Options**:
1. **ALTER TABLE**: Add `source_text_sha256 VARCHAR(64)` to cie_evidence, backfill with SHA256(statement)
2. **Compute On-Demand**: At approval time, calculate SHA256 for each evidence row before passing to commitCanonicalSemanticBatch()
3. **Immutable Evidence Snapshot**: Create immutable copy of evidence rows with hash pre-computed

**Recommendation**: Option 1 (ALTER TABLE) — ensures evidence immutability check succeeds and allows canonical registry to verify evidence integrity independently.

---

## CIE CANONICAL SEMANTIC SUBSET
**Owned Entity/Predicate Categories**:

**Entities**:
- BUSINESS (1 per client, domain_client_id = client_id)
- CUSTOMER_PROFILE (N per ideal/avoid customer segment)
- OFFER (implicit, derivable from services list)

**Facts** (Predicates):
- `has_name` (BUSINESS --predicate--> string) — from normalizedFacts.business_name
- `offers_service` (BUSINESS --predicate--> OFFER) — from normalizedFacts.services
- `serves_customer_profile` (BUSINESS --predicate--> CUSTOMER_PROFILE) — from normalizedFacts.ideal_customers
- `avoids_customer_profile` (BUSINESS --predicate--> CUSTOMER_PROFILE) — from normalizedFacts.avoid_customers
- `operates_in_market` (BUSINESS --predicate--> string) — from normalizedFacts.target_markets
- `has_differentiator` (BUSINESS --predicate--> string) — from normalizedFacts.differentiation
- `targets_growth_focus` (BUSINESS --predicate--> OUTCOME) — from normalizedFacts.growth_focus
- `targets_outcome` (BUSINESS --predicate--> OUTCOME) — from normalizedFacts.ninety_day_outcomes

**NOT Owned** (no discovery question exists):
- PAIN entities (no pain interview yet)
- CAPABILITY entities (no capability discovery; only string differentiation)
- METRIC entities (success metrics tracked separately, not linked to outcomes yet)
- PROGRAM entities (no mission/program semantic depth)
- Relational inference (e.g., PAIN → OUTCOME mapping)

**Epistemic State Mapping**:
- `normalizedFacts.confidence` (0.0–1.0) → `fact.epistemic_state = 'KNOWN' | 'HYPOTHESIS' | 'UNKNOWN'`
  - 0.8–1.0 → KNOWN
  - 0.3–0.8 → HYPOTHESIS
  - 0.0–0.3 → UNKNOWN
- Fallback: If no confidence, use `KNOWN` (operator-stated facts are high confidence)

---

## COMPATIBILITY STRATEGY
**Preferred Path**: Dual Write + Phased Migration

**Phase 1: Runtime Attachment** (Current state, no changes needed):
- getApprovedClientBlueprint() loads session, retrieves normalizedFacts
- Downstream consumers (Max, operatorScorecard) work as-is

**Phase 2: Canonical Commit + Snapshot Linkage** (SPEC-224B):
- approveBlueprint() calls commitCanonicalSemanticBatch()
- Blueprint receives new column: canonical_snapshot_id (FK to canonical_business_snapshots)
- Session also updated with canonical_snapshot_id for observability

**Phase 3: Canonical-Preferred Retrieval** (Future):
- getApprovedClientBlueprint() implementation change:
  ```javascript
  // Try canonical snapshot first
  if (blueprint.canonical_snapshot_id) {
    const normalizedFacts = await projectNormalizedFactsFromSnapshot(
      pool, blueprint.canonical_snapshot_id
    );
    if (normalizedFacts) {
      blueprint.normalizedFacts = normalizedFacts;
      return blueprint;  // Canonical authority used
    }
  }
  
  // Fallback to session (for backward compatibility)
  if (!blueprint.normalizedFacts && blueprint.session_id) {
    const session = await store.getSession(blueprint.session_id);
    if (session?.interview_state?.normalizedFacts) {
      blueprint.normalizedFacts = session.interview_state.normalizedFacts;
    }
  }
  return blueprint;
  ```

**Phase 4: Session Archival** (Later, optional):
- Once all downstream consumers proven compatible with canonical projection
- Prune/archive old interview sessions, keep canonical snapshots as immutable authority

**Backward Compatibility**:
- Old Blueprints (canonical_snapshot_id = NULL) continue working via session fallback
- No requirement to backfill snapshots for historical Blueprints
- New approvals always create snapshots

**Impact on Downstream** (Max, operatorScorecard, playbook handoff):
- ZERO changes required in Phase 2 (dual write phase)
- Blueprint.normalizedFacts still available via session fallback
- Phase 3 transparently switches authority without code changes in consumers

**Status**: READY — Backward-compatible phased transition with zero immediate impact on downstream.

---

## APPROVAL ATOMICITY
**Strategy**: Two-Phase Orchestration (Realistic)

**Phase 1: Canonical Commit** (owned by SPEC-223B):
```
BEGIN TRANSACTION (inside commitCanonicalSemanticBatch)
  ├─ pg_advisory_xact_lock(tenant_id)  [prevent concurrent snapshot writes]
  ├─ validate registry, evidence, entities
  ├─ insert canonical_interpretation_batches
  ├─ insert/reuse canonical_business_entities
  ├─ insert/reuse canonical_business_facts
  ├─ insert canonical_fact_evidence_refs
  ├─ insert canonical_fact_relations
  ├─ create canonical_business_snapshots
  └─ COMMIT [returns snapshot_id]
```

**Phase 2: Blueprint Approval** (owned by CIE approveBlueprint):
```
BEGIN TRANSACTION (inside approveBlueprint)
  ├─ UPDATE cie_business_blueprints SET 
  │   status = 'approved',
  │   canonical_snapshot_id = $snapshot_id,
  │   updated_at = NOW()
  │ WHERE id = $blueprint_id AND status IN ('draft', 'in_review')
  │ RETURNING *
  │
  ├─ [Safety check: SELECT canonical_business_snapshots WHERE id=$snapshot_id]
  │  (Verify snapshot created by Phase 1, defend against race)
  │
  ├─ UPDATE cie_interview_sessions SET 
  │   status = 'APPROVED',
  │   completed_at = NOW(),
  │   interview_state = interview_state || jsonb_build_object(
  │     'canonical_snapshot_id', $snapshot_id,
  │     'approvedAt', NOW()
  │   )
  │ WHERE id = $session_id
  │
  └─ COMMIT
```

**Failure Modes**:

1. **Phase 1 Fails** (canonical commit throws):
   - Transaction rolled back
   - No snapshot created
   - Blueprint remains draft
   - approveBlueprint() never reached Phase 2
   - Retry-safe: idempotency_key prevents duplicate snapshots

2. **Phase 2 Fails After Phase 1 Success** (Blueprint update fails):
   - Canonical snapshot exists (already committed)
   - Blueprint update rolled back
   - System detects orphaned snapshot on retry
   - Action: Log alert "canonical snapshot [id] exists but no approved Blueprint links it"
   - Retry: approveBlueprint() re-runs Phase 2, updates Blueprint to link snapshot

**Orphan Recovery**:
```sql
SELECT id, committed_batch_id FROM canonical_business_snapshots 
  WHERE tenant_id=$1 
    AND id NOT IN (
      SELECT canonical_snapshot_id FROM cie_business_blueprints 
      WHERE client_id IN (SELECT id FROM clients WHERE ...)
    )
  AND created_at > NOW() - INTERVAL '1 hour';
```

**Required Invariant**:
- Blueprint MUST NOT transition to 'approved' status unless canonical snapshot successfully committed
- This is guaranteed because commitCanonicalSemanticBatch is called before store.updateBlueprint()
- If Phase 1 throws, Phase 2 never executes ✓

---

## APPROVAL IDEMPOTENCY
**Strategy**: Stable Idempotency Key Derivation

**Idempotency Key Derivation** (must be stable across retries):
```javascript
const idempotency_key = hash({
  blueprint_id: blueprint.id,
  blueprint_version: blueprint.version,
  session_id: session.id,
  evidence_ordered_digests: ordered_evidence_ids
    .map(id => evidence_digests.get(id))
    .join(':'),
  normalized_facts_digest: hash(session.interview_state.normalizedFacts),
  registry_artifact_id: opts.registry_artifact_id,
  registry_content_digest: opts.registry_content_digest,
  interpreter_id: 'cie-approval-v1',
  interpreter_version: '1.0.0'
});
```

**Retry Guarantees**:

| Scenario | Result |
|----------|--------|
| Retry with same input | idempotency_key matches → canonical batch replayed → same snapshot_id returned |
| Blueprint status check | If 'approved' already → early return alreadyApprovedPayload() |
| Network failure mid-approval | Phase 1 safe: idempotency key prevents duplicate snapshots; Phase 2 safe: Blueprint idempotent via version check |

**Existing Mechanisms Leveraged**:
- Blueprint.version (unique per blueprint_id)
- Session.id (session-level identity)
- Evidence ordering (immutable list of cie_evidence.id[] in discovery order)
- Blueprint status 'approved' (idempotent state marker)

**No Invented Identity**: All keys derived from existing stable identities (blueprint_id, session_id, evidence IDs, registry digest).

---

## CANONICAL BATCH READINESS
**Overall Status**: **NOT_READY**

**Blockers** (in priority order):

### 1. Registry Artifact (BLOCKING)
- **Status**: Does not exist in production
- **Required**: canonical_registry_artifacts row with CIE entity vocabulary and predicate definitions
- **Impact**: commitCanonicalSemanticBatch will fail loadRegistry() check if registry_artifact_id does not exist
- **Resolution**: Publish canonical registry artifact before approveBlueprint() can commit batches

### 2. Evidence Immutability (BLOCKING)
- **Status**: source_text_sha256 column does not exist in cie_evidence
- **Required**: cie_evidence.source_text_sha256 (SHA256 of statement)
- **Impact**: commitCanonicalSemanticBatch will fail EVIDENCE_DIGEST_INVALID check
- **Resolution**: ALTER TABLE cie_evidence ADD COLUMN source_text_sha256 VARCHAR(64); backfill via SHA256(statement)

### 3. Adapter Implementation (NOT_READY)
- **Status**: normalizeEpistemicFacts() → CanonicalSemanticBatch function does not exist
- **Required**: Function to transform normalizedFacts + epistemicFacts + evidence into CanonicalSemanticBatch
- **Impact**: buildCanonicalBatchFromApprovedBlueprint() called in approveBlueprint() will fail
- **Resolution**: Implement normalizeEpistemicFacts() function (estimated 200–300 lines)

### 4. Schema Migration (NOT_READY)
- **Status**: cie_business_blueprints lacks canonical_snapshot_id column
- **Required**: ALTER TABLE cie_business_blueprints ADD COLUMN canonical_snapshot_id UUID NULL
- **Impact**: Blueprint cannot store link to canonical snapshot
- **Resolution**: Migration to add column (quick, non-breaking)

---

## FIRST REMAINING BLOCKER
**EXACT ISSUE**: Production canonical registry artifact does not exist.

- **Root Cause**: No canonical_registry_artifacts row with entity_vocabulary and predicate_definitions for CIE semantic model
- **Discovery**: Searched lib/canonicalSemanticWrite.js (expects registry at line 100) and test/spec223CanonicalSemanticPersistence.test.js (only test fixtures exist)
- **Impact**: approveBlueprint() → commitCanonicalSemanticBatch() → loadRegistry(batch.registry_artifact_id) will throw REGISTRY_NOT_FOUND
- **Prevents**: Any integration work on SPEC-224B until blocker resolved

**Action Required Before Implementation**:
1. Define CIE entity vocabulary: BUSINESS, CUSTOMER_PROFILE, OFFER (at minimum)
2. Define CIE predicate definitions: has_name, serves, avoids, operates_in, has_differentiator, targets_growth_focus, targets_outcome
3. Create and publish canonical_registry_artifacts row in production database
4. Document registry_version (recommend "1.0.0-cie-semantic-model")
5. Compute and pin registry_content_digest (SHA256)

---

## MINIMUM SPEC-224 IMPLEMENTATION
**Files/Functions Only** (do not implement yet):

1. **lib/canonicalSemanticWrite.js** (already exists)
   - No changes required; ready to use

2. **services/clientIntelligenceInterview.js**
   - NEW: `buildCanonicalBatchFromApprovedBlueprint(blueprint, session, opts)` (lines ~8250)
   - NEW: Wire call to `commitCanonicalSemanticBatch()` in `approveBlueprint()` (line ~8300)
   - UPDATE: `approveBlueprint()` to store canonical_snapshot_id

3. **services/clientIntelligencePlaybookHandoff.js**
   - No changes (already exists, used by approveBlueprint)

4. **packages/max/workspace/ClientIntelligenceContext.js**
   - NEW: `projectNormalizedFactsFromSnapshot(pool, snapshotId)` (fallback projection for Phase 3)
   - UPDATE: `loadApprovedClientIntelligence()` to prefer snapshot if canonical_snapshot_id present (Phase 3 only; not blocking)

5. **Database Migration** (1 file)
   - `migrations/XXXX_add_canonical_snapshot_id_to_blueprints.sql`
   - ALTER TABLE cie_business_blueprints ADD COLUMN canonical_snapshot_id UUID NULL
   - ALTER TABLE cie_interview_sessions ADD COLUMN canonical_snapshot_id UUID NULL (optional, for observability)

6. **Evidence Immutability Migration** (blocking prerequisite)
   - `migrations/XXXX_backfill_evidence_digest.sql`
   - ALTER TABLE cie_evidence ADD COLUMN source_text_sha256 VARCHAR(64)
   - UPDATE cie_evidence SET source_text_sha256 = encode(digest(statement, 'sha256'), 'hex')

---

## SUMMARY TABLE

| Field | Value |
|-------|-------|
| **Canonical Commit Boundary** | APPROVAL_COMMIT |
| **Exact Approval Function** | services/clientIntelligenceInterview.js::approveBlueprint (lines 8239–8357) |
| **Canonical Commit Insertion Point** | After line 8298: `const handoff = await createPlaybookFromApprovedBlueprint(...)` |
| **Interview normalizedFacts Role** | WORKING_STATE (mutable during interview; durable in snapshot after approval) |
| **Current Durable Authority** | cie_business_blueprints table; section_provenance epistemicFacts; approved status |
| **Existing BUSINESS Binding** | clients.id → cie_business_blueprints.client_id → cie_evidence.client_id → canonical_business_entities.domain_client_id |
| **Canonical Identity Owner** | SPEC-223B (commitCanonicalSemanticBatch owns all ID generation, deduplication, snapshot creation) |
| **Registry** | BLOCKED — Production registry artifact does not exist; only test fixtures |
| **Evidence** | BLOCKED — cie_evidence.source_text_sha256 column missing; evidence ordering ready |
| **CIE Semantic Subset** | BUSINESS, CUSTOMER_PROFILE, OFFER entities; 8 core predicates (has_name, serves, avoids, operates_in, has_differentiator, targets_growth_focus, targets_outcome, offers_service) |
| **Compatibility Strategy** | Dual-write + phased migration: Phase 1 (approve + commit), Phase 2 (prefer canonical if available), Phase 3 (session archival, optional) |
| **Approval Atomicity** | Two-phase orchestration: canonical commit (SPEC-223B owns transaction) → Blueprint update (CIE owns transaction) |
| **Approval Idempotency** | hash(blueprint_id + version + session_id + evidence_digests + normalizedFacts_digest) → stable key → replay-safe |
| **Canonical Batch Readiness** | NOT_READY (blocked on registry + evidence hash) |
| **First Remaining Blocker** | Production canonical registry artifact must be created and published before any integration work |
| **Minimum Implementation** | 5 files: lib/canonicalSemanticWrite (no changes), services/clientIntelligenceInterview.js (add batch builder + commit call), packages/max/workspace/ClientIntelligenceContext.js (add snapshot projection), 2 migrations (schema + backfill evidence) |

---

**End SPEC-224B Result**

---

### NEXT ACTIONS (Not Part of SPEC-224B, For Planning)

1. **Create production canonical registry** (blocker, must resolve first)
   - Entity vocabulary: BUSINESS, CUSTOMER_PROFILE, OFFER, OUTCOME, METRIC
   - Predicate definitions: has_name, serves, avoids, operates_in, has_differentiator, targets_outcome, targets_growth_focus, offers_service
   - Version: "1.0.0-cie-semantic-model"
   - Compute digest, insert into canonical_registry_artifacts

2. **Backfill cie_evidence.source_text_sha256** (blocker, depends on #1)
   - ALTER TABLE, populate SHA256 for all existing evidence rows

3. **Implement buildCanonicalBatchFromApprovedBlueprint()** (ready to implement after #1 and #2)
   - Transform normalizedFacts + epistemicFacts + evidence into CanonicalSemanticBatch format
   - Map evidence to facts via fact_evidence_links

4. **Wire canonical commit into approveBlueprint()** (ready after #3)
   - Call commitCanonicalSemanticBatch()
   - Store canonical_snapshot_id
   - Add safety checks for orphaned snapshots

5. **Implement snapshot projection** (Phase 3, not blocking)
   - projectNormalizedFactsFromSnapshot() function
   - Reconstruct normalizedFacts from canonical_business_facts + canonical_entity_label_assertions

6. **Test end-to-end approval flow** (after #4)
   - Verify canonical batch creation on approval
   - Verify idempotency on retry
   - Verify backward compatibility with session fallback
