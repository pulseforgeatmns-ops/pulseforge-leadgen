# AUDIT-060 — Provider Execution Persistence

| Field | Value |
|---|---|
| **Status** | Completed — first divergence identified |
| **Date** | 2026-08-26 |
| **Related** | [AUDIT-059](AUDIT-059_External_Discovery_Provider_Failure.md), PR #459 (`cursor/audit-059-discovery-provider-failure-f86e`), [SPEC-172](../specs/SPEC-172_Canonical_Scout_Evidence_Handoff.md), [SPEC-133](../specs/SPEC-133_Discovery_Artifact_Presentation.md) |
| **Scope** | Trace `providerExecution` from Google Places through Scout → Discovery contribution → mission persistence → `validateDiscoveryOutput()` → operator response. Stop at first divergence. |

## Executive summary

**PR #459 is exercised on the mission path.** Google Places executes, `PlacesProvider.lastExecution` is populated, and `providerExecution` is attached to the Scout intelligence payload and the canonical discovery artifact.

**First divergence is contribution normalization.** `normalizeScoutDiscoveryPayload()` in `DiscoveryPayload.js` never copies `artifact.providerExecution` into the discovery contribution. Everything downstream — TME validation, mission commit, presentation, and operator response — reads only the contribution payload, so provider execution details are invisible even when the sensor ran successfully.

**Stop.** This is a persistence/propagation gap, not a provider gap.

When the operator sees **Contribution must attach evidence.**, that is a separate TME validation failure (`assertEvidenceAttached`) that occurs **after** the propagation drop and **before** commit. The provider execution record exists in the in-memory `scoutResult` but is not on `discoveryPayload`, is not checked by `validateDiscoveryOutput()`, and is rolled back with the transaction — so the operator receives only the generic validation message.

---

## Trace

```text
Google Places HTTP
  ↓  PlacesProvider.lastExecution / providerReports
Scout intelligence payload
  ↓  payload.providerExecution = universe.providerReports
Canonical discovery artifact
  ↓  buildScoutDiscoveryArtifact → artifact.providerExecution
Discovery contribution normalization          ← FIRST DIVERGENCE
  ↓  normalizeScoutDiscoveryPayload omits providerExecution
validateDiscoveryOutput()
  ↓  assertEvidenceAttached(payload) — payload has no providerExecution
commitDiscoveryStage()
  ↓  never reached on validation throw; contribution lacks providerExecution even on success
Operator response
  ↓  DiscoveryPresentation has no providerExecution section
```

---

## Answers

### 1. Does Google Places execute?

**Yes**, on the AMO discovery path when an operational Places adapter is available.

Flow: `advanceDiscoveryAfterApproval` → `runScoutForAmoMission` → `Scout.discover` → `runDiscoveryPipeline` → `CandidateUniverse` → `runHypothesisDrivenDiscovery` → `executeProviderAssignment` → `adapter.discover({ evidenceRequest })` → `PlacesProvider.collectEvidence()`.

Abort-before-HTTP cases (missing key, invalid request, empty geography) are recorded on `lastExecution` with `executed: false` and an `abortReason`. See AUDIT-059.

### 2. Is `providerExecution` created?

**Yes, after PR #459.**

| Layer | Field | Location |
|---|---|---|
| Provider | `lastExecution` | `PlacesProvider.js` — query, HTTP/Google status, latency, retries, quota, errors |
| Adapter report | `execution` | `DiscoveryAdapters.js` — copied from `provider.lastExecution` |
| Engine | `providerReports[]` | `HypothesisDrivenDiscoveryEngine.js` — normalized via `ProviderEvidenceContract` |
| Scout payload | `providerExecution` | `ScoutAdapter.js` — `universe.providerReports \|\| []` |

On `main` (pre–PR #459), this record does not exist — Places returned `[]` with no execution metadata. That was AUDIT-059's divergence.

### 3. Is it attached to the Scout result?

**Yes.**

```javascript
// packages/max/scoutAcquisition/ScoutAdapter.js
providerExecution: universe.providerReports || [],
```

Verified: `scoutResult.payload.providerExecution` is populated when hypothesis-driven discovery runs and `CandidateUniverse` receives `engineResult.providerReports`.

### 4. Is it attached to the Discovery contribution?

**No — first divergence.**

`mapScoutIntelligenceToDiscoveryPayload()` calls `buildScoutDiscoveryArtifact()` (which sets `artifact.providerExecution`) then `normalizeScoutDiscoveryPayload()`. The contribution object built in `DiscoveryPayload.js` lists ~30 fields but **does not include `providerExecution`**.

Reproduction on PR #459 branch:

```javascript
artifact.providerExecution   // present
contribution.providerExecution  // undefined
```

`assertScoutEvidenceHandoff()` runs between artifact and contribution but only guards canonical **evidence** counts, not provider execution metadata.

### 5. Is it persisted with the mission?

**No.**

`commitDiscoveryStage()` persists `{ ...discoveryPayload, approvalId, transactionId }`. Because `discoveryPayload` never received `providerExecution`, the Scout discovery contribution on the mission store cannot contain it.

Even when discovery commits successfully (`blocked: true`, incomplete coverage, zero qualified), the persisted contribution lacks provider execution details.

### 6. Is it available to `validateDiscoveryOutput()`?

**No.**

`validateDiscoveryOutput()` in `AmoOperatorApproval.js` reads `output.discoveryPayload` only. It calls:

- `assertContributionContract`
- `assertConfidenceValid`
- `assertEvidenceAttached(payload, { required: !blocked })`
- `assertExecutionResult`

None of these inspect `providerExecution`, `scoutResult.payload.providerExecution`, or `artifact.providerExecution`. Validation cannot cite Places query/status/quota even when the sensor record exists upstream.

### 7. If `assertEvidenceAttached()` throws, does the provider execution record still exist?

**In memory only — not durably.**

TME order (`TransactionalExecution.executeMissionStage`):

1. `execute()` — returns `{ scoutResult, discoveryPayload }`. `scoutResult.payload.providerExecution` exists; `discoveryPayload.providerExecution` does not.
2. `validateOutput()` — `assertEvidenceAttached` throws → transaction rolls back.
3. `commit()` — never runs.

After rollback:

| Location | Provider execution present? |
|---|---|
| `scoutResult.payload.providerExecution` | Yes (discarded with staged output) |
| `discoveryPayload` | No (never copied) |
| Mission store / contributions | No (rolled back) |
| Execution audit | Rollback recorded; provider execution not in audit payload |

`assertEvidenceAttached` throws when `blocked === false` and both `evidence` and `buyingSignals` are empty. Example: `discoveryStatus: 'complete'` with `qualifiedCount: 0` (legitimate empty-market shape) — `resolveBlocked()` returns `false`, evidence is required, validation fails.

When `discoveryStatus: 'incomplete'` and `qualifiedCount: 0`, `blocked: true`, evidence is not required, validation passes — but provider execution is still not persisted (see Q4–Q5).

### 8. Why does the operator see only "Contribution must attach evidence." instead of provider execution details?

Two compounding gaps:

**A. Propagation gap (first divergence).** Provider execution never reaches `discoveryPayload`, so no downstream surface can render it.

**B. Error surfacing gap (when validation throws).** `deriveExecutionBlock()` and the execution router surface `err.message` from the TME validation error. They do not attach `scoutResult.payload.providerExecution` or `PlacesProvider.lastExecution` to the operator block. The message is literally:

```javascript
throw validationError('tme_evidence_missing', 'Contribution must attach evidence.');
```

**C. Presentation gap (when validation passes).** `DiscoveryPresentation.js` formats evidence, coverage, and ranked prospects from the contribution payload. It has no `providerExecution` section. A committed blocked discovery shows summary/coverage warnings but not Places query, Google status, or quota.

---

## First divergence

**`DiscoveryPayload.normalizeScoutDiscoveryPayload()` drops `artifact.providerExecution`.**

PR #459 correctly wires execution through Scout and the canonical artifact. The AMO contribution contract was not updated to carry it forward. Stop there.

---

## Remediation (out of scope for audit stop)

Minimal fix path:

1. Copy `providerExecution: artifact.providerExecution` into the contribution object in `DiscoveryPayload.js`.
2. Extend `DiscoveryPresentation.js` to render provider execution when `blocked` or `discoveryStatus === 'incomplete'`.
3. Optionally enrich TME validation errors with `providerExecution` from `output.scoutResult` when `assertEvidenceAttached` fails, so rollback responses remain diagnosable.

---

## Tests

Reproduction script (PR #459 branch):

```bash
node -e "
const { buildScoutDiscoveryArtifact } = require('./packages/scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('./packages/acquisition-mission/DiscoveryPayload');
const scoutResult = { payload: { qualifiedCount: 0, discoveryStatus: 'incomplete', providerExecution: [{ providerId: 'google_maps' }] } };
const artifact = buildScoutDiscoveryArtifact(scoutResult);
const contribution = normalizeScoutDiscoveryPayload(scoutResult, { discoveryArtifact: artifact });
console.log('artifact:', !!artifact.providerExecution, 'contribution:', !!contribution.providerExecution);
"
# artifact: true contribution: false
```

Provider-layer coverage: `test/audit059ExternalDiscoveryProviderFailure.test.js` (Q1–Q4 stop at provider execution; does not assert AMO contribution persistence).
