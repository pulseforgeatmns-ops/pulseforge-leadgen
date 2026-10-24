# SPEC-261: Anchor quarantined email recovery

Status: In progress, authorized local implementation/tests only. Actual production configuration, operator references, preparation grants and activation manifests remain unset. No production migration, preparation, approval binding, activation, publication or send is authorized by this document.

## Objective
Prepare a bounded tenant-10 first-touch recovery path that excludes the ambiguous historical recipient, CRM contact, company and exact attempt from recovery dispatch while retaining honest UNKNOWN evidence. Separate inspection, existing-artifact preparation, canonical approval binding and a single explicitly approved provider attempt.

## Vision References
Product_Constitution.md and Product_Thesis.md: human authority, explainability, tenant isolation and conservative outbound behavior.

## Problem
The October 19 incident inspection found both outreach-review and call-only flags absent on the shared prospects table. A failed pre-dispatch SELECT was conservatively marked as crossed. Recipient filtering alone cannot remove a tenantwide uncertainty stop safely. Historical logs lack exact item-bound negative provider proof. Expired historical inventory is not current prepared inventory, and an activation manifest cannot be fabricated before its exact envelope/item exists.

## Scope / Out of Scope
Local default-disabled source, strict separate approval inputs, durable no-resend quarantine, read-only preflight/preview, exact existing-artifact preparation, canonical execution-approval binding, disposable PostgreSQL tests and an operator proposal. No historical outcome reclassification, retry-token invention, global admission policy, provenance-based automatic approval, spreadsheet hold release, paid discovery/enrichment, new generated copy, general preparation/refill, production DDL or sends. No broader tenant eligibility.

## Dependencies
Base 8cbeba9d1c95d74d6a658108ca9840a7044744ed; frozen PR956 safety runtime/tests/spec and CI changes, excluding its CURRENT_STATE/CHANGELOG text; boundary-only SPEC260 patch SHA256 aa620f81bab57ec724d941ecb78b4ec2c8d47a4aab6bff60615f0ab8ac2a8d29. This sibling is a separate artifact; neither original archive is edited.

The combined workspace applies a metadata-only numbering overlay: its boundary decision is [ADR-104](../adr/ADR-104_Instrumented_Provider_Boundaries.md), with corresponding SPEC-260 and DECISIONS references updated. The frozen boundary artifact retains ADR-102 and its original patch hash. On October 19, ADR-104 was absent across both local docs/adr and docs/architecture, the read-only default-branch code search, and the complete changed-file list of draft PR955 at head be45bfd432fed2f719b2618d409a5872193330ac. PR955 adds ADR-102_Explicit_Immutable_Legacy_Coverage_Epoch; the base already has architecture ADR-102_Investigation_Follows_Uncertainty. The overlay avoids introducing another ADR-102 into the combined artifact without modifying either dependency. PR955 uses SPEC-SCOUT-CONT-001/002 and adds no SPEC-260, SPEC-261 or ADR-103. Source: https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/955.

## Architecture

### Runtime safety states
Three states: legacy only when recovery schema is absent and maintenance is unarmed; maintenance with covered provider paths closed despite absent/malformed manifests; active only for the exact approved tenant-10 governed Brevo email context. A durable maintenance latch survives environment removal. Rollback retains the gate and latch; ungated older binaries are unsafe rollback targets after schema work.

Closure is scoped to the shared CRM email/SMS authorization paths across tenants and the explicitly guarded non-CRM mailbox-verification path. Manual/contextless sends and other tenants gain no authority through those covered paths. Calls retain legacy behavior. This is not a transport-wide shutdown of all Brevo calls or all transactional/notification senders; inventory and separately control any additional route before relying on broader closure.

Leaf authorization independently validates actual recipient, CRM contact/company, live program/policy, envelope/item/revision/approval/copy/sender, governed Brevo route, exact PostgreSQL attempt token, manifest admission and non-quarantine status. The scoped email read omits only the call-specific flag. It directly requires `ao_outreach_review_required` to exist and be boolean false; missing, NULL, true or malformed values block. Existing DNC, synthetic, verification/provenance, tenant, ownership, suppression, budget, spacing, canonical approval and sender checks remain.

### Separate operational phases
1. **READONLY_PREVIEW.** `prepareOnlyPreview(db, config, { now })` inspects at most one existing current-day authorized envelope and pending, unattempted item. Its API retains the explicit `mode: 'PREPARE_ONLY'` configuration label. It requires tenant 10, exact program/policy and quarantine hashes, an inspection approval reference, a current window no longer than 24 hours and cap one. SELECT-only queries validate frozen snapshot/hash, canonical approval, current CRM flags and sender. The proposed activation manifest has NULL ID/send-approval fields and cannot activate dispatch. Missing current inventory yields `current_prepared_item_required`; nothing is generated or written. `reviewExistingPreparation` separately provides a read-only review of the exact existing-artifact preparation proposal.
2. **APPROVED_PREPARE_EXISTING.** `prepareExistingApproved` requires a separately approved exact current-day ready mission, complete contribution-set and Scout/Max/Paige/Emmett artifact IDs/hashes, prepared revision, exact candidate/contact/company/recipient/copy/sender, authorized operator, cap one and bounded activation configuration. With durable maintenance armed and active send manifest absent, it atomically writes one frozen unapproved envelope, one pending item and one audit/consumption event. It does not create or modify missions/artifacts, discover candidates, run Scout/search/enrichment/LLMs, call ordinary prepare/refill, create execution approval or contact a provider. Missing/stale/insufficient artifacts require separate bounded research/preparation approval.
3. **Canonical approval binding.** After explicit approval of the exact frozen recipient/copy/sender and scope, `bindRecoveryApproval` uses the canonical execution-approval writer, verifies the durable contribution and binds it to the envelope. This is a state-changing, separately gated operation, not preview or preparation permission. It does not install activation authority or dispatch. The same explicit approval may cover this binding and the exact one-shot send when its scope says so; technical separation does not require duplicate identical user approval.
4. **Exact cap-one activation/dispatch.** Only after the preceding bindings are verified may a separately approved durable manifest and matching activation configuration admit that one item. Actual values are not supplied by this artifact. Approval does not transfer to another item, recipient, program or day.

### Quarantine and concurrency
Quarantine is additive: UNKNOWN remains UNKNOWN. Records are immutable and non-expiring in this implementation; receipts, reconciliation and retries cannot remove them. This is an operational hold, not an irrevocable business promise about every future contact. Any future override requires a separate explicit decision and separately reviewed implementation; none is provided here.

Every attempted/uncertain row, including NULL attempted_at, must match its own independently approved exact-attempt quarantine or remain a blocker. Raw counts and consumed budgets stay visible. The scheduler excludes only its exact current non-quarantined attempted item. Candidate identity is separate from CRM contact/company identity. Recipient/contact/company/item matches and conservative company-name/domain aliases remain excluded; missing, changed or ambiguous company identity fails closed.

Lock order uses the existing tenant executor lock, scheduler executor lock, shared recovery advisory boundary, control row, sorted identity locks and relevant row locks. Apply/preparation/approval use the required exclusive boundaries; the leaf holds authorization locks through provider handoff. Claim reserves one admission atomically. A separate durable one-shot dispatch CAS commits before provider handoff and survives later authorization COMMIT loss. Neither reservation nor marker is refunded by uncertainty, retry or expiry. The maximum fresh provider attempt is one.

## Data Model
Dedicated recovery control, reservation and exact-attempt quarantine tables; no repurposed lifecycle/DNC state. Quarantine binds explicit item/envelope/program/mission/canonical/candidate/contact/company/recipient/token/revision and company-identity snapshot to an approval reference and decision hash. Apply locks and validates live rows; only an identical decision is idempotent. Historical outcomes, reasons, attempt timestamps and capacity remain unchanged. Live identities and approval references are supplied only by separately approved operational inputs, not embedded in source.

## Implementation Plan
1. Preserve the frozen boundary/supporting patches and the explicit ADR-104 metadata-only overlay.
2. Implement state/manifest/leaf checks, immutable quarantine, read-only preflight and exact apply API.
3. Integrate preparation/tick/claim/scheduler/reconciliation exclusions and durable one-shot handoff.
4. Keep READONLY_PREVIEW, APPROVED_PREPARE_EXISTING, canonical approval binding and activation as separate explicit phases.
5. Run adversarial/native PostgreSQL tests and independent review; deliver the scoped operator proposal without production activation.

## Migration Strategy
The new recovery schema proposal is manual in `docs/sql/anchor-email-recovery-schema-proposal.sql`, outside automatic migration paths. It requires explicit verified-closed-gate/source-revision attestations before DDL. These attest operator evidence; SQL cannot verify deployed workers.

Do not describe initial source deployment as globally zero-DDL. Existing `server.js` startup initializers remain: `runClientsStartup`, `ensureAoProspectRoutingSchema`, `ensureAoCrmSchema` and other unrelated schema initializers can run their existing DDL. This repair adds no automatic recovery-schema runner. Review the actual startup footprint as part of separately approved deployment, drain older workers and verify closure on every covered path before manual recovery DDL.

The review-column schema and admission treatment remain a separate policy decision with a shared-table/global schema footprint. No ALTER prospects, default/backfill choice or row-release workflow is included. Preserve explicit true/false where present; absence/unknown never becomes admission. Preflight reports missing mandatory review schema as blocking and affected counts by tenant. Rollback retains durable maintenance, quarantine and reservations; there is no destructive down migration.

## Testing
Default-disabled compatibility; scoped armed closure including mailbox verification; strict manifests; identity/copy/scope mutations; missing/malformed review flags; multiple UNKNOWN/NULL-token rows; exact scheduler own item; no automatic quarantine release after receipts/reconciliation; native lock ordering/CAS/cap/rollback; SELECT-only previews and preflight; exact current-artifact freeze with no provider or regeneration; canonical approval binding separately from dispatch. Provider responses are mocked and PostgreSQL fixtures are disposable loopback instances.

## Acceptance Criteria
Original outcomes and ledgers remain honest; every uncovered UNKNOWN blocks. Non-expiring quarantine cannot be bypassed through retries or recreated identity. Preview and freeze-only preparation grant no send authority. Only a verified exact one-item tenant-10 governed Brevo manifest can authorize a fresh handoff. Missing policy, schema, approval, locking or evidence closes the covered dispatch path. Local tests do not establish current production readiness or successful delivery.

## Future Work
Recheck document identifiers against any changed publication target. Additional quarantine overrides, wider caps/tenants, new paid preparation, broader transport closure and provenance-based admission require separate decisions and approval. Future boundary observations never fabricate retrospective proof.
