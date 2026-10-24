# Recruiter and Interviewer Guide

**PulseForge is a production-oriented, multi-tenant AI operating system where probabilistic reasoning operates inside deterministic execution boundaries.** Max reasons over objectives and evidence, but acts through bounded capabilities. On the canonical acquisition mission path, software governs mission state, capability contracts, lifecycle transitions, tenant scope, approvals, and execution.

This guide is a proof-of-work map for AI systems, forward-deployed engineering, and solutions engineering reviewers. Start with the architecture below, then follow one failure from investigation to implementation and regression tests.

## Candidate Positioning

**Jacob Maynard**  
**Founder | AI Systems Architect**

Portfolio: [portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co)  
LinkedIn: [linkedin.com/in/jacob-maynard7](https://www.linkedin.com/in/jacob-maynard7/)  
GitHub: [github.com/pulseforgeatmns-ops](https://github.com/pulseforgeatmns-ops)

Jacob built PulseForge around service-business operations: prospect discovery, business understanding, outreach preparation, operator review, and outcome inspection. The portfolio demonstrates his transition from operations leadership into applied AI systems engineering through a founder-led, AI-assisted codebase.

**What to evaluate:** translating an ambiguous business need into a bounded workflow; investigating where the running path diverges from the design; specifying the correction; and checking the resulting code and tests. The audit trails below make that work inspectable.

## What to Look For

### 1. Mission-First Routing and Bounded Capabilities

The workspace resolves mission ownership and execution intent before dispatch. For acquisition work, a Canonical Execution Request enters an execution router; specialists return structured contributions to a shared mission. Scout discovers, Max prioritizes, Paige prepares variants, and Emmett prepares capacity and outbound execution.

- [Workspace routing](../packages/max/workspace/WorkspaceEngine.js) and [routing regression tests](../packages/max/workspace/tests/missionRouting.test.js).
- [SPEC-171: Canonical Execution Router](specs/SPEC-171_Canonical_Execution_Router.md) → [router implementation](../packages/acquisition-mission/ExecutionRouter.js).
- [SPEC-132: Specialist Execution Contract](specs/SPEC-132_Specialist_Execution_Contract.md) → [validated input/result contracts](../packages/acquisition-mission/SpecialistExecutionContract.js).

This describes the canonical acquisition path. Legacy root agent modules and other mission domains remain in the repository; their presence is not proof that all execution has been unified.

### 2. Knowledge, Memory, and Evidence

The system separates evidence from the interpretation placed on it. Client intelligence represents propositions with epistemic status—`KNOWN`, `HYPOTHESIS`, `UNKNOWN`, `UNRESOLVED`, or `NOT_APPLICABLE`—and preserves evidence/provenance. The knowledge package supplies structured claims, queries, timelines, and storage contracts.

- [Epistemic implementation](../services/clientIntelligenceEpistemic.js) and [SPEC-221 regression tests](../test/spec221DurableEpistemicState.test.js).
- [Knowledge package](../packages/knowledge/README.md), [memory architecture](architecture/Memory_Architecture.md), and [Evidence Query Language](specs/SPEC-020_Evidence_Query_Language.md).

Learning also has a boundary: Max's prior-learning adapter requires matching current evidence before influencing prioritization rationale. Its output is advisory and does not mutate heuristic libraries. See the [implementation](../packages/max/workspace/MaxPriorLearningInfluence.js) and [tests](../packages/acquisition-mission/tests/specMaxSecPriorLearning.test.js).

### 3. Reasoning, Policy, and Human Approval

Approval and execution are separate operations. Canonical outbound execution binds authorization to prepared artifacts; changed artifacts or a paused send governor block dispatch. A successful model response alone is insufficient authorization to send.

- [Human approval principle](adr/ADR-003_Human_Approval.md) and [execution approval binding](../packages/acquisition-mission/ExecutionApproval.js).
- [Outbound implementation](../packages/acquisition-mission/OutboundExecution.js) and [regression assertions](../packages/acquisition-mission/tests/spec071ExecuteOutbound.test.js) for artifact drift, governor pause, recipient/copy binding, failed provider results, and replay suppression. See the current validation limits below.

Tenant scope is enforced at the service and mission boundaries: [acquisition routes](../routes/acquisitionMissions.js) reject missing active tenants, [canonical execution](../services/acquisitionMission.js) loads the scoped mission, and the [engine](../packages/acquisition-mission/Engine.js) rejects tenant mismatches. Mission `tenantId` / database `tenant_id` coexist with CRM `client_id`.

### 4. Workflow and Mission Orchestration

Lifecycle progression depends on explicit prerequisites, contributions, and approval state. Transactional Mission Execution validates stage results before committing mission changes; failed validation rolls back the stage. PostgreSQL persistence commits the mission and its related records together. This is a state-commit guarantee, not a claim that an external email can be rolled back.

- [Lifecycle prerequisites](../packages/acquisition-mission/Lifecycle.js) and [SPEC-131: Transactional Mission Execution](specs/SPEC-131_Transactional_Mission_Execution.md).
- [Stage execution](../packages/acquisition-mission/TransactionalExecution.js), [durable stage commit](../services/acquisitionMissionPersistence.js), and [persistence ownership tests](../packages/acquisition-mission/tests/adr075TransactionalPersistence.test.js).

The [acquisition runtime](../services/acquisitionMissionRuntime.js) hydrates tenant missions from storage for inspection. [Canonical mission projection](../packages/acquisition-mission/CanonicalMissionProjection.js) makes persisted state inspectable instead of relying on conversational memory. The broader [mission engine](../packages/mission-engine/) and [capability framework](../packages/capabilities/) remain useful deep dives.

### 5. Production Readiness and Operational Discipline

The runtime is Node.js/Express with PostgreSQL and a Railway deployment target: [server](../server.js), [database pool](../db.js), and [deployment configuration](../railway.json). Production-oriented controls are visible in [migrations](../migrations/), [release records](releases/), and the [disposable PostgreSQL CI workflow](../.github/workflows/revenue-postgres.yml).

Read [CURRENT_STATE.md](../CURRENT_STATE.md) for the dated repository snapshot and retained operating history. Source code and regression fixtures establish implementation evidence; they do not by themselves certify the current deployed environment or every provider integration.

## Selected Engineering Audits / Proof of Work

### Canonical Execution Divergence

- **Problem:** approved discovery failed with `Unknown mission` even though the mission ID was stable.
- **Investigation:** [AUDIT-049](audits/AUDIT-049_Mission_Runtime_Ownership_Crossover.md) traced Scout writing an acquisition-owned mission into the older Mission Engine store. [ADR-090](adr/ADR-090_Canonical_Execution_Routing.md) connects this to the broader pattern of surface-local execution drift.
- **Resulting evidence:** [SPEC-170](specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) defines runtime ownership; [SPEC-171](specs/SPEC-171_Canonical_Execution_Router.md) defines the common request/router. [Ownership tests](../packages/acquisition-mission/tests/spec170MissionRuntimeOwnership.test.js) reject crossover; [router tests](../packages/acquisition-mission/tests/spec171.test.js) and [surface tests](../packages/max/workspace/tests/spec171ExecutionSurface.test.js) exercise canonical dispatch.

### Candidate-Belief Destructive Continuation: 24 → 0

- **Problem:** continuation could lose candidate belief across persistence/reload even while scalar discovery counts survived.
- **Investigation:** the AUDIT-080 finding is recorded in merged [SPEC-204 / PR #492](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/492). Recovery needed to read nested canonical candidate records, not trust the count alone. The fixture contains **24 candidates, 14 qualified**.
- **Resulting evidence:** [belief hydration and integrity checks](../packages/scout/investigation/CandidateBeliefState.js) plus [discovery normalization](../packages/acquisition-mission/DiscoveryPayload.js). [SPEC-204 tests](../test/spec204CandidateBeliefHydration.test.js) preserve 24/14 through normalization, reload, and continuation; reject a count without records; and detect unexplained 24 → 0 collapse. [SPEC-199 tests](../test/spec199CandidateBeliefState.test.js) also check that contradictory evidence can legitimately change qualification.

### Epistemic Classification and Semantic Correction

- **Problem:** uncertainty could become affirmative business prose; negative preferences and hypotheses needed to remain distinct from missing or inapplicable information.
- **Investigation:** [SPEC-221's production case](specs/SPEC-221_Durable_Epistemic_State_for_Business_Understanding.md) records an undefined brand voice becoming a populated fact. [AUDIT-108](../AUDIT-108-SPEC-226-RESULT.md) traces a related correction failure to sentence/keyword routing before approval, where negation and reclassification were lost.
- **Resulting evidence:** [SPEC-226's implementation report](../SPEC-226-RESULT.md) describes working correction operations; [current interview code](../services/clientIntelligenceInterview.js) and [SPEC-221 tests](../test/spec221DurableEpistemicState.test.js) expose the active handling. The later AUDIT-126 regression is preserved in [SPEC-242 tests](../test/spec242PropositionEpistemicSeparation.test.js), which keep a substantive hypothesis separate from its qualifier ([merged implementation PR](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/546)). These are specific regression repairs, not proof of general semantic correctness; SPEC-221's header still says “Proposed.”

### Specialist Integration: Stage Advance Without Max's Contribution

- **Problem:** approval advanced the mission to `UNDERSTAND` while `maxComplete` remained false.
- **Investigation:** [AUDIT-066](architecture/AUDIT-066_Max_Post_Discovery_Dispatch.md) found that prioritization approval bypassed Max's Specialist Execution Contract and required a second path to attach the result.
- **Resulting evidence:** the repair joins [SPEC-131](specs/SPEC-131_Transactional_Mission_Execution.md) stage execution and [SPEC-132](specs/SPEC-132_Specialist_Execution_Contract.md) validation with the [Max executor](../packages/max/workspace/MaxPrioritizationExecutor.js). [Audit regression tests](../packages/acquisition-mission/tests/audit066MaxPostDiscoveryDispatch.test.js) cover success, invalid output, blocked execution, and exceptions without false completion. [Paige handoff tests](../packages/acquisition-mission/tests/spec067PaigePostMaxDispatch.test.js) cover the next specialist boundary.

## Interview Topics This Repo Supports

Use the linked cases to discuss:

- Turning business ambiguity into executable mission and specialist contracts.
- Proving the intended implementation is on the actual execution path.
- Preserving evidence, uncertainty, and accumulated belief across continuation.
- Enforcing human approval, tenant scope, and failure recovery in software.
- Using AI-assisted development while owning investigation and verification.

## Suggested Evaluator Review

| Time | Read | Evaluate |
|---:|---|---|
| 2 min | This guide's opening and Candidate Positioning | What Jacob built and the work to inspect |
| 5 min | [What to Look For](#what-to-look-for) + [SPEC-171](specs/SPEC-171_Canonical_Execution_Router.md) | Current mission, capability, state, and execution boundaries |
| 5 min | [AUDIT-049](audits/AUDIT-049_Mission_Runtime_Ownership_Crossover.md), or another [selected case](#selected-engineering-audits--proof-of-work) | How a real failure was traced to its owning boundary |
| 5 min | [SPEC-170](specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) + [ownership tests](../packages/acquisition-mission/tests/spec170MissionRuntimeOwnership.test.js) (or the matching evidence for your selected case) | Whether the correction is explicit and regression-tested |
| Optional | [Max workspace](../packages/max/workspace/), [mission engine](../packages/mission-engine/), [knowledge](../packages/knowledge/README.md), [specialist contracts](../packages/acquisition-mission/SpecialistExecutionContract.js), [ADRs](adr/README.md) | Implementation depth and design tradeoffs |

For a short technical interview, ask what failed, why the earlier checks missed it, which invariant changed, and what the new test still does not prove.

## Notes

**Validation snapshot (2026-09-17):** 13 linked test files produced 167 passes and 21 failures on both this documentation branch and the untouched starting commit, `acd2348` (Node.js 25.6.1). Failures are in the ADR-075 persistence mock, Paige handoff, outbound execution, and SPEC-221 integration suites. The candidate-belief, routing/ownership, AUDIT-066 Max, prior-learning, and SPEC-242 suites passed. These results do not establish end-to-end production readiness.

This is an active founder-led codebase. Audits and result reports describe the state at the time of investigation; follow their linked current implementations and tests before treating an old blocker or “PASS” as present-day status. Earlier overviews such as [System Architecture](architecture/System_Architecture.md), [Agent Architecture](architecture/Agent_Architecture.md), and the [Max package README](../packages/max/README.md) retain historical framing. The review path above prioritizes the current acquisition implementation.
