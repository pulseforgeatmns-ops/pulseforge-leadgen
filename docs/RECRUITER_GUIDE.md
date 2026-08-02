# Recruiter and Interviewer Guide

This guide is a short map for reviewers evaluating Pulseforge as evidence of AI systems architecture, forward-deployed engineering, solution engineering, or technical leadership.

## Candidate Positioning

**Jacob Maynard**  
**Founder | AI Systems Architect**

Portfolio: [portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co)  
LinkedIn: [linkedin.com/in/jacob-maynard7](https://www.linkedin.com/in/jacob-maynard7/)  
GitHub: [github.com/pulseforgeatmns-ops](https://github.com/pulseforgeatmns-ops)

Pulseforge demonstrates the transition from operations leadership into applied AI systems engineering: identifying real operational bottlenecks, designing software around them, validating the system in live service-business workflows, and documenting the architecture through specs and ADRs.

## What to Look For

### 1. Multi-Agent System Design

Relevant files:

- [docs/architecture/Agent_Architecture.md](architecture/Agent_Architecture.md)
- [AGENT_RULES.md](../AGENT_RULES.md)
- Root agent modules such as `leadgen.js`, `setterHandoffAgent.js`, `emmettAgent.js`, `rileyAgent.js`, and `maxAgent.js`

The agent layer is separated by responsibility: discovery, enrichment, outreach, routing, reporting, triage, and reasoning. The design avoids treating one model prompt as the whole system.

### 2. Knowledge, Memory, and Evidence

Relevant files:

- [packages/knowledge/README.md](../packages/knowledge/README.md)
- [docs/architecture/Knowledge_Graph_Architecture.md](architecture/Knowledge_Graph_Architecture.md)
- [docs/architecture/Memory_Architecture.md](architecture/Memory_Architecture.md)
- [docs/specs/SPEC-001_Persistent_Knowledge_Store.md](specs/SPEC-001_Persistent_Knowledge_Store.md)
- [docs/specs/SPEC-020_Evidence_Query_Language.md](specs/SPEC-020_Evidence_Query_Language.md)

The knowledge layer is designed around structured business memory, evidence, claims, timelines, query surfaces, and swappable storage contracts.

### 3. Reasoning, Policy, and Human Approval

Relevant files:

- [packages/max/README.md](../packages/max/README.md)
- [docs/adr/ADR-003_Human_Approval.md](adr/ADR-003_Human_Approval.md)
- [docs/adr/ADR-009_Evidence_Platform_Architecture.md](adr/ADR-009_Evidence_Platform_Architecture.md)
- [docs/adr/ADR-016_Execution_Does_Not_Decide.md](adr/ADR-016_Execution_Does_Not_Decide.md)
- [docs/adr/ADR-021_Human_Approval_Before_Execution.md](adr/ADR-021_Human_Approval_Before_Execution.md)

Max is designed to construct explainable recommendations, apply explicit policy, and preserve human authority over customer-visible work.

### 4. Workflow and Mission Orchestration

Relevant files:

- [packages/mission-engine/](../packages/mission-engine/)
- [packages/capabilities/](../packages/capabilities/)
- [docs/specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md](specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [docs/specs/SPEC-050_Deterministic_Mission_Planning.md](specs/SPEC-050_Deterministic_Mission_Planning.md)
- [docs/specs/SPEC-056_Evidence_Driven_Capability_Planning.md](specs/SPEC-056_Evidence_Driven_Capability_Planning.md)

The mission layer shows how objectives, artifacts, capabilities, and execution domains are coordinated without relying on unconstrained agent behavior.

### 5. Production Readiness and Operational Discipline

Relevant files:

- [CURRENT_STATE.md](../CURRENT_STATE.md)
- [CHANGELOG.md](../CHANGELOG.md)
- [docs/releases/](releases/)
- [migrations/](../migrations/)
- [test/](../test/)

The repository includes production runbooks, release evidence, migrations, test suites, and explicit current-state documentation.

## Interview Topics This Repo Supports

Pulseforge can support concrete interview discussion around:

- Multi-agent orchestration
- RAG and knowledge retrieval patterns
- Persistent memory and context management
- Evidence-based recommendation design
- Human-in-the-loop AI
- Tenant isolation and scoped business data
- Workflow automation
- CRM and communications automation
- System design tradeoffs
- Building from ambiguity
- Translating business needs into technical architecture
- Operating a production-oriented AI system as a founder

## Suggested 10-Minute Review

1. Read the root [README.md](../README.md).
2. Skim [Product Thesis](vision/Product_Thesis.md).
3. Skim [System Architecture](architecture/System_Architecture.md).
4. Read [packages/max/README.md](../packages/max/README.md).
5. Review the ADR index in [docs/adr/README.md](adr/README.md).

That path gives the fastest signal on architecture, judgment, and implementation depth.

## Notes

This is an active founder-led codebase. Some operational files are intentionally retained because they document real deployment history and live business constraints. The recommended review path above highlights the portfolio-relevant material first.
