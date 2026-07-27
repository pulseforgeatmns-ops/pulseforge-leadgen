# Specs

Implementation contracts for Pulseforge. Specs are the bridge between vision and code.

## Active and planned

| Spec | Title | Status | Release |
|---|---|---|---|
| [SPEC-000](SPEC-000_Repository_Foundation.md) | Repository Foundation & Source of Truth | Done | v0.7.0 |
| [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md) | Knowledge Layer Foundation | Done | v0.7.1 |
| [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md) | Graph Synchronization Engine | Done | v0.7.2 |
| [SPEC-001](SPEC-001_Persistent_Knowledge_Store.md) | Persistent Knowledge Store | Done | v0.7.3 |
| [SPEC-001C](SPEC-001C_Knowledge_Query_Engine.md) | Knowledge Query Engine | Done | v0.7.4 |
| [SPEC-002](SPEC-002_Max_Reasoning_Engine.md) | Max Reasoning Engine | Done | v0.8.0 |
| [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md) | Temporal Intelligence & Memory | Done | v0.8.1 |
| [SPEC-004](SPEC-004_Max_Briefing_Engine.md) | Max Briefing Engine | Done | v0.9.0 |
| [SPEC-005](SPEC-005_Policy_Decision_Engine.md) | Policy & Decision Engine | Done | v0.9.1 |
| [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) | Command Deck Composition Engine | Done | v0.9.2 |
| [SPEC-008](SPEC-008_Command_Deck_UI.md) | Command Deck UI | Implemented | v1.0.0 |
| [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) | Max Intelligence Workspace | Implemented | v1.0.0 |
| [SPEC-010](SPEC-010_Intelligence_Navigation.md) | Intelligence Navigation | Implemented | v1.0.0 |
| [SPEC-011](SPEC-011_Live_Intelligence_Loop.md) | Live Intelligence Loop | Implemented | v1.0.0 |
| [SPEC-012](SPEC-012_Operator_Intelligence.md) | Operator Intelligence | Implemented | v1.0.0 |
| [SPEC-013](SPEC-013_Outcome_Intelligence.md) | Outcome Intelligence | Implemented | v1.0.0 |
| [SPEC-014](SPEC-014_Knowledge_Dual_Write.md) | Knowledge Dual-Write & Operational Readiness | In Progress | v1.0.0 |
| [SPEC-015A](SPEC-015A_Reasoning_Runtime_Decoupling.md) | Reasoning Runtime Decoupling | Done | v1.0.0 |
| [SPEC-015](SPEC-015_Market_Intelligence_Domain.md) | Market Intelligence Domain (MID) | Draft | TBD |
| [SPEC-017](SPEC-017_Domain_Ontology_Framework_and_Market_Ontology.md) | Domain Ontology Framework & Market Ontology | Done | v1.0.1 |
| [SPEC-018](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md) | Deterministic Replay & Temporal Reasoning Engine | Done | v1.0.1 |
| [SPEC-019](SPEC-019_Evidence_Laboratory.md) | Evidence Laboratory | Done | v1.0.1 |
| [SPEC-020](SPEC-020_Evidence_Query_Language.md) | Evidence Query Language (EQL) | Done | v1.0.1 |
| [SPEC-021](SPEC-021_Learning_and_Belief_Evolution_Engine.md) | Learning & Belief Evolution Engine | Done | v1.0.1 |
| [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) | Mission Engine & Agent Orchestration | Implemented (thin slice) | v1.1.1 |
| [SPEC-023](SPEC-023_Capability_Framework.md) | Capability Framework | Implemented (discovery + ranking live) | v1.2.0 |
| [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md) | Prospect Discovery Capability | Implemented | v1.0.0 |
| [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md) | Opportunity Ranking Capability | Implemented | v1.0.0 |
| [SPEC-027B](SPEC-027B_Proposal_Generator_Capability.md) | Proposal Generator Capability | Implemented (v1 thin slice) | v1.2.0 |
| [SPEC-028](SPEC-028_Client_Playbook_Capability.md) | Client Playbook Capability | Implemented (v1) | v1.2.0 |
| [SPEC-029](SPEC-029_Execution_Engine.md) | Execution Engine | Proposed | v1.3.0 |
| [SPEC-030](SPEC-030_Company_Intelligence_Capability.md) | Company Intelligence Capability | Proposed | v1.2.1 |
| [SPEC-031](SPEC-031_Business_Signals_Capability.md) | Business Signals Capability | Proposed | v1.2.2 |
| [SPEC-032](SPEC-032_Mission_Memory.md) | Mission Memory | Proposed | v1.3.0 |
| [SPEC-006](SPEC-006_Command_Deck.md) | Pulseforge Command Deck | Approved | v1.0.0 |
| [SPEC-001_Business_Knowledge_Graph.md](SPEC-001_Business_Knowledge_Graph.md) | Business Knowledge Graph (remaining production ingest) | Draft | — |

## Process

1. Copy [TEMPLATE.md](TEMPLATE.md).
2. Number sequentially.
3. Link vision docs and ADRs under Vision References.
4. Set status in the spec header and in `CURRENT_STATE.md` when work starts.
5. Implement only what Scope allows.
6. Close when Acceptance Criteria pass; update CHANGELOG + CURRENT_STATE.

## Rules

- Specs do not redefine Product Constitution — they implement it.
- Architecture changes require an ADR.
- Every PR links its spec.
- Prefer slicing a large spec into sequenced PRs over silent scope creep.

## Template sections (required)

Objective · Vision References · Problem · Scope · Out of Scope · Dependencies · Architecture · Data Model · Implementation Plan · Migration Strategy · Testing · Acceptance Criteria · Future Work
