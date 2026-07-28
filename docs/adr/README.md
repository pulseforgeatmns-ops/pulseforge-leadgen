# Architecture Decision Records

ADRs capture **why** we locked an architectural choice. They are permanent. Status changes via a new ADR that supersedes an old one—do not silently rewrite history.

## Index

| ADR | Title | Status |
|---|---|---|
| [ADR-001](ADR-001_Conversation_First.md) | Conversation First | Accepted |
| [ADR-002](ADR-002_Explainable_AI.md) | Explainable AI | Accepted |
| [ADR-003](ADR-003_Human_Approval.md) | Human Approval | Accepted |
| [ADR-004](ADR-004_Knowledge_Graph.md) | Knowledge Graph | Accepted |
| [ADR-005](ADR-005_LLM_Presentation_Engine.md) | LLM Presentation Engine | Accepted |
| [ADR-006](ADR-006_Live_Intelligence_Evolution.md) | Live Intelligence Evolution | Accepted |
| [ADR-007](ADR-007_Operator_Intelligence.md) | Operator Intelligence | Accepted |
| [ADR-008](ADR-008_Outcome_Intelligence.md) | Outcome Intelligence | Accepted |
| [ADR-009](ADR-009_Evidence_Platform_Architecture.md) | Evidence Platform Architecture | Accepted |
| [ADR-010](ADR-010_Mission_Engine.md) | Mission Engine | Accepted |
| [ADR-011](ADR-011_Capability_Framework.md) | Capability Framework | Accepted |
| [ADR-014](ADR-014_Personalized_by_Default.md) | Personalized by Default | Accepted |
| [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md) | Strategy Lives in the Playbook | Accepted |
| [ADR-016](ADR-016_Execution_Does_Not_Decide.md) | Execution Does Not Decide | Accepted |
| [ADR-017](ADR-017_Intelligence_Before_Execution.md) | Intelligence Before Execution | Accepted |
| [ADR-018](ADR-018_Time_Matters.md) | Time Matters | Accepted |
| [ADR-019](ADR-019_Missions_Are_Conversations.md) | Missions Are Conversations | Accepted |
| [ADR-021](ADR-021_Human_Approval_Before_Execution.md) | Human Approval Before Execution | Accepted |
| [ADR-022](ADR-022_Execution_Consumes_Approved_Artifacts.md) | Execution Consumes Approved Artifacts | Accepted |
| [ADR-023](ADR-023_Experience_Becomes_Intelligence.md) | Experience Becomes Intelligence | Accepted |
| [ADR-024](ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md) | Human Work Is Coordinated Through the Operator Inbox | Accepted |
| [ADR-025](ADR-025_Active_Missions_Take_Precedence.md) | Active Missions Take Precedence | Accepted |
| [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md) | Business Success Determines Pipeline Progress | Accepted |
| [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md) | Mission Planning Is Objective-Driven | Accepted |
| [ADR-028](ADR-028_Business_State_Flows_Through_Artifacts.md) | Business State Flows Through Artifacts | Accepted |

Also listed in root [`DECISIONS.md`](../../DECISIONS.md).

## When to write an ADR

- New data store or memory model
- Changes to agent authority / automation boundaries
- Auth, tenancy, or approval policy changes
- Any decision that would be expensive to reverse

## Process

1. Copy [TEMPLATE.md](TEMPLATE.md)
2. Discuss in PR with the implementing spec
3. Merge as Accepted (or Proposed if still debating—avoid lingering Proposed without owner)
4. Link from DECISIONS.md, spec, and CHANGELOG
