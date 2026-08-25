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
| [ADR-029](ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md) | Artifact Provenance Must Not Affect Consumption | Accepted |
| [ADR-030](ADR-030_Command_Deck_Is_an_Operator_Workspace.md) | Command Deck Is an Operator Workspace | Accepted |
| [ADR-031](ADR-031_Review_Must_Be_Evidence_First.md) | Review Must Be Evidence-First | Accepted |
| [ADR-032](ADR-032_Strategy_Before_Language.md) | Strategy Before Language | Accepted |
| [ADR-033](ADR-033_Kalshi_Research_Stays_Isolated.md) | Kalshi Research Stays Isolated From Production Execution | Accepted |
| [ADR-034](ADR-034_Intent_Before_Execution.md) | Intent Before Execution | Accepted |
| [ADR-035](ADR-035_Plan_Around_State_Not_Sequence.md) | Plan Around State, Not Sequence | Accepted |
| [ADR-036](ADR-036_Trust_Through_Contracts.md) | Trust Through Contracts | Accepted |
| [ADR-037](ADR-037_Reason_About_Businesses_Not_Companies.md) | Reason About Businesses, Not Companies | Accepted |
| [ADR-038](ADR-038_Explain_Planning_Decisions.md) | Explain Planning Decisions | Accepted |
| [ADR-039](ADR-039_Separate_Understanding_from_Execution.md) | Separate Understanding from Execution | Accepted |
| [ADR-040](ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md) | Separate Evidence Acquisition from Capability Selection | Accepted |
| [ADR-041](ADR-041_Operator_Intent_Selects_Execution_Domain.md) | Operator Intent Selects Execution Domain | Accepted |
| [ADR-042](ADR-042_Diagnostic_Capabilities_Explain_Blocked_Execution.md) | Diagnostic Capabilities Explain Blocked Execution | Accepted |
| [ADR-044](ADR-044_Prospect_Acquisition_Independence.md) | Prospect Acquisition Independence | Accepted |
| [ADR-045](ADR-045_Evidence_Before_Reasoning.md) | Evidence Before Reasoning | Accepted |
| [ADR-046](ADR-046_Intent_Determines_Response_Structure.md) | Intent Determines Response Structure | Accepted |
| [ADR-047](ADR-047_Intelligence_Before_Evidence.md) | Intelligence Before Evidence | Accepted |
| [ADR-048](ADR-048_Intent_Selects_Analysis_Mode.md) | Intent Selects Analysis Mode | Accepted |
| [ADR-050](ADR-050_Compile_Market_Knowledge_Before_Runtime.md) | Compile Market Knowledge Before Runtime | Accepted |
| [ADR-051](ADR-051_Provision_Tenant_Before_Intelligence.md) | Provision Tenant Before Intelligence | Accepted |
| [ADR-052](ADR-052_Workspace_First_Registration.md) | Workspace-First Registration | Accepted |
| [ADR-053](ADR-053_Business_Success_Is_Operator_Defined.md) | Business Success Is Operator-Defined | Accepted |
| [ADR-054](ADR-054_Reputation_Is_Capital.md) | Reputation Is Capital | Accepted |
| [ADR-055](ADR-055_Max_Manages_Missions.md) | Max Manages Missions, Not Agents | Accepted |
| [ADR-056](ADR-056_Mission_Planning_Engine_Is_The_Single_Interpreter.md) | Mission Planning Engine Is The Single Interpreter | Accepted |
| [ADR-057](ADR-057_Transactional_Mission_Execution.md) | Transactional Mission Execution Is Atomic | Accepted |
| [ADR-058](ADR-058_Pending_Operator_Decision_Matches_Execution.md) | Pending Operator Decision Must Match Execution | Accepted |
| [ADR-068](ADR-068_Session_State_Is_Explicit.md) | Session State Is Explicit | Accepted |
| [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md) | Classify Communication Before Cognition | Accepted |
| [ADR-070](ADR-070_Session_State_Is_Inspectable.md) | Session State Is Inspectable | Accepted |
| [ADR-071](ADR-071_Session_Directive_Registry.md) | Session Directive Registry | Accepted |
| [ADR-072](ADR-072_Operator_Messages_May_Contain_Multiple_Intents.md) | Operator Messages May Contain Multiple Intents | Accepted |
| [ADR-075](ADR-075_Transactional_Persistence_Exclusivity.md) | Transactional Persistence Exclusivity | Accepted |
| [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md) | Operator Objective Takes Precedence | Accepted |
| [ADR-088](ADR-088_Canonical_Mission_Projection_Is_The_Verification_Contract.md) | Canonical Mission Projection Is The Verification Contract | Accepted |
| [ADR-089](ADR-089_Mission_Ownership_Shall_Not_Cross_Runtime_Boundaries.md) | Mission Ownership Shall Not Cross Runtime Boundaries | Accepted |
| [ADR-090](ADR-090_Canonical_Execution_Routing.md) | Canonical Execution Routing | Accepted |
| [ADR-092](ADR-092_Identity_Before_Enrichment.md) | Identity Before Enrichment | Accepted |

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
