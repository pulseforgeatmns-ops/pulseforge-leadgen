# Decisions Index

Canonical Architectural Decision Records live in [`docs/adr/`](docs/adr/). This file is the human-readable index.

| ID | Title | Status | Summary |
|---|---|---|---|
| [ADR-001](docs/adr/ADR-001_Conversation_First.md) | Conversation First | Accepted | Primary product interaction model is conversational intelligence, not dashboard-only CRUD |
| [ADR-002](docs/adr/ADR-002_Explainable_AI.md) | Explainable AI | Accepted | Material scores and recommendations must be auditable and explainable |
| [ADR-003](docs/adr/ADR-003_Human_Approval.md) | Human Approval | Accepted | Customer-visible sends/posts require explicit human approval unless a later ADR narrows the gate |
| [ADR-004](docs/adr/ADR-004_Knowledge_Graph.md) | Knowledge Graph | Accepted | Durable business memory is modeled as a knowledge graph (SPEC-001) |
| [ADR-005](docs/adr/ADR-005_LLM_Presentation_Engine.md) | LLM Presentation Engine | Accepted | LLMs communicate verified intelligence only — they never create business intelligence |
| [ADR-006](docs/adr/ADR-006_Live_Intelligence_Evolution.md) | Live Intelligence Evolution | Accepted | Intelligence evolves in place via IntelligenceEvent; refresh is a fallback, not the product language |
| [ADR-007](docs/adr/ADR-007_Operator_Intelligence.md) | Operator Intelligence | Accepted | Operator behavior learning personalizes presentation only — never evidence, confidence, reasoning, or policy |
| [ADR-008](docs/adr/ADR-008_Outcome_Intelligence.md) | Outcome Intelligence | Accepted | Evaluates whether intelligence was right — measures, calibrates, reports; never alters reasoning or confidence |
| [ADR-009](docs/adr/ADR-009_Evidence_Platform_Architecture.md) | Evidence Platform Architecture | Accepted | Domain-neutral Evidence Core + injectable Strategy Packs; core must not contain domain business logic |
| [ADR-010](docs/adr/ADR-010_Mission_Engine.md) | Mission Engine | Accepted | Intent → Capability Registry; durable missions; review before outreach; Mission-First UX on Command Deck |
| [ADR-011](docs/adr/ADR-011_Capability_Framework.md) | Capability Framework | Accepted | Capabilities are the stable API of Pulseforge; agents are implementation details |
| [ADR-014](docs/adr/ADR-014_Personalized_by_Default.md) | Personalized by Default | Accepted | Proposal generation optimizes for relevance, not speed; interchangeable name-swap decks fail |
| [ADR-015](docs/adr/ADR-015_Strategy_Lives_in_the_Playbook.md) | Strategy Lives in the Playbook | Accepted | Client strategy lives in versioned Client Playbooks; capabilities execute strategy |
| [ADR-016](docs/adr/ADR-016_Execution_Does_Not_Decide.md) | Execution Does Not Decide | Accepted | Execution Engine carries out approved strategy only — never creates it |
| [ADR-017](docs/adr/ADR-017_Intelligence_Before_Execution.md) | Intelligence Before Execution | Accepted | Company Intelligence packages before Ranking/Campaign/Proposal/Execution; never fabricate |
| [ADR-018](docs/adr/ADR-018_Time_Matters.md) | Time Matters | Accepted | Business intelligence is time-aware; recent verified signals increase relevance; expired signals decay |
| [ADR-019](docs/adr/ADR-019_Missions_Are_Conversations.md) | Missions Are Conversations | Accepted | A Mission is a persistent collaborative workspace; follow-ups refine in place; capabilities consume the current revision; execution uses the latest approved revision |
| [ADR-021](docs/adr/ADR-021_Human_Approval_Before_Execution.md) | Human Approval Before Execution | Accepted | Generation produces artifacts; review validates; approval authorizes; Execution consumes only the latest approved campaign revision |
| [ADR-022](docs/adr/ADR-022_Execution_Consumes_Approved_Artifacts.md) | Execution Consumes Approved Artifacts | Accepted | Execution never generates content; once Printing begins, campaign artifacts are immutable; changes require a new approved revision |
| [ADR-023](docs/adr/ADR-023_Experience_Becomes_Intelligence.md) | Experience Becomes Intelligence | Accepted | Operational experience becomes structured intelligence only after evidence + operator approval; distinct from SPEC-013 recommendation evaluation |
| [ADR-024](docs/adr/ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md) | Human Work Is Coordinated Through the Operator Inbox | Accepted | Capabilities generate work items; Operator Inbox organizes them; inbox never performs workflow processing |
| [ADR-025](docs/adr/ADR-025_Active_Missions_Take_Precedence.md) | Active Missions Take Precedence | Accepted | Active Mission always outranks IntentRouter; IntentRouter creates new Missions only; conversational flow uses Active Mission Resolver until terminal |
| [ADR-026](docs/adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md) | Business Success Determines Pipeline Progress | Accepted | Pipeline advances only on validated business artifacts; technical execution alone is not enough; empty Discovery is Blocked not Completed |
| [ADR-027](docs/adr/ADR-027_Mission_Planning_Is_Objective_Driven.md) | Mission Planning Is Objective-Driven | Accepted | IntentRouter decides Mission vs not; Mission Planner builds execution graphs from objectives; stage keywords augment, never replace |
| [ADR-028](docs/adr/ADR-028_Business_State_Flows_Through_Artifacts.md) | Business State Flows Through Artifacts | Accepted | Mission Artifact Bus is the canonical business-state exchange; immutable typed artifacts replace flat priorOutputs as source of truth |
| [ADR-029](docs/adr/ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md) | Artifact Provenance Must Not Affect Consumption | Accepted | Consumers resolve artifacts by type, validation status, and revision only; producer identity is provenance and must not alter execution |
| [ADR-030](docs/adr/ADR-030_Command_Deck_Is_an_Operator_Workspace.md) | Command Deck Is an Operator Workspace | Accepted | Command Deck is an operator workspace — presentation polish must not change Mission execution semantics |
| [ADR-031](docs/adr/ADR-031_Review_Must_Be_Evidence_First.md) | Review Must Be Evidence-First | Accepted | Review Workspace presents business artifacts and evidence before implementation metadata |
| [ADR-032](docs/adr/ADR-032_Strategy_Before_Language.md) | Strategy Before Language | Accepted | Separate business reasoning from language generation; Sales Intelligence Profile before channel copy; optimize for Operator Approval Rate |
| [ADR-033](docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md) | Kalshi Research Stays Isolated | Accepted | Kalshi BTC research lives at `packages/kalshi-research`; paper/replay only; never deployed; never wired into production execution |
| [ADR-034](docs/adr/ADR-034_Intent_Before_Execution.md) | Intent Before Execution | Accepted | Mission Plan IR is the executable contract; capabilities never consume raw operator Notes; unknown text becomes Notes |
| [ADR-035](docs/adr/ADR-035_Plan_Around_State_Not_Sequence.md) | Plan Around State, Not Sequence | Accepted | Mission Planning resolves artifact requirements before selecting capabilities; Discovery is an acquisition strategy |
| [ADR-036](docs/adr/ADR-036_Trust_Through_Contracts.md) | Trust Through Contracts | Accepted | Artifacts validated at system boundaries before Artifact Bus; capabilities consume trusted contracts and do not re-verify |
| [ADR-037](docs/adr/ADR-037_Reason_About_Businesses_Not_Companies.md) | Reason About Businesses, Not Companies | Accepted | Model business operations (Business Intelligence) rather than company directories; Sales Intelligence consumes BI; uncertainty explicit |
| [ADR-038](docs/adr/ADR-038_Explain_Planning_Decisions.md) | Explain Planning Decisions | Accepted | Planner explains select/reject/unavailable via Capability Registry diagnostics; no bare Unknown capability / Acquire via unavailable |
| [ADR-039](docs/adr/ADR-039_Separate_Understanding_from_Execution.md) | Separate Understanding from Execution | Accepted | Intent Understanding → MissionIntent → Capability Planning → MissionPlan; language probabilistic, execution deterministic |
| [ADR-040](docs/adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md) | Separate Evidence Acquisition from Capability Selection | Accepted | Evidence Planning between Intent and Capability Planning; acquire missing evidence before review/outcome capabilities |
| [ADR-041](docs/adr/ADR-041_Operator_Intent_Selects_Execution_Domain.md) | Operator Intent Selects Execution Domain | Accepted | Operator intent selects execution domain; market_intelligence is a reserved domain socket |
| [ADR-042](docs/adr/ADR-042_Diagnostic_Capabilities_Explain_Blocked_Execution.md) | Diagnostic Capabilities Explain Blocked Execution | Accepted | Diagnostic capabilities explain blocked execution instead of silent failure |
| [ADR-044](docs/adr/ADR-044_Prospect_Acquisition_Independence.md) | Prospect Acquisition Independence | Accepted | Campaigns operate on ProspectLists; Discovery is one acquisition strategy among many |
| [ADR-045](docs/adr/ADR-045_Evidence_Before_Reasoning.md) | Evidence Before Reasoning | Accepted | Domains collect evidence and do not recommend; reasoning engines consume evidence; facts persist, conclusions are ephemeral |
| [ADR-046](docs/adr/ADR-046_Intent_Determines_Response_Structure.md) | Intent Determines Response Structure | Accepted | Operator intent selects a response contract before retrieval or delegation; advice is not a universal response type |
| [ADR-047](docs/adr/ADR-047_Intelligence_Before_Evidence.md) | Intelligence Before Evidence | Accepted | Max synthesizes first-class business intelligence from grounded claims before presenting evidence; insufficient evidence is Unknown |
| [ADR-048](docs/adr/ADR-048_Intent_Selects_Analysis_Mode.md) | Intent Selects Analysis Mode | Accepted | Operator intent selects an explicit analysis mode before reasoning; diagnosis, unknown analysis, risk, and progress are first-class |
| [ADR-049](docs/adr/ADR-049_Understand_Market_Before_Selling.md) | Understand the Market Before Selling Into It | Accepted | AIM holds client market understanding before Scout acquires; Scout reasons over pain ontology; Paige never starts from zero when qualification exists |

## How to add a decision

1. Copy [`docs/adr/TEMPLATE.md`](docs/adr/TEMPLATE.md).
2. Number sequentially (`ADR-005`, …).
3. Link from this index.
4. Reference from the implementing spec and CHANGELOG.
5. Update CURRENT_STATE **Upcoming Decisions** when resolved.

## Related operational notes

Pre-foundation decisions are scattered across `CLAUDE.md` (Known Architectural Notes), `AGENT_RULES.md`, and flat `docs/*` runbooks. New architectural choices must use ADRs; do not only append to chat or CLAUDE.md.
