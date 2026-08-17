# SPEC-103 — Durable Business Understanding Retrieval

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-084](SPEC-084_Client_Intelligence_Interview_Experience.md), [SPEC-101](SPEC-101_Max_Specialist_Result_Interrogation.md), [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md) |

## Objective

Ensure Max can reliably retrieve and reason from the **approved understanding of a business** before answering operator questions or delegating work.

SPEC-101 taught Max to retrieve before delegating. SPEC-102 taught Max to refuse specialist invocation for retrieval questions. SPEC-103 establishes the **canonical retrieval path** so durable business understanding is actually loaded and consulted.

## Problem

During validation Max correctly classified retrieval questions and refused to hallucinate or invoke specialists. However, when asked:

> What do you currently understand about Anchor Cleaning?

Max answered:

> I don't currently know.

That was correct **behavior** given empty session context. The failure was **architectural**: approved Blueprint data existed in persistent storage but was not loaded during retrieval.

## Core Principle

Business understanding should become durable. Once an operator has taught Max their business through Client Intelligence onboarding, that understanding becomes part of Max's permanent operating context. Max should never behave as though the onboarding interview never happened.

## Retrieval Hierarchy

For retrieval questions, Max consults durable knowledge in this order:

```text
Operator Question
        │
Intent = Retrieval (SPEC-102)
        │
        ▼
Approved Business Blueprint (cie_business_blueprints)
        │
        ▼
Approved Client Playbook (client_playbooks)
        │
        ▼
Knowledge Graph (session envelope / future: KnowledgeService)
        │
        ▼
Mission / Objectives (SPEC-095 operator_objectives)
        │
        ▼
Campaign Context (session envelope)
        │
        ▼
Recent Workspace Context (conversation tail)
        │
        ▼
Unknown
```

Only after this hierarchy is exhausted may Max answer that he does not know enough to answer confidently.

## Business Understanding Contract

Every approved Blueprint exposes a stable retrieval interface via `buildBusinessUnderstandingContract()`:

| Field | Source |
|---|---|
| `companyName` | Blueprint identity |
| `serviceArea` | geography / targetMarkets |
| `services` | services section |
| `targetCustomers` | idealCustomers |
| `targetGeography` | geography |
| `valueProposition` | competitiveAdvantages |
| `businessGoals` | campaignGoals |
| `currentPriorities` | active objectives → campaignGoals fallback |
| `constraints` | avoidCustomers |
| `unknowns` | Blueprint unknowns |

This is the minimum operating context for Max retrieval answers.

## Knowledge States

Max distinguishes three internal states:

| State | Meaning | Operator-facing copy |
|---|---|---|
| `never_learned` | No approved Blueprint exists | Recommends Client Intelligence onboarding |
| `retrieval_failure` | Blueprint should exist but load failed | Architectural failure — not operator's fault |
| `available` | Approved understanding loaded | Natural grounded answer |

Unrelated entity questions (`Who is Aji?`) use the short unknown answer regardless of load state.

## Retrieval Is Read-Only

SPEC-103 does not modify business understanding. It retrieves.

Business understanding changes only through:

- Updated interviews
- Blueprint revisions
- Operator-approved edits

Max never silently rewrites foundational business knowledge.

## Explain the Source

When appropriate, Max answers naturally:

> Based on my current understanding of Anchor Cleaning…

not:

> According to Blueprint Version 3…

The operator should experience continuity, not implementation details.

## Retrieval Before Delegation

Questions such as:

- What do you know about my business?
- What is our service area?
- Who are our ideal customers?
- What are our goals?

must **never** invoke specialists. They are answered from durable understanding via SPEC-102 + SPEC-103.

## Shared Context for Specialists

Retrieved understanding is attached to the workspace session (`clientIntelligence` attachment). Downstream specialists consume that shared context — Max retrieves once.

## Architecture

```text
Operator message
      ↓
SPEC-101 interrogation
      ↓
SPEC-102 cognitive mode (retrieval / explanation / reflection)
      ↓
SPEC-103 loadDurableBusinessUnderstanding()
      ↓
composeDurableRetrievalAnswer() or unknown
      ↓
Never delegate for retrieval
```

## Implementation

| File | Role |
|---|---|
| `packages/max/workspace/BusinessUnderstandingRetrieval.js` | Canonical load path, contract, compose |
| `packages/max/workspace/RetrievalBeforeDelegationContext.js` | SPEC-102 hook wired to SPEC-103 loader |
| `packages/max/workspace/ClientIntelligenceContext.js` | Blueprint load + formatUnderstandingAnswer |
| `packages/max/workspace/OperatorObjectiveContext.js` | Active objectives in hierarchy |
| `packages/max/specialistDelegation/CognitiveMode.js` | Expanded retrieval patterns |
| `packages/max/workspace/WorkspaceEngine.js` | Passes cieService through retrieval hook |

## Testing

- `packages/max/workspace/tests/businessUnderstandingRetrieval.test.js`
- `packages/max/workspace/tests/retrievalBeforeDelegation.test.js`

## Acceptance Criteria

- [x] "What do you currently understand about Anchor Cleaning?" → rich business summary from Blueprint
- [x] "What is our service area?" → Manchester (+ approved expansion areas)
- [x] "Who are our ideal customers?" → approved target markets
- [x] "What are our current business priorities?" → durable objectives or goals
- [x] None of these questions invoke Scout
- [x] Fresh session loads Blueprint without prior workspace messages
- [x] Missing Blueprint recommends onboarding — does not invent
- [x] Retrieval failure distinguished from never learned (internal state)

## Future Work

- Wire Knowledge Graph via KnowledgeService query (not session placeholder only)
- Active playbook strategy fields in retrieval compose (channels, sequence, offers)
- Attach retrieved contract to specialist delegation payloads explicitly
