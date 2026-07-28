# SPEC-048 — Sales Intelligence Engine

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-028, SPEC-031, SPEC-033, SPEC-042, SPEC-047; soft-depends SPEC-030 |
| **Blocks** | Multi-channel generators consuming shared sell strategy; approval-rate optimization |
| **ADR** | [ADR-032 Strategy Before Language](../adr/ADR-032_Strategy_Before_Language.md) |

## Objective

Transform Company Intelligence into actionable sales strategy **before** any outreach is generated.

The system shall no longer generate outreach directly from research alone. It shall derive a structured **Sales Intelligence Profile** that every outbound channel consumes.

This becomes the reasoning layer behind:

- Direct Mail
- Email
- LinkedIn
- Call Scripts
- Proposal Generation
- Future Sales Coaching
- Future AI-assisted Selling

**Separate business reasoning from language generation.** Optimize for operator approval, not generation volume.

## Vision References

- [ADR-032 Strategy Before Language](../adr/ADR-032_Strategy_Before_Language.md)
- [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)
- [ADR-017 Intelligence Before Execution](../adr/ADR-017_Intelligence_Before_Execution.md)
- [ADR-031 Review Must Be Evidence-First](../adr/ADR-031_Review_Must_Be_Evidence_First.md)
- [SPEC-028 Client Playbook](SPEC-028_Client_Playbook_Capability.md)
- [SPEC-030 Company Intelligence](SPEC-030_Company_Intelligence_Capability.md) (soft — thin inputs OK in v1)
- [SPEC-031 Business Signals](SPEC-031_Business_Signals_Capability.md)
- [SPEC-033 Mail Package Generator](SPEC-033_Mail_Package_Generator.md)
- [SPEC-042 Mission Artifact Bus](SPEC-042_Mission_Artifact_Bus.md)
- [SPEC-047 Review Workspace Interaction Layer](SPEC-047_Review_Workspace_Interaction_Layer.md)

## Problem

Current flow forced one path to simultaneously understand the company, infer the buyer, identify buying signals, choose positioning, and write copy. Reasoning was hidden inside generation. Operators could not inspect it. Results: generic messaging, wrong-industry assumptions, weak value props, AI-sounding copy.

## Architecture

```text
Company Intelligence
        ↓
Sales Intelligence Profile
        ↓
Messaging Strategy
        ↓
Channel Generator (Mail / Email / LinkedIn / Phone / Proposal)
        ↓
Letter / Copy
```

```text
ProspectList → CompanyIntelligence → OpportunityRanking
  → ★ SalesIntelligenceProfile ★
  → Campaign → MailPackage → Review → Execution
```

## Scope (v1)

- Package `packages/capabilities/salesIntelligence/`
  - `types.js` — `SalesIntelligenceProfile`, messaging strategy, claims, gates
  - `derive.js` — deterministic derivation (no letter prose)
  - `gates.js` — quality gates with rejection reasons
  - `humanTest.js` — Human Test dimensions + Operator Confidence Score
  - `approvalRate.js` — Operator Approval Rate tracking stub
  - `SalesIntelligenceEngine.js` — capability façade
  - `index.js`
- Artifact Bus type `SalesIntelligenceProfile` / alias `sales_intelligence_profile`
- Stage `sales_intelligence` in Stage Library + PipelineGate contract
- Mail Package Generator consumes profile (prospect-first, evidence-backed personalization)
- Campaign Builder fills `mailMerge` from profile when present
- Review Workspace shows Sales Intelligence → Messaging Strategy → Score → Letter
- Tests under `packages/capabilities/tests/`

## Out of Scope (v1)

- Live SPEC-030 Company Intelligence package (use thin / stub CI + signals + ranking brief + playbook)
- Email / LinkedIn / phone script generators
- Required Proposal consumer
- Replacing Client Playbook
- LLM freeform strategy without structured profile
- Operator Confidence Score replacing human approval
- Durable approval-rate dashboard (stub only)

## Artifact: SalesIntelligenceProfile

No prose. Only reasoning.

| Field | Notes |
|---|---|
| `prospectId` | Stable id |
| `company` | Company name |
| `industry` | Verified or evidenced industry |
| `decision_maker` | Inferred buyer role + confidence |
| `buyer_type` | e.g. Relationship Driven |
| `primary_pain` / `secondary_pain` | Structured |
| `business_goal` / `risk_if_unchanged` | Structured |
| `anchor_advantage[]` | From playbook / client strengths (labeled) |
| `recommended_angle` | Positioning angle |
| `call_to_action` | CTA |
| `buying_signals[]` | `{ signal, confidence, evidence, source }` |
| `messaging_strategy` | `{ opening_focus, avoid[], social_proof[], cta, tone[], positioning }` |
| `personalization_claims[]` | `{ claim, evidenceRef, verified }` |
| `confidence` | `High` / `Medium` / `Low` (+ numeric) |
| `sendable` | Generators must not emit sendable copy when false |
| `evidenceRefs[]` | Provenance |
| `operatorConfidence` | Advisory Human Test score object |

## Quality Gates

Reject (or mark non-sendable) when:

- Wrong industry vs playbook / profile markets
- Wrong / unsupported buyer inference without evidence
- Unsupported personalization
- Hallucinated facts
- Generic value proposition with no evidence
- Low reasoning confidence
- Prospect discussed after Anchor in generated copy (mail gate)

Every rejection records: `reason`, `evidence`, `regenerationRecommendation`.

## Writing Principles (channel generators)

1. Begin with the prospect — never with Anchor.
2. Opening paragraph demonstrates understanding before services.
3. At least one verified company-specific observation.
4. Unsupported personalization is prohibited.

## Human Test

Ask: if this appeared in an experienced salesperson's outbox, would another experienced salesperson believe it was intentionally written by that person?

Dimensions: Industry Understanding, Buyer Relevance, Prospect First, Natural Language, Specificity, Sales Judgment, Edit Instinct.

Operator Confidence Score is advisory only.

## North Star

**Operator Approval Rate** = percentage of generated outreach approved without substantive edits.

## Acceptance Criteria

- [x] Every prospect in a Sales Intelligence run produces a `SalesIntelligenceProfile`
- [x] Every personalization claim links to evidence (or is marked unverified and blocked)
- [x] Wrong-industry messaging is rejected / non-sendable
- [x] Mail Package consumes the same Sales Intelligence Profile
- [x] Operators review reasoning before language in Review Workspace
- [x] Outreach begins with the prospect when profile-driven
- [x] Drafts receive an Operator Confidence Score
- [x] Operator Approval Rate stub records approve / edit / reject
- [x] Spec + ADR-032 documented; indexes updated

## Testing

```bash
npm run test:capabilities
npm run test:mission
```

## Future Work

- Email / LinkedIn / call script generators
- Proposal required consumer
- Durable approval-rate analytics on Outcome Intelligence
- Richer SPEC-030 Company Intelligence inputs
