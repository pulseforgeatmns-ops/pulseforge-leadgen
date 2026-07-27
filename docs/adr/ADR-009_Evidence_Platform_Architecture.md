# ADR-009 — Evidence Platform Architecture

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | [SPEC-015A](../specs/SPEC-015A_Reasoning_Runtime_Decoupling.md), [SPEC-015](../specs/SPEC-015_Market_Intelligence_Domain.md) |
| **Supersedes** | — |

## Context

Pulseforge began as a B2B outbound platform built to solve commercial lead generation.

As the architecture evolved, several core systems became independent of the CRM domain:

- Knowledge Graph
- Evidence Store
- Claim Engine
- Confidence Model
- Memory Engine
- Reasoning Runtime

Architectural audit ([SPEC-015A](../specs/SPEC-015A_Reasoning_Runtime_Decoupling.md), [EVIDENCE_CORE_DOMAIN_AUDIT.md](../architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md)) confirmed that these systems are domain-neutral. The remaining CRM assumptions were isolated into injectable Strategy Packs without changing runtime behavior.

This ADR formalizes that architectural boundary.

## Decision

Pulseforge shall be treated as an **Evidence Platform** composed of a domain-neutral core and domain-specific strategy packs.

The **Evidence Core** is responsible for storing observations, accumulating evidence, updating claims, maintaining memory, reasoning over historical information, and producing explainable recommendations.

Domain knowledge is supplied exclusively through injected strategy packs and providers.

The Evidence Core **MUST NOT** contain business logic specific to any domain.

## Architectural Model

```text
                Evidence Platform

        ┌──────────────────────────────┐
        │       Evidence Core          │
        │                              │
        │  Knowledge Graph             │
        │  Evidence Store              │
        │  Claim Engine                │
        │  Confidence Engine           │
        │  Memory Engine               │
        │  Reasoning Runtime           │
        └──────────────┬───────────────┘
                       │
         Strategy Pack Interfaces
                       │
      ┌────────────────┼────────────────┐
      │                │                │
 CRM Strategy     Market Strategy   Future Domains
      │                │                │
 Recommendations  Research Output   Domain Output
```

## Responsibilities

### Evidence Core Owns

- Evidence ingestion
- Claim lifecycle
- Confidence updates
- Memory
- Knowledge graph
- Historical analog retrieval
- Reasoning orchestration
- Explainability
- Replay infrastructure

These components must remain domain-neutral.

### Strategy Packs Own

Strategy Packs provide domain meaning. Examples include:

**CRM**

- outreach recommendations
- pipeline evaluation
- follow-up prioritization

**Markets**

- regime interpretation
- volatility interpretation
- research recommendations

**Future Domains**

- manufacturing
- logistics
- healthcare
- security
- operations

The runtime must not branch on domain type.

## Architectural Principles

### 1. Evidence Before Conclusions

Every recommendation must be supported by explicit evidence. No opaque scoring.

### 2. Claims Are Universal

A Claim is a domain-independent assertion whose confidence changes as evidence accumulates. Claims replace domain-specific hypothesis implementations.

### 3. Memory Is Domain-Neutral

Memory stores observations and state transitions. It does not understand business semantics.

### 4. Reasoning Is Domain-Neutral

The runtime orchestrates reasoning. It never interprets what evidence means. Interpretation belongs to the active Strategy Pack.

### 5. Explainability Is Mandatory

Every recommendation must expose:

- supporting evidence
- contradicting evidence
- confidence
- reasoning trace
- historical analogs

Explainability is a platform capability, not a domain feature.

### 6. Replay Is Foundational

Every decision produced by the platform should be reproducible from recorded evidence. Replay is a first-class architectural capability.

## Explicit Non-Goals

The Evidence Core is not responsible for:

- CRM workflows
- Market trading
- Email sequencing
- Brokerage integration
- Industry-specific heuristics

These belong outside the core.

## Consequences

### Positive

- New domains require strategy packs rather than runtime rewrites
- Improvements to reasoning benefit every domain
- Replay, confidence, and memory are shared capabilities
- Testing becomes simpler because domain logic is isolated

### Negative / tradeoffs

- Strategy pack interfaces become long-term API contracts
- Domain authors must translate observations into generic Evidence and Claims
- Additional abstraction increases initial implementation effort but reduces future coupling

### Status After Adoption

The following components are now considered stable platform primitives:

- Evidence Store
- Knowledge Graph
- Claim Engine
- Confidence Engine
- Memory Engine
- Reasoning Runtime
- Strategy Pack Interfaces

Future work should extend the platform through adapters and strategy packs rather than modifying the Evidence Core unless a change benefits all domains.

### Follow-ups

- [Reasoning_Runtime_Architecture.md](../architecture/Reasoning_Runtime_Architecture.md) — runtime contracts
- [EVIDENCE_CORE_DOMAIN_AUDIT.md](../architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md) — coupling inventory
- Market Domain strategy pack ([SPEC-015](../specs/SPEC-015_Market_Intelligence_Domain.md))
- Deterministic Replay ([SPEC-018](../specs/SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md))
- Evidence Laboratory ([SPEC-019](../specs/SPEC-019_Evidence_Laboratory.md)) — isolated exploration without production mutation
- Evidence Query Language ([SPEC-020](../specs/SPEC-020_Evidence_Query_Language.md)) — domain-neutral declarative queries
