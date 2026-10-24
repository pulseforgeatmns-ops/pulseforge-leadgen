# ADR-044 — Prospect Acquisition Independence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-29 |
| **Spec** | [SPEC-060](../specs/SPEC-060_Prospect_Acquisition_Framework.md) |
| **Related** | [ADR-035](ADR-035_Plan_Around_State_Not_Sequence.md), [ADR-029](ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md), [ADR-028](ADR-028_Business_State_Flows_Through_Artifacts.md), [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md) |
| **Supersedes** | Discovery-as-sole-prerequisite for campaign ProspectLists (implicit prior model) |

## Context

Campaign execution was previously coupled to Discovery in operator mental models and recovery paths. Operational failures in Discovery (missing Places key, provider outages, empty geography) unnecessarily prevented campaign execution even when valid prospects already existed via operator lists, prior missions, or CSV imports.

SPEC-043 and SPEC-051 already allow operator-supplied and resolved ProspectLists. SPEC-060 formalizes acquisition as a provider-agnostic domain so Discovery is one strategy among many, not the default gate.

## Decision

1. **Campaigns operate on validated ProspectLists** — never on a Discovery-specific type.
2. **Prospect Acquisition is an independent domain** responsible only for producing candidate records (`CandidateSet`) with provenance.
3. **Discovery is one acquisition strategy among many** — Manual entry, CSV import, existing repositories, and future integrations are equally valid.
4. **Providers must not publish ProspectLists** — they publish Candidates; Verification owns ProspectList creation.
5. **Campaign Builder and all downstream stages remain acquisition-agnostic** (ADR-029).
6. **Manual acquisition is always available** — Discovery/CSV failure does not block alternate acquisition.

## Consequences

### Positive

- Campaign execution is independent of Discovery provider health
- Operators can execute campaigns immediately from existing or manually supplied data
- PulseForge can operate without paid discovery providers enabled
- New providers require only contract implementation + registration

### Negative / tradeoffs

- Two ingress shapes coexist briefly: Discovery capability (still produces ProspectList for backward compatibility) and Acquisition providers (CandidateSet → Verification)
- Operators must understand Acquisition vs Discovery language in Workspace

### Follow-ups

- [x] SPEC-060 v1: domain, providers, CandidateSet, verification adapter, capability registration, Mission Planning strategy, injectProspectList routing, Workspace panel, acceptance tests
- [ ] Optional: refactor Prospect Discovery capability to call Acquisition + Verification exclusively
- [ ] Future provider registrations (CRM, Apollo, etc.) without planner/builder changes
