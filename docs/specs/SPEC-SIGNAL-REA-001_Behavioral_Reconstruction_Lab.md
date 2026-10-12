# SPEC-SIGNAL-REA-001 — Behavioral Reconstruction Lab

**Status:** Implemented foundation; first capture ready  
**Deadline:** 2026-10-14  
**Owner:** Signal engineering  
**Runtime effect:** None

## Problem

Signal can collect caller events and prospective market evidence, but engineering questions about third-party interfaces are still answered ad hoc. Token identity, route parameters, alert behavior, chart state and app links can be guessed from screenshots or manually observed once, leaving no reusable evidence or bounded verification contract.

## Decision

Use REA as a local engineering instrument for bounded investigation of software the operator owns, is authorized to inspect, or can lawfully observe through a public browser surface. Keep REA outside the production Signal runtime.

REA findings are evidence for engineering decisions. They do not become caller evidence, cohort observations, market outcomes or trading instructions. Any implementation derived from a finding uses normal Signal code and fixture-based verification.

## Foundation

- REA is pinned to `rea-agents@6.4.0` by `scripts/signalReaLab.js`.
- `npm run signal:rea:doctor` checks the Codex registration and REA skill identity.
- `npm run signal:rea:check` validates the committed investigation brief and its fail-closed constraints.
- `npm run signal:rea:brief` prints the canonical first brief.
- Local artifacts live under ignored `signal-v1/rea-workspace/`. Evidence selected for review must be reduced to a safe finding ledger or an explicitly reviewed evidence bundle before commit.

## First investigation

`signal-v1/rea/investigations/fomo-token-actionability.json` asks three bounded questions:

1. Which value is the authoritative Solana token identity?
2. Which URL, app link or intent opens that same token state from a Signal alert?
3. Which fields can Signal show before the destination confirms them?

The browser page already open by the operator is the preferred target. An Android package is only eligible when it is the operator's installed copy or another artifact the operator is authorized to examine.

## Required ledger

Each conclusion records:

- question ID;
- Evidence ID or local evidence-bundle reference;
- observation in plain language;
- confidence and evidence type;
- search boundary;
- unresolved gaps;
- proposed Signal design choice, kept separate from observed behavior.

## Boundaries

- Passive observation is the default.
- Authenticated read-only observation is allowed when the operator owns the session.
- No trade, wallet connection, wallet signature, message send, credential capture or entitlement bypass.
- No REA result writes to prospective research tables or changes the N=50 blinding contract.
- Ambiguous token identity fails closed and produces no action link.
- Captured third-party artifacts and authenticated runtime evidence remain local by default.

## Wednesday acceptance

- [x] REA pinned; Codex setup and readiness commands verified.
- [x] Repo-local readiness command exists and passes.
- [x] First investigation brief is committed and policy checked.
- [ ] Capture the FOMO browser or authorized Android evidence.
- [ ] Complete the Q1-Q3 finding ledger.
- [ ] Implement and test the action-link contract if the evidence supports one; otherwise record the unresolved blocker.

The final three items require an observable target. They are complete only when evidence exists; availability of REA alone does not satisfy them.
