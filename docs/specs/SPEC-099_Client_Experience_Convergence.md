# SPEC-099 — Client Experience Convergence & Pilot Surface

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Depends on** | SPEC-096 (tenant isolation), SPEC-097 (onboarding recovery), SPEC-098 (Max continuity) |
| **Non-goals** | Full visual redesign, new onboarding architecture, autonomous execution |

## Purpose

Make the existing Pulseforge client experience coherent and safe enough for a pilot client (Aji / AS Cleaning) without redesigning the application.

Mental model:

> Pulseforge understands my business through Max, preserves that understanding, and helps me decide what to do next.

## Delivered

1. **Client shell identity** — `/api/me` returns authoritative `client.display_name` from session `client_id`. Shell shows company workspace name for client role (not personal login name). Query/body client id cannot influence it.
2. **Client navigation labels** — Command Deck → **Max**, Client Intel → **My Business** (presentation only; routes unchanged).
3. **Operator surfaces hidden on client Home** — Agent roster, Deploy All, activity panel, client switcher, Anchor sample CTAs. UI hiding is presentation only; SPEC-096 server auth remains authoritative.
4. **Onboarding Home states** — none → Start with Max; in progress / review → Continue with Max (SPEC-097); approved → Ask Max + My Business.
5. **CIE as My Business** — client-facing copy + Max Intelligence Workspace visual tokens (cream / navy / gold / Newsreader).
6. **Max composer layout** — single conversation scroll; header + composer remain accessible (no clipped dock scroll region).
7. **Client Max language** — presentation boundary softens SPEC-*/CIE/ContextEnvelope/Mission Plan IR jargon without removing evidence/confidence vocabulary.

## Tests

- `test/clientExperienceConvergence.test.js`
- Existing SPEC-096/097 (`test/cieIsolationAndRecovery.test.js`) and SPEC-098 (`packages/max/workspace/tests/clientIntelligenceContinuity.test.js`) remain green.

## Explicit non-goals

Complete Pulseforge visual overhaul, public website redesign, new dashboard architecture, new memory architecture.
