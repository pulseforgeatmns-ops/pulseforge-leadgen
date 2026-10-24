# SPEC-PAIGE-ANCHOR-SOCIAL-002 — Anchor Adaptive Content & Publishing System

Extends **SPEC-256 Paige Canonical Social** with reusable organic planning capabilities under `packages/capabilities/paigeOrganicSocial/`.

## Operating loop

Understand → Plan → Create → Approve → Publish → Measure → Learn

Paige autonomously indexes Anchor Drive media, maintains a rolling backlog, recommends platform-specific timing, and learns from durable performance history. External publishing remains approval-gated (SPEC-256 artifacts + operator review).

## Key modules

| Path | Role |
|---|---|
| `packages/capabilities/paigeOrganicSocial/` | Canonical stores, media sync, planner, scheduler, performance bridge |
| `services/paigeAnchorOrganicSocial.js` | Postgres-backed service entry |
| `routes/paigeSocial.js` | `/api/paige/social/organic/*` operator APIs |
| `services/paigeSocialContentExecution.js` | Injects `organicPlan` into SPEC-256 generation for client 10 |
| `migrations/2026-10-07-paige-anchor-organic-social-002.sql` | Durable media, backlog, platform history |

## Configuration

- `PAIGE_ANCHOR_MEDIA_DRIVE_FOLDER_ID` or `clients.metadata.paige.mediaLibrary.driveFolderId`
- `PAIGE_ANCHOR_ORGANIC_SOCIAL_ENABLED=false` to disable (default: enabled for client 10)

## Cron

`POST /cron/paige_organic?client_id=10&secret=…` — sync Drive media + maintain backlog.

## Tests

`node --test test/paigeAnchorOrganicSocial002.test.js` (included in `npm run test:paige`).
