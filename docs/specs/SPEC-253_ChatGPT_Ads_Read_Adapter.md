# SPEC-253 — Canonical Penny ChatGPT Ads Read Adapter

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v1.x |
| **Priority** | High |
| **Owner** | PulseForge |
| **Created** | 2026-09-15 |

## Objective

Add a tenant-safe, internally enforced read-only OpenAI Advertiser API adapter so canonical Penny can observe ChatGPT Ads platform evidence for a linked client without mutating ads, budgets, or audiences.

## Problem

AUDIT-097 / AUDIT-098 established that OpenAI exposes `https://api.ads.openai.com/v1`. After SPEC-252, ChatGPT Ads still failed closed with `PLATFORM_ADAPTER_NOT_IMPLEMENTED` while Google and Meta had live read adapters.

## Scope

- `packages/penny-paid-acquisition/adapters/chatgptAds.js`
- Tenant-scoped `chatgpt_ads` account binding via existing `ad_accounts`
- Credential reuse of `ad_accounts.access_token`
- GET /ad_account identity verification
- Campaign + insights + conversion-insight normalization
- Collector wiring: `chatgpt_ads` → chatgptAds adapter
- Production readiness: `READY` or `BLOCKED_MISSING_CHATGPT_ADS_CREDENTIAL`

## Out of Scope

Campaign/ad/audience/budget mutation, Paige integration, automated spend authority, first-party attribution, CRM outcome translation, Yelp, cron redesign, legacy Penny retirement, Max chat delegation.

## Architecture

Collector resolves the tenant-linked `chatgpt_ads` row, then the adapter:

1. Authenticates with the server-side Ads Manager key.
2. Verifies `GET /ad_account` identity against `ad_accounts.account_id`.
3. Reads campaigns and campaign-level insights.
4. Reads conversion insights when available.
5. Normalizes into the existing Penny platform-evidence contract.

Read-only enforcement is an explicit operation allowlist. Callers cannot pass arbitrary OpenAI Ads paths or methods.

## Conversion semantics

OpenAI attributed counts remain `platformConversions` / `clickThroughConversions` / `viewThroughConversions`. They are not translated into `qualifiedLead`, `walkthrough`, `proposal`, `recurringClient`, or revenue.

## Acceptance Criteria

See `packages/penny-paid-acquisition/tests/spec253ChatgptAdsAdapter.test.js`.
