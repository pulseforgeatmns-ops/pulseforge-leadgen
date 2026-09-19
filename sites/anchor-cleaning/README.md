# Anchor Cleaning website

Source for [goanchorcleaning.com](https://goanchorcleaning.com/). GitHub Pages on `pulseforgeatmns-ops/anchor-cleaning` (`main` → `/`).

## Terminology

**Canonical customer-facing commercial term:** Facility Assessment (singular)

**Legacy internal API terminology:** walkthrough

The public form POSTs to `POST /api/public/walkthrough` and analytics may still emit `walkthrough_form_submit`. Those names are internal compatibility only — do not use "walkthrough" in customer-facing copy.

Use **facility assessment** / **Facility Assessment** in prose and CTAs as grammatically appropriate. Do not use "facilities assessment".

## Brand identity

**Source of truth:** approved gold polo logo (`assets/brand/anchor-polo-logo-source.png`)

**Primary brand identity:** full gold Anchor Cleaning lockup (header, OG preview, social profile avatars)

**Secondary compact icon:** anchor symbol only (favicon, Apple touch, manifest icons)

Do **not** use the symbol-only mark for social profile avatars.

Derived assets are generated from this exact artwork — see `assets/brand/CANONICAL_BRAND_ASSETS.md`.

**Cache busting:** `?v=20260916` on all brand asset URLs. Bump when replacing artwork.

**Platform profile avatars** (Facebook, Instagram, LinkedIn, GBP, Yelp): upload `social-avatar-v20260916.png` (full lockup on navy — not symbol-only).

## Pages

| URL | Purpose |
|---|---|
| `/` | Commercial cleaning — primary CTA: **Request a Facility Assessment** |
| `/residential/` | Residential home cleaning — primary CTA: **Request Home Cleaning** |

## Lead forms

Both forms POST to `POST /api/public/walkthrough` on the Pulseforge app. Submissions write an `agent_actions` row for `client_id=10`.

Residential uses home-cleaning language (not Facility Assessment).

## Tracking

Confirm deployed `<head>` includes both snippets exactly once:

- Microsoft Clarity: `https://www.clarity.ms/tag/yhnafqbr5k`
- OpenAI Ads pixel: `bzrcdn.openai.com/sdk/oaiq.min.js` plus `lead_created` with `submission_id` on successful form submit

Quick check: `curl -sL https://goanchorcleaning.com/ | rg 'yhnafqbr5k|oaiq|lead_created'`

## SEO

- `robots.txt` — allows crawling, references sitemap
- `sitemap.xml` — `/` and `/residential/`
- `assets/brand/site.webmanifest` — PWA manifest

After copying, confirm the deployed `<head>` includes **both** tracking snippets exactly once:

- Microsoft Clarity: `https://www.clarity.ms/tag/yhnafqbr5k` (PR #569)
- OpenAI Ads pixel: `bzrcdn.openai.com/sdk/oaiq.min.js` plus `trackOpenAiLeadCreated` / `lead_created` (PR #609)

Quick check: `curl -sL https://goanchorcleaning.com/ | rg 'yhnafqbr5k|oaiq'`

## Phone

Public number: `(603) 420-2430` → `tel:+16034202430`
