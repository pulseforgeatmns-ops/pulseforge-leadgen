# Anchor Cleaning website

Source for [goanchorcleaning.com](https://goanchorcleaning.com/). GitHub Pages on `pulseforgeatmns-ops/anchor-cleaning` (`main` → `/`).

## Terminology

**Canonical customer-facing commercial term:** Facility Assessment (singular)

**Legacy internal API terminology:** walkthrough

The public form POSTs to `POST /api/public/walkthrough` and analytics may still emit `walkthrough_form_submit`. Those names are internal compatibility only — do not use "walkthrough" in customer-facing copy.

Use **facility assessment** / **Facility Assessment** in prose and CTAs as grammatically appropriate. Do not use "facilities assessment".

## Brand identity / link previews

**Canonical brand mark:** approved gold Anchor polo logo

**Status:** `TEMPORARY_PENDING_CANONICAL_LOGO` — the exact polo logo file is **not** in this repository.

| Asset | Current interim file | Canonical source (pending) |
|---|---|---|
| Favicon ICO/SVG/PNG | `assets/brand/favicon-v20260915.*` | Export from `assets/brand/anchor-logo-canonical.svg` |
| Apple touch icon | `assets/brand/apple-touch-icon-v20260915.png` | Same |
| Web manifest icons | `assets/brand/icon-192/512-v20260915.png` | Same |
| Open Graph / Twitter | `assets/brand/social-preview-v20260915.jpg?v=20260915` | Rebuild with polo logo on navy/cream |
| JSON-LD `logo` | Omitted until canonical file exists | `https://goanchorcleaning.com/assets/brand/anchor-logo-canonical.png` |

Interim favicons are solid navy placeholders (no anchor mark). Interim social preview is typography-only (no logo). Do **not** redraw or use the inline header clip-art SVG for social/favicon assets.

See `assets/brand/CANONICAL_LOGO_PENDING.md` for the required source file and replacement checklist.

**Cache busting:** HTML references use `?v=20260915`. Bump version in filenames and query strings when replacing artwork.

**Platform profile avatars** (Facebook, Instagram, LinkedIn, GBP, Yelp) must be updated manually on each platform.

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
- `assets/brand/site.webmanifest` — PWA manifest (interim icons)

## Phone

Public number: `(603) 420-2430` → `tel:+16034202430`
