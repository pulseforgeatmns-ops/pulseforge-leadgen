# Anchor Cleaning website

Source for [goanchorcleaning.com](https://goanchorcleaning.com/). GitHub Pages on `pulseforgeatmns-ops/anchor-cleaning` (`main` → `/`).

## Pages

| URL | Purpose |
|---|---|
| `/` | Commercial cleaning — primary CTA: **Request a Facility Assessment** |
| `/residential/` | Residential home cleaning — primary CTA: **Request Home Cleaning** |

## Lead forms

Both forms POST to `POST /api/public/walkthrough` on the Pulseforge app (legacy route name; unchanged for backend compatibility). Submissions write an `agent_actions` row for `client_id=10`.

Customer-facing commercial terminology uses **Facility Assessment**. Residential uses home-cleaning language (not facility assessment).

## Tracking

Confirm deployed `<head>` includes both snippets exactly once:

- Microsoft Clarity: `https://www.clarity.ms/tag/yhnafqbr5k`
- OpenAI Ads pixel: `bzrcdn.openai.com/sdk/oaiq.min.js` plus `lead_created` with `submission_id` on successful form submit

Quick check: `curl -sL https://goanchorcleaning.com/ | rg 'yhnafqbr5k|oaiq|lead_created'`

## SEO

- `robots.txt` — allows crawling, references sitemap
- `sitemap.xml` — `/` and `/residential/`
- `assets/og-social-preview.jpg` — shared Open Graph / Twitter preview image

## Phone

Public number: `(603) 420-2430` → `tel:+16034202430`
