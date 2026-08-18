# Anchor Cleaning website

Source copy of [goanchorcleaning.com](https://goanchorcleaning.com/). The live site is GitHub Pages on `pulseforgeatmns-ops/anchor-cleaning` (`main` → `/`).

## Deploy

Commit and push `index.html` to `pulseforgeatmns-ops/anchor-cleaning` on `main`. That repo is the deploy. Do not ask anyone to copy files in the GitHub UI.

This directory stays in sync as the working copy. A site change is not shipped until it is pushed to the Pages repo.

The Cursor GitHub App must include `pulseforgeatmns-ops/anchor-cleaning`. If `cursor[bot]` gets a 403, add that repo to the app installation and push — do not fall back to a manual copy.

Do not add `/commercial-cleaning-manchester-nh` until the first Search campaign has data.

## Phone

Public number: `(603) 420-2430` → `tel:+16034202430`

## Facilities assessment form

The form POSTs to `POST /api/public/walkthrough` on the Pulseforge app. Submissions write an `agent_actions` row for `client_id=10` and email Jacob when Brevo is configured. Public copy uses **facilities assessment**; backend route and analytics event names are unchanged.

If the API is unreachable, the page falls back to a prefilled mailto.

## Tracking

GA4 measurement ID `G-LCOWW1SO7N` is installed as the official gtag snippet in `<head>`. Paste remaining Ads IDs into `window.ANCHOR_ANALYTICS`:

| Field | What to paste |
|---|---|
| `ga4` | Already set (`G-LCOWW1SO7N`) |
| `ads` | Google Ads tag ID (`AW-…`) |
| `formConversion` | Ads conversion label for form submits |
| `callConversion` | Ads conversion label for click-to-call |

Until those are set, the page still emits `dataLayer` events:

- `walkthrough_form_submit`
- `phone_click`
- `email_click`

Call asset / Google forwarding-number conversion is configured in Google Ads, not on this page. Click-to-call is tracked here so a website-call conversion can fire as soon as the label exists.
