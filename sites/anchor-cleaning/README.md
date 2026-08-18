# Anchor Cleaning website

Source for [goanchorcleaning.com](https://goanchorcleaning.com/). Live hosting is GitHub Pages on `pulseforgeatmns-ops/anchor-cleaning` (`main` → `/`).

## Deploy

Copy `index.html` over the Pages repo `index.html` and merge to `main`. Do not add `/commercial-cleaning-manchester-nh` until the first Search campaign has data.

## Phone

Public number: `(603) 420-2430` → `tel:+16034202430`

## Facilities assessment form

The form POSTs to `POST /api/public/walkthrough` on the Pulseforge app. Submissions write an `agent_actions` row for `client_id=10` and email Jacob when Brevo is configured. Public copy uses **facilities assessment**; backend route and analytics event names are unchanged.

If the API is unreachable, the page falls back to a prefilled mailto.

## Tracking before ads

Paste real IDs into `window.ANCHOR_ANALYTICS` in `index.html`:

| Field | What to paste |
|---|---|
| `ga4` | GA4 measurement ID (`G-…`) |
| `ads` | Google Ads tag ID (`AW-…`) |
| `formConversion` | Ads conversion label for form submits |
| `callConversion` | Ads conversion label for click-to-call |

Until those are set, the page still emits `dataLayer` events:

- `walkthrough_form_submit`
- `phone_click`
- `email_click`

Call asset / Google forwarding-number conversion is configured in Google Ads, not on this page. Click-to-call is tracked here so a website-call conversion can fire as soon as the label exists.
