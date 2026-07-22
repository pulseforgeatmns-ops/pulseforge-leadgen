# Revenue Leak Scorecard — setup notes

Lean public funnel for qualifying cleaning-business owners for a PulseForge Revenue Recovery Assessment.

## URLs

| Path | Purpose |
|------|---------|
| `/scorecard` | Landing |
| `/scorecard/form` | Multi-step scorecard |
| `/scorecard/results` | Result + CTAs |
| `POST /api/public/scorecard` | Capture + score (no auth) |

## What was reused

- Express + `public/` HTML (same pattern as `/login` / `/mockups`)
- Brand tokens: `/shared/tokens.css` (Bebas Neue + DM Sans)
- Lead surface: `agent_actions` with `action_type = 'scorecard_lead'`, `client_id = 1` (PulseForge)
- Booking CTA: `https://calendly.com/jacob-gopulseforge/new-meeting` (override with `SCORECARD_BOOKING_URL`)

## What was not built

- Checkout for the $29 kit (CTA notes interest only)
- Email automation / Brevo contact sync
- Analytics dashboard or admin portal
- New database, CMS, or design system

## Capture integration point

All persistence goes through one function:

`lib/scorecardCapture.js` → `captureScorecardLead(answers, result)`

Extend dual-writes (Brevo, dedicated table, Stripe) **only** inside that module.

Payload fields of note: `result_category`, `high_intent`, full answers, `marketing_consent` (defaults unchecked).

## Scoring rules (summary)

1. **Call Recovery Gap** if after-hours process is weak (`voicemail_only` / `inconsistent` / `none`) **or** missed-call text is `no` / `sometimes`
2. Else **Quote Follow-Up Gap** if quote speed is `3_plus_days` / `rarely` **or** follow-up count is `0` / `1`
3. Else **Review Growth Gap** if automatic review request is `no` / `sometimes`
4. **High intent** (assessment primary CTA) if monthly inquiries are `31-75` / `76+` **or** system is `jobber_servicem8` / `crm`
5. Otherwise primary CTA is the $29 kit path; assessment remains available as secondary

## Local check

```bash
node --test test/scorecardScoring.test.js test/scorecardRoutes.test.js
# with server running:
# open http://localhost:PORT/scorecard
```

## Manual pre-launch checklist

- [ ] `/scorecard` loads on mobile width; headline + single “Get my score” CTA visible
- [ ] `/scorecard/form` shows progress; one question per step; Back/Continue work
- [ ] Skipping a radio step shows a required-field error; contact step rejects bad email/phone
- [ ] Marketing consent checkbox starts **unchecked**
- [ ] Submit Call Recovery path (`missed_call_text=no`) → result title Call Recovery Gap
- [ ] Submit Quote path (strong call answers, `quote_follow_up_count=0`) → Quote Follow-Up Gap
- [ ] Submit Review path (strong call+quote, `automatic_review_request=no`) → Review Growth Gap
- [ ] High intent (`monthly_inquiries=31-75` or Jobber/CRM) → assessment Calendly is primary CTA
- [ ] Standard intent → $29 kit block + secondary assessment CTA; Calendly opens `jacob-gopulseforge/new-meeting`
- [ ] Operator DB: latest `agent_actions` row `action_type=scorecard_lead`, `client_id=1`, payload has full `answers`, `result_category`, `high_intent`
- [ ] Honeypot: POST with `company_website` filled returns empty 204 and no new row
- [ ] Forced capture failure (optional) returns generic 500 copy with no stack/DB text

## Env (optional)

- `SCORECARD_BOOKING_URL` — assessment Calendly (or other) URL
- `SCORECARD_KIT_URL` — future kit checkout / waitlist URL
