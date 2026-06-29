# Anchor Cleaning Email Templates: Real Prospect Review

Status: **Mapped for client 10 review; Emmett still disabled**

Generated: 2026-06-29

No email was sent. These samples were rendered offline through `utils/templateMerge.renderTemplate()` using live client 10 prospect/company rows. The selector now maps `client_id=10` verticals in `utils/sendingReadiness.js`: `law_firm -> anchor_law_firm_draft`, `accounting -> anchor_accounting_draft`.

Client 10 currently has `enabled_agents = [scout]`, so Emmett remains disabled. The readiness gate also still blocks live sending unless every check passes. Brevo domain auth passed during this render, but `brevo_sender_active` did not pass.

## Render set

Rendered 4 real prospects with real first names, verified email, mapped vertical, and non-DNC status. Evaluated 12 setter-visible client 10 prospects with no LIMIT.

## Client 10 business-name short values

All client 10 companies were evaluated with no LIMIT.

- Attorney Joseph Kelly Levasseur, PLLC -> Attorney Joseph Kelly Levasseur (high)
- Backus, Meyer & Branch, LLP -> Backus, Meyer & Branch (high)
- Bouchard, Kleinman & Wright, P.A. -> Bouchard, Kleinman & Wright (high)
- Buckley Law Offices, Manchester NH -> Buckley Law Offices, Manchester NH (high)
- Cohen & Winters, PLLC -> Cohen & Winters (high)
- Craighead & Martin, PLLC -> Craighead & Martin (high)
- Curtin Law Office -> Curtin Law Office (high)
- Dahar Law Firm -> Dahar Law Firm (high)
- George T. Campbell, Attorney at Law -> George T. Campbell, Attorney at Law (high)
- Horn Wright, LLP -> Horn Wright (high)
- Law Office of Manning Zimmerman & Oliveira PLLC -> Manning Zimmerman & Oliveira (high)
- Law Offices of Normand Higham -> Normand Higham (high)
- Moore Ames Law, PLLC -> Moore Ames Law (high)
- Morrison Mahoney LLP -> Morrison Mahoney (high)
- Niederman, Stanzel & Lindsey PLLC -> Niederman, Stanzel & Lindsey (high)
- Sekella Law, PLLC -> Sekella Law (high)
- Sheehan Phinney Bass & Green PA -> Sheehan Phinney Bass & Green (high)
- Stephen Law Group Injury Lawyers -> Stephen Law Group (low; flags: low_confidence_descriptor_stripped)
- Teale Law -> Teale Law (high)
- Tenn And Tenn, PA -> Tenn And Tenn (high)
- Ward Law Group, PLLC -> Ward Law Group (high)

## In-sentence before/after checks

Before uses the prior full-name merge. After uses `business_name_short`.

### Law Offices of Normand Higham

Short name: Normand Higham

Day 0:
- Before: Would a short walkthrough of Law Offices of Normand Higham be useful?
- After: Would a short walkthrough of Normand Higham be useful?
Day 4:
- Before: Is that accountability already clear with the crew cleaning Law Offices of Normand Higham?
- After: Is that accountability already clear with the crew cleaning Normand Higham?
Day 8:
- Before: Would you be open to comparing your current scope with what we'd recommend for Law Offices of Normand Higham?
- After: Would you be open to comparing your current scope with what we'd recommend for Normand Higham?

### Bouchard, Kleinman & Wright, P.A.

Short name: Bouchard, Kleinman & Wright

Day 0:
- Before: Would a short walkthrough of Bouchard, Kleinman & Wright, P.A. be useful?
- After: Would a short walkthrough of Bouchard, Kleinman & Wright be useful?
Day 4:
- Before: Is that accountability already clear with the crew cleaning Bouchard, Kleinman & Wright, P.A.?
- After: Is that accountability already clear with the crew cleaning Bouchard, Kleinman & Wright?
Day 8:
- Before: Would you be open to comparing your current scope with what we'd recommend for Bouchard, Kleinman & Wright, P.A.?
- After: Would you be open to comparing your current scope with what we'd recommend for Bouchard, Kleinman & Wright?

### Stephen Law Group Injury Lawyers

Short name: Stephen Law Group

Day 0:
- Before: Would a short walkthrough of Stephen Law Group Injury Lawyers be useful?
- After: Would a short walkthrough of Stephen Law Group be useful?
Day 4:
- Before: Is that accountability already clear with the crew cleaning Stephen Law Group Injury Lawyers?
- After: Is that accountability already clear with the crew cleaning Stephen Law Group?
Day 8:
- Before: Would you be open to comparing your current scope with what we'd recommend for Stephen Law Group Injury Lawyers?
- After: Would you be open to comparing your current scope with what we'd recommend for Stephen Law Group?

### Law Office of Manning Zimmerman & Oliveira PLLC

Short name: Manning Zimmerman & Oliveira

Day 0:
- Before: Would a short walkthrough of Law Office of Manning Zimmerman & Oliveira PLLC be useful?
- After: Would a short walkthrough of Manning Zimmerman & Oliveira be useful?
Day 4:
- Before: Is that accountability already clear with the crew cleaning Law Office of Manning Zimmerman & Oliveira PLLC?
- After: Is that accountability already clear with the crew cleaning Manning Zimmerman & Oliveira?
Day 8:
- Before: Would you be open to comparing your current scope with what we'd recommend for Law Office of Manning Zimmerman & Oliveira PLLC?
- After: Would you be open to comparing your current scope with what we'd recommend for Manning Zimmerman & Oliveira?

## Law Offices of Normand Higham

Prospect: Normand Higham
Email: info@nhattorney.com
Vertical: law_firm
Business name short: Normand Higham
Sequence: anchor_law_firm_draft
Readiness note: renderable, but not sendable while Brevo sender is inactive and Emmett is not enabled.

### Day 0

**Subject:** the office clients see before the meeting

Hi Normand,

Clients form an opinion of a law office before anyone opens a file. Reception, conference rooms, glass, and restrooms all speak first.

Legal offices have another concern too. Access instructions, document areas, and off-limits spaces can't be treated like an ordinary handoff. Anchor Cleaning works from a clear scope, with one person accountable when something needs attention.

Would a short walkthrough of Normand Higham be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 4

**Subject:** when the conference room is not reset

Hi Normand,

A missed wastebasket sounds minor. It feels different when a client is sitting beside it for a sensitive meeting.

The point of a cleaning plan isn't a longer checklist. It's knowing who owns the result, what spaces need special handling, and how a miss gets corrected without three calls.

Is that accountability already clear with the crew cleaning Normand Higham?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 8

**Subject:** who owns the miss?

Hi Normand,

When an office looks right, nobody discusses the cleaning. Good.

When it doesn't, someone at the firm loses time finding the crew, repeating the standard, and checking the correction. Anchor keeps that responsibility in one place.

Would you be open to comparing your current scope with what we'd recommend for Normand Higham?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 13

**Subject:** a reliable backup, on file

Hi Normand,

Even if your current arrangement is working, having a local backup matters when coverage slips or the office needs a deeper reset before clients arrive.

If you'd like, I can walk the space and give you a clear scope to keep on file. No pressure to change what is already working.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

## Bouchard, Kleinman & Wright, P.A.

Prospect: Chris Rennegarbe
Email: crennegarbe@bkwlawyers.com
Vertical: law_firm
Business name short: Bouchard, Kleinman & Wright
Sequence: anchor_law_firm_draft
Readiness note: renderable, but not sendable while Brevo sender is inactive and Emmett is not enabled.

### Day 0

**Subject:** the office clients see before the meeting

Hi Chris,

Clients form an opinion of a law office before anyone opens a file. Reception, conference rooms, glass, and restrooms all speak first.

Legal offices have another concern too. Access instructions, document areas, and off-limits spaces can't be treated like an ordinary handoff. Anchor Cleaning works from a clear scope, with one person accountable when something needs attention.

Would a short walkthrough of Bouchard, Kleinman & Wright be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 4

**Subject:** when the conference room is not reset

Hi Chris,

A missed wastebasket sounds minor. It feels different when a client is sitting beside it for a sensitive meeting.

The point of a cleaning plan isn't a longer checklist. It's knowing who owns the result, what spaces need special handling, and how a miss gets corrected without three calls.

Is that accountability already clear with the crew cleaning Bouchard, Kleinman & Wright?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 8

**Subject:** who owns the miss?

Hi Chris,

When an office looks right, nobody discusses the cleaning. Good.

When it doesn't, someone at the firm loses time finding the crew, repeating the standard, and checking the correction. Anchor keeps that responsibility in one place.

Would you be open to comparing your current scope with what we'd recommend for Bouchard, Kleinman & Wright?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 13

**Subject:** a reliable backup, on file

Hi Chris,

Even if your current arrangement is working, having a local backup matters when coverage slips or the office needs a deeper reset before clients arrive.

If you'd like, I can walk the space and give you a clear scope to keep on file. No pressure to change what is already working.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

## Stephen Law Group Injury Lawyers

Prospect: Robert
Email: robert@stephenlaw.com
Vertical: law_firm
Business name short: Stephen Law Group
Sequence: anchor_law_firm_draft
Readiness note: renderable, but not sendable while Brevo sender is inactive and Emmett is not enabled.

### Day 0

**Subject:** the office clients see before the meeting

Hi Robert,

Clients form an opinion of a law office before anyone opens a file. Reception, conference rooms, glass, and restrooms all speak first.

Legal offices have another concern too. Access instructions, document areas, and off-limits spaces can't be treated like an ordinary handoff. Anchor Cleaning works from a clear scope, with one person accountable when something needs attention.

Would a short walkthrough of Stephen Law Group be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 4

**Subject:** when the conference room is not reset

Hi Robert,

A missed wastebasket sounds minor. It feels different when a client is sitting beside it for a sensitive meeting.

The point of a cleaning plan isn't a longer checklist. It's knowing who owns the result, what spaces need special handling, and how a miss gets corrected without three calls.

Is that accountability already clear with the crew cleaning Stephen Law Group?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 8

**Subject:** who owns the miss?

Hi Robert,

When an office looks right, nobody discusses the cleaning. Good.

When it doesn't, someone at the firm loses time finding the crew, repeating the standard, and checking the correction. Anchor keeps that responsibility in one place.

Would you be open to comparing your current scope with what we'd recommend for Stephen Law Group?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 13

**Subject:** a reliable backup, on file

Hi Robert,

Even if your current arrangement is working, having a local backup matters when coverage slips or the office needs a deeper reset before clients arrive.

If you'd like, I can walk the space and give you a clear scope to keep on file. No pressure to change what is already working.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

## Law Office of Manning Zimmerman & Oliveira PLLC

Prospect: Michaila
Email: michaila@manningzimmermanlaw.com
Vertical: law_firm
Business name short: Manning Zimmerman & Oliveira
Sequence: anchor_law_firm_draft
Readiness note: renderable, but not sendable while Brevo sender is inactive and Emmett is not enabled.

### Day 0

**Subject:** the office clients see before the meeting

Hi Michaila,

Clients form an opinion of a law office before anyone opens a file. Reception, conference rooms, glass, and restrooms all speak first.

Legal offices have another concern too. Access instructions, document areas, and off-limits spaces can't be treated like an ordinary handoff. Anchor Cleaning works from a clear scope, with one person accountable when something needs attention.

Would a short walkthrough of Manning Zimmerman & Oliveira be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 4

**Subject:** when the conference room is not reset

Hi Michaila,

A missed wastebasket sounds minor. It feels different when a client is sitting beside it for a sensitive meeting.

The point of a cleaning plan isn't a longer checklist. It's knowing who owns the result, what spaces need special handling, and how a miss gets corrected without three calls.

Is that accountability already clear with the crew cleaning Manning Zimmerman & Oliveira?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 8

**Subject:** who owns the miss?

Hi Michaila,

When an office looks right, nobody discusses the cleaning. Good.

When it doesn't, someone at the firm loses time finding the crew, repeating the standard, and checking the correction. Anchor keeps that responsibility in one place.

Would you be open to comparing your current scope with what we'd recommend for Manning Zimmerman & Oliveira?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

### Day 13

**Subject:** a reliable backup, on file

Hi Michaila,

Even if your current arrangement is working, having a local backup matters when coverage slips or the office needs a deeper reset before clients arrive.

If you'd like, I can walk the space and give you a clear scope to keep on file. No pressure to change what is already working.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430

## Evaluated but not rendered

These setter-visible client 10 prospects were evaluated with no LIMIT, but were not included in the real-email render set because live template/send prerequisites are incomplete.

- Curtin Law Office: missing first_name, missing email, email not verified
- Moore Ames Law, PLLC: missing first_name
- Backus, Meyer & Branch, LLP: missing first_name, missing email, email not verified
- Attorney Joseph Kelly Levasseur, PLLC: missing email, email not verified
- Morrison Mahoney LLP: missing first_name, missing email, email not verified
- Horn Wright, LLP: missing first_name, missing email, email not verified
- Buckley Law Offices, Manchester NH: missing first_name
- George T. Campbell, Attorney at Law: missing email, email not verified

## Approval checklist

- [ ] Real-prospect law firm sequence reviewed.
- [ ] Day 13 law-firm subject approved: `a reliable backup, on file`.
- [ ] Accounting Day 0 copy approved: `worth 10 minutes?`.
- [ ] Recipient business-name mentions feel natural and do not repeat within an email.
- [ ] Anchor service/accountability claims approved.
- [ ] Signature email and phone approved.
- [ ] Offline render approved.

Approval of this document does not activate Emmett. Client 10 still has only Scout enabled, and live sending remains blocked by the Emmett readiness gate until the client agent config and Brevo sender checks are explicitly cleared.
