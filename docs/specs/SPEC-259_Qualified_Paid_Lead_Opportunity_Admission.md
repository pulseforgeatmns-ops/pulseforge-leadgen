# SPEC-259 — Qualified Paid Lead → Canonical Revenue Opportunity Admission

## Purpose

Bridge executed `QUALIFIED_BY_OPERATOR` qualification reviews (SPEC-258) into
canonical revenue opportunities via an explicit second operator admission action.

QUALIFY alone sets `opportunityCreationReady=true` but does **not** create an
opportunity. Admission requires operator-supplied `estimatedValueCents` and
`serviceType`.

## Entry

`POST /api/v1/lead-qualification-reviews/:id/opportunity`

## Non-goals

Stage advancement beyond `identified`, customer/job/payment creation, won/lost
automation, platform reconciliation, internal campaign registry, CAC/ROAS/LTV.
