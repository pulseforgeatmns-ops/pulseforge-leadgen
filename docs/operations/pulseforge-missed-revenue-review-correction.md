# Pulseforge tenant 1: Missed Revenue Review correction

## Status and publication scope

This is a code-only correction prepared against main 1b2ba8f44f414badae43419c3e881571ae1596ce (PR #907). It is not a production migration or sending authorization. No production state, mailbox, program, migration, touchpoint, or provider send was changed while preparing it.

Recommended publication: a separate draft PR containing this correction and regression tests, after operator approval to publish. Do not combine it with #906, merge, deploy, activate a mailbox/program, or execute bootstrap without separate approval. Review required CI on the exact published commit before considering merge.

## Corrected offer

- Pulseforge, Jacob, jacob@pulseforge.com.
- $297 Missed Revenue Review for southern New Hampshire plumbing, heating and HVAC businesses.
- Public inquiry/booking review; existing follow-up workflow only with permission.
- Up to three actionable improvements, a prioritized plan, and a 30-minute walkthrough.
- Implementation quoted separately. No revenue promises, invented findings, unapproved private access, fixed turnaround, or upfront-payment policy.
- New offer identity: pulseforge_missed_revenue_review_v1.

Copy is a neutral invitation; it does not claim a contact form was tested or a defect/revenue loss was found. Prior public assessments are not rewritten, but a repair-defect diagnosis is no longer an invitation requirement for this diagnostic service.

## Prior manual outreach

The operator reported October 13 manual first touches to Done Right, Overflow, and Clear All. Their exact provided email addresses and website domains are retained in the scoped exclusion receipt in utils/pulseforgeCashflowOffer.js. No message body or delivery receipt was supplied to this correction: do not invent one, infer a price from the new offer, or rewrite existing manual touchpoints/copy.

Exclusions apply to tenant 1's new automated first touches only. Match exact normalized emails or the supplied company domain/subdomains, including www and URL aliases. They are checked at inventory/Paige preparation and the existing governed candidate boundary used for dispatch and scheduled sends. There is no global DNC change, shared Gmail-domain block, name-based fuzzy block, inbound restriction, or new follow-up automation. Existing opt-out/suppression rules still apply.

A rejected scheduled first touch is terminal and definitely unsent when the provider boundary was not crossed. It is not automatically retried or classified as uncertain delivery.

## Review and immutability

Changing a constant does not upgrade an approved source, daily mission, copy asset, envelope, or program policy. Old or incompatible sources fail closed before Max research, authorization, activation, preparation, and scheduled execution; stale copy is also checked immediately before sending. Existing source/policy hashes remain intact.

The bootstrap path now only reads tenant 1 and returns an existing current-offer source unchanged or creates a new missionPlanDraft. It does not overwrite tenant settings, freeze the plan, mark it ready, replay historical shared SQL triggers, or refresh old approval timestamps. Read errors propagate. A new draft must pass the normal plan/discovery/artifact reviews.

The conservative city list is part of the draft and requires review. Admission requires an allowed city and NH/New Hampshire in the same recorded location, rather than an older generic service_area_match=true flag. The list can be inspected with:

    npm run pulseforge:cashflow:review

This command is read-only. It is not a production readiness claim.

create-program requires an explicit reviewed source mission ID. Its first response is a policy review; it no longer accepts its own returned hash. A subsequent explicitly approved call must retain the reviewed startsAt/expiresAt and review hash. Changed Emmett bounds force a fresh review. Activation requires the exact reviewed program ID and policy hash. The old activation confirmation token no longer works.

## Required before any production activation

1. Verify live deployment, tenant-1 sending flags, program/mode, mailbox verification, schema, scheduled items, and actual source IDs through authenticated production access. This work did not independently establish them.
2. Review the code and publish only after approval. Do not automatically run historical trigger migrations as part of deploying this correction.
3. With approval, reconcile stale live tenant profile fields (such as industry/lead_with) separately and minimally. Preserve manual history and approved historical sources. This patch intentionally does not issue a tenant UPDATE.
4. Resolve the existing Scout taxonomy gap before enabling replenishment. Standalone plumbing/heating are not present in the shared canonical enrichable taxonomy; using the new eligibleSubsegments with current missionCompatibleVerticals can reject all replenishment candidates. Add and test a tenant-specific discovery/enrichment mapping without broadening other tenants' defaults. Do not represent this patch as activation-ready.
5. Create/review a fresh source draft for the corrected offer and exact geography; validate current tenant-bound sender, verified contacts, exclusions, and recipient-bound copy. Do not reuse $500 approvals, envelopes, or program scope hashes.
6. Review any replacement shadow program and handling of existing tenant-1 queued items. Confirm current authorized sender/capacity, send window, finite cap/expiry, and exact policy hash. No standing send authority is inferred from approving this offer.
7. Obtain explicit approval for production changes and external communications before activation. Keep all existing kill switches, DNC, suppression, reply, and mailbox health gates.

## Validation

Run focused safety tests with a non-production database sentinel:

    DATABASE_URL=postgresql://localhost:1/pulseforge_test_only DATABASE_SSL=false npm run test:pulseforge-cashflow

Focused tests cover new scope/copy, unapproved draft semantics, existing reviewed-source immutability, read-error propagation, old-token refusal, stale source/copy rejection, exact manual-contact exclusions, tenant isolation, geography ambiguity, pre-research stop, and definitely-unsent durable holds. The focused test is also included in the outbound PR CI job.

See the accompanying local validation report for actual run results and unrun checks. No production acceptance test or live send is included.
