# Studio Substral governed activation

Tenant 17 uses `hello@studiosubstral.com`, mailbox integration
`tmi_17_substral_hello`, and sending identity `tsi_17_substral_hello`.
Mailbox verification, a shadow program, and live sending are separate steps.
This code change does not activate production, change a mission, or send mail.

## Review an existing mission and explicit authority

Use an existing tenant-17 mission that is approved/immutable and at Ready. Do
not call the legacy tenant bootstrap to select or create a mission: its paid
website-assessment objective is not the approved southern-New-Hampshire,
$2,000+ website-redesign acquisition scope. Correcting or approving a live
mission is a separate operator decision through the canonical mission flow.

Prepare a JSON policy file containing all of these explicit values:

- `sourceMissionId`: the reviewed existing tenant-17 mission ID
- `startsAt` and `expiresAt`: fixed ISO timestamps, no more than 30 days apart
- `dailyCap` and `totalCap`: operator-approved ceilings, not sending targets
- `spacingMinutes`: 60–240 minutes
- `startHour` and `endHour`: within 09:00–17:00 America/New_York
- `allowedContactClassifications`: a nonempty reviewed selection of
  `VERIFIED_FOUNDER_EMAIL` and/or `VERIFIED_ROLE_EMAIL`
- Optional `operatorDelegatedMaximumDailyCapacity`: an explicitly reviewed
  outer daily ceiling within the existing policy bounds

Sender, mailbox, identity and tenant are fixed to Studio Substral. If present
in the file they must match. Unknown or unverified contacts are never enabled
by a contact-classification choice. Existing verification, tenant binding,
DNC, suppression, duplicate and prior-contact checks still apply.

```sh
node scripts/studioSubstralMailboxActivation.js review-governed-program \
  --policy-file=/path/to/reviewed-policy.json
```

This command runs a tenant-scoped SELECT only. It does not bootstrap a tenant,
change a mission, verify credentials, produce a capacity envelope, or create a
grant. Review the returned `sourceMissionScope` (especially objective, offer,
geography and constraints), normalized policy, and `reviewHash`. The hash binds
the exact policy and mission scope. This is a proposal, not authorization to
send, and it does not establish current production deliverability or inventory.

## Create a shadow program only after approval

After the operator approves that exact policy and source scope:

```sh
node scripts/studioSubstralMailboxActivation.js create-governed-program \
  --policy-file=/path/to/reviewed-policy.json \
  --review-hash=<approved-review-hash> \
  --confirm=substral-mailbox-2026-09-30
```

This is a production write when run against production. It produces an Emmett
capacity envelope and may run the existing schema checks, then creates a shadow
program through governed authorization. It never calls a send/tick or sets a
program active. `emmett-readiness` is also writeful; do not use it as a read-only
status check.

Missing/stale review hashes, cancelled or non-Ready missions, a blocked or
zero-capacity governor, and existing non-revoked programs are rejected. An
existing grant must be inspected separately; retries cannot silently replace
or demote it. Capacity uses the producer's governor-adjusted `maxSendsPerDay`
without rewriting the operator's reviewed daily/total authority.

Tenant-17 raw-inventory refill is deliberately held with
`tenant_specific_paige_copy_required`. Prepared Paige candidates continue through
the existing copy, recipient, DNC and suppression checks. The generic Anchor copy
fallback cannot generate Studio Substral refill messages. A tenant-specific
reviewed-copy path is required before raw-inventory refill can be enabled.

## Separate live rollout checks

Before considering live authorization, verify the deployed revision, exact
approved source mission, tenant-17 migration, stored delivered SPF/DKIM/DMARC
evidence and app-transport provenance, active mailbox/identity, eligible verified
inventory and reviewed copy, manual Wave 001 dedupe, fresh reply polling, and
the reviewed grant. A test arriving in one inbox does not establish general
inbox placement. Do not send another verification email without approval.

Live operation additionally requires explicit tenant-17 inclusion in
`GOVERNED_OUTBOUND_TENANT_IDS`, its separate enablement switch, and an active
program. This change leaves all of those values untouched. Approve their exact
values and sending scope separately; existing caps, expiry, weekday/window,
spacing, mailbox health and suppression gates remain mandatory.

## Regression coverage

The three `test/anchorDailyOutboundSubstral*.test.js` files are included by the
existing `npm run test:anchor-outbound` wildcard and CI. They cover policy and
identity isolation, read-only scope review, stale approvals, repeated commands,
Ready/cancellation gates, capacity producer shape and governor states, and the
fail-closed copy boundary. Mailbox OAuth, tenant, Paige, execution-clock and
governed-contract regressions remain applicable. PostgreSQL integration requires
the existing disposable PostgreSQL harness in an environment permitting its
local Unix socket.
