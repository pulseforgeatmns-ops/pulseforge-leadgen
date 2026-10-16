# Studio Substral governed activation

Tenant 17 uses `hello@studiosubstral.com`, mailbox integration
`tmi_17_substral_hello`, and sending identity `tsi_17_substral_hello`.
Mailbox verification, a shadow program, and live sending are separate steps.
This code change does not activate production, change a mission, or send mail.

## Review an existing mission and explicit authority

Use an existing tenant-17 mission that is approved/immutable and at Ready. Do
not call the legacy tenant bootstrap to select or create a mission: its paid
website-assessment objective is not the approved nationwide, established-business,
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

The `test/anchorDailyOutboundSubstral*.test.js` files are included by the
existing `npm run test:anchor-outbound` wildcard and CI. They cover policy and
identity isolation, read-only scope review, stale approvals, repeated commands,
Ready/cancellation gates, capacity producer shape and governor states, and the
fail-closed copy boundary. Mailbox OAuth, tenant, Paige, execution-clock and
governed-contract regressions remain applicable. PostgreSQL integration requires
the existing disposable PostgreSQL harness in an environment permitting its
local Unix socket.


## Preparation-only release and approval packet (2026-10-16)

This patch is local preparation, based on main `bb996bcd`. It does not prove
current production state. No production queries, mailbox tests, paid provider
requests, publication, deployment, or configuration writes were performed.
Historical observations of missing program/flags and runtime tenants `10,13`
are not a fresh status check. Do not bypass denied database access.

### Smallest next approval

Approve publication of the reconciled patch as a **draft PR only**, with CI.
Merging, deploying and enabling preparation require a separate explicit decision.
Do not describe the current artifact as production-ready or live.

### Required before any preparation activation

1. Obtain authorized fresh read-only evidence for the deployed revision, existing
   tenant-17 program, runtime tenant list, all three switches below, mailbox and
   identity state, suppression triggers, and the exact source mission. An existing
   grant must be reviewed, never overwritten by the create command.
2. Review a Ready, immutable tenant-17 mission for established businesses across
   the United States and $2,000+ website-redesign projects. Preserve evidence URLs
   and observations for website need, established operations and budget-fit
   indicators. Indicators do not establish an actual budget, intent or demand.
   Exclude unsubstantiated candidates; do not invent need or claim promised leads.
   Paid assessment or regional-only missions require a separately approved mission
   correction. This patch does not make that correction automatically.
3. Existing reviewed Paige messages and Emmett artifacts must be bound to that
   exact mission scope, candidate, company, email and canonical sender. No raw
   inventory copy fallback, new discovery, enrichment or model generation is
   authorized by this grant. If artifacts are missing, preparation remains blocked.
4. Supply the real source mission ID, authorizing operator, exact start/expiry
   timestamps (maximum 30 days), daily/lifetime item ceilings, contact classes,
   spacing and business window. A conservative proposal is 2 prepared items/day,
   10 total over seven days, verified founder email only, 240-minute spacing and
   09:00–17:00 America/New_York. These are proposed ceilings, not approved values.
5. Include `preparationOnly: true` in the policy file. The review hash binds this
   restriction, all authority fields and the mission scope. Use the canonical
   tenant `17`, sender `hello@studiosubstral.com`, integration
   `tmi_17_substral_hello`, and identity `tsi_17_substral_hello`.

### Exact scope of a later activation decision

After the above evidence and exact review hash are supplied, approval must cover:

- Deploying the reviewed release revision and creating only its reviewed shadow
  program (including existing schema checks and Emmett capacity-envelope writes).
- Adding `17` to the freshly observed runtime tenant list while preserving every
  existing entry. If the fresh value is exactly `10,13`, the new value is `10,13,17`.
- Setting `SUBSTRAL_GOVERNED_PREPARATION_ENABLED=true`,
  `SUBSTRAL_GOVERNED_OUTBOUND_ENABLED=false`, and
  `GOVERNED_OUTBOUND_TENANT_17_ENABLED=false`.
- Running preparation only within the approved caps/dates, producing frozen,
  unapproved envelopes from existing reviewed artifacts. Expired and unsent items
  still consume the lifetime preparation cap; daily rollover cannot reset it.

There is no live-send activation in this packet. A preparation-only program cannot
be promoted to active, stripped of its restriction by capacity migration, or used
by dispatch. Subsequent sending needs a separately reviewed authority workflow.
Legacy autosend must remain false. Stop on stale scope/hash, missing artifacts,
blocked mailbox/governor, invalid flags or exhausted budgets; do not loosen gates.
A rollback proposal is to switch preparation off and pause the program, retaining
its audit trail and prepared rows. Obtain approval for production changes.

### Verification of this local reconciliation

- Final non-PostgreSQL regression run: **188 passed, 2 skipped, 0 failed**.
  Includes all daily-outbound tests, context/contract guards, CI selection,
  provider diagnostics, Babrun enrichment evidence, and global startup checks.
- The two skips are disposable PostgreSQL suites. An attempted PostgreSQL 17 run
  failed at local Unix-socket startup; the escalation environment also failed
  before execution. No fresh PostgreSQL pass is claimed for this revision.
- `git diff --check` passed. Remote CI has not run because the patch is unpublished.
- Require the PostgreSQL-enabled `npm run test:anchor-outbound` suite to pass on
  this exact patch before merging. Earlier PostgreSQL evidence for the prior
  preparation commit does not replace this release's check.

- Review correction: the first preparation envelope now checks current planning
  infrastructure and a proceed/slow governor before preparing or freezing rows,
  and applies the current planning-capacity ceiling. Regression cases verify
  thrown/explicit governor halt, missing authority, zero capacity, no committed
  preparation budget on rejection, and a reduced live capacity of one item.
  This closes the first-envelope path that previously froze reviewed artifacts
  before the refill infrastructure check.

- Partial-success reporting preserves the committed prepared-item delta and known
  pending count when a later refill infrastructure check fails. A two-call test
  verifies first-check success followed by governor halt: one frozen item, one
  consumed budget unit, and matching response/audit counts. A retry adds zero.
