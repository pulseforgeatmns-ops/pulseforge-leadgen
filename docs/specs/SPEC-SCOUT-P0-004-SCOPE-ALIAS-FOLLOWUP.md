# SPEC-SCOUT-P0-004: canonical research scope follow-up

Status: approved for implementation, testing and PR review only. No merge,
deployment, production flag change, grant installation or release, paid search,
website investigation, candidate promotion, reconciliation or outbound activity.

## Evidence and root cause

The controlled Tenant 10 cycle `moc_10_c5803ea2742281ef` completed on October 31,
2026 at 19:07:56 UTC on revision
`9a8bb6d4524860f448ec96da7b32b44abc416546`. It selected the Goffstown dealership
query, evaluated six businesses and retained five under the existing research
mission. Its bounded provider receipt also reported two
`facility_research_request_scope_mismatch` causes. The subsequent 19:15 UTC
clock attempt was refused because the two released cycles were consumed.
Private production exports are excluded from this repository.

The research mission authorizes `managed_facilities` and `restaurant_foh`.
The shared coverage engine also emits the canonical workload segments
`property_management` and `commercial_office`. The approved managed-facilities
concept library already contains commercial property management and office
parks, but the research planner compared workload labels literally to mission
segment strings and rejected those canonical workloads before HTTP.

The failing regression reproduces that exact exception for both canonical
labels. Existing successful school and dealership discovery does not mean the
entire provider or coverage pipeline is healthy.

## Bounded correction

Resolve only the two canonical workload labels to their `managed_facilities`
mission authority inside the Tenant 10 facility research planner. Both still
require that exact approved mission segment. A restaurant-only mission cannot
authorize them, and STR, realtor, raw property-manager aliases and other
unapproved labels remain rejected.

The planner continues to generate its entire query set from the approved
mission segments, existing concepts and cities. It does not pass the canonical
label or caller-supplied discovery concept to ordinary query expansion. Query
text, root identities, ranking, search generation, paid history and the
one-page allocation are unchanged. Later authorized task labels return no
second allocation, while invalid later tasks still fail their gates.

Reject a top-level `businessName` as entity work, alongside the existing
entity and non-identity checks. The real Places provider disables market
checkpoints when that field is present, so it must not enter this bounded
market planner. No entity investigation is authorized by this correction.

Classifier admission rules, mission contents, geographic restrictions, provider
denial diagnostics, grant approvals, atomic cycle consumption, spending
reservations, recovery maintenance, suppression and downstream controls are
unchanged. No migration or historical-data update is required.

## Validation

- Both canonical labels can begin allocation, but generate exactly the same
  approved query set, ignoring unapproved caller-supplied search concepts.
- Both require managed-facilities authority. Wrong tenant, geography, entity
  requests, evidence types and unapproved segments fail before a new call,
  including after the cycle already allocated its plan.
- The real Places adapter and checkpoint session run eight synthetic restarted
  passes, starting with either canonical label. They select eight distinct
  categories and towns, make eight simulated searches/reservations total, and
  repeated authorized workloads cannot reserve a second page.
- The disposable PostgreSQL native research journey observes the actual
  canonical workloads, retains suitable facilities, excludes historical
  markers, preserves bounded provider diagnostics, and refuses an unreleased
  next cycle without spending. Protected recovery, suppression, source mission,
  provider policy, prospect and outbound tables remain unchanged.
- Existing per-cycle release concurrency, retry, expiry, revocation, revision,
  research-only and outbound safety suites remain required. Tests use synthetic
  fixtures and external transport denial, never production credentials or paid
  providers. Required PostgreSQL checks must pass, not skip.

## Risks and rollback

The mapping is deliberately limited to the two observed canonical labels.
Future labels must be reviewed against approved mission concepts; do not use
the broader general business taxonomy to infer research authority. Shared
ordinary discovery behavior is unchanged. Removing these specific false scope
denials does not remove the one-page coverage limitation, optional provider
unavailability or the bounded receipt's completeness limit.

Rollback is a code revert with no schema or data rollback. Retain recovery
maintenance and the existing unreleased-cycle hold. A later deployment changes
the revision pin and must fail closed against the current grant. Do not rewrite
that grant, reset consumption, renew expiry, or resume research to compensate.
Any merge, deployment and fresh production activation require separate approval.
