# P0 AO eligibility implementation contract (Issue #1060)

Status: design and regression-test baseline; **not** authorization to deploy or reassign.

## Existing divergent read paths (main at audit)
- `services/aoCrmService.js:fetchAccountRows`: assigned + DNC + prospect_motion, missing explicit review/disqualification.
- `services/aoCommandCenterService.js`: same partial gates.
- `services/aoProspectTaskService.js:listOpenTasks`: filters DNC and disposition, but maps review-required tasks to visible warnings.
- `utils/aoAccountFill.js`: capacity and candidate gates differ from CRM and task lists.
- `routes/ao.js`: additional detail/search/field/queue routes must be checked before claiming completeness.

## Non-negotiable semantics
1. Tenant isolation and authoritative admission precede AO visibility.
2. Duplicates, disqualified, review-required, suppressed, synthetic/test, and unadmitted prospects must never be in any AO working surface, even if previously assigned.
3. Preserve rejected and review-only records in non-AO administrative/audit stores; never delete history to hide it.
4. Channel-specific restrictions are distinct from global eligibility: call suppression alone does not necessarily prohibit a permitted non-call action. Confirm semantics before changing this filter.
5. No eligibility bypass via direct-detail IDs, search, task endpoints, metrics, or counts.
6. No assignment, outbound, paid research, or hold clearance as a side effect of a read or configuration change.
7. Anchor qualified/actionable default owner Tony (user 24), without hardcoding that user into shared eligibility logic.
8. Capability availability, readiness/configuration, and execution authority are separate. Do not modify Max implementation on this branch.

## Implementation prerequisites
- Identify authoritative duplicate, disqualification, admission, review, synthetic and status fields, including legacy fallbacks.
- Define one reusable SQL eligibility expression and equivalent JS evaluator where necessary, with reason codes and null-safe fail-closed semantics.
- Enumerate all AO working read paths and admission/assignment writers; apply the same predicate or an equivalent authorization guard.
- Write tests before changing runtime behavior.

## Required regression cases
- Unqualified, duplicate, review-required, DNC, suppressed, synthetic and unadmitted record excluded from every AO list/search/detail/task/count.
- Previously assigned but newly disqualified record disappears from AO views while history remains.
- Eligible record appears once and is routed only to an active AO of the same tenant.
- Independent channel suppression does not accidentally authorize forbidden calls or erase permitted non-call workflows.
- Cross-tenant access and direct-ID access denied; stale task rows cannot leak.
- Retry/recovery is idempotent; no auto-outreach, paid calls, or approvals are created.
- Existing Max orchestration tests remain untouched and pass.

## Release gates
Failing tests → minimal shared implementation → local/CI pass → read-only Anchor reconciliation → PR review → separate approval for deploy and production reassignment.
