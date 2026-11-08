# Max mission lifecycle hygiene — approved roadmap (2026-11-08)

Status: **Approved for roadmap; NOT authorized for implementation or production cleanup.**

## Operator outcome
Max continuously maintains a trustworthy mission portfolio across Pulseforge tenants, so operators see live business work instead of development residue. This is an operator responsibility, not a separate standalone product. Max proactively detects stale, duplicate, superseded, completed and likely test missions, and recognizes stalled legitimate missions with actionable blockers.

## Authority boundaries
1. **Autonomous, read-only classification:** inventory tenant-scoped missions, identify candidates, retain evidence and confidence, present an active view without changing underlying records. Never infer test status solely from age or name.
2. **Governed, reversible archival:** archive only under explicit approved policy with tenant-scoped checks, audit trail, retention period, recovery path and idempotency. Do not archive live research, prospecting, outbound, pending approvals, customer obligations or mission evidence based on heuristics alone.
3. **Human approval:** permanent deletion, destruction of evidence, retirement of missions with unresolved obligations, or exceptions to retention policies. Require review of exact mission IDs, consequences and dependencies; no blanket delete.

## P0 sequencing and acceptance
- **Do not implement during the current P0 freeze.** First restore and verify Max tenant context, routing, execution and evidence persistence; maintain Scout/outbound authorization gates.
- Then perform a **read-only, tenant-by-tenant mission inventory** with real-vs-test classifications and dependencies; review with operator before any mutation.
- Future implementation must prove cross-tenant isolation, no loss of audit history, correct handling of pending authorizations, reversible archive/restore, duplicate and stale detection, and no unintended execution or sends.
- Max should surface concise hygiene reports and proactively recover stalled missions only within pre-existing authority.

## Specific operator constraints
- Anchor Cleaning's Primrose School in Bedford, NH is excluded from prospecting while serviced by Tyler's Delta Cleaning; do not target absent evidence of a genuinely available opportunity and explicit operator review. This exclusion belongs in durable tenant prospect governance separately from mission cleanup.
- Current Anchor research-only grant and retained research evidence are operational, not disposable test data.
