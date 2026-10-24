# Draft PR: Repair bounded Pulseforge cashflow replenishment taxonomy

## Summary

Repair the verified tenant-1 $297 Missed Revenue Review replenishment blocker on main 697995ce. Use shared explicit plumbing/heating/HVAC aliases from discovery through preparation, preserve the reviewed NH city list, and prevent semantic planning from broadening the three-trade audience.

Keep Babrun's founder-led taxonomy and all other tenants' default enrichment lists unchanged. Require a validated reviewed tenant-1 source before selecting new trades for enrichment. Preserve manual-first-touch, ownership, suppression, contact verification/classification, sender-health, approval, and dispatch controls.

## Validation

- Core mocked regressions: 83/83
- Semantic/geography/startup/CI contracts: 94/94
- Max: 1,349/1,349
- Independent safety-focused validation: 67/67
- Wider AMO suite: 448 passed/41 failed on both the patch and an isolated untouched 697995ce baseline; all 41 failing test names and locations match under network-disabled validation. No new AMO failures; not a clean aggregate pass
- Broad application suite blocked by an untrusted external-site request; not retried, no full-suite claim

See `pulseforge-replenishment-taxonomy.md` for exact limits and the read-only production verification plan.

## Scope

Code-only; no dependencies, migrations, production writes, deployments, activation, or sends. Requires separately approved publication, exact-commit CI review, and normal source/program/contact approval before any production use.
