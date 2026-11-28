# MAX-P0-005 Package A — Review Traceability

This matrix is the acceptance map for PR #1168 Package A only.  It does not
claim Package B+ driver, scheduler, provider, or authorization expansion work.

| Finding / requirement | Durable implementation | Behavioral regression coverage |
| --- | --- | --- |
| R1 root uniqueness / migration safety | P005 root partial unique index and transactional preflight | R1-01 through R1-07; N4 duplicate/malformed-index tests |
| R2 canonical replay | `ObjectiveContract.admitObjectiveRoot` fingerprint replay | R2-01, R2-02 |
| R3 immutable accepted contract | immutable task input plus task-store guard | R3-01 through R3-05 |
| R4 root/child mission fencing | P0-004 ownership and P0-003 ledger fences | R4-01 through R4-07, NF-6 |
| R5 actor attribution / default deny | trusted `objectiveAdmissionAuthorization` boundary | R5-01 through R5-06, N1 |
| R6 provider/specialist/action governance | approved registries in `ObjectiveContract` | R6-01 through R6-06 |
| R7 wake/deadline/idle policy | canonical wake identity, durable wake index, bounded verification policy | R7-01 through R7-08; N2, N3 |
| R8 Package A default-off / no driver | dual feature gates and no timer/worker path | DEF-01, R7-08 |
| NF-1 authorization decision precision | trusted-policy object acceptance only | R5-01, R5-02 |
| NF-2 canonical root scope | root partial unique index | R1-04 |
| NF-3 bounded verification jitter | `nextVerificationWake` limits | R7-02, R7-03 |
| NF-4 replay and expiry precision | immutable replay and deadline handling | R3-04, R3-05, R7-04 |
| NF-5 provider retirement | closed Package A provider registry | R6-01, R6-02 |
| NF-6 root task action fencing | P0-003 task-fence requirement | R4-06 |
| N1 caller-controlled authorization | production policy-only admission | R5-02; unit override regression |
| N2 wake index fail-closed | catalog-verified wake uniqueness invariant | `N2 regression: wake registration fails closed when the deduplication index is absent or malformed` |
| N3 wake identity integrity | canonical root, contract reason/version, tenant/objective binding | `N3 regression: objective wakes accept only canonical root identities and contract reasons` |
| N4 migration/schema verification | duplicate preflight, definition validation, read-only report | N4 duplicate, malformed-index, and R1-07 report cases |
| N5 behavioral review quality | independent pools, executed report, actual policy and registry behavior | strengthened R1-07, R5-05, R6-06, R7-07 |
