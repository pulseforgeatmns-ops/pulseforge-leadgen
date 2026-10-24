# ADR-104 — Instrumented Provider Boundaries

Status: Proposed for authorized local implementation; no production approval. Date: 2026-10-19.

Combined-artifact metadata overlay: this boundary decision is ADR-104 here to avoid the baseline/draft-PR955 ADR-102 identifiers. The separately frozen boundary artifact retains its original ADR-102 filename and patch SHA256 aa620f81bab57ec724d941ecb78b4ec2c8d47a4aab6bff60615f0ab8ac2a8d29. Only the local decision identifier/path and linked metadata are renumbered; the frozen dependency is unchanged.

Context: an adapter that includes authorization is not an irreversible network leaf. Eager marking made a pre-provider missing-column error appear as provider uncertainty.

Decision: retain conservative marking by default. Only the default-disabled tenant-10 governed Brevo wrapper may declare the concrete shared provider instrumentation version. Defer its mark to immediately before HTTP. HTTP errors and COMMIT errors after HTTP remain crossed. Do not infer historical provider acceptance from error codes, absence of receipts, or new code.

Consequences: adds accurate diagnostics for new scoped attempts; does not add columns, bypass admission, change SMS/calls, restore throughput, or reconcile history. Separate recovery design and production approval are required.
