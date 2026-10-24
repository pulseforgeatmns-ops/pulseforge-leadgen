# ADR-107: Explicit tenant-scoped research-provider pause

Status: Proposed implementation, local only. Related [SPEC-265](../specs/SPEC-265_Anchor_Research_Provider_Pause.md).

A process-visible default-off control blocks new paid tenant 10 research/discovery/contact-enrichment/email-verification handoffs. Dedicated immutable async tenant context is captured at authoritative roots and cannot be replaced by provider attribution metadata. Missing or conflicting marked research scope rejects only under the explicitly documented armed diagnostic/operator contract. Other tenants and unrelated field-route geocoding, call/SMS and presentation-generation operations retain their behavior.

A typed control rejection must remain distinguishable from provider failure through retries, fallbacks and candidate persistence. It adds no budget, migration, endpoint, approval grant or secret management. Environment deployment has propagation/in-flight limits; it is not a transactional global stop or a zero-spend guarantee.
