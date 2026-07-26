# Product Constitution

Non-negotiable product rules. Specs may refine mechanisms; they may not violate these without a superseding ADR.

## 1. Human authority

Customer-visible messages and public posts require an explicit human approval path unless a later ADR documents a narrower, audited exception.

## 2. Explainability

Any score, recommendation, or automated state transition that influences operator action must leave an auditable explanation (components, evidence refs, or decision log).

## 3. Tenancy and privacy

Data is scoped by `client_id`. New paths fail closed on missing or mismatched client context. Secrets never appear in logs, events, or API responses.

## 4. Do-not-contact

`do_not_contact` and related suppressions are absolute. Agents check DNC before outreach.

## 5. Safety before scale

New automation that mutates lifecycle or sends externally ships **shadow / default-off** first. Production enablement is an explicit operator decision.

## 6. Source of truth

The repository documents product and engineering truth. Chat is ephemeral. CURRENT_STATE reflects the active sprint.

## 7. Backwards compatibility

Breaking changes to APIs, schema semantics, or client behavior require explicit approval in a spec and ADR.

## 8. Separation of philosophy and implementation

Vision docs define intent. Specs define build contracts. Code implements specs. Do not bury product doctrine only in code comments.

## 9. Named agents, clear roles

Agents have bounded responsibilities. Max reasons and recommends; specialist agents execute within their channel rules.

## 10. Truthful status

UI and docs must not claim delivery, booking, or coverage that the system cannot prove from durable records.
