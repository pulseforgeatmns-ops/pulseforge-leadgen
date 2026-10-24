# Glossary

| Term | Meaning |
|---|---|
| **Pulseforge** | The product and operating company running multi-client AI lead-gen / outreach |
| **Client** | Tenant row in `clients` (`client_id`); isolation boundary |
| **Prospect** | Person/contact in the pipeline (`prospects`) |
| **Company** | Organization entity (`companies`) linked to prospects |
| **Scout** | Lead discovery agent (`leadgen.js`) |
| **Max** | Manager intelligence agent — briefing today; reasoning engine directionally |
| **Emmett** | Outbound email agent (Brevo) |
| **Riley** | Inbound email triage + webhook handling |
| **ICP score** | Fit score used for ranking / setter visibility threshold (≥ 70 typical) |
| **Setter visible** | Prospect qualified into the setter queue |
| **DNC** | Do not contact — hard suppress |
| **Shadow mode** | System records decisions/intents without external side effects |
| **Outbox** | Durable outbound intent (`outbound_messages`); not proof of delivery |
| **Inquiry Foundation** | Inbound inquiry schema + workflow (local/shadow until authorized) |
| **Knowledge Graph (KG)** | Planned durable memory of entities, relations, provenance (SPEC-001) |
| **ADR** | Architecture Decision Record |
| **SPEC** | Implementation specification |
| **Warmth / orchestration** | Max scoring + lifecycle recommendation layer (shadow-default) |
| **Opportunity Intelligence** | Ranks which opportunities matter (SPEC-164) — not a lead score |
| **Strategic Decision** | Allocates finite hours/AOs toward the mix that maximizes the mission (SPEC-165) |
| **Approval** | Human gate before publish/send |
| **Vertical** | Industry/segment tag on prospects (not `industry` column) |
| **Anchor** | Client 10 — commercial cleaning buyer LLC, separate from Pulseforge lead-gen ICP |
| **MSHI** | Client 2 — Mountain State Home Innovations |
| **Command Center** | Operator attention UI for inquiries/work items |
| **CURRENT_STATE** | Repo heartbeat document for version/sprint/blockers |
