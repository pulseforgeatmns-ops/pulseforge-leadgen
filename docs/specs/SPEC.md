# SPEC — Current Max Workstream

**Active:** [SPEC-112 — Reasoning Pipeline Conformance](SPEC-112_Reasoning_Pipeline_Conformance.md) is **Implemented**. [SPEC-111 — Operator Intent Taxonomy](SPEC-111_Operator_Intent_Taxonomy.md) is **Implemented**. [SPEC-110 — Business Intelligence Synthesis](SPEC-110_Business_Intelligence_Synthesis.md) is **Implemented**. [SPEC-109 — Intent-Bound Response Selection](SPEC-109_Intent_Bound_Response_Selection.md) is **Implemented**. [SPEC-108 — Claim Grounding Competency Graduation](SPEC-108_Claim_Grounding_Competency_Graduation.md) is **Implemented**. [SPEC-107A — Recommendation Claim Grounding & Challenge](SPEC-107A_Recommendation_Claim_Grounding.md) is **Completed**.

Max has one operator reasoning pipeline. Intent selects analysis mode. Analysis mode selects the response contract. Retrieval, claim grounding, and business intelligence fill that contract. Blueprints are evidence. Specialists produce intelligence. `ResponseContract` is the only operator-facing composer. Unknown intent fails toward Retrieval, never Blueprint Advisory.

SPEC-111 still classifies diagnosis, unknown analysis, risk, and progress. SPEC-110 still synthesizes intelligence before evidence. SPEC-109 still selects the response contract from operator intent. SPEC-108 still requires supported operating-state claims.

**Next:** durable assimilation of verified operator corrections. That work introduces persistent knowledge after this reasoning competency. It is out of scope for SPEC-112.
