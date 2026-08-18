# SPEC — Current Max Workstream

**Active:** None — Pilot 0 claim grounding graduated.

[SPEC-108 — Claim Grounding Competency Graduation](SPEC-108_Claim_Grounding_Competency_Graduation.md) is **Implemented**. [SPEC-107A — Recommendation Claim Grounding & Challenge](SPEC-107A_Recommendation_Claim_Grounding.md) is **Completed**.

Max now evaluates operating-state claims as supported, partially supported, or unsupported before recommending, and confirm / qualify / retract / revise under operator challenge. The behavior is domain-general (`claim_grounding` in the Competency Registry), not an email-specific patch.

SPEC-105 still owns operating-evidence retrieval. SPEC-106 still owns operator-attested persistence. Max-generated statements are not evidence and are not written into operator memory.

**Next:** durable assimilation of verified operator corrections. That work introduces persistent knowledge after this reasoning competency. It is out of scope for SPEC-108.
