# SPEC — Current Max Workstream

**Active:** [SPEC-107 — Evidence-Grounded Recommendation Orchestration](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md)

SPEC-107 establishes the orchestration contract:

`retrieve → reason → recommend`

for recommendation requests that require durable operating evidence.

It does not authorize execution. SPEC-105 still owns operating-evidence retrieval and epistemic classification. SPEC-106 still owns operator-attested persistence. CIE remains authoritative for durable business understanding and must not swallow an evidence-grounded recommendation after that evidence has been retrieved.
