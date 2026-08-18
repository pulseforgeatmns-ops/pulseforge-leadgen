# NEXT STEP

SPEC-111 is implemented: Max classifies operator intent into an explicit analysis-mode registry before reasoning. Diagnosis, unknown analysis, risk, and progress have their own contracts and consume Business Intelligence objects. Recommendation remains recommendation-first. SPEC-110 intelligence synthesis remains in force. SPEC-109 intent-bound contracts remain in force. SPEC-108 claim grounding remains graduated. SPEC-107A is Completed.

Recommended next work, in order:

1. **Durable assimilation of verified operator corrections** — persist *verified* working-model corrections after claim grounding, without treating Max-generated statements as operating fact. This is the next knowledge-layer milestone; do not start it by weakening SPEC-108, SPEC-109, SPEC-110, or SPEC-111.
2. **CIE typo-repair cleanup** — `app` → `gap` still inflates `unknowns` on CIE-claimed turns. This is no longer a Pilot blocker. Fix it as a narrow CIE semantic-contamination patch.
3. **Event C confirmation UX** — SPEC-106 v1 fail-closes on silent AO cohort mutation. If operators confirm follow-up scheduling, write `ao_follow_up_tasks` / `next_follow_up_date` behind an explicit confirmation turn.
4. **Event B durability (P2 only)** — persist AO training/readiness only if Pilot usage shows a semantically correct store.
5. **Contradiction presentation** — operator-attested vs system-observed mail dates already fail open as conflict. Surface that distinction in existing provenance UI when those components can represent it cleanly.

Do not treat SessionStore transcripts as operating memory.
Do not convert SPEC-107 recommendations into missions or agent enablement automatically.
Do not persist retracted Max-generated premises as SPEC-106 operating facts.
Do not persist SPEC-110 intelligence objects as operating fact.
Do not add graph learning or new memory systems until claim grounding, intent-bound response selection, business-intelligence synthesis, and operator intent taxonomy remain green.
