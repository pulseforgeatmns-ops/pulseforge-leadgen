# NEXT STEP

SPEC-107 is implemented: recommendation requests that require operating evidence now retrieve that evidence and reason over it instead of returning inventory.

Recommended next work, in order:

1. **CIE typo-repair cleanup** — `app` → `gap` still inflates `unknowns` on CIE-claimed turns. This is no longer a Pilot blocker. Fix it as a narrow CIE semantic-contamination patch.
2. **Event C confirmation UX** — SPEC-106 v1 fail-closes on silent AO cohort mutation. If operators confirm follow-up scheduling, write `ao_follow_up_tasks` / `next_follow_up_date` behind an explicit confirmation turn.
3. **Event B durability (P2 only)** — persist AO training/readiness only if Pilot usage shows a semantically correct store.
4. **Contradiction presentation** — operator-attested vs system-observed mail dates already fail open as conflict. Surface that distinction in existing provenance UI when those components can represent it cleanly.

Do not treat SessionStore transcripts as operating memory.
Do not convert SPEC-107 recommendations into missions or agent enablement automatically.
