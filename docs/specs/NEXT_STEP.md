# NEXT STEP

SPEC-106 is implemented: Max can accept operator-reported operating updates, persist eligible Evidence/Claim records, and recover them through SPEC-105 on a fresh workspace.

Recommended next work, in order:

1. **CIE typo-repair cleanup** — `app` → `gap` still inflates `unknowns` on CIE-claimed turns. SPEC-106 now intercepts the Anchor Pilot operating-update path, so this is no longer a Pilot blocker. Fix it as a narrow CIE semantic-contamination patch, not a SPEC-106 change.
2. **Event C confirmation UX** — v1 fail-closes on silent AO cohort mutation. If operators confirm follow-up scheduling, write `ao_follow_up_tasks` / `next_follow_up_date` behind an explicit confirmation turn.
3. **Event B durability (P2 only)** — persist AO training/readiness only if Pilot usage shows a semantically correct store. Do not force training into `touchpoints` or `activity_log`.
4. **Contradiction presentation** — operator-attested vs system-observed mail dates already fail open as conflict. Surface that distinction in existing provenance UI when those components can represent it cleanly.

Do not treat SessionStore transcripts as operating memory.
