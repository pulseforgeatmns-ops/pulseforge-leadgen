# NEXT STEP

SPEC-117 is implemented (v1 thin slice): Emmett Outbound Infrastructure Intelligence. Reputation is capital. Emmett reports explainable inbox health and reasoned capacity, evaluates every send with the Safe Send Governor (Proceed / Slow / Pause / Emergency), and will not send without an operator-approved daily plan. Max cannot override Pause or Emergency silently. Paige provides copy. Outcomes route to Paige, Scout, Max, and Emmett. The product brief called this SPEC-110; repository SPEC-110 remains Business Intelligence Synthesis.

SPEC-116 is implemented (v1 thin slice): Operator Scorecard Intelligence. Max reasons from business objectives and recommends explainable metrics. Operators accept, modify, remove, reorder, or add. Only the approved scorecard is the definition of success. Drafts never report. Executive Business Briefs distinguish Recommended / Approved / Under Review. Daily briefings consume the approved scorecard.

SPEC-115 is implemented (v1 thin slice): Pilot 0 admin-provisioned onboarding. Create tenant + client user + temporary password in the admin UI. The client changes password, completes Client Intelligence, publishes an AIM, asks Max, and Scout returns tenant-scoped prospects. No SQL. Public signup and email verification are out of scope.

SPEC-114 is implemented (v1 thin slice): an operator creates a tenant in the product, PulseForge provisions an empty isolated workspace, and Max fail-closes without an active tenant. The Fedir AIM seed is not a live tenant — create Fedir through `/admin/clients`.

SPEC-113 is implemented (v1 thin slice): market documents compile into an approved Acquisition Intelligence Model. Scout loads only published AIMs. The compiler never executes outreach.

SPEC-112 remains implemented: PulseForge can hold a client's AIM before Scout sells into that market. Fedir is the first seed. Paige receives pain/language/proof/CTA from qualification. AIM findings are not operating fact.

SPEC-111 remains implemented: Max classifies operator intent into an explicit analysis-mode registry before reasoning. SPEC-110 intelligence synthesis remains in force. SPEC-109 intent-bound contracts remain in force. SPEC-108 claim grounding remains graduated. SPEC-107A is Completed.

Recommended next work, in order:

1. **Pilot 0 through the product** — Create tenant, create client user with a temporary password, force password change, complete Client Intelligence, compile and publish an AIM, ask Max, run Scout. If any step needs SQL, that is the next product gap (ADR-052).
2. **Wire Scout and campaign evaluation to the approved scorecard** — SPEC-116 exports runtime helpers; consumers beyond Brief and Daily Briefing land in a later slice.
3. **Paige copy into the Emmett queue** — SPEC-117 requires Paige-authored subject/body before send unless the operator explicitly allows legacy sequences.
4. **Durable assimilation of verified operator corrections** — persist *verified* working-model corrections after claim grounding, without treating Max-generated statements as operating fact. This is the next knowledge-layer milestone; do not start it by weakening SPEC-108, SPEC-109, SPEC-110, SPEC-111, SPEC-112, SPEC-113, SPEC-114, SPEC-115, SPEC-116, or SPEC-117.
5. **Fedir live pilot (operational)** — 50 qualified prospects and outreach begin. The AIM engine already reports this honestly as unmet.
6. **CIE typo-repair cleanup** — `app` → `gap` still inflates `unknowns` on CIE-claimed turns. This is no longer a Pilot blocker. Fix it as a narrow CIE semantic-contamination patch.
7. **Event C confirmation UX** — SPEC-106 v1 fail-closes on silent AO cohort mutation. If operators confirm follow-up scheduling, write `ao_follow_up_tasks` / `next_follow_up_date` behind an explicit confirmation turn.
8. **Event B durability (P2 only)** — persist AO training/readiness only if Pilot usage shows a semantically correct store.
9. **Contradiction presentation** — operator-attested vs system-observed mail dates already fail open as conflict. Surface that distinction in existing provenance UI when those components can represent it cleanly.

Do not treat SessionStore transcripts as operating memory.
Do not convert SPEC-107 recommendations into missions or agent enablement automatically.
Do not persist retracted Max-generated premises as SPEC-106 operating facts.
Do not persist SPEC-110 intelligence objects as operating fact.
Do not persist SPEC-112 AIM findings as operating fact.
Do not let Emmett write copy or override Pause/Emergency without the operator.
Do not invent Fedir case studies, geography, or 50 prospects.
Do not let Scout read compiler documents or draft AIMs.
Do not add graph learning or new memory systems until claim grounding, intent-bound response selection, business-intelligence synthesis, operator intent taxonomy, AIM qualification, AIC publication, and tenant provision remain green.
Do not attach the hand-authored Fedir AIM seed to a newly created Fedir tenant.
Do not create Fedir with a manual `INSERT INTO clients`.
