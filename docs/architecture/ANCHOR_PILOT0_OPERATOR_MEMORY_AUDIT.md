# Anchor Pilot 0 — Operator-Reported Operating Memory Audit

| Field | Value |
|---|---|
| **Status** | Audit only — no implementation |
| **Date** | 2026-08-17 |
| **Client** | Anchor Cleaning (`client_id = 10`) |
| **Related** | SPEC-105 / PR #317 operating-evidence retrieval, SPEC-098/103 CIE, SPEC-001 Knowledge, SPEC-003 Memory, SPEC-104 Operator Context, SPEC-095 Objectives, SPEC-022 Missions |
| **Constraint** | No fixes, migrations, production writes, new memory subsystem, or Pilot 0 fact seeding |

Traced against production code on `main` (post-#317). The exact operator prompt was classified by the live Workspace classifiers (not by speculation).

---

## A. EXECUTIVE VERDICT

**DISCONNECTED**

PulseForge already has a read-side operating-evidence path (SPEC-105 / PR #317) and several durable stores that can *describe* business activity after it exists. It does **not** have a conversational write path that recognizes an operator-reported operating update, classifies the events, preserves operator provenance, and persists them as operating state. When Jake told Max that Campaign 001 was mailed, Mike was trained, and follow-up should begin tomorrow, none of those steps ran.

The failure is recognition-first, not persistence-first. `WorkspaceEngine.ask()` classifies cognitive mode as `unclassified`, SPEC-105 retrieval refuses the turn because the message is a statement rather than an inventory/state question, and CIE then claims the turn under SPEC-103B inverted defaulting. `scoreClientBusinessSemantics()` scores the message as client-business `unknowns` (gap score inflated because typo-repair rewrites “app” → “gap”). `formatUnknownsAnswer()` then emits the Blueprint KNOWN / INFERENCE / UNKNOWN / EVIDENCE NEEDED template and never reads the operator’s asserted facts. That is why Max contradicted newly supplied campaign evidence **in the same turn**.

No later handler can save the turn. Objectives, missions, Paige, domain routing, and general conversation all sit **after** CIE. `WorkspaceEngine.ask()` writes only in-process `SessionStore` messages plus session-scoped CIE continuity. It cannot write AO mail execution, knowledge claims, CIE evidence, operator context, missions, or follow-up tasks from this path.

Existing “memory” systems are the wrong owner if treated as a chat-memory dump. SPEC-003 remembers reasoning **transitions**, not operator facts. Operator context is a **derived snapshot**. Session history is not durable. The closest specified foundation for “Jake said X happened” is Knowledge Evidence + Claim (+ Interaction/Observation), but that graph is not wired to Command Deck Max ask/retrieve. SPEC-105 would retrieve a mail-execution field if one existed on AO leads; that field does not exist, and SPEC-105 explicitly left operator-reported event persistence out of scope.

---

## B. THREE-EVENT FORENSIC TRACE

Prompt classified live:

- Cognitive mode: `unclassified` (`via: unclassified`)
- SPEC-105 `shouldRetrieveOperatingEvidence`: **false**
- CIE `shouldClaimClientIntelligenceTurn` (approved Blueprint): **true**
- CIE semantic mode: **`unknowns`** (`features.gap = 3`)
- Objective establishment: **null**
- Desk/mission intercept: **false**
- Execution intercept: **false**

| Event | Recognized? | Correct semantic type | Existing representation? | Persisted? | Fresh-session retrievable? | State A/B/C/D/E |
|---|---|---|---|---|---|---|
| A — Campaign 001 physically mailed 2026-08-06 | No. CIE classified the whole message as Blueprint gap/unknowns. No operating-event extractor ran. | Completed external execution; operator-attested fact; occurred 2026-08-06 | No production mail-execution record. SPEC-105 *looks for* `mailed_at` / `mail_sent_at` / `physical_mail_sent` on AO leads; those columns are not on `ao_leads`. Knowledge Interaction + Evidence + Claim could map the semantics in-library; Postgres persist of Observation is not supported (`tableForType` only company/person/interaction/evidence/claim). | No | No. #317 would still report mail execution **not recorded**. | **D** (operating SoR). Adjacent **C** only if treating unused Knowledge Evidence/Claim as the intended model. **E** if anyone wrote this as independently verified mail execution. |
| B — Mike / AO trained today on app + workflow | No. Same CIE claim. “app” was typo-repaired to “gap”, which helped force unknowns mode. | Completed internal operational event; training / readiness; not an acquisition outcome | No AO training / readiness event store. `users` can identify an AO named Mike; that is identity, not an event. `activity_log` / `touchpoints` are setter/prospect contact, not internal training. Knowledge Interaction could hold a meeting/training summary; not Max-wired. | No | No | **D** |
| C — Follow-up on the 20 Campaign 001 leads should begin tomorrow | No. “should” was treated as CIE advisory shape, not as a plan/expectation. | Planned / expected follow-up execution; must not be completed | Yes, semantically: `ao_follow_up_tasks` (`due_date`, `status='open'`, `next_action`) and `ao_leads.next_follow_up_date`. Missions can hold planned work (intent, not completion). | No | No as a plan. If tasks already existed, #317 would retrieve AO follow-up **intent/observation**, not “operator said follow-up starts tomorrow.” | **C** (task/date model exists; conversational Max cannot write it). **E** for silently creating 20 tasks or marking them completed when the date passes. |

None of the three events reached State A or State B. Max did not understand them in the current turn, so “recognized but not persisted” does not apply.

---

## C. FAILED TURN ROUTING TRACE

Exact prompt:

> Quick operating update: Campaign 001 was physically mailed on August 6. I met with Mike, one of our AOs, earlier today for training and walked him through the app and workflow. Follow-up on those 20 Campaign 001 leads should begin tomorrow.

### Handler sequence in `WorkspaceEngine.ask()`

```text
ask()
  appendMessage(role=operator)                    // SessionStore only
  maybeHandleSpecialistInterrogationTurn()        // miss — no recent specialist work interrogation
  classifyCognitiveMode()                         // unclassified
  maybeHandleRetrievalBeforeDelegationTurn()      // miss — SPEC-105 gate false
  maybeHandleScoutAcquisitionTurn()               // miss — not a Scout investigation
  maybeHandleClientIntelligenceTurn()             // HIT — handled:true, reason=client_intelligence_unknowns
      formatUnknownsAnswer(blueprintSummary)      // Blueprint template; ignores operator facts
      recordLastCieTurn()                         // session only
      return                                      // STOP
  -- never reached --
  maybeHandleActiveWorkContinuation()
  maybeHandleOperatorObjectiveTurn()
  maybeHandlePaigeCampaignContentDelegation()
  selectExecutionDomain() / Mission Engine
  general conversation
```

### Classifications that actually fired

| Layer | Result | Why |
|---|---|---|
| Cognitive mode | `unclassified` | No retrieval/explanation/reflection/recommendation/planning/execution verb. “was mailed” is a past assertion, not `mail this`. |
| Intent (SPEC-105) | not operating-evidence | `isOperatingEvidenceQuestion()` requires inventory/state question shape (`already`, `existing`, `happened`, `what campaigns have we run`, etc.). “operating update” + past-tense report does not match. |
| Business semantic (CIE) | `isClientBusiness=true`, mode=`unknowns` | `campaign` / `leads` hit strategy; `our` hits discourse; `should` hits advisory; `begin`/`tomorrow` hit priority/specificity. Gap score = 3 because typo-repair maps **app → gap** (`COMMON_UTTERANCE_LEXICON` + doubled-letter heuristic), then `/\bgaps?\b/` adds +2. |
| Retrieval | not claimed | `maybeHandleRetrievalBeforeDelegationTurn`: `shouldRetrieveOperatingEvidence` false; `unclassified` is not a never-delegate mode; `isHardRetrievalQuestion` false → `return null`. |
| CIE | claimed | `shouldClaimClientIntelligenceTurn` with approved Blueprint: `isClientBusiness` or `advisory >= 1`. Then `inferReasoningMode` returns `unknowns` because `features.gap >= 2`. |
| Memory | none | No remember/note/update/record handler. SPEC-003 `remember()` is not on this path. |
| Mission | none | CIE returns before domain/mission routing. `detectObjectiveEstablishment` is null even if it had run. |
| Outcome | none | No outcome-intelligence ingest from conversation. |

### Why CIE won

SPEC-103B inverted claim: with an approved Blueprint, unrecognized client-business wording stays in CIE by default. The message contains campaign/leads/AO/should. SPEC-105’s CIE skip (`shouldRetrieveOperatingEvidence`) does **not** fire for statements. There is no operating-update skip.

### Operating-event extraction / persistence / writes from `ask()`

- Extraction: **not attempted**
- Persistence path: **not considered**
- Writes possible from this CIE return: **session transcript + `lastClientIntelligenceTurn` / active-reasoning session fields only**
- Durable writes: **none**

### Module that caused the CIE unknowns fallback

`packages/max/workspace/ClientIntelligenceContext.js`

1. `maybeHandleClientIntelligenceTurn()` claims the turn (`shouldClaimClientIntelligenceTurn`)
2. `composeClientContextReasoning()` → `inferReasoningMode()` → `'unknowns'`
3. `formatUnknownsAnswer(summary)` emits “EVIDENCE NEEDED: we do not yet have enough campaign evidence…” from Blueprint fields only

`formatUnknownsAnswer` never receives or inspects the operator message.

---

## D. EXISTING MEMORY / EVENT SYSTEM INVENTORY

| Existing system | Durable | Provenance | Temporal semantics | Operator-write capable | Max-readable | Reuse? |
|---|---|---|---|---|---|---|
| SPEC-003 Memory (snapshots / diffs / changes / timeline / evolution / watches) | Optional; default in-memory. Designed for reasoning-score transitions, not facts. SPEC-003: “Max shouldn't remember facts.” | Snapshot metadata, not operator-attested events | `asOf` / snapshot timestamps; change deltas | No conversational write. `remember()` is evaluate→snapshot. | Not wired to Workspace `ask()` / SPEC-105 | **No** as operating-event store. Reuse later for “what changed after a fact was recorded.” |
| Knowledge Company / Person | Yes if dual-write/Postgres enabled (`KNOWLEDGE_DUAL_WRITE`) | Adapter provenance | `createdAt` / `updatedAt` | CRM/Scout dual-write, not Max chat | Not queried by SPEC-105 or CIE workspace | Identity only (Mike as Person). Not the event. |
| Knowledge Interaction | Yes (in `knowledge_nodes`) | Soft; channel/actionType/summary/`occurredAt` | `occurredAt` only | Dual-write from CRM touchpoints; no Max chat writer | Not queried by SPEC-105 | **Possible** for “something happened” (mail, training meeting) if wired. No planned/expected status. |
| Knowledge Observation | Library yes; **Postgres no** (`tableForType` throws) | `observedAt` + `recordedAt` + origin/adapter | observed vs recorded | No production writer from Max | No | Specified invariant, not production-backed. Do not treat as live SoR. |
| Knowledge Evidence | Yes (`knowledge_evidence`) | `sourceType`, `sourceId`, confidence, payload | `createdAt` only on row; provenance lives on Observation (unpersisted) | CIE interview writes **CIE** evidence, not this table. KnowledgeWriter is CRM dual-write. | No Workspace/SPEC-105 read | **Best existing semantic slot** for “operator reported it” if connected. |
| Knowledge Claim | Yes (`knowledge_claims`) | SUPPORTS/ABOUT edges; status `active` / `invalidated` / `merged` | `createdAt` / `updatedAt`; no `occurredAt` / `expectedAt` | `ClaimEngine` API exists; Max chat does not call it | No | **Best existing slot** for “Campaign 001 was mailed August 6” + corrections. |
| Operator context (`operator_contexts`) | Yes — one derived JSON document per client | `last_rebuild_trigger` + rebuild-event audit | `last_rebuild_at`; no event occurred/expected | Rebuild hooks only. `CLIENT_MESSAGE` trigger exists but is **not called** from `ask()`. SPEC-104 out of scope: automatic extraction from client messages | Loaded at workspace **open**, not by SPEC-105 | **Derived view only.** Do not store the mail fact here. |
| Operator objectives | Yes | `created_by`; no evidence class | `created_at` / `updated_at`; `time_horizon` text | Yes, but only on explicit “our objective is / persist this objective” cues. This prompt does not match. | Yes (SPEC-095 + SPEC-105 as **intent**) | Wrong type. Strategic desired state, not execution events. |
| Missions + `mission_audit_events` | Yes | Audit payload JSON | `started_at` / `completed_at` / `created_at` | After CIE, Mission Engine can create/resume. This turn never gets there. SPEC-032 Mission Memory is **Proposed**. | Yes as **intent**, explicitly not execution | Event C could attach as planned work on an existing Campaign 001 mission; do not mark completed. |
| CIE interview evidence (`cie_evidence`) | Yes | `source`, `type` EXPLICIT / INFERRED / OBSERVED / CLIENT_EDITED | `created_at` only | `/client-intel` interview + ConversationMemoryUpdater. **Not** Workspace Max. | CIE Blueprint/Playbook load; not SPEC-105 operating evidence | Wrong corpus. Business-understanding facts, not campaign execution. |
| CIE interview reasoning memory | Session/interview_state | Correction/add-on classes | Pending correction then overwrite accepted fact | Interview only | Interview only | Do not reuse as operating memory. |
| Workspace SessionStore | **No** (in-process, SPEC-009 v1) | None | Message timestamps | Automatic append | Same session only | Conversational history, not durable operating memory. |
| SPEC-105 operating-evidence retrieval | Read-only composition | Epistemic: verified / inferred / not_recorded / unavailable. Layers: intent / execution / observation / outcome / learning | Uses source timestamps if present; no operator-event time model | **Read only** | Yes, when the *question* matches | **Reuse as the fresh-session read seam.** Does not accept writes. Out of scope: operator-reported event persistence. |
| `ao_leads` / Campaign 001 | Yes | Attribution source / campaign_name | `first_contact_date`, `last_contact_date`, `next_follow_up_date` | AO field/Max-AO flows, not Command Deck Max | Yes via `aoBriefingService` | Intent + follow-up state. **No mail-execution column.** |
| `ao_follow_up_tasks` | Yes | Task row; no operator-attested flag | `due_date`, `created_at`, `completed_at` | AO services | Indirectly via AO progress, not as “planned tomorrow” | **Best existing owner for Event C** (planned, not completed). |
| `ao_max_sessions` | Yes | AO workflow payload | created/updated | AO Max flow | No Command Deck retrieval | AO coaching session, not Command Deck operating memory. |
| `activity_log` | Yes | setter_id, action_type, notes | `created_at` only | Setter dashboard | SPEC-105 activity counts | Setter call/email/text. Wrong type for mail/training/plan. |
| `touchpoints` | Yes | channel, action_type, outcome | `created_at` (and richer fields in prod) | Agents / webhooks / `logTouchpoint` | SPEC-105 activity counts | Prospect-channel events. Forcing physical mail or AO training here would fake a touchpoint type. |
| Outcome / revenue / content-outcome intel | Yes (jobs, payments, SPEC-092/093) | Evidence summaries | closed/recorded timestamps | Specialist/outcome pipelines | SPEC-105 outcome counts | Outcomes, not these three events. |
| Scout acquisition state | Yes | Investigation provenance | run timestamps | Scout loop, not this prompt | SPEC-105 Scout state | Unrelated. |
| Watch registry | In-memory detection | Watch ops | Watch windows | API `watch()`, not chat | Not Workspace ask | Alerts on score motion, not operator facts. |

---

## E. KNOWLEDGE GRAPH FINDING

Company / Person / Interaction / Evidence / Claim **exist as a library and as Postgres tables** (Evidence/Claim in dedicated tables; Company/Person/Interaction in `knowledge_nodes`). Observation exists in code and ontology but is **not persistable** in the current Postgres repository.

This is a **viable existing conceptual foundation**, not a production operating-memory path for Command Deck Max.

Natural mapping **without a new model**:

| Pilot meaning | Existing node | Fit |
|---|---|---|
| Something happened | Interaction (`channel`, `actionType`, `summary`, `occurredAt`) | Event A (direct_mail / mailed) and Event B (meeting / training) |
| Operator reported it | Evidence (`sourceType` could be `operator_report`) | All three, if sourceType is allowed to be operator-attested |
| Campaign 001 was mailed August 6 | Claim (`statement`, `status`, SUPPORTS, ABOUT) | Event A; corrections via `invalidateClaim` / `mergeClaims` |

**Limitations that make this incomplete as today’s SoR:**

- Command Deck Max `ask()` never reads or writes the graph.
- SPEC-105 does not query Knowledge.
- SPEC-104 lists Knowledge Graph query wiring as out of scope.
- Dual-write taxonomy is CRM (`comm.email_*`, `call.*`, `meeting.*`) — no physical-mail or AO-training operational event.
- CRM observation vocabulary is `email_sent` / `call_logged` / `meeting_booked` / `crm_field_update`.
- Interaction has `occurredAt` only — Event C’s `expected_at` does not fit without overloading.
- Claim has no `occurredAt` / `expectedAt`; temporal truth would live in statement text or metadata.
- Production ingest is opt-in (`KNOWLEDGE_DUAL_WRITE`) and CRM-shaped.

**Verdict:** Do not rebuild a new knowledge graph. Do not pretend the current graph is already Max’s operating memory. If a later design uses existing architecture, Evidence + Claim (+ Interaction for completed events) is the specified seam — it is unused by this conversation path.

---

## F. CANONICAL OWNERSHIP ANALYSIS

No implementation. Best **existing** owner only.

### Event A — mailed August 6

**No semantically correct production owner that Max can write and #317 can read.**

- SPEC-105’s intended *read* owner is AO mail-execution fields that **do not exist**.
- Knowledge Claim + Evidence is the specified *semantic* owner, unused.
- Missions / objectives = intent, not mailed.
- Touchpoints / activity_log = wrong channel type.
- Operator context = derived; must not become the SoR.
- CIE Blueprint = durable business understanding, not campaign execution.

Operator context should **surface** a canonical mail record, not store “mailed August 6” as a context blob.

### Event B — Mike trained today

**No appropriate existing owner.**

Internal AO operational readiness is not modeled. Do not force this into touchpoints, activity_log, CIE evidence, or Campaign 001 lead status. Knowledge Interaction is the least-wrong unused mapping. Durable persistence is optional; this is readiness, not acquisition outcome.

### Event C — follow-up should begin tomorrow

**Best existing owner:** `ao_follow_up_tasks` (and/or `ao_leads.next_follow_up_date`) as **planned** work.

Secondary: an existing Campaign 001 mission’s plan/progress, still as planned — never `completed`.

Not: operator context as SoR; not a new objective; not a completed touchpoint.

Writing 20 tasks from chat without confirmation would cross the existing human-in-the-loop boundary (State E).

---

## G. CURRENT-TURN FAILURE

Max contradicted the operator **in the same turn** because the handler that won never treats the current message as evidence.

1. **The current operator message is available** as `question` in `ask()` and is appended to `SessionStore`. CIE uses it only to *classify*, then answers from the approved Blueprint summary.

2. **CIE claimed the turn before any general reasoner** could interpret the statements. Scout and SPEC-105 retrieval both declined. There is no “operating update” handler.

3. **`formatUnknownsAnswer(summary)` ignores the current message.** It builds KNOWN/INFERENCE/UNKNOWN/EVIDENCE NEEDED exclusively from Blueprint identity, ICP, geography, goals, metrics, and `summary.unknowns`. The hardcoded line is:

   > EVIDENCE NEEDED: we do not yet have enough campaign evidence…

4. **The Blueprint is treated as the authoritative evidence corpus for this CIE mode.** SPEC-103 explicitly: approved Blueprint/Playbook are authoritative; chat is not persisted as client fact. That rule is correct for *business understanding* and fatal when the utterance is *operating state*.

5. **Max could have acknowledged without persisting.** Nothing in the architecture forbids a read-only “I heard three updates; I have not durably recorded them” response. No such handler exists.

6. **No operator-update / remember / note / record handler was bypassed** — it does not exist on the Workspace path. `ConversationMemoryUpdater` is CIE **interview** memory (corrections to Blueprint sections), not Command Deck operating updates.

7. **Typo-repair made unknowns more likely.** `normalizeClientUtterance` → `repairUtteranceToken`: “app” looks typo-like (`pp`) and is edit-distance-1 from lexicon word “gap”. Normalized text becomes “walked him through the **gap** and workflow.” That token both increments the gap concept family and trips `/\bgaps?\b/`, pushing `features.gap` to 3. Even without that bug, CIE would still claim the turn and answer from Blueprint (likely `focus` / advisory), still ignoring the three facts.

8. **Presentation does not rescue the turn.** `PresentationEngine.present()` renders the structured CIE answer; it does not re-read the operator message as new evidence.

---

## H. DURABILITY GAP

Separated, not collapsed into “memory.”

### Extraction

Missing. No operating-event extractor on `ask()`. The message is classified as CIE business-unknowns, not as three events.

### Classification

Missing for this utterance type. Cognitive modes are question-shaped (retrieve / explain / recommend / execute). CIE modes are Blueprint-advisory (unknowns / targeting / approach). SPEC-105 layers (intent / execution / observation / outcome / learning) exist on the **read** path only. Fact vs plan vs expectation is not applied to conversational input.

### Authorization

Existing safety is conservative and, on this path, vacuously successful: CIE is context-only and “never mutates CRM/outreach state.” There is no authorized conversational write for operator-attested operating events, and no confirmation prompt to create one. That is a gap, not a correctly enforced attestation policy.

### Persistence

No store is written. SessionStore is not durable. Operator context is not rebuilt (`CLIENT_MESSAGE` hook unused). Knowledge / AO / tasks / CIE evidence / missions are untouched.

### Retrieval

#317 can retrieve AO leads, prospects, Scout state, missions, objectives, activity, outcomes — **if they already exist**. It cannot retrieve facts that were only spoken. Fresh session asking “What’s the current state of Campaign 001?” still reports mail execution not durably recorded.

If Event A were written today:

| Store | Fresh Max would know it was mailed? |
|---|---|
| `ao_leads.mailed_at` (field does not exist) | Yes — SPEC-105 `hasDurableMailExecution` |
| Knowledge Claim/Evidence | **No** — SPEC-105 does not read the graph |
| `cie_evidence` | **No** — operating retrieval does not load CIE evidence |
| `operator_contexts` JSON | **No** as operating evidence; maybe a brief mention at open, not #317 inventory |
| `touchpoints` / `activity_log` | Count only; not mail-execution verification |
| Mission completed | **No** — retrieved as intent, not mailed |
| SessionStore | **No** |

Missing seam is **write + recognition**, and for Knowledge also **read integration**. For a hypothetical AO `mailed_at`, the missing seam would be **write only**.

---

## I. EPISTEMIC / TEMPORAL SAFETY FINDINGS

### Epistemic source classes

| Class | Exists today? | Where |
|---|---|---|
| Operator assertion / operator-attested | Partial, interview-only | CIE `EXPLICIT` / `CLIENT_EDITED`; not Workspace operating events |
| Specialist observation | Partial | Agent logs, Scout provenance, specialist delegation traces |
| Imported / system data | Yes | CRM dual-write, Brevo, AO field logging |
| Provider/API observation | Yes | Webhooks, Places, Hunter, etc. |
| Inferred claim | Yes | SPEC-105 `inferred`; CIE `INFERRED`; Max “Level 3 inference” |
| Model-generated recommendation | Yes | CIE advisory; briefs generate recommendations and **do not store** them (SPEC-104) |

**Gap:** Workspace Max cannot persist “Jake says Campaign 001 was mailed August 6” as **operator-attested** distinct from **system-observed** and **model inference**. SPEC-105 would treat a populated `mailed_at` as **verified** — that would collapse attestation into independent verification unless provenance is carried.

### Temporal fields in the wild

| Field | Where | Role |
|---|---|---|
| `occurredAt` | Knowledge Interaction | Event time |
| `observedAt` / `recordedAt` | Knowledge provenance / Observation | Observe vs ingest |
| `created_at` / `updated_at` | Almost every table | Record time only |
| `due_date` / `next_follow_up_date` / `callback_at` | AO tasks, AO leads, prospects | Planned/due |
| `completed_at` / `started_at` / `booked_at` / `closed_at` | Missions, tasks, prospects | Completion |
| `expected_at` | **Not a first-class column** | Event C would be overloaded onto `due_date` |

The Pilot update needs historical completed (Aug 6), current completed (today), and future expected (tomorrow). Current architecture would collapse “should begin tomorrow” into either nothing or a due date, and would collapse “mailed August 6” into `created_at` if someone stuffed it into activity_log.

### Fact / plan / inference distinctions

| Distinction | Exists? |
|---|---|
| FACT / observed event | SPEC-105 `verified` + campaign `execution` layer (read-side). No conversational fact type. |
| PLAN | Missions, AO open tasks, canary preparation. Not extracted from chat. |
| EXPECTATION (“should begin”) | Not distinct from plan. |
| HYPOTHESIS | No engine. Claim is the stand-in (EVIDENCE_CORE_DOMAIN_AUDIT). |
| LEARNING | SPEC-093 content learnings; SPEC-105 `learning` layer unused for campaigns. |
| RECOMMENDATION | CIE / briefing generated; SPEC-104 does not persist recommendations. |

### Correction / superseded

| Mechanism | Preserves history? | Wired to this prompt? |
|---|---|---|
| Knowledge `invalidateClaim` / `mergeClaims` | Yes — old claim invalidated, evidence can move | No |
| CIE interview pending correction | Overwrites accepted interview fact after pending | Interview only |
| SPEC-003 diff / change / timeline | Append-only snapshot diffs of **scores**, not claims | No |
| Operator context rebuild events | Versions the derived document | No fact timeline |
| Mission audit | Append-only mission events | SPEC-032 revision history not implemented |
| activity_log / touchpoints | Append-only rows | Wrong type; no “current effective claim” |

A later “Correction: it went out August 7” has **no production path** that keeps original assertion + correction + current effective claim. The existing *design* that can do that is Knowledge Claim invalidate/merge — unused here.

Passing a due date must not auto-complete Event C. Nothing currently would, because Event C is not written. AO task completion is explicit (`completed_at`).

---

## J. VERIFIED COMPONENTS TO REUSE

Do not rebuild these:

1. **SPEC-105 `OperatingEvidenceRetrieval`** — epistemic states, campaign layers, mail-execution vs intent distinction, tenant fail-closed. Extend reads; do not replace.
2. **CIE skip hook** — `shouldRetrieveOperatingEvidence` already prevents CIE from eating *questions*. Same seam can refuse CIE for operating *updates*.
3. **Approved Blueprint / SPEC-103** — correct for business understanding. Keep it off operating-state updates.
4. **Human-in-the-loop / no CRM mutation from CIE** — keep. Operator assertion must not become silent verified execution.
5. **`ao_leads` + `aoBriefingService` Campaign 001 progress** — existing operating read model.
6. **`ao_follow_up_tasks` / `next_follow_up_date`** — planned follow-up.
7. **Knowledge Evidence + Claim (+ Interaction)** — specified attestation/correction model.
8. **Operator context as derived snapshot** — rebuild from canonical records; do not become the fact store.
9. **Operator objectives** — strategic context only.
10. **Missions as planned/tracked work, not proof of mailing** — already enforced in #317.
11. **SessionStore** — current-turn transcript only.

---

## K. MINIMUM REMEDIATION SURFACE

No implementation spec. Smallest **existing** seams only.

```text
operator statement
  → recognize operating update          [missing gate beside SPEC-105 / CIE skip]
  → classify event / fact / plan        [missing; reuse SPEC-105 layers + planned/expected]
  → preserve operator provenance        [Knowledge Evidence sourceType or equivalent flag]
  → persist safely                      [confirmation; attested ≠ verified]
  → update derived operating context    [existing SPEC-104 rebuild hooks]
  → retrieve in fresh Max session       [existing SPEC-105 loaders]
```

Required seams, in order:

1. **Recognition gate in `ask()` before CIE claim** — statements like “operating update” / completed campaign execution / planned follow-up must not enter `formatUnknownsAnswer`. This is the same class of fix as SPEC-105’s CIE skip, for writes/acknowledgement rather than inventory questions.

2. **Current-turn interpretation that reads the operator message** — even with persist deferred, acknowledgment must use the three clauses. `formatUnknownsAnswer` must not be the handler.

3. **Classification** — completed vs planned/expected; campaign-linked vs internal ops; operator-attested vs system-observed. Reuse SPEC-105 `CAMPAIGN_LAYER` + epistemic enum; add planned/expected so Event C cannot become execution.

4. **Safe persist (policy-gated)**  
   - Event A: do **not** invent a new memory engine. Either (a) attested Evidence/Claim on the existing graph, and teach SPEC-105 to read attested claims without promoting them to `verified` mail execution, or (b) a real AO mail-execution field that SPEC-105 already knows how to read, written only as operator-attested.  
   - Event B: no existing SoR; persist only if an Interaction/attested note is explicitly accepted — otherwise session acknowledgment is enough.  
   - Event C: existing AO follow-up task / next follow-up date, with confirmation; status remains open.

5. **Derived context** — `triggerOperatorContextRebuild` already has event hooks. Rebuild from the canonical row; do not insert the sentence into `operator_contexts.context`.

6. **Fresh-session read** — keep #317. Do not add chat-transcript retrieval.

A new memory subsystem is **not** justified. The hole is a missing conversational recognition + attested-write seam into stores that already exist (or, for Event A’s AO field, that SPEC-105 already pretends exist).

---

## L. PILOT 0 ACCEPTANCE TEST

Use the real Anchor update:

> Campaign 001 was physically mailed on August 6. I met with Mike, one of our AOs, earlier today for training and walked him through the app and workflow. Follow-up on those 20 Campaign 001 leads should begin tomorrow.

A passing system must:

1. Recognize the message as an operating update (not CIE Blueprint unknowns).
2. Distinguish all three events.
3. Distinguish completed (A, B) from planned/expected (C).
4. Preserve operator provenance (Jake attested; not Brevo/AO-logged; not Max inference).
5. Not convert “should begin tomorrow” into completed execution.
6. Not fall into CIE Blueprint unknowns.
7. Acknowledge the update meaningfully in-turn, using the supplied facts.
8. Persist only what policy permits (attested ≠ independently verified).
9. Expose what was persisted vs merely understood.
10. Retrieve persisted state in a fresh workspace.

Fresh-session test:

> What's the current state of Campaign 001?

Passing answer must distinguish:

- Campaign 001 was **operator-reported** as physically mailed August 6
- Mike’s training occurred only if that event was judged durable
- Follow-up was **planned** for the stated next day
- Whether later evidence confirms follow-up actually began
- Observed outcomes currently recorded
- Remaining unknowns

Max must not silently treat the planned follow-up as completed merely because the expected date has passed.

**Current system fails 1–10.** It fails at step 1.

---

## SECOND ACCEPTANCE TEST — CORRECTION

> Correction: Campaign 001 actually went out August 7, not August 6.

### Expected behavior from **existing** architecture (not a new design)

The only existing model that can preserve original assertion + correction + current effective claim + provenance + timeline without silent overwrite is **Knowledge Claim**:

- Keep claim₁ active until correction
- `invalidateClaim(claim₁)` or `mergeClaims` onto claim₂ (“mailed August 7”)
- Attach new Evidence (`sourceType=operator_report`, correction)
- Timeline via claim/evidence `createdAt` and SUPPORTS edges

CIE interview correction overwrites accepted Blueprint facts — wrong corpus, and it does not keep two claim versions as current-vs-superseded operating truth.

SPEC-003 diffs would only show a score snapshot change if someone re-evaluated a company — not this fact.

Operator context rebuild would replace the derived sentence and leave a rebuild-event version, not a claim timeline.

**If nothing is persisted (today’s actual state):** a correction has nothing to correct. Max would again CIE-claim or retrieve “mail not recorded.”

A correct future system must not leave two contradictory **current** facts and must not destroy August 6 history.

---

## CORE ANSWERS

### When the operator tells Max that something changed in the real business, what existing architecture should recognize it, decide what kind of knowledge it is, preserve where it came from, make it durable when appropriate, and allow Max to recover it later?

**Recognize** on the Workspace `ask()` gate beside SPEC-105 — before CIE inverted claim.

**Classify** with SPEC-105’s existing epistemic + campaign-layer vocabulary, plus planned/expected so plans stay plans.

**Preserve provenance** as operator-attested Evidence (Knowledge `sourceType`, or an equivalent flag on whatever row is written). Do not promote to system-verified execution.

**Persist** into the existing owner that matches the event:
- completed campaign execution → attested claim/evidence (and only an AO mail field if one is added for the #317 reader)
- internal training → no current SoR; do not fake a touchpoint
- planned follow-up → `ao_follow_up_tasks` / `next_follow_up_date`, confirmation-gated

**Derive** operator context from that record via existing rebuild hooks.

**Retrieve** through SPEC-105 in a fresh session, still separating attested vs verified vs inferred vs not recorded.

### Why did none of that happen when Jake told Max that Campaign 001 had been mailed, Mike had been trained, and follow-up was expected to begin tomorrow?

Because the turn was never recognized as an operating update. Cognitive mode was unclassified; SPEC-105 retrieval only answers *questions about existing records*; CIE claimed a business-advisory unknowns turn (helped by “app”→“gap”); `formatUnknownsAnswer()` answered from the Blueprint and said campaign evidence was still missing. No extractor, no attested write, no rebuild, no #317 update. The operator’s three facts died at recognition, inside `maybeHandleClientIntelligenceTurn()`.
