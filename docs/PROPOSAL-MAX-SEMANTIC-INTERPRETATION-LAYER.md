# Proposal — Max unified semantic interpretation layer

Status: **proposal for review. No implementation has started.**
Basis: `docs/P0-MAX-SEMANTIC-GENERALIZATION-AUDIT.md` (#1092, merged `478bb9a`), the authorization-polarity fix (#1093, merged `8099716`), and the evaluation harness `packages/max/workspace/eval/semantic/`. Both are deployed to production (`charming-trust` / `pulseforge-leadgen`).

> Language determines intent. Intent determines investigation. Evidence determines conclusions. Authority determines action.

The goal is to replace phrase-dependent routing with a general interpretation of **what the operator means, what it refers to, and what evidence the answer needs**. The deterministic layer keeps sole control of **what may happen** (authorization, tenant scope, execution).

---

## 1. Problem, in one paragraph

Max decides what a turn means through roughly 1,500 lexical rules spread across about 15 stages, and each stage re-derives intent in its own way. When the right owner is reached, the answers are good: the evidence-grounded analysis owner passes 6 of 8 graded turns. Reaching it depends on surface words, though. The reasoning fallback took 30 of 50 graded turns and passed none of them. The same failure class produced the negation P0 (#1093) and the "Cancel the mission." exit/cancel conflict (#1094), where two rules give opposite meanings to the same words. After #1093, the harness scores **24% overall, 26% semantic and 98% safety**.

## 2. Design overview

```
operator text + session
        │
        ▼
┌───────────────────────────── Interpretation (new, once per turn) ─────────────────────────────┐
│ ContextPacket (tenant-scoped, redacted)                                                        │
│   → DeterministicFrameProducer (today's signals, always runs)                                  │
│   → ModelFrameProducer (structured output, enum + candidate-id constrained; shadow first)      │
│   → FrameValidator (schema, candidate ids, tenant) → FrameReconciler → TurnFrame               │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
        │ TurnFrame (data only; carries no authority)
        ├──► Authority (UNCHANGED, deterministic): polarity verdict (#1093) + PendingDecisionResolver
        │        + routeExecutionRequest + AuthorizationConstraints + TME. Two-key rule (§4).
        ├──► Evidence Registry (new, read-only): subject + need → sources (readOnlyQueryable)
        └──► Composition: facet composers (reuse OperationalMissionReasoning) + clarification composer
```

### 2.1 `TurnFrame` contract (versioned, enum-only)

```js
TurnFrame v1 {
  speechAct:   'question' | 'hypothetical' | 'prohibition' | 'authorization' | 'directive'
             | 'correction' | 'report_request' | 'ambiguous' | 'smalltalk',
  subject: {
    kind: 'mission' | 'pending_decision' | 'agent_activity' | 'prospecting' | 'outbound'
        | 'portfolio' | 'session' | 'identity' | 'none',
    referentIds: string[],          // ONLY ids present in ContextPacket.candidates
    resolution: 'explicit' | 'focus' | 'correction' | 'ambiguous' | 'none',
  },
  need: Facet[],                    // status | cause | relevance | consequence | history | comparison
                                    // | next_step | rationale | evidence | overview
  decision: { pendingDecisionId: string|null, polarity: 'approve'|'reject'|'hold'|'cancel'|'revise'|'none' },
  conversationTarget: 'mission' | 'conversation' | null,   // "cancel the mission" vs "leave this topic" (#1094)
  clarification: { required: boolean, options: string[] }, // option ids from candidates only
  confidence: number,               // producer-reported; never used to widen authority
  producer: 'deterministic' | 'model' | 'reconciled',
  version: 1,
}
```

The frame has **no** fields for tenant, permissions, tool names or free text that downstream code executes.

### 2.2 `ContextPacket` (what the model sees)

- The latest operator message and up to 4 prior turns, with prospect PII redacted using the existing `privacy.js` patterns.
- Candidate referents, from the session tenant only, at most 25: missions (`id`, title, stage, status, pending-decision kind and prompt), the bound mission, the session focus/referent stack, and agent names.
- A catalog of evidence sources (names only). It never contains evidence values, prospect rows or credentials.
- A fixed system prompt plus schema, so the prefix can be cached.

### 2.3 Producers

1. **Deterministic producer.** Maps today's signals into the same frame: polarity verdict, `OperationalQuestionFrame`, `MessageTypeClassifier`, `ConversationSubject`, pending-decision resolver outcome. It always runs, so the system degrades to today's behaviour, never below it.
2. **Model producer.** One small model call per turn using structured output (`output_config.format` with JSON schema, enums only), `effort: low`, a 1.5 s timeout, and no tools. On failure or timeout the deterministic frame is used.
3. **Validator and reconciler.**
   - The validator rejects unknown enums and any `referentIds` or `pendingDecisionId` not in the candidate set.
   - The reconciler takes the model frame for *interpretation* (subject, need, speech act for read-only turns).
   - It takes the **more restrictive** of the two for anything touching authority (§4).

### 2.4 Evidence Registry (deterministic, read-only)

This merges the two retrieval stacks the audit found (`OperationalInspection` and `OperatingEvidenceRetrieval`) into one tenant-scoped registry, with every query behind `readOnlyQueryable`:

| Subject kind | Sources |
|---|---|
| `mission` | `acquisition_missions` + events (+ SPEC-022 `missions`) |
| `agent_activity` / `prospecting` | `agent_log`, `prospects` aggregates, Scout state |
| `outbound` | outbound gate, `touchpoints` |
| `pending_decision` | mission package (`ensureReadyExecutionReview`) |

Unavailable sources come back as explicit `unavailable` records, so uncertainty disclosure is structural. The registry also fixes the D1 contradiction (the business-intelligence stack never read `acquisition_missions`) and the `defaultLoadCapabilityPolicy` pool bypass.

### 2.5 Composition

Admission becomes frame-driven. The analysis owner takes any turn with `speechAct ∈ {question, hypothetical, report_request}` and a resolvable subject; session evidence becomes an optimisation rather than a precondition. Answers are built per facet:

- reuse the `OperationalMissionReasoning` composers;
- add composers for `agent_activity/prospecting` (cause analysis from run recency, errors and inventory freshness) and `pending_decision` (consequence, rationale, "not yet approved");
- use a clarification composer whenever `clarification.required` is set.

The generic reasoning fallback is no longer reachable for operational questions.

## 3. What stays deterministic (non-negotiable)

- Tenant scope (session tenant only; named-client refusal), the `readOnlyQueryable` guard, `routeExecutionRequest` as sole dispatcher, TME atomicity, durable `AuthorizationConstraints`, synthetic-evidence isolation, the "package must have been shown" guard, and the #1093 polarity verdict.
- The model never selects a tool, an SQL statement, a tenant or an execution intent.

## 4. Authority: the two-key rule

A pending decision may be consumed only if **both** of the following hold:

1. **Deterministic key.** The polarity verdict is not `held` or `mixed`, and `PendingDecisionResolver` resolves `AFFIRM`/`REJECT`/revision for the bound decision.
2. **Interpretive key.** The model frame (when present) has `speechAct ∈ {authorization, directive}`, `decision.pendingDecisionId` equal to the bound decision, and matching polarity.

If the keys disagree, the turn becomes a clarification and nothing is dispatched. If the model is unavailable, the deterministic key alone applies; that is today's behaviour, which the suite verifies safe. The model can therefore only **narrow** authority, for example by turning a deterministic AFFIRM on "approved?" into a question. It can never widen it. During shadow, both keys are logged, and every case where the model would have widened authority is a release blocker.

## 5. Rollout (each phase is separately reviewable and reversible)

| Phase | Change | User-visible | Model cost |
|---|---|---|---|
| 0 — done | Harness (#1092), negation fix (#1093) | — | 0 |
| 1 | `TurnFrame` contract + deterministic producer; harness scores **frames directly** (labelled speech act, subject, referent and need per graded turn) | none | 0 |
| 2 | Evidence Registry (deterministic); analysis owner reads it | better answers for already-admitted turns; D1 fixed | 0 |
| 3 | Model producer in **shadow**: runs after the response is sent (off the latency path), persists frames to `semantic_frame_shadow` (additive operator migration), review CLI on the `decision_shadow_events` pattern | none | yes (§7) |
| 4 | Promote **frame-driven admission for read-only turns only** (`question` / `hypothetical` / `report_request`), per tenant, behind `MAX_SEMANTIC_FRAME_MODE=off\|shadow\|readonly` and a code-level `DEPLOY_GATES` constant | yes | yes |
| 5 | Two-key authority enforcement (model can narrow only); retire superseded regex gates one stage at a time | yes | yes |

The authorization path itself is never model-led in any phase.

## 6. Evaluation and measurable promotion criteria

**Data**

- This harness: 50 graded turns, 3 tenants, A–F, with frame labels added in Phase 1.
- **A held-out "unseen client language" set**: at least 200 utterances across at least 4 tenants, including a new tenant fixture added at Phase 3. It is written by people other than the implementer: operators, or paraphrases of real anonymised production turns. It must include contrast pairs (question / hypothetical / prohibition / authorization), corrections and multi-mission references. It is never used for prompt tuning: it is frozen and versioned, and only scores are read.
- Production shadow: real turns, redacted, tenant-scoped.

**Gates to enter Phase 4 (read-only promotion)**

| Metric | Threshold |
|---|---|
| Speech-act accuracy (harness + held-out) | ≥ 95% |
| Prohibition / authorization confusion | **0** cases of a prohibition or question framed as authorization |
| Referent accuracy (when a referent exists) | ≥ 95%; wrong referent ≤ 2%; the rest clarification |
| Harness overall / semantic / safety | ≥ 85% / ≥ 85% / **100%** |
| Held-out overall | ≥ 80% |
| Negative controls | ≤ 10% (unchanged rule) |
| Shadow window | ≥ 7 days and ≥ 500 comparable production turns |
| Shadow authority widening (model frame would authorize where deterministic would not) | **0** |
| Shadow model error + timeout rate | ≤ 2% |
| Interpreter latency (measured in shadow) | p95 ≤ 800 ms; worst-case p99 ≤ timeout (1.5 s) with fallback |
| Existing Max, decision and semantic suites | green; no `baseline.json` regression |

**Gates to enter Phase 5 (two-key enforcement)**: 14 days in Phase 4 with 0 safety incidents, plus the same shadow-widening gate on the authority path.

## 7. Cost and latency

List prices (Anthropic, per million tokens; re-verify at implementation): Claude Haiku 5.5 $0.10 in / $0.50 out; Claude Sonnet 5.5 $2 in / $10 out. Cache reads cost about 0.1× input.

Per turn: about 2,000 input tokens (about 1,500 of them a stable, cacheable system prompt plus schema) and about 200 output tokens.

| Model | Per turn (uncached) | Per turn (cached prefix) | Per 10k turns |
|---|---|---|---|
| Claude Haiku 5.5 | ≈ $0.0003 | ≈ $0.0002 | ≈ $2–3 |
| Claude Sonnet 5.5 | ≈ $0.006 | ≈ $0.0033 | ≈ $33–60 |

- The minimum cacheable prefix depends on the model; verify with `usage.cache_read_input_tokens`.
- The held-out evaluation (200 utterances × 3 repeats) costs under $1 on Haiku 5.5.
- **Latency.** Phase 3 adds **zero** latency because the call runs after the response. In Phase 4 the call runs concurrently with evidence pre-fetch, with a 1.5 s timeout and deterministic fallback. Real p50/p95 figures come from shadow measurement and are not assumed here.
- **Recommendation.** Start with Haiku 5.5 at `effort: low`. Run Sonnet 5.5 on the held-out set as a ceiling comparison, and choose Haiku unless it misses a gate that Sonnet meets.

## 8. Rollback

- **Instant (configuration).** `MAX_SEMANTIC_FRAME_MODE=off` (or `shadow`) restores today's routing. A Railway variable change triggers a redeploy, so this takes minutes, not seconds. A per-tenant allowlist allows a single tenant to be rolled back.
- **Automatic.** A model error, timeout or validation failure falls back to the deterministic frame for that turn. If the error rate stays above 5% over 15 minutes, a circuit breaker drops to deterministic until the next deploy.
- **Hard gate.** Promotion also requires a code-reviewed `DEPLOY_GATES` constant (the pattern of `ACTIVE_JEV_ROUTING_ENABLED`), so configuration alone cannot enable `readonly` mode without a merged change.
- **Data.** `semantic_frame_shadow` is additive; a rollback migration ships with it. No existing table changes.
- **Code.** Each phase is one PR, and the existing lexical stages remain until Phase 5 removes them one at a time, each removal gated by the suite.

## 9. Risks

| Risk | Mitigation |
|---|---|
| Model frames a prohibition as authorization | Two-key rule; polarity verdict unchanged; zero-widening gate; F-family required tests |
| Hallucinated referent | Candidate-constrained ids + validator; otherwise clarification |
| Prompt injection via operator text or mission titles | Enum-only output; no tools; text treated as data; titles quoted as values |
| Overfitting to the harness | Frozen held-out set by other authors; new tenant at Phase 3; harness never used for prompt tuning beyond Phase 1 labels |
| Privacy of shadow logs | Same redaction and field allowlist as `decision_shadow_events`; tenant-scoped retention |
| Latency / outage | Off-path in shadow; timeout + fallback + circuit breaker |
| Behaviour drift in exact-prose tests | Shadow first; migrate exact-prose assertions in P0 suites to anchor/structure assertions as owners change |

## 10. Out of scope

New business features; model-chosen actions or tools; any change to outbound gates or the `recovery_kill_switch`; replacing the JEV shadow (it stays as an independent comparator).

## 11. Decisions requested from review

1. Approve the `TurnFrame` v1 contract and the two-key authority rule.
2. Model choice for Phase 3: Haiku 5.5 primary, Sonnet 5.5 comparator.
3. Who authors the held-out set, and whether redacted production turns may seed it.
4. Retention period for `semantic_frame_shadow`.
5. Whether #1094 (cancel vs. exit) should be fixed now with a narrow deterministic patch, or absorbed by `conversationTarget` in Phase 4.
