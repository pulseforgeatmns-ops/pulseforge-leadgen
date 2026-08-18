# Changelog

All notable changes to this project are documented here. Format inspired by [Keep a Changelog](https://keepachangelog.com/). Versions follow the release plans in `docs/releases/`.

## [Unreleased]

### Added

- Anchor Cleaning ads-ready homepage + walkthrough intake
  - Source page at `sites/anchor-cleaning/index.html` (copy to the GitHub Pages repo to ship)
  - Visible `tel:+16034202430` Call/Text path, search-literal commercial-cleaning copy, and a short walkthrough form
  - `POST /api/public/walkthrough` writes `agent_actions` for `client_id=10`
  - Page emits `walkthrough_form_submit`, `phone_click`, and `email_click` for GA4 / Google Ads once IDs are pasted in

- Max Development Framework ([SPEC-102F](docs/specs/SPEC-102F_Max_Development_Framework.md))
  - Competency lifecycle (not started → training → practicing → graduated → regression)
  - Operator training loop: real work → observe → review → principle → implement → retest → graduate
  - Durable competency registry linking SPEC-098 / SPEC-099A / SPEC-101 / SPEC-102 to regression tests
  - Training record + exercise schema; performance review dimensions; real-work-first priority
  - `node scripts/maxTrainingRecord.js` renders inspectable training history (PulseForge-internal)
- Max Retrieval Before Delegation ([SPEC-102](docs/specs/SPEC-102_Max_Retrieval_Before_Delegation.md))
  - Cognitive-mode classification before specialist routing
  - Retrieval gate: answer from durable knowledge before delegating to Scout/Paige
  - Session stickiness no longer auto-delegates retrieval/explanation/reflection questions
- Max Specialist Result Interrogation & Cognitive Trace ([SPEC-101](docs/specs/SPEC-101_Max_Specialist_Result_Interrogation.md))
  - Specialist results become inspectable cognitive history instead of terminal replies
  - Available / supplied / consumed context layers diagnose the correct failure boundary
  - Follow-up interrogation is retrieved before domain routing and does not rerun the specialist
  - Max can explain his own evaluation; unknown cause stays unknown
  - Generic contract for Scout now and future specialists; migration persists result payload
- Scout Acquisition Discovery Foundation ([SPEC-100A](docs/specs/SPEC-100A_Scout_Acquisition_Discovery_Foundation.md))
  - Scout constructs a real candidate universe from Max's bounded acquisition objective
  - Retrieve-before-discover, adapter-based public/business discovery, entity resolution
  - Basic fit is kept separate from timing/intent; strong-fit companies are not discarded
  - Zero evaluated candidates is `blocked`/`partial`, not a completed market-negative result
  - SPEC-099A funnel (`discovered` / `resolved` / `evaluated` / `fit` / `signals` / `supported`) is populated from actual execution
- Max Specialist Delegation Contract ([SPEC-098](docs/specs/SPEC-098_Max_Specialist_Delegation_Contract.md))
  - Canonical `SpecialistDelegation` / `SpecialistResult` language for Max → specialist → Max
  - Durable `specialist_delegations`, `specialist_results`, `specialist_evaluations`
  - Explicit authority + policy supremacy (fail closed; no silent downgrade)
  - Lightweight capability registry (`test_intelligence` callable; Scout/Paige declared, unwired)
  - Max evaluates results as evidence, not ground truth; specialists cannot mutate Command Deck priority
  - Operator direction remains authoritative; no automatic specialist recursion
  - Migration: `migrations/2026-08-16-specialist-delegation.sql`
- Max Specialist Direction & Operator Rationale ([SPEC-096](docs/specs/SPEC-096_Max_Specialist_Direction_and_Operator_Rationale.md))
  - Operator discusses Paige recommendations with Max (Accept / Discuss with Max)
  - Durable `content_recommendations` + `specialist_directions` persistence
  - Max interprets natural-language feedback into direction + rationale + scope
  - Paige refinement via `refineContentRecommendation()` preserving SPEC-092/093 evidence
  - Operator-sourced learnings in `content_learnings` (`learningSource: operator_direction`)
  - Fresh-session direction recovery; tenant isolation; fail-closed refinement
  - Migration: `migrations/2026-08-16-specialist-direction.sql`
- Max Durable Operator Objectives & Pre-Routing Context Resolution ([SPEC-095](docs/specs/SPEC-095_Max_Durable_Operator_Objectives.md))
  - Durable `operator_objectives` (operator/client scope) outside SessionStore
  - Pre-routing retrieval + deterministic reference resolution (fail closed on ambiguity)
  - Status questions about resolved objectives no longer create Missions
  - SPEC-094 Paige delegation receives recovered objective context across fresh sessions
  - Public Max Launch production seed; migration `migrations/2026-08-13-operator-objectives.sql`
- Max → Paige Campaign Content Delegation ([SPEC-094](docs/specs/SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md))
  - Thin Max adapter (`services/maxPaigeCampaignDelegation.js`) calls SPEC-093 `generateContentRecommendation()`
  - Workspace routing hook for launch runway / LinkedIn / thought leadership / ask Paige when campaign context exists
  - Max remains operator-facing; Paige recommendation is review-first (no publish/send/CRM/Buffer)
  - Evidence IDs + uncertainty preserved; tenant isolation enforced
- Paige Outcome Learning Loop ([SPEC-093](docs/specs/SPEC-093_Paige_Outcome_Learning_Loop.md))
  - Deterministic evaluation of SPEC-092 outcomes → durable `content_learnings` (status, observation vs generalization confidence)
  - Single-post safeguard: breakout evidence creates `signal`, not `supported`
  - Attribution-aware reasoning; no universal content score; no content cloning
  - Structured Paige recommendations with experiment preserve/vary + evidence path
  - APIs: `/api/content-learning/evaluate/:id`, `/api/content-learnings*`, `/api/paige/content-recommendation`
  - Operator panels on `/content-outcomes` + CLI `npm run content:learning`
  - Migration: `migrations/2026-08-13-paige-outcome-learning.sql`
- Content Outcome Intelligence ([SPEC-092](docs/specs/SPEC-092_Content_Outcome_Intelligence.md)) — product brief used “SPEC-085”
  - Durable publications, immutable performance snapshots, business outcomes, qualitative signals
  - APIs: `/api/content-publications*`, `/api/content-outcomes*`
  - Operator capture UI at `/content-outcomes` + CLI `npm run content:outcome`
  - Intelligence payload for Max consumers; no LinkedIn API; no Paige strategy mutation
  - Migration: `migrations/2026-08-13-content-outcome-intelligence.sql`
- Prospect List Build Proposal & artifact progression ([SPEC-091](docs/specs/SPEC-091_Prospect_List_Build_Proposal.md))
  - `approval_plus_next_request` advances past approved Prospect List Criteria Preview
  - “Approved. Before we build anything…” produces Prospect List Build Proposal (planning-only)
  - Artifact replay guard: approved criteria are not regenerated unless explicitly re-requested
  - Session memory: `lastArtifactType`, `lastArtifactStatus`, `approvedArtifacts`, `nextRecommendedArtifact`, `pendingUserRequest`
- Max Conversational Reasoning Layer ([SPEC-090](docs/specs/SPEC-090_Max_Conversational_Reasoning_Layer.md))
  - Classifies each CIE interview turn (`direct_answer`, `correction`, `add_on`, `approval`, `clarification_request`, `insufficient_answer`, `off_topic`, `skip`) before question handling
  - Session `reasoningMemory`: accepted facts, pending corrections, open questions, confidence/evidence by section, question debt, artifact progression
  - Cross-section add-ons update the correct prior section (e.g. ICP add-on while on avoid)
  - Vague answers get one focused probing follow-up instead of advancing
  - Artifact readiness checks for Blueprint / Growth Direction / Campaign Preview / Prospect Criteria
  - Synthesis helper rewrites into clean business language (no stitched prompt text)
  - Guardrails unchanged: no campaigns, lists, outreach, CRM/DNS/GBP/social/account changes without approval

### Fixed

- Phone/email visible task guidance no longer uses generic capture/follow-up copy
  - Why this matters: interested property managers should not hunt for a way to reach the business
  - What to do: confirm phone and branded email in header, footer, contact page, and estimate/request flow
  - What to confirm: mobile-tappable phone, clear email path, GBP/outreach match, monitored contacts, approval before site changes
  - Who owns it: Max can check; operator approves fixes; complete when phone/email are easy to find, accurate, and monitored
- Contact form works task guidance no longer uses generic capture/follow-up copy
  - Why this matters: a broken form makes the business look unresponsive and can lose qualified property manager inquiries
  - What to do: submit a test inquiry; confirm delivery and reply ownership
  - What to confirm: successful submit, monitored inbox/tracker, usable reply-to, no unapproved tracking/site changes
  - Who owns it: Operator guided; complete when a test submission is received and the follow-up owner is clear
- SPF/DKIM/DMARC present task guidance no longer uses generic capture/follow-up copy
  - Why this matters: domain authentication improves inbox placement and sender trust for outbound mail
  - What to confirm: SPF, DKIM, DMARC (monitoring OK), branded mailbox send/receive, no unapproved DNS changes
  - Who owns it: Operator guided; complete when records are confirmed or DNS changes documented for approval
- Domain owned task guidance no longer uses generic lead-reply language
  - Why this matters: ownership enables website, branded email, tool verification, and brand protection
  - What to confirm: registration active, where managed, who approves DNS, renewal risk, no credentials in Max
  - Who owns it: Client/operator; complete when ownership and access path are confirmed
- Domain connected to website task guidance no longer uses email/lead-routing confirm items
  - What to confirm covers live site load, www/non-www routing, HTTPS, marketing domain match, and approval before DNS/site changes
  - What to do: confirm the domain points to the live site; document A/CNAME changes for approval
  - Who owns it: Max can check, operator/client approves changes
- Growth Workspace guidance clearance: sticky footer no longer covers expanded task guidance
  - Larger bottom scroll padding (`--gw-sticky-footer-clearance`)
  - Chat log hard-hidden + cleared on Open Task Guidance so no simple card sits under guidance
  - Left panel strips any `data-role="simple-task"` nodes; active-task card count stays at 1
- Growth Workspace expanded task guidance: exactly one active-task card; scroll above sticky footer
  - Removed duplicate simple task summary bubble under expanded guidance
  - Left nav becomes the scroll region with bottom padding so the footer does not cover guidance
  - Overview still shows “Task guidance is open in the left panel.”
- Initial Growth Direction avoid sentence no longer bleeds wrapper language
  - Expected: “The Blueprint also clarifies who Anchor should avoid: customers who only care about the lowest price.”
  - Stale stored artifacts are regenerated/repaired on resolve
- Current Task Guidance renders exactly one structured card (no duplicates / clipping)
  - Full sections: Why this matters · What to do · What to confirm · Who owns it · Complete when
  - Branded email available uses Anchor-specific copy; left panel uses normal document flow
  - Overview suppresses the simple Current Task card while guidance is open
  - Regression coverage in `test/clientIntelligenceGrowthWorkspacePanel.test.js`
- Growth Workspace left panel no longer duplicates active task guidance under Previous Plans
  - Sections: Current Growth Plan → Current Task Guidance (after Open Task Guidance) → Previous Plans (historical only)
  - Owner labels render as Client/operator · Operator guided · Max can check (not raw enums)
  - Regression coverage in `test/clientIntelligenceGrowthWorkspacePanel.test.js`

### Fixed

- First Campaign Plan Preview subtype + exclusion polish ([SPEC-089](docs/specs/SPEC-089_First_Campaign_Planning_Conversation.md))
  - Subtype is the polished property-manager first-test description (never an exclusion summary)
  - Exclusion bullets normalized: institutional PMs, complex properties, lowest-price buyers, out-of-area, no decision-maker
- First Campaign Plan Preview renders from structured fields only ([SPEC-089](docs/specs/SPEC-089_First_Campaign_Planning_Conversation.md))
  - No raw transcript stitching into objective/hypothesis/proof/checkpoints
  - Peels unlabeled include/exclude clauses out of Campaign Objective
  - Peels validation metrics out of Campaign Hypothesis
  - Removes duplicated wrappers (`Prove that The first campaign should prove…`) and avoid-customer Blueprint bleed
  - Proof assets no longer truncate into `reliability / Responsiveness`
  - Fragmented checklist sentences repaired (`Target segment. subtype.` → commas)
  - Canonical fields: `campaignObjective`, `coreValidationQuestion`, `campaignHypothesis`, `risks`, `approvalCheckpointsBeforeList`
  - Preview generation stays planning-only (no list, copy, send, CRM, or account changes)
- First Campaign Plan Preview final copy ([SPEC-089](docs/specs/SPEC-089_First_Campaign_Planning_Conversation.md))
  - Target segment never opens with lowercase/internal labels (`property managers — …`)
  - Uses “who oversee …” polished segment prose
  - Tighter objective close: “rather than ignoring the outreach or responding only on price”
  - Artifact uses “Core validation question:” (no first-person “I’d treat the goal as”)
  - Keeps side-panel subtitle: “Hypothesis and validation gates before any build”
- First Campaign Plan Preview polish ([SPEC-089](docs/specs/SPEC-089_First_Campaign_Planning_Conversation.md))
  - Single planning-only footer disclaimer (no duplicate “planning only” banners)
  - Humanized readiness labels (`not_ready` → plain language in risks)
  - Polished target segment prose (no awkward `segment — subtype` joins)
  - Hypothesis no longer doubles geography (`…in Greater Manchester in Greater Manchester`)
  - Concise proof assets + approval checkpoints; review-first next step before any list build

### Added

- First Campaign Planning Conversation ([SPEC-089](docs/specs/SPEC-089_First_Campaign_Planning_Conversation.md))
  - **Plan First Campaign** from Growth Plan completion opens a review-first Max conversation
  - Carries approved Blueprint, Initial Growth Direction, segment ranking, validation target, readiness report, and completed setup checklist
  - Artifact: First Campaign Plan Preview (objective, segment, market, hypothesis, proof, metrics, risks, checkpoints, next step)
  - APIs: `POST /api/v1/interview/:id/campaign/start|message`; no prospect list, outreach copy, sends, CRM writes, or account changes
- Growth Work Continuation Flow ([SPEC-088](docs/specs/SPEC-088_Growth_Work_Continuation_Flow.md))
  - **Resume Growth Plan** opens the Growth Workspace on the first incomplete task (never a Readiness Report dead end)
  - Workspace tabs: Overview · Tasks · Readiness Report · Blueprint · History
  - Setup tasks can be marked complete and auto-advance; completed plans show next-objective options
  - Dashboard shows current plan progress and collapses previous plans
  - API: `POST /api/v1/interview/:id/growth-plan/tasks/:taskId/complete`
- Growth Infrastructure Readiness Conversation ([SPEC-087](docs/specs/SPEC-087_Growth_Infrastructure_Readiness.md))
  - Separate post–Blueprint-approval Max conversation: assess whether the business can capture, convert, and track demand before campaigns
  - Ten readiness areas (domain/DNS, website, GBP, reviews, social, tracking, lead capture, CRM/pipeline, sales process, brand assets)
  - Item model: status / evidence / owner (`max_can_check` | `operator_guided` | `client_required`) / priority / next step
  - Artifact: Growth Infrastructure Readiness Report with setup sequence; no DNS/GBP/social/tracking mutations; no password asks; no campaigns
  - APIs: `POST /api/v1/interview/:id/readiness/start|message`; `/client-intel` CTA **Check Growth Infrastructure**
- Growth Conversation v1 spec ([SPEC-086](docs/specs/SPEC-086_Growth_Conversation.md))
  - Post–Blueprint-approval Max conversation: choose first market segment before campaigns or prospect lists
  - Inputs: approved Blueprint + growth answers only; output: First Growth Plan Preview
  - Flow: confirm Initial Growth Direction → preference / fit / access / deal quality / constraints / proof → preview CTAs
  - Upgrades the existing thin growth chat APIs; full Growth Planning workspace remains deferred

### Fixed

- Executive Business Brief name/voice/observation polish ([SPEC-085](docs/specs/SPEC-085_Executive_Business_Brief.md))
  - Sanitize business names (`Anchor Cleaning we` → `Anchor Cleaning`); never split on compound hyphens like `commercial-focused`
  - Brand voice strips possessive lead-ins (`anchor’s calm…` → `calm, professional, reliable, and easy to work with`)
  - Observations are one concise synthesized sentence each — no raw answer paragraph dumps
  - Grammar guards: no `a Anchor`, `Anchor Cleaning we’s`, `low — price`, or `great — fit`
  - Anchor regression assertions updated in `test/clientIntelligenceInterview.test.js`
- Executive Business Brief consumes normalized evidence, not raw transcript bleed ([SPEC-085](docs/specs/SPEC-085_Executive_Business_Brief.md))
  - Correction messages target the intended domain (`for services`, geography, brand voice, etc.) and never attach to the active question
  - Session `normalizedFacts` store (services, ideal customers, geography, brand voice, …) feeds Brief synthesis
  - Phrase normalization: `standard home` → `standard home cleaning`, `STR companies` → `short-term rental companies`, place title-casing
  - Brand voice renders as “Anchor’s brand voice should feel …” (no “should sound anchor’s …”)
  - Observations synthesized from normalized facts (no “would feel successful if” / “both geography is” fragments)
  - Anchor transcript regression coverage in `test/clientIntelligenceInterview.test.js`
- Max interview conversation handling + Executive Business Brief synthesis hardening ([SPEC-085](docs/specs/SPEC-085_Executive_Business_Brief.md))
  - Classify every interview message before attaching to the active question (`direct_answer`, `supplemental_context`, `refinement_feedback`, `correction`, `question_to_max`, `off_topic`)
  - Supplemental session memory for out-of-order facts (domain-tagged; does not overwrite the active answer)
  - Corrections supersede relevant stored facts; refinement feedback guides regeneration only
  - Conversational Max acknowledgements when users add context / corrections mid-question
  - Brief synthesis layer normalizes raw answers into polished consultant prose (no Mad-Lib template bleed)
  - Rejects raw interview-question fragments (“when a great-fit customer chooses…”, “we will know the growth work…”, etc.)
  - Regression coverage in `test/clientIntelligenceInterview.test.js`
- Executive Business Brief synthesis no longer treats refinement instructions as business facts ([SPEC-085](docs/specs/SPEC-085_Executive_Business_Brief.md))
  - Classifies responses as `business_fact` / `refinement_feedback` / `system_guidance` / `generated_brief`
  - Refinement intent detection (“please refine”, “this revision”, “instructions to Max”, etc.)
  - Revision guidance stored in session metadata — never in who_you_are / who_you_serve / related commercial fields
  - Pre-render sanitization strips meta-instruction snippets from Brief evidence
  - Polished executive synthesis replaces Mad-Lib raw-answer concatenation
  - Regression coverage in `test/clientIntelligenceInterview.test.js` (Anchor facts preserved; banned meta phrases absent; ratings ignore refinement evidence)

### Added

- Executive Business Brief ([SPEC-085](docs/specs/SPEC-085_Executive_Business_Brief.md))
  - Client-facing consultant synthesis after interview: Who You Are → Conversations I'd Recommend Next
  - Premium Understanding Transition (deliberate checklist; target 3–4s; min 2.5s; never stalls after backend)
  - Evidence-backed Initial Observations + Max's Initial Assessment (stars + confidence %)
  - Always identifies Areas I'd Like To Learn More; conversation starters (not prescriptions)
  - Client validation: Yes / Refine / Keep talking → then editable Business Blueprint
  - API field `executiveSummary` carries the Brief payload (title **Executive Business Brief**)
- Client Intelligence Interview Experience ([SPEC-084](docs/specs/SPEC-084_Client_Intelligence_Interview_Experience.md))
  - Welcome → Discovery → Understanding → read-only Executive Summary → editable Blueprint → on-page completion
  - Live understanding progress (titles/confidence/unknowns only); narratives reserved for Understanding reveal
  - Interruptible premium loading + trust bridge before **My Understanding of Your Business**
  - `POST /api/v1/interview/:id/resume` for refine / keep talking
- Client Intelligence Engine v1 thin slice ([SPEC-083](docs/specs/SPEC-083_Client_Intelligence_Engine.md))
  - Text interview → evidence → simple confidence → Business Blueprint → client approve
  - Durable `cie_interview_sessions`, `cie_interview_turns`, `cie_evidence`, `cie_business_blueprints`
  - Approval generates `pending_review` Client Playbook (SPEC-028) from understanding only — no channels/offers/sequences invented; no Scout/Composer activation
  - APIs under `/api/v1/clients/:id/interview/*`, `/api/v1/interview/*`, `/api/v1/blueprint/*`
  - UI: `/client-intel` · CLI: `npm run client:intel:interview`
  - Migration: `migrations/2026-08-06-client-intelligence-engine.sql`
  - Tests: `test/clientIntelligenceInterview.test.js`, `test/clientIntelligenceHandoff.test.js`, `test/clientIntelligenceRoutes.test.js`
- Relationship Intelligence Interview v1 ([SPEC-064](docs/specs/SPEC-064_Relationship_Intelligence_Interview.md))
  - Durable `relationship_interactions` + `relationship_interaction_insights` (soft entity refs; no CRM FKs)
  - Max-owned state-machine debrief service: start / answer / summarize / commit (notes mode first)
  - Review-before-commit draft payload (`isEvidence: true`); mutations only touch RI tables
  - Admin APIs under `/api/v1/relationship-intel/*`
  - CLI: `npm run relationship:intel:interview -- --type=... --notes="..."`
  - Readiness/acceptance: `npm run relationship:intel:readiness` (+ `--accept` notes fixture, `--check`, `--json`); GET `/api/v1/relationship-intel/readiness`
  - Constraint repair: `migrations/2026-08-05-relationship-intelligence-constraints.sql`; readiness parses PG `ANY (ARRAY[...])` CHECK forms
  - Tests: `test/relationshipIntelligenceInterview.test.js`, `test/relationshipIntelligenceRoutes.test.js`, `test/relationshipIntelligenceReadiness.test.js`
- Evidence-Driven Capability Planning ([SPEC-056](docs/specs/SPEC-056_Evidence_Driven_Capability_Planning.md) / [ADR-040](docs/adr/ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md))
  - Three-stage planning: Intent Understanding → Evidence Planning → Capability Planning → MissionPlan
  - MissionIntent declares `requiresEvidence`; EvidencePlan compares catalog vs requirements
  - Missing evidence schedules read-only Discovery Diagnostics before Campaign Review / Outcome Intelligence
  - Unable to answer when required evidence has no registered producer (never invents incomplete diagnostics)
  - Diagnostic artifact types (DiscoveryTrace, DiscoveryDiagnostics, …) — read-only, no business-state mutation
  - Review Workspace **Evidence Requirements** section
  - Tests: `npm run test:mission` (evidencePlanning.test.js)
- Intent Understanding ([SPEC-055](docs/specs/SPEC-055_Intent_Understanding.md) / [ADR-039](docs/adr/ADR-039_Separate_Understanding_from_Execution.md))
  - Two-stage planning: Intent Understanding → MissionIntent → Capability Planning → MissionPlan
  - Semantic intent categories (Campaign Execution, Diagnostics, Discovery Investigation, …) — not capability aliases
  - Confidence + alternate intents; low confidence returns clarification with suggested interpretations
  - Capabilities still consume MissionPlan only — never parse operator language
  - Review Workspace: Operator Request → Understood Intent → Execution Plan
  - Tests: `npm run test:mission` (intentUnderstanding.test.js)
- Capability Registry & Planner Diagnostics ([SPEC-054](docs/specs/SPEC-054_Capability_Registry_and_Planner_Diagnostics.md) / [ADR-038](docs/adr/ADR-038_Explain_Planning_Decisions.md))
  - Capability contract: `version`, `enabled`, `missionAliases` on registry descriptors
  - Registry queries: `producersOf`, `consumersOf`, `resolveAlias`, `suggestMatches`, `explainSelection`
  - Compatibility Resolver ranks registered producers; missing producers emit deterministic diagnostics
  - Unknown mission text → Notes with suggested matches (never bare "Unknown capability")
  - Review Workspace **Planning Diagnostics** section (selected ✓ / blocked ✗ / recommended actions)
  - Tests: `npm run test:mission` (plannerDiagnostics.test.js) · `npm run test:capabilities`
- Business Intelligence Engine ([SPEC-053](docs/specs/SPEC-053_Business_Intelligence_Engine.md) / [ADR-037](docs/adr/ADR-037_Reason_About_Businesses_Not_Companies.md))
  - Analytical `BusinessIntelligenceProfile` replaces descriptive Company Intelligence as the first reasoning artifact
  - Deterministic Level 1–5 reasoning (facts → model → operations → buying psychology → sales input)
  - Quality gates for revenue, constraints, pressures, problem owner, buying urgency — uncertainty explicit when unanswered
  - Sales Intelligence consumes BI; Mail packages carry BI for review provenance
  - Review Workspace order: Business Intelligence → Sales Intelligence → Messaging Strategy → Letter
  - Stage `business_intelligence` in Mission seeds / Artifact Bus / PipelineGate
  - Tests: `npm run test:capabilities` (businessIntelligence.test.js)
- Typed Artifact Validation ([SPEC-052](docs/specs/SPEC-052_Typed_Artifact_Validation.md) / [ADR-036](docs/adr/ADR-036_Trust_Through_Contracts.md))
  - Artifact Validator pipeline: Identify Type → Schema → Semantic → Compatibility
  - Natural language / mission prose never becomes ProspectList (or other structured artifacts)
  - Artifact Bus publishes consumable revisions only after typed validation
  - Review Workspace surfaces `Artifact Validation` failures (reviewable, non-executable)
  - Tests: `npm run test:mission` (typedArtifactValidation.test.js)
- Kalshi BTC research package migrated into the monorepo ([SPEC-049](docs/specs/SPEC-049_Kalshi_Research_Package.md) / [ADR-033](docs/adr/ADR-033_Kalshi_Research_Stays_Isolated.md))
  - `packages/kalshi-research` — deterministic paper/replay research only
  - Feature extraction + `feature-report` CLI; fee-aware replay/train-test tooling preserved
  - Isolated from production: not imported by Node services, not deployed, no live order path
  - Tests: `npm run test:kalshi-research` (pytest inside the package)
- Artifact Resolution & State-Aware Planning ([SPEC-051](docs/specs/SPEC-051_Artifact_Resolution_and_State_Aware_Planning.md) / [ADR-035](docs/adr/ADR-035_Plan_Around_State_Not_Sequence.md))
  - Artifact Resolver sits between Mission Plan and execution graph
  - Required artifacts resolved before capability selection (Current Mission → Operator → Previous → Workspace → Capability)
  - Discovery skipped when a compatible ProspectList already exists (e.g. `prospectList: current`)
  - Capabilities declare `requires` / `produces`; planner records source, confidence, freshness, compatibility
  - Review Workspace shows Artifact Resolution decisions
  - Tests: `npm run test:mission` (artifactResolution.test.js)
- Deterministic Mission Planning ([SPEC-050](docs/specs/SPEC-050_Deterministic_Mission_Planning.md) / [ADR-034](docs/adr/ADR-034_Intent_Before_Execution.md))
  - Intent Parser classifies every sentence into Objective / Parameters / Execution / Options / Notes
  - Mission Plan IR is the only source of executable nodes; Notes never execute
  - Unknown capability text becomes Notes; reserved runtime fields protected
  - MissionExecutor passes Mission Plan objective to capabilities (not raw operator NL)
  - Review Workspace displays parsed Mission Plan before treating guidance as work
  - Tests: `npm run test:mission` (deterministicMissionPlan.test.js)
- Sales Intelligence Engine ([SPEC-048](docs/specs/SPEC-048_Sales_Intelligence_Engine.md) / [ADR-032](docs/adr/ADR-032_Strategy_Before_Language.md))
  - Structured `SalesIntelligenceProfile` between Company Intelligence and channel generators
  - Messaging strategy, evidence-linked personalization claims, quality gates, Human Test / Operator Confidence Score
  - Mail Package + Campaign mailMerge consume the profile (prospect-first openings)
  - Review Workspace shows Sales Intelligence → Messaging Strategy → Score → Letter
  - Operator Approval Rate tracking stub
  - Tests: `npm run test:capabilities` · `npm run test:mission`
- Review Workspace Interaction Layer ([SPEC-047](docs/specs/SPEC-047_Review_Workspace_Interaction_Layer.md) / [ADR-031](docs/adr/ADR-031_Review_Must_Be_Evidence_First.md))
  - Expandable Campaign / MailPackage deliverables and stage cards
  - Campaign Summary metrics navigate to prospects, packages, warnings, ready queue
  - Warning inspector + one-at-a-time mail package review queue (letter preview without Developer Details)
  - Honest affordance; Developer Details remain optional / last
  - Presentation only — no Mission Engine / Artifact Bus / Campaign Review capability changes
- Command Deck UX Polish ([SPEC-045](docs/specs/SPEC-045_Command_Deck_UX_Polish.md) / [ADR-030](docs/adr/ADR-030_Command_Deck_Is_an_Operator_Workspace.md))
  - Persistent Max composer + sticky suggestions + auto-growing prompt
  - Prospect List pastes render as attachment cards (raw preserved under View)
  - Mission Workspace: expandable objective, business input/stage/artifact summaries, review dashboard
  - Pipeline metadata under Developer Details; stage loading bars from existing statuses
  - Presentation only — no Mission execution changes
- Trade Intelligence Engine ([SPEC-046](docs/specs/SPEC-046_Trade_Intelligence_Engine.md))
  - `@pulseforge/trade-intelligence` — daily/weekly reviews, pattern discovery, calibration, explainable recommendations
  - Immutable Findings; Replay: `reviewTrade` · `compareWeek` · `generateDailyReview`
  - Laboratory: `discoverTradePatterns` · `compareTradeStrategies` · `compareTimeWindows` · `compareConfidenceBands`
  - EQL: `SHOW DailyReview FOR Today` · `SHOW WeeklyReview FOR LastWeek` · `SHOW BestHypotheses` · `SHOW TradeCalibration` · `SHOW SimilarTrades FOR Trade("…")` · `SHOW Recommendations`
  - Tests: `npm run test:trade-intelligence` · `npm run test:eql` · `npm run test:laboratory`
- Trade Capture Engine ([SPEC-044](docs/specs/SPEC-044_Trade_Capture_Engine.md))
  - `@pulseforge/trade-capture` — screenshot-first capture (&lt;15s operator path); OCR never blocks save
  - Immutable `chart_snapshot` observations; Trade → Evidence → Claim → Outcome graph
  - Pluggable extractors (OCR / Chart / Pattern / Indicator / CV)
  - Laboratory: `lab.findTrades` · `compareWinningTrades` · `compareLosingTrades`
  - EQL: `FIND Trades` · `SHOW Screenshots FOR Trade("…")` · `COMPARE WinningTrades WITH LosingTrades`
  - Tests: `npm run test:trade-capture` · `npm run test:eql` · `npm run test:laboratory`
- Operator Artifact Injection ([SPEC-043](docs/specs/SPEC-043_Operator_Artifact_Injection.md) / [ADR-029](docs/adr/ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md))
  - Operator ingress publishes validated `ProspectList` onto the Mission Artifact Bus (CSV / paste / manual)
  - Discovery may be marked **Satisfied (Operator Supplied)**; Mission resumes at Company Intelligence
  - Consumers resolve by type / validation status / revision only — producer is provenance
  - Workspace recovery when Discovery blocks: Retry / Import Prospect List / Cancel
  - API: `POST /api/v1/missions/:id/artifacts/inject`
  - Tests: `npm run test:mission` (operatorArtifactInjection.test.js)
- Mission Artifact Bus ([SPEC-042](docs/specs/SPEC-042_Mission_Artifact_Bus.md) / [ADR-028](docs/adr/ADR-028_Business_State_Flows_Through_Artifacts.md))
  - Typed, immutable, versioned business artifacts (`ProspectList`, `OpportunityRanking`, `Campaign`, …)
  - Artifact Registry + Artifact Bus API: publish / get / getLatest / history / validate / compare / replay / consume
  - MissionExecutor publishes after PipelineGate; stages consume validated latest revisions only
  - Quarantined artifacts invisible to consumers; snapshot in `deliverables.artifactBus`
  - Mission Workspace Artifacts section + compare/replay API routes
  - Flag: `MISSION_ARTIFACT_BUS=0` restores flat `priorOutputs` merge only
  - Tests: `npm run test:mission` (artifactBus.test.js)
- Mission Planner — objective-driven execution graphs ([SPEC-041](docs/specs/SPEC-041_Mission_Planner.md) / [ADR-027](docs/adr/ADR-027_Mission_Planning_Is_Objective_Driven.md))
  - Replaces static `TYPE_CAPABILITY_CHAINS` as planning authority with Stage Library + dependency graph
  - Stage keywords (review, mail package, ready to print) **augment** the graph — never collapse Build Campaign into a single stage
  - Review gates planner-managed; `explainPlan` / validate / insert·remove·replace / incremental replan
  - IntentRouter: Build Campaign preferred over later-stage keywords; focused Review/Mail objectives unchanged
  - Mission Workspace + Max reasoning surface execution graph explanations
  - Tests: `npm run test:mission` (missionPlanner.test.js)
- Mission Artifact Validation & Discovery Resolution ([SPEC-040](docs/specs/SPEC-040_Mission_Artifact_Validation.md) / [ADR-026](docs/adr/ADR-026_Business_Success_Determines_Pipeline_Progress.md))
  - Deterministic Discovery Profile resolver (constraints → override → pinned client → client geography → mission default); never silent geography hop
  - Stage artifact contracts + PipelineGate: Completed / Completed With Warnings / Blocked / Failed
  - Empty Discovery yields Blocked (pipeline pauses); shortfalls warn and may advance
  - Downstream stages consume published validated artifacts only; quarantine on failure
  - Flag: `MISSION_ARTIFACT_VALIDATION=0` restores advance-on-technical-complete
  - Tests: `npm run test:mission` (artifactValidation)
- Active Mission Resolver ([SPEC-039](docs/specs/SPEC-039_Active_Mission_Resolver.md) / [ADR-025](docs/adr/ADR-025_Active_Missions_Take_Precedence.md))
  - First routing layer before IntentRouter on Max Workspace Ask + `/api/max/ask`
  - Session ↔ active Mission binding; Resume / Modify / Diagnose attach (never IntentRouter)
  - Diagnostics like “Investigate why Campaign Review failed” stay on the bound Mission
  - Flag: `ACTIVE_MISSION_RESOLVER=0` falls back to SPEC-022 create-on-intent
  - Tests: `npm run test:mission` (activeMissionResolver + workspace precedence)
- Operator Inbox ([SPEC-037](docs/specs/SPEC-037_Operator_Inbox.md) / [ADR-024](docs/adr/ADR-024_Human_Work_Is_Coordinated_Through_the_Operator_Inbox.md))
  - `packages/capabilities/operatorInbox/` — single coordination surface for human-required work
  - Deterministic priority · dedupe · deep links · auditable complete/approve/reject/snooze/assign/archive
  - Ingests Campaign Review, Direct Mail, Outcome Intelligence, validation events — never runs those workflows
  - Mission type `operator_inbox`; IntentRouter patterns (“Open the operator inbox”)
  - Tests: `npm run test:capabilities` · `npm run test:mission`
- Outcome Intelligence ([SPEC-036](docs/specs/SPEC-036_Outcome_Intelligence.md) / [ADR-023](docs/adr/ADR-023_Experience_Becomes_Intelligence.md))
  - `packages/capabilities/outcomeIntelligence/` — capture campaign outcomes → evidence-backed learnings → pending recommendations
  - Ranking feedback + personalization effectiveness + campaign analytics + Mission Outcome Summary
  - Recommendations require operator approval before playbook / ranking / discovery / template updates
  - Distinct from SPEC-013 / ADR-008 (Max recommendation evaluation)
  - Mission type `outcome_intelligence`; IntentRouter patterns (“Capture campaign outcomes”)
  - Tests: `npm run test:capabilities` · `npm run test:mission`
- Direct Mail Execution ([SPEC-035](docs/specs/SPEC-035_Direct_Mail_Execution.md) / [ADR-022](docs/adr/ADR-022_Execution_Consumes_Approved_Artifacts.md))
  - `packages/capabilities/directMailExecution/` — deterministic print → assemble → mail → response state machine
  - Campaign lock after Printing; immutable audit; consumes approved revision only
- Campaign Review Workspace ([SPEC-034](docs/specs/SPEC-034_Campaign_Review_Workspace.md) / [ADR-021](docs/adr/ADR-021_Human_Approval_Before_Execution.md))
  - `packages/capabilities/campaignReview/` — single operator checkpoint before execution
  - Per-prospect + bulk approve / reject / skip / edit / regenerate; validation blocks approval
  - Campaign Ready to Print only after gates pass; execution package (print / mail merge / labels)
  - Revision history (compare / restore / duplicate); Mission Decision + Mission Revision shapes
  - Mission type `campaign_review`; IntentRouter patterns (“Review Campaign 001”)
  - Tests: `npm run test:capabilities` · `npm run test:mission`
- Mail Package Generator ([SPEC-033](docs/specs/SPEC-033_Mail_Package_Generator.md))
  - `packages/capabilities/mail/` — personalized letters, envelopes, insert checklists, CSV / HTML exports
  - Ready-to-Print vs Needs Review validation; revision store; mission type `mail_package_generation`
- Mission Memory proposed ([SPEC-032](docs/specs/SPEC-032_Mission_Memory.md) / [ADR-019](docs/adr/ADR-019_Missions_Are_Conversations.md))
  - Missions are persistent collaborative workspaces; follow-ups refine in place
  - Append-only revision history; capabilities consume current revision; execution uses latest approved revision
  - Smart corrections + clarification; Mission Workspace as canonical conversation
- Business Signals Capability ([SPEC-031](docs/specs/SPEC-031_Business_Signals_Capability.md) / [ADR-018](docs/adr/ADR-018_Time_Matters.md))
  - `packages/capabilities/signals/` — collect → verify → lifecycle → decay; evidence-only observations
  - Categories: growth / operational / marketing / organizational / buying
  - Ranking Buying Signals factor + Opportunity Briefs prefer Active signals; Campaign stub attaches messaging posture
  - Company Intelligence hook: `buildBusinessSignalsStage`; Knowledge writes separate evidence vs inference
  - Tests: `npm run test:capabilities` (signals.test.js)
- Company Intelligence Capability proposed ([SPEC-030](docs/specs/SPEC-030_Company_Intelligence_Capability.md) / [ADR-017](docs/adr/ADR-017_Intelligence_Before_Execution.md))
  - Expands unfinished SPEC-025 enrichment into evidence-only intelligence packages (company, decision makers, signals, personalization, Opportunity Brief, Knowledge handoff)
  - Never fabricate; verified → evidence, uncertain → inference
  - Ships before Execution so Ranking / Campaign / Proposal / Execution consume packages without modification
- Execution Engine proposed ([SPEC-029](docs/specs/SPEC-029_Execution_Engine.md) / [ADR-016](docs/adr/ADR-016_Execution_Does_Not_Decide.md))
  - Approved campaigns → execution plan → durable touch tasks (do, don’t decide)
  - Human-in-the-loop Waiting, Playbook-owned retry/schedule, fail-closed safety, outcome → next Mission
- Client Playbook Capability ([SPEC-028](docs/specs/SPEC-028_Client_Playbook_Capability.md) / [ADR-015](docs/adr/ADR-015_Strategy_Lives_in_the_Playbook.md))
  - `packages/capabilities/playbook/` — versioned strategy assets (who vs how: Profiles target, Playbooks sell)
  - MissionPlanner pins immutable playbook versions into campaign + proposal constraints
  - Campaign Builder stub consumes channels, sequence, offers, constraints (no hardcoded outreach when playbook present)
  - Proposal Generator consumes brand voice, value props, offers, ideal customer, success metrics
  - Seeds: AS Cleaning Co. + Anchor Cleaning; migration `migrations/2026-07-27-client-playbooks.sql`
- Proposal Generator Capability ([SPEC-027B](docs/specs/SPEC-027B_Proposal_Generator_Capability.md) / [ADR-014](docs/adr/ADR-014_Personalized_by_Default.md))
  - `packages/capabilities/proposal/` — personalization engine (not a template engine), 11-section commercial growth proposal, pricing packages, web/printable HTML, version store
  - Mission type `proposal_generation` — “Generate proposal for …” → review-gated deliverable
  - Evidence-backed sections; uncertainty when discovery is thin; interchangeability tests (ADR-014)
  - Migration: `migrations/2026-07-27-proposal-generator.sql`
- Opportunity Ranking Capability ([SPEC-026](docs/specs/SPEC-026_Opportunity_Ranking_Capability.md))
  - `packages/capabilities/ranking/` — explainable 8-factor scoring, Opportunity Briefs, review package
  - Replaces Opportunity Ranking stub; answers “Who should we contact first?”
  - Operator actions: approve / re-rank / exclude / lock / continue to Campaign Builder
  - Evidence-only (absent signals score 0 — no invented buying signals)
  - Tests: `npm run test:capabilities`
- Prospect Discovery Capability ([SPEC-024](docs/specs/SPEC-024_Prospect_Discovery_Capability.md))
  - Profile-driven discovery (Places + fixture provider), transparent ranking signals, review package
- Mission Engine thin slice ([SPEC-022](docs/specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) / [ADR-010](docs/adr/ADR-010_Mission_Engine.md))
  - `packages/mission-engine/` — IntentRouter, MissionPlanner, MissionExecutor, durable store
  - `packages/capabilities/` — CapabilityRegistry, CapabilityRunner; Discovery + Ranking live, enrichment/knowledge/campaign stubs ([SPEC-023](docs/specs/SPEC-023_Capability_Framework.md) / [ADR-011](docs/adr/ADR-011_Capability_Framework.md))
  - Mission-first Max routing: “Build Campaign 001” → MissionPlanner (not Market Intelligence)
  - Command Deck Operations (Mission Queue) beneath Highest Leverage Action; Mission Workspace
  - Shell Operations nav redirects to `/command-deck#operations`
  - API: `POST/GET /api/v1/missions`, `GET /api/v1/missions/:id`, `POST /api/v1/missions/:id/review`
  - Flag: `MISSION_ENGINE` default on; set `=0` to disable
  - Tests: `npm run test:mission` · `npm run test:capabilities`
- Learning & Belief Evolution Engine ([SPEC-021](docs/specs/SPEC-021_Learning_and_Belief_Evolution_Engine.md))
  - `packages/learning/` — LearningEngine, BeliefTracker, CalibrationEngine, OutcomeEvaluator, LearningSession
  - Outcomes calibrate trust after reality is known (no ML / no history·replay·runtime mutation)
  - EQL: `SHOW Calibration FOR Claim("…")`, `SHOW Accuracy FOR StrategyPack("…")`
  - Laboratory: `lab.compareCalibration(...)`, `lab.replayWithCalibration(...)`
- Evidence Query Language ([SPEC-020](docs/specs/SPEC-020_Evidence_Query_Language.md))
  - `packages/eql/` — Parser, QueryPlanner, Executor, EvidenceCatalog
  - Declarative FIND / SHOW / REPLAY / COMPARE / EXPLAIN (read-only)
  - Domain-neutral subject matching (`subject` / `subjectId` / `companyId`)
  - `lab.query(\`…\`)` on Evidence Laboratory
- Evidence Laboratory ([SPEC-019](docs/specs/SPEC-019_Evidence_Laboratory.md))
  - `packages/laboratory/` — EvidenceLab, ScenarioRunner, EvidenceQuery, ComparisonWorkspace, Experiment
  - Isolated experiments for ablations, injections, strategy/ontology comparison, analogs
  - Asks questions of the Evidence Platform via Replay; nothing mutates production (not paper trading)
- Deterministic Replay & Temporal Reasoning Engine ([SPEC-018](docs/specs/SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md))
  - `packages/replay/` — ReplayEngine, ReplaySession, ReplayTimeline, ReplayComparator
  - Regenerates reasoning from immutable observations (no snapshots / cached conclusions)
  - Temporal queries: belief at T, confidence rises, recommendation changes, claim appearance
  - Version-aware comparison across ontology / strategy pack / runtime
  - Determinism hardening: market recommendation ids, context `builtAt`, fixed analog timestamps; runtime injectable clock
- Evidence Platform Architecture ([ADR-009](docs/adr/ADR-009_Evidence_Platform_Architecture.md))
  - Formalizes domain-neutral Evidence Core vs Strategy Pack boundary
  - Stable primitives: Evidence Store, KG, Claim/Confidence engines, Memory, Reasoning Runtime
  - Audit: [EVIDENCE_CORE_DOMAIN_AUDIT.md](docs/architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md)
- Reasoning Runtime Decoupling ([SPEC-015A](docs/specs/SPEC-015A_Reasoning_Runtime_Decoupling.md))
  - `packages/reasoning-runtime/` — domain-neutral `ReasoningRuntime` + StrategyPack / ContextProvider / RecommendationProvider contracts
  - `CRMStrategyPack` + `CRMContextProvider` + `NextBestActionProvider` (DI wrappers; identical CRM evaluate path)
  - Max `ReasoningEngine` orchestrates via runtime; `subjectId` alias for `companyId`; `createCRMStrategyRegistry` alias
  - Architecture: [Reasoning_Runtime_Architecture.md](docs/architecture/Reasoning_Runtime_Architecture.md)
- Knowledge Dual-Write & Operational Readiness ([SPEC-014](docs/specs/SPEC-014_Knowledge_Dual_Write.md))
  - Outbox-first Knowledge writer (`packages/knowledge/dualWrite/`) with idempotent sync ledger
  - Durable tables: `knowledge_outbox`, `knowledge_sync_ledger`, `knowledge_flight_stages`
  - Producer hooks: `dbClient` company/prospect/touchpoint + Scout fan-out (alongside Max signals)
  - Persistent Knowledge boot for Command Deck / Max (`utils/knowledgeRuntime.js`)
  - Admin Validation Dashboard (`/admin/knowledge-health`) + Flight Recorder (`/admin/flight-recorder`)
  - Outbox drain cron (`/cron/knowledge-outbox`) + `npm run knowledge:e2e`
- Outcome Intelligence ([SPEC-013](docs/specs/SPEC-013_Outcome_Intelligence.md) / [ADR-008](docs/adr/ADR-008_Outcome_Intelligence.md))
  - `RecommendationOutcome` model + lifecycle (Generated → … → Successful / Unsuccessful / Inconclusive)
  - Strategy-level performance metrics (precision, recall, promotion, success, lead time)
  - Confidence calibration reports (empirical success by band — never mutates confidence)
  - Drift detection (strategy underperformance, false positives, acceptance, evidence quality)
  - Internal Intelligence Review dashboard (`GET /api/v1/outcome/review`, admin/manager)
  - `packages/max/outcome/` — OutcomeEngine; `POST /api/v1/outcome/records|lifecycle`
- Operator Intelligence ([SPEC-012](docs/specs/SPEC-012_Operator_Intelligence.md) / [ADR-007](docs/adr/ADR-007_Operator_Intelligence.md))
  - Interaction event model + RecommendationLearning aggregates
  - Explicit recommendation outcome lifecycle + internal trust/usefulness signal (never replaces confidence)
  - Adaptive Command Deck presentation (section order / visual dominance — never hide)
  - Max suggestion personalization from tenant conversational preferences
  - Internal Intelligence Quality Dashboard (`GET /api/v1/operator/quality`, admin/manager)
  - `packages/max/operator/` — OperatorEngine; `POST /api/v1/operator/events|outcomes`
- Live Intelligence Loop ([SPEC-011](docs/specs/SPEC-011_Live_Intelligence_Loop.md) / [ADR-006](docs/adr/ADR-006_Live_Intelligence_Evolution.md))
  - Common `IntelligenceEvent` model + lifecycle (Detected → Archived)
  - `packages/max/live/` — LiveLoopEngine, deck diff, material filter, awareness
  - Soft-poll evolution on Command Deck (gentle fade / one-shot movement; briefing accumulates)
  - Max awareness during active workspace sessions
  - Investigation continuity banner (“New intelligence available / Review”)
  - Per-entity live timeline; notifications limited to material events
  - `GET /api/v1/intelligence/live`, `/notifications`, `/timeline/:entityId`
- Intelligence Navigation ([SPEC-010](docs/specs/SPEC-010_Intelligence_Navigation.md))
  - Investigation trail + Related Intelligence on every node
  - Company Intelligence + Recommendation Detail composers (closes SPEC-006 remaining pages)
  - `GET /api/v1/recommendations/:id`, `GET /api/v1/companies/:id/intelligence`
  - Progressive evidence depth; Max evidence/related entities navigable; MaxContext synced to trail
  - Deep links `#/recommendation/:id`, `#/company/:id`, `#/evidence/:id` on `/command-deck`
- Command Deck UI ([SPEC-008](docs/specs/SPEC-008_Command_Deck_UI.md)) — render-only surface
  - `GET /command-deck` — Morning Brief, Highest Leverage Action, Intelligence Cards, Priority Queue, Ask Max launcher
  - Consumes only `CommandDeckModel` from `GET /api/v1/command-deck`
  - Staged reveal, composer empty states, calm error + last-successful recovery
  - Shell nav link for admin / manager / viewer / client; `/dashboard` unchanged
- Max Intelligence Workspace ([SPEC-009](docs/specs/SPEC-009_Max_Intelligence_Workspace.md))
  - Contextual full-height Ask Max modal from Command Deck entry points (Morning Brief, HLA, Priority Queue, Watch Alerts, launcher)
  - `packages/max/workspace/` — WorkspaceEngine, OpeningState, Suggestions, ResponseComposer, PresentationEngine
  - Deterministic Structured Response Object → Claude presentation only ([ADR-005](docs/adr/ADR-005_LLM_Presentation_Engine.md))
  - `POST /api/v1/max/workspace/open`, `POST /api/v1/max/workspace/ask`
  - Evidence panel + collapsed “Generated from” metadata; session context + switch acknowledgement
  - Legacy dashboard `/api/max/ask` unchanged

### Planned

- Wire Max agent (shadow) to `brief()` + `decide()` + `compose()` before side effects
- Durable IntelligenceEvent / Operator InteractionEvent / Outcome log / SSE transport
- Full Brevo/call/meeting dual-write coverage beyond touchpoint hooks
- One business week of Anchor operated entirely through Pulseforge (SPEC-014 success metric)

### Docs

- SPEC-018 Deterministic Replay & Temporal Reasoning Engine
- SPEC-013 Outcome Intelligence + ADR-008 Outcome Intelligence
- SPEC-012 Operator Intelligence + ADR-007 Operator Intelligence
- SPEC-011 Live Intelligence Loop + ADR-006 Live Intelligence Evolution
- SPEC-010 Intelligence Navigation
- SPEC-009 Max Intelligence Workspace + ADR-005 LLM Presentation Engine
- SPEC-008 Command Deck UI approved and indexed
- SPEC-006 Command Deck product surface remains the parent v1.0 experience spec
- Product Constitution §11 Cognitive load
- Roadmap / CURRENT_STATE / Product Experience / v1.0 release plan aligned to Command Deck

## [0.9.2] — 2026-07-26

### Added

- Command Deck Composition Engine ([SPEC-007](docs/specs/SPEC-007_Command_Deck_Composition_Engine.md))
  - `packages/max/commandDeck/` — CommandDeckComposer, IntelligenceCard contract, empty states
  - `max.compose({ tenantId, asOf, period })` → immutable `CommandDeckModel`
  - Assembles Morning Brief, Highest Leverage Action, Watch Alerts, Market Trends, Priority Queue
  - Explainability metadata on every card; composer-owned empty states
  - `GET /api/v1/command-deck` — one API, one payload, render-only UI contract

### Notes

- Reasoning / Memory / Briefing / Policy cores unchanged
- Enables SPEC-006 Command Deck UI without dashboard-side intelligence orchestration

## [0.9.1] — 2026-07-26

### Added

- Policy & Decision Engine ([SPEC-005](docs/specs/SPEC-005_Policy_Decision_Engine.md))
  - `packages/max/policy/` — PolicyEngine, RuleRegistry, seven initial rules
  - `policy.evaluate({ tenantId, recommendation, context })` / `max.decide(...)`
  - Data-driven per-tenant policy; immutable audit trail; explainability chain
  - Outcomes: allow, warn, requireApproval, block — evaluation only (no execution)

### Notes

- Reasoning / Memory / Briefing cores unchanged; runtime agents remain unwired

## [0.9.0] — 2026-07-26

### Added

- Max Briefing Engine ([SPEC-004](docs/specs/SPEC-004_Max_Briefing_Engine.md))
  - `packages/max/briefing/` — assembles Knowledge + Reasoning + Memory into structured briefings
  - `max.brief({ tenantId, asOf, period })` — daily / weekly / monthly digests
  - Sections: summary, priorities, changes, watchAlerts, risks, recommendations, metrics
  - Deterministic prioritization; Presentation Adapter extension point (structured + markdown)
  - Briefing never calls `evaluate()` — assembles existing intelligence only

### Notes

- Reasoning + Memory cores unchanged; runtime agents remain unwired
- Default output is domain objects only (no UI formatting)

## [0.8.1] — 2026-07-26

### Added

- Temporal Intelligence & Memory ([SPEC-003](docs/specs/SPEC-003_Temporal_Intelligence_Memory.md))
  - `packages/max/memory/` — append-only snapshots, deterministic diffs, change detection
  - Timeline history, recommendation evolution (trend + linear forecast), temporal explanations
  - Memory queries: `whatChanged`, `whyChanged`, `history`, `trend`, `scoreHistory`, `confidenceHistory`
  - Watch registration (detection only — no notifications)
  - Repository parity: InMemory + Serializing snapshot stores

### Notes

- Reasoning core (SPEC-002) unchanged; runtime agents remain unwired
- Snapshots are structured state only — no LLM output

## [0.8.0] — 2026-07-26

### Added

- Max Reasoning Engine ([SPEC-002](docs/specs/SPEC-002_Max_Reasoning_Engine.md))
  - Package `packages/max/` — ReasoningContextBuilder, Strategy Registry, seven strategies
  - Weighted ScoreAggregator with independent confidence (never mixed into score)
  - RecommendationBuilder, ExplanationEngine, ReasoningReport (no LLM)
  - Deterministic tests via `npm run test:max`

### Notes

- Graph access only through KnowledgeService query API — no repository access from reasoning
- Runtime agents/server remain unwired; existing Max briefing behavior unchanged
- Score and confidence are separate; contradictions are first-class on every strategy

## [0.7.4] — 2026-07-26

### Added

- Knowledge Query Engine ([SPEC-001C](docs/specs/SPEC-001C_Knowledge_Query_Engine.md))
  - `packages/knowledge/query/` — QueryEngine, Filters, Traversal, Timeline, Metrics
  - KnowledgeService query API: `findCompanies`, `findPeople`, `findInteractions`, `neighbors`, `related`, `timeline`, `path`
  - Enhanced `explain()` with timeline position (Claim → Evidence → Source → Confidence → Timeline → Reason)
  - Structured per-query metrics (`queryName`, timing, nodes/edges, repository type)
  - In-memory + Postgres repository parity tests

### Notes

- Legacy `(tenantId, …)` signatures for `findEvidence` / `findClaims` / `explain` preserved
- No GraphRepository contract changes; agents/server remain unwired
- Numbered SPEC-001C to avoid colliding with draft SPEC-002 (Max Reasoning Engine)

## [0.7.3] — 2026-07-26

### Added

- Persistent knowledge store ([SPEC-001](docs/specs/SPEC-001_Persistent_Knowledge_Store.md))
  - Postgres tables `knowledge_nodes`, `knowledge_edges`, `knowledge_evidence`, `knowledge_claims`
  - `PersistentGraphRepository` implementing the existing `GraphRepository` contract
  - Migration `2026-07-26-knowledge-graph-persistent.sql`
  - Postgres tests via `npm run test:knowledge:postgres`

### Notes

- `KnowledgeService` public API unchanged (hash-guarded in tests)
- Default runtime remains in-memory unless a persistent repository is injected
- Agents/server remain unwired

## [0.7.2] — 2026-07-26

### Added

- Graph synchronization engine ([SPEC-001B](docs/specs/SPEC-001B_Graph_Synchronization_Engine.md))
  - `GraphSyncEngine` with idempotent `apply` / `applyMany` / `rebuildFromRelational`
  - CRM mappers for companies, prospects, touchpoints, import batch items
  - `InMemorySyncLedger` + `MemoryRelationalSource` + read-only `PostgresRelationalSource`
  - `KnowledgeService.ensureNode` / `ensureEdge` and `EvidenceEngine.ensureEvidence`
  - Idempotent `KnowledgeIngestor` (stable evidence IDs)

### Notes

- No server/agent wiring — production runtime unchanged

## [0.7.1] — 2026-07-26

### Added

- Knowledge layer foundation ([SPEC-001A](docs/specs/SPEC-001A_Knowledge_Layer_Foundation.md))
  - Package `packages/knowledge/` with `KnowledgeService` as the only public graph API
  - `GraphRepository` contract + `InMemoryGraphRepository`
  - `EvidenceEngine`, `ClaimEngine`, confidence helpers
  - Event bus + ingestor (`KnowledgeEventBus`, `KnowledgeIngestor`)
  - `explain()` chain: Claim → Evidence → Original Source → Confidence → Reason
  - Unit tests via `npm run test:knowledge`

### Notes

- No runtime wiring — existing agents/server behavior unchanged
- No persistent graph store yet (deferred to SPEC-001)

## [0.7.0] — 2026-07-26

### Added

- Repository foundation as source of truth ([SPEC-000](docs/specs/SPEC-000_Repository_Foundation.md))
  - Root: `README.md`, `CONTRIBUTING.md`, `PROJECT_CONTEXT.md`, `CURRENT_STATE.md`, `DECISIONS.md`, `CHANGELOG.md`
  - `docs/00_START_HERE.md`
  - Vision suite under `docs/vision/`
  - Architecture suite under `docs/architecture/`
  - Spec templates + SPEC-000/001/002 under `docs/specs/`
  - ADR templates + ADR-001–004 under `docs/adr/`
  - Release plans `v0.7.0` → `v1.0` under `docs/releases/`

### Notes

- Does not change runtime behavior, schema, or production flags.
- Existing flat runbooks under `docs/*.md` remain valid operational references.

## Pre-0.7.0 (summary)

Prior work lived without a versioned product changelog. Notable engineering streams already in-tree:

- Multi-agent lead-gen CRM (Scout, Emmett, Riley, Max, social, setter/closer)
- Max prospect orchestration Phase 1–2.5 (shadow-default)
- Inquiry Foundation, work queue, outbound outbox, Operator Command Center (local/shadow)
- Anchor Cleaning buyer Scout + verified queue / phone setter tooling
- Revenue projection Phase 15–16B tooling and certification docs

See individual files under `docs/` for stream-specific history.
