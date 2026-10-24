# SPEC-JEV-001 — Mission Routing Shadow Evaluator

## Scope and production invariant

DecisionService observes accepted operator turns and records a Jev recommendation.
It never selects a production route, modifies a session/mission, consumes an
approval, executes a specialist, or changes a response. Shadow evaluation and
Jev provider flags default off.
There is deliberately no live routing mode or confidence threshold for execution.
Coverage is the Max/workspace, mission, and AO operator-message entry points.
Structured UI approval buttons, onboarding forms, and intelligence interviews
do not enter this mission-routing message stream.

## Routing findings

- `routes/maxWorkspace.js` validates tenant/session access before calling
  `WorkspaceEngine.ask`. The engine resolves the primary objective and session
  state, then handles early session/execution inspections and compound turns.
- `OperatorIntent` reads the current mission and pending decision;
  `WorkspaceOwnershipResolver` owns mission, specialist, identity, inspection,
  and reasoning dispatch. `ActiveMissionGuard` preserves session-bound mission
  context. `MissionRuntimeDispatch` separates legacy and acquisition missions.
- `PendingDecisionResolver`, `PendingDecisionTurn`, `AmoOperatorApproval`, and
  `AcquisitionMissionExecution` handle clarification, approval, rejection, and
  stage advancement. The new service does not call them or bypass their gates.
- `WorkspaceMissionInspection`, `SessionInspectionOperator`, and
  `ExecutionInspectionOperator` own read-only inspection responses.
- The older `routes/maxChat.js` endpoint has its own identity, active-mission,
  AO-briefing, and intelligence paths, so it has a separate response observer.
- The AMO ask/execute, AO ask/respond, and dedicated AO briefing ask endpoints
  also receive the response observer. Execute requests with no operator message
  are not evaluated; fixed handler hints are audit-only. AO flows without a
  canonical route produce unavailable comparisons.
- Existing ownership, mission-inspection, and approval audits emit structured
  JSON console events. DecisionService uses the same log transport with a
  dedicated `DECISION_SHADOW_EVALUATED` event. SPEC-JEV-002 adds an independent,
  best-effort Postgres copy for review; production routing has no dependency on it.

## Integration and lifecycle

`WorkspaceEngine.ask` wraps the unchanged `_askProduction` body. It takes an
allowlisted, redacted input snapshot and observes the returned result or error.
Once production has read a mission in `OperatorIntent`, a local observer copies
its pre-action state, including the pending decision. No extra mission lookup or
hydration is performed by shadow evaluation. Internal `_miepInternal` calls are
excluded so compound messages produce one audit per operator turn.

The HTTP observer wraps `res.json` after normal input/client resolution;
it preserves the response object, status, and return value. Invalid/unauthorized
requests rejected before either entry point are outside evaluation scope. The
auxiliary endpoints supply limited context because they have no workspace
transcript available at their HTTP boundary.

Provider work starts on a deferred task after the production result is available;
it is never awaited by routing. Every enabled turn normally produces one terminal
event: `evaluated`, `fallback`, `error`, or `skipped`. Requests beyond the pending
limit produce a `capacity_limit` event without a provider request. Timeouts abort
fetch and race even a provider that ignores abort. There are no automatic retries
and no fallback LLM calls. Failures fall back to a noop observation with null
recommendation fields; current routing continues independently.

Stdout logs remain best-effort process logs. The
[SPEC-JEV-002 review path](SPEC-JEV-002_Decision_Shadow_Review.md) independently
copies events into Postgres and provides `npm run decision:review`. Successfully
inserted rows are durable; failed writes or process termination can still lose
observations. No unbounded in-memory audit history is kept.
`DecisionService.drain()` waits for evaluations and persistence for tests or
graceful shutdown; normal routing must not await it.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DECISION_SHADOW_ENABLED` | `false` | Exact `true` enables observations and logs. |
| `DECISION_PROVIDER` | `noop` | `jev` selects Jev; unsupported values fall back to noop. |
| `JEV_ENABLED` | `false` | Separate explicit gate for outbound Jev requests. |
| `JEV_API_KEY` | empty | TypeSafe API key; `TYPESAFE_API_KEY` is an accepted fallback. |
| `JEV_MODEL` | `jev-latest` | Jev model name or version. |
| `JEV_TIMEOUT_MS` | `1500` | Full request/body timeout, valid range 10–10000 ms. |
| `DECISION_SHADOW_MAX_PENDING` | `64` | Bound deferred/in-flight evaluations, valid range 1–1000. |
| `DECISION_SHADOW_LOG_RAW` | `false` | Include a validated, allowlisted response projection. |

To collect noop observations, set only `DECISION_SHADOW_ENABLED=true`. To evaluate
with Jev, also set `DECISION_PROVIDER=jev`, `JEV_ENABLED=true`, and a key through
the deployment's secret manager. Set `DECISION_SHADOW_ENABLED=false` to disable
both evaluations and their logs. Configuration is read when each service is
constructed; restart the application after environment changes. Existing
in-flight evaluations can finish during shutdown. No flags or keys are enabled
or provisioned by this implementation.

SPEC-JEV-002 storage defaults on when shadow mode is enabled and DATABASE_URL is
configured, after applying its migration. Set `DECISION_SHADOW_PERSIST_ENABLED=false`
to keep stdout-only observation. Disabled shadow mode also disables persistence.

## Provider contract and validation

`packages/decision-service/types.d.ts` defines `DecisionProvider`, `DecisionState`,
and `RoutingDecision`. `schema.js` provides a JSON-schema-shaped contract and
strict runtime validation without adding dependencies. Numeric probabilities
must be finite numbers in [0, 1]; missing fields, unknown enums, extra decision
fields, numeric strings, and string booleans fail validation.

The Jev adapter uses the documented
[`POST /v1/systemone` API](https://docs.typesafe.ai/api), with `state`, `model`,
and typed `questions`. Choice answers provide intent, route, and misrouting risk;
Noul answers provide mission binding, approval, inspection, and clarification
probabilities. `confidence` is Jev's route-choice confidence. The boolean
`requires_human_clarification` is derived from its Noul answer at 0.5, for logging
only. Choice distributions must cover exactly the requested choices, sum to 1
within 0.01 rounding tolerance, and choose a maximal-probability option. The full
HTTP response is capped at 64 KiB before parsing. Redirects are rejected.

## Audit and comparison

Each event includes a unique `decision_id`, tenant/session correlation where
available, message index/length, mission ID, timestamp, schema version, source,
provider/requested/resolved model, routing and provider latency, status/errors,
all eight decision fields, and the observed current route. Messages without an
existing session use the resulting workspace session ID. Authentication session
cookies/IDs are never used as legacy correlation IDs.

The comparison maps actual response metadata into the shared route vocabulary:
mission, approval, inspection, clarification, conversation, identity,
session configuration, specialist, and intelligence. It preserves the original
route, owner, pipeline, action, and primary objective alongside that mapping.
Compound responses, production failures, missing route evidence, and Jev's
`unknown` recommendation have `comparison=unavailable` and `route_matches=null`.
No routing classifier is rerun to invent a production outcome. A mismatch is an
observation, not evidence that Jev is correct.

## Data handling

The provider receives at most 8,000 message characters, four bounded recent
messages, selected session policy fields, and a bounded mission/pending-decision
summary. Credential patterns, authentication strings, URLs, emails, and phone
numbers are redacted before transmission. Arbitrary context fields, CRM lists,
headers, cookies, and environment variables are never copied. Pattern redaction
is not a guarantee that all sensitive prose is removed; enable Jev only where
this bounded contextual data is appropriate for the provider.

Audit logs omit message/transcript/objective/prompt text. Optional raw logging
retains only validated enum/numeric answer fields, model, and token counts;
unknown provider fields and echoed prose are dropped. Malformed JSON and error
bodies are never logged. Errors contain bounded known codes and HTTP status,
not exception messages, stacks, credentials, or request/response headers.

## Validation

Run `npm run test:decision` for schema, provider, redaction, configuration,
timeout, saturation, audit-failure, HTTP-observer, and real workspace regression
tests. Workspace tests compare the wrapper with the original production body
with shadow mode both disabled and enabled, including approval, clarification,
session configuration, identity, inspection, mission state, and compound turns.
All provider calls use local fakes; no live Jev request is required.

Implementation validation on 2026-09-20: all 20 new tests passed. An additional
164 existing routing, mission, approval, inspection, and AO tests ran: 152 passed
and 12 failed. The same 12 tests failed on untouched base commit `4d8b412`, with
matching failure assertions. They concern existing discovery-result wording,
inspection wording/ownership, and older audit expectations. No existing test
expectations or production routing logic were changed to make them pass.
