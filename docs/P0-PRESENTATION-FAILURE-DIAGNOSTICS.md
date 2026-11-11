# Presentation provider failure diagnostics

`PresentationEngine` retains deterministic fallback and the existing client error
codes. Its server-only warning now carries `MAX_PRESENTATION_PROVIDER_FAILURE`
(schema version 1), provider, presentation stage, application error code, provider
error type, HTTP status, provider request ID, explicit request field, canonical
validation description, turn/session IDs, tenant ID, operator turn sequence and
deployed revision (`RAILWAY_GIT_COMMIT_SHA`, otherwise `GIT_COMMIT`).

Correlate workspace turns using session ID and the monotonic operator turn
sequence; each `ask` also receives a unique diagnostic turn ID. Async-local
context isolates simultaneous turns. Standalone presenter calls have null turn
context. Missing or invalid revision/identifiers are null, never guessed.

Only allowlisted provider types and request-schema field paths are emitted.
Fields are reported only from provider `param`/`parameter` or an explicit
`field: description` prefix. Missing fields are not inferred from status, model,
or free-form exception prose. Recognized validation messages become fixed
summaries (required, prohibited, type, numeric range). Other messages are omitted;
they are never truncated and logged. Validation text is inspected only up to
4096 characters; output descriptions are fixed strings shorter than 160.

Provider request IDs require `req_` followed by 1–100 alphanumeric characters.
Schema paths are bounded to 120 characters, UUIDs to 36, numeric tenant IDs to 20,
and hexadecimal revisions to 7–40. Unrecognized values become null. The code
does not traverse headers, arbitrary error properties, prompts or responses,
and does not invoke exception getters or serialization hooks. A malformed error
or failed diagnostic sink cannot interrupt fallback or trigger another call.

This is diagnostic instrumentation, not a repair of the unknown production HTTP
400 cause. Unknown validation descriptions intentionally remain omitted. No
model, token budget, prompt, retry, routing, authorization or flag changes are
included. The diagnostic fields do not enter operator-facing response metadata.
