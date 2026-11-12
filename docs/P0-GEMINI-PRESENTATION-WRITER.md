# P0 Gemini Presentation Writer

## Scope

This change replaces only Max's PresentationEngine writer. Max reasoning, routing,
evidence retrieval, authorization, deterministic bypasses, and deterministic
fallback remain unchanged. The request is still capped at 900 output tokens and
30 seconds. There are no retries or provider failovers.

## Selected model and pricing

The selected model is the stable `gemini-3.1-flash-lite` Gemini API model.
Google's official documentation was checked on 2026-11-12:

- Standard paid-tier text/image/video input: **$0.25 per 1M tokens**
- Standard paid-tier output, including thinking tokens: **$1.50 per 1M tokens**
- Pricing: <https://ai.google.dev/gemini-api/docs/pricing#gemini-3.1-flash-lite>
- Model status: <https://ai.google.dev/gemini-api/docs/models/gemini-3.1-flash-lite>

Google labels the model stable. Its published shutdown date is May 7, 2027,
with `gemini-3.5-flash-lite` named as the replacement. The production credential's
single `models.list` discovery request on 2026-11-12 confirmed that this exact
model supports `generateContent`; the lower-priced `gemini-2.5-flash-lite`
returned HTTP 404 for the same credential.

## Deployment prerequisite

Production has a dedicated `GEMINI_API_KEY` in Railway. The key remains confined
to that service environment and was used only for the access check and one
read-only model discovery request. No provider credential is committed or copied.
The selected model is fixed in the writer; there is no runtime failover.

Without the dedicated key, PresentationEngine performs no provider request and
continues to return deterministic source-grounded prose.
