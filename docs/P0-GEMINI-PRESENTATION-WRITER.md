# P0 Gemini Presentation Writer

## Scope

This change replaces only Max's PresentationEngine writer. Max reasoning, routing,
evidence retrieval, authorization, deterministic bypasses, and deterministic
fallback remain unchanged. The request is still capped at 900 output tokens and
30 seconds. There are no retries or provider failovers.

## Selected model and pricing

The selected model is the stable `gemini-2.5-flash-lite` Gemini API model.
Google's official documentation was checked on 2026-11-12:

- Standard paid-tier text/image/video input: **$0.10 per 1M tokens**
- Standard paid-tier output, including thinking tokens: **$0.40 per 1M tokens**
- Pricing: <https://ai.google.dev/gemini-api/docs/pricing#gemini-2.5-flash-lite>
- Model status: <https://ai.google.dev/gemini-api/docs/models/gemini-2.5-flash-lite>

Google labels the model stable and not deprecated, but states that 2.5 model
access is limited to projects that used it previously. New projects are directed
to newer Flash-Lite models. For this reason, published status and pricing are
verified, but this account's access must be verified before deployment.

## Deployment prerequisite

Production currently has no `GEMINI_API_KEY`, and the development environment has
no Google AI credential. An operator must enable billing for the intended Google
AI project, confirm that its API key can access `gemini-2.5-flash-lite`, and add
that key to Railway as `GEMINI_API_KEY` only after merge/deployment approval. Do
not reuse `GOOGLE_PLACES_KEY`, OAuth refresh tokens, service-account JSON, or an
Anthropic credential.

If the account is not eligible for Gemini 2.5 Flash-Lite, verify the least-cost
stable Flash-Lite model exposed to that same account and set its exact model name
as `MAX_PRESENTATION_MODEL` before deployment. This is a single-model selection,
not runtime failover.

Without the dedicated key, PresentationEngine performs no provider request and
continues to return deterministic source-grounded prose.
