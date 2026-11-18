'use strict';

/**
 * Single source of the TurnFrame interpreter model (P0-MAX-REASONING-001 PR 2).
 *
 * The interpreter, the semantic shadow observer and the offline evaluation
 * used three different implicit defaults (claude-haiku-5-5 in the producer,
 * MAX_WORKSPACE_MODEL → claude-sonnet-4-6 in the observer and the eval). The
 * model is now explicit configuration only: MAX_INTERPRETATION_MODEL. There is
 * deliberately no fallback to MAX_WORKSPACE_MODEL (a different job with a
 * different cost profile) and no built-in default: choosing the interpreter
 * model is an approval decision, so an unconfigured model is reported, never
 * guessed.
 */

const ENV_KEY = 'MAX_INTERPRETATION_MODEL';
// Model ids are short, lowercase identifiers (aliases or dated snapshots).
const MODEL_ID = /^[a-z0-9][a-z0-9._-]{1,79}$/;

/**
 * @param {Record<string, string|undefined>} env
 * @returns {{ configured: true, model: string, source: string } | { configured: false, model: null, reason: string }}
 */
function resolveInterpreterModel(env = process.env) {
  const raw = env && typeof env[ENV_KEY] === 'string' ? env[ENV_KEY].trim() : '';
  if (!raw) return { configured: false, model: null, reason: 'interpreter_model_not_configured' };
  if (!MODEL_ID.test(raw)) return { configured: false, model: null, reason: 'interpreter_model_invalid' };
  return { configured: true, model: raw, source: ENV_KEY };
}

const PROVIDER_ENV_KEY = 'MAX_INTERPRETATION_PROVIDER';
const PROVIDERS = Object.freeze(['anthropic', 'gemini']);

/**
 * The interpreter provider. Explicit when set (anthropic | gemini). Unset keeps
 * the historical transport (anthropic) so existing configurations behave
 * exactly as before; switching provider is a configuration decision that
 * needs its own approval, and an unknown value is reported, never guessed.
 * @returns {{ configured: true, provider: string, source: string, explicit: boolean } | { configured: false, provider: null, reason: string }}
 */
function resolveInterpreterProvider(env = process.env) {
  const raw = env && typeof env[PROVIDER_ENV_KEY] === 'string' ? env[PROVIDER_ENV_KEY].trim().toLowerCase() : '';
  if (!raw) return { configured: true, provider: 'anthropic', source: 'default_historical', explicit: false };
  if (!PROVIDERS.includes(raw)) return { configured: false, provider: null, reason: 'interpreter_provider_invalid' };
  return { configured: true, provider: raw, source: PROVIDER_ENV_KEY, explicit: true };
}

const escapeRe = (v) => v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * Is the provider-reported model the requested one?
 * - anthropic: the alias itself or a dated snapshot of it (`<id>-YYYYMMDD`);
 * - gemini: `modelVersion` equal to the id, optionally `models/`-prefixed, or a
 *   numbered stable version of it (`<id>-NNN`).
 * Anything else (another family, a preview, a different size) is a substitution.
 */
function servedModelMatches(requested, served, provider = 'anthropic') {
  if (typeof requested !== 'string' || typeof served !== 'string') return false;
  if (served === requested) return true;
  const id = escapeRe(requested);
  if (provider === 'gemini') return new RegExp(`^(?:models/)?${id}(?:-\\d{3})?$`).test(served);
  return new RegExp(`^${id}-\\d{8}$`).test(served);
}

module.exports = { ENV_KEY, PROVIDER_ENV_KEY, PROVIDERS, resolveInterpreterModel, resolveInterpreterProvider, servedModelMatches };
