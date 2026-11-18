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

/** The provider may report a dated snapshot of the requested alias; nothing else is the same model. */
function servedModelMatches(requested, served) {
  if (typeof requested !== 'string' || typeof served !== 'string') return false;
  if (served === requested) return true;
  const escaped = requested.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^${escaped}-\\d{8}$`).test(served);
}

module.exports = { ENV_KEY, resolveInterpreterModel, servedModelMatches };
