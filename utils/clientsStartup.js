'use strict';
const PHASES = Object.freeze([
  'ensureClientArchitecture', 'ensureEmmettAutosendSchema', 'enforceMiraClientState',
  'ensureAoFieldSchema', 'ensureAoProspectRoutingSchema', 'ensureAoCrmSchema',
]);
// Observability only: retain phase order and stop-on-error. No retry/readiness changes.
async function runClientsStartup(steps, logger = console) {
  for (const phase of PHASES) {
    try {
      await steps[phase]();
    } catch (error) {
      const sqlstate = typeof error?.code === 'string' && /^[0-9A-Z]{5}$/.test(error.code)
        ? error.code : 'unknown';
      logger.error('[clients] startup', { phase, outcome: 'failed', sqlstate });
      return false;
    }
    logger.log('[clients] startup', { phase, outcome: 'succeeded' });
  }
  logger.log('[clients] startup', { phase: 'complete', outcome: 'succeeded' });
  return true;
}
module.exports = { runClientsStartup, PHASES };
