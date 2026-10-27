'use strict';

const { isResearchProviderControlError } = require('./anchorResearchProviderPause');

// A research-provider pause rejects before the agent's own run logging, so a
// blocked Scout or enrichment run used to leave only stdout. Record one durable
// agent_log row so "blocked by governance" is distinguishable from "did not run"
// and from "ran and found nothing". Best-effort: never masks the control error.
async function logResearchProviderBlock({ agentName, action, clientId, error, trigger = null, db = null } = {}) {
  if (!isResearchProviderControlError(error)) return null;
  const parsed = Number(clientId);
  const client = Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
  try {
    const pool = db || require('../db');
    const result = await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
      VALUES ($1, $2, $3::jsonb, 'skipped', $4, NOW(), $5)
      RETURNING id
    `, [agentName, action, JSON.stringify({ blocked: true, reason: error.code, trigger }), error.code, client]);
    return result.rows?.[0]?.id ?? null;
  } catch (err) {
    console.error(`[${agentName}] research block log failed: ${err.message}`);
    return null;
  }
}

module.exports = { logResearchProviderBlock };
