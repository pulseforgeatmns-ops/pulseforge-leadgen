'use strict';

const pool = require('../db');
const { normalizeClientId } = require('./clientContext');
const { normalizeAgentName } = require('./agentAvailability');

const BLOCK_REASON = 'agent_not_enabled_for_client';

/**
 * Fail-closed dispatch gate for generic agent boundaries.
 * Accepts a numeric client id or a client row with enabled_agents.
 *
 * @returns {Promise<{ allowed: boolean, reason: string|null, normalizedAgent: string|null }>}
 */
async function isAgentEnabledForClient(clientOrId, agentName) {
  const normalizedAgent = normalizeAgentName(agentName);
  if (!normalizedAgent) {
    return { allowed: false, reason: 'unknown_agent', normalizedAgent: null };
  }

  let enabledAgents = null;
  let clientId = null;

  if (clientOrId != null && typeof clientOrId === 'object') {
    clientId = normalizeClientId(clientOrId.id);
    if (!Array.isArray(clientOrId.enabled_agents)) {
      return {
        allowed: false,
        reason: 'enabled_agents_unavailable',
        normalizedAgent,
      };
    }
    enabledAgents = clientOrId.enabled_agents;
  } else {
    clientId = normalizeClientId(clientOrId);
    if (!clientId) {
      return {
        allowed: false,
        reason: 'client_context_unresolved',
        normalizedAgent,
      };
    }
    try {
      const { rows } = await pool.query(
        'SELECT id, enabled_agents FROM clients WHERE id = $1 AND active = true LIMIT 1',
        [clientId]
      );
      if (!rows[0]) {
        return {
          allowed: false,
          reason: 'client_not_found',
          normalizedAgent,
        };
      }
      if (!Array.isArray(rows[0].enabled_agents)) {
        return {
          allowed: false,
          reason: 'enabled_agents_unavailable',
          normalizedAgent,
        };
      }
      enabledAgents = rows[0].enabled_agents;
    } catch (_err) {
      return {
        allowed: false,
        reason: 'enabled_agents_unavailable',
        normalizedAgent,
      };
    }
  }

  const normalizedEnabled = [
    ...new Set(enabledAgents.map(normalizeAgentName).filter(Boolean)),
  ];
  if (!normalizedEnabled.includes(normalizedAgent)) {
    return {
      allowed: false,
      reason: BLOCK_REASON,
      normalizedAgent,
    };
  }

  return { allowed: true, reason: null, normalizedAgent };
}

async function logBlockedDispatch({
  agentName,
  clientId,
  reason,
  channel,
  payload = {},
}) {
  try {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
       VALUES ($1, $2, $3, $4, $5, NOW(), $6)`,
      [
        String(agentName || 'unknown'),
        'dispatch_blocked',
        JSON.stringify({
          reason,
          channel,
          client_id: clientId,
          ...payload,
        }),
        'skipped',
        reason,
        clientId,
      ]
    );
  } catch (err) {
    console.error('[agent-dispatch] blocked log failed:', err.message);
  }
}

module.exports = {
  BLOCK_REASON,
  isAgentEnabledForClient,
  logBlockedDispatch,
};
