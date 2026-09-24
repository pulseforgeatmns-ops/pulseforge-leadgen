'use strict';

const pool = require('../db');

async function logAoAuditEvent({
  event,
  clientId,
  aoUserId,
  prospectId = null,
  missionId = null,
  payload = {},
}) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ('ao', $1, $2::uuid, $3::jsonb, 'ok', NOW(), $4)
  `, [
    event,
    prospectId || null,
    JSON.stringify({
      event,
      tenant_id: clientId,
      ao_user_id: aoUserId,
      prospect_id: prospectId,
      mission_id: missionId,
      ...payload,
    }),
    clientId,
  ]);
}

module.exports = {
  logAoAuditEvent,
};
