'use strict';

const {
  resolveOperatorDelegatedMaximumDailyCapacity,
} = require('../packages/emmett-outbound/OperatorDelegatedCapacity');

const AUTOMATIC_ENVELOPE_STATUSES = new Set(['frozen', 'authorized', 'complete']);

function parsePolicy(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch { return {}; }
}

function optionalNumber(value) {
  if (value == null || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function executionMessage(status) {
  if (status === 'frozen') {
    return 'Emmett will bind the frozen daily batch and execute it automatically at the next eligible governed tick.';
  }
  if (status === 'authorized') {
    return 'Emmett is authorized to execute this daily batch automatically at the next eligible governed tick.';
  }
  if (status === 'complete') return 'Emmett completed this governed daily batch.';
  return 'This mission is governed by its standing outbound program.';
}

async function loadGovernedMissionExecutionStatus(pool, { tenantId, missionId }) {
  if (!pool || tenantId == null || !missionId) return null;
  const result = await pool.query(
    `SELECT e.status AS envelope_status, e.approval_id, e.local_day,
            p.id AS program_id, p.mode AS program_mode, p.policy
       FROM acquisition_outbound_envelopes e
       JOIN acquisition_outbound_programs p
         ON p.id = e.program_id AND p.tenant_id = e.tenant_id
      WHERE e.tenant_id = $1 AND e.mission_id = $2
      ORDER BY e.created_at DESC
      LIMIT 1`,
    [String(tenantId), String(missionId)],
  );
  const row = result.rows[0];
  if (!row) return null;

  const policy = parsePolicy(row.policy);
  const standingDelegationActive = row.program_mode === 'active'
    && AUTOMATIC_ENVELOPE_STATUSES.has(row.envelope_status);
  const automaticExecution = standingDelegationActive && row.envelope_status !== 'complete';

  return {
    governed: true,
    programId: row.program_id,
    programMode: row.program_mode,
    envelopeStatus: row.envelope_status,
    localDay: row.local_day,
    standingDelegationActive,
    automaticExecution,
    requiresManualExecutionAuthorization: false,
    emmettControlsCapacity: standingDelegationActive,
    operatorDelegatedMaximumDailyCapacity:
      resolveOperatorDelegatedMaximumDailyCapacity(policy),
    programTotalCap: optionalNumber(policy.totalCap),
    sendWindow: {
      startHour: optionalNumber(policy.startHour),
      endHour: optionalNumber(policy.endHour),
      timeZone: policy.timeZone || null,
      spacingMinutes: optionalNumber(policy.spacingMinutes),
    },
    message: executionMessage(row.envelope_status),
  };
}

module.exports = {
  AUTOMATIC_ENVELOPE_STATUSES,
  loadGovernedMissionExecutionStatus,
};
