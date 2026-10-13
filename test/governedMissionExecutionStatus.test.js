'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  loadGovernedMissionExecutionStatus,
} = require('../services/governedMissionExecutionStatus');

function fixturePool(row) {
  return {
    calls: [],
    async query(sql, args) {
      this.calls.push({ sql, args });
      return { rows: row ? [row] : [] };
    },
  };
}

test('Babrun frozen daily batch inherits standing delegation without manual authorization', async () => {
  const pool = fixturePool({
    envelope_status: 'frozen',
    approval_id: null,
    local_day: '2026-10-13',
    program_id: 'outbound_babrun',
    program_mode: 'active',
    policy: {
      dailyCap: 1,
      operatorDelegatedMaximumDailyCapacity: 20,
      totalCap: 100,
      startHour: 9,
      endHour: 17,
      timeZone: 'America/New_York',
      spacingMinutes: 60,
    },
  });

  const status = await loadGovernedMissionExecutionStatus(pool, {
    tenantId: '13',
    missionId: 'mission_daily_babrun',
  });

  assert.equal(status.standingDelegationActive, true);
  assert.equal(status.automaticExecution, true);
  assert.equal(status.requiresManualExecutionAuthorization, false);
  assert.equal(status.emmettControlsCapacity, true);
  assert.equal(status.operatorDelegatedMaximumDailyCapacity, 20);
  assert.equal(status.programTotalCap, 100);
  assert.match(status.message, /execute it automatically/i);
  assert.deepEqual(pool.calls[0].args, ['13', 'mission_daily_babrun']);
  assert.match(pool.calls[0].sql, /e\.tenant_id = \$1 AND e\.mission_id = \$2/);
});

test('paused governed program never presents automatic execution as active', async () => {
  const pool = fixturePool({
    envelope_status: 'cancelled',
    approval_id: null,
    local_day: '2026-10-13',
    program_id: 'outbound_babrun',
    program_mode: 'paused',
    policy: { operatorDelegatedMaximumDailyCapacity: 20, totalCap: 100 },
  });

  const status = await loadGovernedMissionExecutionStatus(pool, {
    tenantId: '13',
    missionId: 'mission_daily_babrun',
  });

  assert.equal(status.standingDelegationActive, false);
  assert.equal(status.automaticExecution, false);
  assert.equal(status.emmettControlsCapacity, false);
});

test('non-governed mission has no governed execution status', async () => {
  const status = await loadGovernedMissionExecutionStatus(fixturePool(null), {
    tenantId: '13',
    missionId: 'mission_other',
  });
  assert.equal(status, null);
});

test('mission workspace presents standing delegation instead of a manual approval action', () => {
  const ui = fs.readFileSync(
    path.join(__dirname, '../public/acquisition-missions.html'),
    'utf8',
  );
  assert.match(ui, /Standing delegation active/);
  assert.match(ui, /No manual execution approval required/);
  assert.match(ui, /EMMETT AUTO-EXECUTION SCHEDULED/);
  assert.match(ui, /governedExecution\.standingDelegationActive/);
});
