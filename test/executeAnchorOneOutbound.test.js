'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  TENANT_ID,
  MISSION_ID,
  parseArgs,
  run,
  sendableQueueItems,
  assertCapacitySelectionMatch,
  resolveCanonicalActiveCapacity,
} = require('../scripts/executeAnchorOneOutbound');
const { selectActiveCapacityContribution } = require('../scripts/lib/activeCapacitySelection');

const SCRIPT = path.join(__dirname, '..', 'scripts/executeAnchorOneOutbound.js');
const source = fs.readFileSync(SCRIPT, 'utf8');

const OLD_CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const NEW_CAPACITY_ID = 'contrib_9d27afb0-17cf-4bb5-9ec4-d73662c05c6e';

describe('executeAnchorOneOutbound — one controlled production send', () => {
  it('is tenant 10 / expected mission and refuses without --confirm-production', async () => {
    assert.equal(TENANT_ID, '10');
    assert.equal(MISSION_ID, 'mission_ad7753b0-6def-441d-bb1a-3764656f5750');
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('does not hardcode a stale CAPACITY contribution ID', () => {
    assert.doesNotMatch(source, /contrib_55e11312-3837-4485-b95a-58134dcd7601/);
    assert.doesNotMatch(source, /EXPECTED_CAPACITY_ID/);
    assert.match(source, /loadActiveCapacityForMission/);
    assert.match(source, /selectActiveCapacityContribution/);
    assert.match(source, /capacity_mismatch/);
  });

  it('uses the canonical approve then execute path and never mutates activation', () => {
    assert.match(source, /APPROVE_EXECUTION/);
    assert.match(source, /EXECUTE_OUTBOUND/);
    assert.match(source, /maxSends:\s*1/);
    assert.match(source, /prospectId:\s*selectedProspectId/);
    assert.match(source, /probe\.run/);
    assert.match(source, /runtime\.persistOpts\(\{\s*persist:\s*true\s*\}\)/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /ALLOW_FIXTURE_FALLBACK === 'true'/);
    assert.match(source, /allowFixtureFallback:\s*false/);
    assert.match(source, /createProviderCounter/);
  });

  it('classifies sendable queue items without inventing recipients', () => {
    const sendable = sendableQueueItems({
      queue: {
        items: [
          { prospectId: 'a', email: 'a@example.com', sendable: true, paige: { candidateId: 'a' } },
          { prospectId: 'b', email: 'b@example.com', sendable: false },
          { prospectId: 'c', dnc: true, email: 'c@example.com' },
          { prospectId: 'd' },
        ],
      },
    });
    assert.deepEqual(sendable.map((row) => row.prospectId), ['a']);
  });

  it('assertCapacitySelectionMatch aborts when probe and canonical durable CAPACITY differ', () => {
    assert.throws(
      () => assertCapacitySelectionMatch({
        expected: NEW_CAPACITY_ID,
        actual: OLD_CAPACITY_ID,
        context: 'Probe CAPACITY selection vs canonical durable state',
      }),
      (err) => err.code === 'capacity_mismatch'
        && err.message.includes(NEW_CAPACITY_ID)
        && err.message.includes(OLD_CAPACITY_ID)
    );
  });

  it('resolveCanonicalActiveCapacity selects newest revision-bound CAPACITY, not a fixed ID', async () => {
    const missionBody = {
      objective: 'Anchor law firms',
      revisionState: { emmettContributionId: NEW_CAPACITY_ID },
    };
    const pool = {
      query(sql) {
        if (sql.includes('FROM acquisition_missions')) {
          return { rows: [{ mission_id: MISSION_ID, payload: missionBody }] };
        }
        if (sql.includes('FROM acquisition_mission_contributions')) {
          return {
            rows: [
              {
                capacity_id: OLD_CAPACITY_ID,
                at: '2026-09-13T20:00:00.000Z',
                payload: { superseded: true, supersededBy: NEW_CAPACITY_ID },
              },
              {
                capacity_id: NEW_CAPACITY_ID,
                at: '2026-09-13T22:00:00.000Z',
                payload: { queue: { items: [] } },
              },
            ],
          };
        }
        throw new Error(`Unexpected query: ${sql}`);
      },
    };

    const active = await resolveCanonicalActiveCapacity(pool, TENANT_ID, MISSION_ID);
    assert.equal(active.capacity_id, NEW_CAPACITY_ID);
    assert.notEqual(active.capacity_id, OLD_CAPACITY_ID);

    const selected = selectActiveCapacityContribution(missionBody, [
      {
        capacity_id: OLD_CAPACITY_ID,
        at: '2026-09-13T20:00:00.000Z',
        payload: { superseded: true },
      },
      {
        capacity_id: NEW_CAPACITY_ID,
        at: '2026-09-13T22:00:00.000Z',
        payload: { queue: { items: [] } },
      },
    ]);
    assert.equal(selected.capacity_id, NEW_CAPACITY_ID);
  });
});
