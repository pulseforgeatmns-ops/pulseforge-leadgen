'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  run,
  parseArgs,
  DEFAULT_MISSION_ID,
  scriptRejectReason,
} = require('../scripts/auditAnchorCapacitySendability');
const {
  selectActiveCapacityContribution,
  isSupersededContribution,
} = require('../scripts/lib/activeCapacitySelection');
const { MESSAGE_BINDING_SCOPES } = require('../packages/acquisition-mission/types');

const MISSION_ID = DEFAULT_MISSION_ID;
const OLD_CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const NEW_CAPACITY_ID = 'contrib_01f70b44-183d-4ae3-bb1e-598c34ee0d88';

function boundQueueItem(email = 'partner@harborlaw.com') {
  return {
    prospectId: 'co-harbor',
    email,
    sendable: true,
    paige: {
      author: 'paige',
      source: 'paige',
      ready: true,
      candidateId: 'co-harbor',
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      attributableIntelligence: { companyName: 'Harbor Law' },
    },
  };
}

function capacityPayload(items) {
  return {
    id: NEW_CAPACITY_ID,
    specialist: 'emmett',
    kind: 'capacity',
    payload: {
      capacity: { recommended: items.length },
      queue: { items },
      governor: { outcome: 'proceed' },
    },
  };
}

function mockPool({ missionBody, capacityRows, prospects = [] }) {
  return {
    query(sql, params) {
      if (sql.includes('FROM acquisition_missions')) {
        return {
          rows: [{
            mission_id: MISSION_ID,
            payload: missionBody,
            updated_at: '2026-09-13T22:00:00.000Z',
          }],
        };
      }
      if (sql.includes('FROM acquisition_mission_contributions')) {
        return { rows: capacityRows };
      }
      if (sql.includes('FROM prospects')) {
        const prospectId = params[1];
        const row = prospects.find((p) => String(p.id) === String(prospectId));
        return { rows: row ? [row] : [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  };
}

describe('auditAnchorCapacitySendability', () => {
  it('refuses without --confirm-production', () => {
    assert.throws(
      () => parseArgs([]),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('selects active non-superseded CAPACITY instead of a hardcoded superseded id', async () => {
    const staleCapacity = {
      capacity_id: OLD_CAPACITY_ID,
      at: '2026-09-13T23:00:00.000Z',
      payload: {
        id: OLD_CAPACITY_ID,
        specialist: 'emmett',
        kind: 'capacity',
        payload: {
          superseded: true,
          supersededBy: NEW_CAPACITY_ID,
          queue: { items: [{ prospectId: 'co-harbor', email: null, sendable: true }] },
        },
      },
    };
    const freshCapacity = {
      capacity_id: NEW_CAPACITY_ID,
      at: '2026-09-13T22:30:00.000Z',
      payload: capacityPayload([boundQueueItem()]),
    };

    assert.equal(isSupersededContribution(staleCapacity), true);
    assert.equal(isSupersededContribution(freshCapacity), false);

    const selected = selectActiveCapacityContribution({
      revisionState: { emmettContributionId: NEW_CAPACITY_ID },
    }, [staleCapacity, freshCapacity]);
    assert.equal(selected.capacity_id, NEW_CAPACITY_ID);

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: {
          objective: 'Anchor law firms',
          revisionState: { emmettContributionId: NEW_CAPACITY_ID },
        },
        capacityRows: [staleCapacity, freshCapacity],
        prospects: [{
          id: 'co-harbor',
          email: 'partner@harborlaw.com',
          email_status: 'verified',
          email_verified: true,
          do_not_contact: false,
          company_name: 'Harbor Law',
        }],
      }),
    });

    assert.equal(report.capacityContributionId, NEW_CAPACITY_ID);
    assert.notEqual(report.capacityContributionId, OLD_CAPACITY_ID);
    assert.ok(report.sendableCount >= 1);
    assert.equal(report.firstBlocker, null);
  });

  it('prefers mission revision pointer over newest-at superseded row', async () => {
    const staleCapacity = {
      capacity_id: OLD_CAPACITY_ID,
      at: '2026-09-13T23:30:00.000Z',
      payload: {
        id: OLD_CAPACITY_ID,
        payload: {
          superseded: true,
          queue: { items: [] },
        },
      },
    };
    const freshCapacity = {
      capacity_id: NEW_CAPACITY_ID,
      at: '2026-09-13T22:00:00.000Z',
      payload: capacityPayload([boundQueueItem('ops@granitelegal.com')]),
    };

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: {
          objective: 'Anchor law firms',
          revisionState: { emmettContributionId: NEW_CAPACITY_ID },
        },
        capacityRows: [staleCapacity, freshCapacity],
      }),
    });

    assert.equal(report.capacityContributionId, NEW_CAPACITY_ID);
  });

  it('scriptRejectReason matches executeAnchorOneOutbound sendable predicate', () => {
    assert.equal(scriptRejectReason({ email: 'a@b.com', sendable: true }), null);
    assert.equal(scriptRejectReason({ email: '', sendable: true }), 'missing_recipient_email_on_queue_item');
    assert.equal(scriptRejectReason({ email: 'a@b.com', sendable: false }), 'item.sendable=false');
    assert.equal(scriptRejectReason({ email: 'a@b.com', dnc: true }), 'item.dnc=true');
  });
});
