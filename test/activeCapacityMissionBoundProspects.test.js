'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  listProspectIdsFromCapacityPayload,
  listProspectIdsFromActiveCapacity,
  selectActiveCapacityContribution,
  isSupersededContribution,
} = require('../scripts/lib/activeCapacitySelection');
const { listMissionBoundProspectIds } = require('../packages/max/workspace/EmmettMissionCandidates');

const MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const TENANT_ID = '10';

const PRODUCTION_UUIDS = Object.freeze([
  'f47ac10b-58cc-4372-a567-0e02b2c3d479',
  '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
  '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
  '6ba7b812-9dad-11d1-80b4-00c04fd430c8',
  '6ba7b813-9dad-11d1-80b4-00c04fd430c8',
]);

function capacityQueuePayload(prospectIds) {
  return {
    specialist: 'emmett',
    kind: 'capacity',
    payload: {
      capacity: { recommended: prospectIds.length },
      queue: {
        items: prospectIds.map((prospectId, index) => ({
          prospectId,
          company: index === prospectIds.length - 1 ? 'Deliverability Test' : `Law Firm ${index + 1}`,
          sendable: true,
          paige: {
            candidateId: prospectId,
            bindingScope: 'prospect',
          },
        })),
      },
      governor: { outcome: 'proceed' },
    },
  };
}

describe('active CAPACITY mission-bound prospect ID resolution', () => {
  it('extracts production-shaped UUID prospect IDs from queue.items[].prospectId', () => {
    const ids = listProspectIdsFromCapacityPayload(capacityQueuePayload(PRODUCTION_UUIDS));
    assert.deepEqual(ids, [...PRODUCTION_UUIDS]);
  });

  it('unwraps double-nested persisted CAPACITY JSONB before reading queue items', () => {
    const wrapped = {
      id: 'contrib_capacity_active',
      specialist: 'emmett',
      kind: 'capacity',
      payload: capacityQueuePayload([PRODUCTION_UUIDS[0]]).payload,
    };
    assert.deepEqual(
      listProspectIdsFromCapacityPayload(wrapped),
      [PRODUCTION_UUIDS[0]]
    );
  });

  it('selects active non-superseded CAPACITY via revision pointer', () => {
    const staleId = 'contrib_stale_capacity';
    const activeId = 'contrib_active_capacity';
    const stale = {
      capacity_id: staleId,
      at: '2026-09-14T01:00:00.000Z',
      payload: {
        payload: {
          superseded: true,
          supersededBy: activeId,
          queue: { items: [{ prospectId: 'should-not-resolve' }] },
        },
      },
    };
    const active = {
      capacity_id: activeId,
      at: '2026-09-13T23:00:00.000Z',
      payload: capacityQueuePayload([PRODUCTION_UUIDS[0], PRODUCTION_UUIDS[1]]),
    };
    assert.equal(isSupersededContribution(stale), true);
    const selected = selectActiveCapacityContribution(
      { revisionState: { emmettContributionId: activeId } },
      [stale, active]
    );
    assert.equal(selected.capacity_id, activeId);
    assert.deepEqual(
      listProspectIdsFromActiveCapacity(
        { revisionState: { emmettContributionId: activeId } },
        [stale, active]
      ),
      [PRODUCTION_UUIDS[0], PRODUCTION_UUIDS[1]]
    );
  });

  it('reads UUID prospectId from scout discovery when id is absent', () => {
    const mission = {
      id: MISSION_ID,
      tenantId: TENANT_ID,
      targetSegment: 'Law Firms',
      structuredMission: { market: { label: 'Law Firms', segment: 'law_firm' } },
    };
    const contributions = [
      {
        specialist: 'scout',
        kind: 'discovery',
        payload: {
          prospects: PRODUCTION_UUIDS.map((prospectId, index) => ({
            prospectId,
            companyId: `co-${index}`,
            company: `Law Firm ${index + 1}`,
          })),
        },
      },
      {
        specialist: 'max',
        kind: 'prioritization',
        payload: {
          rankedTargets: PRODUCTION_UUIDS.map((_, index) => ({
            id: `co-${index}`,
            companyId: `co-${index}`,
            name: `Law Firm ${index + 1}`,
            rank: index + 1,
          })),
        },
      },
    ];
    assert.deepEqual(listMissionBoundProspectIds(mission, contributions), [...PRODUCTION_UUIDS]);
  });

  it('scout/max lineage still returns [] when discovery prospects carry no identity field', () => {
    const mission = {
      id: MISSION_ID,
      tenantId: TENANT_ID,
      targetSegment: 'Law Firms',
      structuredMission: { market: { label: 'Law Firms', segment: 'law_firm' } },
    };
    const contributions = [
      {
        specialist: 'scout',
        kind: 'discovery',
        payload: {
          prospects: PRODUCTION_UUIDS.map((_, index) => ({
            companyId: `co-${index}`,
            company: `Law Firm ${index + 1}`,
          })),
        },
      },
      {
        specialist: 'max',
        kind: 'prioritization',
        payload: {
          rankedTargets: PRODUCTION_UUIDS.map((_, index) => ({
            id: `co-${index}`,
            companyId: `co-${index}`,
            name: `Law Firm ${index + 1}`,
            rank: index + 1,
          })),
        },
      },
    ];
    assert.deepEqual(listMissionBoundProspectIds(mission, contributions), []);
  });

  it('CAPACITY-first resolution returns UUIDs when scout/max lineage is empty', () => {
    const mission = {
      id: MISSION_ID,
      tenantId: TENANT_ID,
      targetSegment: 'Law Firms',
      structuredMission: { market: { label: 'Law Firms', segment: 'law_firm' } },
    };
    const contributions = [
      {
        specialist: 'scout',
        kind: 'discovery',
        payload: {
          prospects: PRODUCTION_UUIDS.map((prospectId, index) => ({
            prospectId,
            companyId: `co-${index}`,
            company: `Law Firm ${index + 1}`,
          })),
        },
      },
      {
        specialist: 'max',
        kind: 'prioritization',
        payload: {
          rankedTargets: PRODUCTION_UUIDS.map((_, index) => ({
            id: `co-${index}`,
            companyId: `co-${index}`,
            name: `Law Firm ${index + 1}`,
            rank: index + 1,
          })),
        },
      },
    ];
    const activeCapacityPayload = capacityQueuePayload(PRODUCTION_UUIDS);

    let prospectIds = listProspectIdsFromCapacityPayload(activeCapacityPayload);
    if (!prospectIds.length) {
      prospectIds = listMissionBoundProspectIds(mission, contributions);
    }

    assert.deepEqual(prospectIds, [...PRODUCTION_UUIDS]);
  });
});
