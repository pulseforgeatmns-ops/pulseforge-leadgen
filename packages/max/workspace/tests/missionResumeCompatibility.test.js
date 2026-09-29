'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { maybeHandleAcquisitionOwnershipTurn } = require('../AcquisitionOwnership');
const {
  assessMissionResumeCompatibility,
  missionsMateriallyCompatible,
  isMultiSegmentObjective,
  extractSegmentScope,
} = require('../MissionResumeCompatibility');
const { resolveCanonicalObjective } = require('../ResolvedObjective');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

const STR_OBJECTIVE_VARIANT =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

const BROAD_OBJECTIVE = [
  'Acquire one new recurring cleaning client for Anchor Cleaning in Greater Manchester.',
  'Prioritize high-fit commercial and property-management opportunities, including',
  'short-term rental operators where appropriate.',
].join(' ');

const COMMERCIAL_OBJECTIVE =
  'Acquire one recurring commercial cleaning client in Greater Manchester.';

const PROPERTY_MGMT_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from property managers in Greater Manchester.';

describe('SPEC-201 — Mission resume material compatibility', () => {
  let amoEngine;

  beforeEach(() => {
    amoEngine = amo.createAcquisitionMissionEngine();
  });

  it('1 — existing STR mission + new STR-only objective resumes', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });
    existing.structuredMission = {
      missionType: 'acquisition',
      market: { segment: 'short_term_rental', buyer: 'property_operator' },
      geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      successMetric: { type: 'recurring_clients', target: 1 },
    };

    const result = assessMissionResumeCompatibility(existing, STR_OBJECTIVE_VARIANT);
    assert.equal(result.compatible, true, result.reason);
  });

  it('2 — existing STR mission + broader commercial/property objective does NOT resume', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });
    existing.structuredMission = {
      missionType: 'acquisition',
      market: { segment: 'short_term_rental', buyer: 'property_operator' },
      geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      successMetric: { type: 'recurring_clients', target: 1 },
    };

    const result = assessMissionResumeCompatibility(existing, BROAD_OBJECTIVE);
    assert.equal(result.compatible, false);
    assert.equal(result.reason, 'segment_scope_incompatible');
  });

  it('3 — existing commercial mission + same commercial objective resumes', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: COMMERCIAL_OBJECTIVE,
      targetSegment: 'Commercial',
    });

    const result = assessMissionResumeCompatibility(existing, COMMERCIAL_OBJECTIVE);
    assert.equal(result.compatible, true, result.reason);
  });

  it('4 — existing property-management mission + materially different buyer/segment does NOT resume', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: PROPERTY_MGMT_OBJECTIVE,
      targetSegment: 'Property Managers',
    });
    existing.structuredMission = {
      missionType: 'acquisition',
      market: { segment: 'property_management', buyer: 'property_manager' },
      geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      successMetric: { type: 'recurring_clients', target: 1 },
    };

    const lawFirmObjective =
      'Acquire one recurring commercial cleaning client from law firms in Greater Manchester.';

    const result = assessMissionResumeCompatibility(existing, lawFirmObjective);
    assert.equal(result.compatible, false);
    assert.equal(result.reason, 'segment_scope_incompatible');
  });

  it('5 — geography overlap alone is insufficient', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });
    existing.structuredMission = {
      missionType: 'acquisition',
      market: { segment: 'short_term_rental', buyer: 'property_operator' },
      geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      successMetric: { type: 'recurring_clients', target: 1 },
    };

    const differentSegmentSameGeo =
      'Acquire three recurring commercial cleaning clients from law firms in Greater Manchester.';

    const result = assessMissionResumeCompatibility(existing, differentSegmentSameGeo);
    assert.equal(result.compatible, false);
    assert.notEqual(result.reason, null);
  });

  it('6 — shared success metric alone is insufficient', () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });
    existing.structuredMission = {
      missionType: 'acquisition',
      market: { segment: 'short_term_rental', buyer: 'property_operator' },
      geography: { region: 'Greater Manchester', cities: ['Manchester'] },
      successMetric: { type: 'recurring_clients', target: 1 },
    };

    const differentBuyerSameMetric =
      'Acquire one recurring commercial cleaning client from accounting firms in Charleston WV.';

    const result = assessMissionResumeCompatibility(existing, differentBuyerSameMetric);
    assert.equal(result.compatible, false);
  });

  it('7 — historical objective cannot override current operator scope', () => {
    assert.equal(isMultiSegmentObjective(BROAD_OBJECTIVE), true);
    const scope = extractSegmentScope(BROAD_OBJECTIVE);
    assert.equal(scope.mode, 'multi');
    assert.ok(scope.eligibleSegments.includes('property_management'));
    assert.ok(scope.eligibleSegments.includes('short_term_rental'));

    const strScope = extractSegmentScope(STR_OBJECTIVE);
    assert.equal(strScope.mode, 'exclusive');
    assert.equal(strScope.primarySegment, 'short_term_rental');
  });

  it('8 — new broader mission reaches ownership handler with current objective intact', async () => {
    amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });

    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: BROAD_OBJECTIVE,
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      persist: false,
    });

    assert.ok(turn);
    assert.equal(turn.created, true);
    assert.equal(turn.reason, 'acquisition_mission_created');
    assert.match(turn.prose, /Mission Created/);
    assert.doesNotMatch(turn.prose, /Mission Resumed/);
    assert.ok(turn.mission.objective.includes('property-management'));
    assert.ok(turn.mission.objective.includes('where appropriate'));
    assert.notEqual(turn.mission.objective, STR_OBJECTIVE);

    const resolved = resolveCanonicalObjective({ question: BROAD_OBJECTIVE });
    assert.ok(resolved.segmentLabel.includes('Property Management') || resolved.segmentLabel.includes('Commercial'));
  });

  it('maybeHandleAcquisitionOwnershipTurn creates new mission for Anchor broad objective', async () => {
    const existing = amoEngine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Short-Term Rental Operators',
    });

    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: BROAD_OBJECTIVE,
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      persist: false,
    });

    assert.ok(turn);
    assert.equal(turn.created, true);
    assert.notEqual(turn.mission.id, existing.id);
    assert.match(turn.mission.title || '', /Commercial|Property Management/i);
  });

  it('missionsMateriallyCompatible is exported for resume guard', () => {
    assert.equal(typeof missionsMateriallyCompatible, 'function');
  });
});
