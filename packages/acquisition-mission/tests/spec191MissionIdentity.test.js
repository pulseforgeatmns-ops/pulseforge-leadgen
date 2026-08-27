'use strict';

/**
 * SPEC-191 — Canonical Mission Identity Resolution acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { resolveCanonicalObjective } = require('../../max/workspace/ResolvedObjective');
const { maybeHandleAcquisitionOwnershipTurn } = require('../../max/workspace/AcquisitionOwnership');
const {
  buildMissionIdentity,
  missionIdentitiesMatch,
  findResumableMissionByIdentity,
} = require('../MissionIdentity');
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

const STR_SEGMENT = 'short_term_rental';

function resolveStrObjective(question) {
  return resolveCanonicalObjective({ question, targetSegment: 'Short-Term Rental Operators' });
}

describe('SPEC-191 — Canonical Mission Identity Resolution', () => {
  it('Greater Manchester, Nashua, and Concord STR objectives produce distinct identities', () => {
    const greaterManchester = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.'
    );
    const nashua = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in Nashua.'
    );
    const concord = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in Concord.'
    );

    const gmIdentity = buildMissionIdentity(greaterManchester);
    const nashuaIdentity = buildMissionIdentity(nashua);
    const concordIdentity = buildMissionIdentity(concord);

    assert.equal(gmIdentity.targetSegment, STR_SEGMENT);
    assert.equal(nashuaIdentity.targetSegment, STR_SEGMENT);
    assert.equal(concordIdentity.targetSegment, STR_SEGMENT);

    assert.equal(gmIdentity.geography.region, 'greater manchester');
    assert.equal(nashuaIdentity.geography.region, 'nashua');
    assert.equal(concordIdentity.geography.region, 'concord');

    assert.equal(missionIdentitiesMatch(gmIdentity, nashuaIdentity), false);
    assert.equal(missionIdentitiesMatch(gmIdentity, concordIdentity), false);
    assert.equal(missionIdentitiesMatch(nashuaIdentity, concordIdentity), false);
  });

  it('identical Greater Manchester STR objectives match for resume', () => {
    const first = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.'
    );
    const second = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.'
    );

    assert.equal(
      missionIdentitiesMatch(buildMissionIdentity(first), buildMissionIdentity(second)),
      true
    );
  });

  it('findResumableMissionByIdentity resumes by structured objective, not English overlap', () => {
    const resolved = resolveStrObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.'
    );
    const missions = [
      {
        id: 'mission-nashua',
        stage: 'discover',
        objective: 'Acquire one recurring commercial cleaning client from a short-term rental operator in Nashua.',
        resolvedObjective: resolveStrObjective(
          'Acquire one recurring commercial cleaning client from a short-term rental operator in Nashua.'
        ),
      },
      {
        id: 'mission-gm',
        stage: 'discover',
        objective: resolved.objective,
        resolvedObjective: resolved,
      },
    ];

    const match = findResumableMissionByIdentity(missions, resolved);
    assert.ok(match);
    assert.equal(match.id, 'mission-gm');
  });

  it('ownership turn creates separate missions for distinct geographies', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const runtime = createTestAmoRuntime({ engine: amoEngine });
    const tenantId = 'spec191-tenant';

    const gmPrompt =
      'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';
    const nashuaPrompt =
      'Acquire one recurring commercial cleaning client from a short-term rental operator in Nashua.';

    const gmTurn = await maybeHandleAcquisitionOwnershipTurn({
      question: gmPrompt,
      context: { tenantId },
      acquisitionMissionRuntime: runtime,
      persist: false,
    });
    assert.ok(gmTurn.created);
    assert.equal(gmTurn.created, true);

    const nashuaTurn = await maybeHandleAcquisitionOwnershipTurn({
      question: nashuaPrompt,
      context: { tenantId },
      acquisitionMissionRuntime: runtime,
      persist: false,
    });
    assert.ok(nashuaTurn.created);
    assert.notEqual(nashuaTurn.mission.id, gmTurn.mission.id);

    const gmResume = await maybeHandleAcquisitionOwnershipTurn({
      question: gmPrompt,
      context: { tenantId },
      acquisitionMissionRuntime: runtime,
      persist: false,
    });
    assert.equal(gmResume.created, false);
    assert.equal(gmResume.mission.id, gmTurn.mission.id);
  });
});
