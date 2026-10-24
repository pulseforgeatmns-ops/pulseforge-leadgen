'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  unwrapContributionPayload,
  scoutCandidateCount,
  buildVerdict,
} = require('../scripts/validateAnchorCanonicalMission');

describe('validateAnchorCanonicalMission — Scout candidate count extraction', () => {
  it('unwraps durable contribution rows stored in the payload column', () => {
    const wrapped = {
      id: 'contrib_79df3ac3-18f7-484b-93d3-a2994aee75ec',
      missionId: 'mission_1ddd1acb-6bae-4d51-baa8-ca449bab061a',
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        qualifiedCount: 27,
        candidateUniverseCount: 27,
        rankedProspectCount: 27,
        evidence: [{ label: 'Places hit', source: 'google_places' }],
      },
      at: '2026-09-13T12:00:00.000Z',
    };

    const unwrapped = unwrapContributionPayload(wrapped);
    assert.equal(unwrapped.qualifiedCount, 27);
    assert.equal(scoutCandidateCount(wrapped), 27);
  });

  it('reads flat in-memory contribution payloads unchanged', () => {
    const flat = {
      qualifiedCount: 12,
      rankedProspects: [{ name: 'Harbor Law Group' }],
    };
    assert.equal(scoutCandidateCount(flat), 12);
  });

  it('prefers qualifiedCount over candidateUniverseCount', () => {
    assert.equal(
      scoutCandidateCount({ qualifiedCount: 27, candidateUniverseCount: 31 }),
      27
    );
  });

  it('does not treat wrapped contribution metadata as zero candidates', () => {
    const wrapped = {
      id: 'contrib_x',
      specialist: 'scout',
      kind: 'discovery',
      payload: { qualifiedCount: 27, evidence: [{ label: 'x', source: 'google_places' }] },
    };
    assert.notEqual(scoutCandidateCount(wrapped), null);
    assert.equal(scoutCandidateCount(wrapped), 27);
  });

  it('buildVerdict passes when scoutCandidateCount is nonzero after unwrap', () => {
    const verdict = buildVerdict({
      finalStage: 'ready',
      scoutCandidateCount: 27,
    });
    assert.equal(verdict.success, true);
    assert.equal(verdict.scoutDiscoveryProductionValidated, true);
  });

  it('buildVerdict fails when scoutCandidateCount is null (validation bug shape)', () => {
    const verdict = buildVerdict({
      finalStage: 'ready',
      scoutCandidateCount: null,
    });
    assert.equal(verdict.success, false);
    assert.match(verdict.message, /candidate count is zero/i);
  });
});
