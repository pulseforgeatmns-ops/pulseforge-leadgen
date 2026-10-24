'use strict';

/**
 * SPEC-204 — Durable Candidate Belief Hydration & Universe Integrity (AUDIT-080).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  collectCandidateBeliefsFromPayload,
  countQualifiedBeliefs,
  assertCandidateBeliefPersistence,
  checkBeliefRegressionIntegrity,
  beliefsToPreservedCandidates,
} = require('../packages/scout/investigation/CandidateBeliefState');
const {
  INVESTIGATION_MODES,
  extractPayloadFromDiscoveryContribution,
  resolveInvestigationMode,
  buildInvestigationContinuationContext,
} = require('../packages/scout/investigation/EntityInvestigationContinuation');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const { QUALIFICATION_STATUSES, READINESS_STATES } = require('../packages/max/scoutAcquisition/Types');

function qualifiedEvaluation(name = 'Candidate') {
  return {
    qualified: true,
    bucket: 'investigation_required',
    qualification: {
      status: QUALIFICATION_STATUSES.QUALIFIED,
      reason: `${name} matches target segment`,
    },
    readiness: { status: READINESS_STATES.UNKNOWN },
    businessFit: { score: 0.72, basicFit: true, reasons: ['property management'] },
  };
}

function uncertainRecord(id, name) {
  return {
    candidateId: id,
    candidate_id: id,
    canonicalIdentity: id,
    name,
    dedupeStatus: 'primary',
    qualification: { status: QUALIFICATION_STATUSES.UNCERTAIN },
    readiness: { status: READINESS_STATES.UNKNOWN },
  };
}

function qualifiedRecord(id, name, extras = {}) {
  const evaluation = qualifiedEvaluation(name);
  return {
    candidateId: id,
    candidate_id: id,
    canonicalIdentity: id,
    name,
    address: extras.location || 'Manchester, NH',
    cities: [extras.location || 'Manchester, NH'],
    dedupeStatus: 'primary',
    qualification: evaluation.qualification,
    readiness: evaluation.readiness,
    evaluation,
    businessFit: evaluation.businessFit,
    evidenceRefs: extras.evidenceRefs || [],
    ...extras,
  };
}

function blueDoorRecord() {
  return qualifiedRecord('candidate-blue-door', 'Blue Door Living Property Management', {
    location: 'Manchester',
    evidenceRefs: [
      { id: 'ev-a', label: 'Google Places listing' },
      { id: 'ev-b', label: 'STR portfolio page' },
      { id: 'ev-c', label: 'Manchester location confirmed' },
    ],
  });
}

function audit080RawScoutResult() {
  const candidateUniverse = [
    blueDoorRecord(),
    ...Array.from({ length: 13 }, (_, i) =>
      qualifiedRecord(`candidate-qualified-${i + 1}`, `Qualified Co ${i + 1}`)
    ),
    ...Array.from({ length: 10 }, (_, i) =>
      uncertainRecord(`candidate-uncertain-${i + 1}`, `Uncertain Co ${i + 1}`)
    ),
  ];

  return {
    status: 'completed',
    summary: '24 businesses discovered; 14 qualified.',
    payload: {
      qualifiedCount: 14,
      readinessReadyCount: 0,
      readinessUnknownCount: 14,
      readinessNotReadyCount: 0,
      candidateUniverse,
      evidence: [{ label: 'Google Places search', source: 'google_places' }],
      discoveryStatus: 'complete',
      providerExecution: [{ source: 'google_places', succeeded: true, resultCount: 24 }],
    },
  };
}

function findLatestScoutDiscovery(contributions = []) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === 'scout' && row.kind === 'discovery');
}

function commitAndReloadPriorDiscovery() {
  const normalized = normalizeScoutDiscoveryPayload(audit080RawScoutResult(), {
    missionObjective: 'Acquire commercial cleaning customers in Manchester NH.',
  });

  const contribution = {
    specialist: 'scout',
    kind: 'discovery',
    payload: normalized,
  };

  const reloaded = extractPayloadFromDiscoveryContribution(findLatestScoutDiscovery([contribution]));
  return { normalized, reloaded, contribution };
}

describe('SPEC-204 — Durable Candidate Belief Hydration (AUDIT-080)', () => {
  it('Blue Door regression — hydrated belief survives nested discoveryArtifact reload', () => {
    const blueDoor = blueDoorRecord();
    const payload = {
      candidateUniverseCount: 24,
      qualifiedCount: 14,
      candidateUniverse: [],
      discoveryArtifact: {
        candidateUniverse: [blueDoor],
        fitCandidates: [],
        rankedProspects: [],
        uncertainCandidates: [],
        prospectEvaluations: [],
      },
    };

    const beliefs = collectCandidateBeliefsFromPayload(payload);
    const blueDoorBelief = beliefs.get('candidate-blue-door') || [...beliefs.values()][0];

    assert.equal(beliefs.size, 1);
    assert.equal(blueDoorBelief.candidateId, 'candidate-blue-door');
    assert.equal(blueDoorBelief.identity.name, 'Blue Door Living Property Management');
    assert.equal(blueDoorBelief.identity.location, 'Manchester');
    assert.equal(blueDoorBelief.businessFit.basicFit, true);
    assert.equal(blueDoorBelief.qualification.status, QUALIFICATION_STATUSES.QUALIFIED);
    assert.deepEqual(
      blueDoorBelief.evidenceRefs.map((ev) => ev.id),
      ['ev-a', 'ev-b', 'ev-c']
    );
  });

  it('collectCandidateBeliefsFromPayload ingests all canonical nested discoveryArtifact sources', () => {
    const payload = {
      candidateUniverse: [],
      discoveryArtifact: {
        candidateUniverse: [uncertainRecord('candidate-a', 'Alpha PM')],
        fitCandidates: [qualifiedRecord('candidate-b', 'Beta PM')],
        rankedProspects: [
          {
            id: 'candidate-c',
            name: 'Gamma PM',
            qualificationStatus: QUALIFICATION_STATUSES.QUALIFIED,
            evaluation: qualifiedEvaluation('Gamma PM'),
          },
        ],
        uncertainCandidates: [uncertainRecord('candidate-d', 'Delta PM')],
        prospectEvaluations: [
          {
            candidateId: 'candidate-e',
            name: 'Epsilon PM',
            qualification: { status: QUALIFICATION_STATUSES.QUALIFIED },
            readiness: { status: READINESS_STATES.UNKNOWN },
          },
        ],
      },
    };

    const beliefs = collectCandidateBeliefsFromPayload(payload);
    assert.equal(beliefs.size, 5);
  });

  it('AUDIT-080 — normalization, reload, and hydration preserve 24 universe / 14 qualified', () => {
    const { normalized, reloaded } = commitAndReloadPriorDiscovery();

    assert.equal(normalized.candidateUniverse.length, 24);
    assert.equal(normalized.candidateUniverseCount, 24);
    assert.equal(normalized.qualifiedCount, 14);
    assert.ok(Array.isArray(normalized.discoveryArtifact.candidateUniverse));
    assert.equal(normalized.discoveryArtifact.candidateUniverse.length, 24);

    const priorBeliefs = collectCandidateBeliefsFromPayload(reloaded);
    assert.equal(priorBeliefs.size, 24);
    assert.equal(countQualifiedBeliefs(priorBeliefs), 14);
  });

  it('AUDIT-080 — investigation continuation with zero new evidence preserves universe and qualification', () => {
    const { reloaded } = commitAndReloadPriorDiscovery();
    const priorBeliefs = collectCandidateBeliefsFromPayload(reloaded);

    const mode = resolveInvestigationMode({
      priorPayload: reloaded,
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.ENTITY_CONTINUATION);

    const context = buildInvestigationContinuationContext({
      priorPayload: reloaded,
      opts: { investigationContinuation: true },
    });
    assert.equal(context.preservedCandidates.length, 24);
    assert.equal(context.investigationMode, INVESTIGATION_MODES.ENTITY_CONTINUATION);

    const nextPayload = {
      candidateUniverse: beliefsToPreservedCandidates(priorBeliefs).map((row) => ({
        candidateId: row.id,
        candidate_id: row.id,
        canonicalIdentity: row.canonicalIdentity,
        name: row.name,
        address: row.location,
        qualification: row.qualification,
        readiness: row.readiness,
        evaluation: row.evaluation,
        businessFit: row.businessFit,
        evidenceRefs: row.evidenceRefs,
        dedupeStatus: 'primary',
      })),
      qualifiedCount: 14,
      candidateUniverseCount: 24,
    };

    const integrity = checkBeliefRegressionIntegrity({
      priorPayload: reloaded,
      nextPayload,
    });
    assert.equal(integrity.violation, false);
    assert.equal(integrity.priorTotal, 24);
    assert.equal(integrity.nextTotal, 24);
    assert.equal(integrity.priorQualified, 14);
    assert.equal(integrity.nextQualified, 14);

    const nextBeliefs = collectCandidateBeliefsFromPayload(nextPayload);
    assert.equal(nextBeliefs.size, 24);
    assert.equal(countQualifiedBeliefs(nextBeliefs), 14);
  });

  it('corrupted state — count without records fails closed at normalization', () => {
    assert.throws(
      () =>
        normalizeScoutDiscoveryPayload(
          {
            status: 'completed',
            payload: {
              candidateUniverse: [],
              candidateUniverseCount: 24,
              discoveryReport: { candidateUniverse: 24, qualified: 14 },
              evidence: [{ label: 'Google Places search', source: 'google_places' }],
            },
          },
          { missionObjective: 'Test objective.' }
        ),
      (err) => err.code === 'CANDIDATE_BELIEF_PERSISTENCE_FAILURE'
    );
  });

  it('corrupted state — assertCandidateBeliefPersistence rejects scalar-only universe', () => {
    assert.throws(
      () =>
        assertCandidateBeliefPersistence({
          candidateUniverseCount: 24,
          candidateUniverse: [],
          discoveryReport: { candidateUniverse: 24 },
        }),
      (err) => {
        assert.equal(err.code, 'CANDIDATE_BELIEF_PERSISTENCE_FAILURE');
        return true;
      }
    );
  });

  it('checkBeliefRegressionIntegrity rejects 24 → 0 universe collapse without evidence', () => {
    const { reloaded } = commitAndReloadPriorDiscovery();
    const integrity = checkBeliefRegressionIntegrity({
      priorPayload: reloaded,
      nextPayload: { candidateUniverse: [], candidateUniverseCount: 0 },
    });

    assert.equal(integrity.violation, true);
    assert.equal(integrity.priorTotal, 24);
    assert.equal(integrity.nextTotal, 0);
    assert.match(integrity.message, /collapsed to 0/i);
  });

  it('candidateUniverseCount is derived from canonical records during normalization', () => {
    const { normalized } = commitAndReloadPriorDiscovery();
    assert.equal(normalized.candidateUniverseCount, normalized.candidateUniverse.length);
    assert.notEqual(normalized.candidateUniverseCount, null);
  });

  it('resolveInvestigationMode uses hydrated canonical candidates for AUDIT-080 state', () => {
    const { reloaded } = commitAndReloadPriorDiscovery();
    reloaded.candidateUniverse = [];
    reloaded.candidateUniverseCount = 24;

    const mode = resolveInvestigationMode({
      priorPayload: reloaded,
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.ENTITY_CONTINUATION);
  });
});
