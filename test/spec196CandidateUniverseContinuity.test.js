'use strict';

/**
 * SPEC-196 — Candidate Universe Continuity (AUDIT-071).
 * Discovered canonical business identities remain investigable across continuation
 * even when qualifiedCount = 0 and rankedProspects / fitCandidates are empty.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  INVESTIGATION_MODES,
  extractInvestigationCandidatesFromPayload,
  extractPreservedCandidatesFromPayload,
  resolveInvestigationMode,
  buildInvestigationContinuationContext,
  buildEntityGapTasksForCandidate,
  canonicalIdentityKey,
} = require('../packages/scout/investigation/EntityInvestigationContinuation');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const { buildScoutDiscoveryArtifact } = require('../packages/scout/adapters/ScoutDiscoveryArtifact');

const ANCHOR_BUSINESSES = [
  { candidate_id: 'candidate-lot-202', name: 'Lot 202 Property Management', placeId: 'place-lot-202' },
  { candidate_id: 'candidate-blue-door', name: 'Blue Door STR', unknowns: ['STR portfolio uncertainty'] },
  { candidate_id: 'candidate-mill-city', name: 'Mill City PM', unknowns: ['No identifiable operations decision-maker'] },
  { candidate_id: 'candidate-harbor', name: 'Harbor Law Group' },
  { candidate_id: 'candidate-summit', name: 'Summit STR Management' },
  { candidate_id: 'candidate-river', name: 'Riverfront Rentals' },
  { candidate_id: 'candidate-oak', name: 'Oak Street Property Co' },
  { candidate_id: 'candidate-pine', name: 'Pine Valley PM' },
  { candidate_id: 'candidate-maple', name: 'Maple Grove STR' },
  { candidate_id: 'candidate-cedar', name: 'Cedar Hill Properties' },
  { candidate_id: 'candidate-birch', name: 'Birch Lane Management' },
  { candidate_id: 'candidate-willow', name: 'Willow Creek Rentals' },
  { candidate_id: 'candidate-ash', name: 'Ashwood Property Group' },
  { candidate_id: 'candidate-elm', name: 'Elm Street PM' },
  { candidate_id: 'candidate-spruce', name: 'Spruce Ridge STR' },
];

function audit071RawScoutResult() {
  return {
    status: 'completed',
    summary: '15 businesses discovered; none qualified yet.',
    payload: {
      opportunities: [],
      fitCandidates: [],
      uncertainCandidates: [],
      qualifiedCount: 0,
      readinessReadyCount: 0,
      readinessUnknownCount: 0,
      readinessNotReadyCount: 0,
      candidateUniverse: ANCHOR_BUSINESSES.map((row) => ({
        ...row,
        origin: 'external_discovery',
        sources: ['google_places'],
        cities: ['Manchester, NH'],
        confidence: 0.55,
        dedupeStatus: 'primary',
      })),
      evidence: [{ label: 'Google Places search', source: 'google_places' }],
      discoveryStatus: 'complete',
      providerExecution: [{ source: 'google_places', succeeded: true, resultCount: 15 }],
    },
  };
}

function audit071Contribution() {
  return normalizeScoutDiscoveryPayload(audit071RawScoutResult(), {
    missionObjective: 'Acquire commercial cleaning customers in Manchester NH.',
  });
}

describe('SPEC-196 — Candidate Universe Continuity (AUDIT-071)', () => {
  it('candidateUniverse array survives normalization round-trip', () => {
    const contribution = audit071Contribution();
    assert.equal(contribution.candidateUniverseCount, 15);
    assert.equal(contribution.qualifiedCount, 0);
    assert.equal(contribution.rankedProspects.length, 0);
    assert.equal(contribution.candidateUniverse.length, 15);
    assert.equal(contribution.rankedProspectCount, 0);
    assert.ok(contribution.readinessKnownCount != null);
    assert.ok(contribution.excludedCount != null);
  });

  it('ScoutDiscoveryArtifact preserves candidateUniverse array', () => {
    const artifact = buildScoutDiscoveryArtifact(audit071RawScoutResult());
    assert.equal(artifact.candidateUniverse.length, 15);
    assert.equal(artifact.candidateUniverseCount, 15);
  });

  it('AUDIT-071 — extractInvestigationCandidatesFromPayload returns 15 canonical businesses', () => {
    const payload = audit071Contribution();
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    assert.equal(candidates.length, 15);
    assert.ok(candidates.some((row) => row.id === 'candidate-lot-202'));
    assert.ok(candidates.some((row) => row.name === 'Blue Door STR'));
    assert.ok(candidates.some((row) => row.name === 'Mill City PM'));
  });

  it('AUDIT-071 — resolveInvestigationMode returns entity_continuation', () => {
    const payload = audit071Contribution();
    const mode = resolveInvestigationMode({
      priorPayload: payload,
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.ENTITY_CONTINUATION);
  });

  it('AUDIT-071 — CandidateInvestigationTask binds to real candidateId', () => {
    const payload = audit071Contribution();
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    const lot202 = candidates.find((row) => row.id === 'candidate-lot-202');
    assert.ok(lot202);

    const tasks = buildEntityGapTasksForCandidate(lot202);
    assert.ok(tasks.length > 0, 'expected at least one investigation task');
    assert.ok(tasks.every((task) => task.candidateId === 'candidate-lot-202'));
    assert.ok(tasks.every((task) => String(task.id).startsWith('task:candidate-lot-202:')));
  });

  it('candidate IDs survive across executions via preserved candidates', () => {
    const payload = audit071Contribution();
    const preserved = extractPreservedCandidatesFromPayload(payload);
    assert.equal(preserved.length, 15);
    assert.ok(preserved.every((row) => row._preservedFromContinuation === true));
    assert.ok(preserved.some((row) => row.id === 'candidate-lot-202'));
    assert.ok(preserved.some((row) => row.id === 'candidate-blue-door'));
  });

  it('buildInvestigationContinuationContext selects entity continuation with task count', () => {
    const payload = audit071Contribution();
    const context = buildInvestigationContinuationContext({
      priorPayload: payload,
      opts: { investigationContinuation: true },
    });
    assert.equal(context.investigationMode, INVESTIGATION_MODES.ENTITY_CONTINUATION);
    assert.equal(context.entityCandidates.length, 15);
    assert.equal(context.preservedCandidates.length, 15);
    assert.ok(context.entityTaskCount > 0);
  });

  it('count-only payload without array still routes to broad discovery', () => {
    const mode = resolveInvestigationMode({
      priorPayload: {
        qualifiedCount: 0,
        rankedProspects: [],
        fitCandidates: [],
        candidateUniverseCount: 15,
      },
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.BROAD_DISCOVERY);
  });

  it('deduplicates by canonical identity across buckets', () => {
    const payload = {
      rankedProspects: [],
      fitCandidates: [],
      candidateUniverse: [
        { candidate_id: 'candidate-lot-202', name: 'Lot 202 Property Management', dedupeStatus: 'primary' },
        { candidate_id: 'candidate-lot-202-dup', name: 'Lot 202 Property Management', dedupeStatus: 'duplicate' },
        { candidate_id: 'candidate-blue-door', name: 'Blue Door STR', dedupeStatus: 'primary' },
      ],
    };
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    assert.equal(candidates.length, 2);
    const keys = new Set(candidates.map((row) => canonicalIdentityKey(row)));
    assert.equal(keys.size, 2);
  });

  it('explicitly excluded universe records are not investigable', () => {
    const payload = {
      candidateUniverse: [
        { candidate_id: 'candidate-excluded', name: 'Excluded Co', excluded: true, dedupeStatus: 'primary' },
        { candidate_id: 'candidate-active', name: 'Active Co', dedupeStatus: 'primary' },
      ],
    };
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].id, 'candidate-active');
  });

  it('regression — candidateUniverseCount > 0 with uncertainty must not force broad_discovery', () => {
    const payload = audit071Contribution();
    const mode = resolveInvestigationMode({
      priorPayload: payload,
      opts: { investigationContinuation: true },
    });
    assert.notEqual(mode, INVESTIGATION_MODES.BROAD_DISCOVERY);
  });
});
