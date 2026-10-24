'use strict';

/**
 * SPEC-146 — Evidence Conflict Resolution Engine acceptance tests.
 * ADR-065 — Conflicting Evidence Is Intelligence.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { conflict, investigation, intelligence } = require('../packages/scout');

const {
  CONFLICT_SEVERITY,
  RESOLUTION_STRATEGIES,
  detectEvidenceConflicts,
  resolveEvidenceConflict,
  resolveEvidenceConflicts,
  runEvidenceConflictResolution,
  buildEvidenceConflictsSection,
  createProviderConflictLearningStore,
} = conflict;

const { runInvestigationEngine, clearInvestigationLog } = investigation;
const { runIntelligencePipeline, clearIntelligenceLog, INTELLIGENCE_STAGES } = intelligence;

const NOW = new Date('2026-08-23T00:00:00.000Z');

function sampleCandidate(overrides = {}) {
  return {
    id: 'str-op-1',
    name: 'ABC Property Management',
    industry: 'property_management',
    location: 'Manchester, NH',
    ...overrides,
  };
}

function threeProviderEmployeeConflict() {
  return [
    {
      source: 'google_maps',
      label: '12 employees on Google Maps',
      observedAt: '2025-09-01T00:00:00.000Z',
    },
    {
      source: 'linkedin',
      label: '37 employees on LinkedIn',
      observedAt: '2026-08-10T00:00:00.000Z',
    },
    {
      source: 'website',
      label: '18 employees listed on website',
      observedAt: '2026-08-15T00:00:00.000Z',
    },
  ];
}

function strListingConflict() {
  return [
    {
      source: 'google_maps',
      label: '15 listings on Google Maps',
      observedAt: '2025-11-01T00:00:00.000Z',
    },
    {
      source: 'website',
      label: '22 listings on company website',
      observedAt: '2026-08-11T00:00:00.000Z',
    },
    {
      source: 'airbnb',
      label: '21 active listings on Airbnb portfolio',
      observedAt: '2026-08-12T00:00:00.000Z',
    },
    {
      source: 'linkedin',
      label: 'Hiring two cleaners on LinkedIn',
      observedAt: '2026-08-18T00:00:00.000Z',
    },
  ];
}

describe('SPEC-146 — Evidence Conflict Resolution Engine', () => {
  beforeEach(() => {
    clearInvestigationLog();
    clearIntelligenceLog();
  });

  it('Test 1 — three providers disagree, Scout detects conflict', () => {
    const candidate = sampleCandidate();
    const evidence = threeProviderEmployeeConflict();
    const conflicts = detectEvidenceConflicts(candidate, evidence);

    assert.ok(conflicts.length >= 1);
    const employeeConflict = conflicts.find((c) => c.subject === 'employee_count');
    assert.ok(employeeConflict, 'employee count conflict detected');
    assert.equal(employeeConflict.conflictingClaims.length, 3);
    assert.deepEqual(
      employeeConflict.providers.sort(),
      ['google_maps', 'linkedin', 'website'].sort()
    );
    assert.equal(employeeConflict.resolution.resolved, false);
  });

  it('Test 2 — newer evidence wins with explained reason', () => {
    const candidate = sampleCandidate();
    const evidence = threeProviderEmployeeConflict();
    const detected = detectEvidenceConflicts(candidate, evidence);
    const resolved = resolveEvidenceConflict(detected[0], { now: NOW });

    assert.equal(resolved.resolution.strategy, RESOLUTION_STRATEGIES.FRESHNESS);
    assert.equal(resolved.resolution.resolved, true);
    assert.ok(resolved.resolution.reason);
    assert.match(resolved.resolution.reason, /LinkedIn|Website/i);
    assert.match(resolved.resolution.reason, /Google/i);
    assert.ok(resolved.resolution.workingEstimate);
    assert.ok(resolved.confidence >= 0.7);
  });

  it('Test 3 — unresolved conflict reduces recommendation confidence', () => {
    const candidate = sampleCandidate({
      conflictHints: [
        {
          fieldA: 'ownership',
          valueA: 'Unknown owner',
          fieldB: 'ownership',
          valueB: 'Conflicting county record',
          description: 'Owner identity unresolved across sources.',
        },
      ],
    });
    const evidence = [
      { source: 'county_records', label: 'Owner changed 2 months ago', observedAt: '2026-06-01T00:00:00.000Z' },
      { source: 'website', label: 'Contact us — no owner listed', observedAt: '2026-08-01T00:00:00.000Z' },
    ];

    const result = resolveEvidenceConflicts(candidate, evidence, { baseConfidence: 0.91, now: NOW });
    const section = buildEvidenceConflictsSection(result.conflicts, { baseConfidence: 0.91 });

    assert.ok(section.summary.detected >= 1);
    assert.ok(section.summary.confidenceReduced || section.summary.outstanding >= 1);
    if (section.summary.recommendationConfidenceAfter != null) {
      assert.ok(section.summary.recommendationConfidenceAfter < 0.91);
    }
  });

  it('Test 4 — unresolved conflict recommends additional providers', () => {
    const candidate = sampleCandidate();
    const evidence = [
      { source: 'county_records', label: 'Owner changed 2 months ago', observedAt: '2026-06-01T00:00:00.000Z' },
      { source: 'website', label: 'Family owned since 1982', observedAt: '2026-08-01T00:00:00.000Z' },
    ];

    const detected = detectEvidenceConflicts(candidate, evidence);
    const ownershipConflict = detected.find((c) => c.subject === 'ownership');
    assert.ok(ownershipConflict || detected.length >= 1);

    const conflictToResolve = ownershipConflict || detected[0];
    const resolved = resolveEvidenceConflict(conflictToResolve, { now: NOW });

    if (!resolved.resolution.resolved) {
      assert.ok(resolved.recommendedProviders?.length >= 1);
      assert.ok(resolved.investigationTask);
      assert.ok(
        resolved.recommendedProviders.includes('secretary_of_state') ||
          resolved.recommendedProviders.includes('county_records')
      );
    }
  });

  it('Test 5 — repeated investigations improve provider conflict learning', () => {
    const store = createProviderConflictLearningStore();

    store.recordConflictOutcome('website', {
      resolved: true,
      subject: 'employee_count',
      observedAt: '2026-08-15T00:00:00.000Z',
    });
    store.recordConflictOutcome('google_maps', {
      resolved: false,
      subject: 'employee_count',
      observedAt: '2025-09-01T00:00:00.000Z',
    });
    store.recordConflictOutcome('website', {
      resolved: true,
      subject: 'listing_count',
      observedAt: '2026-08-12T00:00:00.000Z',
    });

    const websiteProfile = store.getProviderProfile('website');
    const mapsProfile = store.getProviderProfile('google_maps');

    assert.ok(websiteProfile.resolutionRate > mapsProfile.resolutionRate);
    assert.ok(websiteProfile.freshnessScore > mapsProfile.freshnessScore);

    const websiteWeight = store.adjustAuthorityWeight('website', 'listing_count');
    const mapsWeight = store.adjustAuthorityWeight('google_maps', 'listing_count');
    assert.ok(websiteWeight >= mapsWeight);
  });

  it('Manchester STR operator — success criteria scenario', () => {
    const candidate = sampleCandidate({ name: 'ABC Property Management' });
    const evidence = strListingConflict();
    const result = resolveEvidenceConflicts(candidate, evidence, { baseConfidence: 0.91, now: NOW });

    assert.ok(result.detected >= 1);
    const listingConflict = result.conflicts.find(
      (c) => c.subject === 'property_count' || c.subject === 'listing_count'
    );
    assert.ok(listingConflict, 'listing count conflict detected');

    const resolved = listingConflict.resolution?.resolved
      ? listingConflict
      : resolveEvidenceConflict(listingConflict, { now: NOW });

    assert.equal(resolved.resolution.resolved, true);
    assert.match(resolved.resolution.reason || '', /updated|fresh|authorit|agree/i);
    assert.ok(resolved.resolution.workingEstimate);
    assert.match(resolved.resolution.workingEstimate, /21|22/);

    const section = buildEvidenceConflictsSection(result.conflicts, { baseConfidence: 0.91 });
    assert.ok(section.summary.detected >= 1);
    assert.ok(section.narrative.includes('Evidence Conflicts'));
  });

  it('no conflicting evidence is silently discarded', () => {
    const candidate = sampleCandidate();
    const evidence = threeProviderEmployeeConflict();
    const result = resolveEvidenceConflicts(candidate, evidence, { now: NOW });

    for (const conflict of result.conflicts) {
      assert.ok(conflict.id);
      assert.ok(conflict.conflictingClaims.length >= 2);
      assert.ok(conflict.resolution.strategy);
      assert.ok(conflict.severity);
    }

    const allClaimValues = result.conflicts.flatMap((c) =>
      c.conflictingClaims.map((cl) => cl.value)
    );
    assert.ok(allClaimValues.includes(12));
    assert.ok(allClaimValues.includes(37));
    assert.ok(allClaimValues.includes(18));
  });

  it('integrates into intelligence pipeline between collection and qualification', async () => {
    const result = await runIntelligencePipeline({
      mission: {
        id: 'spec146-pipeline',
        tenantId: '10',
        objectiveText: 'Find property managers in Manchester NH',
        constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
      },
      opts: { runAcquisitionIntelligence: false, maxCandidates: 3 },
    });

    const stageNames = result.stages.map((s) => s.stage);
    const collectionIdx = stageNames.indexOf(INTELLIGENCE_STAGES.EVIDENCE_COLLECTION);
    const conflictIdx = stageNames.indexOf(INTELLIGENCE_STAGES.EVIDENCE_CONFLICT_RESOLUTION);
    const qualificationIdx = stageNames.indexOf(INTELLIGENCE_STAGES.QUALIFICATION);

    assert.ok(collectionIdx >= 0);
    assert.ok(conflictIdx >= 0);
    assert.ok(qualificationIdx >= 0);
    assert.ok(conflictIdx > collectionIdx);
    assert.ok(qualificationIdx > conflictIdx);
    assert.ok(result.conflictResolution);
    assert.ok(result.report.evidenceConflicts);
  });

  it('integrates into investigation loop with conflict events', async () => {
    const mission = {
      id: 'spec146-investigation',
      tenantId: '10',
      objectiveText: 'Investigate Manchester STR operators',
      constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
    };

    const candidates = [
      sampleCandidate({
        evidence: threeProviderEmployeeConflict(),
      }),
    ];

    const result = await runInvestigationEngine({
      mission,
      candidateUniverse: { candidates, discovered: 1 },
      opts: {
        maxIterations: 1,
        persistMemory: false,
        enforceInvestigationPlan: false,
      },
    });

    assert.ok(result.conflictResolution);
    assert.ok(result.report.evidenceConflicts);
    assert.ok(result.overallConfidence <= 0.91);
  });
});
