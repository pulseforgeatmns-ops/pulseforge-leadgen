'use strict';

/**
 * SPEC-192 — Canonical Coverage Contract acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { normalizeCoverage, renderCoverage } = require('../CanonicalCoverage');
const {
  presentationFromDiscoveryPayload,
  formatDiscoveryResultsLines,
} = require('../DiscoveryPresentation');

describe('SPEC-192 — Canonical Coverage Contract', () => {
  it('standard discovery coverage normalizes with null hypothesis metrics', () => {
    const coverage = normalizeCoverage({
      coverage: {
        cities: { searched: 4, planned: 6, ratio: 4 / 6 },
        concepts: { searched: 6, planned: 6, ratio: 1 },
        sources: { searched: 1, planned: 1, ratio: 1 },
        complete: false,
        warnings: ['Only 4 / 6 cities searched.'],
      },
      qualifiedCount: 2,
      confidence: 0.71,
    });

    assert.equal(coverage.cities.searched, 4);
    assert.equal(coverage.cities.planned, 6);
    assert.equal(coverage.concepts.searched, 6);
    assert.equal(coverage.evidenceRequirements.satisfied, null);
    assert.equal(coverage.evidenceRequirements.planned, null);
    assert.equal(coverage.tasks.executed, null);
    assert.equal(coverage.tasks.planned, null);
    assert.equal(coverage.qualified, 2);
    assert.equal(coverage.confidence, 0.71);
    assert.equal(coverage.complete, false);
  });

  it('hypothesis discovery coverage normalizes with null legacy metrics', () => {
    const coverage = normalizeCoverage({
      coverage: {
        cities: { searched: null, planned: null, ratio: null },
        concepts: { searched: null, planned: null, ratio: null },
        sources: { searched: null, planned: null, ratio: null },
        evidenceRequirements: { satisfied: 3, planned: 5, ratio: 0.6 },
        tasks: { executed: 2, planned: 4, ratio: 0.5 },
        complete: false,
        warnings: [],
      },
    });

    assert.equal(coverage.cities.searched, null);
    assert.equal(coverage.concepts.searched, null);
    assert.equal(coverage.sources.searched, null);
    assert.equal(coverage.evidenceRequirements.satisfied, 3);
    assert.equal(coverage.evidenceRequirements.planned, 5);
    assert.equal(coverage.tasks.executed, 2);
    assert.equal(coverage.tasks.planned, 4);
  });

  it('renderCoverage never emits undefined/undefined', () => {
    const hypothesisLines = renderCoverage(
      normalizeCoverage({
        coverage: {
          evidenceRequirements: { satisfied: 2, planned: 4, ratio: 0.5 },
          tasks: { executed: 1, planned: 3, ratio: 1 / 3 },
          complete: false,
          warnings: ['Investigation incomplete.'],
        },
      }),
      { discoveryStatus: 'incomplete' }
    );

    const joined = hypothesisLines.join('\n');
    assert.doesNotMatch(joined, /undefined\/undefined/);
    assert.match(joined, /Evidence requirements: 2\/4/);
    assert.match(joined, /Investigation tasks: 1\/3/);
    assert.doesNotMatch(joined, /Cities searched:/);

    const legacyLines = renderCoverage(
      normalizeCoverage({
        coverage: {
          cities: { searched: 6, planned: 6, ratio: 1 },
          concepts: { searched: 6, planned: 6, ratio: 1 },
          sources: { searched: 1, planned: 1, ratio: 1 },
          complete: true,
          warnings: [],
        },
        qualifiedCount: 0,
        confidence: 0.82,
      })
    );

    const legacyJoined = legacyLines.join('\n');
    assert.doesNotMatch(legacyJoined, /undefined\/undefined/);
    assert.match(legacyJoined, /Cities searched: 6\/6/);
    assert.match(legacyJoined, /Concepts: 6\/6/);
    assert.match(legacyJoined, /Sources: 1\/1/);
    assert.doesNotMatch(legacyJoined, /Evidence requirements:/);
  });

  it('presentationFromDiscoveryPayload uses canonical coverage for both engines', () => {
    const legacyPresentation = presentationFromDiscoveryPayload({
      rankedProspects: [{ rank: 1, name: 'Granite PM' }],
      qualifiedCount: 1,
      summary: 'Found one prospect.',
      discoveryStatus: 'complete',
      coverage: {
        cities: { searched: 6, planned: 6, ratio: 1 },
        concepts: { searched: 6, planned: 6, ratio: 1 },
        sources: { searched: 1, planned: 1, ratio: 1 },
        complete: true,
        warnings: [],
      },
    });

    assert.equal(legacyPresentation.coverage.cities.searched, 6);
    assert.equal(legacyPresentation.coverage.evidenceRequirements.satisfied, null);

    const hypothesisPresentation = presentationFromDiscoveryPayload({
      rankedProspects: [{ rank: 1, name: 'Harbor STR' }],
      qualifiedCount: 1,
      summary: 'Hypothesis investigation partial.',
      discoveryStatus: 'incomplete',
      coverage: {
        cities: { searched: null, planned: null, ratio: null },
        concepts: { searched: null, planned: null, ratio: null },
        sources: { searched: null, planned: null, ratio: null },
        evidenceRequirements: { satisfied: 1, planned: 3, ratio: 1 / 3 },
        tasks: { executed: 2, planned: 5, ratio: 0.4 },
        complete: false,
        warnings: ['Investigation incomplete.'],
      },
    });

    assert.equal(hypothesisPresentation.coverage.cities.searched, null);
    assert.equal(hypothesisPresentation.coverage.evidenceRequirements.satisfied, 1);

    const lines = formatDiscoveryResultsLines(hypothesisPresentation);
    const prose = lines.join('\n');
    assert.doesNotMatch(prose, /undefined\/undefined/);
    assert.match(prose, /Evidence requirements: 1\/3/);
  });
});
