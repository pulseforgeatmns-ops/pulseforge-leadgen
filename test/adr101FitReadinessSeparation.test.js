'use strict';

/**
 * ADR-101 — Fit and buying readiness are separate judgments.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { qualifyCandidate } = require('../packages/max/scoutAcquisition/InvestigationProvenance');
const {
  READINESS_STATES,
  EVIDENCE_KINDS,
  REJECTION_REASONS,
} = require('../packages/max/scoutAcquisition/Types');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('../packages/acquisition-mission/DiscoveryPayload');
const { presentationFromDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPresentation');
const { evaluateBasicFit, attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');

const NOW = new Date('2026-08-27T12:00:00.000Z').getTime();

function searchDefinition() {
  return buildAcquisitionSearchDefinition({
    tenantId: '10',
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['law_firm'],
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['law_firm'],
    },
  });
}

describe('ADR-101 — fit and readiness separation', () => {
  it('qualifies basic-fit candidates even when timing is unknown', () => {
    const classified = {
      name: 'Lot 202 Law Group',
      fit: 0.78,
      signals: [],
      observations: [{ text: 'Meets target profile.' }],
      evidenceRefs: [{ id: 'ev-1', label: 'Company website', sourceKind: 'observed_fact' }],
    };
    const result = qualifyCandidate(classified, { name: 'Lot 202 Law Group', icpScore: 72 }, NOW);

    assert.equal(result.qualified, true);
    assert.equal(result.basicFit, true);
    assert.equal(result.supported, false);
    assert.equal(result.readinessState, READINESS_STATES.UNKNOWN);
    assert.equal(result.evidenceKind, EVIDENCE_KINDS.INSUFFICIENT_EVIDENCE);
    assert.equal(result.reason, null);
    assert.equal(result.rejectedBecause, null);
  });

  it('marks stale timing as not ready without disqualifying the prospect', () => {
    const classified = {
      name: 'Stale Signal PM',
      fit: 0.8,
      signals: [
        {
          type: 'expansion',
          observedAt: '2023-01-01T00:00:00.000Z',
          label: 'Expanded portfolio.',
        },
      ],
      observations: [{ text: 'Portfolio operator.' }],
      evidenceRefs: [{ id: 'ev-2', label: 'Website', sourceKind: 'observed_fact' }],
    };
    const result = qualifyCandidate(classified, { name: 'Stale Signal PM' }, NOW);

    assert.equal(result.qualified, true);
    assert.equal(result.supported, false);
    assert.equal(result.readinessState, READINESS_STATES.NOT_READY);
    assert.equal(result.evidenceKind, EVIDENCE_KINDS.NEGATIVE_EVIDENCE);
    assert.equal(result.reason, REJECTION_REASONS.STALE_EVIDENCE);
  });

  it('marks timely supported evidence as ready', () => {
    const classified = {
      name: 'Ready Co',
      fit: 0.82,
      signals: [
        {
          type: 'hiring',
          observedAt: '2026-07-15T00:00:00.000Z',
          label: 'Hiring facilities coordinator.',
        },
      ],
      observations: [{ text: 'Active hiring signal.' }],
      evidenceRefs: [{ id: 'ev-3', label: 'Job board posting', sourceKind: 'observed_fact' }],
    };
    const result = qualifyCandidate(classified, { name: 'Ready Co' }, NOW);

    assert.equal(result.qualified, true);
    assert.equal(result.supported, true);
    assert.equal(result.readinessState, READINESS_STATES.READY);
    assert.equal(result.evidenceKind, EVIDENCE_KINDS.POSITIVE_EVIDENCE);
  });

  it('does not disqualify when outsourcing responsibility is unknown', () => {
    const definition = searchDefinition();
    const fit = evaluateBasicFit(
      {
        name: 'Harbor Law Group',
        industry: 'law_firm',
        location: 'Manchester, NH',
        website: 'https://harborlaw.example',
        snippet: 'Full-service law firm downtown Manchester',
        icpScore: 76,
      },
      definition
    );
    assert.equal(fit.basicFit, true);

    const attached = attachFitToClassified(
      {
        companyId: 'co-harbor',
        name: 'Harbor Law Group',
        signals: [],
        observations: [],
        unknowns: [],
        evidenceRefs: [],
      },
      {
        id: 'co-harbor',
        name: 'Harbor Law Group',
        industry: 'law_firm',
        location: 'Manchester, NH',
        website: 'https://harborlaw.example',
        icpScore: 76,
      },
      definition,
      NOW
    );

    assert.equal(attached.qualification.qualified, true);
    assert.equal(attached.qualification.readinessState, READINESS_STATES.UNKNOWN);
    assert.equal(attached.classified.qualified, true);
  });

  it('includes fit candidates in rankedProspects and qualifiedCount', () => {
    const scoutResult = {
      status: 'completed',
      summary: '21 qualified prospects; timing unknown.',
      payload: {
        opportunities: [],
        fitCandidates: [
          {
            companyId: 'co-1',
            name: 'Lot 202 Law Group',
            fit: 0.81,
            signals: [],
            evidenceRefs: [
              {
                id: 'ev-lot',
                label: 'Google Places listing',
                snapshot: { source: 'google_places', companyName: 'Lot 202 Law Group' },
              },
            ],
          },
          {
            companyId: 'co-2',
            name: 'Summit Legal Partners',
            fit: 0.77,
            signals: [],
            evidenceRefs: [
              {
                id: 'ev-summit',
                label: 'Google Places listing',
                snapshot: { source: 'google_places', companyName: 'Summit Legal Partners' },
              },
            ],
          },
        ],
        qualifiedCount: 2,
      },
    };

    const payload = normalizeScoutDiscoveryPayload(scoutResult, {
      missionObjective: 'Acquire law firm cleaning customers in Manchester NH.',
    });

    assert.equal(payload.qualifiedCount, 2);
    assert.equal(payload.rankedProspects.length, 2);
    assert.equal(payload.rankedProspects[0].readinessState, READINESS_STATES.UNKNOWN);
    assert.ok(payload.rankedProspects.every((row) => row.name));

    const presentation = presentationFromDiscoveryPayload(payload);
    assert.equal(hasSufficientEvidenceForPrioritization(presentation), true);
  });

  it('sorts ready prospects ahead of unknown-readiness prospects', () => {
    const scoutResult = {
      status: 'completed',
      summary: 'Mixed readiness.',
      payload: {
        opportunities: [
          {
            companyId: 'co-ready',
            name: 'Ready Law Group',
            fit: 0.7,
            timing: 0.8,
            signals: [{ type: 'hiring', label: 'Hiring office manager', source: 'job_board' }],
            evidenceRefs: [
              {
                id: 'ev-ready',
                label: 'Job posting',
                snapshot: { source: 'job_board', companyName: 'Ready Law Group' },
              },
            ],
          },
        ],
        fitCandidates: [
          {
            companyId: 'co-fit',
            name: 'Strong Fit Law',
            fit: 0.9,
            signals: [],
            evidenceRefs: [
              {
                id: 'ev-fit',
                label: 'Website',
                snapshot: { source: 'website', companyName: 'Strong Fit Law' },
              },
            ],
          },
        ],
        qualifiedCount: 2,
      },
    };

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.rankedProspects.length, 2);
    assert.equal(payload.rankedProspects[0].readinessState, READINESS_STATES.READY);
    assert.equal(payload.rankedProspects[0].name, 'Ready Law Group');
    assert.equal(payload.rankedProspects[1].readinessState, READINESS_STATES.UNKNOWN);
  });
});
