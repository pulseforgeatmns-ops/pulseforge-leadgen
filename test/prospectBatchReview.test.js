'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  evaluateScoutCandidate,
  applyMapsOnlyDowngradeToBatch,
  CANDIDATE_STATUS,
  CONFIDENCE,
} = require('../services/scoutQualityGate');
const {
  ARTIFACT_KINDS,
  resolveCampaignArtifactAction,
  looksLikeProspectBatchReviewRequest,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  emptyReasoningMemory,
  markArtifactApproved,
} = require('../services/clientIntelligenceReasoning');
const {
  buildProspectBatchReview,
  formatProspectBatchReviewMessage,
  buildCampaignPlanningReply,
  PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
} = require('../services/clientIntelligenceCampaignPlanning');

function sampleCompletedBatch() {
  return {
    workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
    reviewOnly: true,
    candidates: [
      {
        companyName: 'Keyrenter New England',
        location: 'Bedford NH',
        sourceUrl: 'https://keyrenternewengland.com/auburn-property-management',
        website: 'https://keyrenternewengland.com',
        fitRationale:
          'Keyrenter New England sourced from public listing — address/location on source: Bedford NH — property-management relevance on source: property management',
        risks: 'Public-source only — verify contact before outreach',
        suggestedContactRole: 'Suggested contact role: Owner / property manager',
        confidence: 'high',
        status: 'accepted',
        statusReason: 'Passes NH property-manager quality gates',
      },
      {
        companyName: 'Elm Grove Companies',
        location: 'Hooksett NH',
        sourceUrl: 'https://www.elmgrovecompanies.com/contact',
        website: 'https://www.elmgrovecompanies.com',
        fitRationale:
          'Elm Grove Companies sourced from public listing — address/location on source: Hooksett NH — property-management relevance on source: property management',
        risks: 'Public-source only — verify contact before outreach',
        suggestedContactRole: 'Suggested contact role: Owner / property manager',
        confidence: 'high',
        status: 'accepted',
        statusReason: 'Passes NH property-manager quality gates',
      },
      {
        companyName: 'Cedar Management Group',
        location: 'Hooksett NH',
        sourceUrl:
          'https://www.google.com/maps/place/Cedar+Management+Group/@43.08,-71.45,17z',
        website:
          'https://www.google.com/maps/place/Cedar+Management+Group/@43.08,-71.45,17z',
        fitRationale:
          'Cedar Management Group sourced from public listing — address/location on source: Hooksett NH — listing category/type: real_estate_agency',
        risks: 'No company website on listing — using maps listing as source URL',
        suggestedContactRole: 'Suggested contact role: Owner / property manager',
        confidence: 'high',
        status: 'accepted',
        statusReason: 'Passes NH property-manager quality gates',
      },
      {
        companyName: 'Mill City Property Management',
        location: 'Manchester NH',
        sourceUrl: 'https://www.millcitypm.com/',
        website: 'https://www.millcitypm.com/',
        fitRationale:
          'Mill City Property Management sourced from public listing — address/location on source: Manchester NH — property-management relevance on source: property management',
        risks:
          'outside_primary_town_cluster — Manchester NH is not in Bedford/Hooksett/Londonderry/Auburn/Goffstown unless explicitly approved',
        suggestedContactRole: 'Suggested contact role: Owner / property manager',
        confidence: 'medium',
        status: 'review_required',
        statusReason:
          'Manchester NH outside_primary_town_cluster — review_required unless primary town approval exists',
        reasonCode: 'outside_primary_town_cluster',
      },
    ],
    rejected: [
      {
        companyName: 'National Apartment Trust',
        location: 'Boston MA',
        sourceUrl: 'https://example.com/nat',
        fitRationale: 'Large institutional listing',
        risks: 'large_institutional_firm',
        suggestedContactRole: 'Suggested contact role: Regional operations',
        confidence: 'review_required',
        status: 'rejected',
        statusReason: 'large_institutional_firm — hard reject',
        rejectionReason: 'large_institutional_firm',
      },
    ],
    groups: {
      accepted: null,
      review_required: null,
      rejected: null,
    },
  };
}

describe('Prospect Batch Review — maps-only downgrade', () => {
  it('downgrades Cedar Management Group maps-only listing to review_required / medium', () => {
    const gate = evaluateScoutCandidate(
      {
        companyName: 'Cedar Management Group',
        address: '1558 Hooksett Rd Ste 5, Hooksett, NH 03106, USA',
        location: 'Hooksett NH',
        sourceUrl:
          'https://www.google.com/maps/place/Cedar+Management+Group/Hooksett',
        website:
          'https://www.google.com/maps/place/Cedar+Management+Group/Hooksett',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-485-8503',
      },
      {
        targetSegment: 'Property managers',
        marketBounds: 'Bedford NH, Hooksett NH',
      }
    );
    assert.equal(gate.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
    assert.equal(gate.confidence, CONFIDENCE.MEDIUM);
    assert.match(String(gate.statusReason), /maps-only|no company website/i);
  });

  it('applyMapsOnlyDowngradeToBatch moves accepted maps-only rows into review_required', () => {
    const batch = sampleCompletedBatch();
    batch.groups = {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
    };
    const next = applyMapsOnlyDowngradeToBatch(batch);
    const cedar = next.candidates.find(
      (c) => c.companyName === 'Cedar Management Group'
    );
    assert.ok(cedar);
    assert.equal(cedar.status, 'review_required');
    assert.equal(cedar.confidence, 'medium');
    assert.ok(
      next.groups.review_required.some(
        (c) => c.companyName === 'Cedar Management Group'
      )
    );
    assert.ok(
      !next.groups.accepted.some(
        (c) => c.companyName === 'Cedar Management Group'
      )
    );
  });
});

describe('Prospect Batch Review — artifact routing', () => {
  it('detects Prospect Batch Review request intent', () => {
    const msg =
      'Create the Prospect Batch Review artifact from the latest completed Scout result.';
    assert.equal(looksLikeProspectBatchReviewRequest(msg), true);
    assert.equal(
      classifyProspectAcquisitionIntent(msg),
      PROSPECT_ACQUISITION_INTENTS.EMIT_PROSPECT_BATCH_REVIEW
    );
  });

  it('routes to emit_prospect_batch_review when Scout batch already completed', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    const batch = sampleCompletedBatch();
    batch.groups = {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
    };

    const action = resolveCampaignArtifactAction({
      userMessage:
        'Create the Prospect Batch Review artifact from the latest completed Scout result.\nworkRequestId: f0ac74ac-16a6-4dba-b024-d3727b285a86',
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      priorScoutCandidateBatch: batch,
      priorScoutHandoff: {
        status: 'completed',
        scoutRan: true,
        candidateBatch: batch,
      },
      step: 'scout_handoff_completed',
    });

    assert.equal(action.action, 'emit_prospect_batch_review');
    assert.equal(action.emitKind, ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW);
    assert.notEqual(action.action, 'emit_build_proposal');
    assert.notEqual(action.action, 'emit_prospect_list_draft');
    assert.notEqual(action.action, 'ack_build_approval');
    assert.doesNotMatch(
      String(action.note || ''),
      /Ask me to generate the first reviewable prospect list batch/i
    );
  });

  it('does not ask to regenerate the first batch when Scout results exist', () => {
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    const batch = sampleCompletedBatch();
    batch.groups = {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
    };
    const action = resolveCampaignArtifactAction({
      userMessage: 'Generate the first reviewable prospect list batch',
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      priorScoutCandidateBatch: batch,
      priorScoutHandoff: { status: 'completed', scoutRan: true, candidateBatch: batch },
      step: 'scout_handoff_completed',
    });
    assert.equal(action.action, 'emit_prospect_batch_review');
    assert.notEqual(action.action, 'emit_prospect_list_draft');
  });
});

describe('Prospect Batch Review — formatting', () => {
  it('builds sections and ends with the primary-town vs Manchester question', () => {
    const batch = sampleCompletedBatch();
    batch.groups = {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
    };
    const review = buildProspectBatchReview(batch, {
      workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
    });
    assert.ok(
      review.acceptedFirstPass.every((r) =>
        /Bedford|Hooksett|Londonderry|Auburn|Goffstown/i.test(r.location)
      )
    );
    assert.ok(
      review.acceptedFirstPass.every((r) => r.reviewStatus === 'accepted')
    );
    assert.ok(
      review.optionalExpansion.some(
        (r) => r.companyName === 'Cedar Management Group'
      )
    );
    assert.ok(
      review.optionalExpansion.some((r) => /Manchester/i.test(r.location))
    );
    assert.equal(review.rejected.length, 1);

    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /Prospect Batch Review/);
    assert.match(msg, /Accepted first-pass candidates/);
    assert.match(msg, /Optional expansion candidates/);
    assert.match(msg, /Rejected candidates/);
    assert.match(msg, /Cedar Management Group/);
    assert.match(msg, /Keyrenter New England/);
    assert.ok(msg.includes(PROSPECT_BATCH_REVIEW_CLOSING_QUESTION));
    assert.doesNotMatch(msg, /Ask me to generate the first reviewable/i);
    assert.doesNotMatch(msg, /Build proposal/i);
  });

  it('buildCampaignPlanningReply emits Prospect Batch Review from session Scout batch', () => {
    const batch = sampleCompletedBatch();
    batch.groups = {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
    };
    const reply = buildCampaignPlanningReply(
      'Create the Prospect Batch Review artifact from the latest completed Scout result.\nworkRequestId: f0ac74ac-16a6-4dba-b024-d3727b285a86',
      {
        step: 'scout_handoff_completed',
        slots: {
          previewGenerated: true,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
        },
        prospectListCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
        },
        prospectListBuildProposal: {
          kind: 'prospect_list_build_proposal',
          status: 'approved',
        },
        scoutHandoff: {
          status: 'completed',
          scoutRan: true,
          candidateBatch: batch,
          workRequest: {
            workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
            status: 'completed',
          },
        },
        scoutCandidateBatch: batch,
        scoutWorkRequest: {
          workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
          status: 'completed',
        },
      },
      { businessName: 'Anchor Cleaning' },
      {
        priorScoutCandidateBatch: batch,
        priorScoutHandoff: {
          status: 'completed',
          scoutRan: true,
          candidateBatch: batch,
        },
      }
    );

    assert.equal(reply.intent, 'prospect_batch_review');
    assert.ok(reply.prospectBatchReview);
    assert.match(reply.message, /Prospect Batch Review/);
    assert.match(reply.message, /Cedar Management Group/);
    assert.ok(reply.message.includes(PROSPECT_BATCH_REVIEW_CLOSING_QUESTION));
    assert.doesNotMatch(
      reply.message,
      /Ask me to generate the first reviewable prospect list batch/i
    );
    assert.doesNotMatch(reply.message, /Prospect List Build Proposal/i);
  });
});
