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
  looksLikeProspectBatchReviewCorrection,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  emptyReasoningMemory,
  markArtifactApproved,
  markArtifactGenerated,
  MESSAGE_CLASSES,
} = require('../services/clientIntelligenceReasoning');
const {
  buildProspectBatchReview,
  formatProspectBatchReviewMessage,
  buildCampaignPlanningReply,
  buildProspectBatchReviewClosingQuestion,
  parseRelationshipOverridesFromMessage,
  applyRelationshipOverridesToBatch,
  relationshipOverrideMatchesRow,
  normalizeCompanyIdentity,
  RELATIONSHIP_STATUS,
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

/** Fixture matching the operator Keyrenter-correction scenario (honest counts). */
function sampleKeyrenterCorrectionBatch() {
  const acceptedPrimaries = [
    {
      companyName: 'Elm Grove Companies',
      location: 'Hooksett NH',
      sourceUrl: 'https://www.elmgrovecompanies.com/contact',
      website: 'https://www.elmgrovecompanies.com',
      fitRationale: 'Hooksett NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Keyrenter New England Property Management',
      location: 'Bedford NH',
      sourceUrl: 'https://keyrenternewengland.com/auburn-property-management',
      website: 'https://keyrenternewengland.com',
      fitRationale: 'Bedford NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Pinecrest Property Management',
      location: 'Londonderry NH',
      sourceUrl: 'https://pinecrest.example',
      website: 'https://pinecrest.example',
      fitRationale: 'Londonderry NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Auburn Residential Partners',
      location: 'Auburn NH',
      sourceUrl: 'https://arp.example',
      website: 'https://arp.example',
      fitRationale: 'Auburn NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Goffstown Living Management',
      location: 'Goffstown NH',
      sourceUrl: 'https://glm.example',
      website: 'https://glm.example',
      fitRationale: 'Goffstown NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Bedford Harbor Residences',
      location: 'Bedford NH',
      sourceUrl: 'https://bhr.example',
      website: 'https://bhr.example',
      fitRationale: 'Bedford NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Hooksett Home Stewards',
      location: 'Hooksett NH',
      sourceUrl: 'https://hhs.example',
      website: 'https://hhs.example',
      fitRationale: 'Hooksett NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
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
      fitRationale: 'Hooksett NH listing — maps-only source',
      risks: 'No company website on listing — using maps listing as source URL',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
  ];
  const reviewRequired = [
    {
      companyName: 'Property Management',
      location: 'Manchester NH',
      sourceUrl: 'http://www.realpropertynh.com/',
      website: 'http://www.realpropertynh.com/',
      fitRationale: 'Manchester NH property management listing',
      risks:
        'outside_primary_town_cluster — Manchester NH is not in Bedford/Hooksett/Londonderry/Auburn/Goffstown unless explicitly approved',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'medium',
      status: 'review_required',
      statusReason:
        'Manchester NH outside_primary_town_cluster — review_required unless primary town approval exists',
      reasonCode: 'outside_primary_town_cluster',
    },
  ];
  return {
    workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
    reviewOnly: true,
    candidates: acceptedPrimaries.concat(reviewRequired),
    rejected: [
      {
        companyName: 'Cushman & Wakefield',
        location: 'Boston MA',
        sourceUrl: 'https://example.com/cushman',
        website: 'https://example.com/cushman',
        fitRationale: 'Large institutional firm',
        risks: 'large_institutional_firm',
        suggestedContactRole: 'Regional operations',
        confidence: 'low',
        status: 'rejected',
        statusReason: 'large_institutional_firm — hard reject',
        rejectionReason: 'large_institutional_firm',
      },
    ],
    groups: {
      accepted: acceptedPrimaries,
      review_required: reviewRequired,
      rejected: null,
    },
  };
}

function withGroups(batch) {
  return {
    ...batch,
    groups: {
      accepted: batch.candidates.filter((c) => c.status === 'accepted'),
      review_required: batch.candidates.filter(
        (c) => c.status === 'review_required'
      ),
      rejected: batch.rejected,
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
  it('builds sections and ends with a dynamic closing question', () => {
    const batch = withGroups(sampleCompletedBatch());
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
      review.sourceVerificationRequired.some(
        (r) => r.companyName === 'Cedar Management Group'
      )
    );
    assert.ok(
      review.optionalExpansion.some((r) => /Manchester/i.test(r.location))
    );
    assert.equal(review.rejected.length, 1);

    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /Prospect Batch Review/);
    assert.match(msg, /Accepted cold first-pass candidates/);
    assert.match(msg, /Source-verification required primary-town candidates/);
    assert.match(msg, /Rejected candidates/);
    assert.match(msg, /Cedar Management Group/);
    assert.match(msg, /Keyrenter New England/);
    assert.ok(msg.includes(review.closingQuestion));
    assert.doesNotMatch(msg, /8 primary-town candidates/i);
    assert.doesNotMatch(msg, /Ask me to generate the first reviewable/i);
    assert.doesNotMatch(msg, /Build proposal/i);
  });

  it('buildCampaignPlanningReply emits Prospect Batch Review from session Scout batch', () => {
    const batch = withGroups(sampleCompletedBatch());
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
    assert.ok(reply.message.includes(reply.prospectBatchReview.closingQuestion));
    assert.doesNotMatch(reply.message, /8 primary-town candidates/i);
    assert.doesNotMatch(
      reply.message,
      /Ask me to generate the first reviewable prospect list batch/i
    );
    assert.doesNotMatch(reply.message, /Prospect List Build Proposal/i);
  });
});

describe('Prospect Batch Review — NH market filter + preserve', () => {
  it('never shows out-of-state candidates as usable review candidates', () => {
    const batch = {
      workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
      candidates: [
        {
          companyName: 'Keyrenter New England',
          location: 'Bedford NH',
          sourceUrl: 'https://keyrenter.example',
          website: 'https://keyrenter.example',
          status: 'accepted',
          confidence: 'high',
          fitRationale: 'Bedford NH property management',
          risks: 'none',
          suggestedContactRole: 'Owner / property manager',
        },
        {
          companyName: 'Capitol Property Partners',
          location: 'Washington, DC, USA',
          sourceUrl: 'https://capitolpp.example',
          website: 'https://capitolpp.example',
          status: 'review_required',
          confidence: 'medium',
          fitRationale: 'DC listing',
          risks: 'out of market',
          suggestedContactRole: 'Owner / property manager',
        },
        {
          companyName: 'Arlington Asset Managers',
          location: 'Arlington, VA, USA',
          sourceUrl: 'https://arlingtonam.example',
          website: 'https://arlingtonam.example',
          status: 'review_required',
          confidence: 'medium',
          fitRationale: 'VA listing',
          risks: 'out of market',
          suggestedContactRole: 'Owner / property manager',
        },
        {
          companyName: 'Mill City Property Management',
          location: 'Manchester NH',
          sourceUrl: 'https://millcity.example',
          website: 'https://millcity.example',
          status: 'review_required',
          confidence: 'medium',
          fitRationale: 'Manchester NH expansion',
          risks: 'outside_primary_town_cluster',
          suggestedContactRole: 'Owner / property manager',
        },
      ],
      rejected: [],
    };
    const review = buildProspectBatchReview(batch);
    assert.ok(
      review.acceptedFirstPass.every((r) => /NH|New Hampshire/i.test(r.location))
    );
    assert.ok(
      review.optionalExpansion.every((r) => /NH|New Hampshire/i.test(r.location))
    );
    assert.ok(
      !review.optionalExpansion.some((r) =>
        /Washington|Arlington|DC|VA/i.test(r.location)
      )
    );
    assert.ok(
      review.rejected.some((r) => /Capitol|Arlington/i.test(r.companyName))
    );
  });

  it('preserves prior valid NH primary-town candidates when latest batch drops them', () => {
    const prior = {
      workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
      candidates: [
        {
          companyName: 'Keyrenter New England',
          location: 'Bedford NH',
          sourceUrl: 'https://keyrenter.example',
          website: 'https://keyrenter.example',
          status: 'accepted',
          confidence: 'high',
          fitRationale: 'Bedford NH',
          risks: 'none',
          suggestedContactRole: 'Owner',
        },
        {
          companyName: 'Elm Grove Companies',
          location: 'Hooksett NH',
          sourceUrl: 'https://elmgrove.example',
          website: 'https://elmgrove.example',
          status: 'accepted',
          confidence: 'high',
          fitRationale: 'Hooksett NH',
          risks: 'none',
          suggestedContactRole: 'Owner',
        },
      ],
      rejected: [],
    };
    const latest = {
      workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
      candidates: [
        {
          companyName: 'Keyrenter New England',
          location: 'Bedford NH',
          sourceUrl: 'https://keyrenter.example',
          website: 'https://keyrenter.example',
          status: 'accepted',
          confidence: 'high',
          fitRationale: 'Bedford NH',
          risks: 'none',
          suggestedContactRole: 'Owner',
        },
        {
          companyName: 'Capitol Property Partners',
          location: 'Washington, DC, USA',
          sourceUrl: 'https://capitolpp.example',
          website: 'https://capitolpp.example',
          status: 'review_required',
          confidence: 'medium',
          fitRationale: 'DC',
          risks: 'outside',
          suggestedContactRole: 'Owner',
        },
      ],
      rejected: [],
    };
    const review = buildProspectBatchReview(latest, {
      preserveFromBatch: prior,
    });
    assert.ok(
      review.acceptedFirstPass.some((r) => r.companyName === 'Elm Grove Companies')
    );
    assert.ok(
      review.acceptedFirstPass.some((r) => r.companyName === 'Keyrenter New England')
    );
    assert.ok(
      !review.optionalExpansion.some((r) => /Washington|DC/i.test(r.location))
    );
  });
});

describe('Prospect Batch Review — Keyrenter relationship override', () => {
  const correctionMessage =
    'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.';

  it('parses Keyrenter as existing_relationship from operator correction', () => {
    const overrides = parseRelationshipOverridesFromMessage(correctionMessage);
    assert.equal(overrides.length, 1);
    assert.equal(
      overrides[0].relationship,
      RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP
    );
    assert.match(overrides[0].companyName, /Keyrenter/i);
  });

  it('Keyrenter relationship override removes it from accepted candidates', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    assert.ok(
      !review.acceptedFirstPass.some((r) => /keyrenter/i.test(r.companyName))
    );
    assert.equal(review.counts.accepted, 6);
    assert.equal(review.acceptedFirstPass.length, 6);
  });

  it('Keyrenter renders in Existing relationship / nurture', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    assert.equal(review.existingRelationship.length, 1);
    assert.match(review.existingRelationship[0].companyName, /Keyrenter/i);
    assert.equal(
      review.existingRelationship[0].relationship,
      RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP
    );
    assert.equal(review.existingRelationship[0].doNotOutreach, true);
    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /Existing relationship \/ nurture/i);
    assert.match(msg, /Keyrenter/);
    assert.match(msg, /do not include in campaign outreach/i);
  });

  it('Cedar remains medium/review_required in source-verification', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    const cedar = review.sourceVerificationRequired.find((r) =>
      /Cedar Management Group/i.test(r.companyName)
    );
    assert.ok(cedar);
    assert.equal(cedar.reviewStatus, 'review_required');
    assert.equal(cedar.confidence, 'medium');
    assert.equal(review.counts.sourceVerificationRequired, 1);
  });

  it('counts match rendered sections', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    assert.equal(review.counts.accepted, review.acceptedFirstPass.length);
    assert.equal(
      review.counts.sourceVerificationRequired,
      review.sourceVerificationRequired.length
    );
    assert.equal(
      review.counts.existingRelationship,
      review.existingRelationship.length
    );
    assert.equal(review.counts.rejected, review.rejected.length);
    assert.equal(review.counts.accepted, 6);
    assert.equal(review.counts.sourceVerificationRequired, 1);
    assert.equal(review.counts.existingRelationship, 1);
    assert.equal(review.counts.rejected, 1);
    assert.ok(
      review.rejected.some((r) => /Cushman\s*&\s*Wakefield/i.test(r.companyName))
    );

    const msg = formatProspectBatchReviewMessage(review);
    assert.match(
      msg,
      /Accepted cold first-pass:\s*6/
    );
    assert.match(msg, /Source-verification required:\s*1/);
    assert.match(msg, /Existing relationship \/ nurture:\s*1/);
    assert.match(msg, /Rejected:\s*1/);
    assert.doesNotMatch(msg, /8 primary-town candidates/i);
    assert.equal(
      review.closingQuestion,
      'Do you want to approve the 6 accepted cold first-pass candidates, include Cedar after source verification, and keep Keyrenter as an existing-relationship nurture account?'
    );
    assert.ok(msg.includes(review.closingQuestion));
  });

  it('Prospect Batch Review correction does not route backward to Build Proposal prompt', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    let mem = emptyReasoningMemory();
    mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_CRITERIA);
    mem = markArtifactApproved(
      mem,
      ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
    );
    mem = markArtifactGenerated(
      mem,
      ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      'draft'
    );

    const priorReview = buildProspectBatchReview(batch, {
      workRequestId: batch.workRequestId,
    });

    assert.equal(
      looksLikeProspectBatchReviewCorrection(correctionMessage, {
        priorProspectBatchReview: priorReview,
        step: 'prospect_batch_review',
        memory: mem,
      }),
      true
    );
    assert.equal(
      classifyProspectAcquisitionIntent(correctionMessage, {
        priorProspectBatchReview: priorReview,
        step: 'prospect_batch_review',
        memory: mem,
      }),
      PROSPECT_ACQUISITION_INTENTS.CORRECT_PROSPECT_BATCH_REVIEW
    );

    const action = resolveCampaignArtifactAction({
      userMessage: correctionMessage,
      messageClass: MESSAGE_CLASSES.CORRECTION,
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
      priorProspectBatchReview: priorReview,
      step: 'prospect_batch_review',
    });

    assert.equal(action.action, 'emit_prospect_batch_review');
    assert.notEqual(action.action, 'ack_build_approval');
    assert.notEqual(action.action, 'emit_build_proposal');
    assert.doesNotMatch(
      String(action.note || ''),
      /Build proposal already approved/i
    );
    assert.doesNotMatch(
      String(action.note || ''),
      /Ask me to generate the first reviewable prospect list batch/i
    );

    const reply = buildCampaignPlanningReply(
      correctionMessage,
      {
        step: 'prospect_batch_review',
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
        scoutCandidateBatch: batch,
        prospectBatchReview: priorReview,
      },
      { businessName: 'Anchor Cleaning' },
      {
        priorScoutCandidateBatch: batch,
        priorProspectBatchReview: priorReview,
        messageClass: MESSAGE_CLASSES.CORRECTION,
      }
    );

    assert.equal(reply.intent, 'prospect_batch_review');
    assert.ok(reply.prospectBatchReview);
    assert.match(reply.message, /Prospect Batch Review/);
    assert.match(reply.message, /Existing relationship \/ nurture/i);
    assert.match(reply.message, /Keyrenter/);
    assert.doesNotMatch(
      reply.message,
      /Build proposal already approved/i
    );
    assert.doesNotMatch(
      reply.message,
      /Ask me to generate the first reviewable prospect list batch/i
    );
    assert.doesNotMatch(reply.message, /8 primary-town candidates/i);
    assert.ok(
      !reply.prospectBatchReview.acceptedFirstPass.some((r) =>
        /keyrenter/i.test(r.companyName)
      )
    );
    assert.equal(reply.prospectBatchReview.counts.accepted, 6);
    assert.equal(
      reply.prospectBatchReview.closingQuestion,
      buildProspectBatchReviewClosingQuestion(reply.prospectBatchReview)
    );
  });

  it('applyRelationshipOverridesToBatch moves Keyrenter out of accepted', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const overrides = parseRelationshipOverridesFromMessage(correctionMessage);
    const { batch: next, existingRelationship } =
      applyRelationshipOverridesToBatch(batch, overrides);
    assert.ok(
      !(next.groups.accepted || []).some((r) => /keyrenter/i.test(r.companyName))
    );
    assert.equal(existingRelationship.length, 1);
    assert.match(existingRelationship[0].companyName, /Keyrenter/i);
  });

  it('Keyrenter is the only existing_relationship record', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    assert.equal(review.existingRelationship.length, 1);
    assert.equal(review.counts.existingRelationship, 1);
    assert.match(
      review.existingRelationship[0].companyName,
      /Keyrenter New England Property Management/i
    );
    assert.ok(
      !review.existingRelationship.some((r) =>
        /^Property Management$/i.test(r.companyName)
      )
    );
    assert.ok(
      !review.existingRelationship.some((r) =>
        /realpropertynh\.com/i.test(r.sourceUrl || '')
      )
    );
  });

  it('realpropertynh.com remains optional expansion / review_required', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    const realProp = review.optionalExpansion.find((r) =>
      /realpropertynh\.com/i.test(String(r.sourceUrl || ''))
    );
    assert.ok(realProp, 'realpropertynh.com must stay in optional expansion');
    assert.equal(realProp.reviewStatus, 'review_required');
    assert.match(realProp.companyName, /^Property Management$/i);
    assert.ok(
      !review.existingRelationship.some((r) =>
        /realpropertynh\.com/i.test(String(r.sourceUrl || ''))
      )
    );
    assert.equal(review.counts.accepted, 6);
    assert.equal(review.counts.sourceVerificationRequired, 1);
    assert.equal(review.counts.existingRelationship, 1);
    assert.equal(review.counts.rejected, 1);
  });

  it('Existing relationship count is 1', () => {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
    });
    assert.equal(review.counts.existingRelationship, 1);
    assert.equal(review.existingRelationship.length, 1);
    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /Existing relationship \/ nurture:\s*1/);
  });

  it('Generic “Property Management” is never promoted through substring matching', () => {
    const override = parseRelationshipOverridesFromMessage(correctionMessage)[0];
    assert.ok(override);
    assert.equal(
      relationshipOverrideMatchesRow(
        {
          companyName: 'Property Management',
          sourceUrl: 'http://www.realpropertynh.com/',
          website: 'http://www.realpropertynh.com/',
          status: 'review_required',
        },
        override
      ),
      false
    );
    assert.equal(
      relationshipOverrideMatchesRow(
        {
          companyName: 'Keyrenter New England Property Management',
          sourceUrl: 'https://keyrenternewengland.com/auburn-property-management',
          website: 'https://keyrenternewengland.com',
          status: 'accepted',
        },
        override
      ),
      true
    );
    // Domain identity alone also matches Keyrenter.
    assert.equal(
      relationshipOverrideMatchesRow(
        {
          companyName: 'Keyrenter NE',
          sourceUrl: 'https://www.keyrenternewengland.com/',
          status: 'accepted',
        },
        override
      ),
      true
    );
    assert.equal(normalizeCompanyIdentity('Property Management'), '');
    assert.match(normalizeCompanyIdentity('Keyrenter New England Property Management'), /keyrenter/i);
  });
});
