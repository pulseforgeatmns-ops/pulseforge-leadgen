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
  looksLikeProspectBatchReviewApproval,
  looksLikeOutreachStrategyPreviewRequest,
  looksLikeOutreachStrategyPreviewApproval,
  looksLikeOutreachCopyPlanRequest,
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
  formatProspectBatchReviewEvidenceMessage,
  buildCampaignPlanningReply,
  buildProspectBatchReviewClosingQuestion,
  parseRelationshipOverridesFromMessage,
  applyRelationshipOverridesToBatch,
  relationshipOverrideMatchesRow,
  normalizeCompanyIdentity,
  approveProspectBatchReviewBatch1,
  approveOutreachStrategyPreview,
  buildOutreachStrategyPreview,
  buildOutreachCopyPlan,
  formatOutreachStrategyPreviewMessage,
  formatOutreachCopyPlanMessage,
  BATCH_1_APPROVED_MESSAGE,
  OUTREACH_STRATEGY_APPROVED_MESSAGE,
  RELATIONSHIP_STATUS,
  OUTREACH_STRATEGY_PREVIEW_TITLE,
  OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
  OUTREACH_COPY_PLAN_TITLE,
  OUTREACH_COPY_PLAN_CLOSING_QUESTION,
  containsRawPromptFragment,
  findRawPromptFragments,
  outreachStrategyPreviewLooksStale,
  findStaleOutreachStrategyFragments,
  repairOutreachStrategyPreview,
} = require('../services/clientIntelligenceCampaignPlanning');
const {
  splitDigestAndEvidence,
  EVIDENCE_COLLAPSED_NOTE,
} = require('../services/operatorReviewDigest');
const {
  renderOperatorReviewDigest,
  analyzeOperatorReviewHtml,
} = require('../public/shared/operator-review-digest');

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
    assert.ok(review.operatorDigest);

    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /## Recommended decision/);
    assert.match(msg, /Approve 2 cold prospects as Batch 1/);
    assert.match(msg, /## What is included/);
    assert.match(msg, /Elm Grove Companies/);
    assert.match(msg, /## Held back/);
    assert.match(msg, /Cedar Management Group — source verification required/);
    assert.match(msg, /## Why this is recommended/);
    assert.match(msg, /clean, net-new prospects in the approved priority towns/i);
    assert.match(msg, /## Next step after approval/);
    assert.match(msg, /Outreach Strategy Preview/);
    assert.match(msg, /View evidence \(collapsed by default\)/);
    // Full evidence dump is not the default operator view.
    assert.doesNotMatch(msg, /## 1\. Accepted cold first-pass candidates/);
    assert.doesNotMatch(msg, /Why it fits:/);
    assert.ok(msg.includes(review.closingQuestion));
    assert.doesNotMatch(msg, /8 primary-town candidates/i);
    assert.doesNotMatch(msg, /Ask me to generate the first reviewable/i);
    assert.doesNotMatch(msg, /Build proposal/i);

    const evidence = formatProspectBatchReviewEvidenceMessage(review);
    assert.match(evidence, /View evidence/);
    assert.match(evidence, /Cedar Management Group/);
    assert.match(evidence, /Source URL:/);
    assert.match(evidence, /fit rationale/i);
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
    assert.match(msg, /Keyrenter New England Property Management — existing relationship \/ nurture only/i);
    assert.match(msg, /Keyrenter/);
    const evidence = formatProspectBatchReviewEvidenceMessage(review);
    assert.match(evidence, /Existing relationship \/ nurture/i);
    assert.match(evidence, /do not include in campaign outreach/i);
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
    assert.match(msg, /Approve 6 cold prospects as Batch 1/);
    assert.match(msg, /## Held back/);
    assert.match(
      msg,
      /Keyrenter New England Property Management — existing relationship \/ nurture only/i
    );
    assert.match(msg, /Cedar Management Group — source verification required/i);
    assert.match(msg, /Optional Manchester candidates — not included yet/i);
    assert.match(msg, /Cushman\s*&\s*Wakefield — rejected as too institutional/i);
    assert.match(msg, /View evidence \(collapsed by default\)/);
    assert.doesNotMatch(msg, /## 1\. Accepted cold first-pass/i);
    assert.doesNotMatch(msg, /8 primary-town candidates/i);
    assert.equal(
      review.closingQuestion,
      'Do you want to approve the 6 accepted cold first-pass candidates, include Cedar after source verification, and keep Keyrenter as an existing-relationship nurture account?'
    );
    assert.ok(msg.includes(review.closingQuestion));
    assert.equal(review.operatorDigest.meta.acceptedCount, 6);
    assert.equal(review.operatorDigest.meta.sourceVerificationCount, 1);
    assert.equal(review.operatorDigest.meta.nurtureCount, 1);
    assert.equal(review.operatorDigest.meta.rejectedCount, 1);
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
    assert.match(
      reply.message,
      /Keyrenter New England Property Management — existing relationship \/ nurture only/i
    );
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
    assert.match(
      msg,
      /Keyrenter New England Property Management — existing relationship \/ nurture only/i
    );
    assert.equal(review.operatorDigest.meta.nurtureCount, 1);
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

describe('Prospect Batch Review — Batch 1 approval transition', () => {
  const correctionMessage =
    'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.';
  const approvalMessage =
    'Approve the 6 accepted cold first-pass candidates as Batch 1. Leave Cedar for source verification and Keyrenter as existing-relationship nurture.';

  function correctedReview() {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    return buildProspectBatchReview(batch, {
      userMessage: correctionMessage,
      workRequestId: batch.workRequestId,
    });
  }

  function approvedSessionState(review) {
    return {
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
      scoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      prospectBatchReview: review,
    };
  }

  it('approval of Prospect Batch Review advances state', () => {
    const review = correctedReview();
    assert.equal(review.counts.accepted, 6);

    assert.equal(
      looksLikeProspectBatchReviewApproval(approvalMessage, {
        priorProspectBatchReview: review,
        step: 'prospect_batch_review',
      }),
      true
    );
    assert.equal(
      classifyProspectAcquisitionIntent(approvalMessage, {
        priorProspectBatchReview: review,
        step: 'prospect_batch_review',
      }),
      PROSPECT_ACQUISITION_INTENTS.APPROVE_PROSPECT_BATCH_REVIEW
    );

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

    const action = resolveCampaignArtifactAction({
      userMessage: approvalMessage,
      messageClass: MESSAGE_CLASSES.APPROVAL,
      memory: mem,
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
      },
      priorBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      priorProspectBatchReview: review,
      priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      step: 'prospect_batch_review',
    });
    assert.equal(action.action, 'approve_prospect_batch_review');
    assert.equal(action.planningState, 'outreach_strategy_preview');
    assert.match(String(action.note || ''), /Batch 1 approved/i);
    assert.match(String(action.note || ''), /Outreach Strategy Preview/i);

    const reply = buildCampaignPlanningReply(
      approvalMessage,
      approvedSessionState(review),
      {
        businessName: 'Anchor Cleaning',
        brandVoice:
          'calm, professional, reliable, direct, and easy to work with',
        competitiveAdvantages:
          'Reliability and accountability. Responsive communication. Peace of mind for recurring cleaning relationships.',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        towns: ['Manchester', 'Bedford', 'Hooksett'],
      },
      {
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
        priorProspectBatchReview: review,
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
        },
        messageClass: MESSAGE_CLASSES.APPROVAL,
      }
    );

    assert.equal(reply.intent, 'prospect_batch_1_approved');
    assert.equal(reply.planningState, 'outreach_strategy_preview');
    assert.equal(reply.step, 'outreach_strategy_preview');
    assert.ok(reply.prospectBatchReview.batch1Approved);
    assert.equal(reply.prospectBatchReview.status, 'batch_1_approved');
    assert.match(reply.message, /Batch 1 approved/i);
    assert.match(reply.message, /Outreach Strategy Preview/);
    assert.ok(reply.message.includes(BATCH_1_APPROVED_MESSAGE));
    assert.doesNotMatch(
      reply.message,
      /Next step:\s*prepare outreach strategy preview/i
    );
    assert.doesNotMatch(
      reply.message,
      /ask me to (?:create|prepare|request).{0,40}outreach strategy/i
    );
    assert.match(
      reply.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.ok(reply.outreachStrategyPreview);
    assert.equal(reply.outreachStrategyPreview.kind, 'outreach_strategy_preview');
    assert.equal(reply.outreachStrategyPreview.status, 'draft');
    assert.ok(reply.outreachStrategyPreview.campaignObjective);
    assert.ok(
      (reply.outreachStrategyPreview.outreachApproach || []).length >= 2
    );
    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
  });

  it('approved batch excludes Cedar, Keyrenter, optional expansion, and rejected candidates', () => {
    const review = correctedReview();
    const approved = approveProspectBatchReviewBatch1(review);
    assert.equal(approved.approvedBatch.candidateCount, 6);
    assert.equal(approved.approvedBatch.candidates.length, 6);
    assert.ok(
      approved.approvedBatch.candidates.every((c) => c.approvedInBatch1)
    );
    assert.ok(
      !approved.approvedBatch.candidates.some((c) =>
        /cedar|keyrenter|cushman/i.test(c.companyName)
      )
    );
    assert.ok(
      !approved.approvedBatch.candidates.some((c) =>
        /^Property Management$/i.test(c.companyName)
      )
    );
    assert.ok(
      approved.approvedBatch.excludedSourceVerification.some((n) =>
        /Cedar/i.test(n)
      )
    );
    assert.ok(
      approved.approvedBatch.excludedExistingRelationship.some((n) =>
        /Keyrenter/i.test(n)
      )
    );
    assert.ok(
      (approved.approvedBatch.excludedOptionalExpansion || []).some((n) =>
        /Property Management/i.test(n)
      ) ||
        (approved.optionalExpansion || []).some((r) =>
          /realpropertynh\.com/i.test(r.sourceUrl || '')
        )
    );
    assert.ok(
      approved.approvedBatch.excludedRejected.some((n) => /Cushman/i.test(n))
    );

    const reply = buildCampaignPlanningReply(
      approvalMessage,
      approvedSessionState(review),
      { businessName: 'Anchor Cleaning' },
      {
        priorProspectBatchReview: review,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );
    const batch = reply.prospectBatchReview.approvedBatch;
    assert.equal(batch.candidateCount, 6);
    assert.ok(batch.excludedSourceVerification.some((n) => /Cedar/i.test(n)));
    assert.ok(
      batch.excludedExistingRelationship.some((n) => /Keyrenter/i.test(n))
    );
    assert.match(reply.message, /Cedar remains source-verification/i);
    assert.match(reply.message, /Keyrenter remains existing-relationship/i);
    assert.match(reply.message, /Optional expansion candidates remain excluded/i);
  });

  it('repeated approval does not duplicate or re-render the review artifact', () => {
    const review = correctedReview();
    const first = buildCampaignPlanningReply(
      approvalMessage,
      approvedSessionState(review),
      { businessName: 'Anchor Cleaning' },
      {
        priorProspectBatchReview: review,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );
    assert.equal(first.intent, 'prospect_batch_1_approved');
    assert.doesNotMatch(first.message, /## 1\. Accepted cold first-pass/i);
    assert.doesNotMatch(
      first.message,
      /Do you want to approve the 6 accepted cold first-pass candidates/i
    );

    const second = buildCampaignPlanningReply(
      approvalMessage,
      {
        ...approvedSessionState(first.prospectBatchReview),
        step: 'outreach_strategy_preview',
        outreachStrategyPreview: first.outreachStrategyPreview,
      },
      { businessName: 'Anchor Cleaning' },
      {
        priorProspectBatchReview: first.prospectBatchReview,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );
    assert.equal(second.intent, 'prospect_batch_1_approved');
    assert.equal(second.planningState, 'outreach_strategy_preview');
    assert.match(second.message, /Batch 1 approved/i);
    assert.doesNotMatch(second.message, /## 1\. Accepted cold first-pass/i);
    assert.doesNotMatch(
      second.message,
      /Do you want to approve the 6 accepted cold first-pass candidates/i
    );
    assert.equal(
      second.prospectBatchReview.approvedBatch.candidateCount,
      6
    );
  });

  it('next state is outreach strategy preview / copy planning, not live send', () => {
    const review = correctedReview();
    const reply = buildCampaignPlanningReply(
      approvalMessage,
      approvedSessionState(review),
      { businessName: 'Anchor Cleaning' },
      {
        priorProspectBatchReview: review,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );
    assert.equal(reply.planningState, 'outreach_strategy_preview');
    assert.equal(reply.outreachStrategyPreview.status, 'draft');
    assert.equal(reply.outreachStrategyPreview.reviewFirst, true);
    assert.equal(reply.outreachStrategyPreview.planningOnly, true);
    assert.equal(reply.outreachStrategyPreview.outreachCopyGenerated, false);
    assert.equal(reply.outreachStrategyPreview.sendsMade, false);
    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
    assert.match(
      reply.outreachStrategyPreview.summary,
      /copy planning only|not live send/i
    );
    assert.doesNotMatch(
      reply.message,
      /campaign is live|I (?:am )?sending|launching outreach now/i
    );
  });

  it('create Outreach Strategy Preview after Batch 1 creates or shows strategy, not a request prompt', () => {
    const review = correctedReview();
    const approved = approveProspectBatchReviewBatch1(review);
    const createMsg = 'Create the Outreach Strategy Preview';

    assert.equal(looksLikeOutreachStrategyPreviewRequest(createMsg), true);
    assert.equal(
      classifyProspectAcquisitionIntent(createMsg, {
        priorProspectBatchReview: approved,
        step: 'outreach_strategy_preview',
      }),
      PROSPECT_ACQUISITION_INTENTS.EMIT_OUTREACH_STRATEGY_PREVIEW
    );

    const created = buildCampaignPlanningReply(
      createMsg,
      {
        ...approvedSessionState(approved),
        step: 'outreach_strategy_preview',
      },
      {
        businessName: 'Anchor Cleaning',
        brandVoice: 'calm, professional, reliable',
        competitiveAdvantages:
          'Reliability and accountability for property managers.',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
      },
      {
        priorProspectBatchReview: approved,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove property managers will book a walkthrough conversation.',
        },
      }
    );

    assert.equal(created.step, 'outreach_strategy_preview');
    assert.equal(created.planningState, 'outreach_strategy_preview');
    assert.match(
      created.intent,
      /outreach_strategy_preview|prospect_batch_1_approved/
    );
    assert.ok(created.outreachStrategyPreview);
    assert.ok(created.outreachStrategyPreview.campaignObjective);
    assert.match(created.message, /Outreach Strategy Preview/);
    assert.match(
      created.message,
      /Does this Outreach Strategy Preview look right to approve|revise a specific section/i
    );
    assert.doesNotMatch(
      created.message,
      /Next step:\s*prepare outreach strategy preview/i
    );
    assert.doesNotMatch(
      created.message,
      /ask me to (?:create|prepare|request).{0,40}outreach strategy/i
    );
    assert.doesNotMatch(created.message, /## 1\. Accepted cold first-pass/i);
    assert.doesNotMatch(
      created.message,
      /Do you want to approve the 6 accepted cold first-pass candidates/i
    );
    assert.equal(created.outreachCopyGenerated, false);
    assert.equal(created.sendsMade, false);
    assert.equal(created.crmWritesMade, false);
    assert.equal(created.exportMade, false);
    assert.equal(created.accountChangesMade, false);

    const second = buildCampaignPlanningReply(
      createMsg,
      {
        ...approvedSessionState(approved),
        step: 'outreach_strategy_preview',
        outreachStrategyPreview: created.outreachStrategyPreview,
      },
      { businessName: 'Anchor Cleaning' },
      {
        priorProspectBatchReview: approved,
        priorOutreachStrategyPreview: created.outreachStrategyPreview,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );
    assert.equal(second.intent, 'show_outreach_strategy_preview');
    assert.equal(second.planningState, 'outreach_strategy_preview');
    assert.match(second.message, /already available|for approval or revision/i);
    assert.match(
      second.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.doesNotMatch(
      second.message,
      /Creating the Outreach Strategy Preview from the approved Blueprint/i
    );
    assert.doesNotMatch(second.message, /## 1\. Accepted cold first-pass/i);
    assert.equal(
      second.outreachStrategyPreview.campaignObjective,
      created.outreachStrategyPreview.campaignObjective
    );
  });

  it('Outreach Strategy Preview carries Blueprint voice, objective, and Batch 1 prospects', () => {
    const review = correctedReview();
    const approved = approveProspectBatchReviewBatch1(review);
    const strategy = buildOutreachStrategyPreview(
      approved,
      {
        businessName: 'Anchor Cleaning',
        brandVoice:
          "Brand voice should read as calm, professional, reliable, and easy to work with. Tone guidance constrains later language without choosing channels or campaigns.",
        competitiveAdvantages:
          'Customers choose this business for reliability and accountability. Responsive communication. Peace of mind for recurring relationships.',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        towns: ['Manchester', 'Bedford'],
      },
      {
        priorCriteriaPreview: {
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation.',
        },
      }
    );
    assert.equal(strategy.kind, 'outreach_strategy_preview');
    assert.equal(strategy.approvedCandidateCount, 6);
    assert.match(strategy.campaignObjective, /discovery conversation/i);
    assert.match(strategy.voiceTone, /calm|professional|reliable/i);
    assert.ok(strategy.differentiators.length >= 1);
    assert.ok(strategy.batchProspects.some((n) => /Elm Grove/i.test(n)));
    assert.ok(!strategy.batchProspects.some((n) => /Keyrenter|Cedar/i.test(n)));
    assert.equal(strategy.outreachCopyGenerated, false);

    const msg = formatOutreachStrategyPreviewMessage(strategy);
    assert.match(msg, /Outreach Strategy Preview/);
    assert.match(msg, /Batch 1 scope/i);
    assert.match(msg, /Voice & tone/i);
    assert.match(msg, new RegExp(OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION));
    assert.doesNotMatch(msg, /Subject:|Hi \{|Dear /);
  });

  it('Outreach Strategy Preview section 5 uses phrase-safe synthesis, not raw criteria stitching', () => {
    const review = correctedReview();
    const approved = approveProspectBatchReviewBatch1(review);
    const rawCriteria = {
      targetSegment:
        'Small to mid-sized local property managers in Greater Manchester who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces.',
      targetSubtype:
        'property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces',
      marketBound:
        'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope, but keep the first test tight enough to learn quickly.',
      campaignObjective:
        'Prove that Greater Manchester property managers will take a discovery conversation.',
      status: 'approved',
    };
    const strategy = buildOutreachStrategyPreview(
      approved,
      {
        businessName: 'Anchor Cleaning',
        brandVoice:
          'Brand voice should read as calm, professional, reliable, and easy to work with.',
        competitiveAdvantages:
          "Customers choose this business for reliability and responsiveness. Responsive communication. Peace of mind for recurring relationships.",
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
      },
      { priorCriteriaPreview: rawCriteria }
    );

    assert.equal(
      strategy.outreachAudiencePhrase,
      'small to mid-sized property managers'
    );
    assert.equal(
      strategy.outreachMarketPhrase,
      'Bedford, Hooksett, Londonderry, Auburn, and Goffstown'
    );
    assert.equal(strategy.outreachAnglePhrase, 'reliability and responsiveness');
    assert.match(strategy.outreachCtaPhrase, /conversation or walkthrough/i);
    assert.match(strategy.approvedBatchPhrase, /approved Batch 1 record/i);
    assert.ok(strategy.synthesisPhrases);
    assert.equal(
      strategy.synthesisPhrases.outreachAudiencePhrase,
      strategy.outreachAudiencePhrase
    );

    const lead = strategy.outreachApproach[0];
    assert.equal(
      lead,
      "Lead with Anchor's reliability and responsiveness for small to mid-sized property managers in Bedford, Hooksett, Londonderry, Auburn, and Goffstown."
    );
    assert.match(
      strategy.outreachApproach[1],
      /Personalize by town, property type, and any public role signal from the approved Batch 1 record/i
    );
    assert.match(
      strategy.outreachApproach[2],
      /Keep the first ask simple: a short conversation or walkthrough/i
    );
    assert.match(
      strategy.outreachApproach[3],
      /validation campaign, not a broad launch/i
    );

    const msg = formatOutreachStrategyPreviewMessage(strategy);
    const hits = findRawPromptFragments(msg);
    assert.equal(
      hits.length,
      0,
      `Outreach Strategy Preview leaked raw fragments: ${hits.join(', ')}\n---\n${msg}`
    );
    assert.equal(containsRawPromptFragment(msg), false);
    assert.doesNotMatch(
      msg,
      /Small to mid-sized local property managers in Greater Manchester who oversee/i
    );
    assert.doesNotMatch(msg, /Start with Bedford/i);
    assert.doesNotMatch(msg, /Keep Greater Manchester in scope/i);
    assert.doesNotMatch(msg, /keep the first test tight enough to learn quickly/i);
    assert.doesNotMatch(msg, /differentiators for /i);
    assert.doesNotMatch(msg, /(?<!\.)\.\.(?!\.)/);
    assert.doesNotMatch(msg, /Subject:|Hi \{|Dear /);
    assert.equal(strategy.outreachCopyGenerated, false);
    assert.equal(strategy.sendsMade, false);
    assert.equal(strategy.crmWritesMade, false);
    assert.equal(strategy.exportMade, false);
    assert.equal(strategy.accountChangesMade, false);
  });

  it('repairs stale stored Outreach Strategy Preview before display', () => {
    const review = correctedReview();
    const approved = approveProspectBatchReviewBatch1(review);
    const rawCriteria = {
      targetSegment:
        'Small to mid-sized local property managers in Greater Manchester who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces.',
      marketBound:
        'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope, but keep the first test tight enough to learn quickly.',
      campaignObjective:
        'Prove that Greater Manchester property managers will take a discovery conversation.',
      status: 'approved',
    };
    const anchorCtx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'Brand voice should read as calm, professional, reliable, and easy to work with.',
      competitiveAdvantages:
        'Customers choose this business for reliability and responsiveness.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
    };

    const stale = {
      kind: 'outreach_strategy_preview',
      title: OUTREACH_STRATEGY_PREVIEW_TITLE,
      status: 'draft',
      campaignObjective: rawCriteria.campaignObjective,
      batch1Scope: '6 approved cold first-pass prospects in Batch 1.',
      voiceTone: 'Calm, professional, reliable.',
      differentiators: ['Reliability and accountability'],
      outreachApproach: [
        "Lead with Anchor's reliability and responsiveness differentiators for Small to mid-sized local property managers in Greater Manchester who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces. in Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope, but keep the first test tight enough to learn quickly..",
      ],
      proofFraming: ['Hold final scripts until after strategy approval.'],
      guardrails: [
        'No final outreach copy in this step',
        'No sends',
        'No CRM writes',
        'No export',
        'No account, DNS, GBP, social, or tracking changes',
      ],
      generatedAt: '2026-08-11T04:00:00.000Z',
      workRequestId: approved.workRequestId || 'wr-stale-osp',
    };

    assert.equal(outreachStrategyPreviewLooksStale(stale), true);
    assert.ok(findStaleOutreachStrategyFragments(stale).length >= 1);

    const repairedDirect = repairOutreachStrategyPreview(
      stale,
      approved,
      anchorCtx,
      { priorCriteriaPreview: rawCriteria }
    );
    assert.equal(repairedDirect.repairedFromStale, true);
    assert.equal(outreachStrategyPreviewLooksStale(repairedDirect), false);
    assert.equal(
      repairedDirect.outreachApproach[0],
      "Lead with Anchor's reliability and responsiveness for small to mid-sized property managers in Bedford, Hooksett, Londonderry, Auburn, and Goffstown."
    );

    const reply = buildCampaignPlanningReply(
      'Show the Outreach Strategy Preview',
      {
        ...approvedSessionState(approved),
        step: 'outreach_strategy_preview',
        outreachStrategyPreview: stale,
      },
      anchorCtx,
      {
        priorProspectBatchReview: approved,
        priorOutreachStrategyPreview: stale,
        priorCriteriaPreview: rawCriteria,
        priorScoutCandidateBatch: withGroups(sampleKeyrenterCorrectionBatch()),
      }
    );

    assert.equal(reply.intent, 'repair_outreach_strategy_preview');
    assert.equal(reply.planningState, 'outreach_strategy_preview');
    assert.equal(reply.batch1Approved, true);
    assert.equal(reply.prospectBatchReviewApproved, true);
    assert.ok(reply.outreachStrategyPreview);
    assert.equal(reply.outreachStrategyPreview.repairedFromStale, true);
    assert.equal(outreachStrategyPreviewLooksStale(reply.outreachStrategyPreview), false);

    assert.equal(
      reply.outreachStrategyPreview.outreachApproach[0],
      "Lead with Anchor's reliability and responsiveness for small to mid-sized property managers in Bedford, Hooksett, Londonderry, Auburn, and Goffstown."
    );
    assert.match(
      reply.outreachStrategyPreview.outreachApproach[1],
      /Personalize by town, property type, and any public role signal from the approved Batch 1 record/i
    );
    assert.match(
      reply.outreachStrategyPreview.outreachApproach[2],
      /Keep the first ask simple: a short conversation or walkthrough/i
    );
    assert.match(
      reply.outreachStrategyPreview.outreachApproach[3],
      /validation campaign, not a broad launch/i
    );

    assert.match(reply.message, /repaired stale|for approval or revision/i);
    assert.match(
      reply.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.doesNotMatch(
      reply.message,
      /ask me to (?:create|prepare|request).{0,40}outreach strategy/i
    );
    assert.doesNotMatch(reply.message, /## 1\. Accepted cold first-pass/i);
    assert.doesNotMatch(
      reply.message,
      /Do you want to approve the 6 accepted cold first-pass candidates/i
    );

    const hits = findRawPromptFragments(reply.message);
    assert.equal(
      hits.length,
      0,
      `Repaired preview still has banned fragments: ${hits.join(', ')}\n---\n${reply.message}`
    );
    assert.doesNotMatch(reply.message, /for Small to mid-sized/);
    assert.doesNotMatch(reply.message, /in Start with/);
    assert.doesNotMatch(reply.message, /Keep Greater Manchester in scope/i);
    assert.doesNotMatch(reply.message, /(?<!\.)\.\.(?!\.)/);
    assert.doesNotMatch(reply.message, /differentiators for /i);

    // Workflow state preserved: Cedar / Keyrenter / optional expansion / Batch 1.
    assert.equal(reply.prospectBatchReview.batch1Approved, true);
    assert.equal(reply.prospectBatchReview.status, 'batch_1_approved');
    assert.ok(
      (reply.prospectBatchReview.sourceVerificationRequired || []).some((r) =>
        /cedar/i.test(String(r.companyName || ''))
      )
    );
    assert.ok(
      (reply.prospectBatchReview.existingRelationship || []).some((r) =>
        /keyrenter/i.test(String(r.companyName || ''))
      )
    );
    assert.ok((reply.prospectBatchReview.optionalExpansion || []).length >= 1);
    assert.ok(
      !(reply.prospectBatchReview.approvedBatch.candidates || []).some((c) =>
        /cedar|keyrenter/i.test(String(c.companyName || ''))
      )
    );

    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
  });
});

/** Fixture matching Operator Review Digest acceptance (Batch 1 names). */
function sampleOperatorDigestBatch1() {
  const cold = [
    {
      companyName: 'Real Property Management Thrive',
      location: 'Bedford NH',
      sourceUrl: 'https://rpmthrive.example',
      website: 'https://rpmthrive.example',
      fitRationale: 'Bedford NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
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
      companyName: 'Real Property Management Premier Network',
      location: 'Londonderry NH',
      sourceUrl: 'https://rpmpremier.example',
      website: 'https://rpmpremier.example',
      fitRationale: 'Londonderry NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Northcity Property Management',
      location: 'Auburn NH',
      sourceUrl: 'https://northcity.example',
      website: 'https://northcity.example',
      fitRationale: 'Auburn NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'The MEG Companies',
      location: 'Goffstown NH',
      sourceUrl: 'https://meg.example',
      website: 'https://meg.example',
      fitRationale: 'Goffstown NH property management',
      risks: 'Public-source only',
      suggestedContactRole: 'Owner / property manager',
      confidence: 'high',
      status: 'accepted',
      statusReason: 'Passes NH property-manager quality gates',
    },
    {
      companyName: 'Avise Properties',
      location: 'Bedford NH',
      sourceUrl: 'https://avise.example',
      website: 'https://avise.example',
      fitRationale: 'Bedford NH property management',
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
    candidates: cold.concat(reviewRequired),
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
      accepted: cold,
      review_required: reviewRequired,
      rejected: null,
    },
  };
}

describe('Operator Review Digest — Prospect Batch Review acceptance', () => {
  const correctionMessage =
    'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.';
  const approvalMessage =
    'Approve the 6 accepted cold first-pass candidates as Batch 1. Leave Cedar for source verification and Keyrenter as existing-relationship nurture.';

  function digestReview() {
    return buildProspectBatchReview(withGroups(sampleOperatorDigestBatch1()), {
      userMessage: correctionMessage,
      workRequestId: 'f0ac74ac-16a6-4dba-b024-d3727b285a86',
    });
  }

  it('digest uses correct counts and sections for Batch 1', () => {
    const review = digestReview();
    const digest = review.operatorDigest;
    assert.equal(digest.recommendedDecision, 'Approve 6 cold prospects as Batch 1.');
    assert.deepEqual(digest.included, [
      'Real Property Management Thrive',
      'Elm Grove Companies',
      'Real Property Management Premier Network',
      'Northcity Property Management',
      'The MEG Companies',
      'Avise Properties',
    ]);
    assert.equal(digest.sectionTitles.excluded, 'Held back');
    assert.deepEqual(digest.heldBack, digest.excluded);
    assert.ok(
      digest.heldBack.some((line) =>
        /Cedar Management Group — source verification required/i.test(line)
      )
    );
    assert.ok(
      digest.heldBack.some((line) =>
        /Keyrenter New England Property Management — existing relationship \/ nurture only/i.test(
          line
        )
      )
    );
    assert.ok(
      digest.heldBack.some((line) =>
        /Optional Manchester candidates — not included yet/i.test(line)
      )
    );
    assert.ok(
      digest.heldBack.some((line) =>
        /Cushman\s*&\s*Wakefield — rejected as too institutional/i.test(line)
      )
    );
    assert.match(
      digest.whyRecommended.join(' '),
      /clean, net-new prospects in the approved priority towns/i
    );
    assert.equal(digest.nextStepAfterApproval, OUTREACH_STRATEGY_PREVIEW_TITLE);
    assert.equal(digest.nextStepAfterApproval, 'Outreach Strategy Preview');
    assert.ok(digest.primaryActions.some((a) => a.id === 'approve_batch_1'));
    assert.equal(digest.evidence.collapsedByDefault, true);
  });

  it('Held back section appears in digest before evidence', () => {
    const review = digestReview();
    const msg = formatProspectBatchReviewMessage(review);
    assert.match(msg, /## Held back/);
    assert.match(
      msg,
      /Cedar Management Group — source verification required/
    );
    assert.match(
      msg,
      /Keyrenter New England Property Management — existing relationship \/ nurture only/
    );
    assert.match(msg, /Optional Manchester candidates — not included yet/);
    assert.match(
      msg,
      /Cushman\s*&\s*Wakefield — rejected as too institutional/
    );
    // Held back comes after included and before why.
    const includedIdx = msg.indexOf('## What is included');
    const heldIdx = msg.indexOf('## Held back');
    const whyIdx = msg.indexOf('## Why this is recommended');
    assert.ok(includedIdx >= 0 && heldIdx > includedIdx);
    assert.ok(whyIdx > heldIdx);
    assert.ok(msg.indexOf('View evidence') > heldIdx || msg.includes(EVIDENCE_COLLAPSED_NOTE));
    assert.doesNotMatch(msg, /Source URL:/);

    const html = renderOperatorReviewDigest(review.operatorDigest, {
      elementId: 'prospectBatchReview',
      evidenceOpen: false,
    });
    const analysis = analyzeOperatorReviewHtml(html);
    assert.equal(analysis.hasHeldBackSection, true);
    assert.equal(analysis.digestBeforeEvidence, true);
    assert.match(html, /Cedar Management Group — source verification required/);
    assert.match(
      html,
      /Keyrenter New England Property Management — existing relationship \/ nurture only/
    );
  });

  it('digest renders before evidence and evidence is collapsed by default', () => {
    const review = digestReview();
    const msg = formatProspectBatchReviewMessage(review);
    const split = splitDigestAndEvidence(msg);
    assert.match(split.digest, /## Recommended decision/);
    assert.match(split.digest, /Approve 6 cold prospects as Batch 1/);
    assert.match(split.digest, /## Held back/);
    assert.ok(split.digest.includes(EVIDENCE_COLLAPSED_NOTE));
    assert.equal(split.evidence, '');
    assert.equal(split.evidenceCollapsed, true);
    // Default message must not dump full sourced records.
    assert.doesNotMatch(msg, /Source URL:/);
    assert.doesNotMatch(msg, /Why it fits:/);
    assert.doesNotMatch(msg, /## 1\. Accepted cold first-pass/);

    const evidence = formatProspectBatchReviewEvidenceMessage(review);
    assert.match(evidence, /View evidence/);
    assert.match(evidence, /Real Property Management Thrive/);
    assert.match(evidence, /Source URL:/);
    assert.match(evidence, /fit rationale/i);
    assert.match(evidence, /Confidence:/);
    assert.match(evidence, /Cedar Management Group/);
    assert.match(evidence, /Cushman\s*&\s*Wakefield/);

    const html = renderOperatorReviewDigest(review.operatorDigest, {
      elementId: 'prospectBatchReview',
      evidenceOpen: false,
    });
    const analysis = analyzeOperatorReviewHtml(html);
    assert.equal(analysis.hasDigest, true);
    assert.equal(analysis.hasEvidenceDrawer, true);
    assert.equal(analysis.hasPrimaryActions, true);
    assert.equal(analysis.hasHeldBackSection, true);
    assert.equal(analysis.digestBeforeEvidence, true);
    assert.equal(analysis.actionsBeforeEvidence, true);
    assert.equal(analysis.evidenceCollapsedByDefault, true);
  });

  it('approval buttons are visible without scrolling through full evidence', () => {
    const review = digestReview();
    const html = renderOperatorReviewDigest(
      {
        ...review.operatorDigest,
        closingQuestion: review.closingQuestion,
      },
      { elementId: 'prospectBatchReview', evidenceOpen: false }
    );
    const analysis = analyzeOperatorReviewHtml(html);
    assert.ok(analysis.actionsIndex >= 0);
    assert.ok(analysis.evidenceIndex >= 0);
    assert.ok(
      analysis.actionsIndex < analysis.evidenceIndex,
      'primary actions must appear before the evidence drawer'
    );
    assert.match(html, /data-ord-action="approve_batch_1"/);
    assert.match(html, /Approve Batch 1/);
    // Evidence body exists but drawer is not open by default.
    assert.match(html, /data-role="view-evidence"/);
    assert.doesNotMatch(
      html,
      /data-role="view-evidence"[^>]*\sopen\b/i
    );
  });

  it('approval advances to next review-first step and does not re-ask Batch 1', () => {
    const review = digestReview();
    const reply = buildCampaignPlanningReply(
      approvalMessage,
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
        scoutCandidateBatch: withGroups(sampleOperatorDigestBatch1()),
        prospectBatchReview: review,
      },
      { businessName: 'Anchor Cleaning' },
      {
        priorScoutCandidateBatch: withGroups(sampleOperatorDigestBatch1()),
        priorProspectBatchReview: review,
        messageClass: MESSAGE_CLASSES.APPROVAL,
      }
    );

    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
  });
});

describe('Outreach Strategy Preview → Outreach Copy Plan approval transition', () => {
  const strategyApprovalMessage =
    'Approve the Outreach Strategy Preview. Next step: create the Outreach Copy Plan.';

  function strategySessionFixtures() {
    const review = (() => {
      const batch = withGroups(sampleKeyrenterCorrectionBatch());
      const built = buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      });
      return approveProspectBatchReviewBatch1(built);
    })();
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: ['Manchester', 'Bedford', 'Hooksett'],
    };
    const strategy = buildOutreachStrategyPreview(review, ctx, {
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
        campaignObjective:
          'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
      },
    });
    const sessionState = {
      step: 'outreach_strategy_preview',
      answers: {},
      slots: {
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
        buildProposalGenerated: true,
        buildProposalApproved: true,
        prospectBatchReviewApproved: true,
        batch1Approved: true,
        outreachStrategyPreviewGenerated: true,
      },
      prospectListCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
        campaignObjective:
          'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
      },
      prospectListBuildProposal: {
        kind: 'prospect_list_build_proposal',
        status: 'approved',
      },
      prospectBatchReview: review,
      outreachStrategyPreview: strategy,
    };
    return { review, ctx, strategy, sessionState };
  }

  it('detects approval intent for the active Outreach Strategy Preview', () => {
    const { strategy, review } = strategySessionFixtures();
    assert.equal(
      looksLikeOutreachStrategyPreviewApproval(strategyApprovalMessage, {
        priorOutreachStrategyPreview: strategy,
        priorProspectBatchReview: review,
        step: 'outreach_strategy_preview',
      }),
      true
    );
    assert.equal(
      classifyProspectAcquisitionIntent(strategyApprovalMessage, {
        priorOutreachStrategyPreview: strategy,
        priorProspectBatchReview: review,
        step: 'outreach_strategy_preview',
      }),
      PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_STRATEGY_PREVIEW
    );
    assert.equal(
      looksLikeOutreachStrategyPreviewRequest(strategyApprovalMessage),
      false
    );
  });

  it('approval of Outreach Strategy Preview advances to Outreach Copy Plan', () => {
    const { review, ctx, strategy, sessionState } = strategySessionFixtures();

    const action = resolveCampaignArtifactAction({
      userMessage: strategyApprovalMessage,
      messageClass: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
      memory: (() => {
        let mem = emptyReasoningMemory();
        mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW);
        mem = markArtifactGenerated(
          mem,
          ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
          'draft'
        );
        return mem;
      })(),
      priorProspectBatchReview: review,
      priorOutreachStrategyPreview: strategy,
      step: 'outreach_strategy_preview',
    });
    assert.equal(action.action, 'approve_outreach_strategy_preview');
    assert.equal(action.emitKind, ARTIFACT_KINDS.OUTREACH_COPY_PLAN);
    assert.equal(action.planningState, 'outreach_copy_plan');

    const reply = buildCampaignPlanningReply(
      strategyApprovalMessage,
      sessionState,
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        messageClass: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
      }
    );

    assert.match(
      reply.intent,
      /outreach_strategy_preview_approved|produce_outreach_copy_plan|show_outreach_copy_plan/
    );
    assert.equal(reply.planningState, 'outreach_copy_plan');
    assert.equal(reply.step, 'outreach_copy_plan');
    assert.equal(reply.outreachStrategyPreview.status, 'approved');
    assert.ok(reply.outreachCopyPlan);
    assert.equal(reply.outreachCopyPlan.kind, 'outreach_copy_plan');
    assert.equal(reply.outreachCopyPlan.status, 'draft');
    assert.match(reply.message, /Outreach Copy Plan/);
    assert.match(
      reply.message,
      /Does this Outreach Copy Plan look right to approve/
    );
    assert.doesNotMatch(
      reply.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.ok((reply.outreachCopyPlan.channelSequence || []).length >= 1);
    assert.ok(reply.outreachCopyPlan.firstTouchGoal);
    assert.ok(reply.outreachCopyPlan.ctaToTest);
    assert.ok((reply.outreachCopyPlan.personalizationInputs || []).length >= 1);
    assert.ok((reply.outreachCopyPlan.proofPoints || []).length >= 1);
    assert.ok((reply.outreachCopyPlan.followUpTiming || []).length >= 1);
    assert.ok((reply.outreachCopyPlan.approvalGate || []).length >= 1);
    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.finalOutreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
  });

  it('repeating approval does not re-render the strategy preview', () => {
    const { review, ctx, strategy, sessionState } = strategySessionFixtures();
    const first = buildCampaignPlanningReply(
      strategyApprovalMessage,
      sessionState,
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
      }
    );
    assert.equal(first.outreachStrategyPreview.status, 'approved');
    assert.ok(first.outreachCopyPlan);

    const second = buildCampaignPlanningReply(
      strategyApprovalMessage,
      {
        ...sessionState,
        step: 'outreach_copy_plan',
        outreachStrategyPreview: first.outreachStrategyPreview,
        outreachCopyPlan: first.outreachCopyPlan,
        slots: {
          ...sessionState.slots,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: first.outreachStrategyPreview,
        priorOutreachCopyPlan: first.outreachCopyPlan,
      }
    );

    assert.equal(second.planningState, 'outreach_copy_plan');
    assert.ok(second.outreachCopyPlan);
    assert.match(second.message, /Outreach Copy Plan/);
    assert.doesNotMatch(
      second.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.match(
      second.message,
      /Does this Outreach Copy Plan look right to approve|already available/i
    );
  });

  it('creates Outreach Copy Plan when missing and shows existing when present', () => {
    const { review, ctx, strategy, sessionState } = strategySessionFixtures();

    const created = buildCampaignPlanningReply(
      'Create the Outreach Copy Plan',
      {
        ...sessionState,
        outreachStrategyPreview: approveOutreachStrategyPreview(strategy),
        slots: {
          ...sessionState.slots,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: approveOutreachStrategyPreview(strategy),
      }
    );
    assert.ok(created.outreachCopyPlan);
    assert.equal(created.intent, 'produce_outreach_copy_plan');
    assert.equal(created.outreachCopyPlan.status, 'draft');

    const shown = buildCampaignPlanningReply(
      'Show the Outreach Copy Plan',
      {
        ...sessionState,
        step: 'outreach_copy_plan',
        outreachStrategyPreview: created.outreachStrategyPreview,
        outreachCopyPlan: created.outreachCopyPlan,
        slots: {
          ...sessionState.slots,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: created.outreachStrategyPreview,
        priorOutreachCopyPlan: created.outreachCopyPlan,
      }
    );
    assert.equal(shown.intent, 'show_outreach_copy_plan');
    assert.equal(
      shown.outreachCopyPlan.campaignObjective,
      created.outreachCopyPlan.campaignObjective
    );
    assert.match(
      shown.message,
      /Does this Outreach Copy Plan look right to approve/
    );
  });

  it('Outreach Copy Plan asks for copy-plan approval, not strategy approval again', () => {
    const { review, ctx, strategy } = strategySessionFixtures();
    const plan = buildOutreachCopyPlan(
      approveOutreachStrategyPreview(strategy),
      review,
      ctx,
      {}
    );
    const msg = formatOutreachCopyPlanMessage(plan);
    assert.match(msg, new RegExp(OUTREACH_COPY_PLAN_TITLE));
    assert.match(msg, /Recommended channel sequence/i);
    assert.match(msg, /First-touch message goal/i);
    assert.match(msg, /CTA to test/i);
    assert.match(msg, /Personalization inputs from Batch 1/i);
    assert.match(msg, /Proof points to use/i);
    assert.match(msg, /Follow-up timing and purpose/i);
    assert.match(msg, /Approval gate before drafting final copy/i);
    assert.match(msg, new RegExp(OUTREACH_COPY_PLAN_CLOSING_QUESTION));
    assert.doesNotMatch(
      msg,
      new RegExp(OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION)
    );
    assert.equal(plan.finalOutreachCopyGenerated, false);
    assert.equal(plan.outreachCopyGenerated, false);
    assert.equal(plan.sendsMade, false);
    assert.equal(plan.crmWritesMade, false);
    assert.equal(plan.exportMade, false);
    assert.equal(plan.accountChangesMade, false);
    assert.match(OUTREACH_STRATEGY_APPROVED_MESSAGE, /Outreach Copy Plan/);
  });
});
