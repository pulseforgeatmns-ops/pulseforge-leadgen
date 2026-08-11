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
  REVIEW_ARTIFACT_CHAIN,
  resolveCampaignArtifactAction,
  looksLikeProspectBatchReviewRequest,
  looksLikeProspectBatchReviewCorrection,
  looksLikeProspectBatchReviewApproval,
  looksLikeOutreachStrategyPreviewRequest,
  looksLikeOutreachStrategyPreviewApproval,
  looksLikeOutreachCopyPlanRequest,
  looksLikeOutreachCopyPlanApproval,
  looksLikeOutreachDraftPreviewRequest,
  looksLikeOutreachDraftPreviewApproval,
  looksLikeOutreachLaunchGateApproval,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  emptyReasoningMemory,
  markArtifactApproved,
  markArtifactGenerated,
  nextReviewArtifactKind,
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
  approveOutreachCopyPlan,
  approveOutreachDraftPreview,
  buildOutreachStrategyPreview,
  buildOutreachCopyPlan,
  buildOutreachDraftPreview,
  buildOutreachLaunchGate,
  formatOutreachStrategyPreviewMessage,
  formatOutreachCopyPlanMessage,
  formatOutreachDraftPreviewMessage,
  formatOutreachLaunchGateMessage,
  BATCH_1_APPROVED_MESSAGE,
  OUTREACH_STRATEGY_APPROVED_MESSAGE,
  OUTREACH_COPY_PLAN_APPROVED_MESSAGE,
  RELATIONSHIP_STATUS,
  OUTREACH_STRATEGY_PREVIEW_TITLE,
  OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
  OUTREACH_COPY_PLAN_TITLE,
  OUTREACH_COPY_PLAN_CLOSING_QUESTION,
  OUTREACH_DRAFT_PREVIEW_TITLE,
  OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
  OUTREACH_LAUNCH_GATE_TITLE,
  OUTREACH_LAUNCH_GATE_CLOSING_QUESTION,
  containsRawPromptFragment,
  findRawPromptFragments,
  outreachStrategyPreviewLooksStale,
  findStaleOutreachStrategyFragments,
  repairOutreachStrategyPreview,
  outreachCopyPlanLooksStale,
  findStaleOutreachCopyPlanFragments,
  repairOutreachCopyPlan,
  outreachDraftPreviewLooksStale,
  findStaleOutreachDraftFragments,
  repairOutreachDraftPreview,
  buildCampaignSynthesisContext,
  ensureCampaignMemory,
  applyBatchReviewLearnings,
  rejectsStreetAddressPersonalization,
  DEFAULT_OPERATOR_LEARNINGS,
  OPERATOR_BANNED_FRAGMENT_RES,
  STALE_OUTREACH_COPY_PLAN_FRAGMENT_RES,
  produceOutreachDraftPreviewRevisionResult,
  RESPONSE_MODES,
} = require('../services/clientIntelligenceCampaignPlanning');
const {
  mergeOperatorLearnings,
  draftOutputFingerprint,
} = require('../services/maxSynthesis');
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

describe('Outreach Copy Plan section 4 and 5 operator-facing quality', () => {
  function extractNumberedSection(message, sectionNumber) {
    const msg = String(message || '');
    const re = new RegExp(
      `${sectionNumber}\\.[^\\n]*\\n[\\s\\S]*?(?=\\n\\d+\\. |$)`
    );
    const match = msg.match(re);
    return match ? match[0] : '';
  }

  function buildPlan(overrides = {}) {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage: 'Approve Batch 1',
        workRequestId: batch.workRequestId,
      })
    );
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: [
        'Bedford',
        'Hooksett',
        'Londonderry',
        'Auburn',
        'Goffstown',
      ],
      proofFromPrior: 'checklist, photos, response-time expectation',
      ...overrides.ctx,
    };
    const strategy = buildOutreachStrategyPreview(review, ctx, {
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
        campaignObjective:
          'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
      },
    });
    // Simulate contaminated strategy proofFraming (must not leak into Copy Plan §5).
    strategy.proofFraming = [
      'Carry forward proof already noted: approved Blueprint proof assets',
      'Hold final email/SMS/call scripts until after strategy approval.',
      'Competitive edge is described as reliable crews.',
      'This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
      ...(overrides.extraProofFraming || []),
    ];
    const plan = buildOutreachCopyPlan(
      approveOutreachStrategyPreview(strategy),
      review,
      ctx,
      {}
    );
    return { plan, msg: formatOutreachCopyPlanMessage(plan), ctx, strategy };
  }

  it('section 2 uses operator-facing first-touch goal without meta Batch 1 language', () => {
    const { plan, msg } = buildPlan();
    const section2 = extractNumberedSection(msg, 2);
    assert.match(section2, /First-touch message goal/i);
    assert.match(
      section2,
      /Open a low-pressure conversation with approved Batch 1 property managers about recurring cleaning support, with the goal of earning a short conversation, walkthrough, or estimate request\./
    );
    assert.equal(
      plan.firstTouchGoal,
      'Open a low-pressure conversation with approved Batch 1 property managers about recurring cleaning support, with the goal of earning a short conversation, walkthrough, or estimate request.'
    );
    assert.doesNotMatch(section2, /approved Batch 1 record/i);
    assert.doesNotMatch(section2, /Stay inside Batch 1/i);
    assert.doesNotMatch(section2, /do not expand the list/i);
    assert.doesNotMatch(section2, /Hold final email\/SMS\/call scripts/i);
  });

  it('section 4 lists all towns without ellipses or clipped lists', () => {
    const { plan, msg } = buildPlan();
    const section4 = extractNumberedSection(msg, 4);
    assert.match(section4, /Personalization inputs from Batch 1/i);
    assert.match(
      section4,
      /Prospect town: Bedford, Hooksett, Londonderry, Auburn, or Goffstown\./
    );
    assert.doesNotMatch(section4, /…|\.\.\.|, \u2026/);
    assert.doesNotMatch(section4, /prefer\s+Bedford/i);
    assert.doesNotMatch(section4, /Voice:/i);
    assert.match(
      section4,
      /Property type or portfolio cue when publicly visible\./
    );
    assert.match(
      section4,
      /Public role or decision-maker title when present\./
    );
    assert.match(
      section4,
      /Any visible signal that reliability, responsiveness, or recurring service may matter\./
    );
    assert.deepEqual(plan.personalizationInputs, [
      'Prospect town: Bedford, Hooksett, Londonderry, Auburn, or Goffstown.',
      'Property type or portfolio cue when publicly visible.',
      'Public role or decision-maker title when present.',
      'Any visible signal that reliability, responsiveness, or recurring service may matter.',
    ]);
  });

  it('section 5 is operator-facing proof points without internal/meta language', () => {
    const { plan, msg } = buildPlan();
    const section5 = extractNumberedSection(msg, 5);
    assert.match(section5, /Proof points to use/i);
    assert.match(section5, /Simple commercial cleaning checklist\./);
    assert.match(section5, /Clear response-time expectation\./);
    assert.match(section5, /Clear service area\./);
    assert.match(section5, /Professional walkthrough \/ estimate process\./);
    assert.match(
      section5,
      /Before\/after photos, references, or reviews if available\./
    );
    assert.match(
      section5,
      /Anchor's practical promise: reliable cleaning, responsive communication, and fewer vendor-chasing headaches\./
    );
    assert.doesNotMatch(section5, /Carry forward proof already noted/i);
    assert.doesNotMatch(section5, /Hold final email\/SMS\/call scripts/i);
    assert.doesNotMatch(section5, /Competitive edge is described as/i);
    assert.doesNotMatch(section5, /operator-stated/i);
    assert.doesNotMatch(section5, /Differentiator to lean on/i);
    assert.doesNotMatch(section5, /approved Blueprint proof assets/i);
    assert.doesNotMatch(section5, /Do not invent testimonials/i);
    assert.deepEqual(plan.proofPoints, [
      'Simple commercial cleaning checklist.',
      'Clear response-time expectation.',
      'Clear service area.',
      'Professional walkthrough / estimate process.',
      'Before/after photos, references, or reviews if available.',
      "Anchor's practical promise: reliable cleaning, responsive communication, and fewer vendor-chasing headaches.",
    ]);
  });

  it('keeps guardrails in the approval gate section only', () => {
    const { plan, msg } = buildPlan();
    const section4 = extractNumberedSection(msg, 4);
    const section5 = extractNumberedSection(msg, 5);
    const section7 = extractNumberedSection(msg, 7);
    assert.doesNotMatch(section4, /No sends|No CRM writes|No export/i);
    assert.doesNotMatch(section5, /No sends|No CRM writes|No export/i);
    assert.match(section7, /Approval gate before drafting final copy/i);
    assert.match(section7, /No final email\/SMS\/call scripts/i);
    assert.match(section7, /No sends, CRM writes, exports/i);
    assert.equal(plan.finalOutreachCopyGenerated, false);
    assert.equal(plan.outreachCopyGenerated, false);
    assert.equal(plan.sendsMade, false);
    assert.equal(plan.crmWritesMade, false);
    assert.equal(plan.exportMade, false);
    assert.equal(plan.accountChangesMade, false);
  });
});

describe('Outreach Copy Plan stale-artifact repair before display', () => {
  function extractNumberedSection(message, sectionNumber) {
    const msg = String(message || '');
    const re = new RegExp(
      `${sectionNumber}\\.[^\\n]*\\n[\\s\\S]*?(?=\\n\\d+\\. |$)`
    );
    const match = msg.match(re);
    return match ? match[0] : '';
  }

  function copyPlanFixtures() {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      })
    );
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: [
        'Bedford',
        'Hooksett',
        'Londonderry',
        'Auburn',
        'Goffstown',
      ],
    };
    const strategy = approveOutreachStrategyPreview(
      buildOutreachStrategyPreview(review, ctx, {
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
        },
      })
    );
    return { batch, review, ctx, strategy };
  }

  function staleCopyPlan(review) {
    return {
      kind: 'outreach_copy_plan',
      title: OUTREACH_COPY_PLAN_TITLE,
      status: 'draft',
      channelSequence: [
        'Email first — short intro.',
        'Hold phone / SMS until after approval.',
      ],
      firstTouchGoal:
        'Stay inside Batch 1 (the approved Batch 1 record) and hold scripts until later.',
      ctaToTest: 'A short discovery conversation.',
      personalizationInputs: [
        'prefer Bedford, Hooksett, Londonderry, Auburn, …',
        'Voice: calm professional',
      ],
      proofPoints: [
        'Carry forward proof already noted: approved Blueprint proof assets',
        'Hold final email/SMS/call scripts until after strategy approval.',
        'Differentiator to lean on: Competitive edge is described as reliable crews.',
        'This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
      ],
      followUpTiming: ['Follow-up 1 in a few days.'],
      approvalGate: [
        'Operator must approve before drafting.',
        'No sends, CRM writes, exports.',
      ],
      generatedAt: '2026-08-11T12:00:00.000Z',
      workRequestId: review.workRequestId || 'wr-stale-ocp',
    };
  }

  it('repairs stale Outreach Copy Plan before display', () => {
    const { review, ctx, strategy, batch } = copyPlanFixtures();
    const stale = staleCopyPlan(review);

    assert.equal(outreachCopyPlanLooksStale(stale), true);
    assert.ok(findStaleOutreachCopyPlanFragments(stale).length >= 1);

    const repairedDirect = repairOutreachCopyPlan(
      stale,
      strategy,
      review,
      ctx,
      {}
    );
    assert.equal(repairedDirect.repairedFromStale, true);
    assert.equal(outreachCopyPlanLooksStale(repairedDirect), false);
    assert.equal(
      repairedDirect.firstTouchGoal,
      'Open a low-pressure conversation with approved Batch 1 property managers about recurring cleaning support, with the goal of earning a short conversation, walkthrough, or estimate request.'
    );

    const reply = buildCampaignPlanningReply(
      'Show the Outreach Copy Plan',
      {
        step: 'outreach_copy_plan',
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
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanGenerated: true,
        },
        outreachStrategyPreview: strategy,
        outreachCopyPlan: stale,
        prospectBatchReview: review,
        scoutCandidateBatch: batch,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: stale,
        priorScoutCandidateBatch: batch,
      }
    );

    assert.equal(reply.intent, 'repair_outreach_copy_plan');
    assert.equal(reply.planningState, 'outreach_copy_plan');
    assert.equal(reply.step, 'outreach_copy_plan');
    assert.equal(reply.outreachStrategyPreviewApproved, true);
    assert.ok(reply.outreachCopyPlan);
    assert.equal(reply.outreachCopyPlan.repairedFromStale, true);
    assert.equal(outreachCopyPlanLooksStale(reply.outreachCopyPlan), false);
    assert.equal(reply.repairedFromStale, true);

    assert.match(reply.message, /repaired stale|for approval or revision/i);
    assert.match(reply.message, /Outreach Copy Plan/);
    assert.match(
      reply.message,
      /Does this Outreach Copy Plan look right to approve/
    );
    assert.match(
      reply.message,
      /Not re-rendering the Outreach Strategy Preview/
    );
    assert.doesNotMatch(
      reply.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
    assert.doesNotMatch(reply.message, /^Outreach Strategy Preview$/m);

    // Workflow state unchanged — still on Outreach Copy Plan, strategy stays approved.
    assert.equal(reply.planningState, 'outreach_copy_plan');
    assert.equal(reply.outreachStrategyPreview.status, 'approved');
    assert.equal(reply.prospectBatchReview.status, 'batch_1_approved');
  });

  it('banned fragments never render after repair', () => {
    const { review, ctx, strategy, batch } = copyPlanFixtures();
    const stale = staleCopyPlan(review);
    const reply = buildCampaignPlanningReply(
      'Show the Outreach Copy Plan',
      {
        step: 'outreach_copy_plan',
        answers: {},
        slots: {
          previewApproved: true,
          criteriaApproved: true,
          buildProposalApproved: true,
          prospectBatchReviewApproved: true,
          batch1Approved: true,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanGenerated: true,
        },
        outreachStrategyPreview: strategy,
        outreachCopyPlan: stale,
        prospectBatchReview: review,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: stale,
        priorScoutCandidateBatch: batch,
      }
    );

    const msg = reply.message;
    for (const re of STALE_OUTREACH_COPY_PLAN_FRAGMENT_RES) {
      assert.doesNotMatch(
        msg,
        re,
        `Banned fragment still rendered: ${re}`
      );
    }
    assert.doesNotMatch(msg, /prefer\s+Bedford/i);
    assert.doesNotMatch(msg, /Carry forward proof already noted/i);
    assert.doesNotMatch(msg, /Hold final email\/SMS\/call scripts/i);
    assert.doesNotMatch(msg, /Competitive edge is described as/i);
    assert.doesNotMatch(msg, /This is operator-stated differentiation/i);
    assert.doesNotMatch(msg, /approved Batch 1 record/i);
    assert.doesNotMatch(msg, /Differentiator to lean on/i);
    assert.doesNotMatch(msg, /…/);
    assert.doesNotMatch(msg, /,\s*\.\.\./);
  });

  it('no ellipses or clipped town lists in repaired Copy Plan', () => {
    const { review, ctx, strategy } = copyPlanFixtures();
    const stale = staleCopyPlan(review);
    const repaired = repairOutreachCopyPlan(stale, strategy, review, ctx, {});
    const msg = formatOutreachCopyPlanMessage(repaired);
    const section4 = extractNumberedSection(msg, 4);
    assert.match(
      section4,
      /Prospect town: Bedford, Hooksett, Londonderry, Auburn, or Goffstown\./
    );
    assert.doesNotMatch(section4, /…|\.\.\.|, \u2026/);
    assert.doesNotMatch(section4, /prefer\s+Bedford/i);
    assert.deepEqual(repaired.personalizationInputs, [
      'Prospect town: Bedford, Hooksett, Londonderry, Auburn, or Goffstown.',
      'Property type or portfolio cue when publicly visible.',
      'Public role or decision-maker title when present.',
      'Any visible signal that reliability, responsiveness, or recurring service may matter.',
    ]);
  });

  it('guardrail language stays in section 7 only', () => {
    const { review, ctx, strategy } = copyPlanFixtures();
    const stale = staleCopyPlan(review);
    const repaired = repairOutreachCopyPlan(stale, strategy, review, ctx, {});
    const msg = formatOutreachCopyPlanMessage(repaired);
    const section2 = extractNumberedSection(msg, 2);
    const section4 = extractNumberedSection(msg, 4);
    const section5 = extractNumberedSection(msg, 5);
    const section7 = extractNumberedSection(msg, 7);

    assert.match(
      section2,
      /Open a low-pressure conversation with approved Batch 1 property managers about recurring cleaning support/
    );
    assert.doesNotMatch(section2, /No sends|No CRM writes|No export/i);
    assert.doesNotMatch(section4, /No sends|No CRM writes|No export/i);
    assert.doesNotMatch(section5, /No sends|No CRM writes|No export/i);
    assert.doesNotMatch(section5, /Hold final email\/SMS\/call scripts/i);
    assert.match(section7, /Approval gate before drafting final copy/i);
    assert.match(section7, /No final email\/SMS\/call scripts/i);
    assert.match(section7, /No sends, CRM writes, exports/i);
    assert.deepEqual(repaired.proofPoints, [
      'Simple commercial cleaning checklist.',
      'Clear response-time expectation.',
      'Clear service area.',
      'Professional walkthrough / estimate process.',
      'Before/after photos, references, or reviews if available.',
      "Anchor's practical promise: reliable cleaning, responsive communication, and fewer vendor-chasing headaches.",
    ]);
  });

  it('workflow state remains unchanged when repairing Copy Plan', () => {
    const { review, ctx, strategy, batch } = copyPlanFixtures();
    const stale = staleCopyPlan(review);
    const reply = buildCampaignPlanningReply(
      'Show the Outreach Copy Plan',
      {
        step: 'outreach_copy_plan',
        answers: {},
        slots: {
          previewApproved: true,
          criteriaApproved: true,
          buildProposalApproved: true,
          prospectBatchReviewApproved: true,
          batch1Approved: true,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanGenerated: true,
        },
        outreachStrategyPreview: strategy,
        outreachCopyPlan: stale,
        prospectBatchReview: review,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: stale,
        priorScoutCandidateBatch: batch,
      }
    );

    assert.equal(reply.planningState, 'outreach_copy_plan');
    assert.equal(reply.step, 'outreach_copy_plan');
    assert.equal(reply.batch1Approved, true);
    assert.equal(reply.prospectBatchReviewApproved, true);
    assert.equal(reply.outreachStrategyPreviewApproved, true);
    assert.equal(reply.strategyApproved, true);
    assert.equal(reply.liveSourcingApproved, false);
    assert.equal(reply.outreachCopyGenerated, false);
    assert.equal(reply.finalOutreachCopyGenerated, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
    // Does not bounce back to strategy preview or re-ask strategy approval.
    assert.notEqual(reply.planningState, 'outreach_strategy_preview');
    assert.doesNotMatch(
      reply.message,
      /Does this Outreach Strategy Preview look right to approve/
    );
  });
});

describe('Review artifact chain — Copy Plan → Draft Preview → Launch Gate', () => {
  const copyPlanApprovalMessage =
    'Approve the Outreach Copy Plan. Next step: create the Outreach Draft Preview.';
  const draftApprovalMessage =
    'Approve the Outreach Draft Preview. Next step: create the Outreach Launch Gate.';

  function chainFixtures() {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      })
    );
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: [
        'Bedford',
        'Hooksett',
        'Londonderry',
        'Auburn',
        'Goffstown',
      ],
    };
    const strategy = approveOutreachStrategyPreview(
      buildOutreachStrategyPreview(review, ctx, {
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
        },
      })
    );
    const plan = buildOutreachCopyPlan(strategy, review, ctx, {});
    const sessionState = {
      step: 'outreach_copy_plan',
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
        outreachStrategyPreviewApproved: true,
        strategyApproved: true,
        outreachCopyPlanGenerated: true,
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
      outreachCopyPlan: plan,
    };
    return { review, ctx, strategy, plan, sessionState };
  }

  it('REVIEW_ARTIFACT_CHAIN advances generically', () => {
    assert.deepEqual(REVIEW_ARTIFACT_CHAIN, [
      ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
      ARTIFACT_KINDS.OUTREACH_COPY_PLAN,
      ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW,
      ARTIFACT_KINDS.OUTREACH_LAUNCH_GATE,
    ]);
    assert.equal(
      nextReviewArtifactKind(ARTIFACT_KINDS.OUTREACH_COPY_PLAN),
      ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW
    );
    assert.equal(
      nextReviewArtifactKind(ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW),
      ARTIFACT_KINDS.OUTREACH_LAUNCH_GATE
    );
    assert.equal(
      nextReviewArtifactKind(ARTIFACT_KINDS.OUTREACH_LAUNCH_GATE),
      null
    );
  });

  it('Copy Plan approval advances to Draft Preview', () => {
    const { review, ctx, strategy, plan, sessionState } = chainFixtures();

    assert.equal(
      looksLikeOutreachCopyPlanApproval(copyPlanApprovalMessage, {
        priorOutreachCopyPlan: plan,
        priorOutreachStrategyPreview: strategy,
        priorProspectBatchReview: review,
        step: 'outreach_copy_plan',
      }),
      true
    );
    assert.equal(
      classifyProspectAcquisitionIntent(copyPlanApprovalMessage, {
        priorOutreachCopyPlan: plan,
        priorOutreachStrategyPreview: strategy,
        priorProspectBatchReview: review,
        step: 'outreach_copy_plan',
      }),
      PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_COPY_PLAN
    );

    const action = resolveCampaignArtifactAction({
      userMessage: copyPlanApprovalMessage,
      messageClass: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
      memory: (() => {
        let mem = emptyReasoningMemory();
        mem = markArtifactApproved(mem, ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW);
        mem = markArtifactApproved(
          mem,
          ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW
        );
        mem = markArtifactGenerated(
          mem,
          ARTIFACT_KINDS.OUTREACH_COPY_PLAN,
          'draft'
        );
        return mem;
      })(),
      priorProspectBatchReview: review,
      priorOutreachStrategyPreview: strategy,
      priorOutreachCopyPlan: plan,
      step: 'outreach_copy_plan',
    });
    assert.equal(action.action, 'approve_outreach_copy_plan');
    assert.equal(action.emitKind, ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW);
    assert.equal(action.planningState, 'outreach_draft_preview');

    const reply = buildCampaignPlanningReply(
      copyPlanApprovalMessage,
      sessionState,
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        messageClass: MESSAGE_CLASSES.APPROVAL_PLUS_NEXT_REQUEST,
      }
    );

    assert.match(reply.message, /Outreach Draft Preview/);
    assert.match(
      reply.message,
      /Does this Outreach Draft Preview look right to approve/
    );
    assert.doesNotMatch(
      reply.message,
      /Does this Outreach Copy Plan look right to approve/
    );
    assert.equal(reply.planningState, 'outreach_draft_preview');
    assert.equal(reply.outreachCopyPlan.status, 'approved');
    assert.ok(reply.outreachDraftPreview);
    assert.equal(reply.outreachDraftPreview.kind, 'outreach_draft_preview');
    assert.equal(reply.outreachDraftPreview.status, 'draft');
    assert.ok((reply.outreachDraftPreview.subjectOptions || []).length >= 1);
    assert.ok(reply.outreachDraftPreview.firstTouchBody);
    assert.ok(
      (reply.outreachDraftPreview.personalizationByProspect || []).length >= 1
    );
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
  });

  it('repeating Copy Plan approval is idempotent and shows Draft Preview', () => {
    const { review, ctx, strategy, plan, sessionState } = chainFixtures();
    const first = buildCampaignPlanningReply(
      copyPlanApprovalMessage,
      sessionState,
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
      }
    );
    const second = buildCampaignPlanningReply(
      copyPlanApprovalMessage,
      {
        ...sessionState,
        step: 'outreach_draft_preview',
        outreachCopyPlan: first.outreachCopyPlan,
        outreachDraftPreview: first.outreachDraftPreview,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: first.outreachCopyPlan,
        priorOutreachDraftPreview: first.outreachDraftPreview,
      }
    );
    assert.equal(second.planningState, 'outreach_draft_preview');
    assert.match(second.message, /Outreach Draft Preview/);
    assert.doesNotMatch(
      second.message,
      /Does this Outreach Copy Plan look right to approve/
    );
  });

  it('creates Draft Preview when missing and shows existing when present', () => {
    const { review, ctx, strategy, plan, sessionState } = chainFixtures();
    const approvedPlan = approveOutreachCopyPlan(plan);
    const created = buildCampaignPlanningReply(
      'Create the Outreach Draft Preview',
      {
        ...sessionState,
        outreachCopyPlan: approvedPlan,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: approvedPlan,
      }
    );
    assert.ok(created.outreachDraftPreview);
    assert.equal(created.intent, 'produce_outreach_draft_preview');

    const shown = buildCampaignPlanningReply(
      'Show the Outreach Draft Preview',
      {
        ...sessionState,
        step: 'outreach_draft_preview',
        outreachCopyPlan: created.outreachCopyPlan,
        outreachDraftPreview: created.outreachDraftPreview,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: created.outreachCopyPlan,
        priorOutreachDraftPreview: created.outreachDraftPreview,
      }
    );
    assert.equal(shown.intent, 'show_outreach_draft_preview');
    assert.equal(
      shown.outreachDraftPreview.firstTouchBody,
      created.outreachDraftPreview.firstTouchBody
    );
  });

  it('Draft Preview approval advances to Launch Gate', () => {
    const { review, ctx, strategy, plan, sessionState } = chainFixtures();
    const approvedPlan = approveOutreachCopyPlan(plan);
    const draft = buildOutreachDraftPreview(
      approvedPlan,
      strategy,
      review,
      ctx,
      {}
    );

    assert.equal(
      looksLikeOutreachDraftPreviewApproval(draftApprovalMessage, {
        priorOutreachDraftPreview: draft,
        priorOutreachCopyPlan: approvedPlan,
        step: 'outreach_draft_preview',
      }),
      true
    );

    const reply = buildCampaignPlanningReply(
      draftApprovalMessage,
      {
        ...sessionState,
        step: 'outreach_draft_preview',
        outreachCopyPlan: approvedPlan,
        outreachDraftPreview: draft,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: approvedPlan,
        priorOutreachDraftPreview: draft,
      }
    );

    assert.match(reply.message, /Outreach Launch Gate/);
    assert.match(
      reply.message,
      /Does this Outreach Launch Gate look right to approve/
    );
    assert.doesNotMatch(
      reply.message,
      /Does this Outreach Draft Preview look right to approve/
    );
    assert.equal(reply.planningState, 'outreach_launch_gate');
    assert.equal(reply.outreachDraftPreview.status, 'approved');
    assert.ok(reply.outreachLaunchGate);
    assert.equal(reply.outreachLaunchGate.kind, 'outreach_launch_gate');
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
    assert.equal(reply.launched, false);
  });

  it('Launch Gate approval marks readiness without executing', () => {
    const { review, ctx, strategy, plan, sessionState } = chainFixtures();
    const approvedPlan = approveOutreachCopyPlan(plan);
    const draft = approveOutreachDraftPreview(
      buildOutreachDraftPreview(approvedPlan, strategy, review, ctx, {})
    );
    const gate = buildOutreachLaunchGate(
      draft,
      approvedPlan,
      strategy,
      review,
      ctx,
      {}
    );

    const reply = buildCampaignPlanningReply(
      'Approve the Outreach Launch Gate',
      {
        ...sessionState,
        step: 'outreach_launch_gate',
        outreachCopyPlan: approvedPlan,
        outreachDraftPreview: draft,
        outreachLaunchGate: gate,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewApproved: true,
          draftPreviewApproved: true,
          outreachLaunchGateGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: approvedPlan,
        priorOutreachDraftPreview: draft,
        priorOutreachLaunchGate: gate,
      }
    );

    assert.equal(reply.outreachLaunchGate.status, 'approved');
    assert.equal(reply.launchReady, true);
    assert.equal(reply.launched, false);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
    assert.match(reply.message, /readiness only|explicit/i);
  });

  it('operator-facing draft digest comes first without banned fragments', () => {
    const { review, ctx, strategy, plan } = chainFixtures();
    const draft = buildOutreachDraftPreview(
      approveOutreachCopyPlan(plan),
      strategy,
      review,
      ctx,
      {}
    );
    const msg = formatOutreachDraftPreviewMessage(draft);
    assert.match(msg, /## Recommended decision/);
    assert.match(msg, /View evidence/);
    const digestIdx = msg.indexOf('## Recommended decision');
    const bodyIdx = msg.indexOf('First-touch draft');
    assert.ok(digestIdx >= 0 && bodyIdx > digestIdx);
    for (const re of OPERATOR_BANNED_FRAGMENT_RES) {
      assert.doesNotMatch(msg, re);
    }
    assert.doesNotMatch(msg, /\u2026|\.\.\./);
    assert.match(msg, new RegExp(OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION));
    assert.doesNotMatch(msg, new RegExp(OUTREACH_COPY_PLAN_CLOSING_QUESTION));
  });

  it('Batch 1 exclusions remain visible through Draft Preview', () => {
    const batch = withGroups(sampleOperatorDigestBatch1());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      })
    );
    assert.equal(review.counts.accepted, 6);
    const names = (review.approvedBatch.candidates || []).map(
      (c) => c.companyName
    );
    assert.ok(names.includes('Real Property Management Thrive'));
    assert.ok(names.includes('Avise Properties'));
    assert.ok(!names.includes('Cedar Management Group'));
    assert.ok(
      !names.includes('Keyrenter New England Property Management')
    );

    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice: 'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication.',
      towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
    };
    const strategy = approveOutreachStrategyPreview(
      buildOutreachStrategyPreview(review, ctx, {})
    );
    const plan = approveOutreachCopyPlan(
      buildOutreachCopyPlan(strategy, review, ctx, {})
    );
    const draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {});
    const msg = formatOutreachDraftPreviewMessage(draft);
    assert.match(msg, /Cedar|Keyrenter|optional expansion|rejected/i);
    assert.equal(draft.sendsMade, false);
    assert.equal(draft.crmWritesMade, false);
    assert.equal(draft.exportMade, false);
  });

  it('strategy proof framing no longer emits banned carry-forward meta lines', () => {
    const { review, ctx } = chainFixtures();
    const strategy = buildOutreachStrategyPreview(review, ctx, {
      priorCriteriaPreview: {
        campaignObjective:
          'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
      },
    });
    const msg = formatOutreachStrategyPreviewMessage(strategy);
    assert.doesNotMatch(msg, /Carry forward proof already noted/i);
    assert.doesNotMatch(msg, /Competitive edge is described as/i);
    assert.doesNotMatch(msg, /This is operator-stated differentiation/i);
    assert.doesNotMatch(msg, /for Small to mid-sized/);
    assert.doesNotMatch(msg, /Keep Greater Manchester in scope/i);
  });
});

describe('Campaign Memory — CampaignSynthesisContext', () => {
  function memoryFixtures() {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      })
    );
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: [
        'Bedford',
        'Hooksett',
        'Londonderry',
        'Auburn',
        'Goffstown',
      ],
    };
    const strategy = approveOutreachStrategyPreview(
      buildOutreachStrategyPreview(review, ctx, {
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
        },
      })
    );
    const plan = approveOutreachCopyPlan(
      buildOutreachCopyPlan(strategy, review, ctx, {})
    );
    return { review, ctx, strategy, plan };
  }

  it('tested subject pattern keeps {{business_name}} merge token', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    const draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {});
    assert.equal(draft.usedTestedSubjectLine, true);
    assert.equal(draft.keptMergeTokens, true);
    assert.deepEqual(draft.subjectOptions, [
      '{{business_name}} - commercial cleaning',
    ]);
    assert.equal(
      draft.testedSubjectLinePattern,
      DEFAULT_OPERATOR_LEARNINGS.tested_subject_line_pattern
    );
    assert.equal(draft.claimTestedWinner, false);
    assert.doesNotMatch(
      (draft.subjectOptions || []).join('\n'),
      /Quick question about cleaning reliability/i
    );
    assert.doesNotMatch(
      (draft.subjectOptions || []).join('\n'),
      /Worth a brief chat about recurring cleaning coverage/i
    );
    const msg = formatOutreachDraftPreviewMessage(draft);
    assert.doesNotMatch(msg, /Subject line \(tested winner\)/);
    assert.match(msg, /\{\{business_name\}\} - commercial cleaning/);
    assert.doesNotMatch(msg, /Anchor - commercial cleaning/);
  });

  it('Keyrenter remains existing_relationship_nurture across later steps', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    let memory = applyBatchReviewLearnings(ensureCampaignMemory({}), review);
    assert.equal(
      memory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );

    const draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {
      campaignMemory: memory,
      step: 'outreach_draft_preview',
    });
    memory = draft.campaignMemory;
    assert.equal(
      memory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );
    assert.ok(
      !(draft.batchProspects || []).some((n) => /Keyrenter/i.test(n))
    );
    assert.ok(
      !(draft.personalizationByProspect || []).some((r) =>
        /Keyrenter/i.test(r.companyName)
      )
    );

    const gate = buildOutreachLaunchGate(
      approveOutreachDraftPreview(draft),
      plan,
      strategy,
      review,
      ctx,
      { campaignMemory: memory }
    );
    assert.ok(
      !(gate.batchProspects || []).some((n) => /Keyrenter/i.test(n)) ||
        /Keyrenter|existing.?relationship|nurture/i.test(
          formatOutreachLaunchGateMessage(gate)
        )
    );
    assert.equal(
      (draft.campaignLearnings || {}).keyrenter_status,
      'existing_relationship_nurture'
    );
  });

  it('Cedar remains source_verification_required across later steps', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    let memory = applyBatchReviewLearnings(ensureCampaignMemory({}), review);
    assert.equal(
      memory.operatorLearnings.cedar_status,
      'source_verification_required'
    );

    const draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {
      campaignMemory: memory,
    });
    assert.equal(
      draft.campaignMemory.operatorLearnings.cedar_status,
      'source_verification_required'
    );
    assert.ok(!(draft.batchProspects || []).some((n) => /Cedar/i.test(n)));
    assert.ok(
      !(draft.personalizationByProspect || []).some((r) =>
        /Cedar/i.test(r.companyName)
      )
    );

    const synthesis = buildCampaignSynthesisContext({
      context: ctx,
      approvedReview: review,
      approvedOutreachStrategy: strategy,
      approvedOutreachCopyPlan: plan,
      campaignMemory: draft.campaignMemory,
      step: 'outreach_launch_gate',
    });
    assert.equal(
      synthesis.learnings.cedar_status,
      'source_verification_required'
    );
  });

  it('street-address personalization is rejected by default', () => {
    assert.equal(
      rejectsStreetAddressPersonalization(
        'Reference 123 Main Street when personalizing'
      ),
      true
    );
    assert.equal(
      rejectsStreetAddressPersonalization(
        'Reference {{town}} and a public portfolio cue'
      ),
      false
    );

    const { review, ctx, strategy, plan } = memoryFixtures();
    const polluted = {
      ...(review.approvedBatch.candidates[0] || {}),
      companyName: 'Acme Property Group',
      address: '45 Elm Street',
      location: '45 Elm Street, Bedford NH',
    };
    const reviewWithStreet = {
      ...review,
      approvedBatch: {
        ...review.approvedBatch,
        candidates: [polluted, ...(review.approvedBatch.candidates || []).slice(1)],
      },
    };
    const draft = buildOutreachDraftPreview(
      plan,
      strategy,
      reviewWithStreet,
      ctx,
      {}
    );
    for (const row of draft.personalizationByProspect || []) {
      assert.equal(
        rejectsStreetAddressPersonalization(row.personalizationNote),
        false,
        row.personalizationNote
      );
      assert.doesNotMatch(row.personalizationNote, /\d{1,6}\s+\w+\s+Street/i);
      assert.match(
        row.personalizationNote,
        /\{\{town\}\}|town|portfolio|role|company/i
      );
    }
    assert.match(
      draft.campaignLearnings.personalization_rule,
      /do not use street addresses by default/i
    );
  });

  it('Outreach Draft Preview uses {{town}}, not a full town list', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    const draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {});
    assert.match(draft.firstTouchBody, /\{\{town\}\}/);
    assert.doesNotMatch(
      draft.firstTouchBody,
      /Bedford,\s*Hooksett,\s*Londonderry,\s*Auburn/
    );
    assert.doesNotMatch(draft.firstTouchBody, /\bI work with\b/);
    assert.match(draft.firstTouchBody, /Anchor helps/);
    const msg = formatOutreachDraftPreviewMessage(draft);
    assert.match(msg, /\{\{town\}\}/);
    assert.doesNotMatch(
      msg,
      /across Bedford,\s*Hooksett,\s*Londonderry,\s*Auburn/
    );
  });

  it('approved operator learnings survive step transitions', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    const sessionState = {
      step: 'outreach_copy_plan',
      campaignMemory: applyBatchReviewLearnings(ensureCampaignMemory({}), review),
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
        outreachStrategyPreviewApproved: true,
        strategyApproved: true,
        outreachCopyPlanGenerated: true,
      },
      prospectListCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'approved',
        campaignObjective:
          'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
      },
      prospectBatchReview: review,
      outreachStrategyPreview: strategy,
      outreachCopyPlan: plan,
    };

    const reply = buildCampaignPlanningReply(
      'Approve the Outreach Copy Plan. Next step: create the Outreach Draft Preview.',
      sessionState,
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        campaignMemory: sessionState.campaignMemory,
      }
    );

    assert.ok(reply.campaignMemory);
    assert.equal(
      reply.campaignMemory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );
    assert.equal(
      reply.campaignMemory.operatorLearnings.cedar_status,
      'source_verification_required'
    );
    assert.equal(
      reply.campaignMemory.operatorLearnings.tested_subject_line_pattern,
      '{{business_name}} - commercial cleaning'
    );
    assert.equal(
      reply.outreachDraftPreview.campaignMemory.operatorLearnings
        .personalization_rule,
      'do not use street addresses by default'
    );

    // Advance again — learnings must still be present.
    const second = buildCampaignPlanningReply(
      'Show the Outreach Draft Preview',
      {
        ...sessionState,
        step: 'outreach_draft_preview',
        campaignMemory: reply.campaignMemory,
        outreachCopyPlan: reply.outreachCopyPlan,
        outreachDraftPreview: reply.outreachDraftPreview,
        slots: {
          ...sessionState.slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: reply.outreachCopyPlan,
        priorOutreachDraftPreview: reply.outreachDraftPreview,
        campaignMemory: reply.campaignMemory,
      }
    );
    assert.equal(
      second.campaignMemory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );
    assert.equal(
      second.outreachDraftPreview.subjectOptions[0],
      '{{business_name}} - commercial cleaning'
    );
  });

  it('stale stored Outreach Draft Preview is repaired before rendering', () => {
    const { review, ctx, strategy, plan } = memoryFixtures();
    const campaignCtx = buildCampaignSynthesisContext({
      context: ctx,
      approvedReview: review,
      approvedOutreachStrategy: strategy,
      approvedOutreachCopyPlan: plan,
    });

    const stale = {
      kind: 'outreach_draft_preview',
      title: OUTREACH_DRAFT_PREVIEW_TITLE,
      status: 'draft',
      businessName: 'Anchor',
      subjectOptions: [
        'Quick question about cleaning reliability in Greater Manchester',
        'Anchor — responsive commercial cleaning for property managers',
        'Worth a brief chat about recurring cleaning coverage?',
      ],
      firstTouchBody: [
        'Hi {{first_name}},',
        '',
        'I work with property managers across Bedford, Hooksett, Londonderry, Auburn, or Goffstown who want reliable commercial cleaning without chasing vendors.',
        '',
        'Would you be open to a short discovery conversation?',
      ].join('\n'),
      personalizationByProspect: [
        {
          companyName: 'Keyrenter New England Property Management',
          town: 'Bedford',
          personalizationNote:
            'Reference 12 North Street and the Keyrenter office address.',
        },
        {
          companyName: 'Cedar Management Group',
          town: 'Hooksett',
          personalizationNote: 'Use the Hooksett street address on the listing.',
        },
      ],
      batchProspects: [
        'Keyrenter New England Property Management',
        'Cedar Management Group',
      ],
      followUpSketch: ['Hold follow-ups'],
      approvalGate: ['No sends'],
      generatedAt: '2026-01-01T00:00:00.000Z',
    };

    assert.equal(outreachDraftPreviewLooksStale(stale, campaignCtx), true);
    assert.ok(findStaleOutreachDraftFragments(stale).length >= 1);

    const repaired = repairOutreachDraftPreview(
      stale,
      plan,
      strategy,
      review,
      ctx,
      { campaignSynthesisContext: campaignCtx }
    );
    assert.equal(repaired.repairedFromStale, true);
    assert.equal(repaired.usedTestedSubjectLine, true);
    assert.deepEqual(repaired.subjectOptions, [
      '{{business_name}} - commercial cleaning',
    ]);
    assert.match(repaired.firstTouchBody, /\{\{town\}\}/);
    assert.match(repaired.firstTouchBody, /Anchor helps/);
    assert.doesNotMatch(repaired.firstTouchBody, /\bI work with\b/);
    assert.doesNotMatch(
      repaired.firstTouchBody,
      /Bedford,\s*Hooksett,\s*Londonderry/
    );
    assert.ok(
      !(repaired.batchProspects || []).some((n) => /Keyrenter|Cedar/i.test(n))
    );
    for (const row of repaired.personalizationByProspect || []) {
      assert.equal(
        rejectsStreetAddressPersonalization(row.personalizationNote),
        false
      );
    }

    const reply = buildCampaignPlanningReply(
      'Show the Outreach Draft Preview',
      {
        step: 'outreach_draft_preview',
        slots: {
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
          batch1Approved: true,
          outreachStrategyPreviewApproved: true,
        },
        prospectBatchReview: review,
        outreachStrategyPreview: strategy,
        outreachCopyPlan: plan,
        outreachDraftPreview: stale,
        campaignMemory: campaignCtx.campaignMemory,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        priorOutreachDraftPreview: stale,
        campaignMemory: campaignCtx.campaignMemory,
      }
    );
    assert.equal(reply.outreachDraftPreview.repairedFromStale, true);
    assert.match(reply.message, /repaired stale/i);
    assert.match(reply.message, /\{\{business_name\}\} - commercial cleaning/);
    assert.doesNotMatch(reply.message, /Anchor - commercial cleaning/);
    assert.match(reply.message, /\{\{town\}\}/);
    assert.doesNotMatch(reply.message, /\bI work with\b/);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
  });
});

describe('Max Chat Responsiveness — Anchor Outreach Draft Preview', () => {
  function anchorFixtures() {
    const batch = withGroups(sampleKeyrenterCorrectionBatch());
    const review = approveProspectBatchReviewBatch1(
      buildProspectBatchReview(batch, {
        userMessage:
          'Remove Keyrenter New England Property Management from the accepted cold first-pass candidates — it is an existing relationship, not a cold prospect. Keep it as nurture.',
        workRequestId: batch.workRequestId,
      })
    );
    const ctx = {
      businessName: 'Anchor Cleaning',
      brandVoice:
        'calm, professional, reliable, direct, and easy to work with',
      competitiveAdvantages:
        'Reliability and accountability. Responsive communication. Peace of mind for recurring commercial cleaning relationships.',
      primarySegment: 'property managers',
      targetMarket: 'Greater Manchester',
      towns: [
        'Bedford',
        'Hooksett',
        'Londonderry',
        'Auburn',
        'Goffstown',
      ],
    };
    const strategy = approveOutreachStrategyPreview(
      buildOutreachStrategyPreview(review, ctx, {
        priorCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
          campaignObjective:
            'Prove that Greater Manchester property managers will take a discovery conversation about recurring commercial cleaning.',
        },
      })
    );
    const plan = approveOutreachCopyPlan(
      buildOutreachCopyPlan(strategy, review, ctx, {})
    );
    let memory = applyBatchReviewLearnings(ensureCampaignMemory({}), review);
    memory = mergeOperatorLearnings(
      memory,
      {
        tested_subject_line_pattern: '{{business_name}} - commercial cleaning',
        subject_keep_merge_tokens: true,
        claim_tested_winner: false,
        personalization_rule: 'do not use street addresses by default',
        copy_differentiator:
          'reliability, responsiveness, accountability, fewer vendor-chasing headaches',
      },
      'campaign_setup'
    );
    return { review, ctx, strategy, plan, memory };
  }

  it('operator revision overrides stale draft and responds conversationally', () => {
    const { review, ctx, strategy, plan, memory } = anchorFixtures();

    // Stale stored draft — expanded subject, street addresses, sketches only.
    const staleDraft = {
      kind: 'outreach_draft_preview',
      title: OUTREACH_DRAFT_PREVIEW_TITLE,
      status: 'draft',
      businessName: 'Anchor',
      subjectOptions: ['Anchor - commercial cleaning'],
      firstTouchBody: [
        'Hi {{first_name}},',
        '',
        'I work with property managers across Bedford who want cleaning.',
        '',
        'Would you be open to a chat?',
      ].join('\n'),
      personalizationByProspect: [
        {
          companyName: 'Acme Property Group',
          town: 'Bedford',
          personalizationNote: 'Reference 12 North Street in Bedford.',
        },
      ],
      batchProspects: ['Acme Property Group'],
      followUpSketch: [
        'Follow-up 1 (~3 business days): restate the same CTA with one fresh personalization detail.',
        'Follow-up 2 (~7 business days): offer a clear close-the-loop option (reply / book / not now).',
      ],
      approvalGate: ['No sends'],
      operatorDigest: {
        kind: 'outreach_draft_preview_digest',
        title: OUTREACH_DRAFT_PREVIEW_TITLE,
        recommendedDecision: 'Approve',
        primaryActions: [{ id: 'approve', label: 'Approve' }],
      },
      generatedAt: '2026-01-01T00:00:00.000Z',
    };

    const reply = buildCampaignPlanningReply(
      'Revise the Outreach Draft Preview. Use `{{business_name}} - commercial cleaning`; no street addresses; draft actual follow-ups; answer like an LLM/operator, not a workflow renderer.',
      {
        step: 'outreach_draft_preview',
        slots: {
          previewApproved: true,
          criteriaApproved: true,
          buildProposalApproved: true,
          prospectBatchReviewApproved: true,
          batch1Approved: true,
          outreachStrategyPreviewApproved: true,
          strategyApproved: true,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
        prospectBatchReview: review,
        outreachStrategyPreview: strategy,
        outreachCopyPlan: plan,
        outreachDraftPreview: staleDraft,
        campaignMemory: memory,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        priorOutreachDraftPreview: staleDraft,
        campaignMemory: memory,
        messageClass: 'refinement_feedback',
      }
    );

    assert.equal(reply.intent, 'revise_outreach_draft_preview');
    assert.equal(reply.responseMode, 'operator_chat_response');
    assert.ok(reply.outreachDraftPreview);
    assert.deepEqual(reply.outreachDraftPreview.subjectOptions, [
      '{{business_name}} - commercial cleaning',
    ]);
    assert.match(reply.message, /\{\{business_name\}\} - commercial cleaning/);
    assert.doesNotMatch(reply.message, /Anchor - commercial cleaning/);
    assert.match(reply.message, /## First-touch email|# First-touch/i);
    assert.match(reply.message, /## Follow-up 1|Follow-up 1/i);
    assert.match(reply.message, /## Follow-up 2|Follow-up 2/i);
    assert.ok(
      Array.isArray(reply.outreachDraftPreview.followUpDrafts) &&
        reply.outreachDraftPreview.followUpDrafts.length >= 2
    );
    assert.match(
      reply.outreachDraftPreview.followUpDrafts[0].body,
      /Hi \{\{first_name\}\}/
    );
    assert.match(
      reply.outreachDraftPreview.followUpDrafts[1].body,
      /Hi \{\{first_name\}\}/
    );
    assert.match(reply.outreachDraftPreview.firstTouchBody, /\{\{town\}\}/);
    assert.match(reply.outreachDraftPreview.firstTouchBody, /Anchor helps/);
    assert.match(
      reply.outreachDraftPreview.firstTouchBody,
      /reliab|respons|accountab|vendor/i
    );
    for (const row of reply.outreachDraftPreview.personalizationByProspect || []) {
      assert.doesNotMatch(row.personalizationNote, /\d{1,6}\s+\w+\s+Street/i);
    }
    assert.doesNotMatch(reply.message, /View evidence/i);
    assert.doesNotMatch(reply.message, /Primary actions/i);
    assert.doesNotMatch(reply.message, /Recommended decision/i);
    assert.doesNotMatch(reply.message, /tested winner/i);
    assert.equal(reply.sendsMade, false);
    assert.equal(reply.crmWritesMade, false);
    assert.equal(reply.exportMade, false);
    assert.equal(reply.accountChangesMade, false);
    assert.ok(reply.campaignMemory);
    assert.equal(
      reply.campaignMemory.operatorLearnings.tested_subject_line_pattern,
      '{{business_name}} - commercial cleaning'
    );
    assert.equal(
      reply.campaignMemory.operatorLearnings.draft_follow_ups,
      true
    );
    assert.ok(reply.campaignWorkingState);
    assert.match(
      reply.campaignWorkingState.latestOperatorInstruction || '',
      /Revise the Outreach Draft Preview/
    );
  });

  it('repeated identical rejected output triggers stale source diagnostic', () => {
    const { review, ctx, strategy, plan, memory } = anchorFixtures();
    const staleDraft = {
      kind: 'outreach_draft_preview',
      title: OUTREACH_DRAFT_PREVIEW_TITLE,
      status: 'draft',
      subjectOptions: ['Anchor - commercial cleaning'],
      firstTouchBody: 'stale',
      personalizationByProspect: [],
      followUpSketch: ['sketch only'],
      approvalGate: ['No sends'],
      generatedAt: '2026-01-01T00:00:00.000Z',
    };

    const correction =
      'Revise the Outreach Draft Preview. Use `{{business_name}} - commercial cleaning`; no street addresses; draft actual follow-ups; answer like an LLM/operator, not a workflow renderer.';

    const first = buildCampaignPlanningReply(
      correction,
      {
        step: 'outreach_draft_preview',
        slots: {
          batch1Approved: true,
          outreachStrategyPreviewApproved: true,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
        },
        prospectBatchReview: review,
        outreachStrategyPreview: strategy,
        outreachCopyPlan: plan,
        outreachDraftPreview: staleDraft,
        campaignMemory: memory,
      },
      ctx,
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        priorOutreachDraftPreview: staleDraft,
        campaignMemory: memory,
        messageClass: 'correction',
      }
    );

    // Simulate a broken path that re-emits the same rejected fingerprint.
    const rejectedFp = draftOutputFingerprint(staleDraft);
    const working = {
      ...(first.campaignWorkingState || {}),
      rejectedOutputFingerprints: [rejectedFp],
      latestOperatorInstruction: correction,
      activeArtifactKind: 'outreach_draft_preview',
    };

    const second = produceOutreachDraftPreviewRevisionResult(
      ctx,
      {},
      {
        batch1Approved: true,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
      },
      {
        priorProspectBatchReview: review,
        priorOutreachStrategyPreview: strategy,
        priorOutreachCopyPlan: plan,
        priorOutreachDraftPreview: staleDraft,
        campaignMemory: first.campaignMemory || memory,
        campaignWorkingState: working,
      },
      correction
    );

    assert.equal(second.intent, 'stale_source_diagnostic');
    assert.equal(second.responseMode, 'stale_source_diagnostic');
    assert.match(second.message, /Stale source diagnostic/i);
    assert.match(second.message, /Campaign memory retrieved/i);
    assert.match(second.message, /Latest operator instruction included/i);
    assert.match(second.message, /Response mode selected/i);
    assert.match(second.message, /Source that won/i);
    assert.doesNotMatch(second.message, /## First-touch email/);
    assert.equal(second.sendsMade, false);
  });
});
