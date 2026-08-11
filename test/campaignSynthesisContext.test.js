'use strict';

/**
 * Campaign Memory / CampaignSynthesisContext unit tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  DEFAULT_OPERATOR_LEARNINGS,
  emptyCampaignMemory,
  ensureCampaignMemory,
  upsertOperatorLearning,
  applyBatchReviewLearnings,
  buildCampaignSynthesisContext,
  resolveSubjectLines,
  resolveSenderVoiceLine,
  rejectsStreetAddressPersonalization,
  findCampaignMemoryDraftConflicts,
} = require('../services/maxSynthesis');

describe('CampaignSynthesisContext unit', () => {
  it('seeds durable default operator learnings', () => {
    const memory = ensureCampaignMemory({});
    assert.equal(
      memory.operatorLearnings.tested_subject_line_pattern,
      '{{business_name}} - commercial cleaning'
    );
    assert.equal(
      memory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );
    assert.equal(
      memory.operatorLearnings.cedar_status,
      'source_verification_required'
    );
    assert.deepEqual(
      Object.keys(DEFAULT_OPERATOR_LEARNINGS).sort(),
      Object.keys(memory.operatorLearnings).sort()
    );
  });

  it('preserves operator learning overrides across ensure()', () => {
    let memory = emptyCampaignMemory();
    memory = upsertOperatorLearning(
      memory,
      'tested_subject_line_pattern',
      '{{business_name}} :: reliability check',
      'operator'
    );
    memory = ensureCampaignMemory({ campaignMemory: memory });
    assert.equal(
      memory.operatorLearnings.tested_subject_line_pattern,
      '{{business_name}} :: reliability check'
    );
    assert.equal(
      memory.operatorLearnings.personalization_rule,
      'do not use street addresses by default'
    );
  });

  it('applyBatchReviewLearnings locks Keyrenter and Cedar statuses', () => {
    const memory = applyBatchReviewLearnings(emptyCampaignMemory(), {
      existingRelationship: [
        { companyName: 'Keyrenter New England Property Management' },
      ],
      sourceVerificationRequired: [{ companyName: 'Cedar Management Group' }],
      approvedBatch: {
        excludedExistingRelationship: [
          'Keyrenter New England Property Management',
        ],
        excludedSourceVerification: ['Cedar Management Group'],
      },
    });
    assert.equal(
      memory.operatorLearnings.keyrenter_status,
      'existing_relationship_nurture'
    );
    assert.equal(
      memory.operatorLearnings.cedar_status,
      'source_verification_required'
    );
  });

  it('resolveSubjectLines returns only the tested winner when present', () => {
    const ctx = buildCampaignSynthesisContext({
      context: { businessName: 'Anchor Cleaning' },
    });
    const resolved = resolveSubjectLines(ctx);
    assert.equal(resolved.usedTestedWinner, true);
    assert.deepEqual(resolved.subjectOptions, [
      'Anchor - commercial cleaning',
    ]);
  });

  it('resolveSenderVoiceLine defaults to company voice + {{town}}', () => {
    const ctx = buildCampaignSynthesisContext({
      context: { businessName: 'Anchor Cleaning' },
    });
    const voice = resolveSenderVoiceLine(ctx, 'property managers');
    assert.equal(voice.usesFirstPerson, false);
    assert.match(voice.opener, /^Anchor helps /);
    assert.match(voice.opener, /\{\{town\}\}/);
    assert.doesNotMatch(voice.opener, /\bI work with\b/);
  });

  it('rejectsStreetAddressPersonalization flags address recommendations only', () => {
    assert.equal(
      rejectsStreetAddressPersonalization('Reference 12 North Street'),
      true
    );
    assert.equal(
      rejectsStreetAddressPersonalization(
        'Use the street address on the listing'
      ),
      true
    );
    assert.equal(
      rejectsStreetAddressPersonalization(
        'never a street address by default — use {{town}}'
      ),
      false
    );
  });

  it('findCampaignMemoryDraftConflicts detects generic subjects and Keyrenter', () => {
    const ctx = buildCampaignSynthesisContext({
      context: { businessName: 'Anchor Cleaning' },
    });
    const conflicts = findCampaignMemoryDraftConflicts(
      {
        businessName: 'Anchor',
        subjectOptions: [
          'Quick question about cleaning reliability in Greater Manchester',
        ],
        firstTouchBody: 'I work with PMs across Bedford, Hooksett, Londonderry, Auburn',
        personalizationByProspect: [
          {
            companyName: 'Keyrenter New England',
            personalizationNote: 'Reference 9 Main Street',
          },
        ],
        batchProspects: ['Keyrenter New England'],
      },
      ctx
    );
    assert.ok(conflicts.includes('missing_tested_subject_line'));
    assert.ok(conflicts.includes('generic_subject_options_with_tested_winner'));
    assert.ok(conflicts.includes('full_town_list_in_body'));
    assert.ok(conflicts.includes('first_person_work_with'));
    assert.ok(conflicts.includes('street_address_personalization'));
    assert.ok(conflicts.includes('keyrenter_in_cold_batch'));
  });
});
