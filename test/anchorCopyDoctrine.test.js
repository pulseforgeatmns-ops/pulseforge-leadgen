'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  PERSONALIZATION_STATUS,
  FACT_TYPES,
} = require('../utils/scoutPersonalizationEvidence');
const {
  buildAnchorCopy,
  buildDefaultColdEmail,
  buildFirstFollowUpEmail,
  buildProposalFollowUpEmail,
  buildPriceConcernReply,
  buildComparisonQuoteReply,
  buildForwardableBlurb,
  buildEvidenceEnhancedEmail,
  validateAnchorCopyDoctrine,
  validateAnchorSocialCopy,
  buildAnchorCopyDoctrineViolationError,
  LIFECYCLE_STAGES,
  SEGMENTS,
} = require('../utils/anchorCopyDoctrine');

describe('Anchor Copy Doctrine', () => {
  it('default cold email matches doctrine example shape', () => {
    const copy = buildDefaultColdEmail({
      firstName: 'Sarah',
      companyName: 'Harbor Law Group',
      senderName: 'Jacob Maynard',
      serviceArea: 'Manchester',
    });
    assert.equal(copy.subject, 'Cleaning for Harbor Law Group');
    assert.match(copy.body, /I'm Jacob Maynard with Anchor Cleaning/i);
    assert.match(copy.body, /vague on what actually gets done/i);
    assert.match(copy.body, /Want me to send over what we'd need to price the office properly/i);
    assert.doesNotMatch(copy.body, /—/);
    assert.doesNotMatch(copy.body, /I saw /i);
  });

  it('first follow-up matches doctrine example', () => {
    const copy = buildFirstFollowUpEmail({
      firstName: 'Sarah',
      companyName: 'Harbor Law Group',
    });
    assert.equal(copy.subject, 'Cleaning for Harbor Law Group');
    assert.match(copy.body, /Following up on cleaning for Harbor Law Group/i);
    assert.match(copy.body, /Want me to send over what we'd need for a quote/i);
  });

  it('evidence lane uses problem bridge, not I saw observation', () => {
    const copy = buildEvidenceEnhancedEmail({
      firstName: 'Sarah',
      companyName: 'Harbor Law Group',
      evidence: {
        personalization_status: PERSONALIZATION_STATUS.SUPPORTED,
        observed_fact: 'your firm lists multiple locations in Manchester and Bedford',
        fact_type: FACT_TYPES.MULTI_LOCATION,
      },
    });
    assert.equal(copy.usedPersonalization, true);
    assert.match(copy.body, /multiple locations/i);
    assert.doesNotMatch(copy.body, /I saw/i);
    assert.doesNotMatch(copy.body, /I observed/i);
    const validation = validateAnchorCopyDoctrine(copy);
    assert.equal(validation.ok, true, JSON.stringify(validation.violations));
  });

  it('property management cold uses segment-specific close', () => {
    const copy = buildAnchorCopy({
      lifecycleStage: LIFECYCLE_STAGES.COLD,
      companyName: 'Blue Door Living Property Management',
      segment: SEGMENTS.PROPERTY_MANAGEMENT,
    });
    assert.match(copy.body, /Property managers often need recurring cleaning/i);
    assert.match(copy.cta, /one property first/i);
  });

  it('proposal follow-up and price concern match doctrine', () => {
    const proposal = buildProposalFollowUpEmail({ firstName: 'Sarah' });
    assert.match(proposal.body, /frequency fits how the space is actually used/i);

    const price = buildPriceConcernReply({ firstName: 'Sarah' });
    assert.match(price.body, /above the budget you had in mind/i);
  });

  it('comparison quote reply names specific next action', () => {
    const copy = buildComparisonQuoteReply({
      firstName: 'Sarah',
      competitorScopeSummary: 'lobby and restrooms twice weekly',
      anchorScopeSummary: 'lobby, restrooms, break room, and private offices',
    });
    assert.match(copy.body, /prices for different work/i);
    assert.match(copy.cta, /compare the same areas side by side/i);
  });

  it('forwardable blurb is standalone and specific', () => {
    const copy = buildForwardableBlurb({ serviceArea: 'Manchester' });
    assert.match(copy.body, /short version you can forward/i);
    assert.match(copy.body, /what the price actually covers/i);
  });

  it('rejects AI tells, generic closers, and em dashes', () => {
    const bad = validateAnchorCopyDoctrine({
      subject: 'Cleaning',
      body: 'Hope this finds you well. Not just price, but scope — worth a quick conversation?',
      cta: 'Would you be open to a quick call?',
    });
    assert.equal(bad.ok, false);
    assert.ok(bad.violations.some((v) => v.patternId === 'em_dash'));
    assert.ok(bad.violations.some((v) => v.patternId === 'hope_this_finds'));
    assert.ok(bad.violations.some((v) => v.patternId === 'open_to_call'));
  });

  it('accepts doctrine-compliant default cold copy', () => {
    const copy = buildDefaultColdEmail({
      firstName: 'Sarah',
      companyName: 'Harbor Law Group',
    });
    const validation = validateAnchorCopyDoctrine(copy);
    assert.equal(validation.ok, true, JSON.stringify(validation.violations));
  });

  it('validateAnchorSocialCopy composes doctrine and social-only rules', () => {
    const doctrineOnly = validateAnchorSocialCopy('I wanted to reach out about cleaning.');
    assert.equal(doctrineOnly.ok, false);
    assert.ok(doctrineOnly.violations.some((v) => v.source === 'anchor_copy_doctrine' && v.patternId === 'wanted_to_reach_out'));

    const socialOnly = validateAnchorSocialCopy('Book a walkthrough for the office.');
    assert.equal(socialOnly.ok, false);
    assert.ok(socialOnly.violations.some((v) => v.source === 'anchor_social_rule' && v.patternId === 'walkthrough'));

    const compliant = validateAnchorSocialCopy('A facilities assessment gives Manchester office managers a written scope before cleaning starts.');
    assert.equal(compliant.ok, true, JSON.stringify(compliant.violations));
  });

  it('buildAnchorCopyDoctrineViolationError exposes code and violations', () => {
    const violations = [{ source: 'anchor_copy_doctrine', patternId: 'em_dash', match: '—' }];
    const err = buildAnchorCopyDoctrineViolationError(violations);
    assert.equal(err.message, 'anchor_copy_doctrine_violation');
    assert.equal(err.code, 'anchor_copy_doctrine_violation');
    assert.deepEqual(err.violations, violations);
  });
});
