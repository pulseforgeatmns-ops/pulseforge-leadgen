'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  canonicalOutboundEmailIneligibilityReason,
  isCanonicallyOutboundEligible,
  isContaminatedEmailDomain,
  isInferredPatternProvenance,
  isObservedEmailProvenance,
  isReadPathProvenanceLabel,
  isSendableVerifiedCandidate,
  isAllowedObservedWebsiteEmail,
  resolveEmailProvenanceSource,
  planTaintedCrmEmailRemediation,
  TAINTED_EMAIL_ACTIONS,
  resolveOfficialEnrichmentDomain,
  classifyCompanyUrl,
} = require('../utils/canonicalEmailEligibility');
const { isProjectableCrmProspect } = require('../packages/max/workspace/MissionBoundCrmResolver');

function verifiedRow(overrides = {}) {
  return {
    email: 'partner@harborlaw.com',
    email_verified: true,
    email_status: 'valid',
    do_not_contact: false,
    ...overrides,
  };
}

describe('canonicalEmailEligibility', () => {
  it('rejects contaminated social-domain emails such as michael@linkedin.com', () => {
    const row = verifiedRow({
      email: 'michael@linkedin.com',
      verificationSource: 'pattern_first',
    });
    assert.equal(isContaminatedEmailDomain('linkedin.com'), true);
    assert.equal(canonicalOutboundEmailIneligibilityReason(row), 'contaminated_email_domain');
    assert.equal(isProjectableCrmProspect(row), false);
  });

  it('rejects inferred pattern_first addresses even when Bouncer reports valid', () => {
    const row = verifiedRow({
      email: 'peter@solomonlawfirm.com',
      enrichment_provenance: {
        email: { source: 'pattern_first', verifier: 'bouncer', status: 'valid' },
      },
    });
    assert.equal(isInferredPatternProvenance('pattern_first'), true);
    assert.equal(canonicalOutboundEmailIneligibilityReason(row), 'inferred_pattern_provenance');
    assert.equal(isProjectableCrmProspect(row), false);
    assert.equal(isSendableVerifiedCandidate({
      email: 'peter@solomonlawfirm.com',
      verified: true,
      source: 'pattern_first',
    }), false);
  });

  it('keeps observed Hunter email + Bouncer valid eligible', () => {
    const row = verifiedRow({
      email: 'aklug@kluglawoffices.com',
      verificationSource: 'hunter',
    });
    assert.equal(isObservedEmailProvenance('hunter'), true);
    assert.equal(isCanonicallyOutboundEligible(row), true);
    assert.equal(isProjectableCrmProspect(row), true);
  });

  it('keeps observed existing CRM email + valid verification eligible', () => {
    const row = verifiedRow({
      email: 'jmeyer@backusmeyer.com',
      verificationSource: 'existing_crm',
    });
    assert.equal(isCanonicallyOutboundEligible(row), true);
    assert.equal(isProjectableCrmProspect(row), true);
  });

  it('does not let existing_crm overwrite stored pattern_first provenance', () => {
    const row = verifiedRow({
      email: 'peter@solomonlawfirm.com',
      verificationSource: 'existing_crm',
      enrichment_provenance: {
        email: { source: 'pattern_first', original_source: 'pattern_first', verifier: 'bouncer', status: 'valid' },
      },
    });
    assert.equal(isReadPathProvenanceLabel('existing_crm'), true);
    assert.equal(resolveEmailProvenanceSource(row), 'pattern_first');
    assert.equal(canonicalOutboundEmailIneligibilityReason(row), 'inferred_pattern_provenance');
    assert.equal(isProjectableCrmProspect(row), false);
    assert.equal(isCanonicallyOutboundEligible({
      ...row,
      verificationSource: 'existing_crm',
    }), false);
  });

  it('plans preserve-not-delete for pattern_first and invalidate for social-domain contamination', () => {
    assert.deepEqual(planTaintedCrmEmailRemediation(verifiedRow({
      email: 'peter@solomonlawfirm.com',
      enrichment_provenance: { email: { source: 'pattern_first' } },
    })), {
      action: TAINTED_EMAIL_ACTIONS.PRESERVE_UNTRUSTED_PROVENANCE,
      reason: 'inferred_pattern_provenance',
      email: 'peter@solomonlawfirm.com',
      provenance: 'pattern_first',
    });
    assert.equal(planTaintedCrmEmailRemediation(verifiedRow({
      email: 'michael@linkedin.com',
      verificationSource: 'existing_crm',
    })).action, TAINTED_EMAIL_ACTIONS.INVALIDATE_CONTAMINATED);
    assert.equal(planTaintedCrmEmailRemediation(verifiedRow({
      email: 'jmeyer@backusmeyer.com',
      verificationSource: 'existing_crm',
    })).action, TAINTED_EMAIL_ACTIONS.NONE);
  });

  it('keeps observed personal-provider email from a company page eligible when verified', () => {
    const row = verifiedRow({
      email: 'owner@gmail.com',
      enrichment_provenance: {
        email: { source: 'website_email', verifier: 'bouncer', status: 'valid' },
      },
    });
    assert.equal(isAllowedObservedWebsiteEmail('owner@gmail.com', 'examplelaw.com'), true);
    assert.equal(isCanonicallyOutboundEligible(row), true);
  });

  it('treats DNC as an absolute exclusion', () => {
    const row = verifiedRow({
      email: 'aklug@kluglawoffices.com',
      verificationSource: 'hunter',
      do_not_contact: true,
    });
    assert.equal(canonicalOutboundEmailIneligibilityReason(row), 'do_not_contact');
    assert.equal(isProjectableCrmProspect(row), false);
  });

  it('prefers official company domain over LinkedIn profile URL contamination', () => {
    assert.equal(
      resolveOfficialEnrichmentDomain({
        website_url: 'https://www.linkedin.com/in/michael-stlouis/',
        domain: 'lawofficeofmichaelstlouis.com',
      }),
      'lawofficeofmichaelstlouis.com'
    );
    assert.equal(classifyCompanyUrl('https://www.linkedin.com/company/example'), 'social_profile');
  });

  it('still resolves hosted builder domains when no official domain exists', () => {
    assert.equal(
      resolveOfficialEnrichmentDomain({
        domain: 'lawofficeofmichaelstlouis.com',
        website_url: 'https://lawofficeofmichaelstlouis-com.webnode.page/',
      }),
      'lawofficeofmichaelstlouis.com'
    );
    assert.equal(
      resolveOfficialEnrichmentDomain({
        website_url: 'https://lawofficeofmichaelstlouis-com.webnode.page/',
      }),
      'lawofficeofmichaelstlouis-com.webnode.page'
    );
  });
});
