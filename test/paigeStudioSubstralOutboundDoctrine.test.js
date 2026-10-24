'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildStudioSubstralFirstTouchEmail,
  validateStudioSubstralFirstTouchDoctrine,
  evaluateRelationshipAccountGuard,
  humanizeWebsiteObservation,
  RELATIONSHIP_HOLD_REASON,
  DOCTRINE_BLOCKER,
} = require('../utils/paigeStudioSubstralOutboundDoctrine');
const { buildPaigeFirstTouchDoctrineContext } = require('../utils/paigeStudioSubstralOutboundDoctrine');
const { buildStudioSubstralFirstTouchVariant } = require('../utils/studioSubstralFirstTouchEmail');
const { buildPerProspectVariants, runPaigeVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const wave001Golden = require('./fixtures/studioSubstralWave001Golden');

describe('SPEC-PAIGE-SUBSTRAL-001 — humanized first-touch doctrine', () => {
  it('humanizes technical audit findings into operator language', () => {
    const viewport = humanizeWebsiteObservation('Mobile friction — missing viewport meta tag');
    assert.match(viewport.observation, /mobile experience could be working harder/i);

    const reviews = humanizeWebsiteObservation(
      'Weak trust proof — google review strength is not carried through on the homepage'
    );
    assert.match(reviews.observation, /strong reputation/i);

    const title = humanizeWebsiteObservation('Weak first-impression credibility — missing or empty page title');
    assert.match(title.observation, /search and browser tabs/i);

    const load = humanizeWebsiteObservation('Mobile friction — homepage fetch took 8s during audit');
    assert.match(load.observation, /taking a while to load/i);

    const nav = humanizeWebsiteObservation(
      'Unclear homepage CTA — primary navigation does not include a Contact path'
    );
    assert.match(nav.observation, /clearest next step from the homepage/i);
  });

  it('Wave 001 golden fixtures pass doctrine validation when drafted', () => {
    for (const fixture of wave001Golden) {
      const draft = buildStudioSubstralFirstTouchEmail({
        companyName: fixture.company,
        specificIssue: fixture.specificIssue,
        segment: 'professional services',
      });
      assert.equal(draft.held, false, fixture.company);
      const combined = `${draft.body}\n${draft.cta}`;
      for (const re of fixture.mustMatch) {
        assert.match(combined, re, `${fixture.company}: ${re}`);
      }
      for (const re of fixture.mustNotMatch) {
        assert.doesNotMatch(combined, re, `${fixture.company}: ${re}`);
      }
      const validation = validateStudioSubstralFirstTouchDoctrine({
        subject: draft.subject,
        body: draft.body,
        cta: draft.cta,
      });
      assert.equal(validation.ok, true, JSON.stringify(validation.violations));
    }
  });

  it('regression: no em dash, no I noticed, no credibility gap or conversion path labels', () => {
    const draft = buildStudioSubstralFirstTouchEmail({
      companyName: 'Example Roofing',
      specificIssue: 'Weak trust proof — google review strength is not carried through on the homepage',
    });
    assert.doesNotMatch(draft.body, /—/);
    assert.doesNotMatch(draft.body, /\bI noticed\b/i);
    assert.doesNotMatch(draft.body, /credibility gap/i);
    assert.doesNotMatch(draft.body, /conversion path/i);
    assert.match(draft.body, /I was looking at your site and saw/i);
    assert.match(draft.body, /Happy to send over a quick assessment/i);
  });

  it('Keyrenter relationship account is held, not drafted for cold send', () => {
    const guard = evaluateRelationshipAccountGuard({
      companyName: 'Keyrenter New England Property Management',
      website: 'https://www.keyrenter-newengland.com',
    });
    assert.equal(guard.held, true);
    assert.equal(guard.reason, RELATIONSHIP_HOLD_REASON);

    const draft = buildStudioSubstralFirstTouchEmail({
      companyName: 'Keyrenter New England Property Management',
      specificIssue: 'Unclear homepage CTA — primary navigation does not include a Contact path',
    });
    assert.equal(draft.held, true);
    assert.equal(draft.holdReason, RELATIONSHIP_HOLD_REASON);
    assert.equal(draft.body, null);
  });

  it('Paige first-touch doctrine context is injected for operator/LLM prompts', () => {
    const ctx = buildPaigeFirstTouchDoctrineContext();
    assert.equal(ctx.spec, 'SPEC-PAIGE-SUBSTRAL-001');
    assert.equal(ctx.human_review_required, true);
    assert.ok(ctx.approved_cta_examples.length >= 3);
    assert.ok(ctx.relationship_account_guard.some((g) => g.id === 'keyrenter_anchor'));
  });

  it('buildPerProspectVariants uses Substral doctrine for tenant 17 and holds Keyrenter', () => {
    const variants = buildPerProspectVariants({
      clientId: 17,
      max: {
        rankedTargets: [
          {
            id: 'nick-tracey',
            name: 'Nick Tracey Roofing & Exteriors',
            specific_website_issue:
              'Weak trust proof — google review strength is not carried through on the homepage',
          },
          {
            id: 'keyrenter',
            name: 'Keyrenter New England Property Management',
            website: 'keyrenter-newengland.com',
            specific_website_issue:
              'Unclear homepage CTA — primary navigation does not include a Contact path',
          },
        ],
      },
      plan: { senderName: 'Jacob' },
      mission: { clientId: 17, tenantId: '17' },
    });
    assert.equal(variants.length, 1);
    assert.match(variants[0].body, /I was looking at your site and saw/i);
    assert.match(variants[0].body, /strong reputation/i);
    assert.ok(Array.isArray(variants._heldProspects));
    assert.equal(variants._heldProspects.length, 1);
    assert.equal(variants._heldProspects[0].holdReason, RELATIONSHIP_HOLD_REASON);
  });

  it('runPaigeVariants blocks copy that violates Substral doctrine', async () => {
    const bad = validateStudioSubstralFirstTouchDoctrine({
      body: 'Hi,\n\nI noticed your credibility gap on the conversion path — book a discovery call.',
      cta: 'Book a discovery call',
    });
    assert.equal(bad.ok, false);
    assert.equal(bad.blocker, DOCTRINE_BLOCKER);

    const goodVariant = buildStudioSubstralFirstTouchVariant({
      candidate: {
        name: 'Heritage Home Service',
        specific_website_issue:
          'Unclear homepage CTA — no obvious phone, email, form, or contact link on the homepage',
      },
      plan: {},
      mission: {},
    });
    assert.equal(goodVariant.held, false);

    const result = await runPaigeVariants({
      mission: { clientId: 17, tenantId: '17' },
      missionPlan: { senderName: 'Jacob' },
      workspaceContext: {
        max: {
          rankedTargets: [{
            id: 'heritage',
            name: 'Heritage Home Service',
            specific_website_issue:
              'Unclear homepage CTA — no obvious phone, email, form, or contact link on the homepage',
          }],
        },
        scout: {},
      },
      transactionId: 'tx-substral-doctrine',
    });
    assert.equal(result.status, 'SUCCESS');
    assert.match(result.contributions.variants[0].body, /clearest next step/i);
  });
});
