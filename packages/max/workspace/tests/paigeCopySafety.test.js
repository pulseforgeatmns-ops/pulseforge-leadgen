'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  BLOCKER,
  validateCustomerFacingCopy,
  validatePaigeVariantCopy,
  validatePaigeVariantsPayload,
} = require('../PaigeCopySafety');
const {
  buildPerProspectVariants,
  runPaigeVariants,
} = require('../PaigeVariantsExecutor');
const { resolveQueueSendability } = require('../../../emmett-outbound/Queue');

const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';

function strScenario() {
  return {
    max: {
      rankedTargets: [{
        id: PLACE_BLUE,
        companyId: PLACE_BLUE,
        placeId: PLACE_BLUE,
        name: 'Blue Door Living Property Management',
        fit: 0.28,
        timing: 0.2,
        rationale: 'fit 0.28 · timing 0.20 · 6 unknowns',
      }],
      objectives: [{ text: 'Achieve 1 recurring_clients.' }],
      recommendations: ['Prioritize Blue Door Living as the first acquisition focus.'],
    },
    plan: {
      market: { label: 'Short-term rental operators', segment: 'str' },
      geography: { label: 'Manchester' },
    },
    mission: {
      tenantId: '10',
      clientId: 10,
      targetSegment: 'Short-term rental operators',
      objective: 'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester area.',
    },
  };
}

describe('Paige customer-facing copy safety', () => {
  it('rejects Mission focus: Achieve 1 recurring_clients', () => {
    const result = validateCustomerFacingCopy('Mission focus: Achieve 1 recurring_clients');
    assert.equal(result.safe, false);
    assert.equal(result.blocker, BLOCKER);
  });

  it('rejects fit 0.28 · timing 0.20 · 6 unknowns score tuples', () => {
    const result = validateCustomerFacingCopy('Why now: fit 0.28 · timing 0.20 · 6 unknowns');
    assert.equal(result.safe, false);
    assert.equal(result.blocker, BLOCKER);
  });

  it('rejects verbatim Max recommendation prioritization language', () => {
    const result = validateCustomerFacingCopy(
      'Why now: Prioritize Blue Door Living as the first acquisition focus.'
    );
    assert.equal(result.safe, false);
    assert.equal(result.blocker, BLOCKER);
  });

  it('accepts safe customer-facing Anchor STR copy', () => {
    const scenario = strScenario();
    const variants = buildPerProspectVariants({ ...scenario, clientId: 10 });
    assert.equal(variants.length, 1);
    const safety = validatePaigeVariantCopy(variants[0]);
    assert.equal(safety.safe, true, JSON.stringify(safety.violations));
    assert.ok(variants[0].subject.includes('Blue Door Living Property Management'));
    assert.ok(variants[0].body);
    assert.ok(variants[0].cta);
    assert.match(variants[0].body, /Property managers often need recurring cleaning|Managed properties often need recurring cleaning/i);
    assert.match(variants[0].cta, /quote on one property first|price the office properly/i);
    assert.doesNotMatch(variants[0].body, /Mission focus/i);
    assert.doesNotMatch(variants[0].body, /recurring_clients/i);
    assert.doesNotMatch(variants[0].body, /fit 0\./i);
    assert.doesNotMatch(variants[0].body, /I saw /i);
  });

  it('runPaigeVariants returns SUCCESS for safe Anchor revision payload', async () => {
    const scenario = strScenario();
    const result = await runPaigeVariants({
      mission: scenario.mission,
      missionPlan: scenario.plan,
      workspaceContext: {
        max: scenario.max,
        scout: scenario.scout || {},
      },
      transactionId: 'tx-paige-safe',
    });

    assert.equal(result.status, 'SUCCESS');
    assert.equal(Array.isArray(result.contributions?.variants), true);
    assert.equal(result.contributions.variants.length, 1);
    assert.equal(result.contributions.variants[0].candidateId, PLACE_BLUE);
    assert.equal(result.contributions.variants[0].placeId, PLACE_BLUE);
    assert.equal(result.contributions.variants[0].companyId, PLACE_BLUE);
    assert.match(result.contributions.variants[0].cta, /Want me to send over/i);
  });

  it('unsafe Paige variant cannot become sendable CAPACITY queue item', () => {
    const unsafe = resolveQueueSendability({
      email: 'sales@example.com',
      contentSource: 'paige',
      paige: {
        author: 'paige',
        source: 'paige',
        subject: 'Hello',
        body: 'Mission focus: Achieve 1 recurring_clients',
      },
    });
    assert.equal(unsafe.sendable, false);
    assert.equal(unsafe.sendBlocker, BLOCKER);

    const safe = resolveQueueSendability({
      email: 'sales@example.com',
      contentSource: 'paige',
      paige: buildPerProspectVariants(strScenario())[0],
    });
    assert.equal(safe.sendable, true);
    assert.equal(safe.sendBlocker, null);
  });

  it('preserves candidate binding on regenerated variants', () => {
    const variants = buildPerProspectVariants(strScenario());
    assert.equal(variants[0].candidateId, PLACE_BLUE);
    assert.equal(variants[0].placeId, PLACE_BLUE);
    assert.equal(variants[0].companyId, PLACE_BLUE);
    assert.ok(variants[0].attributableIntelligence?.rationale);
    assert.equal(
      variants[0].body.includes(variants[0].attributableIntelligence.rationale),
      false,
      'internal rationale must not appear in customer body'
    );
  });

  it('permits unsafe-looking internal metadata when rendered copy is safe', () => {
    const payload = {
      variants: [{
        candidateId: PLACE_BLUE,
        subject: 'Cleaning for Blue Door Living Property Management',
        body: 'Want me to send over what we\'d need for a quote on one property first?',
        cta: 'Want me to send over what we\'d need for a quote on one property first?',
        attributableIntelligence: {
          rationale: 'Mission focus: Achieve 1 recurring_clients',
          objectiveReason: 'fit 0.28 · timing 0.20 · 6 unknowns',
        },
      }],
      messaging: 'Mission focus: Achieve 1 recurring_clients',
      subjects: ['Mission focus: Achieve 1 recurring_clients'],
      cta: 'Mission focus: Achieve 1 recurring_clients',
    };

    const result = validatePaigeVariantsPayload(payload);
    assert.equal(result.safe, true);
    assert.equal(result.blocker, null);
  });

  it('blocks internal metadata when it is rendered into customer-facing body copy', () => {
    const rationale = 'Mission focus: Achieve 1 recurring_clients';
    const payload = {
      variants: [{
        candidateId: PLACE_BLUE,
        subject: 'Cleaning for Blue Door Living Property Management',
        body: `Would it be useful for us to put a quote together for Blue Door Living Property Management?\n\n${rationale}`,
        cta: 'Reply if a written quote would be useful',
        attributableIntelligence: { rationale },
      }],
    };

    const result = validatePaigeVariantsPayload(payload);
    assert.equal(result.safe, false);
    assert.equal(result.blocker, BLOCKER);
    assert.equal(result.violations.length, 1);
    assert.equal(result.violations[0].candidateId, PLACE_BLUE);
  });

  it('validatePaigeVariantsPayload fails closed on any unsafe variant', () => {
    const payload = {
      variants: [{
        candidateId: 'x',
        subject: 'Hi',
        body: 'Mission focus: Achieve 1 recurring_clients',
        cta: 'Reply',
      }],
      subjects: ['Hi'],
      cta: 'Reply',
    };
    const result = validatePaigeVariantsPayload(payload);
    assert.equal(result.safe, false);
    assert.equal(result.blocker, BLOCKER);
  });
});
