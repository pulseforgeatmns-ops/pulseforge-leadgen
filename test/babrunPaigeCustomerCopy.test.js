'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildCustomerFacingVariantCopy } = require('../packages/max/workspace/PaigeCustomerCopy');
const { buildPerProspectVariants, runPaigeVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const {
  buildBabrunOutboundCopy,
  assertNoAnchorCopyMarkers,
} = require('../utils/babrunOutboundCopy');

const BABRUN_OBJECTIVE =
  'Over the next 90 days, prove that Babrun can reliably acquire the right founder customers for the 12-week program.';

const ANCHOR_PLAN = {
  market: { segment: 'str', label: 'Short-term rental operators' },
  geography: { label: 'Greater Manchester', cities: ['Manchester'] },
  brandName: 'Anchor Cleaning',
  senderName: 'Jacob Maynard',
  senderEmail: 'jacob@goanchorcleaning.com',
};

const BABRUN_MISSION = {
  tenantId: '13',
  clientId: 13,
  objective: BABRUN_OBJECTIVE,
  structuredMission: {
    market: { segment: 'small_business_owners', label: 'Small Business Owners' },
    geography: { region: 'United States', scope: 'nationwide', cities: [] },
    objective: BABRUN_OBJECTIVE,
  },
};

const BABRUN_PLAN = {
  objective: BABRUN_OBJECTIVE,
  senderName: 'Fedir',
  brandName: 'Babrun',
  senderEmail: 'hello@babrun.com',
  market: { segment: 'small_business_owners', label: 'Small Business Owners' },
  geography: { region: 'United States' },
};

function assertBabrunCopy(copy) {
  const combined = `${copy.subject}\n${copy.body}\n${copy.cta || ''}`;
  assert.doesNotMatch(combined, /commercial cleaning/i);
  assert.doesNotMatch(combined, /Anchor Cleaning/i);
  assert.doesNotMatch(combined, /Greater Manchester/i);
  assert.doesNotMatch(combined, /short-term rental/i);
  assert.doesNotMatch(combined, /facility assessment/i);
  assert.doesNotMatch(combined, /goanchorcleaning\.com/i);
  assert.doesNotMatch(combined, /jacob@goanchorcleaning\.com/i);
  assert.match(combined, /12-week/i);
  assert.match(combined, /founder/i);
  assert.match(combined, /Fedir/i);
  assert.match(combined, /Babrun/i);
  assert.deepEqual(assertNoAnchorCopyMarkers(combined), []);
}

test('tenant 13 Babrun canonical copy rejects Anchor/generic commercial-cleaning template', () => {
  const copy = buildCustomerFacingVariantCopy({
    companyName: "Barco's Painting of Colorado",
    plan: BABRUN_PLAN,
    mission: BABRUN_MISSION,
  });
  assertBabrunCopy(copy);
  assert.match(copy.subject, /Barco's Painting of Colorado/);
});

test('tenant 13 generation ignores tenant-10 Anchor plan markers when mission is Babrun', () => {
  const copy = buildCustomerFacingVariantCopy({
    companyName: "Barco's Painting of Colorado",
    plan: { ...ANCHOR_PLAN, ...BABRUN_PLAN },
    mission: BABRUN_MISSION,
  });
  assertBabrunCopy(copy);
});

test('Paige per-prospect variants for tenant 13 cohort inventory use Babrun offer context', async () => {
  const variants = buildPerProspectVariants({
    mission: BABRUN_MISSION,
    clientId: 13,
    plan: BABRUN_PLAN,
    max: {
      priorities: [{
        candidateId: '650a385b-434b-4db1-86cb-2f964146f43e',
        companyId: '650a385b-434b-4db1-86cb-2f964146f43e',
        prospectId: '7fdbead1-f158-4ab0-bf5e-94178275cb76',
        name: "Barco's Painting of Colorado",
      }],
    },
  });
  assert.equal(variants.length, 1);
  assertBabrunCopy(variants[0]);
});

test('runPaigeVariants against production-shaped Babrun mission never emits commercial cleaning', async () => {
  const result = await runPaigeVariants({
    mission: BABRUN_MISSION,
    missionPlan: BABRUN_PLAN,
    workspaceContext: {
      max: {
        priorities: [{
          candidateId: '650a385b-434b-4db1-86cb-2f964146f43e',
          companyId: '650a385b-434b-4db1-86cb-2f964146f43e',
          name: "Barco's Painting of Colorado",
        }],
      },
      scout: { source: 'governed_clean_inventory', qualifiedCount: 1 },
    },
  });
  assert.equal(result.status, 'SUCCESS');
  assertBabrunCopy(result.contributions.variants[0]);
});

test('buildBabrunOutboundCopy helper stays tenant-scoped', () => {
  const copy = buildBabrunOutboundCopy({
    companyName: 'Ventura Landscape',
    plan: BABRUN_PLAN,
    mission: BABRUN_MISSION,
  });
  assertBabrunCopy(copy);
});
