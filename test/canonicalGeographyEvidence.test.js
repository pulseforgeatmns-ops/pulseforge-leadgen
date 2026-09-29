'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  extractCanonicalGeographyEvidence,
  tenantGeographyChoices,
} = require('../packages/acquisition-mission/CanonicalGeographyEvidence');
const { resolveCanonicalObjective } = require('../packages/max/workspace/ResolvedObjective');
const { planMission } = require('../packages/acquisition-mission/MissionPlanner');

const ANCHOR_OBJECTIVE =
  'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.';

const BABRUN_OBJECTIVE_NO_GEO =
  'Over the next 90 days, prove that Babrun can reliably acquire the right founder customers for the 12-week program.';

const BABRUN_CAMPAIGN_GOAL =
  'Book discovery calls with founder-led small business owners in the United States for Babrun\'s 12-week business transformation program.';

describe('Canonical geography evidence (tenant-general)', () => {
  it('retains operator confirmation only for the exact approved canonical snapshot', () => {
    const context={tenantId:'13',summary:{approved:true,canonicalSnapshotId:'snapshot',geography:'United States'},blueprint:{status:'approved',canonicalSnapshotId:'snapshot',sectionProvenance:{targetMarkets:{origin:'operator_confirmation',actor_id:'operator',evidence_id:'evidence',canonical_snapshot_id:'snapshot',geography:{region:'United States'}}}}};
    const evidence=extractCanonicalGeographyEvidence(context);
    assert.equal(evidence.validationState,'operator_confirmed');
    assert.equal(evidence.operatorConfirmation.evidenceId,'evidence');
    const resolved=resolveCanonicalObjective({question:BABRUN_OBJECTIVE_NO_GEO,context});
    assert.equal(resolved.geographyProvenance.operatorConfirmation.evidenceId,'evidence');
    context.blueprint.sectionProvenance.targetMarkets.canonical_snapshot_id='stale';
    assert.equal(extractCanonicalGeographyEvidence(context).operatorConfirmation,undefined);
  });
  it('resolves United States from approved summary targetMarkets', () => {
    const evidence = extractCanonicalGeographyEvidence({
      tenantId: '13',
      clientId: 13,
      summary: {
        approved: true,
        blueprintId: 'bp-babrun',
        targetMarkets: 'United States',
        geography: 'United States',
      },
    });
    assert.ok(evidence);
    assert.equal(evidence.geography.region, 'United States');
    assert.equal(evidence.source, 'approved_blueprint_target_markets');
    assert.equal(evidence.validationState, 'approved');
  });

  it('resolves tenant 13 mission without Manchester/Charleston ambiguity choices', () => {
    const resolved = resolveCanonicalObjective({
      question: '',
      targetSegment: 'Small Business Owners',
      context: {
        tenantId: '13',
        clientId: 13,
        summary: {
          approved: true,
          blueprintId: 'bp-babrun',
          campaignGoals: BABRUN_OBJECTIVE_NO_GEO,
          targetMarkets: 'United States',
          geography: 'United States',
        },
      },
    });
    assert.equal(resolved.ready, true);
    assert.equal(resolved.geography.region, 'United States');
    assert.ok(resolved.geographyProvenance);
    assert.equal(resolved.geographyProvenance.field, 'geography');
    const geoAmb = resolved.ambiguities.find((row) => row.field === 'geography.region');
    assert.equal(geoAmb, undefined);
  });

  it('never offers Pulseforge default regions as tenant choices', () => {
    const choices = tenantGeographyChoices({ tenantId: '13', clientId: 13, summary: { approved: true } });
    assert.deepEqual(choices, []);
  });

  it('fails closed when geography evidence is missing', () => {
    const resolved = resolveCanonicalObjective({
      question: BABRUN_OBJECTIVE_NO_GEO,
      context: { tenantId: '13', clientId: 13, summary: { approved: true } },
    });
    assert.equal(resolved.ready, false);
    const geoAmb = resolved.ambiguities.find((row) => row.field === 'geography.region');
    assert.ok(geoAmb);
    assert.deepEqual(geoAmb.choices, []);
    assert.match(geoAmb.reason, /No geography/i);
  });

  it('preserves Anchor local geography from operator objective', () => {
    const resolved = resolveCanonicalObjective({ question: ANCHOR_OBJECTIVE });
    assert.equal(resolved.geography.region, 'Greater Manchester');
    assert.equal(resolved.ready, true);
  });

  it('plans nationwide scope when canonically supported', () => {
    const resolved = resolveCanonicalObjective({
      question: '',
      targetSegment: 'Small Business Owners',
      context: {
        tenantId: '13',
        clientId: 13,
        summary: {
          approved: true,
          campaignGoals: BABRUN_CAMPAIGN_GOAL,
          targetMarkets: 'United States',
        },
      },
    });
    const planned = planMission(resolved);
    assert.equal(planned.draft.geography.region, 'United States');
    assert.equal(planned.draft.geography.scope, 'nationwide');
    assert.equal(planned.readyForConfirmation, true);
  });

  it('does not hard-code Babrun — uses summary evidence only', () => {
    const evidence = extractCanonicalGeographyEvidence({
      tenantId: '99',
      clientId: 99,
      summary: { approved: true, targetMarkets: 'Oregon' },
    });
    assert.equal(evidence.geography.region, 'Oregon');
  });

  it('falls back to ICP geography when target markets absent', () => {
    const evidence = extractCanonicalGeographyEvidence({
      tenantId: '13',
      clientId: 13,
      summary: {
        approved: true,
        idealCustomersGeography: 'United States',
      },
    });
    assert.equal(evidence.geography.region, 'United States');
    assert.equal(evidence.field, 'idealCustomersGeography');
  });
});
