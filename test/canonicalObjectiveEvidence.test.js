'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../packages/acquisition-mission');
const { resolveCanonicalObjective } = require('../packages/max/workspace/ResolvedObjective');
const {
  extractCanonicalObjectiveEvidence,
} = require('../packages/max/workspace/CanonicalObjectiveEvidence');
const { resetAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');

const ANCHOR_OBJECTIVE =
  'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.';

const BABRUN_CAMPAIGN_GOAL =
  'Book discovery calls with founder-led small business owners in the United States for Babrun\'s 12-week business transformation program.';

describe('Canonical objective evidence (tenant-general)', () => {
  it('extracts approved Blueprint campaignGoals with provenance', () => {
    const evidence = extractCanonicalObjectiveEvidence({
      clientId: 13,
      summary: {
        approved: true,
        blueprintId: 'bp-babrun-1',
        campaignGoals: BABRUN_CAMPAIGN_GOAL,
      },
    });
    assert.ok(evidence);
    assert.equal(evidence.source, 'approved_blueprint_campaign_goals');
    assert.equal(evidence.field, 'campaignGoals');
    assert.match(evidence.text, /Book discovery calls/i);
  });

  it('resolves tenant 13 objective from canonical Blueprint evidence', () => {
    const resolved = resolveCanonicalObjective({
      question: '',
      targetSegment: 'Small Business Owners',
      context: {
        tenantId: '13',
        clientId: 13,
        summary: {
          approved: true,
          blueprintId: 'bp-babrun-1',
          campaignGoals: BABRUN_CAMPAIGN_GOAL,
          geography: 'United States',
        },
      },
    });
    assert.match(resolved.objective, /Book discovery calls/i);
    assert.equal(resolved.objectiveProvenance.source, 'approved_blueprint_campaign_goals');
    assert.equal(resolved.objectiveProvenance.validationState, 'approved');
    assert.equal(resolved.geography.region, 'United States');
    assert.equal(resolved.ready, true);
  });

  it('fails closed when canonical evidence is missing', () => {
    const resolved = resolveCanonicalObjective({
      question: '',
      context: { tenantId: '13', clientId: 13, summary: { approved: true } },
    });
    assert.equal(resolved.objective, '');
    assert.equal(resolved.ready, false);
    assert.equal(resolved.ambiguities[0].field, 'objective');
  });

  it('preserves Anchor operator-style objective resolution unchanged', () => {
    const resolved = resolveCanonicalObjective({ question: ANCHOR_OBJECTIVE });
    assert.match(resolved.objective, /Acquire recurring commercial cleaning/i);
    assert.equal(resolved.objectiveProvenance, null);
  });

  it('does not cross tenant operator objectives', () => {
    const evidence = extractCanonicalObjectiveEvidence({
      clientId: 13,
      operatorObjectives: [
        {
          id: 'obj-anchor',
          scope: 'client',
          clientId: 10,
          status: 'active',
          objectiveText: ANCHOR_OBJECTIVE,
          updatedAt: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'obj-babrun',
          scope: 'client',
          clientId: 13,
          status: 'active',
          objectiveText: BABRUN_CAMPAIGN_GOAL,
          updatedAt: '2026-01-02T00:00:00.000Z',
        },
      ],
    });
    assert.equal(evidence.sourceId, 'obj-babrun');
    assert.match(evidence.text, /Book discovery calls/i);
  });

  it('creates mission plan from canonical evidence without hard-coded Babrun objective', () => {
    resetAcquisitionMissionRuntime();
    const runtime = require('../services/acquisitionMissionRuntime').getAcquisitionMissionRuntime({
      production: false,
      persist: false,
    });
    const resolved = resolveCanonicalObjective({
      question: '',
      context: {
        tenantId: '13',
        clientId: 13,
        summary: {
          approved: true,
          campaignGoals: BABRUN_CAMPAIGN_GOAL,
          geography: 'United States',
        },
      },
    });
    const mission = runtime.engine().create({
      tenantId: '13',
      clientId: 13,
      objective: resolved.objective,
      resolvedObjective: resolved,
      targetSegment: 'Small Business Owners',
      createdBy: 'test',
    });
    assert.match(mission.objective, /Book discovery calls/i);
    assert.ok(mission.missionPlanDraft || mission.resolvedObjective);
  });

  it('rejects empty resolvedObjective shell during planning', () => {
    assert.throws(
      () =>
        amo.createMission({
          tenantId: '13',
          clientId: 13,
          objective: BABRUN_CAMPAIGN_GOAL,
          resolvedObjective: { objective: '', ready: false },
        }),
      /Objective is required|ResolvedObjective\.objective is required/
    );
  });
});
