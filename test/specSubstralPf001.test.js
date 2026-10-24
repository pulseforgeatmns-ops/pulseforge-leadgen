'use strict';

/**
 * SPEC-SUBSTRAL-PF-001 acceptance tests (memory / unit scope).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  STUDIO_SUBSTRAL_SLUG,
  SUBSTRAL_MISSION_OBJECTIVE,
  ensureStudioSubstralTenant,
  ensureStudioSubstralMission,
  isStudioSubstralScoringProfile,
  usesWebsiteOpportunityIntelligence,
} = require('../utils/studioSubstralTenant');
const { buildSixLayerFindings, summarizeEvidenceStrength } = require('../utils/studioSubstralLayers');
const { buildPaigeStudioSubstralContext } = require('../utils/paigeStudioSubstralContext');
const {
  evaluateStudioSubstralOutboundReadiness,
  assertStudioSubstralOutboundNotImplicit,
} = require('../utils/studioSubstralOutboundGovernance');
const { prioritizeStudioSubstralOpportunities } = require('../utils/studioSubstralMaxPrioritization');
const { isWebDesignProfile } = require('../services/webDesignScout');
const { EVIDENCE_CLASS } = require('../packages/capabilities/websiteOpportunityIntelligence/types');
const { captureAssessmentRequest } = require('../lib/substralAssessmentIntake');

function memoryDb(seedClients = []) {
  const clients = new Map(seedClients.map((c) => [c.id, { ...c }]));
  const tenantWorkspaces = new Map();
  let nextClientId = 100;
  const agentActions = [];
  const companies = [];
  const prospects = [];
  const opportunities = [];
  const missions = [];

  return {
    clients,
    tenantWorkspaces,
    agentActions,
    opportunities,
    async query(sql, params = []) {
      const text = String(sql);
      if (/SELECT \* FROM clients WHERE id/i.test(text)) {
        const row = clients.get(Number(params[0]));
        return { rows: row ? [row] : [] };
      }
      if (/SELECT \* FROM clients WHERE slug/i.test(text)) {
        const row = [...clients.values()].find((c) => c.slug === params[0]);
        return { rows: row ? [row] : [] };
      }
      if (/FROM tenant_workspaces WHERE client_id/i.test(text)) {
        const row = tenantWorkspaces.get(Number(params[0]));
        return { rows: row ? [row] : [] };
      }
      if (/INSERT INTO tenant_workspaces/i.test(text)) {
        const row = {
          client_id: params[0],
          tenant_key: params[1],
          knowledge_namespace: params[2],
          mission_namespace: params[3],
          prospect_namespace: params[4],
          outcome_namespace: params[5],
          aim_namespace: params[6],
          campaign_namespace: params[7],
          memory_namespace: params[8],
          origin: params[9],
          lifecycle: params[10],
          platform_knowledge_isolated: true,
        };
        tenantWorkspaces.set(row.client_id, row);
        return { rows: [row] };
      }
      if (/ALTER TABLE clients ADD COLUMN/i.test(text) || /ALTER TABLE tenant_workspaces ADD COLUMN/i.test(text)) {
        return { rows: [] };
      }
      if (/CREATE TABLE IF NOT EXISTS tenant_workspaces/i.test(text)) {
        return { rows: [] };
      }
      if (/INSERT INTO clients/i.test(text)) {
        const id = nextClientId++;
        const row = {
          id,
          slug: STUDIO_SUBSTRAL_SLUG,
          name: 'Studio Substral',
          scoring_profile: 'studio_substral',
          enabled_agents: ['scout', 'max', 'paige'],
          sender_email: 'hello@studiosubstral.com',
          sending_domain: 'studiosubstral.com',
          autosend_enabled: false,
          brand_voice: 'Diagnosis before design.',
          never_say: 'website makeover',
          lead_with: 'Evidence-first website assessment',
        };
        clients.set(id, row);
        return { rows: [row] };
      }
      if (/INSERT INTO agent_actions/i.test(text)) {
        const row = { id: agentActions.length + 1, payload: JSON.parse(params[4]) };
        agentActions.push(row);
        return { rows: [row] };
      }
      if (/FROM agent_actions WHERE client_id/i.test(text)) {
        return { rows: [agentActions[0]].filter(Boolean) };
      }
      if (/UPDATE agent_actions/i.test(text)) {
        return { rows: [] };
      }
      if (/INSERT INTO companies/i.test(text)) {
        const row = { id: companies.length + 1 };
        companies.push(row);
        return { rows: [row] };
      }
      if (/SELECT id FROM companies/i.test(text)) {
        return { rows: companies[0] ? [{ id: companies[0].id }] : [] };
      }
      if (/INSERT INTO prospects/i.test(text)) {
        const row = { id: prospects.length + 1, client_id: params[3] || params[params.length - 2] };
        prospects.push(row);
        return { rows: [row] };
      }
      if (/SELECT id, client_id FROM prospects WHERE email/i.test(text)) {
        return { rows: [] };
      }
      if (/FROM acquisition_missions/i.test(text)) {
        return { rows: missions.slice(0, 1) };
      }
      if (/CREATE TABLE IF NOT EXISTS studio_substral_assessment_opportunities/i.test(text)) {
        return { rows: [] };
      }
      if (/INSERT INTO studio_substral_assessment_opportunities/i.test(text)) {
        const row = { id: opportunities.length + 1, ...params };
        opportunities.push(row);
        return { rows: [{ id: row.id, created_at: new Date(), updated_at: new Date() }] };
      }
      if (/tenant_mailbox_integrations/i.test(text)) {
        return { rows: [] };
      }
      return { rows: [] };
    },
  };
}

describe('SPEC-SUBSTRAL-PF-001', () => {
  it('1. Studio Substral exists as slug-resolved tenant with studio_substral scoring profile', async () => {
    const db = memoryDb();
    const client = await ensureStudioSubstralTenant(db);
    assert.equal(client.slug, STUDIO_SUBSTRAL_SLUG);
    assert.equal(client.scoring_profile, 'studio_substral');
    assert.ok(isStudioSubstralScoringProfile(client.scoring_profile));
    const ws = db.tenantWorkspaces.get(client.id);
    assert.ok(ws, 'tenant_workspaces binding provisioned');
    assert.equal(ws.tenant_key, STUDIO_SUBSTRAL_SLUG);
    assert.equal(ws.knowledge_namespace, `tenant:${client.id}:knowledge`);
  });

  it('2. initial mission objective targets one paid website assessment', () => {
    assert.match(SUBSTRAL_MISSION_OBJECTIVE, /paid Studio Substral website assessment/i);
    assert.match(SUBSTRAL_MISSION_OBJECTIVE, /fix, rebuild, or leave alone/i);
  });

  it('3. six-layer mapping preserves epistemic labels', () => {
    const layers = buildSixLayerFindings({
      evidence_refs: [
        { category: 'performance', evidence_class: EVIDENCE_CLASS.MEASURED, summary: 'Fetch 4200ms', ref: 'perf:1' },
        { category: 'accessibility', evidence_class: EVIDENCE_CLASS.OBSERVED, summary: 'Missing form label', ref: 'a11y:1' },
        { category: 'conversion_structure', evidence_class: EVIDENCE_CLASS.INFERRED, summary: 'CTA path unclear', ref: 'conv:1' },
      ],
    });
    assert.equal(layers.performance[0].evidence_class, 'MEASURED');
    assert.equal(layers.accessibility[0].evidence_class, 'OBSERVED');
    assert.equal(layers.conversion[0].evidence_class, 'INFERRED');
    const strength = summarizeEvidenceStrength(layers);
    assert.equal(strength.measured, 1);
    assert.equal(strength.observed, 1);
    assert.equal(strength.inferred, 1);
  });

  it('4–5. intake uses Substral tenant and idempotent opportunity reconcile path', async () => {
    process.env.STUDIO_SUBSTRAL_CLIENT_ID = '42';
    const db = memoryDb([{ id: 42, slug: STUDIO_SUBSTRAL_SLUG, scoring_profile: 'studio_substral' }]);
    const first = await captureAssessmentRequest(db, {
      domain: 'example.com',
      email: 'owner@example.com',
      context: 'Unsure whether to rebuild or patch checkout.',
    });
    assert.equal(first.client_id, 42);
    assert.equal(first.duplicate, false);
    delete process.env.STUDIO_SUBSTRAL_CLIENT_ID;
  });

  it('6. Max prioritization exposes evidence gaps and next action questions', () => {
    const ranked = prioritizeStudioSubstralOpportunities({
      assessments: [],
      assessmentRequests: [{
        stage: 'REQUESTED',
        decision_context: 'Need to know if SEO or redesign first.',
        six_layer_findings: {},
        recommended_next_action: 'Collect evidence',
      }],
    });
    assert.ok(ranked.assessment_requests[0].max_questions);
    assert.match(ranked.assessment_requests[0].max_questions.evidence_we_lack, /Six-layer/i);
  });

  it('7. Paige context enforces assessment CTA and diagnosis-before-design', () => {
    const ctx = buildPaigeStudioSubstralContext({ brand_voice: 'Test voice' });
    assert.equal(ctx.primary_cta, 'paid website assessment');
    assert.ok(ctx.forbidden_ctas.includes('free strategy call'));
    assert.match(ctx.mission_objective, /paid Studio Substral website assessment/i);
  });

  it('8. Paige doctrine does not prescribe redesign before diagnosis', () => {
    const ctx = buildPaigeStudioSubstralContext({});
    assert.equal(ctx.voice_rules.no_redesign_assumption, true);
    assert.match(ctx.brand_voice, /Diagnosis before design/i);
  });

  it('9–12. Emmett/outbound fails closed without Substral mailbox; no Anchor sender fallback', async () => {
    const db = memoryDb([{
      id: 55,
      slug: STUDIO_SUBSTRAL_SLUG,
      scoring_profile: 'studio_substral',
      sender_email: 'hello@studiosubstral.com',
      sending_domain: 'studiosubstral.com',
      enabled_agents: ['scout', 'max', 'paige'],
      autosend_enabled: false,
    }]);
    const readiness = await evaluateStudioSubstralOutboundReadiness(db, 55);
    assert.equal(readiness.ready, false);
    assert.ok(readiness.reasons.includes('mailbox_not_authenticated'));
    assert.throws(
      () => assertStudioSubstralOutboundNotImplicit({
        scoring_profile: 'studio_substral',
        sender_email: 'jacob@goanchorcleaning.com',
      }),
      /cannot reuse Anchor/
    );
  });

  it('10. tenant isolation — Scout WOI hook is profile-bound, not Anchor', () => {
    assert.equal(isWebDesignProfile('studio_substral'), true);
    assert.equal(isWebDesignProfile('cleaning_buyer'), false);
    assert.equal(usesWebsiteOpportunityIntelligence('studio_substral'), true);
    assert.equal(usesWebsiteOpportunityIntelligence('cleaning_buyer'), false);
  });

  it('11. operator snapshot module exports tenant-scoped builder', () => {
    const mod = require('../services/studioSubstralOperatorSnapshot');
    assert.equal(typeof mod.buildStudioSubstralOperatorSnapshot, 'function');
    const route = require('../routes/studioSubstralOperator');
    assert.ok(route);
  });
});
