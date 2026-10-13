'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  PULSEFORGE_MAILBOX_CONFIG,
  pulseforgeMailboxConfig,
  normalizeIntegration,
  usesGoogleOAuthSmtp,
  isReplyOnlyMailbox,
} = require('../services/tenantMailbox');
const {
  PULSEFORGE_CASHFLOW_OFFER,
  pulseforgeAssessmentReason,
  buildPulseforgeRepairSprintVariant,
  validatePulseforgeCashflowCopy,
} = require('../utils/pulseforgeCashflowOffer');
const { pulseforgeStructuredMission } = require('../utils/pulseforgeCashflowTenant');
const { buildPerProspectVariants, runPaigeVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const { policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');

function assessment() {
  return {
    recommended_action: 'AUDIT_WORTH_REVIEWING',
    commercial_diagnosis: {
      diagnosis_class: 'TARGETED_REMEDIATION',
      what_is_wrong: [{
        id: 'dom_no_contact_nav',
        evidence_class: 'OBSERVED',
        summary: 'Primary navigation does not include a contact path.',
      }],
    },
    evidence_refs: [{
      id: 'dom_no_contact_nav',
      evidence_class: 'OBSERVED',
      summary: 'Primary navigation does not include a contact path.',
    }],
  };
}

test('Pulseforge tenant 1 mailbox is isolated and send+receive capable', () => {
  const cfg = pulseforgeMailboxConfig('1');
  assert.equal(cfg.integration.id, 'tmi_1_pulseforge_jacob');
  assert.equal(cfg.identity.id, 'tsi_1_pulseforge_jacob');
  assert.equal(cfg.identity.senderEmail, 'jacob@pulseforge.com');
  assert.equal(cfg.integration.oauthRefreshSecretRef, 'PULSEFORGE_GOOGLE_REFRESH_TOKEN');
  assert.equal(PULSEFORGE_MAILBOX_CONFIG.senderDisplayName, 'Jacob Maynard');
  const normalized = normalizeIntegration(cfg.integration);
  assert.equal(usesGoogleOAuthSmtp(normalized), true);
  assert.equal(isReplyOnlyMailbox(normalized), false);
});

test('canonical offer is $500 upfront with a 48-hour target', () => {
  assert.equal(PULSEFORGE_CASHFLOW_OFFER.price, 500);
  assert.equal(PULSEFORGE_CASHFLOW_OFFER.paymentTerms, 'paid_upfront');
  assert.equal(PULSEFORGE_CASHFLOW_OFFER.turnaroundTargetHours, 48);
  const mission = pulseforgeStructuredMission('2026-10-13T00:00:00.000Z');
  assert.equal(mission.immutable, true);
  assert.equal(mission.offer.id, PULSEFORGE_CASHFLOW_OFFER.id);
  assert.equal(mission.market.segment, 'small_business_owner');
  assert.equal(mission.geography.scope, 'nationwide');
});

test('repair outreach requires observed targeted-remediation evidence', () => {
  assert.equal(pulseforgeAssessmentReason(assessment()), null);
  assert.equal(pulseforgeAssessmentReason({ ...assessment(), commercial_diagnosis: { diagnosis_class: 'REDESIGN_CANDIDATE' } }), 'targeted_remediation_not_established');
  assert.equal(pulseforgeAssessmentReason(null), 'website_assessment_missing');
});

test('Paige generates the fixed offer from prospect-bound website evidence', async () => {
  const candidate = {
    id: 'prospect-1',
    prospectId: 'prospect-1',
    companyId: 'company-1',
    name: 'Acme Plumbing',
    assessmentId: 'assessment-1',
    assessmentPayload: assessment(),
  };
  const input = {
    clientId: 1,
    mission: { clientId: 1, tenantId: '1' },
    max: { rankedTargets: [candidate] },
    plan: {},
    crmByProspectId: new Map([['prospect-1', { prospect_id: 'prospect-1', company_id: 'company-1', first_name: 'Avery' }]]),
  };
  const copy = buildPerProspectVariants(input)[0];
  assert.match(copy.subject, /Acme Plumbing/);
  assert.match(copy.body, /\$500/);
  assert.match(copy.body, /paid upfront/i);
  assert.match(copy.body, /48 hours/i);
  assert.match(copy.body, /Pulseforge/);
  assert.match(copy.body, /Jacob\s*$/);
  assert.equal(validatePulseforgeCashflowCopy(copy).ok, true);
  const result = await runPaigeVariants({
    ...input,
    transactionId: 'tx-1',
    workspaceContext: { max: input.max, scout: {} },
    missionPlan: input.plan,
  });
  assert.equal(result.status, 'SUCCESS');
});

test('Paige holds Pulseforge prospects without a concrete website defect', () => {
  const variants = buildPerProspectVariants({
    clientId: 1,
    mission: { clientId: 1, tenantId: '1' },
    max: { rankedTargets: [{ id: 'p-2', companyId: 'c-2', name: 'No Evidence Co' }] },
  });
  assert.equal(variants.length, 0);
  assert.equal(variants._heldProspects[0].holdReason, 'observable_website_defect_required');
});

test('tenant 1 policy authorizes only verified founder or role contacts', () => {
  const now = new Date('2026-10-13T15:00:00.000Z');
  const grant = policy({
    tenantId: '1', sourceMissionId: 'mission', senderEmail: 'jacob@pulseforge.com',
    inboxIntegrationId: 'tmi_1_pulseforge_jacob', sendingIdentityId: 'tsi_1_pulseforge_jacob',
    dailyCap: 3, totalCap: 30, spacingMinutes: 240,
    startsAt: now.toISOString(), expiresAt: '2026-11-12T15:00:00.000Z',
  }, now);
  assert.deepEqual(grant.allowedContactClassifications, ['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL']);
  assert.equal(grant.sendingIdentityId, 'tsi_1_pulseforge_jacob');
});

test('Max clean inventory admits tenant 1 only with the canonical repair assessment', async () => {
  const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
  const contact = {
    id: 'prospect-1', client_id: 1, company_id: 'company-1', company_name: 'Acme Plumbing',
    company_domain: 'acme.example', company_website: 'https://acme.example',
    email: 'owner@acme.example', email_verified: true, email_status: 'valid',
    vertical: 'home_services', service_area_match: true, website_opportunity_assessment_id: 'assessment-1',
    website_opportunity_payload: assessment(), website_opportunity_score: 62,
    website_recommended_action: 'AUDIT_WORTH_REVIEWING',
    acquisition_metadata: { contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'owner@acme.example' } },
    enrichment_provenance: { email: { source: 'website_email', source_url: 'https://acme.example/contact' } },
  };
  const pool = { query: async (sql) => ({ rows: sql.includes('SELECT p.*') ? [contact] : [] }) };
  const store = { clientId: 1, candidateOwnership: async () => null, suppression: async () => null };
  const source = { mission: { structuredMission: {
    market: { segment: 'small_business_owner' },
    geography: { region: 'United States', scope: 'nationwide', cities: [] },
    offer: PULSEFORGE_CASHFLOW_OFFER,
  } } };
  const policyInput = { tenantId: '1', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL'] };
  assert.equal((await loadCleanInventory(pool, store, source, 1, policyInput)).clean.length, 1);
  contact.website_opportunity_payload = {
    ...assessment(),
    commercial_diagnosis: { diagnosis_class: 'REDESIGN_CANDIDATE' },
  };
  const rejected = await loadCleanInventory(pool, store, source, 1, policyInput);
  assert.equal(rejected.clean.length, 0);
  assert.equal(rejected.exclusionCounts.targeted_remediation_not_established, 1);
});
