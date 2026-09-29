'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { researchCompanies, scoutCompanies } = require('../services/governedOutboundReplenishment');
const now = new Date('2026-09-21T17:00:00Z');
const source = { structuredMission: { market: { segment: 'short_term_rental' }, geography: { cities: ['Manchester','Bedford'] } } };
const input = [{ name: 'Research Host Co', website: 'https://host.example/', operatingCity: 'Manchester', headquarters: 'Worcester, MA',
  email: 'unverified@host.example', email_verified: true, icpScore: 100, signals: [{ type: 'invented_purchase_intent' }],
  evidence: ['property','services'].map(kind => ({ kind, url: `https://host.example/${kind}`, summary: 'Publicly observed fact', observedAt: '2026-09-21T16:00:00Z' })) }];

test('research retains observed evidence and real headquarters without asserting email verification or buying intent', () => {
  const research = researchCompanies(input, source, now);
  assert.equal(research[0].headquarters, 'Worcester, MA');
  const candidate = scoutCompanies(research)[0];
  assert.equal(candidate.id, 'host.example');
  assert.equal(candidate.email, undefined); assert.equal(candidate.email_verified, undefined);
  assert.equal(candidate.icpScore, undefined); assert.deepEqual(candidate.signals, []);
  assert.match(candidate.location, /managed property; headquarters: Worcester/);
});
test('research cannot expand geography, change market, invent evidence freshness or use a different company domain', () => {
  for (const change of [
    row => { row.operatingCity = 'Nashua'; },
    row => { row.evidence[0].url = 'https://different.example/property'; },
    row => { row.evidence[0].observedAt = '2026-09-01T16:00:00Z'; },
    row => { row.evidence[0].observedAt = '2026-09-22T16:00:00Z'; },
    row => { row.evidence[0].kind = 'purchased_intent'; },
    row => { row.website = 'http://host.example/'; },
  ]) { const rows = structuredClone(input); change(rows[0]); assert.throws(() => researchCompanies(rows, source, now)); }
  assert.throws(() => researchCompanies(input, { structuredMission: { market: { segment: 'law_firm' } } }, now), { code: 'replenishment_str_scope_required' });
  assert.throws(() => researchCompanies([...input, ...input], source, now), { code: 'invalid_research_website' });
});

test('observed operating evidence survives repository normalization without manufacturing buying readiness', () => {
  const { normalizeCompany } = require('../packages/max/scoutAcquisition/ExistingIntelligence');
  const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
  const rows = structuredClone(input);
  rows[0].evidence[0].summary = 'The company lists a short-term rental property in Manchester, NH.';
  rows[0].evidence[1].summary = 'The company advertises Airbnb property management and coordinates housekeeping.';
  const candidate = normalizeCompany(scoutCompanies(researchCompanies(rows, source, now))[0], '10');
  assert.match(candidate.description, /short-term rental property/);
  const evaluate = c => attachFitToClassified({companyId:c.id,name:c.name,signals:[],observations:[],unknowns:[],evidenceRefs:[]},c,
    {geography:{label:'Greater Manchester'},segments:['short_term_rental'],exclusions:[]}, +now).evaluation;
  assert.equal(evaluate(candidate).qualification.status, 'qualified');
  assert.equal(evaluate(candidate).readiness.status, 'unknown');
  assert.deepEqual(evaluate(candidate).readiness.signals, []);
  assert.equal(evaluate({...candidate,description:null}).qualification.status, 'uncertain');
  assert.equal(evaluate({...candidate,description:'Does not manage vacation rentals. Residential management only.'}).qualification.status, 'not_qualified');
});

test('Scout research websites survive discovery normalization and Max ranking into CRM identity', () => {
  const { classifySignals } = require('../packages/max/scoutAcquisition/ScoutAdapter');
  const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
  const { buildMissionBoundCandidates } = require('../packages/max/workspace/EmmettMissionCandidates');
  const { hasSufficientAdmissionIdentity } = require('../packages/max/workspace/MissionBoundCrmAdmission');
  const company = scoutCompanies(researchCompanies(input, source, now))[0];
  const classified = classifySignals({...company,people:[]});
  const payload = normalizeScoutDiscoveryPayload({payload:{fitCandidates:[{...classified,qualified:true,qualificationStatus:'qualified',readinessState:'unknown'}]}});
  const mission = {id:'research',tenantId:'10',...source,targetSegment:'Short-term rental operators'};
  const scout={missionId:'research',specialist:'scout',kind:'discovery',payload};
  for(const contributions of [[scout],[scout,{missionId:'research',specialist:'max',kind:'prioritization',payload:{rankedTargets:[{id:company.id,name:company.name,rank:1}]}}]]) {
    const [candidate]=buildMissionBoundCandidates(mission,contributions);
    assert.equal(candidate.website,company.website);assert.equal(candidate.domain,'host.example');
    assert.equal(candidate.location,company.location);assert.equal(hasSufficientAdmissionIdentity(candidate),true);
    assert.equal(candidate.email,null);
  }
});

test('scraped website-template addresses remain ineligible even if a verifier accepts them', () => {
  const {canonicalOutboundEmailIneligibilityReason}=require('../utils/canonicalEmailEligibility');
  for(const email of ['user@domain.com','example@mysite.com'])assert.ok(canonicalOutboundEmailIneligibilityReason({email,email_verified:true,email_status:'valid',email_provenance_source:'scraped'}));
});
