'use strict';
const { ClarityStore } = require('../packages/max/clarity/store');
const { tenant, failure, enabled } = require('../packages/max/clarity/common');
const { buildRecommendations, summarizeCrossEvidence } = require('../packages/max/clarity/recommendations');
// Adapters receive read-only, deadline-bound transports. Neither may write
// state; only the caller saves recommendations after both observations settle.
function boundedEvidenceTransports({ pool, signal, deadline, httpImpl }) {
  let calls = 0;
  const http = httpImpl || require('axios');
  return {
    http: { post(url,data,options={}) {
      if (++calls > 16 || Date.now() >= deadline || signal.aborted) return Promise.reject(failure('clarity_evidence_budget'));
      const target = new URL(url);
      if (target.protocol !== 'https:' || !['oauth2.googleapis.com','googleads.googleapis.com'].includes(target.hostname)) return Promise.reject(failure('clarity_evidence_destination'));
      return http.post(url,data,{...options,signal,timeout:Math.max(1,Math.min(5000,deadline-Date.now())),maxRedirects:0,maxContentLength:2*1024*1024,maxBodyLength:2*1024*1024});
    } },
    pool: { query(text,values) {
      if (signal.aborted || Date.now() >= deadline || typeof text !== 'string' || !/^\s*SELECT\b/i.test(text)) return Promise.reject(failure('clarity_evidence_readonly'));
      return pool.query({text,values,query_timeout:Math.max(1,Math.min(5000,deadline-Date.now()))});
    } },
  };
}
async function loadClarityGoogleAds({ tenantId, pool, http, window }) {
  // Existing generic resolver performs schema DDL. This feature only reads an
  // already-linked exact tenant account and calls the existing read-only adapter.
  const accounts = (await pool.query(`SELECT * FROM ad_accounts WHERE client_id=$1 AND platform='google_ads' AND is_active=TRUE LIMIT 2`,[tenant(tenantId)])).rows;
  if (accounts.length !== 1 || !accounts[0].refresh_token) return { live: { available:false } };
  const evidence = await require('../packages/penny-paid-acquisition/adapters/googleAds').readGoogleAdsEvidence({ account:accounts[0],http,window });
  if (evidence.availability !== 'AVAILABLE') return { live: { available:false } };
  return require('../packages/penny-paid-acquisition/googleAdsOperatorBrief').buildGoogleAdsOperatorBrief({ evidence,readiness:{blockers:[]} });
}
async function collectCrossEvidence({ clientId, snapshot, pool, loadAds, loadLeads, remainingMs = 15000, httpImpl }) {
  const cid = tenant(clientId), window = { start: snapshot.window_start, end: snapshot.window_end, days: 7 };
  const adsLoader = loadAds || loadClarityGoogleAds;
  const leadsLoader = loadLeads || require('../packages/penny-paid-acquisition/FirstPartyAttributionEvidence').loadFirstPartyAttributionEvidence;
  const budget = Math.max(1,Math.min(15000,remainingMs));
  const controller = new AbortController(), deadline = Date.now()+budget;
  const transports = boundedEvidenceTransports({pool,signal:controller.signal,deadline,httpImpl});
  let timer;
  const timeout = new Promise(resolve=>{timer=setTimeout(()=>{controller.abort();resolve([{status:'rejected'},{status:'rejected'}]);},budget);});
  let observations;
  try {
    observations = await Promise.race([timeout,Promise.allSettled([
      adsLoader({ tenantId: cid, pool:transports.pool, http:transports.http, window, signal:controller.signal }),
      leadsLoader({ clientId: cid, pool:transports.pool, observationWindow: window, limit:500, signal:controller.signal }),
    ])]);
  } finally { clearTimeout(timer); controller.abort(); }
  const [ads,leads]=observations;
  return summarizeCrossEvidence({ clientId: cid, report: snapshot,
    googleAds: ads.status === 'fulfilled' ? { ...ads.value, client_id: cid } : null,
    firstParty: leads.status === 'fulfilled' ? { ...leads.value, client_id: cid } : null });
}
async function prepareRecommendations({ clientId, snapshot, store, pool, loadAds, loadLeads, remainingMs, expectedConsentAt }) {
  const cid = tenant(clientId);
  if (Number(snapshot.client_id) !== cid) throw failure('clarity_tenant_mismatch',403);
  // Do not re-read providers or rewrite a reviewed recommendation on retries.
  const existing = await store.listRecommendations({ clientId: cid, snapshotId: snapshot.id });
  if (existing.length) return existing;
  const history = await store.listSnapshots({ clientId: cid });
  const crossEvidence = await collectCrossEvidence({ clientId: cid, snapshot, pool, loadAds, loadLeads, remainingMs });
  const recommendations = buildRecommendations({ report: snapshot, history, crossEvidence });
  return store.saveRecommendations({ clientId: cid, snapshotId: snapshot.id, recommendations, crossEvidence, expectedConsentAt });
}
async function readClarityBrief({ clientId, pool, env = process.env, store = new ClarityStore(pool), now = new Date() }) {
  const cid = tenant(clientId);
  if (!enabled(env) || cid !== 10) return { available: false, reason: 'disabled' };
  try {
    const integration = await store.getIntegration({ clientId: cid });
    if (!integration?.enabled || integration.consent_status !== 'authorized') return { available: false, reason: 'authorization_required' };
    const [report] = await store.listSnapshots({ clientId: cid, limit: 1 });
    if (!report) return { available: false, reason: 'no_digest' };
    return { available: true, report,
      stale: now - new Date(`${report.window_end}T23:59:59Z`) > 14 * 86400000,
      recommendations: await store.listRecommendations({ clientId: cid, snapshotId: report.id }) };
  } catch (_) { return { available: false, reason: 'unavailable' }; }
}
async function routeClarityDecision(request, context) {
  const { failure: fail } = require('../packages/max/clarity/common');
  if (!enabled(context.env || process.env)) throw fail('clarity_disabled',403);
  if (request.runtimeOwner !== 'clarity_review' || request.missionId || request.executionMode !== 'manual'
    || !['admin','manager'].includes(request.permissions.role) || request.permissions.canExecute !== true
    || !request.operatorId || request.operatorId === 'autonomous_operator') throw fail('clarity_approval_forbidden',403);
  const cid = tenant(context.tenantId);
  if (cid !== 10 || Number(request.payload.client_id) !== cid) throw fail('clarity_tenant_mismatch',403);
  const store = context.clarityStore || new ClarityStore(context.pool);
  const integration = await store.getIntegration({ clientId: cid });
  if (!integration?.enabled || integration.consent_status !== 'authorized') throw fail('clarity_disabled',403);
  return store.decide({ clientId: cid, recommendationId: request.payload.recommendation_id,
    operatorId: request.operatorId, decision: request.payload.decision, expectedDigest: request.payload.expected_digest,
    idempotencyKey: request.payload.idempotency_key, requestId: request.id });
}
module.exports = { loadClarityGoogleAds, boundedEvidenceTransports, collectCrossEvidence, prepareRecommendations, readClarityBrief, routeClarityDecision };
