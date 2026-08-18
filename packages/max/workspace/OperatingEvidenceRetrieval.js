'use strict';

/**
 * SPEC-105 — operating-evidence retrieval over existing PulseForge stores.
 *
 * Thin composition only. Does not persist new operating events.
 * Distinguishes business understanding (CIE) from recorded activity.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  composeEvidenceGroundedRecommendation,
  normalizeCapabilityPolicy,
} = require('./OperatingStateRecommendation');

const EPISTEMIC = Object.freeze({
  VERIFIED: 'verified',
  INFERRED: 'inferred',
  NOT_RECORDED: 'not_recorded',
  UNAVAILABLE: 'unavailable',
  OPERATOR_ATTESTED: 'operator_attested',
  SYSTEM_OBSERVED: 'system_observed',
  PLANNED: 'planned',
  EXPECTED: 'expected',
});

const CAMPAIGN_LAYER = Object.freeze({
  INTENT: 'intent',
  EXECUTION: 'execution',
  OBSERVATION: 'observation',
  OUTCOME: 'outcome',
  LEARNING: 'learning',
});

const PROVENANCE = Object.freeze({
  AO: 'Campaign 001 AO records',
  SCOUT: 'Scout acquisition state',
  PROSPECTS: 'prospect repository',
  MISSION: 'mission',
  OBJECTIVE: 'operator objective',
  ACTIVITY: 'activity log',
  TOUCHPOINT: 'touchpoint',
  OUTCOME: 'revenue outcome',
  BLUEPRINT: 'approved Blueprint',
  OPERATOR: 'operator report',
});

const MAIL_EXECUTION_KEYS = Object.freeze([
  'mailed_at',
  'mail_sent_at',
  'physical_mail_sent',
  'mail_executed_at',
  'postage_confirmed_at',
  'direct_mail_sent_at',
]);

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function resolveTenantId(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.authorizedTenantId ||
      input.tenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId ||
      input.clientId ||
      ''
  ).trim();
}

function resolveClientId(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const raw =
    input.clientId ??
    envelope.clientId ??
    envelope.client_id ??
    sessionCtx.clientId ??
    sessionCtx.client_id ??
    resolveTenantId(input);
  const n = Number(raw);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

const BUSINESS_UNDERSTANDING_RE =
  /\b(who are (?:our|my|the) ideal customers|where do we operate|what services do we (?:provide|offer)|what are our (?:stated )?business goals|who do we serve|what do we sell)\b/i;

const ENTITY_UNDERSTANDING_RE =
  /\bwhat do you (?:currently )?(?:know|understand|remember) about\b/i;

const LIVE_MARKET_SIGNAL_RE =
  /\b(showing (?:buying |purchase )?signals?|who (?:around here )?(?:needs|is expanding)|buying signals? right now)\b/i;

const EXISTING_KNOWLEDGE_INVESTIGATE_RE =
  /\binvestigate\b[\s\S]{0,160}\b(already (?:knows?|recorded|tried|have)|what (?:pulseforge|we|you) already|existing (?:evidence|activity|records?|pipeline))\b/i;

const INVENTORY_INTENT_RE = new RegExp(
  [
    String.raw`\bevidence[- ]based inventory\b`,
    String.raw`\binventory (?:our|the|of)\b`,
    String.raw`\bwhat evidence (?:do we|have we|is)\b`,
    String.raw`\bshow me (?:our|the) existing\b`,
    String.raw`\btell me what (?:we'?ve|you'?ve|pulseforge) already\b`,
    String.raw`\bfirst investigate what (?:pulseforge|we|you) already\b`,
    String.raw`\bwhat (?:pulseforge|we|you) already (?:knows?|have|recorded)\b`,
    String.raw`\bwhat does pulseforge already know\b`,
    String.raw`\bwhat (?:campaigns?|acquisition activity|outreach) (?:have we|is already|has (?:already )?(?:happened|occurred|been))\b`,
    String.raw`\bwhat happened with (?:campaign|our)\b`,
    String.raw`\bwhat campaigns have we run\b`,
    String.raw`\bwhat (?:prospects|leads) do we already have\b`,
    String.raw`\bwhat has happened so far\b`,
    String.raw`\bwhat have we already tried\b`,
    String.raw`\bwhat have we (?:completed|done|finished|accomplished)(?: recently)?\b`,
    String.raw`\bwhat outreach has (?:already )?(?:been )?sent\b`,
    String.raw`\brecently completed\b`,
    String.raw`\bbefore recommending anything\b`,
    String.raw`\bdon'?t recommend a new\b`,
  ].join('|'),
  'i'
);

const OPERATING_OBJECT_RE = new RegExp(
  [
    String.raw`\bcampaigns?\b`,
    String.raw`\bcampaign 001\b`,
    String.raw`\bao leads?\b`,
    String.raw`\bacquisition activity\b`,
    String.raw`\boutreach\b`,
    String.raw`\bprospects?\b`,
    String.raw`\bpipeline\b`,
    String.raw`\bwalkthroughs?\b`,
    String.raw`\btouchpoints?\b`,
    String.raw`\bleads?\b`,
    String.raw`\bmissions?\b`,
    String.raw`\bsetter\b`,
    String.raw`\boutcomes?\b`,
    String.raw`\boperating evidence\b`,
  ].join('|'),
  'i'
);

const EXISTING_STATE_RE = new RegExp(
  [
    String.raw`\balready\b`,
    String.raw`\bexisting\b`,
    String.raw`\brecorded\b`,
    String.raw`\bhappened\b`,
    String.raw`\btried\b`,
    String.raw`\binventory\b`,
    String.raw`\bso far\b`,
    String.raw`\bpast and current\b`,
    String.raw`\bhave we run\b`,
    String.raw`\bhave we tried\b`,
    String.raw`\balready have\b`,
    String.raw`\balready recorded\b`,
    String.raw`\balready know`,
    String.raw`\bcurrently happening\b`,
    String.raw`\balready in motion\b`,
    String.raw`\bin motion\b`,
    String.raw`\bcompleted recently\b`,
    String.raw`\bhave we completed\b`,
    String.raw`\balready been sent\b`,
    String.raw`\bhas already been sent\b`,
  ].join('|'),
  'i'
);

const DEFER_RECOMMEND_RE =
  /\b(don'?t recommend|before recommending|before we (?:start|begin) (?:a )?new|not recommend a new)\b/i;

const GROUNDED_RECOMMENDATION_RE = new RegExp(
  [
    String.raw`\bbased on what (?:we'?ve|you'?ve|pulseforge) already (?:tried|verified|recorded|knows?)\b`,
    String.raw`\bgiven what (?:we'?ve|you'?ve|pulseforge) already (?:tried|verified|can actually verify|knows?)\b`,
    String.raw`\bgiven that update\b`,
    String.raw`\bgiven what(?:'s| is) already in motion\b`,
    String.raw`\bgiven the (?:campaign evidence|current state)\b`,
    String.raw`\bgiven what we'?ve already tried\b`,
    String.raw`\bwhat pulseforge (?:already )?(?:knows?|can actually verify)\b`,
    String.raw`\balready tried\b.{0,80}\bwhat should (?:i|we)\b`,
    String.raw`\bwhat should (?:i|we) .{0,60}(?:already tried|can actually verify)\b`,
    String.raw`\bcurrent acquisition activity\b`,
    String.raw`\bcampaign evidence\b`,
    String.raw`\balready in motion\b`,
  ].join('|'),
  'i'
);

const CURRENT_STATE_RECOMMENDATION_RE = new RegExp(
  [
    String.raw`\bwhat should (?:i|we) (?:focus|prioritize|do next)\b`,
    String.raw`\bwhere should (?:i|we) focus(?: next)?\b`,
    String.raw`\bwhere is the (?:highest[- ]leverage|highest leverage)\b`,
    String.raw`\bwhat is (?:our|the|my) next (?:move|focus|step|priority)\b`,
    String.raw`^(?:(?:max|please),?\s+)?what should (?:i|we)(?:\s+next)?\s*\??$`,
  ].join('|'),
  'i'
);

const BUSINESS_FRAMED_RECOMMENDATION_RE =
  /\bwhat you know about (?:my |our |the )?business\b|\bbased on (?:approved )?business (?:understanding|priorities|blueprint)\b|\babout my business\b/i;

const OPERATING_STATUS_QUESTION_RE = new RegExp(
  [
    String.raw`^(?:(?:max|please),?\s+)?(?:was|were|did|has|have|when)\b.{0,120}\b(?:mailed|mail(?:ed)?|sent|follow[- ]up|begin|begun|started)\b`,
    String.raw`\bwhen (?:was|were|did)\b.{0,80}\b(?:mailed|mail|sent|campaign)\b`,
    String.raw`\bwhat(?:'s| is) the current state of\b`,
    String.raw`\bcurrent state of campaign\b`,
    String.raw`^(?:(?:max|please),?\s+)?did (?:mike'?s? )?follow[- ]up\b`,
    String.raw`^(?:(?:max|please),?\s+)?was campaign \d+ mailed\b`,
    String.raw`\b(?:are|is)\b.{0,80}\b(?:yelp(?:\s+ads?)?|google ads|facebook ads|(?:the )?ads?)\b.{0,40}\b(?:working|effective|performing|converting)\b`,
  ].join('|'),
  'i'
);

const NEW_INVESTIGATION_RE = new RegExp(
  [
    String.raw`\bfind (?:\d+\s+)?(?:additional|more)\s+(?:property managers|prospects|leads|companies|opportunit)`,
    String.raw`\bfind (?:more )?(?:commercial |current )?(?:cleaning )?(?:\w+\s+){0,4}opportunit`,
    String.raw`\blook(?:ing)? for (?:commercial|more|expansion|dentists?|property|competitors?|prospects?|leads?|signals?)`,
    String.raw`\bresearch (?:competitors?|the market|expansion|property|dentists?|prospects?)`,
  ].join('|'),
  'i'
);

function isBusinessUnderstandingQuestion(question) {
  const q = String(question || '');
  if (BUSINESS_UNDERSTANDING_RE.test(q)) return true;
  if (ENTITY_UNDERSTANDING_RE.test(q) && !OPERATING_OBJECT_RE.test(q)) return true;
  return false;
}

function isExistingKnowledgeInvestigate(question) {
  return EXISTING_KNOWLEDGE_INVESTIGATE_RE.test(String(question || ''));
}

function isNewInvestigationRequest(question) {
  const q = String(question || '');
  if (isExistingKnowledgeInvestigate(q)) return false;
  return NEW_INVESTIGATION_RE.test(q);
}

function isInventoryOnlyRequest(question) {
  const q = String(question || '');
  if (DEFER_RECOMMEND_RE.test(q)) return true;
  if (/\binventory\b/i.test(q)) return true;
  if (/\btell me what you can verify\b/i.test(q)) return true;
  if (/\bfirst investigate what (?:pulseforge|we|you) already\b/i.test(q)) return true;
  if (/\bbefore recommending anything\b/i.test(q)) return true;
  return false;
}

function isClaimChallengeQuestion(question) {
  try {
    const challenge = require('./RecommendationClaimChallenge');
    return (
      challenge.isClaimChallenge(question) || challenge.isOperatorClaimCorrection(question)
    );
  } catch (_) {
    return false;
  }
}

function isOperatingEvidenceQuestion(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  if (isClaimChallengeQuestion(q)) return false;
  if (isBusinessUnderstandingQuestion(q)) return false;
  if (isNewInvestigationRequest(q)) return false;
  if (LIVE_MARKET_SIGNAL_RE.test(q) && !EXISTING_STATE_RE.test(q) && !INVENTORY_INTENT_RE.test(q)) {
    return false;
  }
  try {
    const cognitive = require('../specialistDelegation/CognitiveMode');
    if (cognitive.looksLikeCompletedRetrieval(q)) return true;
  } catch (_) {
    /* classifier unavailable */
  }
  if (INVENTORY_INTENT_RE.test(q)) return true;
  if (isExistingKnowledgeInvestigate(q)) return true;
  if (DEFER_RECOMMEND_RE.test(q) && OPERATING_OBJECT_RE.test(q)) return true;
  if (/\bcampaign 001\b/i.test(q) && EXISTING_STATE_RE.test(q)) return true;
  if (OPERATING_OBJECT_RE.test(q) && EXISTING_STATE_RE.test(q)) return true;
  if (OPERATING_STATUS_QUESTION_RE.test(q)) return true;
  return false;
}

function isBusinessFramedRecommendation(question) {
  return BUSINESS_FRAMED_RECOMMENDATION_RE.test(String(question || ''));
}

function hasOperatingGrounding(question) {
  const q = String(question || '');
  if (GROUNDED_RECOMMENDATION_RE.test(q)) return true;
  if (EXISTING_STATE_RE.test(q)) return true;
  if (/\bcampaign 001\b/i.test(q)) return true;
  if (/\bgiven (?:that|what|the)\b/i.test(q) && OPERATING_OBJECT_RE.test(q)) return true;
  if (/\bbased on\b/i.test(q) && OPERATING_OBJECT_RE.test(q) && !isBusinessFramedRecommendation(q)) {
    return true;
  }
  return false;
}

function isBareCurrentStateRecommendation(question) {
  const q = String(question || '').trim();
  if (!CURRENT_STATE_RECOMMENDATION_RE.test(q)) return false;
  if (isBusinessFramedRecommendation(q)) return false;
  if (isBusinessUnderstandingQuestion(q)) return false;
  return true;
}

function isOperatingGroundedRecommendation(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  if (isInventoryOnlyRequest(q)) return false;
  if (isNewInvestigationRequest(q)) return false;
  if (isBusinessUnderstandingQuestion(q)) return false;
  if (isBusinessFramedRecommendation(q)) return false;
  if (hasOperatingGrounding(q) && (GROUNDED_RECOMMENDATION_RE.test(q) || CURRENT_STATE_RECOMMENDATION_RE.test(q))) {
    return true;
  }
  return false;
}

function shouldRetrieveOperatingEvidence(question) {
  try {
    const cognitive = require('../specialistDelegation/CognitiveMode');
    if (cognitive.looksLikeSummary(question) || cognitive.looksLikeCompletedRetrieval(question)) {
      return true;
    }
    if (
      cognitive.looksLikeDiagnosis(question) ||
      cognitive.looksLikeUnknownAnalysis(question) ||
      cognitive.looksLikeRisk(question) ||
      cognitive.looksLikeProgress(question)
    ) {
      return true;
    }
  } catch (_) {
    /* classifier unavailable */
  }
  try {
    const bi = require('./BusinessIntelligence');
    if (bi.isChannelEffectivenessQuestion(question)) {
      return true;
    }
  } catch (_) {
    /* synthesis unavailable */
  }
  return (
    isOperatingEvidenceQuestion(question) ||
    isOperatingGroundedRecommendation(question)
  );
}

function bundleHasUsableOperatingSignal(bundle) {
  if (!bundle || bundle.failClosed) return false;
  const { assembleOperatingState } = require('./OperatingStateRecommendation');
  return assembleOperatingState(bundle).hasAnyOperatingSignal === true;
}

function evidenceItem(input = {}) {
  return {
    epistemic: input.epistemic || EPISTEMIC.NOT_RECORDED,
    layer: input.layer || null,
    claim: present(input.claim),
    provenance: present(input.provenance) || PROVENANCE.AO,
    sourceKind: input.sourceKind || null,
    debugSource: input.debugSource || null,
    counts: input.counts || null,
    records: Array.isArray(input.records) ? input.records : [],
  };
}

function hasDurableMailExecution(leads = [], extras = {}) {
  if (extras.mailExecuted === true) return true;
  const rows = Array.isArray(leads) ? leads : [];
  return rows.some((lead) => {
    if (!lead || typeof lead !== 'object') return false;
    for (const key of MAIL_EXECUTION_KEYS) {
      if (lead[key]) return true;
    }
    const status = String(lead.mail_status || lead.execution_status || '').toLowerCase();
    return status === 'mailed' || status === 'sent' || status === 'executed';
  });
}

function hasYelpEvidence(bundle = {}) {
  if (bundle.yelp && (bundle.yelp.recorded === true || (bundle.yelp.events || []).length)) {
    return true;
  }
  const activity = (bundle.activity && bundle.activity.rows) || [];
  return activity.some((row) => /yelp/i.test(String(row.channel || row.source || row.action_type || '')));
}

function scopedRows(rows, clientId, tenantId) {
  const list = Array.isArray(rows) ? rows : [];
  if (clientId == null && !tenantId) return [];
  return list.filter((row) => {
    if (!row || typeof row !== 'object') return false;
    const owner =
      row.client_id != null
        ? row.client_id
        : row.clientId != null
          ? row.clientId
          : row.tenantId != null
            ? row.tenantId
            : row.tenant_id;
    if (owner == null) return false;
    if (clientId != null && Number(owner) === Number(clientId)) return true;
    if (tenantId && String(owner) === String(tenantId)) return true;
    return false;
  });
}

async function safeLoad(loader, fallback) {
  if (typeof loader !== 'function') return fallback;
  try {
    const result = await loader();
    return result == null ? fallback : result;
  } catch (_) {
    return fallback;
  }
}

function defaultPool(input = {}) {
  if (input.pool) return input.pool;
  try {
    return require('../../../db');
  } catch (_) {
    return null;
  }
}

async function defaultLoadCampaignAo(input = {}) {
  const clientId = resolveClientId(input);
  if (clientId == null) {
    return { available: false, reason: 'missing_tenant' };
  }
  try {
    const ao = input.aoBriefingService || require('../../../services/aoBriefingService');
    const [progress, leads] = await Promise.all([
      ao.getCampaign001Progress(clientId),
      typeof ao.fetchEnrichedLeads === 'function'
        ? ao.fetchEnrichedLeads(clientId)
        : Promise.resolve([]),
    ]);
    return {
      available: true,
      progress: progress || null,
      leads: Array.isArray(leads) ? leads : [],
      campaignName:
        (progress && progress.campaign_name) || ao.CAMPAIGN_001 || 'Campaign 001',
    };
  } catch (_) {
    return { available: false, reason: 'ao_store_unavailable' };
  }
}

async function defaultLoadProspects(input = {}) {
  const clientId = resolveClientId(input);
  if (clientId == null) {
    return { available: false, reason: 'missing_tenant' };
  }
  const pool = defaultPool(input);
  if (!pool || typeof pool.query !== 'function') {
    return { available: false, reason: 'prospect_store_unavailable' };
  }
  try {
    const { rows } = await pool.query(
      `SELECT
         COUNT(*)::int AS total,
         COUNT(*) FILTER (WHERE COALESCE(icp_score, 0) >= 70)::int AS qualified,
         COUNT(*) FILTER (WHERE status = 'cold')::int AS cold,
         COUNT(*) FILTER (WHERE status = 'warm')::int AS warm,
         COUNT(*) FILTER (WHERE COALESCE(is_hot, false) = true)::int AS hot,
         COUNT(*) FILTER (WHERE COALESCE(setter_visible, false) = true)::int AS setter_visible,
         COUNT(*) FILTER (WHERE setter_status = 'booked')::int AS booked,
         COUNT(*) FILTER (WHERE setter_status = 'closed')::int AS closed
       FROM prospects
       WHERE client_id = $1`,
      [clientId]
    );
    const segments = await pool.query(
      `SELECT COALESCE(NULLIF(vertical, ''), 'unspecified') AS vertical, COUNT(*)::int AS count
       FROM prospects
       WHERE client_id = $1
       GROUP BY 1
       ORDER BY count DESC
       LIMIT 8`,
      [clientId]
    );
    return {
      available: true,
      counts: rows[0] || {},
      segments: segments.rows || [],
    };
  } catch (_) {
    return { available: false, reason: 'prospect_store_unavailable' };
  }
}

async function defaultLoadScout(input = {}) {
  const tenantId = resolveTenantId(input);
  if (!tenantId) return { available: false, reason: 'missing_tenant' };
  try {
    const existing = require('../scoutAcquisition/ExistingIntelligence');
    const {
      createMemoryAcquisitionState,
      createPostgresAcquisitionState,
    } = require('../scoutAcquisition/AcquisitionState');
    const intelligence = await existing.loadRepository({
      authorizedTenantId: tenantId,
      tenantId,
      companies: input.companies,
      people: input.people,
      loadCompanies: input.loadCompanies,
      defaultLoadFromDb: input.defaultLoadFromDb,
    });
    let aoStore = input.aoStore;
    if (!aoStore) {
      if (process.env.DATABASE_URL) {
        try {
          aoStore = createPostgresAcquisitionState(defaultPool(input));
        } catch (_) {
          aoStore = createMemoryAcquisitionState();
        }
      } else {
        aoStore = createMemoryAcquisitionState();
      }
    }
    const state = typeof aoStore.get === 'function' ? await aoStore.get(tenantId) : null;
    return {
      available: true,
      intelligence,
      state,
      launchedNewWork: false,
    };
  } catch (_) {
    return { available: false, reason: 'scout_store_unavailable', launchedNewWork: false };
  }
}

async function defaultLoadMissions(input = {}) {
  const tenantId = resolveTenantId(input);
  const clientId = resolveClientId(input);
  if (!tenantId) return { available: false, reason: 'missing_tenant', rows: [] };
  if (typeof input.listMissions === 'function') {
    const rows = await input.listMissions({ tenantId, clientId, limit: 25 });
    return { available: true, rows: Array.isArray(rows) ? rows : [] };
  }
  if (input.missionEngine && typeof input.missionEngine.list === 'function') {
    const rows = await input.missionEngine.list({ tenantId, clientId, limit: 25 });
    return { available: true, rows: Array.isArray(rows) ? rows : [] };
  }
  const pool = defaultPool(input);
  if (!pool || typeof pool.query !== 'function') {
    return { available: false, reason: 'mission_store_unavailable', rows: [] };
  }
  try {
    const params = [String(tenantId)];
    let sql = `SELECT id, tenant_id, client_id, type, status, objective_text, title,
                      created_at, updated_at, completed_at
               FROM missions WHERE tenant_id = $1`;
    if (clientId != null) {
      params.push(clientId);
      sql += ` AND client_id = $${params.length}`;
    }
    sql += ' ORDER BY updated_at DESC LIMIT 25';
    const { rows } = await pool.query(sql, params);
    return {
      available: true,
      rows: (rows || []).map((row) => ({
        id: row.id,
        tenantId: row.tenant_id,
        clientId: row.client_id != null ? Number(row.client_id) : null,
        type: row.type,
        status: row.status,
        title: row.title,
        objectiveText: row.objective_text,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        completedAt: row.completed_at,
      })),
    };
  } catch (_) {
    return { available: false, reason: 'mission_store_unavailable', rows: [] };
  }
}

async function defaultLoadObjectives(input = {}) {
  const tenantId = resolveTenantId(input);
  const clientId = resolveClientId(input);
  if (!tenantId) return { available: false, reason: 'missing_tenant', rows: [] };
  try {
    const service = input.objectiveService || require('../../../services/operatorObjectives');
    const rows = await service.getActiveObjectives(
      { tenantId, clientId, limit: 25 },
      input.objectiveOpts || {}
    );
    return { available: true, rows: Array.isArray(rows) ? rows : [] };
  } catch (_) {
    return { available: false, reason: 'objective_store_unavailable', rows: [] };
  }
}

async function defaultLoadActivity(input = {}) {
  const clientId = resolveClientId(input);
  if (clientId == null) return { available: false, reason: 'missing_tenant', rows: [] };
  const pool = defaultPool(input);
  if (!pool || typeof pool.query !== 'function') {
    return { available: false, reason: 'activity_store_unavailable', rows: [] };
  }
  try {
    const touchpoints = await pool.query(
      `SELECT t.id, t.client_id, t.channel, t.action_type, t.outcome, t.created_at
       FROM touchpoints t
       WHERE t.client_id = $1
       ORDER BY t.created_at DESC
       LIMIT 25`,
      [clientId]
    );
    let activityRows = [];
    try {
      const activity = await pool.query(
        `SELECT al.id, al.client_id, al.action_type, al.notes, al.created_at
         FROM activity_log al
         WHERE al.client_id = $1
         ORDER BY al.created_at DESC
         LIMIT 25`,
        [clientId]
      );
      activityRows = activity.rows || [];
    } catch (_) {
      activityRows = [];
    }
    return {
      available: true,
      touchpoints: touchpoints.rows || [],
      activity: activityRows,
    };
  } catch (_) {
    return { available: false, reason: 'activity_store_unavailable', rows: [] };
  }
}

async function defaultLoadOperatorAttested(input = {}) {
  const tenantId = resolveTenantId(input);
  if (!tenantId) return { available: false, reason: 'missing_tenant', claims: [], evidence: [] };
  let knowledge = input.knowledge || (input.operatingEvidenceOpts && input.operatingEvidenceOpts.knowledge);
  if (!knowledge && typeof input.loadKnowledge === 'function') {
    knowledge = await input.loadKnowledge(input);
  }
  if (!knowledge && input.useKnowledgeBoot === true) {
    try {
      const { getKnowledgeBoot } = require('../../../utils/knowledgeRuntime');
      const boot = await getKnowledgeBoot();
      knowledge = boot && boot.knowledge;
    } catch (_) {
      knowledge = null;
    }
  }
  if (!knowledge || typeof knowledge.findClaims !== 'function') {
    return { available: false, reason: 'knowledge_unavailable', claims: [], evidence: [] };
  }
  try {
    const claims = await knowledge.findClaims({ tenantId, limit: 100 });
    const operating = (claims || []).filter((claim) => claim.metadata && claim.metadata.operatingUpdate);
    let evidence = [];
    if (typeof knowledge.findEvidence === 'function') {
      const rows = await knowledge.findEvidence({ tenantId, sourceType: 'operator_report', limit: 100 });
      evidence = rows || [];
    }
    return {
      available: true,
      claims: operating,
      evidence,
      knowledge,
    };
  } catch (_) {
    return { available: false, reason: 'knowledge_unavailable', claims: [], evidence: [] };
  }
}

function operatorMailState(operatorAttested = {}) {
  const claims = Array.isArray(operatorAttested.claims) ? operatorAttested.claims : [];
  const mail = claims.filter((c) => c.metadata && c.metadata.predicate === 'physical_mail_execution');
  const active = mail.filter((c) => String(c.status || '').toLowerCase() === 'active');
  const superseded = mail.filter((c) => String(c.status || '').toLowerCase() !== 'active');
  const current = active[0] || null;
  return {
    current,
    superseded,
    occurredAt: current && current.metadata ? current.metadata.occurredAt : null,
    statement: current ? current.statement : null,
  };
}

function operatorFollowUpState(operatorAttested = {}) {
  const claims = Array.isArray(operatorAttested.claims) ? operatorAttested.claims : [];
  const rows = claims.filter((c) => c.metadata && c.metadata.predicate === 'campaign_follow_up_expected');
  const active = rows.filter((c) => String(c.status || '').toLowerCase() === 'active');
  const current = active[0] || null;
  const executed = rows.some((c) => {
    const value = String((c.metadata && c.metadata.value) || '').toLowerCase();
    return value === 'completed' || value === 'executed';
  });
  return {
    current,
    expectedAt: current && current.metadata ? current.metadata.expectedAt : null,
    executed,
    temporalClass: current && current.metadata ? current.metadata.temporalClass : 'expected',
  };
}

async function defaultLoadCapabilityPolicy(input = {}) {
  const opts = input.operatingEvidenceOpts || {};
  const injected =
    opts.capability ||
    input.capability ||
    input.capabilityPolicy ||
    null;
  if (injected && typeof injected === 'object') {
    return { available: injected.available !== false, ...injected };
  }

  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const enabled = firstDefined(
    envelope.enabled_agents,
    envelope.enabledAgents,
    sessionCtx.enabled_agents,
    sessionCtx.enabledAgents
  );
  const autosend = firstDefined(
    envelope.autosend_enabled,
    envelope.autosendEnabled,
    sessionCtx.autosend_enabled,
    sessionCtx.autosendEnabled
  );
  const readiness = firstDefined(envelope.readiness, sessionCtx.readiness, {});
  const emailMotionActive = firstDefined(
    envelope.emailMotionActive,
    sessionCtx.emailMotionActive,
    false
  );

  if (Array.isArray(enabled) || autosend != null) {
    return {
      available: true,
      enabled_agents: enabled,
      autosend_enabled: autosend,
      readiness,
      emailMotionActive: emailMotionActive === true,
    };
  }

  const clientId = resolveClientId(input);
  if (clientId == null) return { available: false, reason: 'missing_tenant' };
  try {
    const { getClientConfig } = require('../../../utils/clientContext');
    const row = await getClientConfig(clientId);
    if (!row) return { available: false, reason: 'client_unavailable' };
    return {
      available: true,
      enabled_agents: row.enabled_agents,
      autosend_enabled: row.autosend_enabled,
    };
  } catch (_) {
    return { available: false, reason: 'capability_store_unavailable' };
  }
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
}

async function defaultLoadOutcomes(input = {}) {
  const clientId = resolveClientId(input);
  if (clientId == null) return { available: false, reason: 'missing_tenant' };
  const pool = defaultPool(input);
  if (!pool || typeof pool.query !== 'function') {
    return { available: false, reason: 'outcome_store_unavailable' };
  }
  const out = {
    available: true,
    jobs: 0,
    payments: 0,
    revenue: null,
  };
  try {
    const jobs = await pool.query(
      `SELECT COUNT(*)::int AS count FROM jobs WHERE client_id = $1`,
      [clientId]
    );
    out.jobs = jobs.rows[0] ? Number(jobs.rows[0].count) : 0;
  } catch (_) {
    /* table may not exist */
  }
  try {
    const payments = await pool.query(
      `SELECT COUNT(*)::int AS count FROM payments WHERE client_id = $1`,
      [clientId]
    );
    out.payments = payments.rows[0] ? Number(payments.rows[0].count) : 0;
  } catch (_) {
    /* table may not exist */
  }
  return out;
}

/**
 * Load tenant-scoped operating evidence from existing stores.
 * Never launches Scout. Fails closed without tenant context.
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function loadOperatingEvidence(input = {}) {
  const tenantId = resolveTenantId(input);
  const clientId = resolveClientId(input);
  if (!tenantId || clientId == null) {
    return {
      ok: false,
      failClosed: true,
      reason: 'missing_tenant',
      tenantId: tenantId || null,
      clientId,
      items: [],
      campaign: null,
      prospects: null,
      scout: null,
      missions: [],
      objectives: [],
      activity: null,
      outcomes: null,
      capability: { available: false, reason: 'missing_tenant' },
    };
  }

  const opts = input.operatingEvidenceOpts || {};
  const loadCampaignAo = opts.loadCampaignAo || input.loadCampaignAo || defaultLoadCampaignAo;
  const loadProspects = opts.loadProspects || input.loadProspects || defaultLoadProspects;
  const loadScout = opts.loadScout || input.loadScout || defaultLoadScout;
  const loadMissions = opts.loadMissions || input.loadMissions || defaultLoadMissions;
  const loadObjectives = opts.loadObjectives || input.loadObjectives || defaultLoadObjectives;
  const loadActivity = opts.loadActivity || input.loadActivity || defaultLoadActivity;
  const loadOutcomes = opts.loadOutcomes || input.loadOutcomes || defaultLoadOutcomes;
  const loadOperatorAttested =
    opts.loadOperatorAttested || input.loadOperatorAttested || defaultLoadOperatorAttested;
  const loadCapability =
    opts.loadCapability || input.loadCapability || defaultLoadCapabilityPolicy;

  const loaderInput = {
    ...input,
    ...opts,
    tenantId,
    clientId,
    authorizedTenantId: tenantId,
  };

  const [campaign, prospects, scout, missions, objectives, activity, outcomes, operatorAttested, capability] =
    await Promise.all([
      safeLoad(() => loadCampaignAo(loaderInput), { available: false, reason: 'ao_store_unavailable' }),
      safeLoad(() => loadProspects(loaderInput), { available: false, reason: 'prospect_store_unavailable' }),
      safeLoad(() => loadScout(loaderInput), { available: false, reason: 'scout_store_unavailable', launchedNewWork: false }),
      safeLoad(() => loadMissions(loaderInput), { available: false, rows: [] }),
      safeLoad(() => loadObjectives(loaderInput), { available: false, rows: [] }),
      safeLoad(() => loadActivity(loaderInput), { available: false, touchpoints: [], activity: [] }),
      safeLoad(() => loadOutcomes(loaderInput), { available: false }),
      safeLoad(
        () => loadOperatorAttested(loaderInput),
        { available: false, claims: [], evidence: [] }
      ),
      safeLoad(
        () => loadCapability(loaderInput),
        { available: false, reason: 'capability_store_unavailable' }
      ),
    ]);

  const scopedLeads = scopedRows(campaign.leads, clientId, tenantId);
  const scopedMissions = scopedRows(missions.rows, clientId, tenantId);
  const scopedObjectives = scopedRows(objectives.rows, clientId, tenantId);
  const scopedTouchpoints = scopedRows(activity.touchpoints, clientId, tenantId);
  const scopedActivity = scopedRows(activity.activity, clientId, tenantId);

  const items = [];
  const campaignName = (campaign.progress && campaign.progress.campaign_name) || campaign.campaignName || 'Campaign 001';

  if (campaign.available === false) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.UNAVAILABLE,
        layer: CAMPAIGN_LAYER.INTENT,
        claim: 'Campaign / AO records could not be read for this tenant.',
        provenance: PROVENANCE.AO,
        sourceKind: 'ao',
        debugSource: campaign.reason || 'ao_store_unavailable',
      })
    );
  } else if (campaign.progress) {
    const p = campaign.progress;
    const seeded = Number(p.seeded_in_ao || scopedLeads.length || 0);
    items.push(
      evidenceItem({
        epistemic: seeded > 0 ? EPISTEMIC.VERIFIED : EPISTEMIC.NOT_RECORDED,
        layer: CAMPAIGN_LAYER.INTENT,
        claim:
          seeded > 0
            ? `${campaignName} has ${seeded} AO lead${seeded === 1 ? '' : 's'} attributed in PulseForge.`
            : `No AO leads are currently attributed to ${campaignName} for this tenant.`,
        provenance: PROVENANCE.AO,
        sourceKind: 'ao',
        debugSource: 'ao_leads',
        counts: {
          seeded,
          targetTotal: Number(p.target_total || 0),
          visited: Number(p.visited || 0),
          walkthroughRequests: Number(p.walkthrough_requests || 0),
          escalations: Number(p.escalations || 0),
          remaining: Number(p.remaining_route_queue || 0),
        },
      })
    );

    if (Number(p.visited || 0) > 0) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.VERIFIED,
          layer: CAMPAIGN_LAYER.OBSERVATION,
          claim: `${Number(p.visited)} AO lead${Number(p.visited) === 1 ? '' : 's'} have an operational state other than not-started.`,
          provenance: PROVENANCE.AO,
          sourceKind: 'ao',
          debugSource: 'ao_leads.operational_state',
        })
      );
    }

    if (Number(p.walkthrough_requests || 0) > 0) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.VERIFIED,
          layer: CAMPAIGN_LAYER.OBSERVATION,
          claim: `${Number(p.walkthrough_requests)} walkthrough-request state${Number(p.walkthrough_requests) === 1 ? '' : 's'} recorded in AO.`,
          provenance: PROVENANCE.AO,
          sourceKind: 'ao',
          debugSource: 'ao_leads.walkthrough_requested',
        })
      );
    } else {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.NOT_RECORDED,
          layer: CAMPAIGN_LAYER.OUTCOME,
          claim: 'No walkthrough-request operational states are recorded in AO for this tenant.',
          provenance: PROVENANCE.AO,
          sourceKind: 'ao',
        })
      );
    }

    const mailExecuted = hasDurableMailExecution(scopedLeads, campaign);
    const operatorMail = operatorMailState(operatorAttested);
    const operatorMailDate = operatorMail.occurredAt;
    const systemMailDate =
      campaign.mailExecutedAt ||
      campaign.mailed_at ||
      (mailExecuted && campaign.systemMailDate) ||
      null;
    if (mailExecuted && operatorMail.current && operatorMailDate && systemMailDate && operatorMailDate !== systemMailDate) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.INFERRED,
          layer: CAMPAIGN_LAYER.EXECUTION,
          claim: `Your operating update says ${campaignName} was mailed ${operatorMailDate}, while system evidence records ${systemMailDate}. I have conflicting evidence and won't collapse the two without resolution.`,
          provenance: `${PROVENANCE.OPERATOR}; ${PROVENANCE.AO}`,
          sourceKind: 'conflict',
          debugSource: 'operator_vs_system_mail',
        })
      );
    } else if (mailExecuted && operatorMail.current) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.VERIFIED,
          layer: CAMPAIGN_LAYER.EXECUTION,
          claim: `${campaignName} was mailed ${operatorMailDate || 'on the recorded date'}. This is supported by both operator attestation and system evidence.`,
          provenance: `${PROVENANCE.OPERATOR}; ${PROVENANCE.AO}`,
          sourceKind: 'ao',
          debugSource: 'operator_and_system_mail',
        })
      );
    } else if (operatorMail.current) {
      const history =
        operatorMail.superseded.length > 0
          ? ` An earlier operator report listed ${operatorMail.superseded
              .map((c) => c.metadata && c.metadata.occurredAt)
              .filter(Boolean)
              .join(', ')} and was later corrected.`
          : '';
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.OPERATOR_ATTESTED,
          layer: CAMPAIGN_LAYER.EXECUTION,
          claim: `${campaignName} was operator-reported as physically mailed on ${operatorMailDate}.${history}`,
          provenance: PROVENANCE.OPERATOR,
          sourceKind: 'operator_report',
          debugSource: 'operator_attested_mail',
        })
      );
    } else {
      items.push(
        evidenceItem({
          epistemic: mailExecuted ? EPISTEMIC.VERIFIED : EPISTEMIC.NOT_RECORDED,
          layer: mailExecuted ? CAMPAIGN_LAYER.EXECUTION : CAMPAIGN_LAYER.INTENT,
          claim: mailExecuted
            ? `Durable evidence confirms ${campaignName} physical mail execution.`
            : `I don't currently have a durable record confirming that ${campaignName} was physically mailed.`,
          provenance: PROVENANCE.AO,
          sourceKind: 'ao',
          debugSource: mailExecuted ? 'ao_leads.mail_execution' : 'ao_leads.seed_or_intent',
        })
      );
    }

    if (seeded > 0 && !mailExecuted && !operatorMail.current) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.INFERRED,
          layer: CAMPAIGN_LAYER.INTENT,
          claim: `AO follow-up records suggest these prospects were intended for ${campaignName} outreach. That is intent, not proof of external execution.`,
          provenance: PROVENANCE.AO,
          sourceKind: 'ao',
        })
      );
    }
  } else {
    const operatorMail = operatorMailState(operatorAttested);
    if (operatorMail.current) {
      const history =
        operatorMail.superseded.length > 0
          ? ` An earlier operator report listed ${operatorMail.superseded
              .map((c) => c.metadata && c.metadata.occurredAt)
              .filter(Boolean)
              .join(', ')} and was later corrected.`
          : '';
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.OPERATOR_ATTESTED,
          layer: CAMPAIGN_LAYER.EXECUTION,
          claim: `${campaignName} was operator-reported as physically mailed on ${operatorMail.occurredAt}.${history}`,
          provenance: PROVENANCE.OPERATOR,
          sourceKind: 'operator_report',
          debugSource: 'operator_attested_mail',
        })
      );
    }
  }

  if (!(items || []).some((item) => item.debugSource === 'operator_attested_mail' || item.debugSource === 'operator_and_system_mail' || item.debugSource === 'operator_vs_system_mail')) {
    const operatorMail = operatorMailState(operatorAttested);
    if (operatorMail.current) {
      const history =
        operatorMail.superseded.length > 0
          ? ` An earlier operator report listed ${operatorMail.superseded
              .map((c) => c.metadata && c.metadata.occurredAt)
              .filter(Boolean)
              .join(', ')} and was later corrected.`
          : '';
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.OPERATOR_ATTESTED,
          layer: CAMPAIGN_LAYER.EXECUTION,
          claim: `${campaignName} was operator-reported as physically mailed on ${operatorMail.occurredAt}.${history}`,
          provenance: PROVENANCE.OPERATOR,
          sourceKind: 'operator_report',
          debugSource: 'operator_attested_mail',
        })
      );
    }
  }

  const followUp = operatorFollowUpState(operatorAttested);
  if (followUp.current) {
    items.push(
      evidenceItem({
        epistemic: followUp.executed ? EPISTEMIC.VERIFIED : EPISTEMIC.EXPECTED,
        layer: followUp.executed ? CAMPAIGN_LAYER.EXECUTION : CAMPAIGN_LAYER.INTENT,
        claim: followUp.executed
          ? `Follow-up on ${campaignName} has recorded execution evidence.`
          : `Follow-up on ${campaignName} leads was operator-reported as expected to begin ${followUp.expectedAt}. That is not recorded execution.`,
        provenance: PROVENANCE.OPERATOR,
        sourceKind: 'operator_report',
        debugSource: 'operator_attested_follow_up',
      })
    );
  }

  if (prospects.available === false) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.UNAVAILABLE,
        claim: 'Prospect repository could not be read for this tenant.',
        provenance: PROVENANCE.PROSPECTS,
        sourceKind: 'prospects',
        debugSource: prospects.reason,
      })
    );
  } else {
    const total = Number((prospects.counts && prospects.counts.total) || 0);
    const qualified = Number((prospects.counts && prospects.counts.qualified) || 0);
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.VERIFIED,
        claim:
          total > 0
            ? `Prospect repository has ${total} existing prospect${total === 1 ? '' : 's'}${qualified ? `, ${qualified} at or above qualification threshold` : ''}.`
            : 'No prospects are currently recorded for this tenant.',
        provenance: PROVENANCE.PROSPECTS,
        sourceKind: 'prospects',
        counts: prospects.counts || { total: 0 },
      })
    );
  }

  if (scout.available === false) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.UNAVAILABLE,
        claim: 'Existing Scout acquisition state could not be read.',
        provenance: PROVENANCE.SCOUT,
        sourceKind: 'scout',
      })
    );
  } else {
    const intel = scout.intelligence || {};
    const matched = Number((intel.counts && intel.counts.matched) || (intel.companies || []).length || 0);
    const state = scout.state;
    if (matched > 0 || (state && Number(state.opportunityCount || 0) > 0)) {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.VERIFIED,
          claim: `Existing Scout intelligence has ${matched || Number(state.opportunityCount || 0)} in-scope compan${(matched || Number(state.opportunityCount || 0)) === 1 ? 'y' : 'ies'} on file. This is retrieved state, not a new investigation.`,
          provenance: PROVENANCE.SCOUT,
          sourceKind: 'scout',
          counts: intel.counts || { matched },
        })
      );
    } else {
      items.push(
        evidenceItem({
          epistemic: EPISTEMIC.NOT_RECORDED,
          claim: 'No existing Scout acquisition intelligence is on file for this tenant.',
          provenance: PROVENANCE.SCOUT,
          sourceKind: 'scout',
        })
      );
    }
  }

  if (missions.available === false) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.UNAVAILABLE,
        claim: 'Mission records could not be read for this tenant.',
        provenance: PROVENANCE.MISSION,
        sourceKind: 'mission',
      })
    );
  } else if (scopedMissions.length) {
    const titles = scopedMissions
      .slice(0, 5)
      .map((m) => present(m.title || m.objectiveText || 'Mission'))
      .filter(Boolean);
    const titleBit = titles.length ? ` including ${titles.join('; ')}` : '';
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.VERIFIED,
        layer: CAMPAIGN_LAYER.INTENT,
        claim: `${scopedMissions.length} mission${scopedMissions.length === 1 ? '' : 's'} on file${titleBit}. Mission status is planned or tracked work — not proof that an external action occurred.`,
        provenance: PROVENANCE.MISSION,
        sourceKind: 'mission',
        records: scopedMissions.slice(0, 8).map((m) => ({
          id: m.id,
          title: m.title || m.objectiveText || 'Mission',
          status: m.status,
          createdAt: m.createdAt || m.created_at || null,
        })),
      })
    );
  } else {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.NOT_RECORDED,
        claim: 'No missions are currently recorded for this tenant.',
        provenance: PROVENANCE.MISSION,
        sourceKind: 'mission',
      })
    );
  }

  if (objectives.available !== false && scopedObjectives.length) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.VERIFIED,
        layer: CAMPAIGN_LAYER.INTENT,
        claim: `${scopedObjectives.length} active operator objective${scopedObjectives.length === 1 ? '' : 's'} describe${scopedObjectives.length === 1 ? 's' : ''} desired state. Objectives are not evidence that activity occurred.`,
        provenance: PROVENANCE.OBJECTIVE,
        sourceKind: 'objective',
        records: scopedObjectives.slice(0, 5).map((o) => ({
          id: o.id,
          title: o.title || o.objectiveText || o.objective,
          status: o.status,
        })),
      })
    );
  }

  const activityCount = scopedTouchpoints.length + scopedActivity.length;
  if (activity.available === false) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.UNAVAILABLE,
        claim: 'Activity and touchpoint records could not be read for this tenant.',
        provenance: PROVENANCE.ACTIVITY,
        sourceKind: 'activity',
      })
    );
  } else if (activityCount > 0) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.VERIFIED,
        layer: CAMPAIGN_LAYER.OBSERVATION,
        claim: `${scopedTouchpoints.length} recent touchpoint${scopedTouchpoints.length === 1 ? '' : 's'} and ${scopedActivity.length} setter/activity event${scopedActivity.length === 1 ? '' : 's'} recorded for this tenant.`,
        provenance: scopedTouchpoints.length ? PROVENANCE.TOUCHPOINT : PROVENANCE.ACTIVITY,
        sourceKind: 'activity',
      })
    );
  } else {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.NOT_RECORDED,
        claim: 'No recent tenant-scoped touchpoints or setter activity events are recorded.',
        provenance: PROVENANCE.ACTIVITY,
        sourceKind: 'activity',
      })
    );
  }

  if (!hasYelpEvidence({ activity: { rows: scopedActivity }, yelp: input.yelp })) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.NOT_RECORDED,
        claim: 'No durable Yelp activity is recorded for this tenant.',
        provenance: PROVENANCE.ACTIVITY,
        sourceKind: 'yelp',
      })
    );
  }

  const crmPromoted = scopedLeads.filter(
    (l) => l.operational_state === 'converted_to_crm' || l.crm_prospect_id
  ).length;
  const jobs = Number((outcomes && outcomes.jobs) || 0);
  const payments = Number((outcomes && outcomes.payments) || 0);
  if (crmPromoted || jobs || payments) {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.VERIFIED,
        layer: CAMPAIGN_LAYER.OUTCOME,
        claim: [
          crmPromoted ? `${crmPromoted} AO lead${crmPromoted === 1 ? '' : 's'} promoted into CRM` : null,
          jobs ? `${jobs} job${jobs === 1 ? '' : 's'} recorded` : null,
          payments ? `${payments} payment${payments === 1 ? '' : 's'} recorded` : null,
        ]
          .filter(Boolean)
          .join('; ') + '.',
        provenance: PROVENANCE.OUTCOME,
        sourceKind: 'outcome',
      })
    );
  } else {
    items.push(
      evidenceItem({
        epistemic: EPISTEMIC.NOT_RECORDED,
        layer: CAMPAIGN_LAYER.OUTCOME,
        claim: 'No durable conversion, job, or payment outcomes are recorded for this tenant.',
        provenance: PROVENANCE.OUTCOME,
        sourceKind: 'outcome',
      })
    );
  }

  return {
    ok: true,
    failClosed: false,
    tenantId,
    clientId,
    launchedScout: false,
    campaign: {
      ...campaign,
      leads: scopedLeads,
      mailExecuted: hasDurableMailExecution(scopedLeads, campaign),
    },
    prospects,
    scout: { ...scout, launchedNewWork: false },
    missions: scopedMissions,
    objectives: scopedObjectives,
    activity: {
      ...activity,
      touchpoints: scopedTouchpoints,
      activity: scopedActivity,
    },
    outcomes,
    operatorAttested: {
      ...operatorAttested,
      mail: operatorMailState(operatorAttested),
      followUp: operatorFollowUpState(operatorAttested),
    },
    capability: normalizeCapabilityPolicy(capability),
    items,
  };
}

function itemsByEpistemic(items, epistemic) {
  return (items || []).filter((item) => item.epistemic === epistemic);
}

function formatVerifiedSection(items) {
  const verified = itemsByEpistemic(items, EPISTEMIC.VERIFIED);
  if (!verified.length) {
    return 'I queried the operating stores PulseForge can currently read for this tenant and did not find durable verified activity beyond empty or unavailable records.';
  }
  return verified
    .map((item) => `- ${item.claim} Verified from ${item.provenance}.`)
    .join('\n');
}

function formatInferredSection(items) {
  const inferred = itemsByEpistemic(items, EPISTEMIC.INFERRED);
  if (!inferred.length) return '';
  return inferred.map((item) => `- ${item.claim}`).join('\n');
}

function formatNotRecordedSection(items) {
  const missing = itemsByEpistemic(items, EPISTEMIC.NOT_RECORDED);
  if (!missing.length) return 'I am not currently flagging additional missing operating facts beyond the verified set above.';
  return missing.map((item) => `- ${item.claim}`).join('\n');
}

function formatUnavailableSection(items) {
  const rows = itemsByEpistemic(items, EPISTEMIC.UNAVAILABLE);
  if (!rows.length) return '';
  return rows
    .map((item) => `- ${item.claim}${item.debugSource ? ` Source: ${item.provenance}.` : ` Source: ${item.provenance}.`}`)
    .join('\n');
}

function formatOperatorAttestedSection(items) {
  const rows = (items || []).filter(
    (item) =>
      item.epistemic === EPISTEMIC.OPERATOR_ATTESTED ||
      item.epistemic === EPISTEMIC.EXPECTED ||
      item.epistemic === EPISTEMIC.PLANNED
  );
  if (!rows.length) return '';
  return rows
    .map((item) => {
      const label =
        item.epistemic === EPISTEMIC.EXPECTED || item.epistemic === EPISTEMIC.PLANNED
          ? 'PLANNED / EXPECTED'
          : 'OPERATOR ATTESTED';
      return `- ${item.claim} ${label} from ${item.provenance}.`;
    })
    .join('\n');
}

function isMailStatusQuestion(question) {
  return /\b(was|when was|when were).{0,80}\b(mailed|mail|sent)\b/i.test(String(question || ''));
}

function isFollowUpBeganQuestion(question) {
  return /\b(did|has|have).{0,80}\bfollow[- ]up\b.{0,40}\b(begin|begun|started|happen)/i.test(
    String(question || '')
  ) || /\bdid follow[- ]up begin\b/i.test(String(question || ''));
}

function composeFocusedOperatingAnswer(question, bundle) {
  const mail = bundle.operatorAttested && bundle.operatorAttested.mail;
  const followUp = bundle.operatorAttested && bundle.operatorAttested.followUp;
  const conflict = (bundle.items || []).find((item) => item.sourceKind === 'conflict');

  if (isFollowUpBeganQuestion(question)) {
    if (followUp && followUp.executed) {
      return `Recorded execution evidence shows follow-up on Campaign 001 began.`;
    }
    if (followUp && followUp.expectedAt) {
      return `Follow-up was expected to begin ${followUp.expectedAt}, but I don't have recorded execution confirming that it actually began.`;
    }
    return `I don't have recorded execution confirming that follow-up actually began.`;
  }

  if (isMailStatusQuestion(question)) {
    if (conflict) return conflict.claim;
    if (mail && mail.current) {
      const history =
        mail.superseded && mail.superseded.length
          ? ` An earlier operator report listed ${mail.superseded
              .map((c) => c.metadata && c.metadata.occurredAt)
              .filter(Boolean)
              .join(', ')} and was later corrected.`
          : '';
      return `Campaign 001 was operator-reported as mailed ${mail.occurredAt}.${history}`;
    }
    if (bundle.campaign && bundle.campaign.mailExecuted) {
      return `System evidence records that Campaign 001 was mailed.`;
    }
    return `I don't currently have a durable record confirming that Campaign 001 was physically mailed.`;
  }

  return null;
}

function composeRecommendationFromEvidence(bundle, understanding = null, extras = {}) {
  const composed = composeEvidenceGroundedRecommendation(bundle, {
    businessUnderstanding: understanding,
    now: extras.now,
    capability: extras.capability || (bundle && bundle.capability),
  });
  return composed.prose;
}

function understandingGoals(understanding) {
  const contract = understanding && understanding.contract;
  const summary = understanding && understanding.summary;
  const objectives = understanding && understanding.activeObjectives;
  const titles = Array.isArray(objectives)
    ? objectives.map((row) => present(row && (row.title || row.description))).filter(Boolean)
    : [];
  if (titles.length) return titles.map((title) => `- ${title}`).join('\n');
  const goal =
    (contract && contract.businessGoals) ||
    (summary && (summary.goals || summary.successMetric || summary.nearTermFocus));
  return goal ? present(goal) : 'No approved goals are currently on file.';
}

function understandingUnknowns(understanding, missing, unavailable) {
  const fromBlueprint = [];
  const summary = understanding && understanding.summary;
  const listed = summary && Array.isArray(summary.unknowns) ? summary.unknowns : [];
  for (const item of listed) {
    const text = present(item);
    if (text) fromBlueprint.push(`- ${text}`);
  }
  const operating = [missing, unavailable].filter(Boolean).join('\n');
  const parts = [];
  if (fromBlueprint.length) parts.push(fromBlueprint.join('\n'));
  if (operating) parts.push(operating);
  return parts.join('\n') || 'I am not currently flagging additional unknowns beyond the verified set above.';
}

function optionalSummaryRecommendation(bundle, extras = {}) {
  try {
    const grounded = composeEvidenceGroundedRecommendation(bundle, {
      businessUnderstanding: extras.businessUnderstanding || null,
      now: extras.now,
      capability: extras.capability || (bundle && bundle.capability),
      retractedPremises: extras.retractedPremises,
      operatorDeniedEmailActive: extras.operatorDeniedEmailActive,
    });
    const focus = grounded && grounded.decision && grounded.decision.focus;
    return focus ? present(focus) : '';
  } catch (_) {
    return '';
  }
}

function ensureRecommendationContractSections(prose, bundle) {
  let next = String(prose || '').trim();
  const currentState = [formatVerifiedSection(bundle.items), formatOperatorAttestedSection(bundle.items)]
    .filter(Boolean)
    .join('\n');
  const evidence = (bundle.items || [])
    .filter((item) => item && item.claim)
    .slice(0, 8)
    .map((item) => `- ${item.claim} (${item.epistemic} / ${item.provenance})`)
    .join('\n');
  if (!/\bCurrent state\b/i.test(next) && !/WHAT'S ALREADY IN MOTION/i.test(next) && currentState) {
    next += `\n\nCurrent state\n${currentState}`;
  }
  if (!/\bConfidence\b/i.test(next)) {
    next +=
      '\n\nConfidence\nAdvisory confidence is bounded by retrieved operating evidence. Unsupported current-execution claims were excluded.';
  }
  if (!/\bEvidence\b/i.test(next) && evidence) {
    next += `\n\nEvidence\n${evidence}`;
  }
  return next;
}

function evidenceBullets(bundle, limit = 8) {
  return (bundle.items || [])
    .filter((item) => item && item.claim)
    .slice(0, limit)
    .map((item) => `- ${item.claim} (${item.epistemic} / ${item.provenance})`)
    .join('\n');
}

function composeOperatingEvidenceAnswer(question, bundle, extras = {}) {
  if (!bundle || bundle.failClosed) {
    return {
      prose:
        'I cannot retrieve operating evidence without a tenant context. I will not inspect another client or invent activity.',
      used: [],
      knowledgeState: 'missing_tenant',
      recommend: false,
      launchedScout: false,
    };
  }

  const {
    CONTRACT_IDS,
    RetrievalContract,
    composeAccordingToContract,
    mayIncludeRecommendation,
  } = require('./ResponseContract');
  const {
    synthesizeBusinessIntelligence,
    serializeBusinessIntelligence,
    isChannelEffectivenessQuestion,
    analysisSectionsFromIntelligence,
  } = require('./BusinessIntelligence');
  const contract = extras.contract || null;
  const inventoryOnly = extras.inventoryOnly != null ? extras.inventoryOnly : isInventoryOnlyRequest(question);
  const recommend = extras.recommend === true && !inventoryOnly;
  const analysisMode = (contract && contract.id) || extras.analysisMode || null;
  const synthesis = synthesizeBusinessIntelligence({
    bundle,
    question,
    extras,
    analysisMode,
  });
  const biProse = synthesis.prose;
  const biMeta = serializeBusinessIntelligence(synthesis);
  const analysisSections = analysisSectionsFromIntelligence(synthesis, extras);
  const focused = recommend ? null : composeFocusedOperatingAnswer(question, bundle);
  const verified = formatVerifiedSection(bundle.items);
  const inferred = formatInferredSection(bundle.items);
  const operatorReported = formatOperatorAttestedSection(bundle.items);
  const missing = formatNotRecordedSection(bundle.items);
  const unavailable = formatUnavailableSection(bundle.items);
  const evidence = evidenceBullets(bundle);

  const ANALYTICAL_CONTRACTS = new Set([
    CONTRACT_IDS.DIAGNOSIS,
    CONTRACT_IDS.UNKNOWN_ANALYSIS,
    CONTRACT_IDS.RISK,
    CONTRACT_IDS.PROGRESS,
  ]);
  const isAnalytical = Boolean(contract && ANALYTICAL_CONTRACTS.has(contract.id));

  if (
    isChannelEffectivenessQuestion(question) &&
    !(contract && contract.id === CONTRACT_IDS.SUMMARY) &&
    !isAnalytical
  ) {
    const used = ['operatingEvidence'];
    const channelItems = (bundle.items || []).filter((item) =>
      /yelp|google ads|facebook ads|\bads?\b/i.test(
        `${item && item.sourceKind ? item.sourceKind : ''} ${item && item.claim ? item.claim : ''}`
      )
    );
    const prose = composeAccordingToContract(
      contract && contract.id === CONTRACT_IDS.RETRIEVAL ? contract : RetrievalContract,
      {
        businessIntelligence: biProse,
        verifiedState:
          channelItems.filter((item) => item.epistemic === EPISTEMIC.VERIFIED).length
            ? formatVerifiedSection(channelItems)
            : 'I queried operating evidence for this channel and did not find attributed conversion results.',
        unknowns: 'Insufficient evidence to determine effectiveness.',
        evidence: evidenceBullets({ items: channelItems.length ? channelItems : bundle.items }, 6),
      },
      { completedRecently: false, operatorAsked: false }
    );
    return {
      prose,
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: (contract && contract.id) || CONTRACT_IDS.RETRIEVAL,
      businessIntelligence: biMeta,
    };
  }

  if (focused && !(contract && contract.id === CONTRACT_IDS.SUMMARY) && !isAnalytical) {
    const used = ['operatingEvidence'];
    if (bundle.operatorAttested && bundle.operatorAttested.available !== false) {
      used.push('operatorAttested');
    }
    let prose = focused;
    if (contract && contract.id === CONTRACT_IDS.RETRIEVAL) {
      prose = composeAccordingToContract(
        contract,
        {
          businessIntelligence: biProse,
          verifiedState: focused,
          unknowns:
            'No additional operating facts are required to answer this status question.',
          evidence,
        },
        { completedRecently: false, operatorAsked: false }
      );
    }
    return {
      prose,
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract && contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (recommend && !isAnalytical) {
    const grounded = composeEvidenceGroundedRecommendation(bundle, {
      businessUnderstanding: extras.businessUnderstanding || null,
      now: extras.now,
      capability: extras.capability || bundle.capability,
      retractedPremises: extras.retractedPremises,
      operatorDeniedEmailActive: extras.operatorDeniedEmailActive,
    });
    const used = [];
    if (bundle.campaign && bundle.campaign.available !== false) used.push('campaignAo');
    if (bundle.prospects && bundle.prospects.available !== false) used.push('prospects');
    if (bundle.scout && bundle.scout.available !== false) used.push('scoutState');
    if ((bundle.missions || []).length) used.push('missions');
    if ((bundle.objectives || []).length) used.push('objectives');
    if (bundle.activity && bundle.activity.available !== false) used.push('activity');
    if (bundle.operatorAttested && bundle.operatorAttested.available !== false) used.push('operatorAttested');
    if (bundle.capability && bundle.capability.known) used.push('capabilityPolicy');
    used.push('operatingEvidence');
    const recommendationBody = ensureRecommendationContractSections(grounded.prose, bundle);
    const primaryFinding = synthesis.primary && synthesis.primary.finding;
    const recReferencesFinding =
      primaryFinding && recommendationBody.toLowerCase().includes('bottleneck')
        ? recommendationBody
        : primaryFinding
          ? `${recommendationBody}\n\nThis recommendation follows from the operating finding: ${primaryFinding}`
          : recommendationBody;
    const prose =
      contract && contract.id === CONTRACT_IDS.RECOMMENDATION
        ? composeAccordingToContract(
            contract,
            {
              businessIntelligence: biProse,
              recommendation: recReferencesFinding,
              currentState: [verified, operatorReported].filter(Boolean).join('\n'),
              reasoning: grounded.decision && grounded.decision.inference,
              confidence:
                'Advisory confidence is bounded by retrieved operating evidence. Unsupported current-execution claims were excluded.',
              evidence,
            },
            { recommendationPrimary: true }
          )
        : recReferencesFinding;
    return {
      prose,
      used,
      knowledgeState: 'operating_evidence',
      recommend: true,
      launchedScout: false,
      executed: false,
      items: bundle.items,
      decision: grounded.decision,
      premises: grounded.premises,
      lastClaim: grounded.lastClaim,
      state: grounded.state,
      contract: contract && contract.id,
      businessIntelligence: biMeta,
    };
  }

  const used = [];
  if (bundle.campaign && bundle.campaign.available !== false) used.push('campaignAo');
  if (bundle.prospects && bundle.prospects.available !== false) used.push('prospects');
  if (bundle.scout && bundle.scout.available !== false) used.push('scoutState');
  if ((bundle.missions || []).length) used.push('missions');
  if ((bundle.objectives || []).length) used.push('objectives');
  if (bundle.activity && bundle.activity.available !== false) used.push('activity');
  if (bundle.operatorAttested && bundle.operatorAttested.available !== false) used.push('operatorAttested');
  used.push('operatingEvidence');

  if (contract && contract.id === CONTRACT_IDS.DIAGNOSIS) {
    return {
      prose: composeAccordingToContract(
        contract,
        {
          bottleneck: analysisSections.bottleneck,
          evidence: analysisSections.evidence || evidence,
          confidence: analysisSections.confidence,
          operatorImpact: analysisSections.operatorImpact,
        },
        { includeOptionalRecommendation: false }
      ),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (contract && contract.id === CONTRACT_IDS.UNKNOWN_ANALYSIS) {
    return {
      prose: composeAccordingToContract(contract, {
        unknowns: analysisSections.unknowns,
        evidenceGaps: analysisSections.evidenceGaps || missing,
        operatorImpact: analysisSections.operatorImpact,
        suggestedInvestigations: analysisSections.suggestedInvestigations,
      }),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (contract && contract.id === CONTRACT_IDS.RISK) {
    return {
      prose: composeAccordingToContract(contract, {
        risks: analysisSections.risks,
        evidence: analysisSections.evidence || evidence,
        confidence: analysisSections.confidence,
        potentialImpact: analysisSections.potentialImpact,
      }),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (contract && contract.id === CONTRACT_IDS.PROGRESS) {
    return {
      prose: composeAccordingToContract(contract, {
        progress: analysisSections.progress,
        remainingWork: analysisSections.remainingWork,
        confidence: analysisSections.confidence,
        evidence,
      }),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (contract && contract.id === CONTRACT_IDS.SUMMARY) {
    const observed = [
      verified,
      operatorReported,
      inferred
        ? `${inferred}\nI am labeling interpretation separately from verified fact. Mission intent and AO seed notes are not proof of external execution.`
        : '',
    ]
      .filter(Boolean)
      .join('\n');
    const optionalRec = mayIncludeRecommendation(contract, { includeOptionalRecommendation: true })
      ? optionalSummaryRecommendation(bundle, extras)
      : '';
    return {
      prose: composeAccordingToContract(
        contract,
        {
          businessIntelligence: biProse,
          observedState: observed,
          goals: understandingGoals(extras.businessUnderstanding),
          unknowns: understandingUnknowns(extras.businessUnderstanding, missing, unavailable),
          recommendation: optionalRec,
          evidence,
        },
        { includeOptionalRecommendation: Boolean(optionalRec) }
      ),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  if (contract && contract.id === CONTRACT_IDS.RETRIEVAL) {
    const verifiedBody = [verified, operatorReported].filter(Boolean).join('\n');
    const unknownBody = [missing, unavailable].filter(Boolean).join('\n');
    return {
      prose: composeAccordingToContract(
        contract,
        {
          businessIntelligence: biProse,
          verifiedState: verifiedBody,
          unknowns:
            unknownBody ||
            'I am not currently flagging additional missing operating facts beyond the verified set above.',
          evidence: inferred
            ? `${inferred}\n${evidence}\nI am labeling interpretation separately from verified fact. Mission intent and AO seed notes are not proof of external execution.`
            : evidence,
          nextInvestigation:
            'I do not need Scout to rediscover prospects or campaigns already in PulseForge. New specialist work is only warranted if you want additional accounts that are not already recorded.',
        },
        {
          completedRecently: /\b(?:completed|done|finished|accomplished) recently\b|\bwhat have we (?:completed|done|finished)/i.test(
            String(question || '')
          ),
          operatorAsked: false,
        }
      ),
      used,
      knowledgeState: 'operating_evidence',
      recommend: false,
      launchedScout: false,
      items: bundle.items,
      contract: contract.id,
      businessIntelligence: biMeta,
    };
  }

  const parts = [
    biProse ? `Business Intelligence\n${biProse}` : '',
    'What I can verify',
    verified,
    operatorReported ? 'What the operator reported\n' + operatorReported : '',
    inferred
      ? 'What that tells me\n' +
        inferred +
        '\nI am labeling interpretation separately from verified fact. Mission intent and AO seed notes are not proof of external execution.'
      : 'What that tells me\nRecorded operating state is listed above. I will not treat campaign intent, target lists, or mission artifacts as proof that Campaign 001 was mailed.',
    'What I cannot verify',
    missing + (unavailable ? '\n' + unavailable : ''),
    evidence ? `Evidence\n${evidence}` : '',
    'What I would need to investigate',
    'I do not need Scout to rediscover prospects or campaigns already in PulseForge. New specialist work is only warranted if you want additional accounts that are not already recorded.',
  ];

  if (recommend) {
    parts.push('Recommendation');
    parts.push(composeRecommendationFromEvidence(bundle, extras.businessUnderstanding || null));
  }

  return {
    prose: parts.filter(Boolean).join('\n\n'),
    used,
    knowledgeState: 'operating_evidence',
    recommend,
    launchedScout: false,
    items: bundle.items,
    contract: contract && contract.id,
    businessIntelligence: biMeta,
  };
}

function operatingStructured(answer, extras = {}) {
  const evidence = Array.isArray(extras.items)
    ? extras.items
        .filter((item) =>
          item.epistemic === EPISTEMIC.VERIFIED ||
          item.epistemic === EPISTEMIC.INFERRED ||
          item.epistemic === EPISTEMIC.OPERATOR_ATTESTED ||
          item.epistemic === EPISTEMIC.EXPECTED ||
          item.epistemic === EPISTEMIC.PLANNED
        )
        .map((item, idx) => ({
          id: `op-ev-${idx + 1}`,
          label: item.provenance,
          detail: item.claim,
          epistemic: item.epistemic,
          layer: item.layer,
        }))
    : [];
  return buildStructuredResponse({
    answer,
      reasoning: extras.reasoning || [
        extras.businessIntelligence
          ? 'Synthesized business intelligence from grounded operating evidence, then presented supporting evidence.'
          : 'Retrieved existing PulseForge operating evidence before recommending or delegating.',
      ],
    supportingEvidence: evidence,
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.86,
    nextInvestigations: extras.recommend || extras.claimChallenge
      ? []
      : ['Ask for a recommendation only after reviewing this inventory.'],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: extras.claimChallenge
      ? ['operating_evidence_retrieval', 'spec_105', 'spec_107a']
      : extras.recommend
        ? ['operating_evidence_retrieval', 'spec_105', 'spec_107']
        : extras.businessIntelligence
          ? ['operating_evidence_retrieval', 'spec_105', 'spec_110']
          : ['operating_evidence_retrieval', 'spec_105'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: Boolean(extras.used && extras.used.includes('campaignAo')),
        reasoning: Boolean(extras.recommend),
        memory: true,
        policy: true,
        knowledge: Boolean(extras.used && extras.used.includes('blueprint')),
      },
      evidenceCount: evidence.length,
      asOf: new Date().toISOString(),
      unavailable: extras.unavailable || [],
      cognitiveMode: extras.cognitiveMode || 'retrieval',
      retrievalBeforeDelegation: true,
      operatingEvidenceRetrieval: true,
      evidenceGroundedRecommendation: extras.recommend === true,
      claimChallenge: extras.claimChallenge === true,
      claimVerdict: extras.claimVerdict || null,
      executed: false,
      specialistDelegated: false,
      scoutDelegated: false,
      campaignMailExecuted: extras.mailExecuted === true,
      businessIntelligence: extras.businessIntelligence || null,
      businessIntelligenceSynthesis: Boolean(extras.businessIntelligence),
    },
  });
}

module.exports = {
  EPISTEMIC,
  CAMPAIGN_LAYER,
  PROVENANCE,
  isOperatingEvidenceQuestion,
  isOperatingGroundedRecommendation,
  isBareCurrentStateRecommendation,
  hasOperatingGrounding,
  shouldRetrieveOperatingEvidence,
  bundleHasUsableOperatingSignal,
  isInventoryOnlyRequest,
  isExistingKnowledgeInvestigate,
  isNewInvestigationRequest,
  isBusinessUnderstandingQuestion,
  loadOperatingEvidence,
  composeOperatingEvidenceAnswer,
  composeRecommendationFromEvidence,
  operatingStructured,
  resolveTenantId,
  resolveClientId,
  hasDurableMailExecution,
  scopedRows,
};
