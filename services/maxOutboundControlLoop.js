'use strict';

const { canonicalOutboundEmailIneligibilityReason, normalizeDomain } = require('../utils/canonicalEmailEligibility');
const { normalizeVertical } = require('../utils/normalize');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { adapters: createGovernedAdapters } = require('./governedOutboundAdapters');

const DEFAULT_TARGET_DAYS = 3;
const DEFAULT_ENRICHMENT_BATCH = 5;
const MAX_ENRICHMENT_BATCHES_PER_CYCLE = 3;

function boundedInt(value, fallback, min = 1, max = 100) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

function buildControlPlan({
  dailyCap,
  emmettCapacity,
  sentToday = 0,
  cleanInventory = 0,
  targetDays = DEFAULT_TARGET_DAYS,
}) {
  const policyCap = Math.max(0, Number(dailyCap || 0));
  const infrastructureCap = Math.max(0, Number(emmettCapacity || 0));
  const safeDailyCapacity = Math.min(policyCap, infrastructureCap);
  const target = safeDailyCapacity * boundedInt(targetDays, DEFAULT_TARGET_DAYS, 1, 7);
  const clean = Math.max(0, Number(cleanInventory || 0));
  const deficit = Math.max(0, target - clean);
  const todayRemaining = Math.max(0, safeDailyCapacity - Math.max(0, Number(sentToday || 0)));

  let state = 'healthy';
  if (safeDailyCapacity <= 0) state = 'delivery_halted';
  else if (clean < safeDailyCapacity) state = 'critical';
  else if (clean < target) state = 'replenish';

  return {
    state,
    safeDailyCapacity,
    todayRemaining,
    targetDays: boundedInt(targetDays, DEFAULT_TARGET_DAYS, 1, 7),
    targetInventory: target,
    cleanInventory: clean,
    deficit,
    shouldReplenish: deficit > 0 && safeDailyCapacity > 0,
  };
}

function sourceScope(source) {
  const payload = source?.payload || source || {};
  const structured = payload.structuredMission || {};
  const market = structured.market || {};
  const geography = structured.geography || {};
  return {
    segment: normalizeVertical(market.segment || payload.targetSegment || ''),
    industry: normalizeVertical(market.industry || ''),
    region: geography.region || null,
    cities: Array.isArray(geography.cities) ? geography.cities.map(x => String(x).toLowerCase()) : [],
  };
}

function segmentAliases(scope) {
  const aliases = new Set([scope.segment, scope.industry].filter(Boolean));
  if (scope.segment === 'short_term_rental' || scope.segment === 'short_term_rental_operators') {
    ['short_term_rental', 'str_manager', 'property_manager', 'property_management', 'hospitality']
      .forEach(x => aliases.add(x));
  }
  return aliases;
}

function missionCandidateReason(row, scope) {
  if (row.service_area_match !== true) return 'service_area_not_confirmed';
  const vertical = normalizeVertical(row.vertical || row.industry || '');
  const aliases = segmentAliases(scope);
  if (aliases.size && (!vertical || !aliases.has(vertical))) return 'mission_segment_mismatch';
  return null;
}

async function loadSource(pool, program) {
  return (await pool.query(
    "SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id='10' AND id=$1",
    [program.source_mission_id]
  )).rows[0] || null;
}

async function loadCleanInventory(pool, store, source) {
  const scope = sourceScope(source);
  const { rows } = await pool.query(`
    SELECT p.*, c.name AS company_name, c.domain AS company_domain, c.website AS company_website,
      c.industry AS industry, c.location AS company_location
    FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=10
      AND p.email IS NOT NULL
      AND COALESCE(p.do_not_contact,false)=false
    ORDER BY p.updated_at DESC NULLS LAST, p.id
  `);

  const clean = [];
  const excluded = [];
  for (const row of rows) {
    const missionReason = missionCandidateReason(row, scope);
    const emailReason = missionReason ? null : canonicalOutboundEmailIneligibilityReason(row);
    const candidate = {
      candidateId: String(row.id),
      prospectId: String(row.id),
      companyId: String(row.company_id || ''),
      company: row.company_name,
      domain: row.company_domain,
      website: row.company_website,
      email: String(row.email || '').toLowerCase(),
    };
    const ownership = missionReason || emailReason ? null : await store.candidateOwnership(candidate);
    const suppression = missionReason || emailReason || ownership
      ? null
      : await store.suppression(candidate, '__max_inventory_buffer__');
    const blocked = missionReason || emailReason || ownership || suppression;
    if (blocked) excluded.push({ prospectId: candidate.prospectId, reason: blocked });
    else clean.push(candidate);
  }
  return { clean, excluded, scope };
}

function scoutInput(program, source, plan) {
  const scope = sourceScope(source);
  const payload = source?.payload || {};
  const region = scope.region || scope.cities.join(', ') || 'Greater Manchester';
  const segment = scope.segment || 'short_term_rental';
  return {
    authorizedTenantId: '10',
    tenantId: '10',
    workflow: 'outbound_inventory_replenishment',
    inventoryDeficit: plan.deficit,
    question: `Max needs Scout to replenish verified outbound inventory for ${segment} in ${region}.`,
    objective: `Find enough net-new, in-scope prospects to close an outbound inventory deficit of ${plan.deficit} while preserving ownership, prior-contact, DNC and suppression boundaries.`,
    reason: `Emmett safe daily capacity is ${plan.safeDailyCapacity}; Max requires a ${plan.targetDays}-day buffer of ${plan.targetInventory}, but only ${plan.cleanInventory} clean prospects are currently available.`,
    authority: 'observe',
    force: true,
    businessContext: {
      serviceGeography: region,
      commercialCapability: 'commercial_cleaning',
      preferredSegments: [segment],
      acquisitionDirection: source?.objective || payload.objective || null,
      exclusions: payload.constraints || [],
    },
    targetContext: {
      geography: region,
      segments: [segment],
      businessType: 'commercial_cleaning',
      desiredSignals: ['decision_maker', 'service_gap', 'portfolio_growth', 'turnover_support'],
    },
    operatorDirection: 'Maintain verified inventory ahead of governed outbound demand. Do not contact prospects.',
  };
}

async function persistDiscoveredCompanies(pool, store, { companies = [] }) {
  let inserted = 0;
  for (const company of companies) {
    const name = String(company.name || '').trim();
    const website = String(company.website || '').trim() || null;
    const domain = normalizeDomain(company.domain || website);
    if (!name || !domain) continue;
    const ownership = await store.candidateOwnership({ company: name, domain, website });
    if (ownership) continue;
    const result = await pool.query(`
      INSERT INTO scout_unenriched (
        client_id, company, website_url, domain, vertical, location, source,
        enrichment_attempts, last_attempt_at, notes
      )
      SELECT 10,$1,$2,$3,$4,$5,'max_buffer_replenishment',0,NULL,$6
      WHERE NOT EXISTS (
        SELECT 1 FROM scout_unenriched
        WHERE client_id=10 AND (
          lower(domain)=lower($3) OR lower(trim(company))=lower(trim($1))
        )
      )
      RETURNING id
    `, [
      name,
      website || `https://${domain}`,
      domain,
      normalizeVertical(company.industry || 'short_term_rental'),
      company.location || null,
      'Discovered by Max-directed Scout inventory replenishment; no contact performed.',
    ]);
    inserted += result.rowCount;
  }
  return { inserted };
}

function mapReuseCompanyRows(rows) {
  return rows.map(row => ({
    id: String(row.id),
    tenantId: '10',
    name: row.name,
    website: row.website || (row.domain ? `https://${row.domain}` : null),
    industry: row.vertical || 'short_term_rental',
    location: row.location || null,
    icpScore: row.icp_score,
    updatedAt: row.updated_at,
  }));
}

async function loadReuseCompanies(pool) {
  const { rows } = await pool.query(`
    SELECT c.id,c.name,c.domain,c.website,c.location,p.vertical,p.icp_score,p.updated_at
    FROM companies c
    LEFT JOIN prospects p ON p.company_id=c.id AND p.client_id=c.client_id
    WHERE c.client_id=10
  `);
  return mapReuseCompanyRows(rows);
}

async function runEnrichmentBatches(enrichment, pool, requested) {
  const summaries = [];
  let promoted = 0;
  const batches = Math.min(
    MAX_ENRICHMENT_BATCHES_PER_CYCLE,
    Math.ceil(Math.max(0, requested) / DEFAULT_ENRICHMENT_BATCH)
  );
  for (let i = 0; i < batches; i += 1) {
    const remaining = Math.max(1, requested - promoted);
    const summary = await enrichment.run({
      client_id: 10,
      limit: Math.min(DEFAULT_ENRICHMENT_BATCH, remaining),
      retryHours: 1,
      db: pool,
    });
    summaries.push(summary);
    promoted += Number(summary?.promoted || 0);
    if (!summary?.considered) break;
  }
  return { promoted, summaries };
}

async function defaultScoutRamp({ pool, store, program, source, plan, logger = console }) {
  const enrichment = require('../scoutUnenrichedEnrichmentAgent');
  const first = await runEnrichmentBatches(enrichment, pool, plan.deficit);
  let promoted = first.promoted;
  let discovery = null;
  let persisted = { inserted: 0 };

  if (promoted < plan.deficit) {
    discovery = await require('./scoutAcquisitionIntelligence').runAcquisitionIntelligenceLoop(
      scoutInput(program, source, plan),
      {
        loadCompanies: async () => loadReuseCompanies(pool),
        persistCompanies: async input => {
          persisted = await persistDiscoveredCompanies(pool, store, input);
          return persisted;
        },
        enablePlaces: true,
      }
    );

    if (persisted.inserted > 0 && promoted < plan.deficit) {
      const second = await runEnrichmentBatches(enrichment, pool, plan.deficit - promoted);
      promoted += second.promoted;
      first.summaries.push(...second.summaries);
    }
  }

  logger.log?.('[max-outbound-control] Scout ramp', JSON.stringify({
    requested: plan.deficit,
    promoted,
    discoveredQueued: persisted.inserted,
    discovery: discovery?.kind || null,
  }));
  return {
    promoted,
    enrichmentBatches: first.summaries,
    discoveredQueued: persisted.inserted,
    discovery,
  };
}

async function runMaxOutboundControlLoop(options = {}) {
  const pool = options.pool || require('../db');
  const logger = options.logger || console;
  const store = options.store || new GovernedOutboundStore(pool);
  const program = options.program || await store.program();
  if (!program || ['paused', 'revoked'].includes(program.mode)) {
    return { halted: 'no_enabled_program' };
  }

  const source = options.source || await loadSource(pool, program);
  if (!source) return { halted: 'source_mission_missing', programId: program.id };

  const governedAdapters = options.governedAdapters || createGovernedAdapters(pool);
  const infrastructure = options.infrastructure
    || await governedAdapters.infrastructure(program);
  const inventoryBefore = options.inventory
    || await loadCleanInventory(pool, store, source);
  const sentToday = Number(infrastructure?.snapshot?.sentToday || 0);
  const plan = buildControlPlan({
    dailyCap: program.policy.dailyCap,
    emmettCapacity: infrastructure.cap,
    sentToday,
    cleanInventory: inventoryBefore.clean.length,
    targetDays: options.targetDays || process.env.ANCHOR_MAX_OUTBOUND_BUFFER_DAYS || DEFAULT_TARGET_DAYS,
  });

  let scout = null;
  if (plan.shouldReplenish && options.execute !== false) {
    const ramp = options.scoutRamp || defaultScoutRamp;
    scout = await ramp({ pool, store, program, source, plan, logger });
  }

  const inventoryAfter = options.inventoryAfter
    || (plan.shouldReplenish && options.execute !== false
      ? await loadCleanInventory(pool, store, source)
      : inventoryBefore);
  const finalPlan = buildControlPlan({
    dailyCap: program.policy.dailyCap,
    emmettCapacity: infrastructure.cap,
    sentToday,
    cleanInventory: inventoryAfter.clean.length,
    targetDays: plan.targetDays,
  });

  await store.event('max_outbound_control', [
    program.id,
    new Date().toISOString().slice(0, 13),
    finalPlan.state,
  ], {
    programId: program.id,
    policyHash: program.policy_hash,
    sourceMissionId: program.source_mission_id,
    emmettCapacity: infrastructure.cap,
    sentToday,
    bufferTarget: finalPlan.targetInventory,
    cleanInventoryBefore: inventoryBefore.clean.length,
    cleanInventoryAfter: inventoryAfter.clean.length,
    deficit: finalPlan.deficit,
    state: finalPlan.state,
    scoutInvoked: Boolean(scout),
    scoutPromoted: Number(scout?.promoted || 0),
    scoutQueued: Number(scout?.discoveredQueued || 0),
  });

  return {
    programId: program.id,
    mode: program.mode,
    emmett: {
      safeCapacity: infrastructure.cap,
      governor: infrastructure.assessed?.governor?.outcome || null,
      healthScore: infrastructure.assessed?.health?.score || null,
    },
    plan: finalPlan,
    scout,
    excludedCount: inventoryAfter.excluded.length,
    sourceScope: inventoryAfter.scope,
  };
}

module.exports = {
  DEFAULT_TARGET_DAYS,
  MAX_ENRICHMENT_BATCHES_PER_CYCLE,
  buildControlPlan,
  sourceScope,
  missionCandidateReason,
  loadCleanInventory,
  runMaxOutboundControlLoop,
  _test: {
    scoutInput,
    mapReuseCompanyRows,
    loadReuseCompanies,
    persistDiscoveredCompanies,
    runEnrichmentBatches,
  },
};
