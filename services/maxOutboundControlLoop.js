'use strict';

const { canonicalOutboundEmailIneligibilityReason } = require('../utils/canonicalEmailEligibility');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { adapters: createGovernedAdapters } = require('./governedOutboundAdapters');

const DEFAULT_TARGET_DAYS = 3;
const DEFAULT_ENRICHMENT_BATCH = 5;

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

async function loadCleanInventory(pool, store) {
  const { rows } = await pool.query(`
    SELECT p.*, c.name AS company_name, c.domain AS company_domain, c.website AS company_website
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
    const reason = canonicalOutboundEmailIneligibilityReason(row);
    const candidate = {
      candidateId: String(row.id),
      prospectId: String(row.id),
      companyId: String(row.company_id || ''),
      company: row.company_name,
      domain: row.company_domain,
      website: row.company_website,
      email: String(row.email || '').toLowerCase(),
    };
    const ownership = reason ? null : await store.candidateOwnership(candidate);
    const suppression = reason || ownership ? null : await store.suppression(candidate, '__max_inventory_buffer__');
    const blocked = reason || ownership || suppression;
    if (blocked) excluded.push({ prospectId: candidate.prospectId, reason: blocked });
    else clean.push(candidate);
  }
  return { clean, excluded };
}

function scoutInput(program, source, plan) {
  const structured = source?.payload?.structuredMission || source?.structuredMission || {};
  const geography = structured.geography || {};
  const market = structured.market || {};
  const cities = Array.isArray(geography.cities) ? geography.cities : [];
  const region = geography.region || cities.join(', ') || 'Greater Manchester';
  const segment = market.segment || 'short_term_rental';
  return {
    authorizedTenantId: '10',
    tenantId: '10',
    question: `Max needs Scout to replenish verified outbound inventory for ${segment} in ${region}.`,
    objective: `Find enough net-new, in-scope prospects to close an outbound inventory deficit of ${plan.deficit} while preserving ownership, prior-contact, DNC and suppression boundaries.`,
    reason: `Emmett safe daily capacity is ${plan.safeDailyCapacity}; Max requires a ${plan.targetDays}-day buffer of ${plan.targetInventory}, but only ${plan.cleanInventory} clean prospects are currently available.`,
    authority: 'observe',
    force: true,
    businessContext: {
      serviceGeography: region,
      commercialCapability: 'commercial_cleaning',
      preferredSegments: [segment],
      acquisitionDirection: source?.objective || source?.payload?.objective || null,
      exclusions: source?.payload?.constraints || [],
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

async function defaultScoutRamp({ pool, program, source, plan, logger = console }) {
  const enrichment = require('../scoutUnenrichedEnrichmentAgent');
  const enrichmentLimit = Math.min(
    DEFAULT_ENRICHMENT_BATCH,
    Math.max(1, plan.deficit)
  );
  const promoted = await enrichment.run({
    client_id: 10,
    limit: enrichmentLimit,
    retryHours: 1,
    db: pool,
  });

  let discovery = null;
  if (plan.deficit > Number(promoted?.promoted || 0)) {
    discovery = await require('./scoutAcquisitionIntelligence').runAcquisitionIntelligenceLoop(
      scoutInput(program, source, plan),
      {
        loadCompanies: async () => {
          const { rows } = await pool.query(`
            SELECT c.id,c.name,c.domain,c.website,p.vertical,p.icp_score,p.updated_at
            FROM companies c
            LEFT JOIN prospects p ON p.company_id=c.id AND p.client_id=c.client_id
            WHERE c.client_id=10
          `);
          return rows.map(row => ({
            id: String(row.id),
            tenantId: '10',
            name: row.name,
            website: row.website || (row.domain ? `https://${row.domain}` : null),
            industry: row.vertical || 'short_term_rental',
            icpScore: row.icp_score,
            updatedAt: row.updated_at,
          }));
        },
        enablePlaces: true,
      }
    );
  }
  logger.log?.('[max-outbound-control] Scout ramp', JSON.stringify({
    requested: plan.deficit,
    promoted: promoted?.promoted || 0,
    discovery: discovery?.kind || null,
  }));
  return { promoted, discovery };
}

async function runMaxOutboundControlLoop(options = {}) {
  const pool = options.pool || require('../db');
  const logger = options.logger || console;
  const store = options.store || new GovernedOutboundStore(pool);
  const program = options.program || await store.program();
  if (!program || ['paused', 'revoked'].includes(program.mode)) {
    return { halted: 'no_enabled_program' };
  }

  const governedAdapters = options.governedAdapters || createGovernedAdapters(pool);
  const infrastructure = options.infrastructure
    || await governedAdapters.infrastructure(program);
  const inventoryBefore = options.inventory
    || await loadCleanInventory(pool, store);
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
    const sourceRow = (await pool.query(
      "SELECT id,objective,payload FROM acquisition_missions WHERE tenant_id='10' AND id=$1",
      [program.source_mission_id]
    )).rows[0];
    const ramp = options.scoutRamp || defaultScoutRamp;
    scout = await ramp({ pool, program, source: sourceRow, plan, logger });
  }

  const inventoryAfter = options.inventoryAfter
    || (plan.shouldReplenish && options.execute !== false
      ? await loadCleanInventory(pool, store)
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
    emmettCapacity: infrastructure.cap,
    sentToday,
    bufferTarget: finalPlan.targetInventory,
    cleanInventoryBefore: inventoryBefore.clean.length,
    cleanInventoryAfter: inventoryAfter.clean.length,
    deficit: finalPlan.deficit,
    state: finalPlan.state,
    scoutInvoked: Boolean(scout),
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
  };
}

module.exports = {
  DEFAULT_TARGET_DAYS,
  buildControlPlan,
  loadCleanInventory,
  runMaxOutboundControlLoop,
  _test: { scoutInput },
};
