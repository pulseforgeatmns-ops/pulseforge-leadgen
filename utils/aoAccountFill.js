'use strict';

const {
  routeProspect,
  companyName,
  inServiceArea,
} = require('../services/aoProspectRoutingService');
const {
  createTaskFromRouting,
  persistRouting,
} = require('../services/aoProspectTaskService');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const { addBusinessDays, todayISOInZone, distributeDueDates } = require('../utils/aoAssignment');
const { CRM_NEXT_TO_LEGACY } = require('./aoCrmTypes');

/** Minimum assigned accounts visible in AO CRM "My Accounts" after a fill run. */
const DEFAULT_MIN_ACCOUNTS_PER_AO = 10;
const DEFAULT_TARGET_ACCOUNTS_PER_AO = 15;
const DEFAULT_TODAY_QUEUE_SIZE = 5;

/**
 * SPEC-250 Anchor allocation segments (by AO display name, case-insensitive).
 * Jake is intentionally omitted from bulk fill — warm relationships stay operator-owned.
 */
const AO_SEGMENT_AFFINITY = Object.freeze({
  zach: Object.freeze(['property_manager', 'str_manager', 'commercial_real_estate']),
  tony: Object.freeze(['property_manager', 'str_manager', 'commercial_office', 'medical_dental', 'dental']),
  rory: Object.freeze(['property_manager', 'law_firm', 'accounting', 'commercial_office', 'medical_dental', 'dental']),
  mike: Object.freeze(['law_firm', 'accounting', 'commercial_office', 'property_manager']),
});

const BULK_FILL_EXCLUDED_AO_NAMES = new Set(['jake']);

function normalizeAoKey(name) {
  return String(name || '').trim().toLowerCase();
}

function segmentAffinityForAo(aoName) {
  const key = normalizeAoKey(aoName).split(/\s+/)[0];
  return AO_SEGMENT_AFFINITY[key] || null;
}

function scoreProspectForAo({ prospect, company, aoName }) {
  const vertical = String(prospect?.vertical || '').trim();
  const icp = Number(prospect?.icp_score || 0);
  const affinity = segmentAffinityForAo(aoName);
  let score = icp;

  if (affinity && affinity.includes(vertical)) score += 25;
  else if (affinity) score -= 8;

  if (aoName && normalizeAoKey(aoName).startsWith('rory')) {
    if (['law_firm', 'accounting', 'commercial_office', 'medical_dental', 'dental'].includes(vertical)) {
      score += 12;
    }
    if (vertical === 'property_manager' && icp >= 70 && icp < 90) score += 10;
    if (vertical === 'property_manager' && icp >= 90) score -= 5;
  }

  if (aoName && normalizeAoKey(aoName).startsWith('zach')) {
    if (vertical === 'property_manager' && icp >= 88) score += 15;
    if (['law_firm', 'accounting'].includes(vertical)) score -= 10;
  }

  const loc = `${prospect?.service_area_match || ''} ${company?.location || ''}`.toLowerCase();
  if (loc.includes('manchester')) score += 3;

  const phoneDigits = String(prospect?.phone || '').replace(/\D/g, '');
  if (phoneDigits.length >= 10) score += 5;

  return score;
}

async function countCrmVisibleAccounts({ clientId, aoUserId, db }) {
  const { rows } = await db.query(`
    SELECT COUNT(*)::int AS n
    FROM prospects p
    WHERE p.client_id = $1
      AND p.assigned_ao_id = $2
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
  `, [clientId, aoUserId]);
  return rows[0]?.n || 0;
}

async function fetchActiveAos(clientId, db) {
  const { rows } = await db.query(`
    SELECT id, name, email, territory, active
    FROM users
    WHERE client_id = $1 AND role = 'ao' AND active = true
    ORDER BY id ASC
  `, [clientId]);
  return rows.filter(row => !BULK_FILL_EXCLUDED_AO_NAMES.has(normalizeAoKey(row.name)));
}

async function fetchUnassignedCandidates({ clientId, limit, db }) {
  const { rows } = await db.query(`
    SELECT
      p.*,
      c.name AS company_name,
      c.location AS company_location,
      c.website AS company_website,
      c.industry AS company_industry
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND p.assigned_ao_id IS NULL
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.prospect_motion, '') NOT IN ('SUPPRESS', 'EMAIL_LED')
      AND COALESCE(p.icp_score, 0) >= 70
    ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
    LIMIT $2
  `, [clientId, limit]);
  return rows.map(row => ({
    prospect: row,
    company: row.company_id ? {
      name: row.company_name,
      location: row.company_location,
      website: row.company_website,
      industry: row.company_industry,
    } : null,
  }));
}

function initCrmFieldsForAssignment({ rankAmongNew, today, timeZone = 'America/New_York' }) {
  const inTodayQueue = rankAmongNew < DEFAULT_TODAY_QUEUE_SIZE;
  const dueDates = distributeDueDates(DEFAULT_TODAY_QUEUE_SIZE, { today, timeZone });
  if (inTodayQueue) {
    const due = dueDates[rankAmongNew] || today;
    return {
      ao_current_status: 'ready_to_call',
      ao_next_action: 'call',
      next_action_due_at: new Date(`${due}T17:00:00`).toISOString(),
    };
  }
  const futureDue = addBusinessDays(today, 2 + rankAmongNew, timeZone);
  return {
    ao_current_status: 'researching',
    ao_next_action: 'research_contact',
    next_action_due_at: new Date(`${futureDue}T17:00:00`).toISOString(),
  };
}

async function applyProspectAssignment({
  clientId,
  prospectId,
  routing,
  aoName,
  crmInit,
  db,
}) {
  await persistRouting(prospectId, clientId, routing, db);
  await createTaskFromRouting({
    clientId,
    prospectId,
    routing,
    deadline: crmInit.next_action_due_at?.slice(0, 10) || null,
    db,
  });
  const legacyNext = CRM_NEXT_TO_LEGACY[crmInit.ao_next_action] || null;
  await db.query(`
    UPDATE prospects SET
      ao_current_status = $3,
      ao_next_action = $4,
      next_action = COALESCE(next_action, $5),
      next_action_due_at = $6,
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [
    prospectId,
    clientId,
    crmInit.ao_current_status,
    crmInit.ao_next_action,
    legacyNext,
    crmInit.next_action_due_at,
  ]);
  return {
    prospect_id: prospectId,
    account: routing.reasoning?.account,
    ao_name: aoName,
    ao_id: routing.recommended_ao_id,
    crm: crmInit,
  };
}

async function fillActiveAoAccounts({
  clientId,
  db,
  dryRun = true,
  minPerAo = DEFAULT_MIN_ACCOUNTS_PER_AO,
  targetPerAo = DEFAULT_TARGET_ACCOUNTS_PER_AO,
  maxCandidateScan = 400,
} = {}) {
  if (!clientId) throw new Error('clientId required');
  await ensureAoProspectRoutingSchema(db);

  const aos = await fetchActiveAos(clientId, db);
  const today = todayISOInZone();
  const gaps = [];
  for (const ao of aos) {
    const current = await countCrmVisibleAccounts({ clientId, aoUserId: ao.id, db });
    const need = Math.max(0, targetPerAo - current);
    gaps.push({ ao, current, need });
  }

  const totalNeed = gaps.reduce((sum, g) => sum + g.need, 0);
  if (totalNeed === 0) {
    return { dryRun, assigned: [], skipped: [], gaps, message: 'All active AOs meet minimum account counts.' };
  }

  const candidates = await fetchUnassignedCandidates({
    clientId,
    limit: Math.max(maxCandidateScan, totalNeed * 3),
    db,
  });

  const assigned = [];
  const skipped = [];
  const reservedProspectIds = new Set();

  for (const gap of gaps) {
    if (gap.need <= 0) continue;
    const { ao } = gap;
    let rank = 0;
    const ranked = candidates
      .filter(({ prospect }) => !reservedProspectIds.has(prospect.id))
      .map(bundle => ({
        ...bundle,
        aoScore: scoreProspectForAo({ prospect: bundle.prospect, company: bundle.company, aoName: ao.name }),
      }))
      .filter(({ prospect, company }) => {
        if (!inServiceArea(prospect, company)) return false;
        const routing = routeProspect({
          prospect,
          company,
          touchpoints: [],
          availableAos: [{ id: ao.id, name: ao.name, active: true, open_task_count: 0 }],
          aoName: ao.name,
          existingAssignment: prospect,
        });
        if (['SUPPRESS', 'EMAIL_LED'].includes(routing.recommended_motion)) return false;
        if (!routing.recommended_ao_id) return false;
        return true;
      })
      .sort((a, b) => b.aoScore - a.aoScore);

    let filled = 0;
    for (const bundle of ranked) {
      if (filled >= gap.need) break;
      const { prospect, company } = bundle;
      const routing = routeProspect({
        prospect,
        company,
        touchpoints: [],
        availableAos: [{ id: ao.id, name: ao.name, active: true, open_task_count: filled }],
        aoName: ao.name,
        existingAssignment: prospect,
      });
      routing.recommended_ao_id = ao.id;
      routing.recommended_ao_name = ao.name;

      const crmInit = initCrmFieldsForAssignment({ rankAmongNew: rank, today });
      rank += 1;

      if (dryRun) {
        assigned.push({
          dry_run: true,
          prospect_id: prospect.id,
          company: companyName(prospect, company),
          ao_id: ao.id,
          ao_name: ao.name,
          vertical: prospect.vertical,
          icp_score: prospect.icp_score,
          ao_score: bundle.aoScore,
          motion: routing.recommended_motion,
          crm: crmInit,
        });
      } else {
        const result = await applyProspectAssignment({
          clientId,
          prospectId: prospect.id,
          routing,
          aoName: ao.name,
          crmInit,
          db,
        });
        assigned.push(result);
      }
      reservedProspectIds.add(prospect.id);
      filled += 1;
    }

    if (filled < gap.need) {
      skipped.push({
        ao_id: ao.id,
        ao_name: ao.name,
        requested: gap.need,
        filled,
        reason: 'insufficient_eligible_candidates',
      });
    }
  }

  return { dryRun, assigned, skipped, gaps, today };
}

async function deprioritizeAccountForContactResearch({
  clientId,
  prospectId,
  reason = 'bad_contact_info',
  db,
}) {
  const futureDue = addBusinessDays(todayISOInZone(), 5);
  await db.query(`
    UPDATE prospects SET
      ao_current_status = 'researching',
      ao_next_action = 'research_contact',
      ao_paused = true,
      help_requested = false,
      help_reason = $3,
      next_action_due_at = $4,
      ao_disqualification_reason = COALESCE(ao_disqualification_reason, $3),
      updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [
    prospectId,
    clientId,
    reason,
    new Date(`${futureDue}T17:00:00`).toISOString(),
  ]);
  await db.query(`
    UPDATE ao_prospect_tasks
    SET deadline = $3, priority = 'normal'
    WHERE client_id = $1 AND prospect_id = $2::uuid AND status IN ('open', 'in_progress')
  `, [clientId, prospectId, futureDue]);
}

async function assertMinimumAoAccountCounts({
  clientId,
  db,
  minPerAo = DEFAULT_MIN_ACCOUNTS_PER_AO,
  excludeAoNames = BULK_FILL_EXCLUDED_AO_NAMES,
} = {}) {
  const aos = await fetchActiveAos(clientId, db);
  const failures = [];
  const counts = [];

  for (const ao of aos) {
    if (excludeAoNames.has(normalizeAoKey(ao.name))) continue;
    const count = await countCrmVisibleAccounts({ clientId, aoUserId: ao.id, db });
    counts.push({ ao_id: ao.id, ao_name: ao.name, count });
    if (count < minPerAo) {
      failures.push({ ao_id: ao.id, ao_name: ao.name, count, minimum: minPerAo });
    }
  }

  return {
    ok: failures.length === 0,
    counts,
    failures,
  };
}

module.exports = {
  DEFAULT_MIN_ACCOUNTS_PER_AO,
  DEFAULT_TARGET_ACCOUNTS_PER_AO,
  DEFAULT_TODAY_QUEUE_SIZE,
  AO_SEGMENT_AFFINITY,
  BULK_FILL_EXCLUDED_AO_NAMES,
  segmentAffinityForAo,
  scoreProspectForAo,
  countCrmVisibleAccounts,
  fillActiveAoAccounts,
  deprioritizeAccountForContactResearch,
  assertMinimumAoAccountCounts,
};
