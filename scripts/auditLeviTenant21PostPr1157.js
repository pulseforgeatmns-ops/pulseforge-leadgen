#!/usr/bin/env node
'use strict';

// P0-SCOUT-LEVI-001 post-PR #1157 audit for tenant 21 (Levi).
//
// READ ONLY. One `BEGIN READ ONLY` transaction, a SELECT/WITH-only statement
// guard, tenant-scoped SQL (client_id = 21 everywhere) and a ROLLBACK at the
// end. It never loads server.js, never calls Scout, SODA, Places or any other
// provider, never claims or replays a weekly cycle and never writes.
//
// Output is aggregate PASS / FAIL / UNVERIFIED checks. No names, addresses,
// emails, phones, URLs or raw metadata are emitted.
//
// Usage (operator):
//   AUDIT_DATABASE_URL=... node scripts/auditLeviTenant21PostPr1157.js \
//     --tenant 21 --confirm-read-only --expect-database=pulseforge_production
// Exit: 0 all checks PASS, 1 any FAIL, 2 access unavailable / blocked,
//       3 no FAIL but at least one UNVERIFIED.

const { TENANT_ID, evaluateProspect } = require('../services/leviScoutProductionContract');
const { identityKey } = require('../services/leviScoutResearchOnlyExecution');
const { planRow } = require('../services/leviScoutCityBackfill');
const { inferCityFromProvenance } = require('../services/leviScoutProvenance');
const { MAX_REQUESTS_PER_LOOKUP } = require('../services/leviScoutSodaClient');
const { AGENT } = require('../services/leviScoutEnablement');
const { SETTER_VISIBILITY_THRESHOLD } = require('../utils/qualificationThreshold');

const RESEARCH_SOURCE = 'scout_research_only';
const CLAIMED_CYCLE = 'levi-week-2026-11-23';
const EXPECTED = Object.freeze({ totalProspects: 29, researchOnly: 10, qualified: 0 });

// Every query is tenant-bound through $1. Optional tables run behind a
// savepoint so a missing table/column becomes UNVERIFIED, never a crash.
const QUERIES = Object.freeze({
  identity: `SELECT current_database() AS database,
      current_setting('transaction_read_only') AS tx_read_only,
      current_setting('server_version_num')::int AS server_version_num,
      $1::int AS tenant`,
  tenant: `SELECT id, active, enabled_agents FROM clients WHERE id = $1`,
  prospects: `SELECT p.id, p.client_id, p.source, p.status, p.first_name, p.last_name, p.email, p.phone,
      p.linkedin_url, p.job_title, p.notes, p.service_area_match, p.do_not_contact, p.setter_visible,
      p.setter_status, p.icp_score, p.ao_outreach_review_required, p.discovery_method,
      p.last_contacted_at, p.acquisition_metadata, p.created_at, c.name AS company_name,
      to_jsonb(p)->>'setter_visibility_reason' AS setter_visibility_reason,
      c.location AS company_location
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
    ORDER BY p.id`,
  agentLog: `SELECT action, status, payload->>'cycleId' AS cycle_id, payload->>'status' AS payload_status,
      payload->>'reason' AS reason, (payload->>'persisted')::text AS persisted,
      (payload->>'outreachAttempted')::text AS outreach_attempted,
      (payload->>'paidProvidersUsed')::text AS paid_providers_used,
      count(*)::int AS rows, min(ran_at) AS first_at, max(ran_at) AS last_at
    FROM agent_log
    WHERE client_id = $1 AND agent_name = '${AGENT}'
    GROUP BY 1,2,3,4,5,6,7,8 ORDER BY first_at`,
  briefings: `SELECT payload->>'cycleId' AS cycle_id, payload->>'status' AS cycle_status, status,
      count(*)::int AS rows, min(created_at) AS first_at, max(created_at) AS last_at
    FROM agent_actions
    WHERE client_id = $1 AND created_by = '${AGENT}' AND action_type = 'levi_scout_research_briefing'
    GROUP BY 1,2,3 ORDER BY first_at`,
  claimIndex: `SELECT count(*)::int AS unique_claim_indexes
    FROM pg_indexes
    WHERE schemaname = current_schema() AND tablename = 'agent_log'
      AND indexdef ILIKE '%UNIQUE%' AND indexdef ILIKE '%cycleId%' AND $1::int IS NOT NULL`,
  touchpoints: `SELECT channel, action_type, count(*)::int AS rows
    FROM touchpoints WHERE client_id = $1 GROUP BY 1,2 ORDER BY 3 DESC`,
  activityLog: `SELECT action_type, count(*)::int AS rows
    FROM activity_log WHERE client_id = $1 GROUP BY 1 ORDER BY 2 DESC`,
  // email_performance is deprecated (utils/emailPerformance.js); email_events is the source of truth.
  emailEvents: `SELECT event_type, count(*)::int AS rows FROM email_events WHERE client_id = $1 GROUP BY 1 ORDER BY 2 DESC`,
  outboundItems: `SELECT status, count(*)::int AS rows FROM acquisition_outbound_items WHERE tenant_id = $1::int::text GROUP BY 1`,
  scheduledSends: `SELECT count(*)::int AS rows FROM tenant_outreach_scheduled_sends WHERE tenant_id = $1::int::text`,
  sendPlans: `SELECT status, count(*)::int AS rows FROM emmett_send_plans WHERE tenant_id = $1::int::text OR client_id = $1::int GROUP BY 1`,
  outreachActions: `SELECT action_type, status, count(*)::int AS rows
    FROM agent_actions
    WHERE client_id = $1 AND action_type <> 'levi_scout_research_briefing'
      AND (action_type ILIKE '%outreach%' OR action_type ILIKE '%send%' OR action_type ILIKE '%email%'
        OR action_type ILIKE '%sms%' OR action_type ILIKE '%call%' OR action_type ILIKE '%handoff%'
        OR action_type ILIKE '%enroll%')
    GROUP BY 1,2 ORDER BY 3 DESC`,
});

const OPTIONAL = new Set(['tenant', 'briefings', 'claimIndex', 'touchpoints', 'activityLog', 'emailEvents', 'outboundItems', 'scheduledSends', 'sendPlans', 'outreachActions']);

function assertSelectOnly(sql) {
  const body = String(sql).replace(/--[^\n]*/g, '').trim();
  if (!/^(SELECT|WITH)\b/i.test(body) || body.includes(';')) throw new Error('audit_statement_not_read_only');
  if (/\b(INSERT|UPDATE|DELETE|MERGE|ALTER|DROP|CREATE|TRUNCATE|GRANT|REVOKE|COPY|CALL|DO|LOCK|VACUUM|REFRESH|SET|RESET)\b/i
    .test(body.replace(/'[^']*'/g, "''").replace(/current_setting\(/gi, 'cs('))) {
    throw new Error('audit_statement_not_read_only');
  }
  return sql;
}

function metadataOf(row) {
  const value = row && row.acquisition_metadata;
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_) { return {}; }
}

function check(id, status, evidence, note) {
  return Object.freeze({ id, status, evidence, ...(note ? { note } : {}) });
}

const countBy = (list, fn) => list.reduce((out, item) => {
  const key = String(fn(item));
  out[key] = (out[key] || 0) + 1;
  return out;
}, {});

function unavailable(section) {
  return !Array.isArray(section);
}

// Pure: sections -> checks. Exported for tests.
function evaluate(sections, { expectDatabase = null, criteria = null, criteriaStatus = 'not_loaded' } = {}) {
  const checks = [];
  const identity = Array.isArray(sections.identity) ? sections.identity[0] : null;

  // 0. Target and transaction mode
  if (!identity) {
    checks.push(check('target.database', 'UNVERIFIED', { reason: 'identity_unavailable' }));
  } else {
    const dbOk = !expectDatabase || identity.database === expectDatabase;
    checks.push(check('target.database', dbOk ? 'PASS' : 'FAIL',
      { database: identity.database, expected: expectDatabase || 'not_asserted' }));
    checks.push(check('target.read_only_transaction', identity.tx_read_only === 'on' ? 'PASS' : 'FAIL',
      { transactionReadOnly: identity.tx_read_only }));
  }

  if (unavailable(sections.prospects)) {
    checks.push(check('inventory.prospects', 'UNVERIFIED', { reason: sections.prospects && sections.prospects.unavailable }));
    return checks;
  }
  const all = sections.prospects;
  const foreign = all.filter(row => Number(row.client_id) !== Number(TENANT_ID));
  const research = all.filter(row => row.source === RESEARCH_SOURCE);

  // 1. Inventory checkpoint
  checks.push(check('inventory.tenant_scope', foreign.length === 0 ? 'PASS' : 'FAIL', { foreignRows: foreign.length }));
  checks.push(check('inventory.checkpoint',
    all.length === EXPECTED.totalProspects && research.length === EXPECTED.researchOnly ? 'PASS' : 'FAIL',
    { totalProspects: all.length, researchOnly: research.length, expected: EXPECTED, bySource: countBy(all, r => r.source || 'null') },
    'A change since the confirmed checkpoint means a cycle, import or manual write ran after it.'));

  // 2. City inference and provenance conflicts (research-only rows)
  const cityRows = research.map(row => {
    const metadata = metadataOf(row);
    const plan = planRow(row);
    const hostOnly = inferCityFromProvenance({
      sourceUrls: metadata.sourceUrls,
      location: row.service_area_match,
      serviceAreaMatch: row.service_area_match,
    });
    const stored = metadata.city ? String(metadata.city).trim() : '';
    const recordedConflict = Boolean(metadata.cityInference && metadata.cityInference.conflict);
    const evidenceConflict = Boolean(hostOnly.conflict || (stored && hostOnly.city && hostOnly.city !== stored));
    return { stored, plan, hostOnly, recordedConflict, evidenceConflict, inference: metadata.cityInference || null };
  });
  const cityFromConflict = cityRows.filter(r => r.stored && r.inference && r.inference.conflict === false && r.evidenceConflict);
  checks.push(check('city.no_city_invented_from_conflict', cityFromConflict.length === 0 ? 'PASS' : 'FAIL', {
    researchRows: cityRows.length,
    cityPresent: cityRows.filter(r => r.stored).length,
    cityMissing: cityRows.filter(r => !r.stored).length,
    byCity: countBy(cityRows.filter(r => r.stored), r => r.stored),
    byInferenceSource: countBy(cityRows, r => (r.inference && r.inference.source) || 'not_recorded'),
    inferredDespiteConflict: cityFromConflict.length,
  }));
  const unrecorded = cityRows.filter(r => r.evidenceConflict && !r.recordedConflict);
  checks.push(check('city.conflicts_recorded', unrecorded.length === 0 ? 'PASS' : 'FAIL', {
    evidenceConflicts: cityRows.filter(r => r.evidenceConflict).length,
    recordedConflicts: cityRows.filter(r => r.recordedConflict).length,
    conflictsWithoutRecord: unrecorded.length,
  }, 'Rows persisted before #1157 never ran the conflict recorder; a non-zero count here is a backfill gap, not a #1157 regression.'));
  checks.push(check('city.backfill_pending', 'PASS', {
    wouldWriteIfBackfillApproved: cityRows.filter(r => r.plan.wouldWrite).length,
    blockedByConflict: cityRows.filter(r => !r.stored && r.hostOnly.conflict).length,
  }, 'Informational: dry-run of services/leviScoutCityBackfill planRow. Nothing is written.'));

  // 3. Chicago SODA research evidence and completeness
  const chicago = research.filter(row => /chicago/i.test(String(metadataOf(row).city || '')))
    .map(row => ({ row, contact: metadataOf(row).contactResearch || null }));
  const lookups = chicago.filter(x => x.contact && x.contact.lookup);
  const withCompleteness = lookups.filter(x => x.contact.lookup.completeness);
  const zeroMatchOnIncomplete = withCompleteness.filter(x => x.contact.lookup.authoritativeZeroMatch === true
    && x.contact.lookup.completeness !== 'authoritative');
  const overBudget = lookups.filter(x => Number(x.contact.lookup.requests || 0) > MAX_REQUESTS_PER_LOOKUP);
  const fabricated = research.filter(row => {
    const c = metadataOf(row).contactResearch;
    return c && (c.fabricated === true || c.paidProvidersUsed === true
      || (c.verifiable === true && !c.phone && !c.email && !c.address));
  });
  checks.push(check('soda.request_budget', overBudget.length === 0 ? 'PASS' : 'FAIL',
    { chicagoRows: chicago.length, lookupsRecorded: lookups.length, overBudget: overBudget.length, maxPerLookup: MAX_REQUESTS_PER_LOOKUP }));
  checks.push(check('soda.incomplete_not_zero_match', zeroMatchOnIncomplete.length === 0 ? 'PASS' : 'FAIL', {
    completeness: countBy(lookups, x => x.contact.lookup.completeness || 'pre_1157_not_recorded'),
    authoritativeZeroMatch: lookups.filter(x => x.contact.lookup.authoritativeZeroMatch === true).length,
    zeroMatchOnIncomplete: zeroMatchOnIncomplete.length,
    ownersExactMatch: lookups.filter(x => x.contact.lookup.ownersExactMatch === true).length,
    licensesExactMatch: lookups.filter(x => x.contact.lookup.licensesExactMatch === true).length,
  }));
  checks.push(check('soda.evidence_present', chicago.length === 0 ? 'UNVERIFIED'
    : (withCompleteness.length === chicago.length ? 'PASS' : 'UNVERIFIED'), {
    chicagoRows: chicago.length,
    withContactResearch: chicago.filter(x => x.contact).length,
    withPost1157Completeness: withCompleteness.length,
  }, 'Rows researched before #1157 have no completeness field; they are not re-researched by this audit.'));
  checks.push(check('soda.no_fabrication_or_paid', fabricated.length === 0 ? 'PASS' : 'FAIL', {
    researchRows: research.length,
    verifiableContacts: research.filter(row => (metadataOf(row).contactResearch || {}).verifiable === true).length,
    violations: fabricated.length,
  }));

  // 4. Research-only classification
  const badResearch = research.filter(row => {
    const m = metadataOf(row);
    return row.setter_visible === true || row.ao_outreach_review_required !== true || m.outreachAuthorized !== false
      || m.importMode !== 'review_only' || m.verifiedBorrowerIntent === true || m.permitDoesNotEstablishIntent !== true
      || row.discovery_method !== 'open_data_permit';
  });
  checks.push(check('research_only.classification', badResearch.length === 0 ? 'PASS' : 'FAIL', {
    researchRows: research.length,
    violations: badResearch.length,
    bySourceType: countBy(research, row => metadataOf(row).sourceType || 'null'),
    byBucket: countBy(research, row => metadataOf(row).bucket || 'null'),
    byReviewState: countBy(research, row => metadataOf(row).reviewState || 'null'),
  }));

  // 5. Qualified-contact and setter-visibility gates
  // Same function and same criteria source the runner uses (approved Blueprint
  // range via loadApprovedCriteria; null when no approved range exists).
  const evaluated = all.map(row => evaluateProspect(row, { criteria }));
  const setterVisible = all.filter(row => row.setter_visible === true);
  const qualifiedBucket = research.filter(row => metadataOf(row).bucket === 'qualified_prospect');
  const storedNotRecomputed = all.filter((row, i) => metadataOf(row).bucket === 'qualified_prospect' && !evaluated[i].qualified);
  checks.push(check('gates.qualified_contacts',
    evaluated.filter(e => e.qualified).length === EXPECTED.qualified && storedNotRecomputed.length === 0 ? 'PASS' : 'FAIL', {
    contractQualified: evaluated.filter(e => e.qualified).length,
    expectedAtCheckpoint: EXPECTED.qualified,
    qualifiedBucket: qualifiedBucket.length,
    storedQualifiedButNotRecomputed: storedNotRecomputed.length,
    criteria: criteria ? { approved: true, minAmount: criteria.minAmount, maxAmount: criteria.maxAmount } : 'none_approved',
    criteriaStatus,
    reviewStates: countBy(evaluated, e => e.reviewState || 'null'),
    classifications: countBy(evaluated, e => e.classification),
    topBlockers: Object.entries(countBy(evaluated.flatMap(e => e.blockers), b => b)).sort((a, b) => b[1] - a[1]).slice(0, 8),
  }, 'Checkpoint is 0 qualified. Re-baseline if an approved Blueprint financing range is added; storedQualifiedButNotRecomputed must always be 0.'));
  checks.push(check('gates.setter_visibility', setterVisible.length === 0 ? 'PASS' : 'FAIL', {
    setterVisible: setterVisible.length,
    icpScoreAtLeast70: all.filter(row => Number(row.icp_score) >= 70).length,
    setterStatusNotNew: all.filter(row => (row.setter_status || 'new') !== 'new').length,
  }));
  // R8 invariant (utils/setterVisibility outreachReviewHeld): a row that still
  // requires outreach review is never setter-visible. FAIL if any held row is
  // visible now. Also report held rows the pre-fix gate (tenant-10-only hold)
  // would expose on a setter_handoff recompute, i.e. the risk while the old
  // code is deployed.
  const held = all.filter(row => row.ao_outreach_review_required === true);
  const heldVisible = held.filter(row => row.setter_visible === true);
  const preFixExposed = held.filter(row => !['dead', 'disqualified', 'bounced', 'do_not_email'].includes(String(row.status || ''))
    && row.do_not_contact !== true
    && Number(row.icp_score || 0) >= SETTER_VISIBILITY_THRESHOLD
    && String(row.service_area_match || '').trim().length > 0);
  checks.push(check('gates.review_hold_never_visible', heldVisible.length === 0 ? 'PASS' : 'FAIL', {
    heldRows: held.length,
    heldRowsSetterVisible: heldVisible.length,
    heldRowsExposedUnderPreFixGate: preFixExposed.length,
    preFixExposureBySource: countBy(preFixExposed, row => row.source || 'null'),
    preFixExposureByReviewState: countBy(preFixExposed, row => metadataOf(row).reviewState || 'null'),
    threshold: SETTER_VISIBILITY_THRESHOLD,
  }, 'heldRowsExposedUnderPreFixGate > 0 is a live risk until the R8 gate is deployed; do not run setter_handoff for this tenant before then.'));
  // 6. Outreach prevention and deduplication
  const contacted = all.filter(row => row.last_contacted_at);
  const tp = Array.isArray(sections.touchpoints) ? sections.touchpoints : null;
  const act = Array.isArray(sections.activityLog) ? sections.activityLog : null;
  const ee = Array.isArray(sections.emailEvents) ? sections.emailEvents : null;
  const oi = Array.isArray(sections.outboundItems) ? sections.outboundItems : null;
  const ss = Array.isArray(sections.scheduledSends) ? sections.scheduledSends : null;
  const sp = Array.isArray(sections.sendPlans) ? sections.sendPlans : null;
  const total = list => (list ? list.reduce((sum, r) => sum + r.rows, 0) : null);
  const oa = Array.isArray(sections.outreachActions) ? sections.outreachActions : null;
  const outboundTouchpoints = tp ? tp.filter(r => !/inbound|research|note|review/i.test(`${r.channel} ${r.action_type}`))
    .reduce((s, r) => s + r.rows, 0) : null;
  const outreachSignals = [contacted.length, outboundTouchpoints, act ? act.reduce((s, r) => s + r.rows, 0) : null,
    total(ee), total(oi), total(ss), total(sp), total(oa)];
  const outreachStatus = outreachSignals.some(v => Number(v) > 0) ? 'FAIL'
    : (outreachSignals.some(v => v == null) ? 'UNVERIFIED' : 'PASS');
  checks.push(check('outreach.none', outreachStatus, {
    prospectsWithLastContactedAt: contacted.length,
    outboundTouchpoints: outboundTouchpoints == null ? 'unavailable' : outboundTouchpoints,
    touchpointsByChannel: tp || 'unavailable',
    setterActivity: act || 'unavailable',
    emailEvents: ee || 'unavailable',
    outboundItems: oi || 'unavailable',
    scheduledSends: ss ? total(ss) : 'unavailable',
    sendPlans: sp || 'unavailable',
    outreachActionCards: oa || 'unavailable',
  }));
  const keyed = countBy(all, row => identityKey({
    ...row, name: [row.first_name, row.last_name].filter(Boolean).join(' '),
  }));
  const nameCompany = countBy(all, row => `${[row.first_name, row.last_name].filter(Boolean).join(' ').toLowerCase()}|${String(row.company_name || '').toLowerCase()}`);
  const dupKeys = Object.values(keyed).filter(n => n > 1).length;
  const dupNameCompany = Object.values(nameCompany).filter(n => n > 1).length;
  checks.push(check('dedup.no_duplicate_identities', dupKeys === 0 && dupNameCompany === 0 ? 'PASS' : 'FAIL',
    { duplicateIdentityKeyGroups: dupKeys, duplicateNameCompanyGroups: dupNameCompany }));
  checks.push(check('dedup.suppression', 'PASS', { doNotContact: all.filter(r => r.do_not_contact === true).length },
    'Informational.'));

  // 7. Weekly cycle state and bounded retry behaviour
  if (unavailable(sections.agentLog)) {
    checks.push(check('cycle.claimed_once', 'UNVERIFIED', { reason: sections.agentLog && sections.agentLog.unavailable }));
  } else {
    const log = sections.agentLog;
    const sum = (action, cycle) => log.filter(r => r.action === action && (!cycle || r.cycle_id === cycle)).reduce((s, r) => s + r.rows, 0);
    const claimsByCycle = log.filter(r => r.action === 'research_cycle_claim')
      .reduce((out, r) => ({ ...out, [r.cycle_id]: (out[r.cycle_id] || 0) + r.rows }), {});
    const diagnostics = log.filter(r => r.action === 'research_cycle_diagnostic');
    const claimed = claimsByCycle[CLAIMED_CYCLE] || 0;
    checks.push(check('cycle.claimed_once', claimed === 1 ? 'PASS' : 'FAIL',
      { cycle: CLAIMED_CYCLE, claims: claimed, claimsByCycle }));
    checks.push(check('cycle.no_double_claim', Object.values(claimsByCycle).every(n => n <= 1) ? 'PASS' : 'FAIL', { claimsByCycle }));
    const diagForCycle = diagnostics.filter(r => r.cycle_id === CLAIMED_CYCLE);
    checks.push(check('cycle.single_diagnostic', sum('research_cycle_diagnostic', CLAIMED_CYCLE) <= 1 ? 'PASS' : 'FAIL', {
      diagnostics: diagForCycle.map(r => ({ status: r.status, payloadStatus: r.payload_status, reason: r.reason,
        persisted: r.persisted, outreachAttempted: r.outreach_attempted, paidProvidersUsed: r.paid_providers_used, rows: r.rows })),
    }));
    checks.push(check('cycle.no_outreach_or_paid_in_diagnostics',
      diagnostics.every(r => r.outreach_attempted !== 'true' && r.paid_providers_used !== 'true') ? 'PASS' : 'FAIL',
      { diagnostics: diagnostics.length }));
    const nextWeekClaims = Object.keys(claimsByCycle).filter(id => id > CLAIMED_CYCLE);
    checks.push(check('cycle.no_later_cycle_claimed', nextWeekClaims.length === 0 ? 'PASS' : 'FAIL',
      { laterCycles: nextWeekClaims }, 'A later cycle appearing means the next weekly run has happened; re-baseline.'));
  }
  if (Array.isArray(sections.briefings)) {
    const replay = sections.briefings.filter(r => r.cycle_status === 'replay_blocked').reduce((s, r) => s + r.rows, 0);
    checks.push(check('cycle.replay_attempts_bounded', 'PASS', {
      briefings: sections.briefings.map(r => ({ cycle: r.cycle_id, cycleStatus: r.cycle_status, cardStatus: r.status, rows: r.rows })),
      replayBlockedBriefings: replay,
    }, 'Each Render restart or cron retry inside the week is blocked by the claim but still inserts a pending briefing card.'));
  } else {
    checks.push(check('cycle.replay_attempts_bounded', 'UNVERIFIED', { reason: sections.briefings && sections.briefings.unavailable }));
  }
  if (Array.isArray(sections.claimIndex)) {
    const n = sections.claimIndex[0].unique_claim_indexes;
    checks.push(check('cycle.claim_race_protection', n > 0 ? 'PASS' : 'FAIL', { uniqueClaimIndexes: n },
      'Without a unique index, INSERT ... WHERE NOT EXISTS can double-claim under concurrent startup catch-up + cron.'));
  }
  return checks;
}

function summarize(checks) {
  const counts = countBy(checks, c => c.status);
  const overall = counts.FAIL ? 'FAIL' : (counts.UNVERIFIED ? 'UNVERIFIED' : 'PASS');
  return { overall, counts };
}

async function collect(client, { expectDatabase = null } = {}) {
  const sections = {};
  let criteria = null;
  let criteriaStatus = 'not_loaded';
  await client.query('BEGIN TRANSACTION READ ONLY');
  try {
    await client.query("SET LOCAL statement_timeout = '15s'");
    await client.query("SET LOCAL lock_timeout = '2s'");
    for (const [name, sql] of Object.entries(QUERIES)) {
      assertSelectOnly(sql);
      await client.query(`SAVEPOINT a_${name}`);
      try {
        sections[name] = (await client.query(sql, [Number(TENANT_ID)])).rows;
        await client.query(`RELEASE SAVEPOINT a_${name}`);
      } catch (error) {
        await client.query(`ROLLBACK TO SAVEPOINT a_${name}`);
        sections[name] = { unavailable: error.code || 'query_failed', optional: OPTIONAL.has(name) };
      }
    }
    // Application qualification authority: the approved Blueprint range, loaded
    // the way the runner loads it, through a SELECT-only wrapper.
    await client.query('SAVEPOINT a_criteria');
    try {
      const { loadTenantUnderstanding } = require('../services/leviScoutBlueprintReadiness');
      const { fromUnderstanding } = require('../services/leviScoutApprovedCriteria');
      const { readOnlyQueryable } = require('../packages/max/workspace/OperationalInspection');
      const understanding = await loadTenantUnderstanding({
        db: readOnlyQueryable(client, { writesBlocked: 0, reads: 0 }), tenantId: TENANT_ID,
      });
      criteria = fromUnderstanding(understanding, TENANT_ID);
      criteriaStatus = understanding && understanding.loadError ? `load_error:${understanding.loadError}` : 'loaded';
    } catch (error) {
      criteriaStatus = `unavailable:${error.code || 'error'}`;
    }
    await client.query('ROLLBACK TO SAVEPOINT a_criteria').catch(() => {});
  } finally {
    await client.query('ROLLBACK').catch(() => {});
  }
  const checks = evaluate(sections, { expectDatabase, criteria, criteriaStatus });
  return {
    audit: 'levi_tenant21_post_pr1157',
    tenantId: Number(TENANT_ID),
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: summarize(checks),
    checks,
  };
}

function parseArgs(argv) {
  const args = { tenant: null, confirm: false, expectDatabase: null, allowAppCredential: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--tenant') args.tenant = argv[++i];
    else if (a === '--confirm-read-only') args.confirm = true;
    else if (a === '--allow-app-credential') args.allowAppCredential = true;
    else if (/^--expect-database=/.test(a)) args.expectDatabase = a.split('=')[1];
    else throw new Error('audit_argument_invalid');
  }
  if (String(args.tenant) !== String(TENANT_ID) || !args.confirm) throw new Error('explicit_tenant_confirmation_required');
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const url = process.env.AUDIT_DATABASE_URL || (args.allowAppCredential ? process.env.DATABASE_URL : '');
  if (!url) throw new Error('audit_database_url_required');
  const { Client } = require('pg');
  const client = new Client({ connectionString: url, connectionTimeoutMillis: 10000, application_name: 'levi_tenant21_post_pr1157_audit' });
  await client.connect();
  try {
    const report = await collect(client, { expectDatabase: args.expectDatabase });
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    process.exitCode = { PASS: 0, FAIL: 1, UNVERIFIED: 3 }[report.summary.overall];
  } finally {
    await client.end().catch(() => {});
  }
}

if (require.main === module) {
  main().catch(error => {
    const known = ['explicit_tenant_confirmation_required', 'audit_database_url_required', 'audit_argument_invalid', 'audit_statement_not_read_only'];
    console.error(JSON.stringify({ status: 'blocked', error: known.includes(error.message) ? error.message : (error.code || 'audit_unavailable') }));
    process.exitCode = 2;
  });
}

module.exports = { QUERIES, CLAIMED_CYCLE, EXPECTED, assertSelectOnly, evaluate, summarize, collect, parseArgs };
