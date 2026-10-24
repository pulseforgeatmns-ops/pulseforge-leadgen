'use strict';

const { COST_SQL, LIMITS } = require('../utils/apiSpendControl');
const DAY = 86400000;
const midnight = date => new Date(`${date.toISOString().slice(0, 10)}T00:00:00Z`);
const money = value => value == null ? 'unknown' : `$${Number(value).toFixed(2)}`;
const num = value => Number(value || 0);
const UNMETERED = ['google_custom_search', 'prospeo', 'hunter', 'serpapi', 'brevo', 'twilio', 'bland'];

function aggregate(rows) {
  const requests = rows.reduce((n, r) => n + num(r.requests), 0);
  const unknownCostRequests = rows.reduce((n, r) => n + num(r.unknown_cost_requests), 0);
  const knownGrossUsd = rows.reduce((n, r) => n + num(r.known_gross_usd), 0);
  return { requests, knownGrossUsd, unknownCostRequests, estimatedGrossUsd: unknownCostRequests ? null : knownGrossUsd };
}
function grouped(rows, key) {
  const groups = new Map();
  for (const row of rows) {
    const label = row[key] ?? 'unattributed';
    if (!groups.has(label)) groups.set(label, []);
    groups.get(label).push(row);
  }
  return [...groups].map(([name, values]) => ({ name, ...aggregate(values) }))
    .sort((a, b) => b.knownGrossUsd - a.knownGrossUsd || b.requests - a.requests);
}
function buildReport({ rows, policies = [], availability, earliest, requestedSince, until, now, tenantId = null, duplicateRequests = 0 }) {
  const select = since => rows.filter(r => since === requestedSince && r.in_requested != null ? r.in_requested : `${r.day}T00:00:00.000Z` >= since && `${r.day}T00:00:00.000Z` < until);
  const todayStart = midnight(now).toISOString();
  const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString();
  const windowRows = select(requestedSince);
  const periods = {
    today: { since: todayStart, until, ...aggregate(select(todayStart)) },
    monthToDate: { since: monthStart, until, ...aggregate(select(monthStart)) },
    requested: { since: requestedSince, until, ...aggregate(windowRows) },
  };
  const alerts = [];
  const providerSummary = ['google_places', 'anthropic'].map(provider => {
    const source = provider === 'google_places' ? availability.places || availability.events : availability.events;
    const providerRows = rows.filter(r => r.provider === provider);
    const policy = policies.find(p => p.provider === provider) || null;
    const daily = aggregate(providerRows.filter(r => `${r.day}T00:00:00.000Z` >= todayStart));
    const monthly = aggregate(providerRows.filter(r => `${r.day}T00:00:00.000Z` >= monthStart));
    if (!source) alerts.push({ provider, code: 'metering_unavailable' });
    const limitsConfigured = Boolean(policy && LIMITS.some(key => policy[key] != null));
    if (!limitsConfigured && !policy?.paused) alerts.push({ provider, code: 'budget_not_configured' });
    if (policy?.paused) alerts.push({ provider, code: 'provider_paused' });
    if (source && !earliest[provider]) alerts.push({ provider, code: 'no_usage_observations' });
    if (earliest[provider] && earliest[provider] > requestedSince) alerts.push({ provider, code: 'partial_history', since: earliest[provider] });
    if (providerRows.some(r => r.status === 'reserved')) alerts.push({ provider, code: 'unsettled_requests' });
    if (providerRows.some(r => num(r.unknown_cost_requests) > 0)) alerts.push({ provider, code: 'unpriced_usage' });
    if (policy && tenantId == null) for (const key of LIMITS) {
      const period = key.startsWith('daily') ? daily : monthly;
      const limit = policy[key] == null ? null : Number(policy[key]);
      const used = key.endsWith('request_limit') ? period.requests : period.estimatedGrossUsd;
      if (limit !== null && (used == null || limit === 0 || used / limit >= 0.8)) {
        alerts.push({ provider, code: used == null ? 'budget_usage_unknown' : (used >= limit ? 'budget_reached' : 'budget_near_limit'), limitType: key, used, limit });
      }
    }
    const requested = aggregate(windowRows.filter(r => r.provider === provider));
    if (!source) { daily.estimatedGrossUsd = null; monthly.estimatedGrossUsd = null; requested.estimatedGrossUsd = null; }
    return { provider, metering: source ? 'application_ledger' : 'unavailable',
      firstObservedAt: earliest[provider] || null, billingStatus: 'not_reconciled', policy, limitsConfigured,
      today: daily, monthToDate: monthly, requested };
  });
  if (!availability.places && !availability.events) for (const p of Object.values(periods)) p.estimatedGrossUsd = null;
  return {
    budgetScope: 'all_tenants', budgetAlertsEvaluated: tenantId == null,
    generatedAt: now.toISOString(), timezone: 'UTC', scope: tenantId == null ? 'all_tenants' : 'tenant', tenantId,
    billing: { status: 'not_reconciled', actualUsd: null, note: 'Payment receipts are not usage for a date range. Reconcile Google billing by project and SKU; app keys cannot read billing.' },
    estimateBasis: 'Gross application estimates before free tiers, credits, discounts and taxes. Places use an upper field-tier bound. Unknown costs remain unknown.',
    guardStatus: availability.events && availability.policies ? 'available' : 'not_installed',
    periods, providers: providerSummary, alerts,
    byDay: grouped(windowRows, 'day'), byAgent: grouped(windowRows, 'agent_name'),
    byTenant: grouped(windowRows, 'tenant_id'), byMission: grouped(windowRows, 'mission_id'),
    byOperation: grouped(windowRows, 'operation'), byModel: grouped(windowRows.filter(r => r.model), 'model'),
    duplicatePlacesRequests: duplicateRequests,
    unmeteredProviders: UNMETERED,
    automaticControl: 'Every covered paid attempt reserves capacity before network I/O. Configured caps block further attempts. Max may tighten or pause; only operators may raise or resume.',
  };
}

async function getApiUsageReport({ db = require('../db'), tenantId = null, days = 14, now = new Date() } = {}) {
  if (!Number.isInteger(days) || days < 1 || days > 90) throw new Error('invalid_report_days');
  if (!(now instanceof Date) || !Number.isFinite(now.getTime())) throw new Error('invalid_report_date');
  if (tenantId != null && (!Number.isSafeInteger(Number(tenantId)) || Number(tenantId) <= 0)) throw new Error('invalid_report_tenant');
  const until = now.toISOString();
  // Calendar-day summaries are labelled UTC throughout (including quota resets).
  const requestedSince = new Date(now.getTime() - days * DAY).toISOString();
  const monthSince = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString();
  const since = requestedSince < monthSince ? requestedSince : monthSince;
  const schema = (await db.query(`SELECT to_regclass('public.places_api_requests') AS places,
    to_regclass('public.api_usage_events') AS events, to_regclass('public.api_spend_policies') AS policies`)).rows[0];
  const availability = { places: Boolean(schema?.places), events: Boolean(schema?.events), policies: Boolean(schema?.policies) };
  const rows = [], earliest = {};
  const params = [since, until, tenantId == null ? null : Number(tenantId)];
  if (availability.events) {
    const firstRows = (await db.query('SELECT provider, MIN(created_at) AS first_at FROM api_usage_events GROUP BY provider')).rows;
    for (const r of firstRows) earliest[r.provider] = new Date(r.first_at).toISOString();
    rows.push(...(await db.query(`SELECT provider, agent_name, tenant_id, mission_id, operation, model, status,
      to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day, created_at >= $4 AS in_requested, COUNT(*)::int AS requests,
      COALESCE(SUM(COALESCE(estimated_usd, reserved_usd)),0) AS known_gross_usd,
      COUNT(*) FILTER (WHERE COALESCE(estimated_usd, reserved_usd) IS NULL)::int AS unknown_cost_requests
      FROM api_usage_events WHERE created_at >= $1 AND created_at < $2 AND ($3::int IS NULL OR tenant_id=$3)
      GROUP BY provider,agent_name,tenant_id,mission_id,operation,model,status,day,in_requested`, [...params, requestedSince])).rows);
  }
  let duplicateRequests = 0;
  if (availability.places) {
    const cutoff = earliest.google_places || until;
    const first = (await db.query('SELECT MIN(created_at) AS first_at FROM places_api_requests')).rows[0]?.first_at;
    if (first && (!earliest.google_places || new Date(first).toISOString() < earliest.google_places)) earliest.google_places = new Date(first).toISOString();
    rows.push(...(await db.query(`SELECT 'google_places' AS provider, caller AS agent_name, tenant_id, mission_id, endpoint AS operation,
      NULL AS model, 'historical' AS status, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day, created_at >= $5 AS in_requested,
      COUNT(*)::int AS requests, COALESCE(SUM(${COST_SQL}),0) AS known_gross_usd,
      COUNT(*) FILTER (WHERE (${COST_SQL}) IS NULL)::int AS unknown_cost_requests
      FROM places_api_requests WHERE cache_hit IS NOT TRUE AND created_at >= $1 AND created_at < $2
        AND ($3::int IS NULL OR tenant_id=$3) AND created_at < $4
      GROUP BY caller,tenant_id,mission_id,endpoint,day,in_requested`, [...params, cutoff, requestedSince])).rows);
    duplicateRequests = num((await db.query(`SELECT COALESCE(SUM(n-1),0) AS duplicates FROM (
      SELECT COUNT(*) AS n FROM places_api_requests WHERE cache_hit IS NOT TRUE
        AND normalized_query IS NOT NULL AND created_at >= $1 AND created_at < $2 AND ($3::int IS NULL OR tenant_id=$3)
      GROUP BY tenant_id,endpoint,normalized_query HAVING COUNT(*) > 1) q`, [requestedSince, until, params[2]])).rows[0]?.duplicates);
  }
  const policies = availability.policies ? (await db.query('SELECT * FROM api_spend_policies ORDER BY provider')).rows : [];
  return buildReport({ rows, policies, availability, earliest, requestedSince, until, now, tenantId, duplicateRequests });
}
function formatApiUsageBrief(report) {
  const p = report.periods;
  const lines = [
    `API usage (${report.timezone}; ${p.requested.since.slice(0,10)} through ${p.requested.until.slice(0,10)}).`,
    `Gross estimates: today ${money(p.today.estimatedGrossUsd)}; month to date ${money(p.monthToDate.estimatedGrossUsd)}; selected period ${money(p.requested.estimatedGrossUsd)} (${p.requested.requests} paid attempts).`,
    ...report.providers.map(p => `${p.provider}: ${p.metering}; selected-period gross estimate ${money(p.requested.estimatedGrossUsd)}; ${p.policy?.paused ? 'paused' : p.limitsConfigured ? 'limits configured' : 'no spending limits configured'}.`),
    'Actual provider bill: not reconciled. These estimates do not establish the amount billed.',
    `Other APIs without this meter: ${report.unmeteredProviders.join(', ')}.`,
  ];
  if (report.alerts.length) lines.push(`Attention: ${report.alerts.map(a => `${a.provider}: ${a.code}`).join('; ')}.`);
  const top = report.byAgent.slice(0,3);
  if (top.length) lines.push(`Largest tracked callers: ${top.map(r => `${r.name} (${r.requests} attempts, ${money(r.estimatedGrossUsd)})`).join('; ')}.`);
  if (report.duplicatePlacesRequests) lines.push(`${report.duplicatePlacesRequests} repeated uncached Places requests in this window; inspect cache and replenishment cadence.`);
  lines.push('Configured caps stop paid calls before network I/O. Max can tighten or pause; an operator must raise or resume.');
  return lines.join('\n');
}
function isApiUsageQuestion(question) {
  return /\b(api\s+(?:usage|spend(?:ing)?|costs?|billing|budgets?)|(?:usage|spend|costs?|billing|budget)\s+(?:of|for|on)?\s*(?:our\s+)?apis?|google\s+(?:places|cloud).*(?:costs?|spend(?:ing)?|usage|bill(?:ing)?|budgets?)|(?:claude|anthropic|model).*(?:costs?|spend(?:ing)?|usage|bill(?:ing)?|budgets?))\b/i.test(String(question));
}
function parseApiSpendCommand(question) {
  const text = String(question).trim().replace(/^max[,!:]?\s+/i, '');
  const providerFor = name => /google/i.test(name) ? 'google_places' : 'anthropic';
  const pause = text.match(/^(?:pause|stop)\s+(google places|claude|anthropic)(?:\s+api)?(?:\s+requests)?[.!]?$/i);
  if (pause) return { provider: providerFor(pause[1]), patch: { paused: true } };
  const cap = text.match(/^(?:cap|limit)\s+(google places|claude|anthropic)(?:\s+api)?\s+(?:at|to)\s+\$(\d+(?:\.\d{1,2})?)\s+(?:per|a)\s+(day|month)[.!]?$/i);
  return cap ? { provider: providerFor(cap[1]), patch: { [cap[3].toLowerCase() === 'day' ? 'daily_usd_limit' : 'monthly_usd_limit']: Number(cap[2]) } } : null;
}
async function answerApiUsageQuestion(question, deps = {}) {
  const command = parseApiSpendCommand(question);
  let policy = null;
  if (command) policy = await require('../utils/apiSpendControl').updateApiPolicy(command.provider, command.patch, { db: deps.db, actor: 'max' });
  const report = await getApiUsageReport(deps);
  const prose = [policy ? `Max applied a tighter ${command.provider} policy.` : '', formatApiUsageBrief(report)].filter(Boolean).join('\n');
  return { ok: true, answer: prose, reply: prose, prose, route: 'api_usage', structured: { answer: prose, apiUsage: report },
    metadata: { presentation: 'deterministic', apiUsage: true }, sources: ['api_usage_events','places_api_requests','api_spend_policies'], apiUsage: report };
}

module.exports = { aggregate, buildReport, getApiUsageReport, formatApiUsageBrief, isApiUsageQuestion, parseApiSpendCommand, answerApiUsageQuestion };
