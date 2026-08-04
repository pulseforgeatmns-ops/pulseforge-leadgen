'use strict';

/**
 * SPEC-071 — Market Intelligence briefing / query surfaces.
 * Read-only synthesis over the observational corpus.
 * Not raw evidence (isEvidence: false). No scoring, recommendations, or writes.
 */

const defaultPool = require('../db');
const {
  buildMarketIntelReadinessReport,
} = require('./marketIntelligenceReadiness');
const {
  getCompanyCampaignTimeline,
  diffTimelineField,
} = require('./marketIntelligenceQuery');

const DEFAULT_DAYS = 30;
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;
const EXAMPLE_REF_CAP = 5;
const CHANGE_FIELDS = ['cta', 'offer', 'positioning'];
const THEME_FIELDS = ['positioning', 'urgency', 'social_proof', 'headline', 'guarantee'];
const MIN_CHANGE_CONFIDENCE_SAMPLES = 2;

function clampLimit(limit, fallback = DEFAULT_LIMIT) {
  const n = Number(limit);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), MAX_LIMIT);
}

function clampDays(days, fallback = DEFAULT_DAYS) {
  const n = Number(days);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), 3650);
}

function toIso(value) {
  if (value == null) return null;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function resolveWindow(options = {}) {
  const until = options.until ? new Date(options.until) : new Date();
  const untilSafe = Number.isNaN(until.getTime()) ? new Date() : until;

  let since;
  if (options.since) {
    since = new Date(options.since);
    if (Number.isNaN(since.getTime())) {
      since = new Date(untilSafe.getTime() - clampDays(options.days) * 86400000);
    }
  } else {
    const days = clampDays(options.days);
    since = new Date(untilSafe.getTime() - days * 86400000);
  }

  const ms = untilSafe.getTime() - since.getTime();
  const days = Math.max(1, Math.round(ms / 86400000) || clampDays(options.days));

  return {
    days,
    since: since.toISOString(),
    until: untilSafe.toISOString(),
    sinceDate: since,
    untilDate: untilSafe,
  };
}

/**
 * Shared filter clause for windowed market_emails joins.
 * Observations are scoped via email received_at (corpus time axis).
 */
function buildEmailWindowFilters(options = {}, { alias = 'e', startParam = 1 } = {}) {
  const window = resolveWindow(options);
  const where = [
    `${alias}.received_at >= $${startParam}`,
    `${alias}.received_at <= $${startParam + 1}`,
  ];
  const params = [window.sinceDate, window.untilDate];
  let next = startParam + 2;

  if (options.companyId) {
    where.push(`${alias}.company_id = $${next}`);
    params.push(String(options.companyId).trim());
    next += 1;
  }

  const intent = options.importIntent || options.intent;
  if (intent) {
    where.push(`${alias}.import_intent = $${next}`);
    params.push(String(intent).trim());
    next += 1;
  }

  return { where, params, nextParam: next, window };
}

function uniqStrings(values, cap = EXAMPLE_REF_CAP) {
  const out = [];
  const seen = new Set();
  for (const raw of values || []) {
    if (raw == null) continue;
    const v = String(raw);
    if (!v || seen.has(v)) continue;
    seen.add(v);
    out.push(v);
    if (out.length >= cap) break;
  }
  return out;
}

function normalizeCompanies(values) {
  return uniqStrings(
    (values || []).filter((v) => v != null && String(v).trim() !== ''),
    25
  );
}

async function getCorpusSummary(options = {}) {
  const pool = options.pool || defaultPool;
  const { where, params, window } = buildEmailWindowFilters(options);
  const obsFilters = buildEmailWindowFilters(options, { alias: 'e2', startParam: 1 });

  const result = await pool.query(
    `SELECT
       COUNT(DISTINCT e.id)::int AS email_count,
       COUNT(DISTINCT e.company_id)::int AS company_count,
       (
         SELECT COUNT(*)::int
           FROM market_observations o
           JOIN market_emails e2 ON e2.id = o.email_id
          WHERE ${obsFilters.where.join(' AND ')}
       ) AS observation_count,
       COALESCE(
         ARRAY_AGG(DISTINCT e.import_intent ORDER BY e.import_intent)
           FILTER (WHERE e.import_intent IS NOT NULL),
         ARRAY[]::text[]
       ) AS import_intents
     FROM market_emails e
     WHERE ${where.join(' AND ')}`,
    params
  );

  const row = result.rows[0] || {};
  let readinessStatus = null;
  try {
    const readiness = await buildMarketIntelReadinessReport({ pool });
    readinessStatus = readiness.status || null;
  } catch {
    readinessStatus = null;
  }

  return {
    emailCount: Number(row.email_count || 0),
    companyCount: Number(row.company_count || 0),
    observationCount: Number(row.observation_count || 0),
    importIntents: Array.isArray(row.import_intents) ? row.import_intents : [],
    readinessStatus,
    window,
  };
}

/**
 * Aggregate observation field values (offers, CTAs, themes).
 */
async function aggregateObservationField(field, options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit);
  const { where, params, nextParam, window } = buildEmailWindowFilters(options);
  const category = options.category ? String(options.category).trim() : null;

  const filterParams = params.slice();
  filterParams.push(field);
  const fieldIdx = nextParam;
  let categoryClause = '';
  if (category) {
    filterParams.push(category);
    categoryClause = `AND o.category = $${fieldIdx + 1}`;
  }
  filterParams.push(limit);

  const result = await pool.query(
    `SELECT
       o.value_text AS label,
       COUNT(*)::int AS count,
       ARRAY_AGG(DISTINCT c.name) FILTER (
         WHERE c.name IS NOT NULL AND COALESCE(c.is_unknown, FALSE) = FALSE
       ) AS companies,
       MAX(e.received_at) AS latest_observed_at,
       (ARRAY_AGG(o.id::text ORDER BY e.received_at DESC, o.extracted_at DESC))[1:${EXAMPLE_REF_CAP}]
         AS example_observation_ids,
       (ARRAY_AGG(e.id::text ORDER BY e.received_at DESC, o.extracted_at DESC))[1:${EXAMPLE_REF_CAP}]
         AS example_email_ids
     FROM market_observations o
     JOIN market_emails e ON e.id = o.email_id
     JOIN market_companies c ON c.id = o.company_id
     WHERE ${where.join(' AND ')}
       AND o.field = $${fieldIdx}
       AND o.value_text IS NOT NULL
       AND TRIM(o.value_text) <> ''
       AND LOWER(TRIM(o.value_text)) <> 'none'
       ${categoryClause}
     GROUP BY o.value_text
     ORDER BY COUNT(*) DESC, MAX(e.received_at) DESC, o.value_text ASC
     LIMIT $${filterParams.length}`,
    filterParams
  );

  return {
    window,
    items: result.rows.map((row) => ({
      label: row.label,
      count: Number(row.count || 0),
      companies: normalizeCompanies(row.companies),
      latestObservedAt: toIso(row.latest_observed_at),
      exampleObservationIds: uniqStrings(row.example_observation_ids),
      exampleEmailIds: uniqStrings(row.example_email_ids),
    })),
  };
}

async function getTopOffers(options = {}) {
  const { window, items } = await aggregateObservationField('offer', options);
  return items.map((item) => ({
    label: item.label,
    count: item.count,
    companies: item.companies,
    latestObservedAt: item.latestObservedAt,
    exampleObservationIds: item.exampleObservationIds,
    exampleEmailIds: item.exampleEmailIds,
  }));
}

async function getTopCtas(options = {}) {
  const { items } = await aggregateObservationField('cta', options);
  return items.map((item) => ({
    cta: item.label,
    count: item.count,
    companies: item.companies,
    latestObservedAt: item.latestObservedAt,
    exampleObservationIds: item.exampleObservationIds,
  }));
}

async function getCompanyCadence(options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit);
  const { where, params, nextParam } = buildEmailWindowFilters(options);
  const queryParams = params.concat([limit]);

  const result = await pool.query(
    `SELECT
       c.id AS company_id,
       c.name AS company_name,
       COUNT(DISTINCT e.id)::int AS email_count,
       COUNT(o.id)::int AS observation_count,
       MIN(e.received_at) AS first_observed_at,
       MAX(e.received_at) AS last_observed_at,
       (ARRAY_AGG(e.import_intent ORDER BY e.received_at DESC)
         FILTER (WHERE e.import_intent IS NOT NULL))[1] AS import_intent
     FROM market_emails e
     JOIN market_companies c ON c.id = e.company_id
     LEFT JOIN market_observations o ON o.email_id = e.id
     WHERE ${where.join(' AND ')}
       AND COALESCE(c.is_unknown, FALSE) = FALSE
     GROUP BY c.id, c.name
     ORDER BY COUNT(DISTINCT e.id) DESC, MAX(e.received_at) DESC, c.name ASC
     LIMIT $${nextParam}`,
    queryParams
  );

  return result.rows.map((row) => ({
    companyId: row.company_id,
    companyName: row.company_name,
    emailCount: Number(row.email_count || 0),
    observationCount: Number(row.observation_count || 0),
    firstObservedAt: toIso(row.first_observed_at),
    lastObservedAt: toIso(row.last_observed_at),
    importIntent: row.import_intent || null,
  }));
}

async function getMessagingThemes(options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit);
  const { where, params, nextParam, window } = buildEmailWindowFilters(options);
  const category = options.category ? String(options.category).trim() : null;

  const filterParams = params.slice();
  filterParams.push(THEME_FIELDS);
  const fieldsIdx = nextParam;
  let categoryClause = '';
  if (category) {
    filterParams.push(category);
    categoryClause = `AND o.category = $${fieldsIdx + 1}`;
  }
  filterParams.push(limit);

  // Theme axis: category + field when category-scoped; otherwise field:value.
  const themeExpr = category
    ? `o.category || ':' || o.field || ':' || o.value_text`
    : `o.field || ':' || o.value_text`;

  const result = await pool.query(
    `SELECT
       ${themeExpr} AS theme,
       COUNT(*)::int AS count,
       ARRAY_AGG(DISTINCT c.name) FILTER (
         WHERE c.name IS NOT NULL AND COALESCE(c.is_unknown, FALSE) = FALSE
       ) AS companies,
       (ARRAY_AGG(o.id::text ORDER BY e.received_at DESC, o.extracted_at DESC))[1:${EXAMPLE_REF_CAP}]
         AS example_observation_ids
     FROM market_observations o
     JOIN market_emails e ON e.id = o.email_id
     JOIN market_companies c ON c.id = o.company_id
     WHERE ${where.join(' AND ')}
       AND o.field = ANY($${fieldsIdx}::text[])
       AND o.value_text IS NOT NULL
       AND TRIM(o.value_text) <> ''
       AND LOWER(TRIM(o.value_text)) <> 'none'
       ${categoryClause}
     GROUP BY 1
     ORDER BY COUNT(*) DESC, theme ASC
     LIMIT $${filterParams.length}`,
    filterParams
  );

  return {
    window,
    items: result.rows.map((row) => ({
      theme: row.theme,
      count: Number(row.count || 0),
      companies: normalizeCompanies(row.companies),
      exampleObservationIds: uniqStrings(row.example_observation_ids),
    })),
  };
}

async function getObservationsByIntent(options = {}) {
  const pool = options.pool || defaultPool;
  const { where, params } = buildEmailWindowFilters(options);

  const result = await pool.query(
    `SELECT
       e.import_intent AS import_intent,
       COUNT(DISTINCT e.id)::int AS email_count,
       COUNT(o.id)::int AS observation_count,
       COUNT(DISTINCT e.company_id)::int AS company_count,
       MAX(e.received_at) AS latest_observed_at
     FROM market_emails e
     LEFT JOIN market_observations o ON o.email_id = e.id
     WHERE ${where.join(' AND ')}
     GROUP BY e.import_intent
     ORDER BY COUNT(DISTINCT e.id) DESC, e.import_intent ASC NULLS LAST`,
    params
  );

  return result.rows.map((row) => ({
    importIntent: row.import_intent || 'unknown',
    emailCount: Number(row.email_count || 0),
    observationCount: Number(row.observation_count || 0),
    companyCount: Number(row.company_count || 0),
    latestObservedAt: toIso(row.latest_observed_at),
  }));
}

function midpointSplit(timeline) {
  if (!timeline || timeline.length < MIN_CHANGE_CONFIDENCE_SAMPLES) return null;
  const mid = Math.floor(timeline.length / 2);
  if (mid < 1 || mid >= timeline.length) return null;
  return {
    previous: timeline.slice(0, mid),
    recent: timeline.slice(mid),
  };
}

function modeFromTouches(touches, prop) {
  const counts = new Map();
  for (const t of touches || []) {
    const v = t[prop];
    if (v == null || v === '') continue;
    const key = String(v);
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  let best = null;
  let bestCount = 0;
  for (const [key, count] of counts) {
    if (count > bestCount) {
      best = key;
      bestCount = count;
    }
  }
  return best;
}

function avgCadence(touches) {
  const nums = (touches || [])
    .map((t) => t.cadenceDaysFromPrevious)
    .filter((n) => n != null && Number.isFinite(n));
  if (!nums.length) return null;
  return Math.round((nums.reduce((a, b) => a + b, 0) / nums.length) * 10) / 10;
}

/**
 * Heuristic recent messaging changes per company.
 * Empty + caveat when corpus is too thin for safe diffs.
 */
async function getRecentMessagingChanges(options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit);
  const window = resolveWindow(options);
  const caveats = [];

  // Prefer active companies in-window; fall back empty if none.
  const cadence = await getCompanyCadence({ ...options, pool, limit: Math.max(limit, 15) });
  if (!cadence.length) {
    caveats.push('recent_changes_unavailable: no company email activity in the selected window');
    return { items: [], caveats };
  }

  const changes = [];
  for (const company of cadence) {
    if (changes.length >= limit) break;

    const timeline = await getCompanyCampaignTimeline(company.companyId, {
      pool,
      limit: 100,
    });
    const inWindow = timeline.filter((t) => {
      const at = new Date(t.receivedAt);
      return at >= window.sinceDate && at <= window.untilDate;
    });
    const split = midpointSplit(inWindow.length >= MIN_CHANGE_CONFIDENCE_SAMPLES ? inWindow : timeline);
    if (!split) continue;

    const supporting = [];
    for (const field of CHANGE_FIELDS) {
      const propMap = { cta: 'cta', offer: 'offer', positioning: 'positioning' };
      const prop = propMap[field];
      const prevMode = modeFromTouches(split.previous, prop);
      const recentMode = modeFromTouches(split.recent, prop);
      if (!prevMode || !recentMode || prevMode === recentMode) continue;

      const diffs = diffTimelineField(split.previous.concat(split.recent), field);
      for (const d of diffs) {
        if (d.toEmailId) supporting.push(d.toEmailId);
        if (d.fromEmailId) supporting.push(d.fromEmailId);
      }

      const changeType =
        field === 'cta' ? 'cta_changed' : field === 'offer' ? 'new_offer_appeared' : 'message_pattern_changed';

      changes.push({
        companyId: company.companyId,
        companyName: company.companyName,
        changeType,
        summary: `${field} shifted from "${prevMode}" to "${recentMode}"`,
        previousWindow: {
          since: toIso(split.previous[0]?.receivedAt),
          until: toIso(split.previous[split.previous.length - 1]?.receivedAt),
          sampleSize: split.previous.length,
          value: prevMode,
        },
        recentWindow: {
          since: toIso(split.recent[0]?.receivedAt),
          until: toIso(split.recent[split.recent.length - 1]?.receivedAt),
          sampleSize: split.recent.length,
          value: recentMode,
        },
        confidence:
          split.previous.length >= 2 && split.recent.length >= 2 ? 'medium' : 'low',
        supportingObservationIds: uniqStrings(supporting, EXAMPLE_REF_CAP),
      });
      break;
    }

    if (changes.length >= limit) break;

    // Cadence increase heuristic (descriptive only).
    const prevCadence = avgCadence(split.previous);
    const recentCadence = avgCadence(split.recent);
    if (
      prevCadence != null &&
      recentCadence != null &&
      prevCadence > 0 &&
      recentCadence > 0 &&
      recentCadence <= prevCadence * 0.6 &&
      split.recent.length >= 2
    ) {
      changes.push({
        companyId: company.companyId,
        companyName: company.companyName,
        changeType: 'cadence_increased',
        summary: `Average gap between emails shortened from ${prevCadence}d to ${recentCadence}d`,
        previousWindow: {
          since: toIso(split.previous[0]?.receivedAt),
          until: toIso(split.previous[split.previous.length - 1]?.receivedAt),
          sampleSize: split.previous.length,
          avgCadenceDays: prevCadence,
        },
        recentWindow: {
          since: toIso(split.recent[0]?.receivedAt),
          until: toIso(split.recent[split.recent.length - 1]?.receivedAt),
          sampleSize: split.recent.length,
          avgCadenceDays: recentCadence,
        },
        confidence: 'low',
        supportingObservationIds: uniqStrings(
          split.recent.map((t) => t.id),
          EXAMPLE_REF_CAP
        ),
      });
    }
  }

  if (!changes.length) {
    caveats.push(
      'recent_changes_unavailable: not enough contrasting observations to report messaging shifts safely'
    );
  }

  return { items: changes.slice(0, limit), caveats };
}

function buildCaveats({ corpus, sections }) {
  const caveats = [];
  if (!corpus.emailCount) {
    caveats.push('empty_corpus: no market emails in the selected window');
  }
  if (corpus.emailCount > 0 && !corpus.observationCount) {
    caveats.push('no_observations: emails exist but extraction has not produced observations yet');
  }
  if (corpus.readinessStatus && corpus.readinessStatus !== 'ready') {
    caveats.push(`corpus_readiness:${corpus.readinessStatus}`);
  }
  if (!(sections.topOffers || []).length && corpus.observationCount > 0) {
    caveats.push('thin_offers: no offer observations matched the selected filters');
  }
  if (!(sections.topCtas || []).length && corpus.observationCount > 0) {
    caveats.push('thin_ctas: no CTA observations matched the selected filters');
  }
  if (!(sections.messagingThemes || []).length && corpus.observationCount > 0) {
    caveats.push('thin_themes: no messaging theme observations matched the selected filters');
  }
  if (!(sections.companyCadence || []).length && corpus.emailCount > 0) {
    caveats.push('thin_cadence: no known companies with email activity in the selected window');
  }
  caveats.push(
    'synthesis_not_evidence: briefing aggregates observational corpus; isEvidence is false'
  );
  return caveats;
}

/**
 * Full Market Intelligence briefing (synthesis, not raw evidence).
 */
async function getMarketIntelligenceBriefing(options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit);
  const generatedAt = new Date().toISOString();
  const opts = { ...options, pool, limit };

  const corpus = await getCorpusSummary(opts);
  const window = corpus.window;

  const [
    topOffers,
    topCtas,
    messagingThemesResult,
    companyCadence,
    recentChangesResult,
    observationsByIntent,
  ] = await Promise.all([
    getTopOffers(opts),
    getTopCtas(opts),
    getMessagingThemes(opts).then((r) => r.items),
    getCompanyCadence(opts),
    getRecentMessagingChanges(opts),
    getObservationsByIntent(opts),
  ]);

  const sections = {
    topOffers,
    topCtas,
    messagingThemes: messagingThemesResult,
    companyCadence,
    recentChanges: recentChangesResult.items,
    observationsByIntent,
  };

  const caveats = [
    ...buildCaveats({ corpus, sections }),
    ...(recentChangesResult.caveats || []),
  ];

  return {
    ok: true,
    kind: 'market_intelligence_briefing',
    isEvidence: false,
    generatedAt,
    window: {
      days: window.days,
      since: window.since,
      until: window.until,
    },
    corpus: {
      emailCount: corpus.emailCount,
      companyCount: corpus.companyCount,
      observationCount: corpus.observationCount,
      importIntents: corpus.importIntents,
      readinessStatus: corpus.readinessStatus,
    },
    sections,
    caveats: [...new Set(caveats)],
    internal: true,
    observationalOnly: true,
  };
}

function formatBriefingReport(briefing) {
  const c = briefing.corpus || {};
  const s = briefing.sections || {};
  const w = briefing.window || {};
  const lines = [
    'Market Intelligence Briefing',
    `Status: ${c.readinessStatus || 'unknown'}`,
    `Window: last ${w.days || '?'} days (${w.since || '?'} → ${w.until || '?'})`,
    `Generated: ${briefing.generatedAt || ''}`,
    `isEvidence: ${briefing.isEvidence === false ? 'false' : String(briefing.isEvidence)}`,
    '',
    'Corpus:',
    `- Emails: ${Number(c.emailCount || 0).toLocaleString('en-US')}`,
    `- Observations: ${Number(c.observationCount || 0).toLocaleString('en-US')}`,
    `- Companies: ${Number(c.companyCount || 0).toLocaleString('en-US')}`,
  ];

  if (c.importIntents && c.importIntents.length) {
    lines.push(`- Import intents: ${c.importIntents.join(', ')}`);
  }

  const listSection = (title, items, formatter) => {
    lines.push('', `${title}:`);
    if (!items || !items.length) {
      lines.push('(none)');
      return;
    }
    items.forEach((item, i) => {
      lines.push(`${i + 1}. ${formatter(item)}`);
    });
  };

  listSection('Top Offers', s.topOffers, (item) => {
    const companies = (item.companies || []).slice(0, 3).join(', ') || 'n/a';
    return `${item.label} (${item.count}; ${companies})`;
  });

  listSection('Top CTAs', s.topCtas, (item) => {
    const companies = (item.companies || []).slice(0, 3).join(', ') || 'n/a';
    return `${item.cta} (${item.count}; ${companies})`;
  });

  listSection('Messaging Themes', s.messagingThemes, (item) => {
    const companies = (item.companies || []).slice(0, 3).join(', ') || 'n/a';
    return `${item.theme} (${item.count}; ${companies})`;
  });

  listSection('Most Active Companies', s.companyCadence, (item) => {
    return `${item.companyName} — ${item.emailCount} emails, ${item.observationCount} observations`;
  });

  listSection('Recent Messaging Changes', s.recentChanges, (item) => {
    return `${item.companyName}: ${item.summary} [${item.changeType}; ${item.confidence}]`;
  });

  listSection('Observations By Import Intent', s.observationsByIntent, (item) => {
    return `${item.importIntent} — emails ${item.emailCount}, observations ${item.observationCount}, companies ${item.companyCount}`;
  });

  if (briefing.caveats && briefing.caveats.length) {
    lines.push('', 'Caveats:');
    for (const caveat of briefing.caveats) {
      lines.push(`- ${caveat}`);
    }
  }

  return lines.join('\n');
}

module.exports = {
  DEFAULT_DAYS,
  DEFAULT_LIMIT,
  THEME_FIELDS,
  CHANGE_FIELDS,
  clampDays,
  clampLimit,
  formatBriefingReport,
  getCompanyCadence,
  getCorpusSummary,
  getMarketIntelligenceBriefing,
  getMessagingThemes,
  getObservationsByIntent,
  getRecentMessagingChanges,
  getTopCtas,
  getTopOffers,
  resolveWindow,
};
