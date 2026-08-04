'use strict';

/**
 * SPEC-065 — Market Intelligence query layer.
 * Timelines, descriptive diffs, company profiles, cross-market patterns.
 * Observational only — no scoring or recommendations.
 */

const defaultPool = require('../db');

function groupObservations(rows) {
  const byCategory = {};
  for (const row of rows) {
    const category = row.category;
    if (!byCategory[category]) byCategory[category] = [];
    byCategory[category].push({
      id: row.id,
      field: row.field,
      valueText: row.value_text,
      valueJson: row.value_json || {},
      evidenceQuote: row.evidence_quote,
      evidencePath: row.evidence_path,
      extractor: row.extractor,
      extractedAt: row.extracted_at,
      emailId: row.email_id,
    });
  }
  return byCategory;
}

function observationValue(observationsByCategory, field) {
  for (const list of Object.values(observationsByCategory || {})) {
    const hit = list.find((o) => o.field === field);
    if (hit) return hit.valueText;
  }
  return null;
}

/**
 * Chronological campaign timeline with structured observations per touch.
 */
async function getCompanyCampaignTimeline(companyId, { pool = defaultPool, limit = 500 } = {}) {
  const emails = await pool.query(
    `SELECT
       e.id,
       e.gmail_id,
       e.thread_id,
       e.subject,
       e.from_email,
       e.from_name,
       e.received_at,
       e.sent_at,
       e.links,
       c.name AS company_name,
       c.domain AS company_domain
     FROM market_emails e
     JOIN market_companies c ON c.id = e.company_id
     WHERE e.company_id = $1
     ORDER BY e.received_at ASC, e.imported_at ASC
     LIMIT $2`,
    [companyId, limit]
  );

  if (!emails.rows.length) return [];

  const emailIds = emails.rows.map((r) => r.id);
  const obs = await pool.query(
    `SELECT *
       FROM market_observations
      WHERE email_id = ANY($1::uuid[])
      ORDER BY extracted_at ASC`,
    [emailIds]
  );

  const byEmail = new Map();
  for (const row of obs.rows) {
    if (!byEmail.has(row.email_id)) byEmail.set(row.email_id, []);
    byEmail.get(row.email_id).push(row);
  }

  return emails.rows.map((row, index) => {
    const observations = groupObservations(byEmail.get(row.id) || []);
    const prev = index > 0 ? emails.rows[index - 1] : null;
    let cadenceDays = null;
    if (prev) {
      const ms = new Date(row.received_at) - new Date(prev.received_at);
      cadenceDays = Math.round((ms / 86400000) * 10) / 10;
    }
    return {
      touch: index + 1,
      id: row.id,
      gmailId: row.gmail_id,
      threadId: row.thread_id,
      subject: row.subject,
      subjectLength: String(row.subject || '').length,
      fromEmail: row.from_email,
      fromName: row.from_name,
      receivedAt: row.received_at,
      sentAt: row.sent_at,
      links: row.links,
      companyName: row.company_name,
      companyDomain: row.company_domain,
      cadenceDaysFromPrevious: cadenceDays,
      observations,
      cta: observationValue(observations, 'cta'),
      pricingMention: observationValue(observations, 'pricing_mention'),
      positioning: observationValue(observations, 'positioning'),
      guarantee: observationValue(observations, 'guarantee'),
      offer: observationValue(observations, 'offer'),
    };
  });
}

/**
 * Descriptive diffs only — no improvement / success language.
 */
function diffTimelineField(timeline, field) {
  const keyMap = {
    cta: 'cta',
    pricing_mention: 'pricingMention',
    positioning: 'positioning',
    guarantee: 'guarantee',
    offer: 'offer',
    subject_length: 'subjectLength',
    cadence_days: 'cadenceDaysFromPrevious',
  };
  const prop = keyMap[field] || field;
  const changes = [];

  for (let i = 1; i < (timeline || []).length; i += 1) {
    const prev = timeline[i - 1];
    const curr = timeline[i];
    let before;
    let after;
    if (field === 'guarantee_presence') {
      before = Boolean(prev.guarantee);
      after = Boolean(curr.guarantee);
    } else {
      before = prev[prop];
      after = curr[prop];
    }
    if (String(before ?? '') === String(after ?? '')) continue;
    changes.push({
      field,
      fromTouch: prev.touch,
      toTouch: curr.touch,
      before: before ?? null,
      after: after ?? null,
      fromEmailId: prev.id,
      toEmailId: curr.id,
      at: curr.receivedAt,
    });
  }
  return changes;
}

function modeValue(values) {
  const counts = new Map();
  for (const v of values) {
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

function median(nums) {
  if (!nums.length) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return Math.round(((sorted[mid - 1] + sorted[mid]) / 2) * 100) / 100;
  }
  return sorted[mid];
}

/**
 * Rebuild observational company profile from emails + observations.
 */
async function rebuildCompanyProfile(companyId, { pool = defaultPool } = {}) {
  const company = await pool.query(
    `SELECT id, name, domain, is_unknown FROM market_companies WHERE id = $1 LIMIT 1`,
    [companyId]
  );
  if (!company.rows[0]) {
    return null;
  }

  const timeline = await getCompanyCampaignTimeline(companyId, { pool });
  const offers = timeline.map((t) => t.offer).filter(Boolean);
  const distinctOffers = new Set(offers.map((o) => o.toLowerCase()));
  const ctas = timeline.map((t) => t.cta).filter(Boolean);
  const positionings = timeline.map((t) => t.positioning).filter(Boolean);
  const cadences = timeline
    .map((t) => t.cadenceDaysFromPrevious)
    .filter((n) => n != null && Number.isFinite(n));

  const first = timeline[0] || null;
  const last = timeline[timeline.length - 1] || null;

  const evidenceRefs = timeline.map((t) => t.id);
  const lastHeadline = last
    ? observationValue(last.observations, 'headline') || last.subject
    : null;

  const profile = {
    companyId,
    companyName: company.rows[0].name,
    domain: company.rows[0].domain,
    campaignsObserved: timeline.length,
    emailsObserved: timeline.length,
    firstSeen: first?.receivedAt || null,
    lastSeen: last?.receivedAt || null,
    distinctOffers: distinctOffers.size,
    primaryPositioning: modeValue(positionings),
    currentCta: last?.cta || modeValue(ctas),
    averageSequence: timeline.length,
    averageCadenceDays: cadences.length
      ? Math.round((cadences.reduce((a, b) => a + b, 0) / cadences.length) * 100) / 100
      : null,
    latestDirection: last?.positioning || lastHeadline,
    evidenceRefs,
    fieldEvidence: {
      primaryPositioning: timeline.filter((t) => t.positioning).map((t) => t.id),
      currentCta: last?.cta ? [last.id] : [],
      distinctOffers: timeline.filter((t) => t.offer).map((t) => t.id),
      latestDirection: last ? [last.id] : [],
    },
  };

  await pool.query(
    `INSERT INTO market_company_profiles (
       company_id, first_seen_at, last_seen_at, emails_observed, distinct_offers,
       avg_sequence_length, primary_positioning, current_cta, latest_direction,
       profile_json, rebuilt_at
     ) VALUES (
       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, NOW()
     )
     ON CONFLICT (company_id) DO UPDATE SET
       first_seen_at = EXCLUDED.first_seen_at,
       last_seen_at = EXCLUDED.last_seen_at,
       emails_observed = EXCLUDED.emails_observed,
       distinct_offers = EXCLUDED.distinct_offers,
       avg_sequence_length = EXCLUDED.avg_sequence_length,
       primary_positioning = EXCLUDED.primary_positioning,
       current_cta = EXCLUDED.current_cta,
       latest_direction = EXCLUDED.latest_direction,
       profile_json = EXCLUDED.profile_json,
       rebuilt_at = NOW()`,
    [
      companyId,
      profile.firstSeen,
      profile.lastSeen,
      profile.emailsObserved,
      profile.distinctOffers,
      profile.averageSequence,
      profile.primaryPositioning,
      profile.currentCta,
      profile.latestDirection,
      JSON.stringify(profile),
    ]
  );

  return profile;
}

async function getCompanyProfile(companyId, { pool = defaultPool, rebuildIfMissing = true } = {}) {
  const existing = await pool.query(
    `SELECT p.*, c.name AS company_name, c.domain
       FROM market_company_profiles p
       JOIN market_companies c ON c.id = p.company_id
      WHERE p.company_id = $1
      LIMIT 1`,
    [companyId]
  );
  if (existing.rows[0]) {
    const row = existing.rows[0];
    return {
      ...(row.profile_json || {}),
      companyId: row.company_id,
      companyName: row.company_name,
      domain: row.domain,
      emailsObserved: row.emails_observed,
      distinctOffers: row.distinct_offers,
      primaryPositioning: row.primary_positioning,
      currentCta: row.current_cta,
      latestDirection: row.latest_direction,
      firstSeen: row.first_seen_at,
      lastSeen: row.last_seen_at,
      avgSequenceLength: row.avg_sequence_length != null
        ? Number(row.avg_sequence_length)
        : null,
      rebuiltAt: row.rebuilt_at,
      evidenceRefs: (row.profile_json && row.profile_json.evidenceRefs) || [],
    };
  }
  if (!rebuildIfMissing) return null;
  return rebuildCompanyProfile(companyId, { pool });
}

function mapCompanyListRow(row) {
  return {
    id: row.id,
    name: row.name,
    domain: row.domain,
    isUnknown: row.is_unknown,
    emailsObserved: row.emails_observed || 0,
    lastSeen: row.last_seen_at || null,
    currentCta: row.current_cta || null,
    primaryPositioning: row.primary_positioning || null,
  };
}

async function listMarketCompanies({ pool = defaultPool, q = '', limit = 50 } = {}) {
  const capped = Math.min(200, Math.max(1, Number(limit) || 50));
  const query = String(q || '').trim();
  if (query) {
    const result = await pool.query(
      `SELECT
         c.id,
         c.name,
         c.domain,
         c.is_unknown,
         p.emails_observed,
         p.last_seen_at,
         p.current_cta,
         p.primary_positioning
       FROM market_companies c
       LEFT JOIN market_company_profiles p ON p.company_id = c.id
       WHERE c.name ILIKE $1 OR c.domain ILIKE $1
       ORDER BY p.last_seen_at DESC NULLS LAST, c.name ASC
       LIMIT $2`,
      [`%${query}%`, capped]
    );
    return result.rows.map(mapCompanyListRow);
  }

  const result = await pool.query(
    `SELECT
       c.id,
       c.name,
       c.domain,
       c.is_unknown,
       p.emails_observed,
       p.last_seen_at,
       p.current_cta,
       p.primary_positioning
     FROM market_companies c
     LEFT JOIN market_company_profiles p ON p.company_id = c.id
     ORDER BY p.last_seen_at DESC NULLS LAST, c.name ASC
     LIMIT $1`,
    [capped]
  );
  return result.rows.map(mapCompanyListRow);
}

const PATTERN_FIELDS = new Set([
  'cta',
  'offer',
  'guarantee',
  'pricing_mention',
  'positioning',
  'format_style',
  'voice_style',
  'signal',
  'urgency',
  'social_proof',
]);

async function crossMarketPatterns({ pool = defaultPool, field = 'cta', limit = 25 } = {}) {
  const safeField = PATTERN_FIELDS.has(field) ? field : 'cta';
  const capped = Math.min(100, Math.max(1, Number(limit) || 25));
  const result = await pool.query(
    `SELECT
       value_text,
       COUNT(*)::int AS count,
       ARRAY_AGG(email_id::text ORDER BY extracted_at DESC) AS evidence_refs
     FROM market_observations
     WHERE field = $1
       AND value_text <> ''
       AND value_text <> 'none'
     GROUP BY value_text
     ORDER BY COUNT(*) DESC, value_text ASC
     LIMIT $2`,
    [safeField, capped]
  );

  return {
    field: safeField,
    patterns: result.rows.map((row) => ({
      value: row.value_text,
      count: row.count,
      evidenceRefs: (row.evidence_refs || []).slice(0, 5),
    })),
  };
}

async function crossMarketSequenceStats({ pool = defaultPool } = {}) {
  const lengths = await pool.query(
    `SELECT company_id, COUNT(*)::int AS seq_len
       FROM market_emails
      GROUP BY company_id`
  );
  const seqLens = lengths.rows.map((r) => r.seq_len);

  const gaps = await pool.query(
    `WITH ordered AS (
       SELECT
         company_id,
         received_at,
         LAG(received_at) OVER (
           PARTITION BY company_id
           ORDER BY received_at ASC, imported_at ASC
         ) AS prev_at
       FROM market_emails
     )
     SELECT EXTRACT(EPOCH FROM (received_at - prev_at)) / 86400.0 AS cadence_days
       FROM ordered
      WHERE prev_at IS NOT NULL`
  );

  const cadenceDays = gaps.rows
    .map((r) => Number(r.cadence_days))
    .filter((n) => Number.isFinite(n));

  const sum = seqLens.reduce((a, b) => a + b, 0);
  const cadenceSum = cadenceDays.reduce((a, b) => a + b, 0);

  return {
    companies: seqLens.length,
    averageSequenceLength: seqLens.length
      ? Math.round((sum / seqLens.length) * 100) / 100
      : null,
    medianSequenceLength: median(seqLens),
    averageFollowUpSpacingDays: cadenceDays.length
      ? Math.round((cadenceSum / cadenceDays.length) * 100) / 100
      : null,
    medianFollowUpSpacingDays: median(
      cadenceDays.map((n) => Math.round(n * 100) / 100)
    ),
    sampleSizes: {
      sequences: seqLens.length,
      followUpGaps: cadenceDays.length,
    },
  };
}

async function getEmailEvidence(emailId, { pool = defaultPool } = {}) {
  const email = await pool.query(
    `SELECT
       e.id,
       e.company_id,
       e.gmail_id,
       e.thread_id,
       e.subject,
       e.from_email,
       e.from_name,
       e.received_at,
       e.links,
       c.name AS company_name,
       c.domain AS company_domain
     FROM market_emails e
     JOIN market_companies c ON c.id = e.company_id
     WHERE e.id = $1
     LIMIT 1`,
    [emailId]
  );
  if (!email.rows[0]) return null;

  const obs = await pool.query(
    `SELECT * FROM market_observations WHERE email_id = $1 ORDER BY category, field`,
    [emailId]
  );

  const row = email.rows[0];
  return {
    id: row.id,
    companyId: row.company_id,
    companyName: row.company_name,
    companyDomain: row.company_domain,
    gmailId: row.gmail_id,
    threadId: row.thread_id,
    subject: row.subject,
    fromEmail: row.from_email,
    fromName: row.from_name,
    receivedAt: row.received_at,
    links: row.links,
    observations: groupObservations(obs.rows),
    evidenceRefs: [row.id],
  };
}

module.exports = {
  PATTERN_FIELDS,
  crossMarketPatterns,
  crossMarketSequenceStats,
  diffTimelineField,
  getCompanyCampaignTimeline,
  getCompanyProfile,
  getEmailEvidence,
  listMarketCompanies,
  median,
  modeValue,
  rebuildCompanyProfile,
};
