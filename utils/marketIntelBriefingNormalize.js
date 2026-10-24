'use strict';

/**
 * SPEC-071 briefing-only CTA / theme normalization.
 * Never mutates stored market_observations — synthesis layer only.
 */

const IMAGE_EXT_RE = /\.(?:png|jpe?g|gif|webp|svg|ico|bmp|tiff?)(?:[?#].*)?$/i;
const TRACKING_HOST_RE =
  /\b(?:email\.|click\.|trk\.|track\.|links?\.|open\.|pixel\.|t\.|go\.|mail\.|em\.|e\.)/i;
const TRACKING_PATH_RE =
  /(?:\/(?:trk|track|click|open|pixel|beacon|wf\/click|ls\/click)|[?&](?:utm_|mc_|mkt_|sfmc_|elq|trk))/i;
const UNSUBSCRIBE_RE =
  /unsub(?:scribe)?|manage[-_\s]?preferences?|email[-_\s]?preferences?|opt[-_\s]?out|preference[-_\s]?center|list-unsubscribe/i;
const PRIVACY_RE = /privacy(?:[-_\s]?policy)?|terms(?:[-_\s]?of[-_\s]?service)?|cookie(?:[-_\s]?policy)?|legal|gdpr|ccpa/i;
const FOOTER_NAV_RE =
  /(?:^|\/)(?:about(?:-us)?|contact(?:-us)?|help|support|careers?|jobs|blog|news|press|media|faq|sitemap|login|signin|sign-in|account|profile|settings)(?:\/|$|\?)/i;
const SOCIAL_HOST_RE =
  /(?:^|\.)(?:facebook|fb|instagram|instagr\.am|twitter|x|t\.co|linkedin|lnkd\.in|youtube|youtu\.be|tiktok|pinterest|threads\.net|whatsapp|telegram|snapchat|reddit|discord)\./i;
const SOCIAL_PATH_RE =
  /(?:facebook\.com|instagram\.com|twitter\.com|x\.com|linkedin\.com|lnkd\.in|youtube\.com|youtu\.be|tiktok\.com|pinterest\.com|threads\.net)/i;

const PATH_LABEL_MAP = [
  { re: /(?:^|\/)(?:sign[-_]?up|signup|register|registration)(?:\/|$|\?)/i, label: 'sign up' },
  { re: /(?:^|\/)(?:get[-_]?quote|request[-_]?quote|quote)(?:\/|$|\?)/i, label: 'get quote' },
  { re: /(?:^|\/)(?:book[-_]?demo|request[-_]?demo|schedule[-_]?demo|demo)(?:\/|$|\?)/i, label: 'book demo' },
  { re: /(?:^|\/)(?:free[-_]?trial|start[-_]?trial|trial)(?:\/|$|\?)/i, label: 'start free trial' },
  { re: /(?:^|\/)(?:get[-_]?started|getstarted)(?:\/|$|\?)/i, label: 'get started' },
  { re: /(?:^|\/)(?:contact[-_]?sales|talk[-_]?to[-_]?sales|speak[-_]?to[-_]?sales)(?:\/|$|\?)/i, label: 'talk to sales' },
  { re: /(?:^|\/)(?:pricing|plans?)(?:\/|$|\?)/i, label: 'see pricing' },
  { re: /(?:^|\/)(?:claim|redeem)(?:[-_]?offer)?(?:\/|$|\?)/i, label: 'claim offer' },
  { re: /(?:^|\/)(?:subscribe|join)(?:\/|$|\?)/i, label: 'subscribe' },
  { re: /(?:^|\/)(?:learn[-_]?more|learnmore)(?:\/|$|\?)/i, label: 'learn more' },
  { re: /(?:^|\/)(?:shop(?:[-_]?now)?|buy(?:[-_]?now)?)(?:\/|$|\?)/i, label: 'shop now' },
  { re: /(?:^|\/)(?:book|schedule)(?:[-_]?(?:a[-_]?)?(?:call|meeting))?((?:\/|$|\?))/i, label: 'book a call' },
];

const STRUCTURED_THEME_FIELDS = Object.freeze([
  'positioning',
  'urgency',
  'social_proof',
  'guarantee',
]);

const HEADLINE_THEME_FIELDS = Object.freeze(['headline']);

function looksLikeUrl(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  if (/^https?:\/\//i.test(s)) return true;
  if (/^\/\//.test(s)) return true;
  if (/^www\./i.test(s)) return true;
  if (/^[a-z0-9.-]+\.[a-z]{2,}(?:\/|\?|$)/i.test(s) && /\//.test(s)) return true;
  return false;
}

function safeParseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    if (/^https?:\/\//i.test(raw) || /^\/\//.test(raw)) {
      return new URL(raw.startsWith('//') ? `https:${raw}` : raw);
    }
    if (/^www\./i.test(raw) || /^[a-z0-9.-]+\.[a-z]{2,}\//i.test(raw)) {
      return new URL(`https://${raw}`);
    }
    if (raw.startsWith('/')) {
      return new URL(raw, 'https://example.invalid');
    }
  } catch {
    return null;
  }
  return null;
}

function pickAnchorText(valueJson = {}, evidenceQuote = '') {
  const json = valueJson && typeof valueJson === 'object' ? valueJson : {};
  const candidates = [
    json.anchorText,
    json.buttonText,
    json.linkText,
    json.label,
    json.text,
    json.ctaText,
  ];
  for (const c of candidates) {
    const s = String(c || '').replace(/\s+/g, ' ').trim();
    if (!s) continue;
    if (looksLikeUrl(s)) continue;
    if (s.length < 2 || s.length > 80) continue;
    return s.toLowerCase();
  }

  const quote = String(evidenceQuote || '').replace(/\s+/g, ' ').trim();
  if (quote && !looksLikeUrl(quote) && quote.length >= 2 && quote.length <= 80) {
    // Prefer short phrase-like quotes, not long body snippets.
    if (!/[.!?].+\s/.test(quote) && quote.split(' ').length <= 8) {
      return quote.toLowerCase();
    }
  }
  return null;
}

function classifyExcludedUrl(value, parsed) {
  const raw = String(value || '').trim();
  const href = parsed ? `${parsed.hostname}${parsed.pathname}${parsed.search}` : raw;
  const host = parsed ? parsed.hostname.toLowerCase() : '';
  const path = parsed ? `${parsed.pathname || ''}${parsed.search || ''}` : raw;

  if (IMAGE_EXT_RE.test(raw) || IMAGE_EXT_RE.test(path)) {
    return 'image_url';
  }
  if (
    /(?:pixel|beacon|1x1|tracking[-_]?pixel)/i.test(raw) ||
    TRACKING_PATH_RE.test(href) ||
    (TRACKING_HOST_RE.test(host) && /(?:open|click|track|pixel)/i.test(path))
  ) {
    return 'tracking_pixel_or_click';
  }
  if (UNSUBSCRIBE_RE.test(href) || UNSUBSCRIBE_RE.test(raw)) {
    return 'unsubscribe_or_preferences';
  }
  if (PRIVACY_RE.test(path) || PRIVACY_RE.test(raw)) {
    return 'privacy_or_legal';
  }
  if (SOCIAL_HOST_RE.test(host) || SOCIAL_PATH_RE.test(raw)) {
    return 'social_profile_link';
  }
  if (FOOTER_NAV_RE.test(path) || FOOTER_NAV_RE.test(raw)) {
    return 'footer_or_navigation';
  }
  return null;
}

function labelFromPath(pathname = '') {
  const path = String(pathname || '');
  for (const entry of PATH_LABEL_MAP) {
    if (entry.re.test(path)) return entry.label;
  }
  return null;
}

/**
 * Normalize a CTA observation value for briefing synthesis.
 * Raw observation rows are never modified.
 *
 * @returns {{
 *   included: boolean,
 *   label: string|null,
 *   ctaQuality: 'high'|'medium'|'low'|'excluded',
 *   excludedReason: string|null,
 *   source: 'anchor_text'|'path_label'|'phrase'|'url'|'empty',
 *   original: string
 * }}
 */
function normalizeBriefingCta(value, options = {}) {
  const original = String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
  if (!original || original.toLowerCase() === 'none') {
    return {
      included: false,
      label: null,
      ctaQuality: 'excluded',
      excludedReason: 'empty',
      source: 'empty',
      original,
    };
  }

  const valueJson = options.valueJson || options.value_json || {};
  const evidenceQuote = options.evidenceQuote || options.evidence_quote || '';
  const anchorText = pickAnchorText(valueJson, evidenceQuote);
  const parsed = looksLikeUrl(original) ? safeParseUrl(original) : null;

  if (parsed || looksLikeUrl(original)) {
    const excludedReason = classifyExcludedUrl(original, parsed);
    if (excludedReason) {
      // Anchor text can still rescue a social/footer URL only when it is a real CTA phrase.
      if (anchorText && labelFromPath(`/${anchorText.replace(/\s+/g, '-')}`)) {
        return {
          included: true,
          label: anchorText,
          ctaQuality: 'medium',
          excludedReason: null,
          source: 'anchor_text',
          original,
        };
      }
      if (anchorText && /(?:demo|trial|quote|sign\s*up|get\s*started|shop|subscribe|learn\s*more|book|claim)/i.test(anchorText)) {
        return {
          included: true,
          label: anchorText,
          ctaQuality: 'medium',
          excludedReason: null,
          source: 'anchor_text',
          original,
        };
      }
      return {
        included: false,
        label: null,
        ctaQuality: 'excluded',
        excludedReason,
        source: 'url',
        original,
      };
    }

    if (anchorText) {
      return {
        included: true,
        label: anchorText,
        ctaQuality: 'high',
        excludedReason: null,
        source: 'anchor_text',
        original,
      };
    }

    const pathLabel = parsed ? labelFromPath(parsed.pathname) : labelFromPath(original);
    if (pathLabel) {
      return {
        included: true,
        label: pathLabel,
        ctaQuality: 'medium',
        excludedReason: null,
        source: 'path_label',
        original,
      };
    }

    // Generic http(s) CTA URL without a useful path label — keep out of briefing tops.
    return {
      included: false,
      label: null,
      ctaQuality: 'excluded',
      excludedReason: 'uninformative_url',
      source: 'url',
      original,
    };
  }

  // Non-URL phrase CTAs.
  return {
    included: true,
    label: original.toLowerCase(),
    ctaQuality: 'high',
    excludedReason: null,
    source: 'phrase',
    original,
  };
}

function isExcludedBriefingCta(value, options = {}) {
  return !normalizeBriefingCta(value, options).included;
}

/**
 * Aggregate CTA observation rows into briefing top-CTA items.
 * Rows may include value_json / evidence_quote for anchor-text preference.
 */
function aggregateNormalizedCtas(rows, { limit = 10 } = {}) {
  const groups = new Map();
  const excluded = [];

  for (const row of rows || []) {
    const raw = row.value_text != null ? row.value_text : row.label != null ? row.label : row.cta;
    const normalized = normalizeBriefingCta(raw, {
      valueJson: row.value_json || row.valueJson,
      evidenceQuote: row.evidence_quote || row.evidenceQuote,
    });

    if (!normalized.included) {
      excluded.push({
        original: normalized.original,
        excludedReason: normalized.excludedReason,
        observationId: row.id || row.observation_id || null,
      });
      continue;
    }

    const key = normalized.label;
    if (!groups.has(key)) {
      groups.set(key, {
        cta: key,
        count: 0,
        companies: new Set(),
        latestObservedAt: null,
        exampleObservationIds: [],
        exampleEmailIds: [],
        ctaQuality: normalized.ctaQuality,
        source: normalized.source,
      });
    }
    const g = groups.get(key);
    g.count += Number(row.count || 1);
    if (normalized.ctaQuality === 'high') g.ctaQuality = 'high';
    for (const company of row.companies || []) {
      if (company) g.companies.add(String(company));
    }
    if (row.company_name) g.companies.add(String(row.company_name));
    if (row.companyName) g.companies.add(String(row.companyName));

    const observedAt = row.latest_observed_at || row.latestObservedAt || row.received_at || row.receivedAt;
    if (observedAt) {
      const iso = observedAt instanceof Date ? observedAt.toISOString() : String(observedAt);
      if (!g.latestObservedAt || iso > g.latestObservedAt) g.latestObservedAt = iso;
    }

    const obsId = row.id || row.observation_id || null;
    const emailId = row.email_id || row.emailId || null;
    if (obsId && g.exampleObservationIds.length < 5) {
      const s = String(obsId);
      if (!g.exampleObservationIds.includes(s)) g.exampleObservationIds.push(s);
    }
    if (Array.isArray(row.exampleObservationIds)) {
      for (const id of row.exampleObservationIds) {
        if (g.exampleObservationIds.length >= 5) break;
        const s = String(id);
        if (!g.exampleObservationIds.includes(s)) g.exampleObservationIds.push(s);
      }
    }
    if (emailId && g.exampleEmailIds.length < 5) {
      const s = String(emailId);
      if (!g.exampleEmailIds.includes(s)) g.exampleEmailIds.push(s);
    }
    if (Array.isArray(row.exampleEmailIds)) {
      for (const id of row.exampleEmailIds) {
        if (g.exampleEmailIds.length >= 5) break;
        const s = String(id);
        if (!g.exampleEmailIds.includes(s)) g.exampleEmailIds.push(s);
      }
    }
  }

  const items = [...groups.values()]
    .map((g) => ({
      cta: g.cta,
      count: g.count,
      companies: [...g.companies].filter(Boolean).sort(),
      latestObservedAt: g.latestObservedAt,
      exampleObservationIds: g.exampleObservationIds,
      ctaQuality: g.ctaQuality,
    }))
    .sort((a, b) => b.count - a.count || a.cta.localeCompare(b.cta))
    .slice(0, Math.max(1, Number(limit) || 10));

  return { items, excluded };
}

/**
 * Sanitize a timeline touch CTA for recent-change heuristics.
 * Excluded CTAs become null so they cannot drive change detection.
 */
function sanitizeTimelineCta(ctaValue, options = {}) {
  const normalized = normalizeBriefingCta(ctaValue, options);
  if (!normalized.included) {
    return {
      cta: null,
      ctaQuality: 'excluded',
      excludedReason: normalized.excludedReason,
      original: normalized.original,
    };
  }
  return {
    cta: normalized.label,
    ctaQuality: normalized.ctaQuality,
    excludedReason: null,
    original: normalized.original,
  };
}

function sanitizeTimelineForBriefing(timeline) {
  return (timeline || []).map((touch) => {
    const sanitized = sanitizeTimelineCta(touch.cta, {
      valueJson: touch.ctaValueJson || touch.valueJson,
      evidenceQuote: touch.ctaEvidenceQuote || touch.evidenceQuote,
    });
    return {
      ...touch,
      cta: sanitized.cta,
      _ctaMeta: sanitized,
    };
  });
}

module.exports = {
  HEADLINE_THEME_FIELDS,
  STRUCTURED_THEME_FIELDS,
  aggregateNormalizedCtas,
  classifyExcludedUrl,
  isExcludedBriefingCta,
  labelFromPath,
  looksLikeUrl,
  normalizeBriefingCta,
  pickAnchorText,
  sanitizeTimelineCta,
  sanitizeTimelineForBriefing,
};
