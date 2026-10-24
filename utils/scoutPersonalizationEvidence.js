'use strict';

/**
 * Scout-supported personalization evidence for Anchor lifecycle messaging.
 * Returns structured, source-backed facts — never outreach copy.
 */

const axios = require('axios');

const PERSONALIZATION_STATUS = Object.freeze({
  SUPPORTED: 'SUPPORTED',
  NO_USEFUL_FACT: 'NO_USEFUL_FACT',
  INSUFFICIENT_EVIDENCE: 'INSUFFICIENT_EVIDENCE',
});

const FACT_TYPES = Object.freeze({
  NEW_LOCATION: 'NEW_LOCATION',
  MULTI_LOCATION: 'MULTI_LOCATION',
  PROPERTY_MANAGEMENT: 'PROPERTY_MANAGEMENT',
  FACILITY_EXPANSION: 'FACILITY_EXPANSION',
  VENDOR_PROCESS: 'VENDOR_PROCESS',
  COMMERCIAL_SPACE_SIGNAL: 'COMMERCIAL_SPACE_SIGNAL',
  OTHER: 'OTHER',
});

const RELEVANCE_HYPOTHESES = Object.freeze({
  [FACT_TYPES.NEW_LOCATION]: 'A new or recently opened location may still be arranging commercial cleaning.',
  [FACT_TYPES.MULTI_LOCATION]: 'Multiple locations can mean cleaning is handled centrally or per site.',
  [FACT_TYPES.PROPERTY_MANAGEMENT]: 'Property managers often coordinate cleaning across managed buildings.',
  [FACT_TYPES.FACILITY_EXPANSION]: 'Expanded or renovated space can change cleaning scope or vendor needs.',
  [FACT_TYPES.VENDOR_PROCESS]: 'A documented vendor process may affect how cleaning vendors are selected.',
  [FACT_TYPES.COMMERCIAL_SPACE_SIGNAL]: 'Commercial office space typically needs recurring cleaning support.',
  [FACT_TYPES.OTHER]: 'This observable detail may affect how commercial cleaning is arranged.',
});

const WEBSITE_PATHS = Object.freeze([
  '/',
  '/about',
  '/about-us',
  '/locations',
  '/our-locations',
  '/services',
  '/news',
  '/blog',
]);

function stripHtml(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split(/[/?#\s]/)[0].toLowerCase() || null;
  }
}

function buildUrl(domain, path = '/') {
  const normalized = normalizeDomain(domain);
  if (!normalized) return null;
  return `https://${normalized}${path.startsWith('/') ? path : `/${path}`}`;
}

function excerptAround(text, index, radius = 120) {
  const start = Math.max(0, index - radius);
  const end = Math.min(text.length, index + radius);
  return text.slice(start, end).trim();
}

function cleanObservedFact(text) {
  const fact = String(text || '').replace(/\s+/g, ' ').trim();
  if (!fact) return null;
  if (fact.length > 240) return `${fact.slice(0, 237).trim()}...`;
  return fact;
}

function normalizePersonalizationEvidence(raw = {}, checkedAt = new Date().toISOString()) {
  const status = PERSONALIZATION_STATUS[raw.personalization_status] || PERSONALIZATION_STATUS.NO_USEFUL_FACT;
  const factType = raw.fact_type && FACT_TYPES[raw.fact_type] ? raw.fact_type : null;
  const confidence = Number.isFinite(Number(raw.confidence))
    ? Math.max(0, Math.min(1, Number(raw.confidence)))
    : 0;

  return {
    personalization_status: status,
    observed_fact: status === PERSONALIZATION_STATUS.SUPPORTED ? cleanObservedFact(raw.observed_fact) : null,
    source_url: status === PERSONALIZATION_STATUS.SUPPORTED ? (raw.source_url || null) : null,
    supporting_excerpt: status === PERSONALIZATION_STATUS.SUPPORTED ? (raw.supporting_excerpt || null) : null,
    checked_at: raw.checked_at || checkedAt,
    event_date: raw.event_date || null,
    fact_type: factType,
    relevance_hypothesis: raw.relevance_hypothesis || (factType ? RELEVANCE_HYPOTHESES[factType] : null),
    confidence,
  };
}

function emptyEvidence(checkedAt = new Date().toISOString()) {
  return normalizePersonalizationEvidence({
    personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
    confidence: 0,
    checked_at: checkedAt,
  });
}

function supportedEvidence({
  observedFact,
  sourceUrl,
  supportingExcerpt,
  factType,
  eventDate = null,
  confidence = 0.75,
  checkedAt = new Date().toISOString(),
}) {
  const fact = cleanObservedFact(observedFact);
  if (!fact || !sourceUrl || !supportingExcerpt) {
    return emptyEvidence(checkedAt);
  }
  return normalizePersonalizationEvidence({
    personalization_status: PERSONALIZATION_STATUS.SUPPORTED,
    observed_fact: fact,
    source_url: sourceUrl,
    supporting_excerpt: supportingExcerpt,
    event_date: eventDate,
    fact_type: factType || FACT_TYPES.OTHER,
    relevance_hypothesis: RELEVANCE_HYPOTHESES[factType || FACT_TYPES.OTHER],
    confidence,
    checked_at: checkedAt,
  });
}

function extractPlacesPersonalizationFact() {
  // Places listing metadata alone is too generic for Anchor personalization.
  return null;
}

function extractSnippetPersonalizationFact(lead = {}) {
  const snippet = String(lead.snippet || '').replace(/\s+/g, ' ').trim();
  const url = lead.url ? buildUrl(lead.url, '/') : null;
  if (!snippet || !url || snippet.length < 20) return null;

  const patterns = [
    {
      re: /\b(?:multiple|several|\d+)\s+locations?\b/i,
      factType: FACT_TYPES.MULTI_LOCATION,
    },
    {
      re: /\bproperty management\b/i,
      factType: FACT_TYPES.PROPERTY_MANAGEMENT,
    },
    {
      re: /\b(?:new|opened|opening)\s+(?:office|location|facility)\b/i,
      factType: FACT_TYPES.NEW_LOCATION,
    },
    {
      re: /\b(?:office park|business center|commercial office)\b/i,
      factType: FACT_TYPES.COMMERCIAL_SPACE_SIGNAL,
    },
  ];

  for (const pattern of patterns) {
    const match = snippet.match(pattern.re);
    if (!match) continue;
    return supportedEvidence({
      observedFact: snippet,
      sourceUrl: url,
      supportingExcerpt: snippet,
      factType: pattern.factType,
      confidence: 0.6,
    });
  }
  return null;
}

function detectWebsiteFact(text, url, vertical = '') {
  const lower = text.toLowerCase();

  const multiLocationMatch = text.match(
    /\b(?:our\s+)?(?:\d+|multiple|several)\s+locations?\b[^.]{0,80}/i
  );
  if (multiLocationMatch) {
    return supportedEvidence({
      observedFact: cleanObservedFact(multiLocationMatch[0]),
      sourceUrl: url,
      supportingExcerpt: excerptAround(text, multiLocationMatch.index),
      factType: FACT_TYPES.MULTI_LOCATION,
      confidence: 0.8,
    });
  }

  const locationsPageMatch = text.match(
    /\blocations?\s+in\s+[A-Z][a-z]+(?:,\s+[A-Z]{2})?(?:\s+and\s+[A-Z][a-z]+(?:,\s+[A-Z]{2})?)+/i
  );
  if (locationsPageMatch) {
    return supportedEvidence({
      observedFact: cleanObservedFact(locationsPageMatch[0]),
      sourceUrl: url,
      supportingExcerpt: excerptAround(text, locationsPageMatch.index),
      factType: FACT_TYPES.MULTI_LOCATION,
      confidence: 0.78,
    });
  }

  if (/property_manager|property_management/i.test(vertical)) {
    const pmMatch = text.match(/\b(?:a|our|full[- ]service)?\s*property management (?:company|firm|services)\b[^.]{0,60}/i);
    if (pmMatch) {
      return supportedEvidence({
        observedFact: cleanObservedFact(pmMatch[0]),
        sourceUrl: url,
        supportingExcerpt: excerptAround(text, pmMatch.index),
        factType: FACT_TYPES.PROPERTY_MANAGEMENT,
        confidence: 0.82,
      });
    }
  }

  const vendorMatch = text.match(
    /\b(?:request for proposal|vendor selection|procurement process|approved vendor list)\b[^.]{0,80}/i
  );
  if (vendorMatch) {
    return supportedEvidence({
      observedFact: cleanObservedFact(vendorMatch[0]),
      sourceUrl: url,
      supportingExcerpt: excerptAround(text, vendorMatch.index),
      factType: FACT_TYPES.VENDOR_PROCESS,
      confidence: 0.76,
    });
  }

  const datedExpansionMatch = text.match(
    /\b(?:opened|opening|relocated to|new office in|expanded (?:to|into))\b[^.]{0,100}\b(?:20\d{2}|january|february|march|april|may|june|july|august|september|october|november|december)\b[^.]{0,40}/i
  );
  if (datedExpansionMatch) {
    const dateMatch = datedExpansionMatch[0].match(/\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+20\d{2}\b|\b20\d{2}\b/i);
    return supportedEvidence({
      observedFact: cleanObservedFact(datedExpansionMatch[0]),
      sourceUrl: url,
      supportingExcerpt: excerptAround(text, datedExpansionMatch.index),
      factType: FACT_TYPES.NEW_LOCATION,
      eventDate: dateMatch ? dateMatch[0] : null,
      confidence: 0.74,
    });
  }

  const commercialSpaceMatch = text.match(/\b(?:office park|business center|commercial office space|professional office building)\b[^.]{0,60}/i);
  if (commercialSpaceMatch) {
    return supportedEvidence({
      observedFact: cleanObservedFact(commercialSpaceMatch[0]),
      sourceUrl: url,
      supportingExcerpt: excerptAround(text, commercialSpaceMatch.index),
      factType: FACT_TYPES.COMMERCIAL_SPACE_SIGNAL,
      confidence: 0.7,
    });
  }

  if (lower.includes('property management') && !/property_manager|property_management/i.test(vertical)) {
    return normalizePersonalizationEvidence({
      personalization_status: PERSONALIZATION_STATUS.INSUFFICIENT_EVIDENCE,
      observed_fact: null,
      source_url: url,
      supporting_excerpt: excerptAround(lower, lower.indexOf('property management')),
      fact_type: null,
      relevance_hypothesis: null,
      confidence: 0.35,
    });
  }

  return null;
}

async function fetchWebsitePage(url, options = {}) {
  const http = options.http || axios;
  try {
    const res = await http.get(url, {
      timeout: options.timeoutMs || 5000,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)' },
      validateStatus: () => true,
    });
    if (res.status < 200 || res.status >= 400) return null;
    return {
      url: res.request?.res?.responseUrl || url,
      text: stripHtml(res.data),
    };
  } catch {
    return null;
  }
}

async function extractWebsitePersonalizationFact(lead = {}, options = {}) {
  const domain = normalizeDomain(lead.url || lead.website || lead.domain);
  if (!domain) return null;

  for (const path of WEBSITE_PATHS) {
    const url = buildUrl(domain, path);
    const page = await fetchWebsitePage(url, options);
    if (!page || !page.text || page.text.length < 40) continue;
    const fact = detectWebsiteFact(page.text, page.url, lead.vertical || '');
    if (fact?.personalization_status === PERSONALIZATION_STATUS.SUPPORTED) {
      return fact;
    }
    if (fact?.personalization_status === PERSONALIZATION_STATUS.INSUFFICIENT_EVIDENCE) {
      return fact;
    }
  }
  return null;
}

async function collectScoutPersonalizationEvidence(lead = {}, options = {}) {
  const checkedAt = new Date().toISOString();

  const websiteFact = await extractWebsitePersonalizationFact(lead, options);
  if (websiteFact) {
    return normalizePersonalizationEvidence({ ...websiteFact, checked_at: checkedAt });
  }

  const snippetFact = extractSnippetPersonalizationFact(lead);
  if (snippetFact) {
    return normalizePersonalizationEvidence({ ...snippetFact, checked_at: checkedAt });
  }

  const placesFact = extractPlacesPersonalizationFact(lead);
  if (placesFact) {
    return normalizePersonalizationEvidence({ ...placesFact, checked_at: checkedAt });
  }

  return emptyEvidence(checkedAt);
}

function readScoutPersonalizationFromMetadata(metadata = {}) {
  if (!metadata || typeof metadata !== 'object') return null;
  const raw = metadata.scout_personalization || metadata.scoutPersonalization || null;
  if (!raw || typeof raw !== 'object') return null;
  return normalizePersonalizationEvidence(raw, raw.checked_at || new Date().toISOString());
}

async function ensureScoutPersonalizationSchema(pool) {
  await pool.query(`
    ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
  `);
}

async function persistScoutPersonalizationEvidence(pool, prospectId, evidence, clientId = null) {
  if (!pool || !prospectId || !evidence) return null;
  await ensureScoutPersonalizationSchema(pool);
  const patch = JSON.stringify({ scout_personalization: evidence });
  const params = clientId == null
    ? [prospectId, patch]
    : [prospectId, patch, clientId];
  const sql = clientId == null
    ? `UPDATE prospects
         SET acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $2::jsonb,
             updated_at = NOW()
       WHERE id = $1
       RETURNING acquisition_metadata`
    : `UPDATE prospects
         SET acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $2::jsonb,
             updated_at = NOW()
       WHERE id = $1 AND client_id = $3
       RETURNING acquisition_metadata`;
  const result = await pool.query(sql, params);
  return result.rows[0]?.acquisition_metadata || null;
}

module.exports = {
  PERSONALIZATION_STATUS,
  FACT_TYPES,
  RELEVANCE_HYPOTHESES,
  normalizePersonalizationEvidence,
  collectScoutPersonalizationEvidence,
  extractWebsitePersonalizationFact,
  extractSnippetPersonalizationFact,
  extractPlacesPersonalizationFact,
  readScoutPersonalizationFromMetadata,
  persistScoutPersonalizationEvidence,
  ensureScoutPersonalizationSchema,
};
