'use strict';

/**
 * SPEC-066 thin slice — Max reads Market Intelligence as evidence.
 * SPEC-071 — Max may also inspect a read-only briefing synthesis.
 * Read-only adapter. No scoring, recommendations, or MI writes.
 */

const DEFAULT_FIELDS = ['cta', 'offer', 'positioning'];

function defaultService() {
  return require('../../../services/marketIntelligenceQuery');
}

function defaultBriefingService() {
  return require('../../../services/marketIntelligenceBriefing');
}

function extractMarketSearchTerm(question) {
  const q = String(question || '').trim();
  if (!q) return '';

  const after =
    q.match(/\b(?:monitor|watch|research|inspect|show|about|for)\s+([A-Z][\w&.\- ]{1,80})/i) ||
    q.match(/\b(?:competitor|vendor|market)\s+(?:called|named)\s+([A-Z][\w&.\- ]{1,80})/i);
  if (after && after[1]) {
    return after[1]
      .replace(/[?.!,;:]+$/g, '')
      .replace(/\b(?:campaigns?|emails?|outreach|market|intelligence|positioning|messaging|ctas?|calls?[- ]?to[- ]?action|offers?|pricing|patterns?)\b.*$/i, '')
      .trim();
  }

  return '';
}

function requestedPatternFields(question) {
  const q = String(question || '').toLowerCase();
  const fields = [];
  if (/\bcta|call[- ]?to[- ]?action|demo|trial\b/.test(q)) fields.push('cta');
  if (/\boffer|discount|free trial|promotion\b/.test(q)) fields.push('offer');
  if (/\bposition|positioning|message|messaging\b/.test(q)) fields.push('positioning');
  if (/\bprice|pricing|\$|cost\b/.test(q)) fields.push('pricing_mention');
  if (/\bguarantee\b/.test(q)) fields.push('guarantee');
  if (/\burgency|urgent|limited|expires\b/.test(q)) fields.push('urgency');
  return fields.length ? [...new Set(fields)] : DEFAULT_FIELDS;
}

async function safeCall(fn, fallback) {
  try {
    return await fn();
  } catch (err) {
    return {
      ...fallback,
      error: err && err.message ? String(err.message) : 'market_intel_unavailable',
    };
  }
}

/**
 * Read-only briefing inspection for Max / future apps.
 * Synthesis only — never treated as raw evidence or an action instruction.
 */
async function getMarketIntelligenceBriefing(context = {}) {
  const briefingService = context.briefingService || defaultBriefingService();
  const options = {
    days: context.days,
    limit: context.limit,
    importIntent: context.importIntent || context.intent,
    companyId: context.companyId,
    category: context.category,
    since: context.since,
    until: context.until,
    pool: context.pool,
  };

  const briefing = await briefingService.getMarketIntelligenceBriefing(options);
  return {
    ...briefing,
    ok: briefing && briefing.ok !== false,
    kind: 'market_intelligence_briefing',
    isEvidence: false,
    source: 'SPEC-071',
    inspectionOnly: true,
  };
}

async function buildMarketIntelligenceContext(question, options = {}) {
  const service = options.service || defaultService();
  const term = options.searchTerm != null
    ? String(options.searchTerm || '').trim()
    : extractMarketSearchTerm(question);
  const fields = requestedPatternFields(question);

  const companiesResult = await safeCall(
    () => service.listMarketCompanies({ q: term, limit: term ? 5 : 10 }),
    { companies: [], error: null }
  );
  const companies = Array.isArray(companiesResult)
    ? companiesResult
    : companiesResult.companies || [];
  const unavailable = [];
  if (companiesResult.error) unavailable.push('market_companies');

  const selectedCompany = companies[0] || null;
  let profile = null;
  let timeline = [];
  if (selectedCompany && selectedCompany.id) {
    profile = await safeCall(
      () => service.getCompanyProfile(selectedCompany.id, { rebuildIfMissing: false }),
      { error: 'market_company_profile_unavailable' }
    );
    if (profile && profile.error) {
      unavailable.push('market_company_profile');
      profile = null;
    }

    timeline = await safeCall(
      () => service.getCompanyCampaignTimeline(selectedCompany.id, { limit: 12 }),
      { timeline: [], error: 'market_company_timeline_unavailable' }
    );
    if (timeline && timeline.error) {
      unavailable.push('market_company_timeline');
      timeline = [];
    }
  }

  const patterns = [];
  for (const field of fields) {
    const result = await safeCall(
      () => service.crossMarketPatterns({ field, limit: 5 }),
      { field, patterns: [], error: 'market_patterns_unavailable' }
    );
    if (result.error) unavailable.push(`market_patterns:${field}`);
    patterns.push({
      field: result.field || field,
      patterns: Array.isArray(result.patterns) ? result.patterns : [],
    });
  }

  const sequenceStats = await safeCall(
    () => service.crossMarketSequenceStats(),
    { error: 'market_sequence_stats_unavailable' }
  );
  if (sequenceStats.error) unavailable.push('market_sequence_stats');

  let briefing = null;
  if (options.includeBriefing) {
    const briefingResult = await safeCall(
      () =>
        getMarketIntelligenceBriefing({
          briefingService: options.briefingService,
          days: options.days,
          limit: options.limit || 5,
          importIntent: options.importIntent || options.intent,
          companyId: options.companyId || selectedCompany?.id,
          pool: options.pool,
        }),
      { error: 'market_briefing_unavailable' }
    );
    if (briefingResult.error) {
      unavailable.push('market_briefing');
    } else {
      briefing = briefingResult;
    }
  }

  return {
    status: unavailable.length ? 'partial' : 'available',
    searchTerm: term,
    companies,
    selectedCompany,
    profile,
    timeline: Array.isArray(timeline) ? timeline : [],
    patterns,
    sequenceStats: sequenceStats.error ? null : sequenceStats,
    briefing,
    unavailable,
    source: 'SPEC-065',
  };
}

module.exports = {
  buildMarketIntelligenceContext,
  extractMarketSearchTerm,
  getMarketIntelligenceBriefing,
  requestedPatternFields,
};
