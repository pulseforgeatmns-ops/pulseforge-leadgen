'use strict';

const { normalizeDomain } = require('../../packages/capabilities/websiteOpportunityIntelligence');
const { normalizeCompanyIdentity } = require('../../services/clientIntelligenceCampaignPlanning');
const { scoreScoutProspect } = require('../../utils/studioSubstralMaxPrioritization');

const GENERIC_ISSUE_PATTERNS = [
  /homepage trust and conversion path need strengthening/i,
  /trust and conversion path need strengthening/i,
  /digital presentation may undersell operational credibility/i,
];

const NEGATIVE_COMMERCIAL_FINDING_PRIORITY = [
  'dom_no_contact_nav',
  'tech_no_https',
  'tech_http_error',
  'mobile_no_viewport',
  'perf_fetch_time',
  'seo_missing_title',
  'seo_missing_description',
  'a11y_missing_alt',
  'conv_no_obvious_path',
];

const FIRST_WAVE_STRONG_TIER_SIZE = 13;

const FINDING_SPECIFIC_LABEL = Object.freeze({
  conv_no_obvious_path: 'Unclear homepage CTA — no obvious phone, email, form, or contact link on the homepage',
  dom_no_contact_nav: 'Unclear homepage CTA — primary navigation does not include a Contact path',
  tech_no_https: 'Poor first-impression credibility — homepage is not served over HTTPS',
  mobile_no_viewport: 'Mobile friction — missing viewport meta tag for responsive layout',
  seo_missing_title: 'Weak first-impression credibility — missing or empty page title',
  seo_missing_description: 'Weak trust proof — missing meta description for search/social previews',
  a11y_missing_alt: 'Outdated visual hierarchy — multiple homepage images lack descriptive alt text',
});

function parseIntel(row) {
  if (!row) return {};
  if (typeof row.studio_scout_intelligence === 'object' && row.studio_scout_intelligence) {
    return row.studio_scout_intelligence;
  }
  try {
    return JSON.parse(row.studio_scout_intelligence || '{}');
  } catch {
    return {};
  }
}

function prospectWebsiteDomain(row, intel = parseIntel(row)) {
  return normalizeDomain(
    row.website_url
    || intel.website_url
    || row.company_website
    || intel.website
  );
}

function prospectIdentityKey(row, intel = parseIntel(row)) {
  const domain = prospectWebsiteDomain(row, intel);
  if (domain) return `domain:${domain}`;
  const companyKey = normalizeCompanyIdentity(intel.company_name || row.company_name || '');
  if (companyKey) return `company:${companyKey}`;
  return `prospect:${row.id}`;
}

function rowRank(row) {
  const score = Number(row.studio_fit_score || 0);
  const created = row.created_at ? new Date(row.created_at).getTime() : 0;
  return { score, created };
}

/**
 * One row per canonical domain/company — keeps highest studio_fit_score, then newest.
 */
function dedupeProspectRows(rows) {
  const bestByKey = new Map();
  for (const row of rows) {
    const key = prospectIdentityKey(row);
    const existing = bestByKey.get(key);
    if (!existing) {
      bestByKey.set(key, row);
      continue;
    }
    const a = rowRank(row);
    const b = rowRank(existing);
    if (a.score > b.score || (a.score === b.score && a.created > b.created)) {
      bestByKey.set(key, row);
    }
  }
  return [...bestByKey.values()].sort((x, y) => {
    const a = rowRank(x);
    const b = rowRank(y);
    if (b.score !== a.score) return b.score - a.score;
    return b.created - a.created;
  });
}

function isGenericIssueText(text) {
  const t = String(text || '').trim();
  if (!t) return true;
  return GENERIC_ISSUE_PATTERNS.some((re) => re.test(t));
}

function collectFindingsFromAssessment(assessmentPayload = {}) {
  if (!assessmentPayload) return [];
  const payload = assessmentPayload.assessment ? assessmentPayload : { assessment: assessmentPayload };
  const inner = payload.assessment || payload || {};
  return []
    .concat(inner.verified_findings || [])
    .concat(inner.findings || [])
    .concat(payload.evidence_refs || [])
    .concat(inner.evidence_refs || []);
}

function findingToSpecificIssue(finding) {
  if (!finding || !finding.summary) return null;
  const id = String(finding.id || '');
  if (FINDING_SPECIFIC_LABEL[id]) return FINDING_SPECIFIC_LABEL[id];
  if (id === 'perf_fetch_time' || id === 'perf_slow_fetch') {
    const ms = finding.measurement?.fetch_ms;
    if (ms != null && Number(ms) >= 3500) {
      return `Mobile friction — homepage fetch took ${Math.round(Number(ms) / 1000)}s during audit`;
    }
  }
  if (/conv_no_obvious_path|dom_no_contact_nav/i.test(id)) {
    return FINDING_SPECIFIC_LABEL.conv_no_obvious_path;
  }
  const summary = String(finding.summary).trim();
  const lower = summary.toLowerCase();
  if (/no obvious phone|contact link|conversion path/i.test(summary)) {
    return `Unclear homepage CTA — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (/missing viewport|mobile/i.test(lower)) {
    return `Mobile friction — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (/missing meta description|missing or empty document title|https/i.test(lower)) {
    return `Weak trust proof — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (/form element|mailto|booking|schedule/i.test(lower) && /no |not |missing/i.test(lower)) {
    return `Weak lead capture — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (/fetch completed|performance|slow/i.test(lower) && finding.category === 'performance') {
    return `Mobile friction — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (finding.category === 'conversion_structure' || finding.category === 'conversion') {
    return `Confusing service explanation / conversion path — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  if (finding.category === 'design' || /visual|hierarchy|layout/i.test(lower)) {
    return `Outdated visual hierarchy — ${summary.charAt(0).toLowerCase()}${summary.slice(1)}`;
  }
  return null;
}

function listCandidateWebsiteIssues({ intel, assessmentPayload, websitePainSummary, row = {} }) {
  const candidates = [];
  for (const line of intel.website_issues_observed || []) {
    if (!isGenericIssueText(line)) candidates.push(line.replace(/\.$/, ''));
  }
  const painLine = String(websitePainSummary || '').split(';').map((s) => s.trim()).find(Boolean);
  if (painLine && !isGenericIssueText(painLine)) candidates.push(painLine);

  const findings = collectFindingsFromAssessment(assessmentPayload);
  const byId = new Map(findings.map((f) => [f.id, f]));
  for (const id of NEGATIVE_COMMERCIAL_FINDING_PRIORITY) {
    const label = findingToSpecificIssue(byId.get(id));
    if (label) candidates.push(label);
  }
  for (const finding of findings) {
    if (finding.evidence_class === 'INFERRED') continue;
    if (/present on homepage|returned HTTP 200|served over HTTPS|phone link present|phone number visible/i.test(finding.summary || '')) {
      continue;
    }
    const label = findingToSpecificIssue(finding);
    if (label) candidates.push(label);
  }

  if (!candidates.length) {
    const reviews = Number(row.google_review_count ?? intel.google_review_count);
    if (reviews >= 25) {
      candidates.push('Weak trust proof — Google review strength is not carried through on the homepage');
    } else if ((intel.studio_category || '').includes('property')) {
      candidates.push('Confusing service explanation — property-management value proposition is hard to parse quickly from the homepage');
    } else {
      candidates.push('Poor first-impression credibility — homepage does not quickly establish who you serve and why to trust you');
    }
  }

  return [...new Set(candidates.filter(Boolean))];
}

function pickSpecificWebsiteIssue(options, usedIssueKeys = null) {
  const candidates = listCandidateWebsiteIssues({ row: options.row, ...options });
  if (!candidates.length) return null;
  if (!usedIssueKeys) return candidates[0];
  for (const candidate of candidates) {
    const key = candidate.toLowerCase().slice(0, 48);
    if (!usedIssueKeys.has(key)) return candidate;
  }
  return candidates[0];
}

function assignFirstWaveCandidates(prospects, rowsById) {
  const ranked = prospects
    .map((prospect) => {
      const row = rowsById.get(prospect.prospect_id) || {};
      const maxPriority = scoreScoutProspect(row).max_priority_score;
      return {
        prospect,
        maxPriority,
        fit: Number(prospect.studio_fit_score_raw || 0),
      };
    })
    .sort((a, b) => b.maxPriority - a.maxPriority || b.fit - a.fit);

  const waveSlice = ranked.slice(0, FIRST_WAVE_STRONG_TIER_SIZE);
  const waveIds = new Set(waveSlice.map((entry) => entry.prospect.prospect_id));
  for (const entry of ranked) {
    entry.prospect.max_priority_score = entry.maxPriority;
    entry.prospect.first_wave_candidate = waveIds.has(entry.prospect.prospect_id);
  }
  return waveSlice.map((entry) => entry.prospect);
}

async function fetchLatestAssessmentPayload(pool, clientId, domain) {
  if (!pool || !domain) return null;
  const res = await pool.query(
    `SELECT payload FROM website_opportunity_assessments
      WHERE client_id = $1 AND domain = $2
      ORDER BY updated_at DESC NULLS LAST, created_at DESC
      LIMIT 1`,
    [clientId, domain]
  );
  return res.rows[0]?.payload || null;
}

async function runDeepWebsitePass(row, clientId, pool) {
  const intel = parseIntel(row);
  const domain = prospectWebsiteDomain(row, intel);
  if (!domain) return { assessmentPayload: null, issue: null };

  let assessmentPayload = await fetchLatestAssessmentPayload(pool, clientId, domain);
  const existingIssue = pickSpecificWebsiteIssue({
    intel,
    assessmentPayload,
    websitePainSummary: row.website_pain_summary,
  });
  if (existingIssue && !isGenericIssueText(existingIssue)) {
    return { assessmentPayload, issue: existingIssue };
  }

  const { assessDiscoveredBusiness } = require('../../services/webDesignScout');
  const result = await assessDiscoveredBusiness({
    client_id: clientId,
    prospect_id: row.id,
    domain,
    url: `https://${domain}`,
    company: intel.company_name || row.company_name,
    location: intel.location || row.company_location,
    vertical: row.vertical || intel.industry,
    skipPuppeteer: false,
    skipPageSpeed: false,
  }, { pool, skipPuppeteer: false });

  assessmentPayload = result?.assessment ? { assessment: result.assessment } : assessmentPayload;
  const issue = pickSpecificWebsiteIssue({
    intel,
    assessmentPayload,
    websitePainSummary: row.website_pain_summary,
  });
  return { assessmentPayload, issue };
}

async function deepenTopProspectIssues(prospects, rowsById, clientId, pool, topN = 10) {
  const top = prospects.slice(0, topN);
  const usedIssueKeys = new Set();
  for (const record of top) {
    const row = rowsById.get(record.prospect_id);
    if (!row) continue;
    try {
      // eslint-disable-next-line no-await-in-loop
      const { assessmentPayload, issue: cachedIssue } = await runDeepWebsitePass(row, clientId, pool);
      const intel = parseIntel(row);
      const issue = pickSpecificWebsiteIssue({
        intel,
        row,
        assessmentPayload,
        websitePainSummary: row.website_pain_summary,
      }, usedIssueKeys) || cachedIssue;
      if (issue) {
        usedIssueKeys.add(issue.toLowerCase().slice(0, 48));
        record.specific_website_issue = issue;
      }
    } catch (err) {
      record.evidence_source_notes = `${record.evidence_source_notes} | deep_audit_error:${err.message}`;
    }
  }
}

module.exports = {
  GENERIC_ISSUE_PATTERNS,
  FIRST_WAVE_STRONG_TIER_SIZE,
  parseIntel,
  prospectIdentityKey,
  dedupeProspectRows,
  isGenericIssueText,
  listCandidateWebsiteIssues,
  pickSpecificWebsiteIssue,
  assignFirstWaveCandidates,
  runDeepWebsitePass,
  deepenTopProspectIssues,
};
