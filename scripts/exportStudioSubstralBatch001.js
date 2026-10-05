#!/usr/bin/env node
'use strict';

/**
 * Export Studio Substral Outbound Batch 001 for human review (no send).
 *
 * Usage:
 *   node scripts/exportStudioSubstralBatch001.js [--min-fit=7] [--limit=25]
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { resolveStudioSubstralClientId } = require('../utils/studioSubstralTenant');
const { STUDIO_MIN_FIT_SCORE } = require('../services/studioSubstralScoutIntelligence');

const BATCH_ID = 'OUTBOUND-BATCH-001';
const OUT_DIR = path.join(__dirname, '..', 'artifacts', 'studio-substral');

function parseArgs() {
  const minFitTen = Number(process.argv.find((a) => a.startsWith('--min-fit='))?.split('=')[1] || 7);
  const limit = Number(process.argv.find((a) => a.startsWith('--limit='))?.split('=')[1] || 25);
  // Align 1–10 review scale with scout floor (65 ≈ 7/10).
  const minStudioScore = Math.max(STUDIO_MIN_FIT_SCORE, minFitTen * 10 - 5);
  return { minFitTen, minStudioScore, limit };
}

function fitScoreTen(studioFitScore) {
  return Math.max(1, Math.min(10, Math.round(Number(studioFitScore || 0) / 10)));
}

function parseCity(intel, row) {
  const loc = intel.location || row.notes || '';
  const m = String(loc).match(/([A-Za-z][A-Za-z\s]+),\s*NH\b/);
  if (m) return m[1].trim();
  const svc = row.service_area_match;
  if (svc) return String(svc).split(',')[0].trim();
  return loc || 'NH';
}

function buildCredibilityGapDiagnosis(intel) {
  const issue = (intel.website_issues_observed || [])[0];
  const strength = (intel.business_strength_signals || [])[0];
  if (strength && issue) {
    return `${strength}, but the site still ${issue.replace(/\.$/, '').toLowerCase()} — a credibility gap versus what buyers expect before they call or book.`;
  }
  if (intel.recommended_outreach_angle) {
    return intel.recommended_outreach_angle;
  }
  return 'Established local operator whose website likely undersells operational credibility.';
}

function buildFirstTouchEmail(intel, row) {
  const dm = intel.decision_maker_name || row.first_name || '';
  const greeting = dm ? `Hi ${dm.split(/\s+/)[0]},` : 'Hi there,';
  const issue = (intel.website_issues_observed || [])[0] || 'the homepage does not make the next step obvious';
  const company = intel.company_name || 'your team';
  const body = [
    greeting,
    '',
    `I was looking at ${company}'s site and noticed ${issue.replace(/\.$/, '').toLowerCase()}.`,
    'From the outside, the business looks more established than the site currently communicates — which can slow trust before someone calls, requests a quote, or books.',
    '',
    'I run Studio Substral. We start with a structured website assessment (diagnosis before design) so you know what is actually worth fixing.',
    '',
    'Would you be open to a quick assessment conversation, or should I send a short overview of what we look at?',
    '',
    'Jacob',
    'Studio Substral',
  ].join('\n');
  return body;
}

function commercialWhy(intel) {
  if (intel.conversion_or_trust_risk) return intel.conversion_or_trust_risk;
  const cat = (intel.studio_category || 'local service').replace(/_/g, ' ');
  return `For ${cat}, buyers decide on trust before they reach out; a weak site adds friction to calls, quotes, and bookings.`;
}

async function loadProspects(clientId, minStudioScore, limit) {
  const res = await pool.query(
    `SELECT p.*, c.name AS company_name, c.location AS company_location, c.website AS company_website
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND p.studio_fit_score IS NOT NULL
        AND p.studio_fit_score >= $2
        AND COALESCE(p.studio_outreach_status, 'new') NOT IN ('not_fit', 'closed')
      ORDER BY p.studio_fit_score DESC, p.created_at DESC
      LIMIT $3`,
    [clientId, minStudioScore, limit]
  );
  return res.rows;
}

function toProspectRecord(row) {
  const intel = typeof row.studio_scout_intelligence === 'object'
    ? row.studio_scout_intelligence
    : JSON.parse(row.studio_scout_intelligence || '{}');
  const fitTen = fitScoreTen(row.studio_fit_score);
  const websiteUrl = row.website_url || intel.website_url || row.company_website || '';
  const issue = (intel.website_issues_observed || [])[0]
    || row.website_pain_summary?.split(';')[0]
    || 'Homepage trust and conversion path need strengthening';

  return {
    batch_id: BATCH_ID,
    prospect_id: row.id,
    business_name: intel.company_name || row.company_name || `${row.first_name || ''} ${row.last_name || ''}`.trim(),
    website_url: websiteUrl,
    city_town: parseCity(intel, row),
    segment: (intel.studio_category || row.studio_category || row.vertical || '').replace(/_/g, ' '),
    decision_maker_name: intel.decision_maker_name || [row.first_name, row.last_name].filter(Boolean).join(' ') || '',
    decision_maker_role: intel.decision_maker_role || row.job_title || '',
    contact_path: intel.contact_path || [row.email, row.phone].filter(Boolean).join(' | '),
    specific_website_issue: issue,
    commercial_why_it_matters: commercialWhy(intel),
    credibility_gap_diagnosis: buildCredibilityGapDiagnosis(intel),
    fit_score_1_to_10: fitTen,
    studio_fit_score_raw: row.studio_fit_score,
    recommended_outreach_angle: intel.recommended_outreach_angle || row.recommended_outreach_angle,
    suggested_first_touch_email: buildFirstTouchEmail(intel, row),
    evidence_source_notes: [
      intel.confidence ? `confidence:${intel.confidence}` : null,
      row.google_review_count != null ? `google_reviews:${row.google_review_count}@${row.google_rating ?? 'n/a'}` : null,
      (intel.business_strength_signals || []).slice(0, 2).join('; ') || null,
      (intel.website_issues_observed || []).slice(0, 3).join('; ') || row.website_pain_summary,
      `scout_status:${row.studio_outreach_status || 'new'}`,
    ].filter(Boolean).join(' | '),
    outreach_status: row.studio_outreach_status,
    first_wave_candidate: fitTen >= 9,
  };
}

function renderMarkdown(batch) {
  const lines = [
    `# Studio Substral — ${BATCH_ID}`,
    '',
    `Generated: ${batch.generated_at}`,
    `Prospects: ${batch.prospect_count} (min fit ${batch.min_fit_1_to_10}/10)`,
    `First-wave candidates (9–10): ${batch.first_wave_candidates.length}`,
    '',
    '**Human review required — do not send until approved.**',
    '',
  ];

  batch.prospects.forEach((p, i) => {
    lines.push(`## ${i + 1}. ${p.business_name} (fit ${p.fit_score_1_to_10}/10)`);
    lines.push('');
    lines.push(`| Field | Value |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Website | ${p.website_url} |`);
    lines.push(`| City | ${p.city_town} |`);
    lines.push(`| Segment | ${p.segment} |`);
    lines.push(`| Decision maker | ${p.decision_maker_name || '—'} (${p.decision_maker_role || '—'}) |`);
    lines.push(`| Contact | ${p.contact_path} |`);
    lines.push(`| Website issue | ${p.specific_website_issue} |`);
    lines.push(`| Commercial why | ${p.commercial_why_it_matters} |`);
    lines.push(`| Credibility gap | ${p.credibility_gap_diagnosis} |`);
    lines.push(`| Outreach angle | ${p.recommended_outreach_angle} |`);
    lines.push(`| Evidence | ${p.evidence_source_notes} |`);
    lines.push('');
    lines.push('**Suggested first-touch email**');
    lines.push('');
    lines.push('```');
    lines.push(p.suggested_first_touch_email);
    lines.push('```');
    lines.push('');
  });

  if (batch.first_wave_candidates.length) {
    lines.push('---');
    lines.push('');
    lines.push('## Suggested first send wave (5–10)');
    lines.push('');
    batch.first_wave_candidates.slice(0, 10).forEach((p) => {
      lines.push(`- **${p.business_name}** (${p.segment}, fit ${p.fit_score_1_to_10}) — ${p.city_town}`);
    });
  }

  return lines.join('\n');
}

function buildBatchFromRows(rows, { clientId, minFitTen, limit }) {
  const prospects = rows.map(toProspectRecord);
  const firstWave = prospects.filter((p) => p.fit_score_1_to_10 >= 8).slice(0, 10);
  return {
    batch_id: BATCH_ID,
    generated_at: new Date().toISOString(),
    client_id: clientId,
    min_fit_1_to_10: minFitTen,
    prospect_count: prospects.length,
    prospects: prospects.slice(0, limit),
    first_wave_candidates: firstWave,
    tracking: {
      prospects_contacted: 0,
      positive_replies: 0,
      discovery_calls: 0,
      assessment_requests: 0,
      objections: [],
      segment_response_quality: {},
    },
  };
}

async function main() {
  const { minFitTen, minStudioScore, limit } = parseArgs();
  const clientId = await resolveStudioSubstralClientId();
  const rows = await loadProspects(clientId, minStudioScore, limit);
  const batch = buildBatchFromRows(rows, { clientId, minFitTen, limit });

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const jsonPath = path.join(OUT_DIR, 'outbound-batch-001.json');
  const mdPath = path.join(OUT_DIR, 'outbound-batch-001.md');
  fs.writeFileSync(jsonPath, JSON.stringify(batch, null, 2));
  fs.writeFileSync(mdPath, renderMarkdown(batch));

  console.log(`[export] Wrote ${batch.prospect_count} prospects to ${jsonPath}`);
  console.log(`[export] Markdown review pack: ${mdPath}`);
  if (batch.prospect_count < limit) {
    console.warn(`[export] WARNING: only ${batch.prospect_count}/${limit} prospects at fit >= ${minFitTen}. Run scout batch to fill inventory.`);
    process.exitCode = 2;
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = {
  BATCH_ID,
  parseArgs,
  fitScoreTen,
  loadProspects,
  toProspectRecord,
  buildBatchFromRows,
};
