'use strict';

// Pure policy + bounded SELECT on the operating-evidence pool. No execution API.
const { SETTER_VISIBILITY_THRESHOLD } = require('../../../utils/qualificationThreshold');
const { anchorOutreachReviewValueSql } = require('../../../utils/anchorOutreachReview');
const LIMIT = 1000;
const PAGE_SIZE = 10;
const enabled = () => !/^(false|0|off)$/i.test(process.env.MAX_PROSPECT_RECORD_INTELLIGENCE || '');
const requested = input => enabled() && Boolean(input.operationalRead &&
  (input.operationalRead.wantsRecordList || input.operationalRead.wantsRanking));

async function loadRecords(pool, clientId, { after = null } = {}) {
  if (!Number.isSafeInteger(clientId) || clientId <= 0 || !pool?.query) {
    return { status: 'unavailable', rows: [], complete: false };
  }
  if (after != null && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(after)) return { status: 'unavailable', rows: [], complete: false };
  const rows = [];
  let cursor = after;
  try {
    // Keyset pagination, including one lookahead record. Scope every page and
    // the company-review subquery at the query boundary. Optional columns are
    // projected through to_jsonb so older schemas fail only on actual reads.
    while (rows.length <= LIMIT) {
      const result = await pool.query(`SELECT p.id, p.client_id,
        to_jsonb(p) - 'acquisition_metadata' AS record,
        p.acquisition_metadata,
        ${anchorOutreachReviewValueSql('p')} AS ao_outreach_review_required
        FROM prospects p
        WHERE p.client_id = $1 AND ($2::uuid IS NULL OR p.id > $2::uuid)
        ORDER BY p.id ASC LIMIT $3`, [clientId, cursor, Math.min(PAGE_SIZE, LIMIT + 1 - rows.length)]);
      const page = result.rows || [];
      if (page.some(r => String(r.client_id) !== String(clientId) || !r.id)) throw new Error('scope_or_identity');
      rows.push(...page.map(r => ({ ...r.record, id: r.id, client_id: r.client_id,
        acquisition_metadata: r.acquisition_metadata,
        ao_outreach_review_required: r.ao_outreach_review_required })));
      if (page.length < Math.min(PAGE_SIZE, LIMIT + 1 - (rows.length - page.length))) break;
      const next = page.at(-1)?.id;
      if (!next || next === cursor) throw new Error('pagination_stalled');
      cursor = next;
    }
    return { status: rows.length ? 'found' : 'empty', rows: rows.slice(0, LIMIT), complete: rows.length <= LIMIT,
      limit: LIMIT, after, order: 'id ASC', nextCursor: rows.length > LIMIT ? rows[LIMIT - 1].id : null };
  } catch (_) {
    // Never label partial pages verified following a failed query.
    return { status: 'failed', rows: [], complete: false };
  }
}
const text = value => value == null || value === '' ? 'not recorded' : String(value);
function evaluate(row) {
  const metadata = row.acquisition_metadata || {};
  const review = metadata.reviewState;
  const missing = [];
  if (!row.first_name && !row.last_name) missing.push('name');
  if (!metadata.category) missing.push('prospect type');
  if (!row.source) missing.push('source attribution');
  if (!review) missing.push('review state');
  if (!row.status) missing.push('pipeline status');
  if (!row.notes) missing.push('supporting notes');
  const blocked = ['reject', 'rejected', 'hold', 'qualify_first'].includes(review) ||
    ['dead', 'disqualified', 'bounced', 'do_not_email', 'closed', 'reject', 'rejected', 'hold', 'held'].includes(row.status) ||
    ['dead', 'closed', 'reject', 'rejected', 'hold', 'held'].includes(row.setter_status) ||
    row.do_not_contact !== false || row.ao_outreach_review_required !== false;
  const qualified = Number.isFinite(Number(row.icp_score)) && row.icp_score != null &&
    Number(row.icp_score) >= SETTER_VISIBILITY_THRESHOLD && Boolean(String(row.service_area_match || '').trim());
  const eligible = !blocked && review === 'contact_now' && qualified;
  const ready = eligible && !missing.length && Boolean(row.email || row.phone || row.linkedin_url);
  if (!row.email && !row.phone && !row.linkedin_url) missing.push('contact channel');
  if (row.do_not_contact !== false) missing.push('DNC clearance');
  if (row.ao_outreach_review_required !== false) missing.push('outreach review clearance');
  if (!qualified) missing.push('qualification score >= 70 and service-area gate');
  // Only recorded engagement state establishes a priority tier. Score, review
  // state, alphabetical order and ID never establish opportunity value.
  const tier = row.status === 'hot' || row.is_hot === true ? 2 : row.status === 'warm' ? 1 : 0;
  return { id: String(row.id), row, review, eligible, qualified, ready, missing, tier,
    next: !eligible ? 'Review recorded gates and missing information; do not initiate outreach.' :
      !ready ? 'Verify missing information before proposing outreach.' :
        'Propose an operator review of the recorded contact channel and notes before any outreach; separate authorization is required.' };
}
function composeRecords(bundle, read) {
  const result = bundle.prospects?.records;
  if (!result) return null; // Legacy/injected count-only loaders keep their disclosure.
  const status = result.status;
  if (!['found', 'empty'].includes(status)) return {
    prose: `Prospect record retrieval: ${status}. ${status === 'failed' ? 'The query failed; no records were verified.' : 'The record store was not available; no records were verified.'}`,
    recordIntelligence: true, items: [], sourceStatuses: { prospects: status }, operationalReadFocused: true,
    used: ['prospects'], recommend: false, launchedScout: false,
  };
  const seen = new Set();
  let duplicates = 0;
  const candidates = [];
  // Strict second boundary for injected loaders. Never allow ownerless rows.
  for (const row of result.rows || []) {
    if (!row?.id || String(row.client_id) !== String(bundle.clientId)) continue;
    if (seen.has(String(row.id))) { duplicates++; continue; }
    seen.add(String(row.id));
    candidates.push(evaluate(row));
  }
  candidates.sort((a, b) => a.id.localeCompare(b.id));
  const filtered = read.reviewState ? candidates.filter(c => c.review === read.reviewState) : candidates;
  const eligible = filtered.filter(c => c.eligible);
  const rankable = read.wantsRanking && !duplicates && !result.after && result.complete === true && eligible.length > 0 && eligible.every(c => c.ready);
  // Ties share priority; ID orders display only. Never invent a unique winner.
  if (rankable) eligible.sort((a, b) => b.tier - a.tier || a.id.localeCompare(b.id));
  const count = read.topCount || 5;
  const selected = rankable ? eligible.filter(c => c.tier >= eligible[Math.min(count, eligible.length) - 1].tier) : filtered;
  const lines = [rankable ? 'Calculated outreach priority (recorded engagement tiers: hot, warm, cold; ties share position). This is not estimated opportunity value.' :
    read.wantsRanking ? 'Unranked candidates: evidence is insufficient for a defensible outreach ordering. All eligible candidates need complete identity, type, source, status, notes and a contact channel; retrieval must be complete.' : 'Actual prospect records for this workspace.'];
  if (read.reason === 'ranking_follow_up_after_operational_read') lines.unshift('This is a fresh record read and priority calculation; changes since the earlier response may change the ordering.');
  lines.push(`Retrieved ${candidates.length} unique records; showing ${selected.length}. Order: ${rankable ? 'engagement tier, then ID for ties' : 'record ID'}. Retrieval ${result.complete === true ? 'complete' : 'incomplete'}.`);
  if (result.complete !== true) lines.push(`Bounded read stopped at ${result.limit || LIMIT} records. Additional records may exist; no full-set ranking is claimed. Continue with: List my prospects after record ID ${text(result.nextCursor)}.`);
  if (result.after) lines.push(`Continuation after record ID ${result.after}; this is a subset, not a full-set ranking.`);
  if (duplicates) lines.push(`${duplicates} duplicate record IDs suppressed. Distinct IDs with matching names remain separate records.`);
  if (read.wantsRanking) lines.push(`${eligible.length} records meet the conservative recorded outreach criteria; excluded records are not outreach recommendations.`);
  const items = selected.map(c => {
    const r = c.row;
    const position = rankable ? 1 + eligible.filter(other => other.tier > c.tier).length : null;
    const reference = `prospect:${bundle.clientId}:${c.id}`;
    const claim = `${text([r.first_name, r.last_name].filter(Boolean).join(' '))} [${reference}]; type: ${text(r.acquisition_metadata?.category)}; business category: ${text(r.vertical)}; review: ${text(c.review)}; pipeline: ${text(r.status)}; setter: ${text(r.setter_status)}; source: ${text(r.source)}; provenance URLs: ${text(Array.isArray(r.acquisition_metadata?.sourceUrls) ? r.acquisition_metadata.sourceUrls.join(', ') : null)}; qualification score: ${text(r.icp_score)} (qualification only; not opportunity value); service area: ${text(r.service_area_match)}; eligible: ${c.eligible}; readiness: ${c.ready ? 'enough recorded information for a proposal' : 'incomplete or blocked'}; ${position ? `priority: ${position}, because recorded engagement tier is ${c.tier} (${text(r.status)} / is_hot=${text(r.is_hot)}); ` : ''}recorded notes: ${text(r.notes)}; created: ${text(r.created_at)}; updated: ${text(r.updated_at)}; last contacted: ${text(r.last_contacted_at)}; missing/uncertain: ${c.missing.join(', ') || 'current validity of recorded information is unverified'}; next action (proposal): ${c.next}`;
    lines.push(`- ${claim}`);
    return { recordRef: reference, recordId: c.id, sourceKind: 'prospects', provenance: 'prospects table',
      epistemic: 'verified', claim, priority: position ? { position, tier: c.tier, epistemic: 'calculated', policy: 'engagement-tier-v1' } : null,
      limitations: c.missing, retrievalStatus: 'found' };
  });
  if (!selected.length) lines.push('No matching authorized records were returned by the prospects table query.');
  lines.push('Recorded facts are not independently verified external facts. Recommendations are proposals only. No emails, calls, assignments, imports, modifications, or specialist execution occurred.');
  return { prose: lines.join('\n\n'), items, sourceStatuses: { prospects: status }, operationalReadFocused: true,
    recordIntelligence: true, used: ['prospects'], recommend: false, launchedScout: false };
}
module.exports = { requested, loadRecords, evaluate, composeRecords, LIMIT, PAGE_SIZE };
