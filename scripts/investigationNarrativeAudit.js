#!/usr/bin/env node
'use strict';

/*
 * investigationNarrativeAudit.js — read-only P0 audit for PR #1120.
 *
 * Verifies the narrative deal-claim extractor (packages/max/workspace/
 * ProspectDealClaims.js) against the six real LA Endeavors (tenant 21)
 * review-only `contact_now` prospects, and checks whether the approved
 * Blueprint carries a financing range the current criteria parser recognizes.
 *
 * Standalone diagnostic. Not part of any request path, cron or startup hook;
 * it runs only when an operator invokes it. It reuses the existing
 * DATABASE_URL (db.js), Max's own prospect loader (ProspectRecordIntelligence.
 * loadRecords) and Blueprint loader (ClientIntelligenceContext.
 * loadApprovedClientIntelligence). No new credential, permission or endpoint.
 *
 * Read-only by construction: one connection, `BEGIN TRANSACTION READ ONLY`
 * (verified via SHOW transaction_read_only), every statement through the
 * existing SELECT/WITH-only guard (OperationalInspection.readOnlyQueryable),
 * always ROLLBACK. No writes, imports, assignments, outreach or specialists.
 *
 * Output is redacted: shortened record IDs, per-category states and the
 * minimal matched phrase only. Never names, contact details, URLs or full
 * notes. Blueprint output is limited to financing-range-related facts.
 *
 * Run (operator, from a checkout of the PR branch, Railway CLI linked to the
 * production project):
 *   railway run --service pulseforge-leadgen --environment production \
 *     node scripts/investigationNarrativeAudit.js
 *
 * Exit codes: 0 PASS, 1 FAIL (review findings), 2 access/read unavailable.
 */

const { extractDealClaims, section, FIELDS } = require('../packages/max/workspace/ProspectDealClaims');
const { tenantCriteria, composeInvestigation } = require('../packages/max/workspace/ProspectInvestigationPriority');
const { loadRecords, evaluate } = require('../packages/max/workspace/ProspectRecordIntelligence');
const { readOnlyQueryable } = require('../packages/max/workspace/OperationalInspection');

const TENANT_ID = 21;
const EXPECTED_COHORT = 6;
const FIELD_NAMES = { amount: 'financing request', stage: 'deal stage', security: 'security position', exit: 'exit plan', closing: 'closing date' };
const STATE_CODES = { recorded_unverified: 'R', absent: '-', non_comparable: 'N', contradictory: 'C' };

// Cue words that suggest a category is discussed. A cue with no detected claim
// is a possible missed explicit statement for the operator to check.
const MISSED_CUES = {
  amount: /\$\s?\d/,
  stage: /\b(?:under\s+contract|purchase\s+(?:contract|agreement)|purchased|acquired|owns?|owned|identified|in\s+rehab|rehab\s+(?:underway|started))\b/i,
  security: /\b(?:lien|mortgage|deed\s+of\s+trust|trust\s+deed|first[- ]position|second[- ]position)\b/i,
  exit: /\b(?:exit|refinanc\w*|refi|resale|resell|sell|sale|rental|BRRRR|DSCR)\b/i,
  closing: /\b(?:clos(?:e|es|ing)|settlement|COE)\b/i,
};
// Words that make a detected claim worth a second look (guards exist, but the
// operator confirms on real wording).
const SUSPICIOUS = {
  amount: /\b(?:budget|ARV|after[- ]repair|purchase\s+price|valu(?:e|ed|ation)|appraised|worth|profit|proceeds|projected|estimated?)\b/i,
  security: /\b(?:existing|current|prior|owes?|owed|holds?|held|behind|senior|underlying)\b/i,
  closing: /\b(?:exit|refinanc\w*|refi|resale|sell|sale)\b/i,
};

const shortId = id => String(id || '').slice(0, 8);
const escapeRe = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// Remove anything personal that could ride along inside a matched phrase.
function redact(text, row = {}) {
  let out = String(text || '')
    .replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, '[email]')
    .replace(/\bhttps?:\/\/\S+|\bwww\.\S+/gi, '[url]')
    .replace(/\+?\(?\d[\d\s().-]{8,}\d/g, m => (m.replace(/\D/g, '').length >= 10 ? '[phone]' : m));
  for (const token of [row.first_name, row.last_name, row.email, row.phone, row.company_name]
    .flatMap(v => String(v || '').split(/\s+/)).filter(t => t.length >= 2)) {
    out = out.replace(new RegExp(`\\b${escapeRe(token)}\\b`, 'gi'), '[name]');
  }
  out = out.replace(/\s+/g, ' ').trim();
  return out.length > 80 ? `${out.slice(0, 77)}...` : out;
}

function classifyRecord(row) {
  const details = extractDealClaims(row.notes);
  const fit = section(row.notes, 'Fit evidence') || '';
  const categories = {};
  const findings = [];
  for (const field of FIELDS) {
    const claim = details.claims[field];
    const unparsed = details.unparsed.find(u => u.field === field);
    let entry;
    if (details.conflicts.includes(field)) entry = { state: 'contradictory', phrase: null };
    else if (claim) entry = { state: 'recorded_unverified', origin: claim.origin, phrase: redact(claim.match, row) };
    else if (unparsed) entry = { state: 'non_comparable', reason: unparsed.reason, phrase: redact(unparsed.values.join(' / '), row) };
    else entry = { state: 'absent', phrase: null };
    categories[field] = entry;
    if (entry.state === 'absent' && MISSED_CUES[field].test(fit)) {
      findings.push({ type: 'possible_missed_claim', field, cue: redact(fit.match(MISSED_CUES[field])[0], row) });
    }
    if (claim && SUSPICIOUS[field] && SUSPICIOUS[field].test(claim.snippet)) {
      findings.push({ type: 'possible_false_positive', field, cue: redact(claim.snippet.match(SUSPICIOUS[field])[0], row) });
    }
  }
  if (!fit) findings.push({ type: 'no_fit_evidence_section', field: null, cue: null });
  return { id: shortId(row.id), coverage: details.count, categories, findings, extractor: details.extractor };
}

// Only financing-related service facts are reported; nothing else from the
// Blueprint is printed.
const FINANCING_FACT = /\b(?:financ\w*|loan\w*|lend\w*|borrow\w*|capital|range)\b|\$\s?\d/i;
function inspectBlueprint(understanding, bundleIds) {
  const durable = understanding && understanding._bundle;
  const summary = durable && durable.summary;
  const criteria = tenantCriteria(understanding, bundleIds);
  const known = (summary?.businessFacts?.services || []);
  const candidates = [
    ...known.filter(f => f && typeof f.value === 'string' && FINANCING_FACT.test(f.value)).map(f => ({
      source: 'service_fact', epistemicState: f.epistemic_state || null, hasProvenance: Boolean(f.id && f.provenance),
      text: redact(f.value) })),
    ...(String(durable?.contract?.services || '').split(/\s*\|\s*/).filter(t => FINANCING_FACT.test(t))
      .map(t => ({ source: 'contract_services', epistemicState: null, hasProvenance: null, text: redact(t) }))),
  ];
  return {
    approvedBlueprint: Boolean(summary && summary.approved),
    knowledgeState: durable?.knowledgeState || 'unavailable',
    semanticAuthority: summary?.semanticAuthority || null,
    criteriaTrusted: Boolean(criteria.ref),
    parserRange: criteria.range,
    financingRangeCandidates: candidates,
  };
}

async function loadBlueprint(db, deps) {
  const ctx = deps.clientIntelligence || require('../packages/max/workspace/ClientIntelligenceContext');
  const { buildBusinessUnderstandingContract } = deps.businessUnderstanding || require('../packages/max/workspace/BusinessUnderstandingRetrieval');
  const cie = deps.cie || require('../packages/max/clientIntelligence');
  // Expose only the approved-Blueprint read, bound to the guarded connection.
  const cieService = { getApprovedClientBlueprint: (id, opts = {}) => cie.getApprovedClientBlueprint(id, { ...opts, pool: db }) };
  try {
    const loaded = await ctx.loadApprovedClientIntelligence({ tenantId: String(TENANT_ID), clientId: TENANT_ID,
      cieService, cieOpts: { pool: db }, propagateLoadErrors: true });
    const summary = loaded.summary || null;
    return { _bundle: { tenantId: String(TENANT_ID), summary, blueprintSource: summary ? 'blueprint' : 'none',
      knowledgeState: summary && summary.approved ? 'available' : 'never_learned',
      contract: buildBusinessUnderstandingContract(summary, null, []) } };
  } catch (err) {
    const notFound = err && (err.code === 'not_found' || /No approved blueprint/i.test(String(err.message || '')));
    return { _bundle: { tenantId: String(TENANT_ID), summary: null, blueprintSource: 'none',
      knowledgeState: notFound ? 'never_learned' : 'retrieval_failure', contract: null },
      loadError: notFound ? 'no_approved_blueprint' : (err && (err.code || err.name)) || 'blueprint_load_failed' };
  }
}

/**
 * Pure audit over an already read-only, guarded queryable. Exported for tests.
 */
async function runAudit(db, deps = {}) {
  const report = { tenantId: TENANT_ID, cohort: { expected: EXPECTED_COHORT }, records: [], blueprint: null,
    ranking: null, conclusion: 'FAIL', reasons: [] };
  const result = await loadRecords(db, TENANT_ID);
  report.cohort.retrieval = result.status;
  report.cohort.complete = result.complete === true;
  if (result.status !== 'found' || result.complete !== true) {
    report.reasons.push(`prospect retrieval ${result.status}${result.complete === true ? '' : ' or incomplete'}`);
    return report;
  }
  const scoped = result.rows.filter(r => String(r.client_id) === String(TENANT_ID));
  const cohortRows = scoped.filter(r => r.source === 'review_only_import' && evaluate(r).review === 'contact_now');
  report.cohort.found = cohortRows.length;
  report.records = cohortRows.map(classifyRecord);

  const understanding = await loadBlueprint(db, deps);
  report.blueprint = { ...inspectBlueprint(understanding, { tenantId: String(TENANT_ID), clientId: TENANT_ID }),
    loadError: understanding.loadError || null };

  // Same composer Max uses; only calculated positions are reported (no prose).
  const candidates = cohortRows.map(evaluate);
  const composed = composeInvestigation({ tenantId: String(TENANT_ID), clientId: TENANT_ID,
    prospects: { records: { ...result, rows: cohortRows } } },
    { wantsRanking: true, priorityMode: 'investigation', reviewState: 'contact_now' }, candidates, 0, understanding);
  report.ranking = { ranked: composed.investigationPriority.ranked,
    positions: composed.items.map(i => ({ id: shortId(i.recordId), position: i.priority ? i.priority.position : null,
      coverage: i.priority ? i.priority.evidenceCoverage : null })) };

  if (cohortRows.length !== EXPECTED_COHORT) report.reasons.push(`cohort size ${cohortRows.length}, expected ${EXPECTED_COHORT}`);
  const flagged = report.records.filter(r => r.findings.length);
  if (flagged.length) report.reasons.push(`${flagged.length} record(s) with possible missed claims or false positives`);
  const contradictory = report.records.filter(r => Object.values(r.categories).some(c => c.state === 'contradictory'));
  if (contradictory.length) report.reasons.push(`${contradictory.length} record(s) with contradictory labelled claims`);
  if (!report.ranking.ranked) report.reasons.push('cohort remains unranked');
  report.conclusion = report.reasons.length ? 'FAIL' : 'PASS';
  return report;
}

function formatReport(report) {
  const lines = [`Investigation narrative audit — tenant ${report.tenantId} (read-only)`];
  lines.push(`Cohort: ${report.cohort.found ?? 0}/${report.cohort.expected} review-only contact_now records; retrieval ${report.cohort.retrieval}${report.cohort.complete ? ', complete' : ', incomplete'}.`);
  if (report.records.length) {
    lines.push('', 'Coverage matrix (R recorded-unverified, - absent, N non-comparable, C contradictory):');
    lines.push(['id      ', ...FIELDS.map(f => f.padEnd(8)), 'cover', 'pos'].join(' '));
    for (const r of report.records) {
      const pos = report.ranking?.positions.find(p => p.id === r.id)?.position;
      lines.push([r.id.padEnd(8), ...FIELDS.map(f => STATE_CODES[r.categories[f].state].padEnd(8)), `${r.coverage}/5`.padEnd(5), pos == null ? '-' : String(pos)].join(' '));
    }
    lines.push('', 'Audit phrases (minimal matched text; confirm each interpretation):');
    for (const r of report.records) {
      for (const f of FIELDS) {
        const c = r.categories[f];
        if (c.state === 'absent') continue;
        lines.push(`  ${r.id} ${FIELD_NAMES[f]}: ${c.state}${c.reason ? ` (${c.reason})` : ''}${c.origin ? ` [${c.origin}]` : ''}${c.phrase ? ` "${c.phrase}"` : ''}`);
      }
      for (const x of r.findings) lines.push(`  ${r.id} REVIEW ${x.type}${x.field ? ` ${FIELD_NAMES[x.field]}` : ''}${x.cue ? ` (cue: "${x.cue}")` : ''}`);
    }
  }
  if (report.blueprint) {
    const b = report.blueprint;
    lines.push('', `Blueprint: approved=${b.approvedBlueprint}; knowledge=${b.knowledgeState}; authority=${b.semanticAuthority || 'n/a'}; criteria trusted=${b.criteriaTrusted}${b.loadError ? `; load=${b.loadError}` : ''}.`);
    lines.push(`Financing range recognized by current parser: ${b.parserRange ? `$${b.parserRange.min.toLocaleString('en-US')}–$${b.parserRange.max.toLocaleString('en-US')}` : 'none'}.`);
    if (b.financingRangeCandidates.length) {
      lines.push('Financing-related Blueprint facts:');
      for (const c of b.financingRangeCandidates) lines.push(`  [${c.source}${c.epistemicState ? ` ${c.epistemicState}` : ''}${c.hasProvenance === false ? ' no-provenance' : ''}] "${c.text}"`);
    } else lines.push('Financing-related Blueprint facts: none.');
  }
  if (report.ranking) lines.push('', `Ranking with current code: ${report.ranking.ranked ? 'ranked' : 'unranked'}.`);
  lines.push('', `Conclusion: ${report.conclusion}${report.reasons.length ? ` — ${report.reasons.join('; ')}` : ''}.`);
  lines.push('No writes, imports, assignments, outreach or specialist actions were performed (READ ONLY transaction, rolled back).');
  return lines.join('\n');
}

// A connection already inside BEGIN TRANSACTION READ ONLY, wrapped so every
// statement passes the SELECT/WITH-only guard. Loaders that ask for connect()
// get the same guarded transaction, never a fresh pool connection.
function guardedConnection(client, boundary) {
  const guarded = readOnlyQueryable(client, boundary);
  return { query: guarded.query, connect: async () => ({ query: guarded.query, release() {} }) };
}

const safeError = err => String((err && (err.code || err.name)) || 'error').replace(/[^A-Za-z0-9_]/g, '').slice(0, 40);

async function main({ pool = null, out = console.log, deps = {}, argv = process.argv } = {}) {
  if (!pool && !process.env.DATABASE_URL) {
    out('ACCESS UNAVAILABLE: DATABASE_URL is not set. Run through the authorized Railway environment; do not paste credentials.');
    return 2;
  }
  const db = pool || require('../db');
  let client;
  try {
    client = await db.connect();
  } catch (err) {
    out(`ACCESS UNAVAILABLE: database connection refused (${safeError(err)}). No data was read.`);
    return 2;
  }
  const boundary = { reads: 0, writesBlocked: 0 };
  try {
    await client.query('BEGIN TRANSACTION READ ONLY');
    const mode = (await client.query('SHOW transaction_read_only')).rows[0]?.transaction_read_only;
    if (mode !== 'on') throw Object.assign(new Error('read-only transaction unavailable'), { code: 'READ_ONLY_REQUIRED' });
    await client.query("SET LOCAL statement_timeout = '20s'");
    const report = await runAudit(guardedConnection(client, boundary), deps);
    report.readOnly = { transaction: 'READ ONLY', reads: boundary.reads, writesBlocked: boundary.writesBlocked };
    out(argv.includes('--json') ? JSON.stringify(report, null, 2) : formatReport(report));
    if (boundary.writesBlocked) return 1;
    return report.conclusion === 'PASS' ? 0 : 1;
  } catch (err) {
    const denied = err && (err.code === '42501' || err.code === 'READ_ONLY_REQUIRED');
    out(`${denied ? 'ACCESS UNAVAILABLE' : 'AUDIT FAILED'}: ${safeError(err)}. No data was written.`);
    return 2;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
    if (!pool) await db.end().catch(() => {});
  }
}

if (require.main === module) {
  main().then(code => { process.exitCode = code; });
}

module.exports = { TENANT_ID, EXPECTED_COHORT, redact, classifyRecord, inspectBlueprint, guardedConnection, runAudit, formatReport, main };
