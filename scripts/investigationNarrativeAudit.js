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

const { extractDealClaims, section, FIELDS, LABELS } = require('../packages/max/workspace/ProspectDealClaims');
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

// ---------------------------------------------------------------------------
// Explainable warnings: short redacted context + abstention diagnostics.
//
// Context is a window of at most 6 words either side of the cue, inside the
// cue's sentence. Redaction fails closed:
//   - the record's own name tokens, emails, URLs, phone numbers, street
//     addresses and long digit runs are masked;
//   - any capitalized word that is not a known deal acronym, month/weekday or
//     common deal-vocabulary word is masked as [word] (it may be a person,
//     business or place);
//   - the masked text is then re-checked; if anything still looks personal, or
//     masking removed most of the window, the context is withheld entirely.
// Lowercase personal names that are not the record's own name cannot be
// recognized; the capitalization rule and the final check are the safeguards.
// ---------------------------------------------------------------------------
const SENTENCE_RE = /[^\n]+?(?:[.;!?](?=\s|$)|$)/gm; // same sentence split as ProspectDealClaims
const SAFE_CAPS = new Set(['BRRRR', 'DSCR', 'ARV', 'SFR', 'COE', 'HELOC', 'REO', 'MLS', 'LTV', 'LTC', 'ROI', 'FHA', 'K',
  'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December',
  'Jan', 'Feb', 'Mar', 'Apr', 'Jun', 'Jul', 'Aug', 'Sep', 'Sept', 'Oct', 'Nov', 'Dec',
  'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']);
const DEAL_VOCAB = new Set(('a an the and or of for to in on at by with from into after before about around approximately roughly '
  + 'this that these those it its is are was were be been has have had will would should could may might can plans plan planning '
  + 'intends intend expects expected targeting target targeted requesting request requests requested needs need needed seeking seeks '
  + 'looking wants want asking ask borrow borrowing raising offers offering offered proposed proposes owns owned own purchased purchase '
  + 'acquired identified under contract signed executed recently currently active investor investors operator borrower property '
  + 'properties duplex triplex fourplex house home sfr single family multifamily condo townhouse lot building rehab renovation repair '
  + 'flip flips flipping fix resale resell sell sale selling sold rental rentals rent hold buy refinance refinanced refi exit strategy '
  + 'closing close closed closes settlement escrow scheduled set date by within weeks days months week day month year years first '
  + 'second third position lien mortgage deed trust loan loans funding financing capital gap equity bridge private hard money cash '
  + 'budget cost costs price value valued profit proceeds projected estimated deal deals project projects portfolio public profile '
  + 'no not never without unknown unclear missing tbd none also only two three four five several multiple new existing current prior '
  + 'free clear long term short term focused experienced verified unverified claimed').split(/\s+/));
const NAME_LIKE = /\b[A-Z][A-Za-z'’.-]*/g;
const ADDRESS = /\b\d{1,6}\s+(?:[A-Za-z0-9'.-]+\s+){0,3}(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Boulevard|Dr|Drive|Ln|Lane|Ct|Court|Way|Pl|Place|Pkwy|Parkway|Hwy|Highway|Cir|Circle|Ter|Terrace)\b\.?/gi;
const RESIDUAL_PERSONAL = /@|https?:|www\.|\b\d{3}[\s.-]?\d{3}[\s.-]?\d{4}\b/i;

function safeContext(windowText, row) {
  let text = String(windowText || '')
    .replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, '[email]')
    .replace(/\bhttps?:\/\/\S+|\bwww\.\S+/gi, '[url]')
    .replace(/\+?\(?\d[\d\s().-]{8,}\d/g, m => (m.replace(/\D/g, '').length >= 10 ? '[phone]' : m))
    .replace(ADDRESS, '[address]')
    .replace(/(?<![$\d,.])\d{5,}\b/g, '[number]');
  const nameTokens = [row.first_name, row.last_name].flatMap(v => String(v || '').split(/\s+/)).filter(t => t.length >= 2);
  for (const token of nameTokens) text = text.replace(new RegExp(`\\b${escapeRe(token)}\\b`, 'gi'), '[name]');
  let masked = 0;
  text = text.replace(NAME_LIKE, word => {
    const bare = word.replace(/[.'’-]+$/, '');
    if (SAFE_CAPS.has(bare) || DEAL_VOCAB.has(bare.toLowerCase())) return word;
    masked += 1;
    return '[word]';
  });
  text = text.replace(/\s+/g, ' ').trim();
  const words = text.split(' ').filter(Boolean);
  const placeholders = words.filter(w => /^\W*\[(?:word|name|email|url|phone|address|number)\]\W*$/.test(w)).length;
  if (RESIDUAL_PERSONAL.test(text) || nameTokens.some(t => new RegExp(`\\b${escapeRe(t)}\\b`, 'i').test(text))) {
    return { context: null, withheld: 'redaction_unverified' };
  }
  if (words.length && placeholders / words.length > 0.5) return { context: null, withheld: 'mostly_redacted' };
  return { context: text.length > 140 ? `${text.slice(0, 137)}...` : text, masked };
}

function cueOccurrences(fit, cue, limit = 3) {
  const out = [];
  const global = new RegExp(cue.source, cue.flags.includes('g') ? cue.flags : `${cue.flags}g`);
  for (const s of String(fit || '').matchAll(SENTENCE_RE)) {
    const sentence = s[0].trim();
    for (const m of sentence.matchAll(global)) {
      const before = sentence.slice(0, m.index).split(/\s+/).filter(Boolean);
      const after = sentence.slice(m.index).split(/\s+/).filter(Boolean);
      const window = [...before.slice(-6), ...after.slice(0, 7)].join(' ');
      out.push({ sentence, cue: m[0], index: m.index, window: `${before.length > 6 ? '… ' : ''}${window}${after.length > 7 ? ' …' : ''}` });
      if (out.length >= limit) return out;
    }
  }
  return out;
}

// Confirmed reasons are facts established by running the real extractor (or
// reading the record's labels). Hypotheses are this script's heuristics about
// why no extraction pattern matched; they are not extractor behaviour.
function labelPresent(notes, field) {
  return new RegExp(`(?:^|[;\\n])[ \\t]*(?:Fit evidence:[ \\t]*|Risks:[ \\t]*)?(?:${LABELS[field].join('|')}):`, 'i').test(String(notes || ''));
}
const NEGATION_OR_HEDGE = /\b(?:no|not|never|without|unknown|unclear|missing|tbd|none|nor|lacks?|isn't|doesn't|hasn't|may|might|could|possibly|perhaps|maybe|if|whether|considering|potentially)\b/i;
const HYPOTHESES = {
  amount: [
    ['no_request_wording_before_amount', 'amount is not directly preceded by request wording (requesting/needs/seeking/looking for/wants/borrow)',
      (s, i) => !/\b(?:request\w*|need\w*|seek\w*|sought|looking\s+for|asking\s+for|ask\w*\s+for|wants?|borrow\w*)\s+(?:(?:a|an)\s+)?$/i.test(s.slice(0, i))],
    ['non_request_amount_context', 'sentence describes a budget, price, value, profit, rent or existing debt',
      s => /\b(?:budget|ARV|after[- ]repair|price|valu(?:e|ed|ation)|worth|profit|proceeds|rent|income|owes?|owed|mortgage|debt|payoff)\b/i.test(s)],
    ['approximate_or_range', 'amount is approximate, open-ended or a range', (s, i) => /~|\b(?:about|approx\w*|roughly|around|up\s+to|at\s+least)\s*$/i.test(s.slice(0, i)) || /^\$?\s?\d[\d,.]*\s?[kKmM]?\s*(?:\+|-|–|—|to\s+\$)/.test(s.slice(i))],
    ['amount_purpose_ambiguous', 'amount is followed by "for …" without request wording, so it may be a budget rather than a request', (s, i) => /^\$\s?\d[\d,.]*\s?[kK]?\s+for\b/.test(s.slice(i))],
  ],
  stage: [
    ['track_record_wording', 'wording describes past deals or a portfolio, not the current deal', s => /\b\d+\s+(?:properties|deals|flips|units|homes|houses|rentals|doors)\b|\bportfolio\b|\bhas\s+(?:purchased|acquired|owned)\b/i.test(s)],
    ['ownership_without_subject_property', 'ownership/purchase word without "a/the <property>" or "free and clear" in the form the extractor recognizes', s => /\b(?:owns?|owned|purchased|acquired)\b/i.test(s)],
    ['contract_without_status', 'purchase contract mentioned without signed/executed/under-contract status', s => /\bpurchase\s+(?:contract|agreement)\b/i.test(s) && !/\b(?:signed|executed|under\s+contract|in\s+place|recorded)\b/i.test(s)],
  ],
  security: [
    ['existing_debt_context', 'lien or mortgage appears to describe existing debt', s => /\b(?:existing|current|prior|underlying|owes?|owed|holds?|held|behind|senior)\b/i.test(s)],
    ['lien_term_without_position', 'lien or mortgage mentioned without a first/second/third position', s => !/\b(?:first|1st|second|2nd|third|3rd)\b/i.test(s)],
  ],
  exit: [
    ['generic_strategy_term', 'strategy term (BRRRR, rental, flip, DSCR, sale) without exit/plan wording, which may describe the investor rather than this deal\'s exit',
      s => /\b(?:BRRRR|rental|flip\w*|DSCR|sale|resale|buy[- ]and[- ]hold)\b/i.test(s) && !/\b(?:exit|plans?\s+to|planning\s+to|intends?\s+to|will)\b/i.test(s)],
    ['exit_alternatives', 'more than one exit is mentioned', s => /\b(?:or|\/)\s*(?:a\s+)?(?:refinanc|refi|sale|sell|resale|rent|hold|BRRRR)/i.test(s)],
  ],
  closing: [
    ['no_date_or_timeframe', 'closing word without a date or "in/within N days/weeks/months"', s => !/\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2}|\d{1,3}\s+(?:days?|weeks?|months?))\b/i.test(s)],
    ['past_tense_or_history', 'closing word refers to past deals', s => /\bclosed\b|\bclosings?\s+(?:on|of)\s+\d+\b/i.test(s)],
    ['exit_or_refinance_context', 'closing appears after exit/refinance wording (exit timelines are never closing dates)', (s, i) => /\b(?:exit|refinanc\w*|refi|resale|resell|sell|sale|DSCR)\b/i.test(s.slice(0, i))],
  ],
};

function explainOccurrence(row, field, occ) {
  const confirmed = [];
  const isolated = extractDealClaims(`Fit evidence: ${occ.sentence}`);
  const isolatedHit = Boolean(isolated.claims[field] || isolated.unparsed.some(u => u.field === field));
  if (labelPresent(row.notes, field)) {
    confirmed.push({ id: 'labelled_field_governs', text: 'a labelled field for this category exists in the notes; the extractor uses only the labelled value, and it is absent, unknown or outside Fit evidence' });
  }
  confirmed.push(isolatedHit
    ? { id: 'sentence_matches_in_isolation', text: 'the extractor does match this sentence on its own; the record-level result differs because of other record content' }
    : { id: 'no_pattern_matches_sentence', text: 'the extractor finds no claim for this category in this sentence on its own' });
  const hypotheses = isolatedHit ? [] : (HYPOTHESES[field] || []).filter(([, , test]) => test(occ.sentence, occ.index)).map(([id, text]) => ({ id, text }));
  const { clause } = (() => {
    const parts = occ.sentence.split(/,\s+|\s+(?:but|however|although|though|whereas)\s+/i);
    let at = 0;
    for (const p of parts) { const start = occ.sentence.indexOf(p, at); if (occ.index >= start && occ.index <= start + p.length) return { clause: p }; at = start + p.length; }
    return { clause: occ.sentence };
  })();
  if (!isolatedHit && NEGATION_OR_HEDGE.test(clause)) hypotheses.push({ id: 'negation_or_hedge_in_clause', text: 'the cue\'s clause contains a negation or hedge word' });
  const { context, withheld, masked } = safeContext(occ.window, row);
  return { context, contextWithheld: withheld || null, masked: masked || 0, confirmed, hypotheses };
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
      const occurrences = cueOccurrences(fit, MISSED_CUES[field]);
      findings.push({ type: 'possible_missed_claim', field, cue: redact(occurrences[0].cue, row),
        occurrences: occurrences.map(occ => ({ cue: redact(occ.cue, row), ...explainOccurrence(row, field, occ) })) });
    }
    if (claim && SUSPICIOUS[field] && SUSPICIOUS[field].test(claim.snippet)) {
      const occ = cueOccurrences(claim.snippet, SUSPICIOUS[field], 1)[0];
      const { context, withheld } = safeContext(occ.window, row);
      findings.push({ type: 'possible_false_positive', field, cue: redact(occ.cue, row),
        occurrences: [{ cue: redact(occ.cue, row), context, contextWithheld: withheld || null, confirmed: [], hypotheses: [] }] });
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
      for (const x of r.findings) {
        lines.push(`  ${r.id} REVIEW ${x.type}${x.field ? ` ${FIELD_NAMES[x.field]}` : ''}`);
        for (const o of x.occurrences || []) {
          lines.push(`    cue "${o.cue}" — context: ${o.context ? `"${o.context}"` : `withheld (${o.contextWithheld})`}`);
          for (const c of o.confirmed) lines.push(`      confirmed: ${c.text}`);
          for (const h of o.hypotheses) lines.push(`      hypothesis (unconfirmed): ${h.text}`);
        }
      }
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
  if (report.readOnly) {
    const n = report.readOnly.nested || {};
    lines.push(`Read-only: ${report.readOnly.transaction}; end state ${report.readOnly.endState}; ${report.readOnly.reads} reads; ${report.readOnly.writesBlocked} blocked statements; nested savepoints begun ${n.begun || 0}, released ${n.released || 0}, rolled back ${n.rolledBack || 0}, cleaned up ${n.cleanedUp || 0}.`);
  }
  lines.push('No writes, imports, assignments, outreach or specialist actions were performed (READ ONLY transaction, rolled back).');
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Guarded connection with savepoint-based nested transactions.
//
// The audit holds ONE connection inside the outer READ ONLY transaction. Some
// loaders (the canonical Blueprint projection) take a connection via
// connect() and run their own BEGIN READ ONLY … COMMIT/ROLLBACK. Passing those
// through would let their COMMIT end the outer transaction (escape); rejecting
// them made the first production run report CANONICAL_PROJECTION_FAILURE
// without reading anything. Instead, on a connect()-ed session:
//   BEGIN [READ ONLY] [ISOLATION LEVEL …] [DEFERRABLE] -> SAVEPOINT audit_nested_N
//   COMMIT / END                                       -> RELEASE SAVEPOINT
//   ROLLBACK / ABORT                                   -> ROLLBACK TO SAVEPOINT + RELEASE
// Everything else must be a single SELECT/WITH statement (existing guard).
// Rejected: READ WRITE, a second nested BEGIN, COMMIT with no nested
// transaction, explicit SAVEPOINT/RELEASE/ROLLBACK TO, AND CHAIN, PREPARE
// TRANSACTION, SET TRANSACTION, and any multi-statement text. A ROLLBACK with
// no nested transaction is a no-op that is never sent (it would end the outer
// transaction). The outer transaction is only ever ended by main()'s ROLLBACK.
// ---------------------------------------------------------------------------
const TXN_BEGIN = /^(?:begin|start\s+transaction)(?:\s+(?:work|transaction))?(?:\s+(?:read\s+only|isolation\s+level\s+(?:serializable|repeatable\s+read|read\s+committed|read\s+uncommitted)|deferrable|not\s+deferrable)\s*,?)*$/i;
const TXN_COMMIT = /^(?:commit|end)(?:\s+(?:work|transaction))?$/i;
const TXN_ROLLBACK = /^(?:rollback|abort)(?:\s+(?:work|transaction))?$/i;
const TXN_CONTROL = /^(?:begin|start\s+transaction|commit|end|rollback|abort|savepoint|release|prepare\s+transaction|set\s+(?:local\s+|session\s+)?transaction|set\s+session\s+characteristics)\b/i;
const statementText = sql => String(typeof sql === 'string' ? sql : (sql && sql.text) || '').trim().replace(/;\s*$/, '').replace(/\s+/g, ' ');

function guardedConnection(client, boundary) {
  const guarded = readOnlyQueryable(client, boundary);
  boundary.nested = boundary.nested || { begun: 0, released: 0, rolledBack: 0, ignoredRollbacks: 0, cleanedUp: 0, rejected: 0 };
  const state = { open: null, seq: 0 };
  const reject = code => {
    boundary.writesBlocked += 1; boundary.nested.rejected += 1;
    throw Object.assign(new Error(code), { code: 'AUDIT_TRANSACTION_GUARD' });
  };
  const singleRead = async (sql, params) => {
    // One statement only: "SELECT 1; COMMIT" would otherwise pass a
    // starts-with-SELECT check and end the outer transaction.
    if (statementText(sql).includes(';')) reject('audit_multiple_statements_rejected');
    return guarded.query(sql, params);
  };
  const closeOpen = () => {
    const { name } = state.open;
    state.open = null;
    // Queued back to back on the same client, in order.
    return Promise.all([client.query(`ROLLBACK TO SAVEPOINT ${name}`), client.query(`RELEASE SAVEPOINT ${name}`)]);
  };
  async function nestedQuery(session, sql, params) {
    const text = statementText(sql);
    if (text.includes(';')) reject('audit_multiple_statements_rejected');
    if (TXN_BEGIN.test(text)) {
      if (state.open) reject('audit_nested_transaction_already_open');
      const name = `audit_nested_${++state.seq}`;
      await client.query(`SAVEPOINT ${name}`);
      state.open = { name, session };
      boundary.nested.begun += 1;
      return { rows: [], rowCount: 0, command: 'BEGIN' };
    }
    if (TXN_COMMIT.test(text)) {
      if (!state.open || state.open.session !== session) reject('audit_commit_without_nested_transaction');
      const { name } = state.open;
      state.open = null;
      await client.query(`RELEASE SAVEPOINT ${name}`);
      boundary.nested.released += 1;
      return { rows: [], rowCount: 0, command: 'COMMIT' };
    }
    if (TXN_ROLLBACK.test(text)) {
      if (!state.open || state.open.session !== session) {
        boundary.nested.ignoredRollbacks += 1;
        return { rows: [], rowCount: 0, command: 'ROLLBACK' };
      }
      await closeOpen();
      boundary.nested.rolledBack += 1;
      return { rows: [], rowCount: 0, command: 'ROLLBACK' };
    }
    if (TXN_CONTROL.test(text)) reject('audit_transaction_control_rejected');
    return guarded.query(sql, params);
  }
  return {
    query: singleRead,
    connect: async () => {
      const session = {};
      return {
        query: (sql, params) => nestedQuery(session, sql, params),
        // A session released with its savepoint still open is rolled back to it.
        release() {
          if (state.open && state.open.session === session) {
            boundary.nested.cleanedUp += 1;
            closeOpen().catch(() => {});
          }
        },
      };
    },
    hasOpenNestedTransaction: () => Boolean(state.open),
  };
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
    // One consistent snapshot for the whole audit; nested BEGINs become
    // savepoints inside it, so they inherit read-only + repeatable read.
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const mode = (await client.query('SHOW transaction_read_only')).rows[0]?.transaction_read_only;
    if (mode !== 'on') throw Object.assign(new Error('read-only transaction unavailable'), { code: 'READ_ONLY_REQUIRED' });
    await client.query("SET LOCAL statement_timeout = '20s'");
    const conn = guardedConnection(client, boundary);
    const report = await runAudit(conn, deps);
    // Prove the outer transaction was never escaped: it must still be open and
    // read-only. An aborted transaction (25P02) is still open and cannot write.
    let endState;
    try {
      endState = (await client.query('SHOW transaction_read_only')).rows[0]?.transaction_read_only === 'on' ? 'read_only' : 'escaped';
    } catch (err) {
      endState = err && err.code === '25P02' ? 'aborted_read_only' : 'unverified';
    }
    if (endState === 'escaped' || endState === 'unverified' || conn.hasOpenNestedTransaction()) {
      out(`AUDIT FAILED: TRANSACTION_ESCAPE_OR_UNVERIFIED (${endState}). Report withheld.`);
      return 2;
    }
    report.readOnly = { transaction: 'REPEATABLE READ, READ ONLY', endState, reads: boundary.reads,
      writesBlocked: boundary.writesBlocked, nested: boundary.nested };
    if (boundary.writesBlocked) {
      report.reasons.push(`${boundary.writesBlocked} statement(s) blocked by the read-only guard`);
      report.conclusion = 'FAIL';
    }
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

module.exports = { TENANT_ID, EXPECTED_COHORT, redact, safeContext, cueOccurrences, explainOccurrence, classifyRecord, inspectBlueprint, guardedConnection, runAudit, formatReport, main };
