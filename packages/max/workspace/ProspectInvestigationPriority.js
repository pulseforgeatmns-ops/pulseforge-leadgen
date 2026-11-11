'use strict';

// Read-only investigation priority over already retrieved prospect records.
//
// Investigation priority answers "which recorded opportunity is worth looking
// into first?". It is deliberately separate from outreach authorization
// (ProspectRecordIntelligence.evaluate) and from financing suitability. This
// module is pure: no database, no model, no execution API. It never changes
// eligibility, never authorizes contact and never writes records.
//
// Signals come only from recorded fields and bounded, pattern-based reading
// of recorded notes. Each dimension is ordinal (-1 contrary / 0 not recorded,
// conflicting or not assessable / +1 supported) and cites the recorded text.
// Ordering uses evidence dominance: a record is placed above another only when
// it is at least as strong on every dimension and stronger on at least one.
// No weights are invented, so records stronger on different dimensions share
// a position and indistinguishable records are never forced into an order.

const POLICY = 'evidence-dominance-v1';
const MAX_NOTE_CHARS = 4000;
const MAX_SENTENCES = 80;
const MAX_NAMES = 5;

const DIMENSIONS = Object.freeze([
  { key: 'dealSpecificity', label: 'deal specificity' },
  { key: 'financingFit', label: 'financing amount vs tenant criteria' },
  { key: 'securityPosition', label: 'security position / collateral clarity' },
  { key: 'exitClarity', label: 'exit clarity' },
  { key: 'trackRecord', label: 'operator track record' },
  { key: 'verification', label: 'verification gaps' },
  { key: 'timing', label: 'timing' },
]);

// Lines that are workflow boilerplate, plans or link lists, not evidence.
const NON_EVIDENCE_LINE = /^(?:review state|next action|sources?|public profile)\s*:|review-only prospect import|outreach[^.]*not authorized/i;
const RISK_LINE = /^(?:risks?|concerns?|red flags?)\s*:/i;
const FINANCING_CONTEXT = /\b(loans?|financ\w*|capital|funding|funds?|borrow\w*|gaps?|need(?:s|ed)?|request\w*|seek(?:s|ing)?|rais(?:e|ing)|bridge)\b/i;
const AMOUNT_RE = /\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|mm|m|million|thousand)?\b/gi;

const PATTERNS = {
  dealSpecificity: {
    neg: /\b(no (?:specific|active|current|identified|recorded) (?:deal|project|property)|no deal (?:identified|recorded)|speculative|generic (?:interest|inquiry))\b/i,
  },
  securityPosition: {
    pos: /\b(first[- ]lien|1st[- ](?:lien|position)|first[- ]position|senior (?:lien|debt)|second[- ]lien|2nd[- ](?:lien|position)|second[- ]position|junior lien|collateral(?:ized)?|secured by|deed of trust|clear (?:title|ownership)|free and clear|owns? the (?:property|asset))\b/i,
    neg: /\b(?:unclear|unknown|unverified|unconfirmed|disputed|no|missing)\s+(?:\w+\s+){0,2}(?:collateral|ownership|title|lien|security)\b|\bunsecured\b|\b(?:collateral|ownership|title)\s+(?:is\s+|remains\s+)?(?:unclear|unknown|unverified|unconfirmed)\b/i,
  },
  exitClarity: {
    pos: /\b(exit(?: plan| strategy)?\s*(?::|-|is|via|through|by)|refinanc\w*|refi\b|dscr|resale|retail sale|sale of the (?:property|asset)|sell(?:ing)? (?:the )?(?:property|asset|units?) (?:after|upon|once)|take-?out)\b/i,
    neg: /\b(?:no|unclear|unknown|uncertain|undefined|weak|without(?: a)?|missing)\s+(?:\w+\s+){0,2}exit\b|\bexit\s+(?:plan\s+|strategy\s+)?(?:is\s+|remains\s+)?(?:unclear|unknown|uncertain|undefined|unverified)\b/i,
  },
  trackRecord: {
    pos: /\b(\d+\+?\s+(?:completed\s+|prior\s+|past\s+)?(?:flips|deals|projects|properties|rehabs|transactions|renovations)|(?:\d+|several|multiple|many)\+?\s+years?(?:\s+of)?\s+(?:\w+\s+)?experience|experienced (?:operator|investor|flipper|developer|builder|borrower)|track record|repeat (?:borrower|operator|investor)|portfolio of)\b/i,
    neg: /\b(first[- ]time|no (?:prior |verified |documented |known )?(?:track record|experience|deals|projects|flips)|inexperienced|new (?:investor|operator|flipper)|limited (?:track record|experience)|track record\s+(?:is\s+)?(?:unverified|unknown|unclear|unconfirmed))\b/i,
  },
  verification: {
    neg: /\b(unverified|not (?:yet )?verified|unconfirmed|needs? (?:to be )?verif\w*|verify|could not (?:confirm|verify)|stale|outdated|unknown|unclear)\b/i,
  },
  timing: {
    pos: /\b(time[- ]sensitive|urgent|asap|deadline|clos(?:e|ing) (?:by|in|within|on)|within \d+ (?:days|weeks)|under contract|auction|expir\w+|this (?:month|quarter)|immediate(?:ly)?)\b/i,
    neg: /\b(?:project|deal|property|loan)\s+(?:was\s+|is\s+|has been\s+)?(?:completed|sold|closed|funded)\b|\balready (?:funded|closed|sold|financed)\b|\b(stale|inactive|dormant|no longer active)\b/i,
  },
};

const ADDRESS_RE = /\b\d{2,6}\s+(?:[A-Z][a-z]+\s+){1,3}(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Ln|Lane|Blvd|Boulevard|Ct|Court|Way|Pl|Place|Pkwy|Ter|Terrace|Cir|Circle)\b/;
const PROPERTY_RE = /\b(under contract|(?:single|multi)[- ]family|duplex|triplex|fourplex|\d+[- ]unit|condo|townhome|mixed[- ]use|storefront|warehouse)\b/i;
const PROJECT_RE = /\b(fix[- ]and[- ]flip|flip|rehab(?:ilitation)?|renovation|brrrr|ground[- ]up|new construction|buy[- ]and[- ]hold|equity gap|bridge)\b/i;

function parseAmount(num, suffix) {
  const n = Number(String(num).replace(/,/g, ''));
  if (!Number.isFinite(n) || n <= 0) return null;
  const s = String(suffix || '').toLowerCase();
  const scale = s === 'k' || s === 'thousand' ? 1e3 : s === 'm' || s === 'mm' || s === 'million' ? 1e6 : 1;
  return Math.round(n * scale);
}
const money = n => (n >= 1e6 ? `$${+(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `$${+(n / 1e3).toFixed(1)}k` : `$${n}`);
const quote = s => `"${String(s).trim().replace(/\s+/g, ' ').slice(0, 160)}"`;

function sentencesOf(notes) {
  const out = [];
  for (const rawLine of String(notes || '').slice(0, MAX_NOTE_CHARS).split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || NON_EVIDENCE_LINE.test(line)) continue;
    const risk = RISK_LINE.test(line);
    for (const s of line.split(/(?<=[.;!?])\s+/)) {
      const text = s.trim();
      if (text) out.push({ text, risk });
      if (out.length >= MAX_SENTENCES) return out;
    }
  }
  return out;
}

function amountsIn(sentences) {
  const amounts = [];
  for (const s of sentences) {
    if (!FINANCING_CONTEXT.test(s.text)) continue;
    for (const m of s.text.matchAll(AMOUNT_RE)) {
      const value = parseAmount(m[1], m[2]);
      if (value) amounts.push({ value, evidence: s.text });
    }
  }
  return amounts;
}

/**
 * Tenant financing-amount criteria, only when the approved business
 * understanding explicitly records a numeric range. Never defaulted.
 */
function tenantFinancingCriteria(understanding) {
  const contract = (understanding && understanding.contract) || null;
  if (!contract) return null;
  const fields = ['services', 'targetCustomers', 'constraints', 'valueProposition', 'businessGoals'];
  const texts = fields.map(f => contract[f]).concat(Array.isArray(contract.currentPriorities) ? contract.currentPriorities : [])
    .filter(v => typeof v === 'string' && v.trim());
  for (const t of texts) {
    for (const sentence of t.split(/(?<=[.;!?])\s+/)) {
      if (!FINANCING_CONTEXT.test(sentence) && !/\b(deal|loan) size|amount\b/i.test(sentence)) continue;
      const range = sentence.match(/\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|mm|m|million|thousand)?\s*(?:-|–|—|to|and)\s*\$?\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|mm|m|million|thousand)?\b/i);
      if (range) {
        let min = parseAmount(range[1], range[2] || (Number(range[1]) < 1000 ? range[4] : null));
        let max = parseAmount(range[3], range[4]);
        if (min && max && min > max) [min, max] = [max, min];
        if (min && max) return { min, max, evidence: sentence.trim() };
      }
      const up = sentence.match(/\b(?:up to|max(?:imum)?(?: of)?|no more than)\s+\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|mm|m|million|thousand)?\b/i);
      const low = sentence.match(/\b(?:min(?:imum)?(?: of)?|at least|starting at)\s+\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|mm|m|million|thousand)?\b/i);
      if (up || low) {
        return { min: low ? parseAmount(low[1], low[2]) : null, max: up ? parseAmount(up[1], up[2]) : null, evidence: sentence.trim() };
      }
    }
  }
  return null;
}

function patternDimension(sentences, { pos, neg }, { riskCountsPositive = false } = {}) {
  const support = [];
  const contrary = [];
  const concerns = [];
  for (const s of sentences) {
    if (neg && neg.test(s.text)) { contrary.push(s.text); continue; }
    if (pos && pos.test(s.text)) (s.risk && !riskCountsPositive ? concerns : support).push(s.text);
  }
  if (support.length && contrary.length) {
    return { value: 0, state: 'conflicting', evidence: [support[0], contrary[0]], concerns };
  }
  if (support.length) return { value: 1, state: 'supported', evidence: [support[0]], concerns };
  if (contrary.length) return { value: -1, state: 'contrary', evidence: [contrary[0]], concerns };
  return { value: 0, state: concerns.length ? 'flagged_as_risk' : 'not_recorded', evidence: concerns.slice(0, 1), concerns };
}

/**
 * Extract per-dimension recorded signals from one prospect record.
 * @param {object} row prospect record (already tenant-scoped)
 * @param {{min:number|null,max:number|null}|null} criteria tenant amount criteria
 */
function extractSignals(row, criteria) {
  const sentences = sentencesOf(row.notes);
  const evidenceSentences = sentences.filter(s => !s.risk);
  const amounts = amountsIn(sentences);
  const dims = {};

  // Deal specificity: count distinct concrete deal facts actually recorded.
  const facts = [];
  const addr = evidenceSentences.find(s => ADDRESS_RE.test(s.text));
  if (addr) facts.push(['property address', addr.text]);
  const prop = evidenceSentences.find(s => PROPERTY_RE.test(s.text));
  if (prop) facts.push(['property/contract detail', prop.text]);
  const proj = evidenceSentences.find(s => PROJECT_RE.test(s.text));
  if (proj) facts.push(['project type', proj.text]);
  if (amounts.length) facts.push(['capital amount', amounts[0].evidence]);
  const specNeg = sentences.find(s => PATTERNS.dealSpecificity.neg.test(s.text));
  if (specNeg && facts.length < 2) {
    dims.dealSpecificity = { value: -1, state: 'contrary', evidence: [specNeg.text], facts: facts.map(f => f[0]) };
  } else if (facts.length >= 2) {
    dims.dealSpecificity = { value: 1, state: 'supported', evidence: [...new Set(facts.map(f => f[1]))].slice(0, 2), facts: facts.map(f => f[0]) };
  } else {
    dims.dealSpecificity = { value: 0, state: facts.length ? 'partial' : 'not_recorded', evidence: [...new Set(facts.map(f => f[1]))], facts: facts.map(f => f[0]) };
  }

  // Financing amount against tenant criteria: only when both are recorded.
  if (!amounts.length) {
    dims.financingFit = { value: 0, state: 'amount_not_recorded', evidence: [] };
  } else if (!criteria || (criteria.min == null && criteria.max == null)) {
    dims.financingFit = { value: 0, state: 'criteria_not_recorded', evidence: [amounts[0].evidence], amounts: amounts.map(a => a.value) };
  } else {
    const inside = a => (criteria.min == null || a.value >= criteria.min) && (criteria.max == null || a.value <= criteria.max);
    const ins = amounts.filter(inside);
    const out = amounts.filter(a => !inside(a));
    dims.financingFit = ins.length && !out.length ? { value: 1, state: 'within_criteria', evidence: [ins[0].evidence], amounts: amounts.map(a => a.value) }
      : out.length && !ins.length ? { value: -1, state: 'outside_criteria', evidence: [out[0].evidence], amounts: amounts.map(a => a.value) }
        : { value: 0, state: 'conflicting', evidence: [ins[0].evidence, out[0].evidence], amounts: amounts.map(a => a.value) };
  }

  dims.securityPosition = patternDimension(sentences, PATTERNS.securityPosition);
  dims.exitClarity = patternDimension(sentences, PATTERNS.exitClarity);
  dims.trackRecord = patternDimension(sentences, PATTERNS.trackRecord);
  dims.timing = patternDimension(sentences, PATTERNS.timing);

  // Verification gaps only subtract: absence of a flagged gap is not proof
  // of verification, so this dimension is never positive.
  const gaps = [];
  const flagged = sentences.filter(s => PATTERNS.verification.neg.test(s.text));
  if (flagged.length) gaps.push(`recorded as unverified/unclear: ${quote(flagged[0].text)}`);
  if (!row.source) gaps.push('source attribution not recorded');
  const urls = row.acquisition_metadata && Array.isArray(row.acquisition_metadata.sourceUrls) ? row.acquisition_metadata.sourceUrls : [];
  if (!urls.length && !/\bhttps?:\/\//i.test(String(row.notes || ''))) gaps.push('no provenance URL recorded');
  if (!row.email && !row.phone && !row.linkedin_url) gaps.push('no contact channel recorded');
  if (!sentences.length) gaps.push('no supporting notes recorded');
  dims.verification = { value: gaps.length ? -1 : 0, state: gaps.length ? 'gaps_recorded' : 'no_gap_flagged', evidence: flagged.slice(0, 1).map(s => s.text), gaps };

  return dims;
}

const vector = dims => DIMENSIONS.map(d => dims[d.key].value);
function dominates(a, b) {
  let strict = false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] < b[i]) return false;
    if (a[i] > b[i]) strict = true;
  }
  return strict;
}
const betterOn = (a, b) => DIMENSIONS.filter((_, i) => a[i] > b[i]).map(d => d.label);

/**
 * Dominance layering. Returns position per id (1-based), or null for all when
 * the evidence yields a single layer (no defensible differentiation).
 */
function layer(candidates) {
  let remaining = candidates.slice();
  const positions = new Map();
  let position = 0;
  while (remaining.length) {
    position++;
    const front = remaining.filter(c => !remaining.some(o => o !== c && dominates(o.vector, c.vector)));
    for (const c of front) positions.set(c.id, position);
    remaining = remaining.filter(c => !front.includes(c));
  }
  return { positions, layers: position };
}

function describeDimension(d, dim) {
  const ev = dim.evidence && dim.evidence.length ? ` — recorded: ${dim.evidence.map(quote).join(' / ')}` : '';
  switch (dim.state) {
    case 'supported': return `${d.label}: supported${d.key === 'dealSpecificity' ? ` (${dim.facts.join(', ')})` : ''}${ev}`;
    case 'contrary': return `${d.label}: contrary evidence${ev}`;
    case 'conflicting': return `${d.label}: conflicting recorded evidence${ev}`;
    case 'flagged_as_risk': return `${d.label}: mentioned only as a risk${ev}`;
    case 'partial': return `${d.label}: partial (${dim.facts.join(', ')} only)${ev}`;
    case 'within_criteria': return `${d.label}: recorded amount ${dim.amounts.map(money).join(', ')} is within the tenant's recorded criteria${ev}`;
    case 'outside_criteria': return `${d.label}: recorded amount ${dim.amounts.map(money).join(', ')} is outside the tenant's recorded criteria${ev}`;
    case 'criteria_not_recorded': return `${d.label}: recorded amount ${dim.amounts.map(money).join(', ')}; not assessable — no tenant amount criteria recorded${ev}`;
    case 'amount_not_recorded': return `${d.label}: no capital amount recorded`;
    case 'gaps_recorded': return `${d.label}: ${dim.gaps.join('; ')}`;
    case 'no_gap_flagged': return `${d.label}: none flagged in the record (not independently verified)`;
    default: return `${d.label}: not recorded`;
  }
}

const NEXT_STEPS = {
  verification: 'Resolve the recorded verification gaps from the cited sources or public records',
  financingFit: 'Establish the capital need from recorded sources or public project evidence and compare it with the tenant\'s criteria',
  securityPosition: 'Confirm collateral, ownership and lien position from public property records',
  exitClarity: 'Establish the likely exit (sale or refinance) from recorded project evidence',
  trackRecord: 'Verify the operator\'s prior completed projects from public records',
  dealSpecificity: 'Identify the specific property and project behind this record',
  timing: 'Confirm whether the project and capital need are current',
};

const STEP_ORDER = ['verification', 'financingFit', 'securityPosition', 'exitClarity', 'trackRecord', 'dealSpecificity', 'timing'];
function nextStep(dims) {
  // Contrary evidence is investigated before merely missing evidence. A
  // recorded amount without tenant criteria is a tenant gap, not a prospect gap.
  const open = key => {
    const d = dims[key];
    if (key === 'verification') return d.value < 0;
    if (key === 'financingFit' && d.state === 'criteria_not_recorded') return false;
    if (key === 'timing' && d.state === 'not_recorded' && dims.dealSpecificity.value > 0) return false;
    return d.value <= 0;
  };
  const key = STEP_ORDER.find(k => open(k) && dims[k].value < 0) || STEP_ORDER.find(open);
  return key ? `${NEXT_STEPS[key]}. Investigation only — do not contact the prospect to obtain it without separate authorization.`
    : 'Prepare an operator review of the cited evidence. Investigation only — any outreach requires separate authorization.';
}

const STATE_TEXT = { criteria_not_recorded: 'not assessable (no tenant criteria recorded)', amount_not_recorded: 'no amount recorded',
  not_recorded: 'not recorded', flagged_as_risk: 'flagged as a risk', within_criteria: 'within tenant criteria',
  outside_criteria: 'outside tenant criteria', conflicting: 'conflicting', supported: 'supported', contrary: 'contrary' };
function financingSuitability(dims) {
  const parts = ['financingFit', 'securityPosition', 'exitClarity'].map(k => {
    const d = dims[k];
    const label = DIMENSIONS.find(x => x.key === k).label;
    return `${label} ${STATE_TEXT[d.state] || d.state.replace(/_/g, ' ')}`;
  });
  const values = ['financingFit', 'securityPosition', 'exitClarity'].map(k => dims[k].value);
  const overall = values.some(v => v < 0) ? 'concerns recorded' : values.every(v => v > 0) ? 'recorded evidence consistent with criteria' : 'undetermined';
  return { overall, detail: parts.join('; ') };
}

const nameOf = c => [c.row.first_name, c.row.last_name].filter(Boolean).join(' ') || 'Unnamed record';
const names = list => list.length <= MAX_NAMES ? list.map(nameOf).join(', ')
  : `${list.slice(0, MAX_NAMES).map(nameOf).join(', ')} and ${list.length - MAX_NAMES} more`;

function relativePosition(c, all, positions) {
  const out = [];
  const above = all.filter(o => o !== c && dominates(c.vector, o.vector) && positions.get(o.id) === positions.get(c.id) + 1);
  const below = all.filter(o => o !== c && dominates(o.vector, c.vector) && positions.get(o.id) === positions.get(c.id) - 1);
  const peers = all.filter(o => o !== c && positions.get(o.id) === positions.get(c.id));
  if (below.length) {
    const reasons = [...new Set(below.flatMap(o => betterOn(o.vector, c.vector)))];
    out.push(`below ${names(below)} (stronger recorded ${reasons.join(', ')}; not weaker on any dimension)`);
  }
  if (above.length) {
    const reasons = [...new Set(above.flatMap(o => betterOn(c.vector, o.vector)))];
    out.push(`above ${names(above)} (stronger recorded ${reasons.join(', ')}; not weaker on any dimension)`);
  }
  if (peers.length) {
    const same = peers.filter(o => o.vector.every((v, i) => v === c.vector[i]));
    const differing = peers.filter(o => !same.includes(o));
    if (same.length) out.push(`shares position with ${names(same)} (indistinguishable on recorded evidence)`);
    if (differing.length) out.push(`shares position with ${names(differing)} (stronger on different dimensions; no tenant weighting is recorded to separate them)`);
  }
  return out.join('; ') || 'no other candidate to compare';
}

const EXCLUDED_REVIEW = new Set(['reject', 'rejected', 'hold']);
const TERMINAL_STATUS = new Set(['dead', 'disqualified', 'closed', 'reject', 'rejected', 'hold', 'held']);

/**
 * Investigation candidates: excludes rejected/held review decisions and
 * terminal pipeline states. Outreach gates (DNC, review clearance, score)
 * do NOT exclude a record from read-only analysis.
 */
function isInvestigationCandidate(c) {
  return !EXCLUDED_REVIEW.has(c.review) && !TERMINAL_STATUS.has(c.row.status) && !TERMINAL_STATUS.has(c.row.setter_status);
}

function outreachLine(c) {
  if (c.eligible) {
    return c.ready ? 'eligible under recorded criteria; separate operator authorization still required before any contact'
      : 'eligible but incomplete; separate operator authorization still required before any contact';
  }
  return `blocked — ${c.blockers.join(', ')}`;
}

/**
 * @param {Array} evaluated ProspectRecordIntelligence.evaluate() results (tenant-filtered, unique)
 * @param {{understanding?:object, topCount?:number}} opts
 */
function rankForInvestigation(evaluated, opts = {}) {
  const criteria = tenantFinancingCriteria(opts.understanding);
  const candidates = evaluated.filter(isInvestigationCandidate).map(c => {
    const dims = extractSignals(c.row, criteria);
    return { ...c, dims, vector: vector(dims) };
  });
  const excluded = evaluated.length - candidates.length;
  const { positions, layers } = layer(candidates);
  const differentiated = layers > 1;
  return { criteria, candidates, excluded, positions, layers, differentiated };
}

function composeInvestigationLines(result, { clientId, topCount = 5, outreachPriority = new Map() }) {
  const { criteria, candidates, excluded, positions, differentiated } = result;
  const sorted = candidates.slice().sort((a, b) =>
    (differentiated ? positions.get(a.id) - positions.get(b.id) : 0) ||
    ((outreachPriority.get(a.id)?.position ?? Infinity) - (outreachPriority.get(b.id)?.position ?? Infinity)) ||
    a.id.localeCompare(b.id));
  let selected = sorted;
  if (differentiated && sorted.length > topCount) {
    const cutoff = positions.get(sorted[topCount - 1].id);
    selected = sorted.filter(c => positions.get(c.id) <= cutoff);
  }
  const eligibleCount = candidates.filter(c => c.eligible).length;
  const lines = [];
  lines.push('Provisional investigation priority — read-only analysis of recorded evidence. This is not an outreach recommendation, not a financing decision and not opportunity value.');
  lines.push(`Outreach authorization: ${eligibleCount} of ${candidates.length} candidates meet the recorded outreach criteria. ${eligibleCount ? 'Even eligible records require separate operator authorization before any contact.' : 'Outreach remains blocked for every candidate.'} Investigation priority does not change any outreach gate.`);
  lines.push(`Method (${POLICY}): each candidate is assessed on ${DIMENSIONS.map(d => d.label).join(', ')}, using only recorded fields and recorded notes. A record is placed above another only when its recorded evidence is at least as strong on every dimension and stronger on at least one. No weights are applied; records stronger on different dimensions share a position.`);
  lines.push(criteria
    ? `Tenant financing criteria (from the approved business understanding): ${criteria.min != null ? money(criteria.min) : 'no minimum'} to ${criteria.max != null ? money(criteria.max) : 'no maximum'} — recorded: ${quote(criteria.evidence)}.`
    : 'Tenant financing criteria: no financing-amount range is recorded in this workspace\'s approved business understanding, so recorded amounts are not judged against criteria.');
  if (excluded) lines.push(`${excluded} record(s) excluded from investigation priority because they are rejected, held or in a terminal pipeline state.`);
  lines.push(differentiated
    ? `Recorded evidence separates the ${candidates.length} candidates into ${result.layers} investigation positions; showing ${selected.length}${selected.length > Math.min(topCount, sorted.length) ? ' (ties at the cutoff included)' : ''}.`
    : candidates.length
      ? `No investigation order is given: the recorded evidence does not support a defensible differentiation between these ${candidates.length} candidates (${candidates.every(c => c.vector.every((v, i) => v === candidates[0].vector[i])) ? 'they are indistinguishable on every assessed dimension' : 'each is stronger on different dimensions and no tenant weighting is recorded'}). They are listed unordered with the evidence and gaps for each.`
      : 'No records qualify for investigation priority.');
  const items = selected.map(c => {
    const position = differentiated ? positions.get(c.id) : null;
    const reference = `prospect:${clientId}:${c.id}`;
    const suit = financingSuitability(c.dims);
    const uncertainties = DIMENSIONS.filter(d => c.dims[d.key].value <= 0 && c.dims[d.key].state !== 'no_gap_flagged').map(d => d.label);
    const claim = [
      `${nameOf(c)} [${reference}]`,
      `investigation priority: ${position ? `position ${position} (provisional, calculated)` : 'unordered (not differentiated by recorded evidence)'}`,
      `relative position: ${differentiated ? relativePosition(c, candidates, positions) : 'not ranked'}`,
      `financing suitability: ${suit.overall} — ${suit.detail}`,
      `outreach authorization: ${outreachLine(c)}`,
      `evidence: ${DIMENSIONS.map(d => describeDimension(d, c.dims[d.key])).join(' | ')}`,
      `review: ${c.review || 'not recorded'}; pipeline: ${c.row.status || 'not recorded'}; source: ${c.row.source || 'not recorded'}; qualification score: ${c.row.icp_score ?? 'not recorded'} (not used for investigation priority)`,
      `uncertainties: ${uncertainties.join(', ') || 'current validity of recorded information is unverified'}`,
      `next investigative step: ${nextStep(c.dims)}`,
    ].join('; ');
    lines.push(`- ${claim}`);
    return {
      recordRef: reference, recordId: c.id, sourceKind: 'prospects', provenance: 'prospects table',
      epistemic: 'verified', claim, retrievalStatus: 'found',
      priority: outreachPriority.get(c.id) || null,
      investigationPriority: position ? { position, epistemic: 'calculated', policy: POLICY, provisional: true,
        dimensions: Object.fromEntries(DIMENSIONS.map(d => [d.key, c.dims[d.key].value])) } : null,
      financingSuitability: suit.overall,
      outreachAuthorization: c.eligible ? 'requires_operator_authorization' : 'blocked',
      limitations: [...c.missing, ...uncertainties.map(u => `unsupported: ${u}`)],
    };
  });
  return { lines, items };
}

module.exports = {
  POLICY,
  DIMENSIONS,
  tenantFinancingCriteria,
  extractSignals,
  rankForInvestigation,
  composeInvestigationLines,
  isInvestigationCandidate,
  dominates,
};
