'use strict';

// Read-only analytical policy. No database, LLM, specialist or execution API.
const { createHash } = require('node:crypto');
const POLICY = 'recorded-deal-coverage-v1';
const enabled = () => !/^(false|0|off|no|disabled)$/i.test(process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY || '');
function canonical(value) {
  if (value instanceof Date) return value.toJSON();
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])]));
  return value;
}
const fingerprint = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const clean = value => String(value || '').trim();

// Parse only explicit labels, never infer financing facts from free prose,
// profile descriptions, review state, qualification score or contact channels.
const LABELS = {
  amount: ['Financing request', 'Funding request', 'Loan amount', 'Requested financing'],
  stage: ['Deal stage'], security: ['Security position', 'Lien position'],
  exit: ['Exit plan'], closing: ['Closing date'],
};
function section(notes, label) {
  const lines = String(notes || '').split('\n');
  const start = lines.findIndex(line => line.toLowerCase().startsWith(label.toLowerCase() + ':'));
  if (start < 0) return null;
  const content = [lines[start].slice(label.length + 1).trim()];
  for (const line of lines.slice(start + 1)) {
    if (/^(?:Review state|Market|Fit evidence|Risks|Next action|Public profile|Sources):/i.test(line)) break;
    content.push(line);
  }
  return content.join('\n').trim();
}
function recordedDetails(notes) {
  const input = section(notes, 'Fit evidence') || String(notes || '');
  const values = {};
  const conflicts = [];
  for (const [key, labels] of Object.entries(LABELS)) {
    const regex = new RegExp(`(?:^|[;\\n])\\s*(?:${labels.join('|')}):\\s*([^;\\n]+)`, 'gi');
    const allClaims = String(notes || '').replace(/^(?:Fit evidence|Risks):\s*/gmi, '');
    const allMatches = [...allClaims.matchAll(regex)].map(m => clean(m[1]));
    const matches = [...input.matchAll(regex)].map(m => clean(m[1]));
    const unique = [...new Set(matches)];
    if (new Set(allMatches).size > 1) { conflicts.push(key); continue; }
    if (unique.length === 1 && !/\b(?:unknown|unclear|missing|not recorded|not known|not verified|unverified|none|no|TBD|to be determined)\b/i.test(unique[0])) values[key] = unique[0];
  }
  const amount = values.amount?.match(/^\$([\d,]+)$/);
  if (values.amount && (!amount || !Number.isSafeInteger(Number(amount[1].replace(/,/g, ''))) || Number(amount[1].replace(/,/g, '')) <= 0)) { delete values.amount; conflicts.push('unparsed amount'); }
  return { values, amount: amount && values.amount ? Number(amount[1].replace(/,/g, '')) : null, conflicts,
    count: Object.keys(values).length };
}

function tenantCriteria(understanding, bundle) {
  const durable = understanding?._bundle;
  const summary = durable?.summary;
  const contract = durable?.contract;
  const canonicalSource = summary?.semanticAuthority === 'CANONICAL' && summary?.canonicalSnapshotId;
  const approvedRecord = durable?.blueprintSource === 'blueprint' && summary?.blueprintId &&
    summary?.status === 'approved';
  const trusted = durable?.knowledgeState === 'available' && String(durable.tenantId) === String(bundle.tenantId) &&
    summary?.approved === true && String(summary.clientId) === String(bundle.clientId) &&
    (canonicalSource || approvedRecord);
  if (!trusted) return { ref: null, description: null, range: null,
    limitation: 'Authorized approved business criteria are unavailable; no financing range or business-fit assumption was substituted.' };
  // Use original KNOWN facts with provenance before lossy prose projections
  // (the current summary can split comma-formatted monetary amounts).
  const known = key => (Array.isArray(summary.businessFacts?.[key]) ? summary.businessFacts[key] : []).filter(f => f.epistemic_state === 'KNOWN' &&
    f.id && f.provenance && typeof f.value === 'string');
  const serviceFacts = known('services');
  const constraintFacts = known('disqualified_customers');
  const services = serviceFacts.length ? serviceFacts.map(f => f.value).join(' | ') : canonicalSource ? contract?.services : null;
  const constraints = constraintFacts.length ? constraintFacts.map(f => f.value).join(' | ') : canonicalSource ? contract?.constraints : null;
  const description = [services, constraints].filter(Boolean).join(' | ');
  const matches = [...String(services || '').matchAll(/(?:financing|loan|lending|working financing)\s+range:\s*\$([\d,]+)\s*(?:-|–|—|to)\s*\$([\d,]+)/gi)];
  const range = matches.length === 1 ? { min: Number(matches[0][1].replace(/,/g, '')), max: Number(matches[0][2].replace(/,/g, '')) } : null;
  return { ref: `blueprint:${bundle.clientId}:${summary.canonicalSnapshotId || summary.blueprintId}`, description,
    factRefs: [...serviceFacts, ...constraintFacts].map(f => ({ id: f.id, provenance: f.provenance })),
    range: range && Number.isSafeInteger(range.min) && Number.isSafeInteger(range.max) && range.min > 0 && range.max >= range.min ? range : null,
    limitation: 'Criteria describe the approved business context, not verified prospect suitability. Only a single explicit dollar range can be compared; currency, terms and current capacity still require verification.' };
}
function financeConcern(details, criteria) {
  if (details.conflicts.length) return `Conflicting or unparsed financing/deal information (${details.conflicts.join(', ')}); reconcile the recorded claims before assessing suitability.`;
  if (!criteria.range) return 'Financing-fit comparison unavailable: no unambiguous authorized financing range. No range assumed.';
  if (details.amount == null) return 'No explicit financing request recorded; financing fit cannot be assessed.';
  return details.amount < criteria.range.min || details.amount > criteria.range.max
    ? `Recorded request $${details.amount.toLocaleString('en-US')} is outside the recorded business range $${criteria.range.min.toLocaleString('en-US')}–$${criteria.range.max.toLocaleString('en-US')}; this is a potential mismatch, not a financing decision.`
    : 'Recorded request is within the recorded business range; this alone does not establish suitability, lien security or probability of funding.';
}
function shouldCompose(read, candidates) {
  return enabled() && read.wantsRanking && read.priorityMode !== 'outreach' &&
    (read.priorityMode === 'investigation' || candidates.some(c => c.row.source === 'review_only_import'));
}
function composeInvestigation(bundle, read, candidates, duplicates, understanding) {
  const result = bundle.prospects.records;
  const criteria = tenantCriteria(understanding, bundle);
  const criteriaFingerprint = fingerprint(criteria);
  const prior = read.previousRanking?.tenantId === String(bundle.tenantId) ? read.previousRanking : null;
  const scoped = candidates.filter(c => String(c.row.client_id) === String(bundle.clientId));
  const selectedIds = prior?.tenantId === String(bundle.tenantId) ? prior.selectedRecordIds : null;
  const cohort = selectedIds ? scoped.filter(c => selectedIds.includes(c.id)) :
    read.reviewState ? scoped.filter(c => c.review === read.reviewState) : scoped;
  const enriched = cohort.map(c => {
    const details = recordedDetails(c.row.notes);
    const excluded = ['hold', 'reject', 'rejected'].includes(c.review) ||
      ['dead', 'disqualified', 'closed', 'rejected', 'hold', 'held'].includes(c.row.status) ||
      ['dead', 'closed', 'rejected', 'hold', 'held'].includes(c.row.setter_status) || c.row.do_not_contact !== false;
    const traceable = Boolean((c.row.first_name || c.row.last_name) && c.row.source && c.row.notes);
    return { ...c, details, excluded, traceable, fingerprint: fingerprint(c.row) };
  });
  const missingIds = selectedIds?.filter(id => !enriched.some(c => c.id === id)) || [];
  const stale = Boolean(prior && (prior.policy !== POLICY || prior.criteriaFingerprint !== criteriaFingerprint ||
    missingIds.length || enriched.some(c => prior.fingerprints?.[c.id] !== c.fingerprint)));
  const incomplete = result.complete !== true || Boolean(result.after) || duplicates > 0;
  const comparable = enriched.filter(c => !c.excluded);
  const differentiated = new Set(comparable.map(c => c.details.count)).size > 1;
  const noPrior = read.reason === 'ranking_follow_up_after_operational_read' && !prior;
  const ranked = !noPrior && !incomplete && !stale && comparable.length > 1 && comparable.every(c => c.traceable && !c.details.conflicts.length) && differentiated;
  enriched.sort((a, b) => ranked ? Number(a.excluded) - Number(b.excluded) || b.details.count - a.details.count || a.id.localeCompare(b.id) : a.id.localeCompare(b.id));
  const position = c => ranked && !c.excluded ? 1 + comparable.filter(other => other.details.count > c.details.count).length : null;
  // No implicit top-five. Preserve all candidates unless an explicit cutoff
  // can be defended; a tie at the boundary is included and disclosed.
  const cutoff = ranked && read.topCount && !prior ? [...comparable].sort((a,b) => b.details.count - a.details.count)[Math.min(read.topCount, comparable.length) - 1]?.details.count : null;
  const displayed = cutoff == null ? enriched : enriched.filter(c => !c.excluded && c.details.count >= cutoff);
  const lines = [ranked ? 'Provisional investigation priority — analytical attention only, not outreach priority or predicted funding likelihood.' :
    'Unranked investigation candidates — the available evidence does not support a defensible comparative ordering.'];
  if (noPrior) lines.push('No investigation ranking was produced in the preceding turn. Current records are shown unranked; there is no first-ranked record to explain.');
  if (prior) lines.push(stale ? 'The previous ranking is stale or unavailable: selected records or authorized criteria changed. Current facts are shown without reusing old positions; request a new ranking after reviewing the changes.' :
    'Explaining the previous selected cohort against a fresh record read. These records and criteria are unchanged; this is not a new ranking of the entire workspace.');
  if (missingIds.length) lines.push(`Previously selected IDs not returned by the current authorized read: ${missingIds.join(', ')}. They are not verified by this retrieval.`);
  lines.push(`Retrieved ${scoped.length} unique authorized records; selected cohort ${enriched.length}; showing ${displayed.length}. Retrieval ${result.complete === true ? 'complete' : 'incomplete'}${result.after ? ' for a continuation subset' : ''}.`);
  if (incomplete) lines.push(`A global ranking is withheld because retrieval is incomplete, is a continuation subset, or contains duplicate IDs (${duplicates}).${result.nextCursor ? ` Continue with: List my prospects after record ID ${result.nextCursor}.` : ''}`);
  if (ranked) lines.push('Policy: compare coverage of five explicitly labelled recorded claims — financing request, deal stage, security position, exit plan and closing date. Greater coverage makes a concrete case easier to investigate first; it does not make the claims true. Ties share position; ID orders display only.');
  else lines.push('Ordering needs traceable identity/source/notes, non-conflicting labelled deal claims, meaningful differences and complete retrieval. Profile descriptions, qualification scores and review states alone do not supply that evidence.');
  if (cutoff != null && displayed.length > read.topCount) lines.push(`Requested top ${read.topCount}; showing ${displayed.length} because the cutoff is tied. No evidence distinguishes the tied candidates.`);
  lines.push('All deal details are recorded claims requiring independent verification. Borrower intent, current readiness, security and likelihood of funding are unverified. Missing contact channels do not block read-only investigation.');
  lines.push(criteria.description ? `Recorded tenant criteria [${criteria.ref}]: ${criteria.description}\n${criteria.limitation}` : criteria.limitation);
  const items = displayed.map(c => {
    const r = c.row;
    const reference = `prospect:${bundle.clientId}:${c.id}`;
    const outreachBlocked = !c.eligible || r.acquisition_metadata?.outreachAuthorized === false;
    const facts = `${[r.first_name, r.last_name].filter(Boolean).join(' ') || 'Name not recorded'} [${reference}]; type: ${r.acquisition_metadata?.category || 'not recorded'}; review: ${c.review || 'not recorded'}; pipeline: ${r.status || 'not recorded'}; setter: ${r.setter_status || 'not recorded'}; source: ${r.source || 'not recorded'}.`;
    const fit = section(r.notes, 'Fit evidence');
    const risks = section(r.notes, 'Risks');
    const sourceUrls = Array.isArray(r.acquisition_metadata?.sourceUrls) ? r.acquisition_metadata.sourceUrls : [];
    const sources = `Recorded source URLs (not independently checked): ${sourceUrls.join(', ') || 'none recorded'}${r.acquisition_metadata?.profileUrl ? `; recorded profile: ${r.acquisition_metadata.profileUrl}` : ''}.`;
    const evidence = Object.entries(c.details.values).map(([k,v]) => `${k}: ${v}`).join('; ') || (fit || 'No explicit labelled deal details recorded.');
    const fieldNames = { amount: 'financing request', stage: 'deal stage', security: 'lien/security position', exit: 'exit plan', closing: 'closing date' };
    const gaps = Object.keys(LABELS).filter(k => !c.details.values[k]).map(k => fieldNames[k]);
    const finance = financeConcern(c.details, criteria);
    const action = c.excluded ? 'Keep the recorded exclusion; review existing records only. No outreach or review-state change is proposed.' :
      `Read-only next step: inspect existing supporting documents and reconcile ${gaps.length ? gaps.join(', ') : 'all five claims against their source documents'}. Check title/lien documents and the exit evidence.`;
    const claim = `${facts}\n${sources}\n${position(c) ? `Investigation position ${position(c)}: ${c.details.count}/5 labelled deal claims available to check.` : 'Unranked.'} ${c.excluded ? 'Held, rejected, terminal or DNC-blocked records are excluded from prioritization.' : ''}\nRecorded claims (unverified): ${evidence}${risks ? `\nRecorded risks (unverified): ${risks}` : ''}\nFinancing fit: ${finance}\nMissing verification: ${gaps.join(', ') || 'independent verification of every deal claim'}. ${!r.email && !r.phone && !r.linkedin_url ? 'Contact channel not recorded.' : ''}\nOutreach eligibility: ${!outreachBlocked ? 'recorded gates pass; separate execution authorization is still required' : 'blocked; existing gates remain in force'}.\n${action}`;
    lines.push(claim);
    return { recordRef: reference, recordId: c.id, recordFingerprint: c.fingerprint,
      sourceKind: 'prospects', provenance: 'prospects table', epistemic: 'verified', claim,
      priority: position(c) ? { mode: 'investigation', position: position(c), evidenceCoverage: c.details.count, epistemic: 'calculated', policy: POLICY } : null,
      // Raw notes are retained in structured evidence, not repeated in prose.
      recordedClaims: { epistemic: 'recorded_claim', notes: r.notes || null, details: c.details.values,
        sourceUrls,
        financingConcern: finance },
      limitations: [...gaps, 'external verification absent', ...c.details.conflicts], retrievalStatus: 'found' };
  });
  if (!displayed.length) lines.push('No matching authorized prospect records were returned.');
  lines.push('All actions above are proposals for read-only review. No writes, emails, calls, assignments, imports, review-state changes or specialist execution occurred.');
  return { prose: lines.join('\n\n'), items, recordIntelligence: true, operationalReadFocused: true,
    sourceStatuses: { prospects: result.status }, used: ['prospects'], recommend: false, launchedScout: false,
    investigationPriority: { policy: POLICY, ranked, tenantId: String(bundle.tenantId), criteriaFingerprint,
      criteriaRef: criteria.ref, criteriaEvidence: criteria.ref ? { ref: criteria.ref, factRefs: criteria.factRefs, description: criteria.description } : null, reviewState: read.reviewState || null } };
}
module.exports = { enabled, shouldCompose, composeInvestigation, recordedDetails, tenantCriteria, fingerprint, POLICY };
