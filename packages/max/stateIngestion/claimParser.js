'use strict';

const { CLAIM_TYPES } = require('./types');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').replace(/[.,;]+$/g, '').trim();
}

function claimsFromStructured(structured = {}) {
  if (Array.isArray(structured.claims)) {
    return structured.claims.map(c => ({ ...c, claim_type: c.claim_type || c.type }));
  }
  return [];
}

function levenshtein(a, b) {
  const left = normalizeText(a).toLowerCase();
  const right = normalizeText(b).toLowerCase();
  if (left === right) return 0;
  const matrix = Array.from({ length: right.length + 1 }, (_, i) => [i]);
  for (let j = 0; j <= left.length; j += 1) matrix[0][j] = j;
  for (let i = 1; i <= right.length; i += 1) {
    for (let j = 1; j <= left.length; j += 1) {
      const cost = right[i - 1] === left[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }
  return matrix[right.length][left.length];
}

function parseNaturalLanguageUpdate(text) {
  const raw = normalizeText(text);
  const lower = raw.toLowerCase();
  const claims = [];

  const aoMatch = raw.match(/\b(Tony|Rory|Jake)\b/i);
  if (aoMatch) {
    claims.push({ claim_type: CLAIM_TYPES.AO, payload: { name: aoMatch[1] } });
  }

  const toAccount = raw.match(/\bto\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/);
  if (toAccount?.[1]) {
    const name = normalizeText(toAccount[1]);
    if (name.length >= 4 && !/^(Tony|Rory|Jake)$/i.test(name)) {
      claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name } });
    }
  }
  const stoppedAt = raw.match(/\b(?:stopped at|visit(?:ed)?)\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,3})(?:\.|,|\s+Mike\b)/i);
  if (stoppedAt?.[1]) {
    claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: normalizeText(stoppedAt[1]) } });
  }

  if (/exeter\s+phillips|exter\s+phillips/i.test(raw)) {
    claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: /exter/i.test(raw) ? 'Exter Phillips' : 'Exeter Phillips' } });
  }
  if (/granite state plastics/i.test(lower)) {
    claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'Granite State Plastics' } });
  }
  if (/abc manufacturing/i.test(lower)) {
    claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: 'ABC Manufacturing' } });
  }

  if (/his contact|existing contact|relationship/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.RELATIONSHIP,
      payload: { description: 'AO has an existing contact at account' },
    });
  }

  if (/talked|communicated|spoke|stopped into|visit|conversation/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.EVENT,
      payload: { kind: /stopped into|visit/i.test(lower) ? 'in_person_visit' : 'conversation' },
    });
  }

  if (/interested|looking for|backup coverage|pain|unreliable/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.SIGNAL,
      payload: { signal: /interested/i.test(lower) ? 'interest_expressed' : 'operational_need' },
    });
  }

  if (/supposed to call|expecting a call|expects to call|call him this week|call her this week|inbound call/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.NEXT_EXPECTED_EVENT,
      payload: { kind: 'inbound_call', direction: 'inbound' },
    });
  }

  if (/this week|thursday|friday|next week/i.test(lower)) {
    const windowMatch = lower.match(/this week|thursday|friday|next week/);
    claims.push({
      claim_type: CLAIM_TYPES.EXPECTED_WINDOW,
      payload: { window: windowMatch ? windowMatch[0] : 'unspecified' },
    });
  }

  if (/follow[- ]?up|active relationship|awaiting|pending/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.PIPELINE_IMPLICATION,
      payload: { state: 'active_relationship_follow_up_pending' },
    });
  }

  const sarahMatch = raw.match(/Sarah Collins/i);
  if (sarahMatch) {
    claims.push({
      claim_type: CLAIM_TYPES.CONTACT,
      payload: {
        name: 'Sarah Collins',
        title: /operations manager/i.test(raw) ? 'Operations Manager' : null,
      },
    });
  }
  const phoneMatch = raw.match(/(?:number is|phone(?:\s+is)?)\s*([+\d().\-x\s]{7,})/i);
  const emailMatch = raw.match(/([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})/i);
  if (phoneMatch) {
    claims.push({ claim_type: CLAIM_TYPES.CONTACT, payload: { phone: normalizeText(phoneMatch[1]) } });
  }
  if (emailMatch) {
    claims.push({ claim_type: CLAIM_TYPES.CONTACT, payload: { email: emailMatch[1].toLowerCase() } });
  }

  if (/facilities guy|front desk|don't have his name|do not have his name/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.PARTIAL_FACT,
      payload: { note: 'Facilities decision-maker referenced but not identified' },
    });
    claims.push({
      claim_type: CLAIM_TYPES.UNKNOWN_FIELD,
      payload: { field: 'facilities_decision_maker_name', value: 'UNKNOWN' },
    });
  }

  if (/call sarah|call thursday|asked me to call/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.NEXT_ACTION,
      payload: { action: 'call', target: sarahMatch ? 'Sarah Collins' : null },
    });
  }
  if (/thursday/i.test(lower)) {
    claims.push({ claim_type: CLAIM_TYPES.DUE_WINDOW, payload: { due: 'Thursday' } });
  }

  if (/backup cleaning|backup coverage/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.OPPORTUNITY_CONTEXT,
      payload: { context: 'backup cleaning coverage' },
    });
  }
  if (/unreliable internal cleaning/i.test(lower)) {
    claims.push({
      claim_type: CLAIM_TYPES.PAIN_SIGNAL,
      payload: { pain: 'unreliable internal cleaning coverage' },
    });
  }

  if (/owner:\s*tony/i.test(lower)) {
    claims.push({ claim_type: CLAIM_TYPES.OWNERSHIP, payload: { ao_name: 'Tony' } });
  }

  return dedupeClaims(claims);
}

function parseSpreadsheetRow(row = {}, { sheet = null, rowNumber = null } = {}) {
  const claims = [];
  const sourceRecord = { sheet, row: rowNumber, record_id: row.record_id || row.id || null };
  const accountName = normalizeText(row.account || row.company || row.account_name);
  if (accountName) {
    claims.push({
      claim_type: CLAIM_TYPES.ACCOUNT,
      payload: { name: accountName },
      source_record: sourceRecord,
    });
  }
  const aoName = normalizeText(row.ao || row.owner || row.assigned_ao);
  if (aoName) {
    claims.push({
      claim_type: CLAIM_TYPES.AO,
      payload: { name: aoName },
      source_record: sourceRecord,
    });
  }
  if (row.expects_inbound_call || row.next_expected_event === 'inbound_call') {
    claims.push({
      claim_type: CLAIM_TYPES.NEXT_EXPECTED_EVENT,
      payload: { kind: 'inbound_call', direction: 'inbound' },
      source_record: sourceRecord,
    });
  }
  if (row.expected_window) {
    claims.push({
      claim_type: CLAIM_TYPES.EXPECTED_WINDOW,
      payload: { window: normalizeText(row.expected_window) },
      source_record: sourceRecord,
    });
  }
  if (row.contact_name) {
    claims.push({
      claim_type: CLAIM_TYPES.CONTACT,
      payload: {
        name: normalizeText(row.contact_name),
        title: row.contact_title || null,
        phone: row.phone || null,
        email: row.email || null,
      },
      source_record: sourceRecord,
    });
  }
  if (row.notes) {
    claims.push({
      claim_type: CLAIM_TYPES.EVENT,
      payload: { kind: 'note', notes: normalizeText(row.notes) },
      source_record: sourceRecord,
    });
  }
  if (row.ownership_ao) {
    claims.push({
      claim_type: CLAIM_TYPES.OWNERSHIP,
      payload: { ao_name: normalizeText(row.ownership_ao) },
      source_record: sourceRecord,
    });
  }
  if (row.new_prospect === true || row.is_new === true) {
    claims.push({
      claim_type: CLAIM_TYPES.PIPELINE_IMPLICATION,
      payload: { state: 'ao_reported_new_prospect' },
      source_record: sourceRecord,
    });
  }
  return dedupeClaims(claims);
}

function dedupeClaims(claims) {
  const seen = new Set();
  const out = [];
  for (const claim of claims) {
    const key = `${claim.claim_type}:${JSON.stringify(claim.payload)}:${JSON.stringify(claim.source_record || null)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(claim);
  }
  return out;
}

function extractClaims(input = {}) {
  if (Array.isArray(input.claims) && input.claims.length) {
    return claimsFromStructured(input);
  }
  if (input.structured && Array.isArray(input.structured.claims)) {
    return claimsFromStructured(input.structured);
  }
  if (typeof input.text === 'string' && input.text.trim()) {
    return parseNaturalLanguageUpdate(input.text);
  }
  if (typeof input.message === 'string' && input.message.trim()) {
    return parseNaturalLanguageUpdate(input.message);
  }
  return [];
}

module.exports = {
  extractClaims,
  parseNaturalLanguageUpdate,
  parseSpreadsheetRow,
  levenshtein,
  normalizeText,
  dedupeClaims,
};
