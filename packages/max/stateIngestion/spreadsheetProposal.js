'use strict';

// This module is deliberately pure: source text is evidence, never an instruction
// to execute a tool or authorize a write. Only server-owned approved operations
// may later be persisted by SpreadsheetProposalStore.
const crypto = require('crypto');
const { semanticKey: persistedSemanticKey, effectStillPresent } = require('./spreadsheetProposalStore');
const KNOWN_FIELDS = new Set(['company', 'account', 'contact', 'notes', 'phone', 'email', 'address', 'website', 'first_call_date', 'follow_up_call_date', 'first_visit_date', 'provider', 'follow_up_needed', 'status', 'next_step', 'ao']);
const text = value => value == null ? '' : String(value).trim();
const norm = value => text(value).toLowerCase().replace(/\s+/g, ' ');
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}
const digest = value => crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const normalizedPhone = value => { const digits = text(value).replace(/\D/g, ''); return digits.length === 11 && digits.startsWith('1') ? digits.slice(1) : digits; };
function phones(value) {
  return [...text(value).matchAll(/(?:\+?1[ .-]?)?\(?\d{3}\)?[ .\-‑–]*\d{3}[ .\-‑–]*\d{4}/g)].map(match => ({ value: match[0], normalized: normalizedPhone(match[0]) }));
}
function isoDate(value, year) {
  if (!value) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
  if (typeof value === 'number') return null; // Adapter must interpret the workbook's own date system.
  let match = text(value).match(/^(\d{4})-(\d{2})-(\d{2})(?:T.*)?$/);
  let y, m, d;
  if (match) [, y, m, d] = match;
  else {
    match = text(value).match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2}|\d{4}))?$/);
    if (!match || (!match[3] && !year)) return null;
    m = match[1]; d = match[2]; y = match[3] ? (match[3].length === 2 ? `20${match[3]}` : match[3]) : year;
  }
  const date = new Date(Date.UTC(Number(y), Number(m) - 1, Number(d)));
  return date.getUTCFullYear() === Number(y) && date.getUTCMonth() === Number(m) - 1 && date.getUTCDate() === Number(d) ? date.toISOString().slice(0, 10) : null;
}
function domain(value) {
  try { return new URL(text(value).includes('@') ? `https://${text(value).split('@').pop()}` : (/^https?:\/\//i.test(text(value)) ? text(value) : `https://${text(value)}`)).hostname.toLowerCase().replace(/^www\./, ''); } catch (_) { return ''; }
}
function sourceEvidence(row, sheet, field, fileHash) {
  const columns = row.columnProvenance?.columns || {};
  return Object.entries(row.values || {}).filter(([key, value]) => (!field || key === field) && text(value)).map(([key, value]) => {
    const found = Object.entries(columns).find(([header, metadata]) => metadata.canonical === key || header === key);
    const metadata = found?.[1] || {};
    return { fileHash, sheet, row: row.rowNumber, cell: metadata.cellRef || metadata.cell || metadata.cellAddress || metadata.address || null, field: key, rawHeader: metadata.rawHeader || found?.[0] || key, rawValue: metadata.rawValue !== undefined ? metadata.rawValue : value, hyperlink: metadata.hyperlink || null };
  });
}
function identityConflict(values) {
  // An academic acronym must agree with an explicit institutional domain. Other
  // domains are not treated as proof of identity (gmail, providers, parent orgs).
  const name = text(values.company || values.account);
  const acronym = name.split(/\s+/).filter(Boolean).map(word => word[0]).join('').toLowerCase();
  const educational = [values.email, values.website].map(domain).filter(host => /\.edu$/.test(host));
  if (/\b(university|college|academy)\b/i.test(name) && educational.some(host => {
    const label = host.split('.')[0];
    return label.length <= 5 && label !== acronym && !norm(name).includes(label);
  })) return 'Organization name and institutional email/website provide conflicting identity evidence.';
  return null;
}
function matchAccount(values, context) {
  const name = norm(values.company || values.account);
  const scoped = context.prospects || [];
  const companyNames = new Map((context.companies || []).map(company => [String(company.id), company.name]));
  const named = scoped.filter(account => norm(account.company_name || account.account_name || companyNames.get(String(account.company_id)) || account.name) === name);
  const identifiers = account => [values.email && norm(values.email) === norm(account.email), phones(values.phone).some(phone => phone.normalized === normalizedPhone(account.phone)), values.website && domain(values.website) === domain(account.website), values.address && norm(values.address) === norm(account.address || account.location)].filter(Boolean).length;
  const supported = named.filter(account => identifiers(account) > 0);
  if (supported.length === 1 && scoped.some(account => account.id !== supported[0].id && identifiers(account) > 0)) return { status: 'ambiguous', account: null, candidates: scoped.filter(account => identifiers(account) > 0).map(account => account.id), reason: 'Source identifiers point to different existing accounts.' };
  if (supported.length === 1) return { status: 'matched', account: supported[0], candidates: named.map(account => account.id), reason: 'Exact organization name and corroborating identifier.' };
  // Exact name alone may be a collision. Require explicit resolution rather than guessing.
  const significant = value => new Set(norm(value).replace(/[^a-z0-9 ]/g, ' ').split(/\s+/).filter(word => word.length > 2 && !['the', 'inc', 'llc', 'company', 'corporation'].includes(word)));
  const incomingTokens = significant(name);
  const fuzzy = scoped.filter(account => {
    const tokens = significant(account.company_name || account.account_name || companyNames.get(String(account.company_id)) || account.name);
    const shared = [...incomingTokens].filter(token => tokens.has(token)).length;
    return shared > 0 && shared / Math.max(incomingTokens.size, tokens.size) >= 0.6;
  });
  const candidates = named.length ? named : scoped.filter(account => identifiers(account) > 0 || fuzzy.includes(account));
  return { status: candidates.length ? 'ambiguous' : 'new_candidate', account: null, candidates: candidates.map(account => account.id), reason: candidates.length ? 'Identity requires corroboration or selection; names and isolated identifiers are not sufficient.' : 'No verified existing account; review new-account identity before approval.' };
}
function splitContacts(value) {
  return text(value).split(/[\n/;]+/).map(text).filter(Boolean).filter(name => !/^(?:facilities|materials|office|building|general|property).*\b(?:manager|management|director)$/i.test(name));
}
function contactSummary(contact) {
  return { id: contact.id, name: contact.name || [contact.first_name, contact.last_name].filter(Boolean).join(' '), email: contact.email || null, phone: contact.phone || null, title: contact.title || contact.job_title || null, prospectId: contact.prospect_id || contact.account_id };
}
function contactCandidates(name, contacts) {
  const first = norm(name).split(' ')[0];
  return contacts.filter(contact => {
    const candidate = norm(contact.name || [contact.first_name, contact.last_name].filter(Boolean).join(' '));
    return candidate === norm(name) || (first && candidate.split(' ')[0] === first);
  });
}
function existingEffectIntact(effect, operation, context) {
  if (!effect.operation || effect.operation.type !== operation.type || String(effect.operation.target?.accountId) !== String(operation.target?.accountId)) return false;
  return effectStillPresent(effect, context);
}
function taskSemantics(task, proposed = false, expectedOwnerId = undefined) {
  const metadata = task.routing_snapshot || task.data || {};
  const rawDue = proposed ? task.dueDate : task.verified_deadline !== undefined ? task.verified_deadline : task.deadline !== undefined ? task.deadline : task.dueDate !== undefined ? task.dueDate : metadata.dueDate;
  const dueDate = rawDue === null ? null : rawDue === undefined ? undefined : isoDate(rawDue) || `invalid:${text(rawDue)}`;
  return {
    description: norm(proposed ? task.description : task.first_action ?? task.description ?? task.prompt),
    kind: task.kind ?? metadata.kind,
    dueDate,
    externalSendingAuthorized: task.externalSendingAuthorized ?? metadata.externalSendingAuthorized,
    ownerId: proposed ? (expectedOwnerId == null ? undefined : String(expectedOwnerId)) : task.assigned_ao_id == null ? undefined : String(task.assigned_ao_id),
    category: proposed ? 'FOLLOW_UP_REQUIRED' : task.assignment_category,
    motion: proposed ? 'AO_LED' : task.motion,
    lifecycle: proposed || ['open', 'in_progress', 'completed'].includes(task.status) ? 'active_or_completed' : 'unknown_or_cancelled',
  };
}
function taskDeadline(source, asOf) {
  const currentDay = isoDate(asOf) || new Date().toISOString().slice(0, 10);
  const dates = [];
  const action = /\b(?:follow[ -]?up|call|email|send|research|contact|visit|task)\b/i;
  for (const clause of text(source).split(/\n|[.!?](?:\s+|$)/)) {
    if (!action.test(clause)) continue;
    if (/\b(?:tomorrow|today|next\s+(?:week|month|monday|tuesday|wednesday|thursday|friday|saturday|sunday)|in\s+\w+\s+days?)\b/i.test(clause)) return { dueDate: null, error: 'TASK_DATE_AMBIGUOUS', message: 'Relative task timing needs an explicit confirmed calendar date.' };
    for (const match of clause.matchAll(/\b(?:by|on|due|deadline)\s*:?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)(?![\d/])/gi)) {
      const value = match[1];
      if (!/^\d{4}-\d{2}-\d{2}$/.test(value) && !/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(value)) return { dueDate: null, error: 'TASK_DATE_AMBIGUOUS', message: 'Task deadline needs an explicit four-digit year.' };
      const parsed = isoDate(value);
      if (!parsed) return { dueDate: null, error: 'INVALID_TASK_DATE', message: 'Task deadline is not a valid calendar date.' };
      if (parsed < currentDay) return { dueDate: null, error: 'PAST_TASK_DATE', message: 'The source task date has passed; review its present status instead of silently scheduling it.' };
      dates.push(parsed);
    }
  }
  if (new Set(dates).size > 1) return { dueDate: null, error: 'MULTIPLE_TASK_DATES', message: 'Multiple explicit deadlines need separate reviewed task definitions.' };
  return { dueDate: dates[0] || null };
}
function buildSpreadsheetProposal(input = {}) {
  const structuredData = input.structuredData || {};
  const context = input.snapshot || input.context;
  if (!context || typeof context.then === 'function') throw new Error('A complete, awaited CRM snapshot is required.');
  const tenantId = input.tenantId || input.scope?.tenantId || input.scope?.clientId || context.clientId;
  if (!tenantId || (context.clientId && String(context.clientId) !== String(tenantId))) throw new Error('Snapshot tenant mismatch.');
  for (const collection of ['prospects', 'companies', 'contacts', 'activities', 'tasks', 'suppressions', 'effects']) {
    if (!Array.isArray(context[collection])) throw new Error(`Incomplete CRM snapshot: ${collection}.`);
    if (context[collection].some(entity => entity.assigned_ao_id && input.scope?.aoId && String(entity.assigned_ao_id) !== String(input.scope.aoId))) throw new Error('Cross-AO snapshot rejected.');
    if (context[collection].some(entity => entity.client_id && String(entity.client_id) !== String(tenantId))) throw new Error('Cross-tenant snapshot rejected.');
  }
  const fileHash = input.fileHash || input.sourceHash || input.scope?.sourceHash || structuredData.sourceHash || structuredData.fileHash;
  if (!/^[a-f0-9]{64}$/i.test(fileHash || '')) throw new Error('Verified source SHA-256 is required.');
  if (!Array.isArray(input.resolutions || [])) throw new Error('Resolutions must be an array.');
  const seenDecisions = new Set();
  for (const decision of input.resolutions || []) {
    const key = `${decision.sheet}:${decision.rowNumber}`;
    if (decision.sourceHash !== fileHash || seenDecisions.has(key) || !(structuredData.sheets || []).some(sheet => (sheet.sheet || sheet.name) === decision.sheet && sheet.rows.some(row => row.rowNumber === decision.rowNumber))) throw new Error('Resolution source mismatch or duplicate.');
    seenDecisions.add(key);
  }
  const rows = [], operations = [];
  for (const sheet of structuredData.sheets || []) for (const row of sheet.rows || []) {
    const values = row.values || {};
    const decision = (input.resolutions || []).find(item => item.sourceHash === fileHash && item.sheet === (sheet.sheet || sheet.name) && item.rowNumber === row.rowNumber) || null;
    let resolution = matchAccount(values, context);
    if (decision && (decision.accountId || decision.createAccount || decision.acknowledgedIdentityConflict || decision.fields?.length || decision.contacts?.length || decision.providerId) && !text(decision.identityEvidence)) throw new Error('Reviewed resolution requires explicit identity evidence.');
    if (decision?.createAccount && decision.accountId) throw new Error('Account resolution cannot select and create simultaneously.');
    if (decision?.createAccount) {
      if (resolution.status !== 'new_candidate') throw new Error('Existing account candidates must be resolved before creating a duplicate.');
      const hex = digest({ tenantId, aoId: input.scope?.aoId || context.aoId || null, name: norm(values.company || values.account), address: norm(values.address), phones: phones(values.phone).map(item => item.normalized) }).slice(0, 32);
      const id = `${hex.slice(0,8)}-${hex.slice(8,12)}-5${hex.slice(13,16)}-a${hex.slice(17,20)}-${hex.slice(20)}`;
      resolution = { status: 'confirmed_new', account: { id }, candidates: [], reason: 'Explicit new-account identity confirmation.', reviewed: true };
    }
    if (decision?.accountId) {
      const selected = context.prospects.find(account => String(account.id) === String(decision.accountId));
      if (!selected) throw new Error('Resolution account is outside the scoped CRM snapshot.');
      resolution = { status: 'matched', account: selected, candidates: [selected.id], reason: 'Explicit reviewed account resolution.', reviewed: true };
    }
    const report = { sheet: sheet.sheet || sheet.name, rowNumber: row.rowNumber, company: text(values.company || values.account), raw: row.raw || {}, values, evidence: sourceEvidence(row, sheet.sheet || sheet.name, null, fileHash), accountResolution: resolution, comparisons: [], contacts: [], phoneNumbers: phones(values.phone), conflicts: [], operations: [] };
    report.candidateSummaries = resolution.candidates.map(id => context.prospects.find(account => String(account.id) === String(id))).filter(Boolean).map(account => ({ id: account.id, name: account.company_name || account.account_name || context.companies.find(company => String(company.id) === String(account.company_id))?.name || account.name || '', phone: account.phone || null, email: account.email || null, address: account.address || account.location || account.ao_source_address || null, website: account.website || null }));
    const conflict = (code, field, message, blocking = true) => report.conflicts.push({ code, field, message, blocking });
    if (row.hidden) conflict('HIDDEN_SOURCE', 'row', 'Hidden row or sheet requires explicit review.');
    for (const cell of Object.values(row.cells || {})) {
      if (cell.formula) conflict('FORMULA_SOURCE', cell.cellRef, 'Formula result requires explicit source review; cached values are not trusted for persistence.');
      if (cell.type === 'e') conflict('ERROR_SOURCE', cell.cellRef, 'Spreadsheet error cell requires correction.');
    }
    if (!report.company) conflict('MISSING_ORGANIZATION', 'company', 'No business name; this row cannot create an account.');
    if (!['matched', 'confirmed_new'].includes(resolution.status)) conflict('UNRESOLVED_ACCOUNT', 'company', resolution.reason);
    const inconsistent = identityConflict(values);
    if (inconsistent && !decision?.acknowledgedIdentityConflict) conflict('IDENTITY_CONFLICT', 'company', inconsistent);
    const account = resolution.account;
    const target = { accountId: account?.id || null };
    const effects = context.effects || context.appliedOperations || [];
    const add = (type, after, fields, extra = {}, currentMatch = null) => {
      const semanticKey = persistedSemanticKey({ type, target, after, field: extra.field || null });
      if (report.operations.some(operation => operation.semanticKey === semanticKey)) return;
      const prior = effects.find(item => (item.semanticKey || item.semantic_key) === semanticKey);
      if (prior && type !== 'SET_ACCOUNT_FIELD') {
        if (existingEffectIntact(prior, { type, target }, context)) { report.comparisons.push({ field: fields.join(','), incoming: after, result: 'same', reason: 'Persisted effect still present in current CRM snapshot.' }); return; }
        conflict('PERSISTED_EFFECT_CHANGED', fields.join(','), 'A previous imported effect is now missing or changed; review integrity before replay.');
      }
      if (prior && type === 'SET_ACCOUNT_FIELD') conflict('PERSISTED_EFFECT_CHANGED', extra.field, 'Previously applied field value no longer matches current CRM; explicit amendment and fresh approval required.');
      if (currentMatch) { report.comparisons.push({ field: fields.join(','), incoming: after, existing: currentMatch.id || null, result: 'same', reason: 'Business fact matches current scoped CRM record.' }); return; }
      const sameRecord = type === 'ADD_NOTE' ? context.activities.find(item => String(item.prospect_id || item.account_id) === String(account?.id) && norm(item.notes || item.text || item.details || item.content_summary) === norm(after.text)) : null;
      if (sameRecord) { report.comparisons.push({ field: fields.join(','), incoming: after, existing: sameRecord.id || null, result: 'same', reason: 'Exact fact exists in current CRM snapshot.' }); return; }
      if (type === 'ADD_TASK') {
        const candidates = context.tasks.filter(item => String(item.prospect_id || item.account_id) === String(account?.id) && taskSemantics(item).description === norm(after.description));
        if (candidates.length === 1 && digest(taskSemantics(candidates[0])) === digest(taskSemantics(after, true, input.scope?.aoId || account?.assigned_ao_id))) {
          report.comparisons.push({ field: fields.join(','), incoming: after, existing: candidates[0].id || null, result: 'same', reason: 'Task description, kind, deadline and sending authorization match current CRM.' }); return;
        }
        if (candidates.length) {
          report.comparisons.push({ field: fields.join(','), incoming: taskSemantics(after, true, input.scope?.aoId || account?.assigned_ao_id), existing: candidates.map(item => taskSemantics(item)), result: 'conflict' });
          conflict('TASK_SEMANTICS_CONFLICT', 'follow_up_needed', 'An existing task has the same description but different or unknown business semantics, or multiple matching tasks exist; review before adding another task.');
        }
      }
      const operation = { id: `op_${digest({ sheet: report.sheet, row: report.rowNumber, semanticKey }).slice(0, 24)}`, type, target, before: null, after, semanticKey, evidence: fields.flatMap(field => sourceEvidence(row, report.sheet, field, fileHash)), dependsOn: [], blocked: false, ...extra };
      report.operations.push(operation); operations.push(operation);
    };
    if (resolution.status === 'confirmed_new') add('CREATE_ACCOUNT', { name: report.company, identityConfirmed: true, identityEvidence: decision.identityEvidence, outreachReviewRequired: true }, ['company']);
    if (resolution.status === 'new_candidate' && report.company) add('CREATE_ACCOUNT', { name: report.company }, ['company'], { blocked: true });
    for (const field of ['email', 'phone', 'address', 'website']) {
      if (!text(values[field])) continue;
      const fieldDecision = decision?.fields?.find(item => item.field === field);
      if (fieldDecision && !['keep_existing', 'use_source'].includes(fieldDecision.decision)) throw new Error('Unsupported field resolution.');
      const existing = account?.[field] || (field === 'address' ? account?.location : null) || null;
      const multiple = field === 'phone' && report.phoneNumbers.length > 1;
      let incoming = values[field];
      if (fieldDecision?.value !== undefined) {
        if (field !== 'phone' || !report.phoneNumbers.some(phone => phone.normalized === normalizedPhone(fieldDecision.value))) throw new Error('Resolved value is not present in source.');
        incoming = fieldDecision.value;
      }
      const equal = field === 'phone' ? normalizedPhone(existing) === normalizedPhone(incoming) : norm(existing) === norm(incoming);
      report.comparisons.push({ field, incoming: values[field], existing, result: equal ? 'same' : existing ? 'conflict' : 'new' });
      if (fieldDecision?.decision === 'keep_existing') { report.comparisons.at(-1).result = 'reviewed_keep_existing'; continue; }
      if (multiple && !fieldDecision?.value) { conflict('MULTIPLE_PHONES', field, 'Preserve all labeled phone numbers; select destinations before changing a scalar phone field.'); continue; }
      if (existing && !equal && fieldDecision?.decision !== 'use_source') { conflict('FIELD_CONFLICT', field, 'Incoming nonblank value differs; existing CRM value will not be overwritten automatically.'); continue; }
      if (!equal) add('SET_ACCOUNT_FIELD', incoming, [field], { field, before: existing, amendmentReviewed: fieldDecision?.decision === 'use_source', ...(['email', 'phone'].includes(field) ? { outreachReviewRequired: true, admissionChange: { field: 'ao_outreach_review_required', before: account?.ao_outreach_review_required || false, after: true } } : {}) });
    }
    for (const name of splitContacts(values.contact)) {
      const eligible = context.contacts.filter(contact => String(contact.prospect_id || contact.account_id) === String(account?.id));
      const candidates = contactCandidates(name, eligible);
      const summaries = candidates.map(contactSummary);
      const selectedContact = decision?.contacts?.find(item => item.sourceName === name);
      if (selectedContact?.contactId && !eligible.some(contact => String(contact.id) === String(selectedContact.contactId))) throw new Error('Contact resolution is outside selected account.');
      const exact = eligible.filter(contact => norm(contact.name || `${contact.first_name || ''} ${contact.last_name || ''}`) === norm(name));
      if (selectedContact?.contactId) {
        report.contacts.push({ name, status: 'reviewed_match', candidates: [selectedContact.contactId], candidateSummaries: eligible.filter(contact => String(contact.id) === String(selectedContact.contactId)).map(contactSummary) }); continue;
      }
      if (selectedContact?.create === true) {
        report.contacts.push({ name, status: 'reviewed_new_contact', candidates: [], candidateSummaries: [] });
        add('ADD_CONTACT', { name }, ['contact']); continue;
      }
      const partial = !name.includes(' ') || exact.length > 1;
      report.contacts.push({ name, status: exact.length === 1 && !partial ? 'matched' : 'unresolved', candidates: candidates.map(contact => contact.id), candidateSummaries: summaries });
      if (partial) conflict('CONTACT_IDENTITY_UNRESOLVED', 'contact', `Contact “${name}” needs sufficient identity evidence.`, false);
      else if (exact.length === 1) add('ADD_CONTACT', { name }, ['contact'], {}, exact[0]);
      else if (!exact.length) {
        conflict('CONTACT_IDENTITY_UNRESOLVED', 'contact', `Confirm new contact “${name}” or select an existing contact.`, false);
        add('ADD_CONTACT', { name }, ['contact'], { blocked: true });
      }
    }
    for (const evidence of report.evidence) {
      if (evidence.field === 'phone' && evidence.hyperlink?.target?.startsWith('mailto:')) conflict('STALE_HYPERLINK', 'phone', 'Phone cell has an email hyperlink; preserve typed phone value and review stale link.', false);
    }
    const notes = text(values.notes);
    report.contactAssertions = [];
    if (notes) {
      const mentions = [...notes.matchAll(/\b(?:with|contact)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)/g)].map(match => match[1]);
      mentions.push(...[...notes.matchAll(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:is now|now makes|is the (?:decision|person))/g)].map(match => match[1]));
      for (const name of new Set(mentions)) report.contactAssertions.push({ name, status: 'unresolved_source_assertion', evidence: sourceEvidence(row, report.sheet, 'notes', fileHash) });
      for (const match of notes.matchAll(/\b([A-Z][a-z]+\s+[A-Z][a-z]+)\s*\(([A-Z][a-z]+)\)/g)) report.contactAssertions.push({ name: match[1], possibleAlias: match[2], status: 'source_alias_assertion', evidence: sourceEvidence(row, report.sheet, 'notes', fileHash) });
      for (const assertion of report.contactAssertions) {
        const candidates = contactCandidates(assertion.name, context.contacts.filter(contact => String(contact.prospect_id || contact.account_id) === String(account?.id)));
        assertion.candidates = candidates.map(contact => contact.id);
        assertion.candidateSummaries = candidates.map(contactSummary);
        const choice = decision?.contacts?.find(item => item.sourceName === assertion.name);
        if (!choice) continue;
        if (choice.contactId) {
          if (!context.contacts.some(contact => String(contact.id) === String(choice.contactId) && String(contact.prospect_id || contact.account_id) === String(account?.id))) throw new Error('Contact resolution is outside selected account.');
          assertion.status = 'reviewed_match'; assertion.contactId = choice.contactId;
        } else if (choice.create === true) {
          add('ADD_CONTACT', { name: assertion.name }, ['notes']); assertion.status = 'reviewed_new_contact';
        }
      }
      const storedNotes = [...(context.activities || []).filter(item => String(item.prospect_id || item.account_id) === String(account?.id)).map(item => item.details || item.notes || item.content_summary || item.text || item.description), ...(account?.notes ? [account.notes] : [])];
      const equal = storedNotes.some(item => norm(typeof item === 'string' ? item : item?.text) === norm(notes));
      report.comparisons.push({ field: 'notes', incoming: notes, result: equal ? 'same' : 'new' });
      add('ADD_NOTE', { text: notes, category: 'source_note' }, ['notes']);
      if (/(?:is now|now the|in charge|decision maker|decisions)/i.test(notes)) conflict('DECISION_MAKER_REVIEW', 'notes', 'Decision-maker assertion requires identity review; preserve the dated source without replacing existing contacts.', false);
      if (/\b(?:my former|I have|my number|to me)\b/i.test(notes)) conflict('SOURCE_AUTHOR_CONTEXT', 'notes', 'First-person relationship belongs to the source author, not automatically the signed-in user.', false);
      if (/^\d{1,2}\/\d{1,2}:?\s*$/.test(notes)) conflict('INCOMPLETE_NOTE', 'notes', 'Date-only note supplies no outcome or action details.', false);
    }
    for (const choice of decision?.contacts || []) if (![...report.contacts, ...report.contactAssertions].some(contact => contact.name === choice.sourceName)) throw new Error('Contact resolution name is absent from source.');
    if (decision?.providerId && !text(values.provider)) throw new Error('Provider resolution requires source provider evidence.');
    if (text(values.provider)) {
      add('ADD_NOTE', { text: text(values.provider), category: 'provider_assertion' }, ['provider']);
      const candidates = context.prospects.filter(provider => norm(provider.company_name || provider.account_name || provider.name) === norm(values.provider));
      report.provider = { raw: text(values.provider), status: 'unresolved', candidateSummaries: candidates.map(provider => ({ id: provider.id, name: provider.company_name || provider.account_name || provider.name, email: provider.email || null, phone: provider.phone || null, address: provider.address || provider.location || null })) };
      if (decision?.providerId) {
        const provider = context.prospects.find(item => String(item.id) === String(decision.providerId));
        if (!provider || String(provider.id) === String(account?.id)) throw new Error('Provider resolution must select a separate account within scoped CRM.');
        report.provider.status = 'reviewed_match'; report.provider.providerId = provider.id;
        add('ADD_PROVIDER_RELATIONSHIP', { providerId: provider.id, verified: true, sourceAssertion: text(values.provider) }, ['provider']);
      } else conflict('PROVIDER_RELATIONSHIP_REVIEW', 'provider', 'Preserve provider/management assertion without creating or merging a related organization.', false);
    }
    const dateValues = ['first_call_date', 'follow_up_call_date', 'first_visit_date'].map(field => isoDate(values[field])).filter(Boolean);
    const years = [...new Set(dateValues.map(date => date.slice(0, 4)))];
    const year = years.length === 1 ? years[0] : null;
    const groups = new Map();
    const groupFor = (kind, occurredOn) => {
      const key = `${kind}:${occurredOn}`;
      if (!groups.has(key)) groups.set(key, { anchors: [], notes: [] });
      return groups.get(key);
    };
    for (const [field, kind] of [['first_call_date', 'phone_call'], ['follow_up_call_date', 'phone_call'], ['first_visit_date', 'in_person_visit']]) {
      if (!text(values[field])) continue;
      const occurredOn = isoDate(values[field]);
      if (!occurredOn) { conflict('INVALID_HISTORICAL_DATE', field, 'Date requires unambiguous workbook date interpretation.'); continue; }
      const label = { first_call_date: 'First phone call', follow_up_call_date: 'Follow-up phone call', first_visit_date: 'First in-person visit' }[field];
      groupFor(kind, occurredOn).anchors.push({ kind, occurredOn, details: `${label} recorded by source date field; outcome unspecified.`, fields: [field] });
    }
    for (const match of notes.matchAll(/(?:^|\n)\s*(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?):\s*([^\n]*)/g)) {
      if (!text(match[2])) continue;
      const occurredOn = isoDate(match[1], year);
      if (!occurredOn) { conflict('NOTE_DATE_AMBIGUOUS', 'notes', 'Dated note lacks an unambiguous year; no historical event inferred.', false); continue; }
      const kind = /\b(call|called|spoke|vm|voicemail|message)\b/i.test(match[2]) ? 'phone_call' : 'source_observation';
      groupFor(kind, occurredOn).notes.push({ kind, occurredOn, details: text(match[2]), fields: ['notes'] });
    }
    const events = [];
    report.historicalEvidence = [];
    for (const { anchors, notes: datedNotes } of groups.values()) {
      report.historicalEvidence.push(...anchors, ...datedNotes);
      if (!datedNotes.length) { events.push(...anchors); continue; }
      if (!anchors.length) { events.push(...datedNotes); }
      else if (anchors.length === 1 && datedNotes.length === 1) {
        // One date field and one note describe the same identifiable event.
        events.push({ ...datedNotes[0], fields: [...anchors[0].fields, 'notes'] });
      } else {
        // Dates alone cannot associate multiple same-day events. Preserve every
        // narrative and require review instead of merging by (kind, date).
        events.push(...datedNotes);
        if (anchors.length > datedNotes.length) events.push(...anchors);
        conflict('EVENT_ASSOCIATION_REVIEW', 'notes', 'Multiple same-day events or date fields require explicit event correspondence. Distinct narratives remain separate; no activity is saved until reviewed.');
      }
      if (new Set(datedNotes.map(event => norm(event.details))).size !== datedNotes.length) conflict('DUPLICATE_EVENT_TEXT', 'notes', 'Repeated identical dated narratives could be duplicate evidence or separate events; raw occurrences are preserved for review.');
    }
    for (const event of events) {
      const sameDay = [];
      const exists = context.activities.some(item => {
        const rawKind = item.kind || item.activity_type || item.channel;
        const kind = ({ call: 'phone_call', visit: 'in_person_visit', note: 'source_observation' })[rawKind] || rawKind;
        const sameIdentity = String(item.prospect_id || item.account_id) === String(account?.id) && kind === event.kind && String(item.occurredOn || item.occurred_on || item.occurred_at || '').slice(0, 10) === event.occurredOn;
        if (sameIdentity) sameDay.push(item);
        return sameIdentity && norm(item.details || item.notes) === norm(event.details);
      });
      if (!exists && sameDay.length) conflict('HISTORICAL_EVENT_REVIEW', 'notes', 'Existing activity on this date has different details; review whether this is an amendment or another event before adding history.');
      add('ADD_ACTIVITY', { kind: event.kind, occurredOn: event.occurredOn, details: event.details }, event.fields, {}, exists ? { id: sameDay.find(item => norm(item.details || item.notes) === norm(event.details))?.id } : null);
    }
    const followup = text(values.follow_up_needed);
    if (followup) add('ADD_NOTE', { text: followup, category: 'follow_up_assertion' }, ['follow_up_needed']);
    if (/\b(application\s+(?:is\s+)?in\s+progress)\b/i.test(`${followup} ${values.status || ''}`)) {
      if (account?.ao_current_status !== 'application_in_progress') add('SET_ACCOUNT_FIELD', 'application_in_progress', ['follow_up_needed', 'status'], { field: 'ao_current_status', before: account?.ao_current_status || null });
    }
    if (/remove (?:from|.*?from) (?:the )?call list|do not call|don.?t call/i.test(`${followup} ${notes}`)) {
      const currentSuppression = account?.ao_call_suppressed && (context.callSuppressions || context.suppressions || []).find(item => String(item.prospect_id || item.account_id) === String(account?.id) && item.channel === 'call' && !item.contact_id);
      add('SUPPRESS_CALL', { channel: 'call', reason: followup || notes }, ['follow_up_needed', 'notes'], {}, currentSuppression || null);
    } else if (!/^no\b/i.test(followup) && (/^yes\b/i.test(followup) || text(values.next_step))) {
      const taskSource = [text(values.next_step), notes].filter(Boolean).join('\n');
      const deadline = taskDeadline(taskSource, input.asOf);
      if (deadline.error) conflict(deadline.error, text(values.next_step) ? 'next_step' : 'notes', deadline.message);
      const description = taskSource && !/^\d{1,2}\/\d{1,2}:?\s*$/.test(taskSource) ? `Review source follow-up and prepare next action: ${taskSource}` : 'Clarify missing follow-up action and outcome with source owner.';
      const kind = /research/i.test(taskSource) ? 'research' : /email/i.test(taskSource) ? 'prepare_email_draft' : 'review_follow_up';
      add('ADD_TASK', { kind, description, dueDate: deadline.dueDate, externalSendingAuthorized: false }, ['follow_up_needed', 'next_step', 'notes']);
    }
    if (/^no\b/i.test(followup) && text(values.next_step)) conflict('FOLLOWUP_CONFLICT', 'next_step', 'Explicit next action conflicts with a No follow-up field; review source intent before creating a task.');
    report.admissionHolds = report.operations.filter(operation => operation.outreachReviewRequired || operation.after?.outreachReviewRequired).map(operation => ({ operationId: operation.id, field: 'ao_outreach_review_required', before: operation.admissionChange?.before ?? null, after: true, reason: 'Hold automated outreach pending separate admission approval; this is not an opt-out.' }));
    report.followUp = { raw: followup || null, status: /^no\b/i.test(followup) ? 'not_requested' : /^yes\b/i.test(followup) ? 'requested' : 'unspecified' };
    for (const [field, value] of Object.entries(values)) {
      if (text(value) && !KNOWN_FIELDS.has(field)) conflict('UNMAPPED_FIELD', field, 'Source field preserved but needs an explicit mapping.');
      if (text(value) && (field === 'ao' || (field === 'status' && !/\bapplication\s+(?:is\s+)?in\s+progress\b/i.test(value)))) conflict('EXPLICIT_FIELD_REVIEW', field, 'Source field requires explicit supported mapping before persistence.');
    }
    const rowBlocked = report.conflicts.some(item => item.blocking);
    const createOperation = report.operations.find(operation => operation.type === 'CREATE_ACCOUNT' && !operation.blocked);
    for (const operation of report.operations) {
      operation.blocked = operation.blocked || rowBlocked;
      if (createOperation && operation !== createOperation) operation.dependsOn = [createOperation.id];
    }
    report.resolutionDecisions = decision;
    report.outcome = report.conflicts.length || report.operations.some(operation => operation.blocked) ? 'needs_review' : report.operations.length ? 'proposed_changes' : 'unchanged';
    rows.push(report);
  }
  // A workbook is one proposal: contradictory mutations to the same target
  // cannot be exposed as independently selectable operations.
  const fieldGroups = new Map();
  for (const operation of operations) {
    if (!operation.target?.accountId || !['SET_ACCOUNT_FIELD', 'CREATE_ACCOUNT'].includes(operation.type)) continue;
    const key = `${operation.target.accountId}:${operation.type}:${operation.field || ''}`;
    if (!fieldGroups.has(key)) fieldGroups.set(key, []);
    fieldGroups.get(key).push(operation);
  }
  const markConflict = (operation, code, message) => {
    operation.blocked = true;
    const row = rows.find(item => item.operations.includes(operation));
    if (row && !row.conflicts.some(item => item.code === code && item.field === operation.field)) row.conflicts.push({ code, field: operation.field || 'company', message, blocking: true });
  };
  for (const group of fieldGroups.values()) {
    if (group.length < 2) continue;
    const meaning = operation => operation.type === 'CREATE_ACCOUNT' ? norm(operation.after.name) : operation.field === 'phone' ? normalizedPhone(operation.after) : norm(operation.after);
    if (new Set(group.map(meaning)).size > 1) {
      for (const operation of group) markConflict(operation, 'CROSS_ROW_FIELD_CONFLICT', 'Source rows propose different values for the same CRM field; resolve the workbook contradiction before approval.');
    } else {
      const primary = group.find(operation => !operation.blocked) || group[0];
      for (const duplicate of group.filter(operation => operation !== primary)) {
        const knownEvidence = new Set(primary.evidence.map(digest));
        primary.evidence.push(...duplicate.evidence.filter(item => !knownEvidence.has(digest(item))));
        duplicate.duplicateOf = primary.id;
        markConflict(duplicate, 'DUPLICATE_PROPOSED_EFFECT', `Equivalent operation is already proposed as ${primary.id}; duplicate is held.`);
      }
    }
  }
  let dependencyChanged = true;
  while (dependencyChanged) {
    dependencyChanged = false;
    for (const operation of operations) if (!operation.blocked && operation.dependsOn.some(id => operations.find(item => item.id === id)?.blocked)) {
      markConflict(operation, 'BLOCKED_DEPENDENCY', 'Required account creation is held; dependent operation cannot be approved.');
      dependencyChanged = true;
    }
  }
  for (const row of rows) row.outcome = row.conflicts.length || row.operations.some(operation => operation.blocked) ? 'needs_review' : row.operations.length ? 'proposed_changes' : 'unchanged';
  return { version: 1, tenantId, actorId: input.actorId || input.scope?.actorId || null, resolutions: input.resolutions || [], fileHash, sourceHash: fileHash, filename: structuredData.filename || null, rows, operations, sourceRows: structuredData.sourceRows || structuredData.sheets?.flatMap(sheet => sheet.sourceRows || []) || [], summary: { rows: rows.length, operations: operations.length, selectableOperations: operations.filter(operation => !operation.blocked).length, heldRows: rows.filter(row => row.outcome === 'needs_review').length, unchangedRows: rows.filter(row => row.outcome === 'unchanged').length } };
}
function formatSpreadsheetProposal(proposal) {
  const lines = [`Source SHA-256: ${proposal.sourceHash}`, `Source accounting: ${proposal.sourceRows.length} source rows including ${proposal.sourceRows.filter(row => row.classification === 'header').length} header and ${proposal.sourceRows.filter(row => row.classification === 'legend').length} legend rows (including merged/blank layout rows).`, `Spreadsheet comparison: ${proposal.summary.rows} business rows; ${proposal.summary.selectableOperations} selectable proposed operations. Nothing saved.`, 'Approval applies only to selected operation IDs in this server-owned proposal. Held operations cannot be saved.'];
  for (const row of proposal.rows) {
    lines.push('', `${row.sheet}!${row.rowNumber} — ${row.company || '(missing business)'} — ${row.outcome}`, `CRM account: ${row.accountResolution.account?.id || 'unresolved'}; candidates: ${row.accountResolution.candidates.join(', ') || 'none'}.`);
    for (const contact of [...row.contacts, ...row.contactAssertions]) lines.push(`Contact: ${contact.name}${contact.possibleAlias ? ` (source alias: ${contact.possibleAlias})` : ''}; ${contact.status}.`);
    for (const comparison of row.comparisons) lines.push(`Compare ${comparison.field}: ${JSON.stringify(comparison.existing ?? null)} → ${JSON.stringify(comparison.incoming)} (${comparison.result}).`);
    for (const operation of row.operations) lines.push(`${operation.blocked ? 'HELD' : 'PROPOSED'} ${operation.id}: ${operation.type}${operation.field ? ` ${operation.field}` : ''}; ${JSON.stringify(operation.before)} → ${JSON.stringify(operation.after)}; evidence ${operation.evidence.map(item => `${item.sheet}!${item.cell || `row ${item.row} ${item.field}`}`).join(', ')}.`);
    for (const hold of row.admissionHolds || []) lines.push(`Admission hold ${hold.operationId}: ${JSON.stringify(hold.before)} → true. ${hold.reason}`);
    for (const conflict of row.conflicts) lines.push(`Review ${conflict.field}: ${conflict.message}`);
    if (!row.operations.length && !row.conflicts.length) lines.push('No change: all supplied facts already present. Blank cells do not clear CRM.');
  }
  return lines.join('\n');
}
module.exports = { taskSemantics, taskDeadline, existingEffectIntact, buildSpreadsheetProposal, formatSpreadsheetProposal, isoDate, identityConflict, matchAccount, digest };
